// Command grpo-lm post-trains a language model with GRPO, as a Workload.
//
// It is examples/grpo with the table replaced by a real model. The graph is
// the same shape, and everything that made the small one work is doing the
// same job here:
//
//   - the group is a Hash partition on the prompt, so a task's completions
//     meet on one advantage replica and the group statistic needs no shuffle;
//   - the policy travels on a Broadcast feedback edge, so every rollout
//     replica gets the same one;
//   - the two barriers count to constants in application code, because
//     Materialized seals on producer completion and nothing here completes;
//   - the epoch is the step, and maxEpochs ends the run.
//
// Three things differ, and each is a consequence of the model being real.
//
// The weights do not travel on the channel. The model is gigabytes at bf16 and
// even a LoRA adapter is tens of megabytes, while Emit JSON-marshals a value
// and buffers it whole on both sides. The learner writes a checkpoint to
// shared storage and broadcasts a small reference; rollout loads it.
// (sdk.EmitBlob would let the bytes travel pod to pod instead, which would
// drop the shared-storage requirement.)
//
// The model runs beside the worker, not inside it. The SDK is Go; generation
// and training are not. Each pod runs the worker and a sidecar behind a
// localhost HTTP contract — see engine.go. The worker stays the SDK client.
//
// The reward is still a program. Every constraint in verify.go is decided by
// reading the completion, so no judge and no reward model enters the graph.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"sort"
	"strings"
	"time"

	"github.com/pedapudi/stark8s/pkg/sdk"
)

// checkpoint is what the weights channel carries: a reference, not a tensor.
type checkpoint struct {
	Step int32  `json:"step"`
	URI  string `json:"uri"`
}

// completion travels rollout -> reward, and on to advantage with a score.
type completion struct {
	Task   string          `json:"task"`
	Index  int             `json:"index"`
	Text   string          `json:"text"`
	Reward float64         `json:"reward,omitempty"`
	Detail map[string]bool `json:"detail,omitempty"`
}

// group is one task's completions with their advantages: the unit the learner
// consumes, sent as one record because cost is per record.
type group struct {
	Task    string   `json:"task"`
	Samples []scored `json:"samples"`
}

type scored struct {
	Text      string  `json:"text"`
	Reward    float64 `json:"reward"`
	Advantage float64 `json:"advantage"`
}

type metric struct {
	Step       int32              `json:"step"`
	RewardMean float64            `json:"rewardMean"`
	Objective  float64            `json:"objective"`
	KL         float64            `json:"kl"`
	Checkpoint string             `json:"checkpoint"`
	PerTask    map[string]float64 `json:"perTask"`
	// Degenerate counts the groups whose completions all scored the same.
	// Those contribute no gradient, so this is the number to watch: if it
	// stays at the group count, the task is too hard or too easy and the run
	// is learning nothing however healthy it looks.
	Degenerate int `json:"degenerateGroups"`
}

func main() {
	fs := flag.NewFlagSet("grpo-lm", flag.ExitOnError)
	groupSize := fs.Int("group", 8, "completions sampled per task (G)")
	slots := fs.Int("slots", 16, "fresh training instances per step")
	heldN := fs.Int("held", 32, "size of the fixed held-out set")
	heldG := fs.Int("heldgroup", 8, "completions per held-out instance")
	evalEvery := fs.Int("evalevery", 4, "evaluate held-out every N steps")
	baseReps := fs.Int("basereps", 5, "repeat the step-0 evaluation this many times")
	genAddr := fs.String("generator", "http://127.0.0.1:8100", "rollout sidecar")
	trainAddr := fs.String("trainer", "http://127.0.0.1:8200", "learner sidecar")
	if len(os.Args) < 2 {
		log.Fatal("usage: grpo-lm [flags] prompts|rollout|reward|advantage|learner|score|dump")
	}
	op := os.Args[len(os.Args)-1]
	_ = fs.Parse(os.Args[1 : len(os.Args)-1])

	// score reads {tag: [completions]} on stdin and prints the per-constraint
	// rates. It is the calibration gate: a task the model already satisfies,
	// or never satisfies, cannot show learning, and that is cheaper to find
	// out here than after an hour of accelerator time.
	if op == "score" {
		if err := calibrate(os.Stdin, os.Stdout); err != nil {
			log.Fatal(err)
		}
		return
	}
	// dump prints {tag: prompt} for the held-out set, so the calibration
	// sampling can be driven against a bare sidecar with no graph running.
	if op == "dump" {
		out := map[string]string{}
		for _, tk := range held(*heldN) {
			out[tk.ID] = tk.prompt()
		}
		if err := json.NewEncoder(os.Stdout).Encode(out); err != nil {
			log.Fatal(err)
		}
		return
	}

	cfg := runCfg{group: *groupSize, slots: *slots, heldN: *heldN,
		heldG: *heldG, evalEvery: *evalEvery, baseReps: *baseReps}

	w, err := sdk.FromEnv()
	if err != nil {
		log.Fatal(err)
	}
	if op == "rollout" || op == "learner" {
		addr := *genAddr
		if op == "learner" {
			addr = *trainAddr
		}
		if err := awaitSidecar(addr, 30*time.Minute); err != nil {
			log.Fatal(err)
		}
		log.Printf("%s: sidecar ready at %s", op, addr)
	}
	if err := w.Run(context.Background(), handlers(op, cfg,
		newHTTPGenerator(*genAddr), newHTTPTrainer(*trainAddr))); err != nil {
		log.Fatal(err)
	}
}

// runCfg is the shape of a run: how many instances, how many samples each, and
// how often to measure against the held-out set.
type runCfg struct {
	group, slots, heldN, heldG, evalEvery, baseReps int
}

// awaitSidecar blocks until the model process accepts a connection. The
// sidecar binds its port only after the weights are on the accelerator, so a
// successful dial is an exact readiness signal.
//
// A worker that consumes first takes a segment, fails its request, and exits.
// The container restarts inside a pod that is still alive, and the coordinator
// returns in-flight segments to the pending set only when a pod is gone, so
// those records are never redelivered and the graph stalls with every pod
// Running and no error anywhere.
func awaitSidecar(addr string, limit time.Duration) error {
	host := strings.TrimPrefix(addr, "http://")
	deadline := time.Now().Add(limit)
	for {
		c, err := net.DialTimeout("tcp", host, 2*time.Second)
		if err == nil {
			c.Close()
			return nil
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("sidecar %s not ready after %s: %w", host, limit, err)
		}
		time.Sleep(2 * time.Second)
	}
}

// handlers is the whole application, with the model behind an interface so a
// test can drive the same graph on CPU.
func handlers(op string, cfg runCfg, gen generator, tr trainer) sdk.Handlers {
	groupSize := cfg.group
	var h sdk.Handlers
	switch op {
	case "prompts":
		h.Source = func(ctx context.Context, w *sdk.Worker) error {
			// Slot indices, not instances. The slot is what gets partitioned
			// across rollout replicas and stays fixed for the run; the
			// instance behind it changes every epoch, which is what keeps
			// there being nothing to memorize.
			for k := 0; k < cfg.slots; k++ {
				if err := w.Emit("batch", fmt.Sprintf("slot-%d", k), k); err != nil {
					return err
				}
			}
			return nil
		}

	case "rollout":
		// owned is the set of slots this replica was given by the batch
		// channel's partitioning. Redrawing every slot on every replica would
		// duplicate the whole step, so a replica only ever draws its own.
		owned := map[int]bool{}

		draw := func(ctx context.Context, w *sdk.Worker, slot int) error {
			tag := trainTag(w.Epoch(), slot)
			tk := instance(tag)
			texts, err := gen.Generate(ctx, tk.prompt(), groupSize, int64(seedOf(tag)))
			if err != nil {
				return err
			}
			if len(texts) != groupSize {
				return fmt.Errorf("generator returned %d completions, want %d", len(texts), groupSize)
			}
			for i, text := range texts {
				if err := w.Emit("completions", tag, completion{Task: tag, Index: i, Text: text}); err != nil {
					return err
				}
			}
			return nil
		}

		// evaluate scores the fixed held-out set with whatever policy the
		// generator currently holds. It runs here rather than as its own
		// operation because the reward is string comparison: shipping the
		// completions across the graph to spend microseconds on each would
		// measure the graph, not the policy.
		evaluate := func(ctx context.Context, w *sdk.Worker, step int32, rep int) error {
			sum, n, strict := 0.0, 0.0, 0.0
			per, cnt := map[string]float64{}, map[string]float64{}
			for _, tk := range held(cfg.heldN) {
				texts, err := gen.Generate(ctx, tk.prompt(), cfg.heldG,
					int64(seedOf(tk.ID))^int64(rep)*7919)
				if err != nil {
					return err
				}
				for _, t := range texts {
					r, detail := tk.score(t)
					sum += r
					n++
					if tk.strict(t) {
						strict++
					}
					for k, ok := range detail {
						kind := strings.SplitN(k, ":", 2)[0]
						cnt[kind]++
						if ok {
							per[kind]++
						}
					}
				}
			}
			// Iterate the counts, not the hits: a constraint satisfied zero
			// times has no entry in per, and dividing only the hits would drop
			// it from the report, which reads as "not measured" rather than
			// "never met".
			for k, c := range cnt {
				per[k] = per[k] / c
			}
			e := evalPoint{Step: step, Rep: rep, Mean: sum / n, Strict: strict / n,
				Samples: int(n), PerConstraint: per}
			log.Printf("eval step=%d rep=%d held_strict=%.4f held_graded=%.4f n=%d per=%v",
				step, rep, e.Strict, e.Mean, int(n), per)
			return w.Emit("eval", fmt.Sprintf("eval-%03d-%d", step, rep), e)
		}

		h.OnRecord = func(ctx context.Context, w *sdk.Worker, r sdk.Record) error {
			switch r.Channel {
			case "batch":
				var slot int
				if err := json.Unmarshal(r.Value, &slot); err != nil {
					return err
				}
				owned[slot] = true
				// The base band, measured once by whichever replica holds slot
				// zero. Repeating the step-0 evaluation gives the spread of the
				// untrained policy on these instances, which is what any later
				// number has to beat to mean anything.
				if slot == 0 {
					for rep := 0; rep < cfg.baseReps; rep++ {
						if err := evaluate(ctx, w, 0, rep); err != nil {
							return err
						}
					}
				}
				return draw(ctx, w, slot)

			case "weights":
				var cp checkpoint
				if err := json.Unmarshal(r.Value, &cp); err != nil {
					return err
				}
				if err := gen.Load(ctx, cp.URI); err != nil {
					return err
				}
				if owned[0] && cfg.evalEvery > 0 && int(w.Epoch())%cfg.evalEvery == 0 {
					if err := evaluate(ctx, w, w.Epoch(), 0); err != nil {
						return err
					}
				}
				for _, slot := range sortedInts(owned) {
					if err := draw(ctx, w, slot); err != nil {
						return err
					}
				}
			}
			return nil
		}

	case "reward":
		h.OnRecord = func(ctx context.Context, w *sdk.Worker, r sdk.Record) error {
			var c completion
			if err := json.Unmarshal(r.Value, &c); err != nil {
				return err
			}
			c.Reward, c.Detail = instance(c.Task).score(c.Text)
			return w.Emit("scored", c.Task, c)
		}

	case "advantage":
		type key struct {
			step int32
			task string
		}
		open := map[key][]completion{}
		h.OnRecord = func(ctx context.Context, w *sdk.Worker, r sdk.Record) error {
			var c completion
			if err := json.Unmarshal(r.Value, &c); err != nil {
				return err
			}
			k := key{r.Epoch, c.Task}
			open[k] = append(open[k], c)
			if len(open[k]) < groupSize {
				return nil // the group is not complete
			}
			cs := open[k]
			delete(open, k)
			sort.Slice(cs, func(i, j int) bool { return cs[i].Index < cs[j].Index })

			rewards := make([]float64, len(cs))
			for i, c := range cs {
				rewards[i] = c.Reward
			}
			adv := advantages(rewards)
			g := group{Task: c.Task, Samples: make([]scored, len(cs))}
			for i, c := range cs {
				g.Samples[i] = scored{Text: c.Text, Reward: c.Reward, Advantage: adv[i]}
			}
			return w.Emit("advantages", c.Task, g)
		}

	case "learner":
		open := map[int32][]group{}
		h.OnRecord = func(ctx context.Context, w *sdk.Worker, r sdk.Record) error {
			var g group
			if err := json.Unmarshal(r.Value, &g); err != nil {
				return err
			}
			step := r.Epoch
			open[step] = append(open[step], g)
			if len(open[step]) < cfg.slots {
				return nil // still waiting on other tasks
			}
			groups := open[step]
			delete(open, step)
			sort.Slice(groups, func(i, j int) bool { return groups[i].Task < groups[j].Task })

			var batch []trainSample
			perTask := map[string]float64{}
			mean, n, degenerate := 0.0, 0.0, 0
			for _, g := range groups {
				flat := true
				sum := 0.0
				for _, s := range g.Samples {
					batch = append(batch, trainSample{
						Prompt: instance(g.Task).prompt(), Completion: s.Text, Advantage: s.Advantage})
					mean += s.Reward
					sum += s.Reward
					n++
					if s.Advantage != 0 {
						flat = false
					}
				}
				perTask[g.Task] = sum / float64(len(g.Samples))
				if flat {
					degenerate++
				}
			}
			res, err := tr.Step(ctx, batch, step)
			if err != nil {
				return err
			}
			m := metric{Step: step, RewardMean: mean / n, Objective: res.Objective, KL: res.KL,
				Checkpoint: res.Checkpoint, PerTask: perTask, Degenerate: degenerate}
			log.Printf("step %d reward=%.3f kl=%.4f degenerate=%d/%d ckpt=%s",
				m.Step, m.RewardMean, m.KL, m.Degenerate, len(groups), m.Checkpoint)
			if err := w.Emit("metrics", fmt.Sprintf("step-%03d", step), m); err != nil {
				return err
			}
			return w.Emit("weights", "policy", checkpoint{Step: step + 1, URI: res.Checkpoint})
		}

	default:
		log.Fatalf("unknown operation %q", op)
	}
	return h
}

func sortedInts(m map[int]bool) []int {
	out := make([]int, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Ints(out)
	return out
}

// evalPoint is one measurement of the fixed held-out set. Rep distinguishes
// repeats of the same step, which is how the base band at step 0 is built.
type evalPoint struct {
	Step int32 `json:"step"`
	Rep  int   `json:"rep"`
	// Mean is the graded reward, which is what training optimizes. Strict is
	// the fraction of completions meeting every constraint, which is what the
	// task actually asks for. Reporting only Mean would let partial credit
	// stand in for success.
	Mean          float64            `json:"mean"`
	Strict        float64            `json:"strict"`
	Samples       int                `json:"samples"`
	PerConstraint map[string]float64 `json:"perConstraint"`
}

// firstTag names the instance whose arrival marks the start of a run, so the
// base evaluation happens exactly once rather than once per slot.
func firstTag(slots int) string {
	return trainBatch(0, slots)[0].ID
}

// calibrate scores completions produced outside the graph and reports the rate
// at which each constraint kind is met. Input is {"tag": ["completion", ...]}.
//
// A constraint the model meets almost always, or almost never, leaves a group
// with no spread, and a group with no spread produces no gradient. Running
// this before a training run is what tells the two apart from a task that is
// worth training on.
func calibrate(in io.Reader, out io.Writer) error {
	var byTag map[string][]string
	if err := json.NewDecoder(in).Decode(&byTag); err != nil {
		return err
	}
	per, cnt := map[string]float64{}, map[string]float64{}
	var rewards []float64
	strictHits, degenerate, groups := 0, 0, 0
	tags := make([]string, 0, len(byTag))
	for t := range byTag {
		tags = append(tags, t)
	}
	sort.Strings(tags)
	for _, tag := range tags {
		tk := instance(tag)
		groups++
		lo, hi := 2.0, -1.0
		for _, text := range byTag[tag] {
			r, detail := tk.score(text)
			rewards = append(rewards, r)
			if tk.strict(text) {
				strictHits++
			}
			if r < lo {
				lo = r
			}
			if r > hi {
				hi = r
			}
			for k, ok := range detail {
				kind := strings.SplitN(k, ":", 2)[0]
				cnt[kind]++
				if ok {
					per[kind]++
				}
			}
		}
		if hi-lo < 1e-9 {
			degenerate++
		}
	}
	mean := 0.0
	for _, r := range rewards {
		mean += r / float64(len(rewards))
	}
	fmt.Fprintf(out, "instances=%d completions=%d mean reward=%.3f\n", groups, len(rewards), mean)
	fmt.Fprintf(out, "all constraints met: %.1f%% (%d/%d)\n",
		100*float64(strictHits)/float64(len(rewards)), strictHits, len(rewards))
	fmt.Fprintf(out, "groups with no spread: %d/%d\n", degenerate, groups)
	kinds := make([]string, 0, len(per))
	for k := range cnt {
		kinds = append(kinds, k)
	}
	sort.Strings(kinds)
	fmt.Fprintf(out, "%-12s %8s\n", "constraint", "met")
	for _, k := range kinds {
		fmt.Fprintf(out, "%-12s %7.1f%%\n", k, 100*per[k]/cnt[k])
	}
	return nil
}
