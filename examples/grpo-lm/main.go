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
// The weights do not travel on the channel. A 270M model is about 540 MB at
// bf16, and Emit JSON-marshals a value and buffers it whole on both sides.
// The learner writes a checkpoint to shared storage and broadcasts a small
// reference; rollout loads it. (sdk.EmitBlob would let the bytes travel pod to
// pod instead, which would drop the shared-storage requirement.)
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
	"hash/fnv"
	"log"
	"os"
	"sort"

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
	genAddr := fs.String("generator", "http://127.0.0.1:8100", "rollout sidecar")
	trainAddr := fs.String("trainer", "http://127.0.0.1:8200", "learner sidecar")
	if len(os.Args) < 2 {
		log.Fatal("usage: grpo-lm [flags] prompts|rollout|reward|advantage|learner")
	}
	op := os.Args[len(os.Args)-1]
	_ = fs.Parse(os.Args[1 : len(os.Args)-1])

	w, err := sdk.FromEnv()
	if err != nil {
		log.Fatal(err)
	}
	if err := w.Run(context.Background(), handlers(op, *groupSize,
		newHTTPGenerator(*genAddr), newHTTPTrainer(*trainAddr))); err != nil {
		log.Fatal(err)
	}
}

// handlers is the whole application, with the model behind an interface so a
// test can drive the same graph on CPU.
func handlers(op string, groupSize int, gen generator, tr trainer) sdk.Handlers {
	var h sdk.Handlers
	switch op {
	case "prompts":
		h.Source = func(ctx context.Context, w *sdk.Worker) error {
			for _, id := range sortedTaskIDs(tasks) {
				if err := w.Emit("batch", id, tasks[id]); err != nil {
					return err
				}
			}
			return nil
		}

	case "rollout":
		owned := map[string]bool{}
		draw := func(ctx context.Context, w *sdk.Worker, id string) error {
			tk := tasks[id]
			// Seeded from (task, step) so which replica runs a rollout does
			// not change what it draws.
			hsh := fnv.New64a()
			_, _ = hsh.Write([]byte(id))
			seed := int64(hsh.Sum64()) ^ int64(w.Epoch())*1_000_003
			texts, err := gen.Generate(ctx, tk.prompt(), groupSize, seed)
			if err != nil {
				return err
			}
			if len(texts) != groupSize {
				return fmt.Errorf("generator returned %d completions, want %d", len(texts), groupSize)
			}
			for i, text := range texts {
				if err := w.Emit("completions", id, completion{Task: id, Index: i, Text: text}); err != nil {
					return err
				}
			}
			return nil
		}
		h.OnRecord = func(ctx context.Context, w *sdk.Worker, r sdk.Record) error {
			switch r.Channel {
			case "batch":
				owned[r.Key] = true
				return draw(ctx, w, r.Key)
			case "weights":
				var cp checkpoint
				if err := json.Unmarshal(r.Value, &cp); err != nil {
					return err
				}
				// The expensive part of a step, and the reason a real system
				// updates the inference engine in place instead.
				if err := gen.Load(ctx, cp.URI); err != nil {
					return err
				}
				for _, id := range sortedKeys(owned) {
					if err := draw(ctx, w, id); err != nil {
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
			c.Reward, c.Detail = tasks[c.Task].score(c.Text)
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
			if len(open[step]) < len(tasks) {
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
						Prompt: tasks[g.Task].prompt(), Completion: s.Text, Advantage: s.Advantage})
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

func sortedKeys(m map[string]bool) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}
