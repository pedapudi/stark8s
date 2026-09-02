// Command grpo runs Group Relative Policy Optimization as a cyclic workload.
//
//	prompts    (source) emits one record per task, Hash-partitioned so each
//	                    rollout replica owns a disjoint set of them.
//	rollout             holds the current policy and the tasks it owns. For
//	                    each task it draws a group of G completions and emits
//	                    them. It redraws every time new weights arrive.
//	reward              scores one completion. The reward is verifiable, so
//	                    there is no reward model and no judge in the graph.
//	advantage           gathers the G scores of one task and centres them:
//	                    (r - mean) / std. That group statistic is GRPO's
//	                    baseline, which is why no critic appears anywhere.
//	learner             the one owner of theta. It gathers a group from every
//	                    task, applies the update, and broadcasts the new
//	                    weights back to every rollout replica.
//
// Two barriers matter and neither is a channel attribute. `advantage` holds a
// group open until it has all G rewards for one task; `learner` holds a step
// open until it has a group from every task. Materialized delivery cannot do
// this job, because it seals when the producing operation completes and in a
// training loop no operation ever completes. Both counts are constants — the
// group size and the task set are hyperparameters — which is exactly why this
// graph is expressible at all.
//
// The epoch is the step number. It enters the cycle on a weights record and
// every downstream emit inherits it, so a completion, its score and its
// advantage all carry the step they belong to. The engine ends the run: the
// weights record produced by the last update is stamped past maxEpochs and
// dropped.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"sort"

	"github.com/pedapudi/stark8s/pkg/sdk"
)

// The task set. Each task asks for one fixed sequence and scores a completion
// by the fraction of positions it got right, so the optimum is known and the
// run can be checked rather than eyeballed.
var tasks = map[string][]int{
	"alpha": {2, 0, 3},
	"beta":  {1, 3, 1},
	"gamma": {0, 2, 2},
	"delta": {3, 1, 0},
}

const (
	positions = 3 // T: tokens per completion
	vocab     = 4 // V: tokens to choose from
)

func reward(task string, tokens []int) float64 {
	want, ok := tasks[task]
	if !ok || len(tokens) != len(want) {
		return 0
	}
	hit := 0.0
	for i, tok := range tokens {
		if tok == want[i] {
			hit++
		}
	}
	return hit / float64(len(want))
}

// completion travels from rollout to reward, and on to advantage with a score.
type completion struct {
	Task    string  `json:"task"`
	Index   int     `json:"index"`
	Tokens  []int   `json:"tokens"`
	OldLogP float64 `json:"oldLogP"`
	Reward  float64 `json:"reward,omitempty"`
}

// group is one task's completions with their advantages, the unit the learner
// consumes. A whole group travels as one record: cost is per record, so
// sending G of them separately would be paid for G times.
type group struct {
	Task    string      `json:"task"`
	Samples []sampleRec `json:"samples"`
}

// metric is what an outside reader sees while the run is in flight.
type metric struct {
	Step       int32   `json:"step"`
	RewardMean float64 `json:"rewardMean"`
	Objective  float64 `json:"objective"`
	KL         float64 `json:"kl"`
}

func main() {
	fs := flag.NewFlagSet("grpo", flag.ExitOnError)
	groupSize := fs.Int("group", 8, "completions sampled per task (G)")
	lr := fs.Float64("lr", 0.5, "step size")
	eps := fs.Float64("clip", 0.2, "importance-ratio clip")
	beta := fs.Float64("kl", 0.001, "weight on the KL to the reference policy")
	if len(os.Args) < 2 {
		log.Fatal("usage: grpo [flags] prompts|rollout|reward|advantage|learner")
	}
	op := os.Args[len(os.Args)-1]
	_ = fs.Parse(os.Args[1 : len(os.Args)-1])

	w, err := sdk.FromEnv()
	if err != nil {
		log.Fatal(err)
	}

	var h sdk.Handlers
	switch op {
	case "prompts":
		h.Source = func(ctx context.Context, w *sdk.Worker) error {
			for _, task := range sortedTasks() {
				if err := w.Emit("batch", task, tasks[task]); err != nil {
					return err
				}
			}
			return nil
		}

	case "rollout":
		// Every replica starts from the same policy, so step 0 needs no
		// weights record to bootstrap it.
		pol := newPolicy(positions, vocab, sortedTasks())
		owned := map[string]bool{}
		draw := func(w *sdk.Worker, task string) error {
			for i := 0; i < *groupSize; i++ {
				tokens, lp := pol.sample(task, w.Epoch(), i)
				c := completion{Task: task, Index: i, Tokens: tokens, OldLogP: lp}
				if err := w.Emit("completions", task, c); err != nil {
					return err
				}
			}
			return nil
		}
		h.OnRecord = func(ctx context.Context, w *sdk.Worker, r sdk.Record) error {
			switch r.Channel {
			case "batch":
				owned[r.Key] = true
				return draw(w, r.Key)
			case "weights":
				if err := json.Unmarshal(r.Value, &pol.Theta); err != nil {
					return err
				}
				for _, task := range sortedKeys(owned) {
					if err := draw(w, task); err != nil {
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
			c.Reward = reward(c.Task, c.Tokens)
			return w.Emit("scored", c.Task, c)
		}

	case "advantage":
		// A group is keyed by (step, task): the step because the loop revisits
		// every task, the task because the baseline is within a task.
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
			if len(open[k]) < *groupSize {
				return nil // the group is not complete yet
			}
			cs := open[k]
			delete(open, k)
			sort.Slice(cs, func(i, j int) bool { return cs[i].Index < cs[j].Index })

			rewards := make([]float64, len(cs))
			for i, c := range cs {
				rewards[i] = c.Reward
			}
			adv := advantages(rewards)
			g := group{Task: c.Task, Samples: make([]sampleRec, len(cs))}
			for i, c := range cs {
				g.Samples[i] = sampleRec{Prompt: c.Task, Tokens: c.Tokens, OldLogP: c.OldLogP, Adv: adv[i]}
			}
			return w.Emit("advantages", c.Task, g)
		}

	case "learner":
		pol := newPolicy(positions, vocab, sortedTasks())
		ref := pol.clone() // frozen: the KL term pulls back toward it
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

			var batch []sampleRec
			mean, n := 0.0, 0.0
			for _, g := range groups {
				for _, s := range g.Samples {
					batch = append(batch, s)
					mean += reward(g.Task, s.Tokens)
					n++
				}
			}
			obj, kl := pol.step(batch, ref, *lr, *eps, *beta)
			m := metric{Step: step, RewardMean: mean / n, Objective: obj, KL: kl}
			log.Printf("step %d reward=%.3f obj=%+.4f kl=%.4f", m.Step, m.RewardMean, m.Objective, m.KL)
			if err := w.Emit("metrics", fmt.Sprintf("step-%03d", step), m); err != nil {
				return err
			}
			if err := w.Emit("checkpoints", fmt.Sprintf("step-%03d", step), pol.Theta); err != nil {
				return err
			}
			// Broadcast to every rollout replica. This is the feedback edge:
			// the record is stamped with the next step, and when that reaches
			// maxEpochs the engine drops it and the loop ends.
			return w.Emit("weights", "theta", pol.Theta)
		}

	default:
		log.Fatalf("unknown operation %q", op)
	}

	if err := w.Run(context.Background(), h); err != nil {
		log.Fatal(err)
	}
}

func sortedTasks() []string {
	out := make([]string, 0, len(tasks))
	for k := range tasks {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

func sortedKeys(m map[string]bool) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}
