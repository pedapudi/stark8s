// Command grpo-lean runs GRPO over a theorem prover as a Workload.
//
// The graph is examples/grpo. What this example adds is a reward that costs
// seconds of CPU per completion and wants no accelerator at all, so generation
// and verification end up on different machines at different replica counts.
// That separation is the point; see the README for the measurement.
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"math"
	"net"
	"net/http"
	"os"
	"sort"
	"strings"
	"time"

	"github.com/pedapudi/stark8s/pkg/sdk"
)

type checkpoint struct {
	Step int32  `json:"step"`
	URI  string `json:"uri"`
}

// completion carries the statement it was drawn for. The reward pods hold a
// Mathlib checkout and nothing else; making them look the problem up would
// give every replica a second copy of the dataset and a way to disagree with
// the generator about which goal was asked.
type completion struct {
	Slot      string  `json:"slot"`
	Index     int     `json:"index"`
	Statement string  `json:"statement"`
	Text      string  `json:"text"`
	Reward    float64 `json:"reward"`
	// Cat is what the kernel said: proved, unsolved, unknown-lemma,
	// parse-error, timeout. It is the reward's explanation, and it is what
	// tells a flat curve apart from a broken harness.
	Cat string `json:"cat,omitempty"`
}

type sample struct {
	Prompt     string  `json:"prompt"`
	Completion string  `json:"completion"`
	Advantage  float64 `json:"advantage"`
}

type group struct {
	Slot string `json:"slot"`
	// Proved travels with the group because the advantages cannot recover it:
	// a group where every completion proves the goal and a group where none
	// does both centre to all-zero.
	Proved int `json:"proved"`
	// Sum is the group's total graded reward. Neither it nor Proved survives
	// the advantages: a group where every completion proves the goal and one
	// where none does both centre to all-zero.
	Sum     float64  `json:"sum"`
	Samples []sample `json:"samples"`
}

type metric struct {
	Step       int32   `json:"step"`
	Proved     int     `json:"proved"`
	Attempted  int     `json:"attempted"`
	RewardMean float64 `json:"rewardMean"`
	Degenerate int     `json:"degenerateGroups"`
	Checkpoint string  `json:"checkpoint"`
}

var client = &http.Client{Timeout: 30 * time.Minute}

func post(url string, in, out any) error {
	b, _ := json.Marshal(in)
	resp, err := client.Post(url, "application/json", bytes.NewReader(b))
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode/100 != 2 {
		m, _ := io.ReadAll(io.LimitReader(resp.Body, 1024))
		return fmt.Errorf("%s: %s: %s", url, resp.Status, m)
	}
	if out == nil {
		io.Copy(io.Discard, resp.Body)
		return nil
	}
	return json.NewDecoder(resp.Body).Decode(out)
}

// advantages centers one group's rewards: GRPO's baseline, and the reason no
// critic appears anywhere in the graph.
func advantages(rs []float64) []float64 {
	n := float64(len(rs))
	mean := 0.0
	for _, r := range rs {
		mean += r / n
	}
	v := 0.0
	for _, r := range rs {
		v += (r - mean) * (r - mean)
	}
	sd := math.Sqrt(v / n)
	out := make([]float64, len(rs))
	if sd < 1e-8 {
		return out // a flat group says nothing and contributes no gradient
	}
	for i, r := range rs {
		out[i] = (r - mean) / sd
	}
	return out
}

// awaitSidecar blocks until the model process accepts a connection. The
// sidecar binds its port only after the weights are on the accelerator, so a
// successful dial is an exact readiness signal.
//
// A worker that starts consuming first takes a segment, fails the request, and
// exits. The container restarts inside a pod that is still alive, and the
// coordinator returns in-flight segments to the pending set only when a pod is
// gone, so those records are never redelivered and the graph stalls with every
// pod Running and no error anywhere.
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

func byID(id string) (problem, bool) {
	for _, p := range problems {
		if p.ID == id {
			return p, true
		}
	}
	return problem{}, false
}

func main() {
	fs := flag.NewFlagSet("grpo-lean", flag.ExitOnError)
	g := fs.Int("group", 8, "completions per statement")
	side := fs.String("sidecar", "http://127.0.0.1:8100", "model sidecar")
	root := fs.String("lean", "/work/mathlib4", "Mathlib checkout the kernel runs in")
	budget := fs.Duration("verify-timeout", 3*time.Minute, "per-proof kernel budget")
	if len(os.Args) < 2 {
		log.Fatal("usage: grpo-lean [flags] prompts|rollout|reward|advantage|learner")
	}
	op := os.Args[len(os.Args)-1]
	_ = fs.Parse(os.Args[1 : len(os.Args)-1])

	w, err := sdk.FromEnv()
	if err != nil {
		log.Fatal(err)
	}
	var h sdk.Handlers

	if op == "rollout" || op == "learner" {
		if err := awaitSidecar(*side, 30*time.Minute); err != nil {
			log.Fatal(err)
		}
		log.Printf("%s: sidecar ready at %s", op, *side)
	}

	switch op {
	case "prompts":
		h.Source = func(ctx context.Context, w *sdk.Worker) error {
			for _, p := range problems {
				if err := w.Emit("batch", p.ID, p); err != nil {
					return err
				}
			}
			return nil
		}

	case "rollout":
		owned := map[string]bool{}
		drawn := 0
		draw := func(w *sdk.Worker, id string) error {
			p, ok := byID(id)
			if !ok {
				return fmt.Errorf("unknown statement %q", id)
			}
			var out struct {
				Completions []string `json:"completions"`
			}
			if err := post(*side+"/generate", map[string]any{
				// 80 tokens is ample for a tactic. Budget against the longest
				// thing the model is asked to emit, or truncation quietly
				// becomes the dominant failure mode and reads as inability.
				"prompt": prompt(p), "n": *g, "max_new_tokens": 80,
			}, &out); err != nil {
				return err
			}
			for i, t := range out.Completions {
				if err := w.Emit("completions", id, completion{
					Slot: id, Index: i, Statement: p.Statement, Text: t}); err != nil {
					return err
				}
			}
			drawn++
			// The generation and verification stages run on different machines
			// at different replica counts, so the interesting quantity is how
			// long the graph spends in each. This line closes the generation
			// window; the learner's step line closes the verification one.
			log.Printf("epoch %d: generated %d completions (%d/%d statements)",
				w.Epoch(), len(out.Completions), drawn, len(problems))
			return nil
		}
		h.OnRecord = func(ctx context.Context, w *sdk.Worker, r sdk.Record) error {
			switch r.Channel {
			case "batch":
				var p problem
				if err := json.Unmarshal(r.Value, &p); err != nil {
					return err
				}
				owned[p.ID] = true
				return draw(w, p.ID)
			case "weights":
				var cp checkpoint
				if err := json.Unmarshal(r.Value, &cp); err != nil {
					return err
				}
				if err := post(*side+"/load", map[string]any{"checkpoint": cp.URI}, nil); err != nil {
					return err
				}
				keys := make([]string, 0, len(owned))
				for k := range owned {
					keys = append(keys, k)
				}
				sort.Strings(keys)
				drawn = 0
				for _, k := range keys {
					if err := draw(w, k); err != nil {
						return err
					}
				}
			}
			return nil
		}

	case "reward":
		// The whole reason this operation exists: seconds of CPU per record,
		// no accelerator, and a replica count that is the throughput dial.
		v := leanVerifier{Root: *root, Timeout: *budget}
		h.OnRecord = func(ctx context.Context, w *sdk.Worker, r sdk.Record) error {
			var c completion
			if err := json.Unmarshal(r.Value, &c); err != nil {
				return err
			}
			// One line, unfenced. Taking the whole completion would let
			// trailing prose fail an otherwise correct proof; over 136
			// completions the first line and the full block scored
			// identically, so nothing is lost by the narrower rule.
			tactic := firstLine(unfence(c.Text))
			c.Reward, c.Cat = v.Score(ctx, c.Statement, tactic)
			return w.Emit("scored", c.Slot, c)
		}

	case "advantage":
		type key struct {
			step int32
			slot string
		}
		open := map[key][]completion{}
		h.OnRecord = func(ctx context.Context, w *sdk.Worker, r sdk.Record) error {
			var c completion
			if err := json.Unmarshal(r.Value, &c); err != nil {
				return err
			}
			k := key{r.Epoch, c.Slot}
			open[k] = append(open[k], c)
			if len(open[k]) < *g {
				return nil // the group is not complete
			}
			cs := open[k]
			delete(open, k)
			sort.Slice(cs, func(i, j int) bool { return cs[i].Index < cs[j].Index })
			rs := make([]float64, len(cs))
			for i, c := range cs {
				rs[i] = c.Reward
			}
			adv := advantages(rs)
			p, _ := byID(c.Slot)
			gr := group{Slot: c.Slot, Samples: make([]sample, len(cs))}
			for i, c := range cs {
				gr.Samples[i] = sample{Prompt: prompt(p), Completion: c.Text, Advantage: adv[i]}
				gr.Sum += c.Reward
				if c.Reward == 1.0 {
					gr.Proved++
				}
			}
			return w.Emit("advantages", c.Slot, gr)
		}

	case "learner":
		open := map[int32][]group{}
		h.OnRecord = func(ctx context.Context, w *sdk.Worker, r sdk.Record) error {
			var gr group
			if err := json.Unmarshal(r.Value, &gr); err != nil {
				return err
			}
			open[r.Epoch] = append(open[r.Epoch], gr)
			if len(open[r.Epoch]) < len(problems) {
				return nil // still waiting on other statements
			}
			groups := open[r.Epoch]
			delete(open, r.Epoch)

			var batch []sample
			proved, attempted, deg, sum := 0, 0, 0, 0.0
			for _, gg := range groups {
				proved += gg.Proved
				sum += gg.Sum
				flat := true
				for _, s := range gg.Samples {
					attempted++
					if s.Advantage != 0 {
						flat = false
						batch = append(batch, s)
					}
				}
				if flat {
					deg++
				}
			}
			var res struct {
				Checkpoint string `json:"checkpoint"`
			}
			if err := post(*side+"/step", map[string]any{
				"samples": batch, "step": r.Epoch}, &res); err != nil {
				return err
			}
			m := metric{Step: r.Epoch, Proved: proved, Attempted: attempted,
				RewardMean: sum / float64(max(attempted, 1)),
				Degenerate: deg, Checkpoint: res.Checkpoint}
			log.Printf("step %d reward=%.3f proved=%d/%d degenerate=%d/%d ckpt=%s",
				m.Step, m.RewardMean, proved, attempted, deg, len(groups), res.Checkpoint)
			if err := w.Emit("metrics", fmt.Sprintf("step-%03d", r.Epoch), m); err != nil {
				return err
			}
			return w.Emit("weights", "policy", checkpoint{Step: r.Epoch + 1, URI: res.Checkpoint})
		}

	default:
		log.Fatalf("unknown operation %q", op)
	}
	if err := w.Run(context.Background(), h); err != nil {
		log.Fatal(err)
	}
}
