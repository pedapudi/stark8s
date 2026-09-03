// Command grpo-gemma runs GRPO over a real language model as a Workload.
//
// The graph is examples/grpo exactly; what changed is inside two of the pods.
// `rollout` and `learner` each pair the SDK worker with a sidecar holding the
// model, behind a localhost HTTP contract. `reward` needs neither an
// accelerator nor a sidecar, because scoring a tour is arithmetic — which is
// the whole reason it is a separate operation with its own replica count.
//
// The weights channel carries a checkpoint URI, not the weights: the model is
// gigabytes and Emit marshals a value and buffers it whole on both sides.
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"os"
	"sort"
	"strings"
	"time"

	"github.com/pedapudi/stark8s/pkg/sdk"
)

const cities = 8

type checkpoint struct {
	Step int32  `json:"step"`
	URI  string `json:"uri"`
}

type completion struct {
	Slot   string  `json:"slot"`
	Index  int     `json:"index"`
	Text   string  `json:"text"`
	Reward float64 `json:"reward,omitempty"`
}

type sample struct {
	Prompt     string  `json:"prompt"`
	Completion string  `json:"completion"`
	Advantage  float64 `json:"advantage"`
}

type group struct {
	Slot    string   `json:"slot"`
	Samples []sample `json:"samples"`
}

type metric struct {
	Step       int32   `json:"step"`
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

// awaitSidecar blocks until the model process accepts a connection. The
// sidecar binds its port only after the weights are on the accelerator, so a
// successful dial is an exact readiness signal.
//
// The wait is not cosmetic. A worker that starts consuming before the sidecar
// answers takes a segment, fails the request, and exits; the container
// restarts inside a pod that is still alive, so the coordinator — which
// returns in-flight segments to the pending set when a pod is *gone* — has no
// reason to redeliver them. The records are lost to a graph that otherwise
// looks healthy. Wait first.
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

func main() {
	fs := flag.NewFlagSet("grpo-gemma", flag.ExitOnError)
	g := fs.Int("group", 8, "completions per prompt")
	slots := fs.Int("slots", 4, "prompts per step")
	side := fs.String("sidecar", "http://127.0.0.1:8100", "model sidecar")
	if len(os.Args) < 2 {
		log.Fatal("usage: grpo-gemma [flags] prompts|rollout|reward|advantage|learner")
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
			for k := 0; k < *slots; k++ {
				if err := w.Emit("batch", fmt.Sprintf("slot-%d", k), k); err != nil {
					return err
				}
			}
			return nil
		}

	case "rollout":
		owned := map[string]bool{}
		draw := func(w *sdk.Worker, slot string) error {
			cs := instance(slot, w.Epoch(), cities)
			var out struct {
				Completions []string `json:"completions"`
			}
			// The prompt is built here and only here. The learner scores
			// completions against this text, so a second copy in the sidecar
			// would be a drift the graph could not detect.
			if err := post(*side+"/generate", map[string]any{
				"prompt": prompt(cs), "n": *g, "max_new_tokens": 64,
			}, &out); err != nil {
				return err
			}
			for i, t := range out.Completions {
				if err := w.Emit("completions", slot,
					completion{Slot: slot, Index: i, Text: t}); err != nil {
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
				for _, k := range keys {
					if err := draw(w, k); err != nil {
						return err
					}
				}
			}
			return nil
		}

	case "reward":
		// No accelerator, no sidecar, no model: scoring a tour is arithmetic.
		h.OnRecord = func(ctx context.Context, w *sdk.Worker, r sdk.Record) error {
			var c completion
			if err := json.Unmarshal(r.Value, &c); err != nil {
				return err
			}
			c.Reward = reward(instance(c.Slot, r.Epoch, cities), c.Text)
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
			p := prompt(instance(c.Slot, r.Epoch, cities))
			gr := group{Slot: c.Slot, Samples: make([]sample, len(cs))}
			for i, c := range cs {
				gr.Samples[i] = sample{Prompt: p, Completion: c.Text, Advantage: adv[i]}
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
			if len(open[r.Epoch]) < *slots {
				return nil // still waiting on other prompts
			}
			groups := open[r.Epoch]
			delete(open, r.Epoch)
			var batch []sample
			mean, n, deg := 0.0, 0.0, 0
			for _, gg := range groups {
				flat := true
				for _, s := range gg.Samples {
					if s.Advantage != 0 {
						flat = false
					}
					if s.Advantage != 0 {
						batch = append(batch, s)
					}
					mean += reward(instance(gg.Slot, r.Epoch, cities), s.Completion)
					n++
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
			m := metric{Step: r.Epoch, RewardMean: mean / n, Degenerate: deg,
				Checkpoint: res.Checkpoint}
			log.Printf("step %d reward=%.3f degenerate=%d/%d ckpt=%s",
				m.Step, m.RewardMean, deg, len(groups), res.Checkpoint)
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
