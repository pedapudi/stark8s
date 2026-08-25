// Command pagerank runs PageRank as a cyclic workload.
//
//	seed   (source)  emits one record per vertex: its out-edges, epoch 0
//	rank             holds vertex state, keyed by vertex on a Hash channel.
//	                 Each epoch it receives contributions from neighbours on
//	                 the feedback channel, recomputes ranks at the epoch
//	                 barrier, and emits next-epoch contributions back into
//	                 the feedback channel. After the loop bound it writes
//	                 final ranks to an external result channel.
//
// The rank operation consumes two channels: "graph" (seed → rank, sealed
// after seeding) and "contrib" (rank → rank, feedback). Because contrib is
// Hash-partitioned by vertex, every replica sees all contributions for the
// vertices it owns, so the algorithm is correct with any replica count.
package main

import (
	"context"
	"encoding/json"
	"log"
	"os"

	"github.com/pedapudi/stark8s/pkg/sdk"
)

const damping = 0.85

// edges is a small fixed graph.
var edges = map[string][]string{
	"a": {"b", "c"},
	"b": {"c"},
	"c": {"a"},
	"d": {"c", "a"},
	"e": {"d", "b"},
}

type vertex struct {
	Out  []string
	Rank float64
	// incoming accumulates contributions for the epoch in progress.
	incoming float64
}

func main() {
	if len(os.Args) < 2 {
		log.Fatal("usage: pagerank seed|rank")
	}
	w, err := sdk.FromEnv()
	if err != nil {
		log.Fatal(err)
	}
	var h sdk.Handlers
	switch os.Args[1] {
	case "seed":
		h.Source = func(ctx context.Context, w *sdk.Worker) error {
			for v, out := range edges {
				if err := w.Emit("graph", v, out); err != nil {
					return err
				}
			}
			return nil
		}
	case "rank":
		state := map[string]*vertex{}
		n := float64(len(edges))
		h.OnRecord = func(ctx context.Context, w *sdk.Worker, r sdk.Record) error {
			switch r.Channel {
			case "graph":
				var out []string
				if err := json.Unmarshal(r.Value, &out); err != nil {
					return err
				}
				state[r.Key] = &vertex{Out: out, Rank: 1 / n}
			case "contrib":
				var c float64
				if err := json.Unmarshal(r.Value, &c); err != nil {
					return err
				}
				if v, ok := state[r.Key]; ok {
					v.incoming += c
				}
			}
			return nil
		}
		h.OnEpochEnd = func(ctx context.Context, w *sdk.Worker, epoch int32) error {
			for name, v := range state {
				if epoch > 0 {
					v.Rank = (1-damping)/n + damping*v.incoming
					v.incoming = 0
				}
				share := v.Rank / float64(len(v.Out))
				for _, dst := range v.Out {
					if err := w.Emit("contrib", dst, share); err != nil {
						return err
					}
				}
				log.Printf("epoch %d %s rank=%.4f", epoch, name, v.Rank)
			}
			return nil
		}
		h.OnDrain = func(ctx context.Context, w *sdk.Worker) error {
			for name, v := range state {
				if err := w.Emit("ranks", name, v.Rank); err != nil {
					return err
				}
			}
			return nil
		}
	default:
		log.Fatalf("unknown operation %q", os.Args[1])
	}
	if err := w.Run(context.Background(), h); err != nil {
		log.Fatal(err)
	}
}
