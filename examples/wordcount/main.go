// Command wordcount is a three-operation map/shuffle/reduce workload.
//
//	read  (source)      emits lines of an embedded corpus
//	map                 splits lines into (word, 1) records on a Hash channel
//	reduce              sums counts per word; on drain writes totals to an
//	                    externally readable result channel
//
// The operation is selected by the first argument: read, map or reduce.
package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"strings"

	"github.com/pedapudi/stark8s/pkg/sdk"
)

const corpus = `the quick brown fox jumps over the lazy dog
a directed graph of operations connected by channels
each operation owns an independently scaled pool of pods
records flow over channels and channels are the only edges
the exchange enforces partitioning delivery and sealing
a feedback channel closes a cycle and gives it superstep semantics
the quick brown dog jumps over the lazy fox`

func main() {
	if len(os.Args) < 2 {
		log.Fatal("usage: wordcount read|map|reduce")
	}
	w, err := sdk.FromEnv()
	if err != nil {
		log.Fatal(err)
	}
	ctx := context.Background()
	var h sdk.Handlers
	switch os.Args[1] {
	case "read":
		repeat := 200
		h.Source = func(ctx context.Context, w *sdk.Worker) error {
			n := 0
			for i := 0; i < repeat; i++ {
				for _, line := range strings.Split(corpus, "\n") {
					if err := w.Emit("lines", fmt.Sprint(n), line); err != nil {
						return err
					}
					n++
				}
			}
			log.Printf("emitted %d lines", n)
			return nil
		}
	case "map":
		h.OnRecord = func(ctx context.Context, w *sdk.Worker, r sdk.Record) error {
			var line string
			if err := unmarshal(r, &line); err != nil {
				return err
			}
			for _, word := range strings.Fields(line) {
				if err := w.Emit("shuffle", word, 1); err != nil {
					return err
				}
			}
			return nil
		}
	case "reduce":
		counts := map[string]int{}
		h.OnRecord = func(ctx context.Context, w *sdk.Worker, r sdk.Record) error {
			var n int
			if err := unmarshal(r, &n); err != nil {
				return err
			}
			counts[r.Key] += n
			return nil
		}
		h.OnDrain = func(ctx context.Context, w *sdk.Worker) error {
			for word, n := range counts {
				log.Printf("%s=%d", word, n)
				if err := w.Emit("totals", word, n); err != nil {
					return err
				}
			}
			return nil
		}
	default:
		log.Fatalf("unknown operation %q", os.Args[1])
	}
	if err := w.Run(ctx, h); err != nil {
		log.Fatal(err)
	}
}

func unmarshal(r sdk.Record, v any) error {
	return jsonUnmarshal(r.Value, v)
}
