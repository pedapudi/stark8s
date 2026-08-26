// Command paramserver runs the canonical parameter-server program — one
// stateful server holding the weights, many workers computing gradients
// against their own shard — as a Workload with no driver loop.
//
//	data     (source) emits one record per shard: that shard's points.
//	worker   owns whichever shards hash to it. On a shard it registers with
//	         the server; on a weight vector it computes that shard's
//	         gradient and sends it back.
//	server   exactly one replica. It holds the weights, applies the
//	         gradients of a round once every shard has reported, broadcasts
//	         the new weights, and writes each round to "checkpoints".
//
// The loop is closed by "weights", a Broadcast feedback channel from server
// to worker: every worker needs the same vector, and the record's epoch is
// the round number. "gradients" runs back the other way with one hash
// partition, so every gradient reaches the single server replica.
//
// The feedback mode is Asynchronous, which carries the round per record and
// imposes no barrier. The round barrier is the server's own rule — it holds a
// round open until every shard has reported — because the engine's
// Synchronous barrier cannot serve a loop whose producer and consumer are
// different operations. docs/ray-mapping.md says why, and main_test.go pins
// the behaviour down.
//
// Rounds are counted by the record epoch, and round 0 is registration: a
// worker emits an empty gradient for each shard it has just received, and
// the server replies with the initial weights once all of them have
// arrived. That is what orders the graph — the first weight vector cannot
// be broadcast until every shard has reached a worker — so no worker can
// ever be asked for a gradient over data it does not yet hold.
//
// The problem is a two-parameter least-squares fit on a fixed synthetic
// dataset, trained by plain gradient descent at a fixed learning rate.
// Nothing here is random, so the weights after a given number of rounds are
// the same on every run and main_test.go checks them against the closed
// form.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"sort"

	"github.com/pedapudi/stark8s/pkg/sdk"
)

// params is the size of the model: y ~= w[0] + w[1]*x.
const params = 2

// samples is the size of the training set.
const samples = 40

// point returns training point i. The line is y = 1 + 3x with a fixed
// wobble that repeats every four points and sums to zero over them, so with
// x symmetric about zero the least-squares intercept is exactly 1 and only
// the slope has to be solved for.
func point(i int) (x, y float64) {
	x = -1 + 2*float64(i)/float64(samples-1)
	y = 1 + 3*x + 0.25*(float64(i%4)-1.5)
	return x, y
}

// shardID names a shard. It is the record key of the shard on "shards" and
// of that shard's gradient on "gradients".
func shardID(s int) string { return fmt.Sprintf("shard-%d", s) }

// Shard is one worker's slice of the training set.
type Shard struct {
	X []float64 `json:"x"`
	Y []float64 `json:"y"`
}

// Weights is the model as the server broadcasts it.
type Weights struct {
	// Round is the training round these weights open. Round 0 does not
	// exist on this channel: the first broadcast is round 1.
	Round int32     `json:"round"`
	W     []float64 `json:"w"`
}

// Gradient is one shard's contribution to one round. It carries unnormalised
// sums and the number of points behind them so the server can average over
// the whole training set without knowing how it was split.
type Gradient struct {
	Round int32 `json:"round"`
	// Sum is the sum over the shard's points of the residual times the
	// feature, one entry per parameter. It is nil in round 0.
	Sum []float64 `json:"sum,omitempty"`
	N   int       `json:"n"`
}

func main() {
	shards := flag.Int("shards", 4, "number of data shards; the server waits for one gradient from each")
	rate := flag.Float64("rate", 1.4, "learning rate")
	flag.Parse()
	if flag.NArg() < 1 {
		log.Fatal("usage: paramserver [flags] data|server|worker")
	}
	w, err := sdk.FromEnv()
	if err != nil {
		log.Fatal(err)
	}
	var h sdk.Handlers
	switch flag.Arg(0) {
	case "data":
		h.Source = sourceShards(*shards)
	case "server":
		s := newServer(*shards, *rate)
		h.OnRecord, h.OnDrain = s.onGradient, s.onDrain
	case "worker":
		h.OnRecord = newWorker().onRecord
	default:
		log.Fatalf("unknown operation %q", flag.Arg(0))
	}
	if err := w.Run(context.Background(), h); err != nil {
		log.Fatal(err)
	}
}

// --- data -------------------------------------------------------------------

// sourceShards emits the training set as n shards, point i going to shard
// i%n. The channel is Hash-partitioned on the shard key, so which worker
// replica ends up with which shard is the coordinator's decision, not this
// operation's.
func sourceShards(n int) func(context.Context, *sdk.Worker) error {
	return func(ctx context.Context, w *sdk.Worker) error {
		parts := make([]Shard, n)
		for i := 0; i < samples; i++ {
			x, y := point(i)
			s := &parts[i%n]
			s.X = append(s.X, x)
			s.Y = append(s.Y, y)
		}
		for s := range parts {
			if err := w.Emit("shards", shardID(s), parts[s]); err != nil {
				return err
			}
			log.Printf("shard %s: %d points", shardID(s), len(parts[s].X))
		}
		return nil
	}
}

// --- server -----------------------------------------------------------------

// server is the stateful actor: one replica owns the weights.
type server struct {
	shards int
	rate   float64
	w      []float64
	// last is the last round the server completed.
	last int32
	// round holds the gradients of the rounds in progress, keyed by round
	// and then by shard. Keying by shard makes the round idempotent under
	// redelivery and lets the sum be taken in a fixed order.
	round map[int32]map[string]Gradient
}

func newServer(shards int, rate float64) *server {
	return &server{
		shards: shards,
		rate:   rate,
		w:      make([]float64, params),
		round:  map[int32]map[string]Gradient{},
	}
}

// onGradient collects one shard's gradient. A round is complete when every
// shard has reported it; the server then applies it and broadcasts the next
// weight vector, which is the only thing that lets the workers proceed. The
// engine stamps that broadcast with the next epoch because "weights" is an
// outbound feedback channel, so the round number and the epoch are the same
// number throughout.
func (s *server) onGradient(ctx context.Context, w *sdk.Worker, r sdk.Record) error {
	var g Gradient
	if err := json.Unmarshal(r.Value, &g); err != nil {
		return err
	}
	round := r.Epoch
	in, ok := s.round[round]
	if !ok {
		in = map[string]Gradient{}
		s.round[round] = in
	}
	in[r.Key] = g
	if len(in) < s.shards {
		return nil
	}
	delete(s.round, round)
	if round > 0 {
		s.apply(in)
	}
	s.last = round
	log.Printf("round %d complete: w=%v", round, s.w)
	if err := w.Emit("checkpoints", fmt.Sprintf("round-%d", round), Weights{Round: round, W: append([]float64(nil), s.w...)}); err != nil {
		return err
	}
	// Emitting on the feedback channel stamps the record with round+1. Past
	// the loop bound the engine drops it and counts it as overflow, which is
	// how the loop ends; the weights themselves are already on "checkpoints".
	return w.Emit("weights", "w", Weights{Round: round + 1, W: append([]float64(nil), s.w...)})
}

// apply takes one gradient-descent step on the mean squared error over the
// whole training set. The shard keys are sorted first so the sum is formed
// in the same order however the gradients arrived.
func (s *server) apply(in map[string]Gradient) {
	keys := make([]string, 0, len(in))
	for k := range in {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	sum := make([]float64, params)
	n := 0
	for _, k := range keys {
		g := in[k]
		n += g.N
		for j := 0; j < params && j < len(g.Sum); j++ {
			sum[j] += g.Sum[j]
		}
	}
	if n == 0 {
		return
	}
	for j := range s.w {
		s.w[j] -= s.rate * sum[j] / float64(n)
	}
}

// onDrain writes the trained model once the gradients channel is sealed.
func (s *server) onDrain(ctx context.Context, w *sdk.Worker) error {
	log.Printf("trained after %d rounds: w=%v", s.last, s.w)
	return w.Emit("checkpoints", "final", Weights{Round: s.last, W: s.w})
}

// --- worker -----------------------------------------------------------------

// worker owns the shards that hash to it. It keeps no model state; the
// weights it works from arrive with every round.
type worker struct {
	shards map[string]Shard
}

func newWorker() *worker { return &worker{shards: map[string]Shard{}} }

func (wk *worker) onRecord(ctx context.Context, w *sdk.Worker, r sdk.Record) error {
	switch r.Channel {
	case "shards":
		var s Shard
		if err := json.Unmarshal(r.Value, &s); err != nil {
			return err
		}
		wk.shards[r.Key] = s
		log.Printf("%s: took %s (%d points)", w.Instance, r.Key, len(s.X))
		// Register the shard with the server. Round 0 carries no gradient;
		// it only tells the server this shard exists and how big it is.
		return w.Emit("gradients", r.Key, Gradient{Round: 0, N: len(s.X)})
	case "weights":
		var wts Weights
		if err := json.Unmarshal(r.Value, &wts); err != nil {
			return err
		}
		keys := make([]string, 0, len(wk.shards))
		for k := range wk.shards {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		for _, k := range keys {
			if err := w.Emit("gradients", k, gradient(wts.W, wk.shards[k], r.Epoch)); err != nil {
				return err
			}
		}
		log.Printf("%s: round %d over %d shards, w=%v", w.Instance, r.Epoch, len(keys), wts.W)
		return nil
	default:
		return fmt.Errorf("unexpected channel %s", r.Channel)
	}
}

// gradient is the sum over the shard of the residual times each feature.
// Dividing by the point count is left to the server, which is the only place
// that knows the size of the whole training set.
func gradient(w []float64, s Shard, round int32) Gradient {
	g := Gradient{Round: round, Sum: make([]float64, params), N: len(s.X)}
	for i, x := range s.X {
		residual := w[0] + w[1]*x - s.Y[i]
		g.Sum[0] += residual
		g.Sum[1] += residual * x
	}
	return g
}
