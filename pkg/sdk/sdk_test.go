package sdk

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/pedapudi/stark8s/api/v1alpha1"
	"github.com/pedapudi/stark8s/pkg/coordinator"
)

// harness runs a coordinator in-process and builds workers against it.
type harness struct {
	t   *testing.T
	co  *coordinator.Coordinator
	srv *httptest.Server
	ctx context.Context
	wg  sync.WaitGroup
}

func newHarness(t *testing.T, specs []v1alpha1.Channel) (*harness, context.CancelFunc) {
	t.Helper()
	seg := httptest.NewServer(nil)
	co := coordinator.New(strings.TrimPrefix(seg.URL, "http://"))
	seg.Config.Handler = coordinator.SegmentHandler(co)
	co.Configure(specs)
	srv := httptest.NewServer(coordinator.Handler(co))
	ctx, cancel := context.WithCancel(context.Background())
	h := &harness{t: t, co: co, srv: srv, ctx: ctx}
	return h, func() {
		cancel()
		h.wg.Wait()
		srv.Close()
		seg.Close()
	}
}

func (h *harness) worker(op, instance string, inbound, outbound []string) *Worker {
	return &Worker{
		Coordinator:   h.srv.URL,
		Operation:     op,
		Instance:      instance,
		Inbound:       inbound,
		Outbound:      outbound,
		SegmentDir:    h.t.TempDir(),
		SegmentListen: "127.0.0.1:0",
	}
}

// run starts the worker in the background; a handler error fails the test.
func (h *harness) run(w *Worker, hs Handlers) {
	h.wg.Add(1)
	go func() {
		defer h.wg.Done()
		if err := w.Run(h.ctx, hs); err != nil && h.ctx.Err() == nil {
			h.t.Errorf("%s: %v", w.Instance, err)
		}
	}()
}

func (h *harness) waitComplete(op string) {
	h.t.Helper()
	deadline := time.Now().Add(20 * time.Second)
	for time.Now().Before(deadline) {
		for _, om := range h.co.Metrics().Operations {
			if om.Name == op && om.Complete {
				return
			}
		}
		time.Sleep(50 * time.Millisecond)
	}
	h.t.Fatalf("operation %s did not complete: %+v", op, h.co.Metrics())
}

func TestWorkersExchangeSegmentsDirectly(t *testing.T) {
	h, stop := newHarness(t, []v1alpha1.Channel{
		{Name: "words", From: "read", To: "count", Partitioning: v1alpha1.Partitioning{Mode: v1alpha1.PartitionHash, Partitions: 4}},
		{Name: "totals", From: "count"},
	})
	defer stop()

	const lines = 300
	h.run(h.worker("read", "read-0", nil, []string{"words"}), Handlers{
		Source: func(ctx context.Context, w *Worker) error {
			for i := 0; i < lines; i++ {
				if err := w.Emit("words", fmt.Sprintf("w%d", i%7), 1); err != nil {
					return err
				}
			}
			return nil
		},
	})
	var mu sync.Mutex
	seen := map[string]string{}
	for _, inst := range []string{"count-0", "count-1"} {
		inst := inst
		counts := map[string]int{}
		h.run(h.worker("count", inst, []string{"words"}, []string{"totals"}), Handlers{
			OnRecord: func(ctx context.Context, w *Worker, r Record) error {
				var n int
				if err := json.Unmarshal(r.Value, &n); err != nil {
					return err
				}
				counts[r.Key] += n
				mu.Lock()
				defer mu.Unlock()
				if prev, ok := seen[r.Key]; ok && prev != inst {
					return fmt.Errorf("key %s reached both %s and %s", r.Key, prev, inst)
				}
				seen[r.Key] = inst
				return nil
			},
			OnDrain: func(ctx context.Context, w *Worker) error {
				for k, n := range counts {
					if err := w.Emit("totals", k, n); err != nil {
						return err
					}
				}
				return nil
			},
		})
	}
	// The controller's role: seal the source's output once it is complete.
	h.waitComplete("read")
	if err := h.co.Seal("words"); err != nil {
		t.Fatal(err)
	}
	h.waitComplete("count")

	recs, _, err := h.co.Records("totals", "", 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	total := 0
	for _, r := range recs {
		total += int(r.Value.(float64))
	}
	if len(recs) != 7 || total != lines {
		t.Fatalf("totals: %d records summing to %d: %+v", len(recs), total, recs)
	}
	for _, om := range h.co.Metrics().Operations {
		if om.Name == "read" && om.HoldsUnconsumed {
			t.Fatalf("source still holds acknowledged segments: %+v", om)
		}
	}
	for _, cm := range h.co.Metrics().Channels {
		if cm.Name == "words" && (cm.Produced != lines || cm.Pending != 0 || cm.InFlight != 0 || cm.Lost != 0) {
			t.Fatalf("words: %+v", cm)
		}
	}
}

func TestSynchronousLoopRunsSupersteps(t *testing.T) {
	h, stop := newHarness(t, []v1alpha1.Channel{
		{Name: "graph", From: "seed", To: "rank", Partitioning: v1alpha1.Partitioning{Mode: v1alpha1.PartitionHash, Partitions: 2}},
		{Name: "contrib", From: "rank", To: "rank",
			Partitioning: v1alpha1.Partitioning{Mode: v1alpha1.PartitionHash, Partitions: 2},
			Feedback:     &v1alpha1.Feedback{Mode: v1alpha1.FeedbackSynchronous, MaxEpochs: 4}},
		{Name: "ranks", From: "rank"},
	})
	defer stop()

	h.run(h.worker("seed", "seed-0", nil, []string{"graph"}), Handlers{
		Source: func(ctx context.Context, w *Worker) error {
			for _, v := range []string{"a", "b", "c"} {
				if err := w.Emit("graph", v, 1.0); err != nil {
					return err
				}
			}
			return nil
		},
	})
	var mu sync.Mutex
	epochs := map[string][]int32{}
	received := map[string]int{}
	for _, inst := range []string{"rank-0", "rank-1"} {
		inst := inst
		w := h.worker("rank", inst, []string{"graph", "contrib"}, []string{"contrib", "ranks"})
		w.SetFeedback([]string{"contrib"}, []string{"contrib"})
		state := map[string]float64{}
		h.run(w, Handlers{
			OnRecord: func(ctx context.Context, w *Worker, r Record) error {
				var v float64
				if err := json.Unmarshal(r.Value, &v); err != nil {
					return err
				}
				if r.Channel == "graph" {
					state[r.Key] = v
				} else {
					state[r.Key] += v
					mu.Lock()
					received[r.Key]++
					mu.Unlock()
				}
				return nil
			},
			OnEpochEnd: func(ctx context.Context, w *Worker, epoch int32) error {
				mu.Lock()
				epochs[inst] = append(epochs[inst], epoch)
				mu.Unlock()
				for k := range state {
					if err := w.Emit("contrib", k, 1.0); err != nil {
						return err
					}
				}
				return nil
			},
			OnDrain: func(ctx context.Context, w *Worker) error {
				for k, v := range state {
					if err := w.Emit("ranks", k, v); err != nil {
						return err
					}
				}
				return nil
			},
		})
	}
	h.waitComplete("seed")
	_ = h.co.Seal("graph")
	h.waitComplete("rank")

	mu.Lock()
	defer mu.Unlock()
	for inst, es := range epochs {
		if fmt.Sprint(es) != "[0 1 2 3]" {
			t.Fatalf("%s ran epochs %v, want [0 1 2 3]", inst, es)
		}
	}
	recs, _, _ := h.co.Records("ranks", "", 0, 0)
	if len(recs) != 3 {
		t.Fatalf("ranks: %+v", recs)
	}
	// Each vertex receives one contribution per epoch after the first
	// (epochs 1..3); contributions emitted at epoch 3 fall beyond the bound.
	for _, r := range recs {
		if r.Value.(float64) != 4 || received[r.Key] != 3 {
			t.Fatalf("vertex %s: value %v received %d", r.Key, r.Value, received[r.Key])
		}
	}
}

func TestAsynchronousLoopDivertsAtBound(t *testing.T) {
	h, stop := newHarness(t, []v1alpha1.Channel{
		{Name: "prompts", To: "agent", Partitioning: v1alpha1.Partitioning{Mode: v1alpha1.PartitionHash, Partitions: 2}},
		{Name: "turns", From: "agent", To: "agent",
			Partitioning: v1alpha1.Partitioning{Mode: v1alpha1.PartitionHash, Partitions: 2},
			Feedback:     &v1alpha1.Feedback{Mode: v1alpha1.FeedbackAsynchronous, MaxEpochs: 3, Overflow: "unfinished"}},
		{Name: "unfinished", From: "agent"},
		{Name: "answers", From: "agent"},
	})
	defer stop()

	w := h.worker("agent", "agent-0", []string{"prompts", "turns"}, []string{"turns", "unfinished", "answers"})
	w.SetFeedback([]string{"turns"}, []string{"turns"})
	var mu sync.Mutex
	epochsSeen := map[string][]int32{}
	h.run(w, Handlers{
		OnRecord: func(ctx context.Context, w *Worker, r Record) error {
			mu.Lock()
			epochsSeen[r.Key] = append(epochsSeen[r.Key], r.Epoch)
			mu.Unlock()
			if r.Key == "short" && r.Epoch == 1 {
				return w.Emit("answers", r.Key, "done")
			}
			// Every other conversation keeps looping until the bound.
			return w.Emit("turns", r.Key, r.Epoch+1)
		},
	})
	// External producer: two conversations.
	if err := h.co.Produce("prompts", "", []coordinator.Record{{Key: "short", Value: 0}, {Key: "long", Value: 0}}); err != nil {
		t.Fatal(err)
	}
	_ = h.co.Seal("prompts")
	h.waitComplete("agent")

	mu.Lock()
	defer mu.Unlock()
	if fmt.Sprint(epochsSeen["short"]) != "[0 1]" || fmt.Sprint(epochsSeen["long"]) != "[0 1 2]" {
		t.Fatalf("epochs seen: %v", epochsSeen)
	}
	answers, _, _ := h.co.Records("answers", "short", 0, 0)
	if len(answers) != 1 {
		t.Fatalf("answers: %+v", answers)
	}
	spilled, _, _ := h.co.Records("unfinished", "", 0, 0)
	if len(spilled) != 1 || spilled[0].Key != "long" || spilled[0].Epoch != 3 {
		t.Fatalf("overflow: %+v", spilled)
	}
	for _, cm := range h.co.Metrics().Channels {
		if cm.Name == "turns" && (!cm.Sealed || cm.Produced != 3) {
			t.Fatalf("turns: %+v", cm)
		}
	}
}

func TestSegmentDirFallsBackWhenNotWritable(t *testing.T) {
	s, err := openStore("/proc/stark8s-not-writable", "pod-x")
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(s.dir, "stark8s-segments-pod-x") {
		t.Fatalf("fallback dir: %s", s.dir)
	}
	id, n, err := s.write("pod-x", []wireRecord{{Key: "k", Value: json.RawMessage(`1`)}})
	if err != nil || n == 0 || id == "" {
		t.Fatalf("write: %s %d %v", id, n, err)
	}
	s.remove(id)
}

func TestSynchronousLoopDoesNotStallOnIdlePods(t *testing.T) {
	const supersteps = 8
	h, stop := newHarness(t, []v1alpha1.Channel{
		{Name: "graph", From: "seed", To: "rank", Partitioning: v1alpha1.Partitioning{Mode: v1alpha1.PartitionHash, Partitions: 4}},
		{Name: "contrib", From: "rank", To: "rank",
			Partitioning: v1alpha1.Partitioning{Mode: v1alpha1.PartitionHash, Partitions: 4},
			Feedback:     &v1alpha1.Feedback{Mode: v1alpha1.FeedbackSynchronous, MaxEpochs: supersteps}},
	})
	defer stop()

	h.run(h.worker("seed", "seed-0", nil, []string{"graph"}), Handlers{
		Source: func(ctx context.Context, w *Worker) error { return w.Emit("graph", "only", 1.0) },
	})
	// Two pods, one key: the key lands in a single partition, so one of the
	// two pods never receives a record and spends the whole loop waiting at
	// the barrier. The barrier cannot advance without it, so its poll rate
	// sets the pace of every superstep.
	for _, inst := range []string{"rank-0", "rank-1"} {
		w := h.worker("rank", inst, []string{"graph", "contrib"}, []string{"contrib"})
		w.SetFeedback([]string{"contrib"}, []string{"contrib"})
		keys := map[string]bool{}
		h.run(w, Handlers{
			OnRecord: func(ctx context.Context, w *Worker, r Record) error {
				keys[r.Key] = true
				return nil
			},
			OnEpochEnd: func(ctx context.Context, w *Worker, epoch int32) error {
				for k := range keys {
					if err := w.Emit("contrib", k, 1.0); err != nil {
						return err
					}
				}
				return nil
			},
		})
	}
	h.waitComplete("seed")
	if err := h.co.Seal("graph"); err != nil {
		t.Fatal(err)
	}
	start := time.Now()
	h.waitComplete("rank")
	elapsed := time.Since(start)
	// Comfortably above the ~300ms a superstep costs when the idle pod wakes
	// promptly, and comfortably below the seconds it costs when it does not.
	if budget := supersteps * time.Second; elapsed > budget {
		t.Fatalf("%d supersteps of no real work took %v, over the %v budget: a pod idle at the barrier is sleeping through its release", supersteps, elapsed, budget)
	}
}

func TestUnfetchableSegmentFailsInsteadOfHanging(t *testing.T) {
	h, stop := newHarness(t, []v1alpha1.Channel{
		{Name: "s", From: "produce", To: "consume", Partitioning: v1alpha1.Partitioning{Mode: v1alpha1.PartitionRoundRobin, Partitions: 1}},
	})
	defer stop()

	// A segment announced by a pod that is no longer serving it. The
	// coordinator still queues it, and nothing can ever fetch it.
	if err := h.co.Announce("s", "produce", []coordinator.SegmentAnnouncement{{
		ID: "seg-gone", Channel: "s", Records: 1, Bytes: 2,
		Holder: "127.0.0.1:1", Producer: "produce-0",
	}}); err != nil {
		t.Fatal(err)
	}
	errc := make(chan error, 1)
	w := h.worker("consume", "consume-0", []string{"s"}, nil)
	go func() { errc <- w.Run(h.ctx, Handlers{}) }()
	select {
	case err := <-errc:
		if err == nil || !strings.Contains(err.Error(), "seg-gone") {
			t.Fatalf("Run returned %v, want an error naming the segment it could not fetch", err)
		}
	case <-time.After(30 * time.Second):
		t.Fatal("Run never returned: a segment that cannot be fetched hangs the worker instead of failing it")
	}
}
