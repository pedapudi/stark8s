package sdk

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http/httptest"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
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

// --- ticking and externally fed channels ------------------------------------

// tickWorker builds a worker with a tick interval set.
func (h *harness) tickWorker(op, instance string, inbound, outbound []string, every time.Duration) *Worker {
	w := h.worker(op, instance, inbound, outbound)
	w.TickInterval = every
	return w
}

// waitFor blocks until done is closed, and fails the test rather than hanging
// when it is not. Every test below can deadlock if the change under test is
// wrong, so none of them may wait forever.
func waitFor(t *testing.T, done <-chan struct{}, within time.Duration, what string) {
	t.Helper()
	select {
	case <-done:
	case <-time.After(within):
		t.Fatalf("%s did not happen within %s", what, within)
	}
}

// TestExternallyFedChannelDoesNotBlockDrain pins the deadlock. An operation
// that takes one channel from another operation and one from outside the
// workload must finish when the first is done. Nothing seals a channel with no
// producing operation, so counting it would keep the operation open forever:
// OnDrain would never run, the operation would never report done, its outbound
// channels would never be sealed, and any consumer behind a Materialized edge
// would wait for a seal that cannot come.
func TestExternallyFedChannelDoesNotBlockDrain(t *testing.T) {
	h, stop := newHarness(t, []v1alpha1.Channel{
		{Name: "work", From: "gen", To: "poll"},
		// No From: fed from outside through the records API.
		{Name: "config", To: "poll"},
		{Name: "out", From: "poll"},
	})
	defer stop()

	// A configuration record is waiting before the operation starts, so the
	// test also shows that an external channel is still consumed rather than
	// merely ignored.
	if err := h.co.Produce("config", "", []coordinator.Record{{Key: "region", Value: "north"}}); err != nil {
		t.Fatal(err)
	}

	h.run(h.worker("gen", "gen-0", nil, []string{"work"}), Handlers{
		Source: func(ctx context.Context, w *Worker) error {
			for i := 0; i < 3; i++ {
				if err := w.Emit("work", fmt.Sprintf("w%d", i), i); err != nil {
					return err
				}
			}
			return nil
		},
	})

	var mu sync.Mutex
	byChannel := map[string]int{}
	drained := make(chan struct{})
	h.run(h.worker("poll", "poll-0", []string{"config", "work"}, []string{"out"}), Handlers{
		OnRecord: func(ctx context.Context, w *Worker, r Record) error {
			mu.Lock()
			byChannel[r.Channel]++
			mu.Unlock()
			return nil
		},
		OnDrain: func(ctx context.Context, w *Worker) error {
			close(drained)
			return nil
		},
	})

	// The controller's job: seal a completed operation's outbound channels.
	// Nothing ever does this for "config", which is the whole point.
	h.waitComplete("gen")
	if err := h.co.Seal("work"); err != nil {
		t.Fatal(err)
	}

	waitFor(t, drained, 20*time.Second, "OnDrain on an operation with an externally fed inbound channel")

	mu.Lock()
	defer mu.Unlock()
	if byChannel["work"] != 3 {
		t.Errorf("consumed %d records on work, want 3", byChannel["work"])
	}
	if byChannel["config"] != 1 {
		t.Errorf("consumed %d records on config, want the 1 produced from outside", byChannel["config"])
	}
}

// TestExternallyFedChannelDoesNotBlockCompletion is the same rule one layer
// down. The worker deciding it has drained is not enough: the coordinator
// gates completion on every inbound channel being sealed too, and the
// controller seals an operation's outbound channels only once the coordinator
// calls it complete. Both layers have to agree or a Materialized edge
// downstream still waits forever.
func TestExternallyFedChannelDoesNotBlockCompletion(t *testing.T) {
	h, stop := newHarness(t, []v1alpha1.Channel{
		{Name: "work", From: "gen", To: "poll"},
		{Name: "config", To: "poll"},
		{Name: "out", From: "poll"},
	})
	defer stop()

	h.run(h.worker("gen", "gen-0", nil, []string{"work"}), Handlers{
		Source: func(ctx context.Context, w *Worker) error {
			return w.Emit("work", "w0", 1)
		},
	})
	h.run(h.worker("poll", "poll-0", []string{"config", "work"}, []string{"out"}), Handlers{
		OnRecord: func(ctx context.Context, w *Worker, r Record) error { return nil },
	})

	h.waitComplete("gen")
	if err := h.co.Seal("work"); err != nil {
		t.Fatal(err)
	}
	h.waitComplete("poll")
}

// TestTickFiresOnTheInterval checks both halves of the schedule: it fires
// repeatedly, and it does not fire faster than the interval. The operation
// consumes one externally fed channel, which never drains, so the loop keeps
// running for the whole test.
func TestTickFiresOnTheInterval(t *testing.T) {
	h, stop := newHarness(t, []v1alpha1.Channel{
		{Name: "config", To: "poll"},
		{Name: "out", From: "poll"},
	})
	defer stop()

	const every = 20 * time.Millisecond
	const want = 8
	// ceiling separates a tick scheduled against the clock from one that
	// merely rides the idle backoff. The backoff doubles from 100ms to a
	// two-second ceiling, so without capping the sleep at the next tick these
	// gaps would run to hundreds of milliseconds and then to seconds.
	const ceiling = 500 * time.Millisecond

	var mu sync.Mutex
	var at []time.Time
	enough := make(chan struct{})
	h.run(h.tickWorker("poll", "poll-0", []string{"config"}, []string{"out"}, every), Handlers{
		Tick: func(ctx context.Context, w *Worker) error {
			mu.Lock()
			defer mu.Unlock()
			at = append(at, time.Now())
			if len(at) == want {
				close(enough)
			}
			return nil
		},
	})

	waitFor(t, enough, 20*time.Second, fmt.Sprintf("%d ticks at %s", want, every))

	mu.Lock()
	defer mu.Unlock()
	// The interval is a floor. Consecutive ticks are measured from the end of
	// the previous handler, so no gap may be shorter than it. A millisecond of
	// slack absorbs clock granularity.
	for i := 1; i < len(at); i++ {
		gap := at[i].Sub(at[i-1])
		if gap < every-time.Millisecond {
			t.Errorf("tick %d came %s after tick %d, closer than the %s interval", i, gap, i-1, every)
		}
		if gap > ceiling {
			t.Errorf("tick %d came %s after tick %d, far past the %s interval: the idle backoff is outrunning the clock", i, gap, i-1, every)
		}
	}
	t.Logf("%d ticks at a %s interval over %s", len(at), every, at[len(at)-1].Sub(at[0]))
}

// goroutineID returns the calling goroutine's id, read from its own stack.
// Nothing in production should do this. A test may, because the property
// being checked here is exactly which goroutine a handler runs on.
func goroutineID(t *testing.T) uint64 {
	t.Helper()
	var buf [64]byte
	n := runtime.Stack(buf[:], false)
	// The first line reads "goroutine 123 [running]:".
	fields := strings.Fields(string(buf[:n]))
	if len(fields) < 2 {
		t.Fatalf("cannot read a goroutine id from %q", string(buf[:n]))
	}
	id, err := strconv.ParseUint(fields[1], 10, 64)
	if err != nil {
		t.Fatalf("cannot read a goroutine id from %q: %v", string(buf[:n]), err)
	}
	return id
}

// TestTickNeverOverlapsOnRecord is the safety argument for calling Tick from
// the consume loop rather than from a goroutine of its own. The handlers share
// the emit buffers and the worker's epoch field, neither of which is guarded,
// so overlapping them would corrupt both.
//
// The check is goroutine identity, because that is the actual property and it
// holds on every run. Watching for a collision instead would be statistical:
// a Tick moved onto its own goroutine is only spawned between passes over the
// inbound channels, so whether it lands inside a record handler depends on how
// the records happen to be batched, and a run that saw no collision would
// prove nothing. Same goroutine implies no overlap and needs no luck.
//
// Two weaker detectors run alongside it and cost nothing: a flag that catches
// a collision if one does occur, and a counter touched by both handlers with
// no synchronisation, which makes -race report an overlap.
func TestTickNeverOverlapsOnRecord(t *testing.T) {
	h, stop := newHarness(t, []v1alpha1.Channel{
		{Name: "work", From: "gen", To: "poll"},
		{Name: "config", To: "poll"},
		{Name: "out", From: "poll"},
	})
	defer stop()

	const records = 200
	h.run(h.worker("gen", "gen-0", nil, []string{"work"}), Handlers{
		Source: func(ctx context.Context, w *Worker) error {
			for i := 0; i < records; i++ {
				if err := w.Emit("work", fmt.Sprintf("w%d", i), i); err != nil {
					return err
				}
			}
			return nil
		},
	})

	var inHandler atomic.Int32
	var overlaps atomic.Int32
	var ticks atomic.Int32
	var recordGo, tickGo atomic.Uint64
	// unguarded is touched by both handlers with no synchronisation at all.
	// If they ever overlap, -race reports it.
	unguarded := 0
	consumed := 0

	enter := func() func() {
		if !inHandler.CompareAndSwap(0, 1) {
			overlaps.Add(1)
		}
		return func() { inHandler.Store(0) }
	}

	drained := make(chan struct{})
	h.run(h.tickWorker("poll", "poll-0", []string{"config", "work"}, []string{"out"}, time.Millisecond), Handlers{
		OnRecord: func(ctx context.Context, w *Worker, r Record) error {
			defer enter()()
			recordGo.Store(goroutineID(t))
			unguarded++
			consumed++
			// Both handlers dwell, so that a Tick running anywhere else has a
			// wide window to land inside this one. Without the dwell the
			// handlers are so short that an overlapping Tick usually misses
			// them and the flag alone would not see it.
			time.Sleep(200 * time.Microsecond)
			return nil
		},
		Tick: func(ctx context.Context, w *Worker) error {
			defer enter()()
			tickGo.Store(goroutineID(t))
			unguarded++
			ticks.Add(1)
			time.Sleep(200 * time.Microsecond)
			return nil
		},
		OnDrain: func(ctx context.Context, w *Worker) error {
			close(drained)
			return nil
		},
	})

	h.waitComplete("gen")
	if err := h.co.Seal("work"); err != nil {
		t.Fatal(err)
	}
	waitFor(t, drained, 20*time.Second, "OnDrain on a ticking operation")

	if ticks.Load() == 0 {
		t.Fatal("Tick never fired, so the no-overlap result proves nothing")
	}
	if consumed == 0 {
		t.Fatal("no records were consumed, so the no-overlap result proves nothing")
	}
	if r, k := recordGo.Load(), tickGo.Load(); r != k {
		t.Errorf("OnRecord ran on goroutine %d and Tick on goroutine %d; Tick must run on the consume loop's own goroutine, which is what lets the two share the emit buffers and the epoch field without a lock", r, k)
	}
	if n := overlaps.Load(); n != 0 {
		t.Errorf("Tick and OnRecord overlapped %d times", n)
	}
	if consumed != records {
		t.Errorf("consumed %d records, want %d", consumed, records)
	}
	if unguarded != consumed+int(ticks.Load()) {
		t.Errorf("the unguarded counter reads %d, want %d; the two handlers interleaved", unguarded, consumed+int(ticks.Load()))
	}
	t.Logf("%d records and %d ticks on one goroutine, no overlap", consumed, ticks.Load())
}

// TestTickingOperationStillCompletes is the combined case the feature exists
// for: an operation driven by both a channel and a clock, which still finishes
// when its bounded input is done.
func TestTickingOperationStillCompletes(t *testing.T) {
	h, stop := newHarness(t, []v1alpha1.Channel{
		{Name: "work", From: "gen", To: "poll"},
		{Name: "out", From: "poll"},
	})
	defer stop()

	h.run(h.worker("gen", "gen-0", nil, []string{"work"}), Handlers{
		Source: func(ctx context.Context, w *Worker) error {
			for i := 0; i < 20; i++ {
				if err := w.Emit("work", fmt.Sprintf("w%d", i), i); err != nil {
					return err
				}
			}
			return nil
		},
	})

	var ticks atomic.Int32
	h.run(h.tickWorker("poll", "poll-0", []string{"work"}, []string{"out"}, 2*time.Millisecond), Handlers{
		OnRecord: func(ctx context.Context, w *Worker, r Record) error { return nil },
		Tick: func(ctx context.Context, w *Worker) error {
			ticks.Add(1)
			// Emitting from Tick must reach the channel like any other output.
			return w.Emit("out", "tick", ticks.Load())
		},
	})

	h.waitComplete("gen")
	if err := h.co.Seal("work"); err != nil {
		t.Fatal(err)
	}
	h.waitComplete("poll")

	if ticks.Load() == 0 {
		t.Fatal("the operation completed without ticking once")
	}
	recs, _, err := h.co.Records("out", "", 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(recs) != int(ticks.Load()) {
		t.Errorf("out carries %d records for %d ticks; records emitted from Tick are being lost", len(recs), ticks.Load())
	}
}

// TestTickWithoutInboundChannelsIsRejected: an operation with no inbound
// channels runs Source and never reaches the loop that calls Tick, so the
// handler would sit there uncalled. Run says so instead, before it touches
// the network.
func TestTickWithoutInboundChannelsIsRejected(t *testing.T) {
	w := &Worker{Operation: "poll", Instance: "poll-0", TickInterval: time.Second}
	err := w.Run(context.Background(), Handlers{
		Tick: func(ctx context.Context, w *Worker) error { return nil },
	})
	if err == nil {
		t.Fatal("a Tick handler with no inbound channels was accepted")
	}
	if !strings.Contains(err.Error(), "no inbound channels") {
		t.Errorf("error is %q, want it to name the missing inbound channels", err)
	}
}

// TestAllExternalInboundNeverDrains: an operation fed only from outside the
// workload has no bounded input, and nothing can ever say the outside world
// has stopped sending. Draining it would report done before it had processed
// anything and abandon the stream it exists to serve, so it stays in the loop
// instead. This is what lets a poller keep polling.
func TestAllExternalInboundNeverDrains(t *testing.T) {
	h, stop := newHarness(t, []v1alpha1.Channel{
		{Name: "config", To: "poll"},
		{Name: "out", From: "poll"},
	})
	defer stop()

	var ticks atomic.Int32
	drained := make(chan struct{})
	h.run(h.tickWorker("poll", "poll-0", []string{"config"}, []string{"out"}, 5*time.Millisecond), Handlers{
		Tick: func(ctx context.Context, w *Worker) error {
			ticks.Add(1)
			return nil
		},
		OnDrain: func(ctx context.Context, w *Worker) error {
			close(drained)
			return nil
		},
	})

	select {
	case <-drained:
		t.Fatal("an operation fed only from outside the workload reported drained; it would stop serving its stream")
	case <-time.After(300 * time.Millisecond):
	}
	if ticks.Load() == 0 {
		t.Error("the operation neither drained nor ticked, so it is not running at all")
	}
	t.Logf("still running after 300ms with %d ticks and no drain", ticks.Load())
}
