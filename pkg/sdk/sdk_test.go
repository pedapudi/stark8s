package sdk

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/pedapudi/stark8s/api/graph"
	"github.com/pedapudi/stark8s/pkg/coordinator"
	"github.com/pedapudi/stark8s/pkg/storage"
)

// harness runs a coordinator in-process and builds workers against it.
type harness struct {
	t   *testing.T
	co  *coordinator.Coordinator
	srv *httptest.Server
	ctx context.Context
	wg  sync.WaitGroup
}

func TestDurableSegmentSurvivesProducerAndCoordinatorReplacement(t *testing.T) {
	ctx := context.Background()
	blobs, err := storage.NewLocal(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	blobServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		obj, err := blobs.Get(r.Context(), strings.TrimPrefix(r.URL.Path, "/"))
		if err != nil {
			http.NotFound(w, r)
			return
		}
		_, _ = w.Write(obj.Bytes)
	}))
	defer blobServer.Close()
	metadata, err := storage.NewLocal(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	co, err := coordinator.NewDurable(ctx, "coordinator:8090", metadata, "state", "writer-1")
	if err != nil {
		t.Fatal(err)
	}
	if err := co.Configure([]graph.Channel{{Name: "data", From: "source", To: "sink", Durability: graph.DurabilityRetained, Partitioning: graph.Partitioning{Partitions: 1}}}); err != nil {
		t.Fatal(err)
	}
	control := httptest.NewServer(coordinator.Handler(co))
	producer := &Worker{Coordinator: control.URL, Operation: "source", Instance: "source-0", Outbound: []string{"data"}, DurableSegments: blobs, SegmentBaseURL: blobServer.URL, SegmentPrefix: "workload"}
	producer.init()
	if err := co.Register(producer.registration()); err != nil {
		t.Fatal(err)
	}
	if err := producer.Emit("data", "key", map[string]int{"value": 7}); err != nil {
		t.Fatal(err)
	}
	if err := producer.Flush(); err != nil {
		t.Fatal(err)
	}
	control.Close()
	restored, err := coordinator.NewDurable(ctx, "replacement:8090", metadata, "state", "writer-2")
	if err != nil {
		t.Fatal(err)
	}
	if err := restored.Register(coordinator.PodRegistration{Operation: "sink", Pod: "sink-0", Slots: 1}); err != nil {
		t.Fatal(err)
	}
	work, err := restored.Consume("data", "sink", "sink-0", 1)
	if err != nil {
		t.Fatal(err)
	}
	if len(work.Work) != 1 || len(work.Work[0].Segments) != 1 {
		t.Fatalf("restored work: %+v", work)
	}
	consumer := &Worker{DurableSegments: blobs, SegmentBaseURL: blobServer.URL, SegmentPrefix: "workload"}
	consumer.init()
	records, err := consumer.fetch(ctx, work.Work[0].Segments[0])
	if err != nil || len(records) != 1 || records[0].Key != "key" {
		t.Fatalf("records=%+v err=%v", records, err)
	}
}

func TestOnlyRankZeroCanEmitGraphRecords(t *testing.T) {
	w := &Worker{CollectiveSize: 2, CollectiveRank: 1}
	if err := w.Emit("output", "key", 1); err == nil {
		t.Fatal("nonzero rank emitted a graph record")
	}
	if !(&Worker{CollectiveSize: 2, CollectiveRank: 0}).CollectiveIOEnabled() {
		t.Fatal("rank zero cannot use graph I/O")
	}
}

func TestCollectiveRanksExitAndOnlyRankZeroRegisters(t *testing.T) {
	h, stop := newHarness(t, nil)
	defer stop()
	rankOne := h.worker("train", "train-1", nil, nil)
	rankOne.CollectiveSize, rankOne.CollectiveRank = 2, 1
	if err := rankOne.Run(context.Background(), Handlers{Source: func(context.Context, *Worker) error { return nil }}); err != nil {
		t.Fatal(err)
	}
	for _, operation := range h.co.Metrics().Operations {
		if operation.Name == "train" && operation.LivePods != 0 {
			t.Fatalf("nonzero rank registered as graph worker: %+v", operation)
		}
	}

	rankZero := h.worker("train", "train-0", nil, nil)
	rankZero.CollectiveSize, rankZero.CollectiveRank = 2, 0
	if err := rankZero.Run(context.Background(), Handlers{Source: func(context.Context, *Worker) error { return nil }}); err != nil {
		t.Fatal(err)
	}
	for _, operation := range h.co.Metrics().Operations {
		if operation.Name == "train" && operation.LivePods != 1 {
			t.Fatalf("rank zero registration missing: %+v", operation)
		}
	}
}

func TestDurableFetchUsesAuthenticatedStoreClient(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer read-token" {
			http.Error(w, "missing credentials", http.StatusUnauthorized)
			return
		}
		_, _ = w.Write([]byte(`[{"key":"key","value":7,"epoch":0}]`))
	}))
	defer server.Close()
	client := server.Client()
	client.Transport = bearerRoundTripper{base: client.Transport}
	store, err := storage.NewHTTP(server.URL+"/bucket", client)
	if err != nil {
		t.Fatal(err)
	}
	worker := &Worker{DurableSegments: store, SegmentBaseURL: server.URL + "/bucket", SegmentPrefix: "workload-a"}
	worker.init()
	records, err := worker.fetch(context.Background(), coordinator.SegmentRef{ID: "segment-1", Holder: server.URL + "/bucket"})
	if err != nil || len(records) != 1 || records[0].Key != "key" {
		t.Fatalf("records=%+v err=%v", records, err)
	}
}

func TestDurableFetchBoundsBodyBeforeDecode(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = io.WriteString(w, `[{"key":"key","value":"`+strings.Repeat("x", 256)+`","epoch":0}]`)
	}))
	defer server.Close()
	store, err := storage.NewHTTP(server.URL, server.Client())
	if err != nil {
		t.Fatal(err)
	}
	worker := &Worker{DurableSegments: store, MaxFetchedBytes: 64, MaxFetchedRecords: 10}
	worker.init()
	_, err = worker.fetch(context.Background(), coordinator.SegmentRef{ID: "oversized"})
	if err == nil || !errors.Is(err, storage.ErrObjectTooLarge) {
		t.Fatalf("durable oversized fetch error = %v", err)
	}
}

type bearerRoundTripper struct{ base http.RoundTripper }

func (t bearerRoundTripper) RoundTrip(request *http.Request) (*http.Response, error) {
	copy := request.Clone(request.Context())
	copy.Header.Set("Authorization", "Bearer read-token")
	return t.base.RoundTrip(copy)
}

func newHarness(t *testing.T, specs []graph.Channel) (*harness, context.CancelFunc) {
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
	h, stop := newHarness(t, []graph.Channel{
		{Name: "words", From: "read", To: "count", Partitioning: graph.Partitioning{Mode: graph.PartitionHash, Partitions: 4}},
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
	h, stop := newHarness(t, []graph.Channel{
		{Name: "graph", From: "seed", To: "rank", Partitioning: graph.Partitioning{Mode: graph.PartitionHash, Partitions: 2}},
		{Name: "contrib", From: "rank", To: "rank",
			Partitioning: graph.Partitioning{Mode: graph.PartitionHash, Partitions: 2},
			Feedback:     &graph.Feedback{Mode: graph.FeedbackSynchronous, MaxEpochs: 4}},
		{Name: "ranks", From: "rank"},
	})
	defer stop()
	h.co.SetOperations([]coordinator.OperationSpec{{Name: "rank", Replicas: 2}})

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
	h, stop := newHarness(t, []graph.Channel{
		{Name: "prompts", To: "agent", Partitioning: graph.Partitioning{Mode: graph.PartitionHash, Partitions: 2}},
		{Name: "turns", From: "agent", To: "agent",
			Partitioning: graph.Partitioning{Mode: graph.PartitionHash, Partitions: 2},
			Feedback:     &graph.Feedback{Mode: graph.FeedbackAsynchronous, MaxEpochs: 3, Overflow: "unfinished"}},
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

// combineOutput drains a Retained channel through the coordinator and returns
// the surviving records keyed by their record key.
func combineOutput(t *testing.T, h *harness, channel string) map[string]float64 {
	t.Helper()
	recs, _, err := h.co.Records(channel, "", 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	out := map[string]float64{}
	for _, r := range recs {
		n, ok := r.Value.(float64)
		if !ok {
			t.Fatalf("record %q value %v is not a number", r.Key, r.Value)
		}
		out[r.Key] = n
	}
	return out
}

// A channel that declares Combine folds records sharing a key before they go
// on the wire, so the segment carries one record per key instead of one per
// emitted fact. This is the map-side half of a reduce-by-key.
func TestCombineFoldsRecordsBeforeTheWire(t *testing.T) {
	for _, tc := range []struct {
		mode graph.CombineMode
		want map[string]float64
	}{
		{graph.CombineSum, map[string]float64{"a": 6, "b": 40}},
		{graph.CombineMin, map[string]float64{"a": 1, "b": 10}},
		{graph.CombineMax, map[string]float64{"a": 3, "b": 30}},
		{graph.CombineCount, map[string]float64{"a": 3, "b": 2}},
	} {
		t.Run(string(tc.mode), func(t *testing.T) {
			h, done := newHarness(t, []graph.Channel{{
				Name: "out", From: "src", Durability: graph.DurabilityRetained,
				Partitioning: graph.Partitioning{Mode: graph.PartitionHash, Partitions: 1},
				Combine:      tc.mode,
			}})
			defer done()
			w := h.worker("src", "src-0", nil, []string{"out"})
			h.run(w, Handlers{Source: func(ctx context.Context, w *Worker) error {
				for _, e := range []struct {
					k string
					v int
				}{{"a", 1}, {"b", 10}, {"a", 2}, {"b", 30}, {"a", 3}} {
					if err := w.Emit("out", e.k, e.v); err != nil {
						return err
					}
				}
				return nil
			}})
			h.waitComplete("src")

			got := combineOutput(t, h, "out")
			if len(got) != len(tc.want) {
				t.Fatalf("%s emitted %d records, want %d (one per key): %v", tc.mode, len(got), len(tc.want), got)
			}
			for k, want := range tc.want {
				if got[k] != want {
					t.Fatalf("%s key %q = %v, want %v", tc.mode, k, got[k], want)
				}
			}
			// Five facts went in; the combined channel must carry two records.
			if m := h.co.Metrics().Channels[0]; m.Produced != 2 {
				t.Fatalf("%s put %d records on the wire, want 2 from 5 facts", tc.mode, m.Produced)
			}
		})
	}
}

// Without Combine the same program ships every fact, which is the behaviour
// the feature exists to avoid and the regression guard for the default path.
func TestWithoutCombineEveryFactGoesOnTheWire(t *testing.T) {
	h, done := newHarness(t, []graph.Channel{{
		Name: "out", From: "src", Durability: graph.DurabilityRetained,
		Partitioning: graph.Partitioning{Mode: graph.PartitionHash, Partitions: 1},
	}})
	defer done()
	w := h.worker("src", "src-0", nil, []string{"out"})
	h.run(w, Handlers{Source: func(ctx context.Context, w *Worker) error {
		for i := 0; i < 5; i++ {
			if err := w.Emit("out", "a", 1); err != nil {
				return err
			}
		}
		return nil
	}})
	h.waitComplete("src")
	if m := h.co.Metrics().Channels[0]; m.Produced != 5 {
		t.Fatalf("uncombined channel produced %d records, want 5", m.Produced)
	}
}

// A non-numeric record on an arithmetic combine channel is a programming
// error and must be reported at the Emit that caused it, not silently dropped
// or deferred to a decode failure in the consumer.
func TestCombineRejectsNonNumericValues(t *testing.T) {
	h, done := newHarness(t, []graph.Channel{{
		Name: "out", From: "src", Durability: graph.DurabilityRetained,
		Partitioning: graph.Partitioning{Mode: graph.PartitionHash, Partitions: 1},
		Combine:      graph.CombineSum,
	}})
	defer done()
	w := h.worker("src", "src-0", nil, []string{"out"})
	if err := w.Emit("out", "a", "not a number"); err == nil {
		t.Fatal("Emit accepted a string on a Sum channel")
	} else if !strings.Contains(err.Error(), "must be a number") {
		t.Fatalf("unhelpful error: %v", err)
	}
}

func TestCombineModeIdempotence(t *testing.T) {
	for mode, want := range map[graph.CombineMode]bool{
		graph.CombineMin: true, graph.CombineMax: true,
		graph.CombineSum: false, graph.CombineCount: false,
	} {
		if got := mode.Idempotent(); got != want {
			t.Fatalf("%s.Idempotent() = %v, want %v", mode, got, want)
		}
	}
}

func TestSynchronousLoopDoesNotStallOnIdlePods(t *testing.T) {
	const supersteps = 8
	h, stop := newHarness(t, []graph.Channel{
		{Name: "graph", From: "seed", To: "rank", Partitioning: graph.Partitioning{Mode: graph.PartitionHash, Partitions: 4}},
		{Name: "contrib", From: "rank", To: "rank",
			Partitioning: graph.Partitioning{Mode: graph.PartitionHash, Partitions: 4},
			Feedback:     &graph.Feedback{Mode: graph.FeedbackSynchronous, MaxEpochs: supersteps}},
	})
	defer stop()

	h.run(h.worker("seed", "seed-0", nil, []string{"graph"}), Handlers{
		Source: func(ctx context.Context, w *Worker) error { return w.Emit("graph", "only", 1.0) },
	})
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
	if elapsed, budget := time.Since(start), supersteps*time.Second; elapsed > budget {
		t.Fatalf("%d supersteps took %v, over the %v budget", supersteps, elapsed, budget)
	}
}

func TestUnfetchableSegmentFailsInsteadOfHanging(t *testing.T) {
	h, stop := newHarness(t, []graph.Channel{
		{Name: "s", From: "produce", To: "consume", Partitioning: graph.Partitioning{Mode: graph.PartitionRoundRobin, Partitions: 1}},
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

func TestSegmentFetchStopsWhenResponseBodyStalls(t *testing.T) {
	release := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("["))
		w.(http.Flusher).Flush()
		<-release
	}))
	defer func() {
		close(release)
		srv.Close()
	}()
	w := &Worker{}
	w.init()
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	start := time.Now()
	_, err := w.fetchRetry(ctx, coordinator.SegmentRef{ID: "stalled", Holder: strings.TrimPrefix(srv.URL, "http://")})
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("fetch returned %v, want context deadline", err)
	}
	if elapsed := time.Since(start); elapsed > time.Second {
		t.Fatalf("fetch took %v after cancellation", elapsed)
	}
}

func TestLargeRecordsFlushOnBytes(t *testing.T) {
	h, stop := newHarness(t, []graph.Channel{
		{Name: "big", From: "a", To: "b", Partitioning: graph.Partitioning{Mode: graph.PartitionRoundRobin, Partitions: 1}},
		{Name: "small", From: "a", To: "b", Partitioning: graph.Partitioning{Mode: graph.PartitionRoundRobin, Partitions: 1}},
	})
	defer stop()
	w := h.worker("a", "a-0", nil, []string{"big", "small"})

	// Ten records of 1 MiB each: 10 MiB buffered, nowhere near the 500
	// records the count threshold waits for.
	value := strings.Repeat("x", 1<<20)
	for i := 0; i < 10; i++ {
		if err := w.Emit("big", fmt.Sprintf("k%d", i), value); err != nil {
			t.Fatal(err)
		}
	}
	if n := len(w.unannounced["big"]); n < 2 {
		t.Fatalf("10 MiB in 10 records produced %d segments, want at least 2: the buffer grows without bound until it holds %d records", n, flushRecords)
	}

	// Small records still flush on the count alone, at exactly the same
	// point as before.
	for i := 0; i < flushRecords-1; i++ {
		if err := w.Emit("small", "k", i); err != nil {
			t.Fatal(err)
		}
	}
	if n := len(w.unannounced["small"]); n != 0 {
		t.Fatalf("%d small records produced %d segments, want none before the %dth", flushRecords-1, n, flushRecords)
	}
	if err := w.Emit("small", "k", 0); err != nil {
		t.Fatal(err)
	}
	if n := len(w.unannounced["small"]); n != 1 {
		t.Fatalf("%d small records produced %d segments, want exactly 1", flushRecords, n)
	}
}

func TestTickFiresWhileWaitingForInputSeal(t *testing.T) {
	h, stop := newHarness(t, []graph.Channel{{Name: "input", To: "poll"}})
	defer stop()
	w := h.worker("poll", "poll-0", []string{"input"}, nil)
	w.TickInterval = 10 * time.Millisecond
	ticked := make(chan struct{}, 1)
	h.run(w, Handlers{Tick: func(context.Context, *Worker) error {
		select {
		case ticked <- struct{}{}:
		default:
		}
		return nil
	}})
	select {
	case <-ticked:
	case <-time.After(time.Second):
		t.Fatal("Tick did not run while the worker waited for input")
	}
}

func TestExternalInputMustBeSealedBeforeCompletion(t *testing.T) {
	h, stop := newHarness(t, []graph.Channel{
		{Name: "bounded", From: "source", To: "sink"},
		{Name: "external", To: "sink"},
	})
	defer stop()
	if err := h.co.Produce("external", "", []coordinator.Record{{Key: "config", Value: "ready"}}); err != nil {
		t.Fatal(err)
	}
	h.run(h.worker("source", "source-0", nil, []string{"bounded"}), Handlers{
		Source: func(context.Context, *Worker) error { return nil },
	})
	seen := make(chan struct{}, 1)
	h.run(h.worker("sink", "sink-0", []string{"bounded", "external"}, nil), Handlers{
		OnRecord: func(_ context.Context, _ *Worker, r Record) error {
			if r.Channel == "external" {
				seen <- struct{}{}
			}
			return nil
		},
	})
	h.waitComplete("source")
	if err := h.co.Seal("bounded"); err != nil {
		t.Fatal(err)
	}
	select {
	case <-seen:
	case <-time.After(time.Second):
		t.Fatal("external input was not consumed")
	}
	time.Sleep(200 * time.Millisecond)
	for _, op := range h.co.Metrics().Operations {
		if op.Name == "sink" && op.Complete {
			t.Fatal("sink completed before its external input was sealed")
		}
	}
	if err := h.co.Seal("external"); err != nil {
		t.Fatal(err)
	}
	h.waitComplete("sink")
}

func TestAggregateBufferLimitAcrossPartitions(t *testing.T) {
	h, stop := newHarness(t, []graph.Channel{{
		Name: "out", From: "source", To: "sink",
		Partitioning: graph.Partitioning{Mode: graph.PartitionHash, Partitions: 128},
	}})
	defer stop()
	w := h.worker("source", "source-0", nil, []string{"out"})
	w.MaxBufferedBytes = 1024
	for i := 0; i < 200; i++ {
		if err := w.Emit("out", fmt.Sprintf("key-%d", i), strings.Repeat("x", 100)); err != nil {
			t.Fatal(err)
		}
		if w.bufferedBytes > w.MaxBufferedBytes {
			t.Fatalf("buffered %d bytes, limit %d", w.bufferedBytes, w.MaxBufferedBytes)
		}
	}
	if len(w.unannounced["out"]) == 0 {
		t.Fatal("aggregate pressure did not flush any buffers")
	}
}

func TestOversizedRecordSuggestsBlob(t *testing.T) {
	h, stop := newHarness(t, []graph.Channel{{Name: "out", From: "source", To: "sink"}})
	defer stop()
	w := h.worker("source", "source-0", nil, []string{"out"})
	w.MaxRecordBytes = 32
	err := w.Emit("out", "key", strings.Repeat("x", 64))
	if err == nil || !strings.Contains(err.Error(), "EmitBlob") || !strings.Contains(err.Error(), "32") {
		t.Fatalf("oversized record error = %v", err)
	}
}

func TestFetchedSegmentLimits(t *testing.T) {
	w := &Worker{MaxFetchedBytes: 64, MaxFetchedRecords: 2}
	w.init()
	if _, err := w.fetch(context.Background(), coordinator.SegmentRef{ID: "large", Bytes: 65}); err == nil || !strings.Contains(err.Error(), "65") {
		t.Fatalf("declared byte limit error = %v", err)
	}
	if _, err := w.fetch(context.Background(), coordinator.SegmentRef{ID: "many", Records: 3}); err == nil || !strings.Contains(err.Error(), "3") {
		t.Fatalf("declared record limit error = %v", err)
	}
	srv := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(rw).Encode([]wireRecord{{Key: "k", Value: json.RawMessage(`"` + strings.Repeat("x", 100) + `"`)}})
	}))
	defer srv.Close()
	_, err := w.fetch(context.Background(), coordinator.SegmentRef{ID: "actual-large", Holder: strings.TrimPrefix(srv.URL, "http://")})
	if err == nil || !strings.Contains(err.Error(), "64") {
		t.Fatalf("actual byte limit error = %v", err)
	}
}

func TestFetchedSegmentLimitConsumesTheWholeBody(t *testing.T) {
	w := &Worker{MaxFetchedBytes: 64, MaxFetchedRecords: 2}
	w.init()
	w.client = &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
		body := io.MultiReader(strings.NewReader("[]"), strings.NewReader(strings.Repeat(" ", 100)))
		return &http.Response{StatusCode: http.StatusOK, Body: io.NopCloser(body), Header: make(http.Header)}, nil
	})}
	_, err := w.fetch(context.Background(), coordinator.SegmentRef{ID: "trailing"})
	if err == nil || !strings.Contains(err.Error(), "64") {
		t.Fatalf("trailing-byte limit error = %v", err)
	}
}

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

type countingReader struct {
	r io.Reader
	n int
}

func (r *countingReader) Read(p []byte) (int, error) {
	if len(p) > 16 {
		p = p[:16]
	}
	n, err := r.r.Read(p)
	r.n += n
	return n, err
}

func TestDecodeSegmentStopsAtRecordLimit(t *testing.T) {
	body := "[" + strings.Repeat(`{"key":"k","value":1},`, 1000) + `{"key":"k","value":1}]`
	reader := &countingReader{r: strings.NewReader(body)}
	_, err := decodeSegment(reader, "many", 2)
	if err == nil || !strings.Contains(err.Error(), "limit 2") {
		t.Fatalf("record limit error = %v", err)
	}
	if reader.n >= len(body)/2 {
		t.Fatalf("decoder read %d of %d bytes before enforcing record limit", reader.n, len(body))
	}
}

func TestRuntimeLimitsFromEnvironment(t *testing.T) {
	t.Setenv(coordinator.EnvCoordinator, "http://coordinator")
	t.Setenv(coordinator.EnvOperation, "worker")
	t.Setenv(coordinator.EnvMaxBufferedBytes, "1234")
	t.Setenv(coordinator.EnvMaxFetchedRecords, "17")
	w, err := FromEnv()
	if err != nil {
		t.Fatal(err)
	}
	if w.MaxBufferedBytes != 1234 || w.MaxFetchedRecords != 17 {
		t.Fatalf("limits = %d bytes and %d records", w.MaxBufferedBytes, w.MaxFetchedRecords)
	}
}

func TestSegmentStorePressureFailsWithoutWaiting(t *testing.T) {
	h, stop := newHarness(t, []graph.Channel{{Name: "out", From: "source", To: "sink"}})
	defer stop()
	w := h.worker("source", "source-0", nil, []string{"out"})
	h.co.SetOperations([]coordinator.OperationSpec{{Name: "source", Replicas: 1}, {Name: "sink", Replicas: 1}})
	w.init()
	if err := h.co.Register(w.registration()); err != nil {
		t.Fatal(err)
	}
	w.MaxBufferedBytes = 256
	w.MaxRecordBytes = 256
	w.MaxSegmentStoreBytes = 350
	if err := w.Emit("out", "first", strings.Repeat("x", 180)); err != nil {
		t.Fatal(err)
	}
	if err := w.Flush(); err != nil {
		t.Fatal(err)
	}
	start := time.Now()
	if err := w.Emit("out", "second", strings.Repeat("y", 180)); err != nil {
		t.Fatal(err)
	}
	err := w.Flush()
	if err == nil || !errors.Is(err, errStorageCapacity) {
		t.Fatalf("storage pressure error = %v", err)
	}
	if elapsed := time.Since(start); elapsed > time.Second {
		t.Fatalf("storage pressure took %v to fail", elapsed)
	}
}

func TestDurableSegmentsRejectPodLocalBlobHandles(t *testing.T) {
	store, err := storage.NewLocal(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	w := &Worker{DurableSegments: store, specs: map[string]graph.Channel{"out": {Name: "out", From: "source", To: "sink", Partitioning: graph.Partitioning{Partitions: 1}}}}
	if err := w.EmitBlob("out", "key", strings.NewReader("payload")); err == nil || !strings.Contains(err.Error(), "pod-local") {
		t.Fatalf("EmitBlob error = %v", err)
	}
	handle := BlobHandle{Blob: "local", Holder: "pod:8090", Size: 7}
	if err := w.Emit("out", "key", handle); err == nil || !strings.Contains(err.Error(), "pod-local") {
		t.Fatalf("reserved blob handle error = %v", err)
	}
	if err := w.Emit("out", "key", map[string]string{"object": "durable/model"}); err != nil {
		t.Fatalf("application-owned durable reference rejected: %v", err)
	}
}
