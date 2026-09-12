package sdk

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/pedapudi/stark8s/api/graph"
	"github.com/pedapudi/stark8s/pkg/coordinator"
	"github.com/pedapudi/stark8s/pkg/storage"
)

func TestReducerRecoversAfterInputAckBeforeFinalOutput(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	segments, _ := storage.NewLocal(t.TempDir())
	checkpoints, _ := storage.NewLocal(t.TempDir())
	objectServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		obj, err := segments.Get(r.Context(), strings.TrimPrefix(r.URL.Path, "/"))
		if err != nil {
			http.NotFound(w, r)
			return
		}
		_, _ = w.Write(obj.Bytes)
	}))
	defer objectServer.Close()
	segmentServer := httptest.NewServer(nil)
	co, err := coordinator.NewDurable(ctx, strings.TrimPrefix(segmentServer.URL, "http://"), segments, "workload/coordinator.json", "coordinator-1")
	if err != nil {
		t.Fatal(err)
	}
	segmentServer.Config.Handler = coordinator.SegmentHandler(co)
	control := httptest.NewServer(coordinator.Handler(co))
	defer segmentServer.Close()
	defer control.Close()
	if err := co.Configure([]graph.Channel{
		{Name: "input", From: "source", To: "reduce", Partitioning: graph.Partitioning{Partitions: 1}},
		{Name: "output", From: "reduce", To: "sink", Durability: graph.DurabilityRetained, Partitioning: graph.Partitioning{Partitions: 1}},
	}); err != nil {
		t.Fatal(err)
	}
	if err := co.Produce("input", "source", []coordinator.Record{{Key: "k", Value: 7}}); err != nil {
		t.Fatal(err)
	}
	if err := co.Seal("input"); err != nil {
		t.Fatal(err)
	}

	state := 0
	handlers := Handlers{
		OnRecord: func(_ context.Context, _ *Worker, r Record) error {
			var value int
			_ = json.Unmarshal(r.Value, &value)
			state += value
			return nil
		},
		OnDrain:  func(_ context.Context, w *Worker) error { return w.Emit("output", "k", state) },
		Snapshot: func(context.Context) ([]byte, error) { return json.Marshal(state) },
		Restore:  func(_ context.Context, body []byte) error { return json.Unmarshal(body, &state) },
	}
	crashed := errors.New("crash after input acknowledgement")
	first := &Worker{Coordinator: control.URL, Operation: "reduce", Instance: "reduce-0", Inbound: []string{"input"}, Outbound: []string{"output"}, SegmentListen: "127.0.0.1:0", DurableSegments: segments, SegmentBaseURL: objectServer.URL, SegmentPrefix: "workload", CheckpointStore: checkpoints, CheckpointPrefix: "workload"}
	first.checkpointAfterAck = func() error { return crashed }
	if err := first.Run(ctx, handlers); !errors.Is(err, crashed) {
		t.Fatalf("first run: %v", err)
	}
	metrics := co.Metrics()
	if metrics.Channels[0].Acknowledged != 1 || metrics.Channels[1].Produced != 0 {
		t.Fatalf("visible before recovery: %+v", metrics.Channels)
	}
	control.Close()
	co, err = coordinator.NewDurable(ctx, strings.TrimPrefix(segmentServer.URL, "http://"), segments, "workload/coordinator.json", "coordinator-2")
	if err != nil {
		t.Fatal(err)
	}
	segmentServer.Config.Handler = coordinator.SegmentHandler(co)
	control = httptest.NewServer(coordinator.Handler(co))
	defer control.Close()

	state = 0
	second := &Worker{Coordinator: control.URL, Operation: "reduce", Instance: "reduce-0", Inbound: []string{"input"}, Outbound: []string{"output"}, SegmentListen: "127.0.0.1:0", DurableSegments: segments, SegmentBaseURL: objectServer.URL, SegmentPrefix: "workload", CheckpointStore: checkpoints, CheckpointPrefix: "workload"}
	done := make(chan error, 1)
	go func() { done <- second.Run(ctx, handlers) }()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		metrics = co.Metrics()
		if metrics.Channels[0].Acknowledged == 1 && metrics.Channels[1].Produced == 1 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if state != 7 || metrics.Channels[0].Acknowledged != 1 || metrics.Channels[1].Produced != 1 {
		t.Fatalf("state=%d metrics=%+v", state, metrics.Channels)
	}
	cancel()
	<-done
}

func TestIncompleteSnapshotLeavesPreviousCheckpoint(t *testing.T) {
	ctx := context.Background()
	store, _ := storage.NewLocal(t.TempDir())
	w := &Worker{Operation: "learner", Instance: "learner-0", Incarnation: "first", CheckpointStore: store, CheckpointPrefix: "workload"}
	w.init()
	state := []byte("one")
	h := Handlers{Snapshot: func(context.Context) ([]byte, error) { return state, nil }, Restore: func(context.Context, []byte) error { return nil }}
	if err := w.startCheckpoint(ctx, h); err != nil {
		t.Fatal(err)
	}
	if err := w.commitCheckpoint(ctx, h, "", nil, false, false); err != nil {
		t.Fatal(err)
	}
	if err := storage.PutImmutable(ctx, store, "workload/checkpoints/learner/learner-0/state-incomplete", []byte("two")); err != nil {
		t.Fatal(err)
	}
	var restored []byte
	replacement := &Worker{Operation: "learner", Instance: "learner-0", Incarnation: "second", CheckpointStore: store, CheckpointPrefix: "workload"}
	replacement.init()
	h.Restore = func(_ context.Context, body []byte) error { restored = append([]byte(nil), body...); return nil }
	if err := replacement.startCheckpoint(ctx, h); err != nil {
		t.Fatal(err)
	}
	if string(restored) != "one" {
		t.Fatalf("restored %q", restored)
	}
}

func TestCheckpointRejectsTickBeforeRegistration(t *testing.T) {
	store, _ := storage.NewLocal(t.TempDir())
	w := &Worker{Operation: "poll", Instance: "poll-0", Inbound: []string{"input"}, CheckpointStore: store, TickInterval: time.Second}
	err := w.Run(context.Background(), Handlers{Tick: func(context.Context, *Worker) error { return nil }})
	if err == nil || !strings.Contains(err.Error(), "outside an input checkpoint boundary") {
		t.Fatalf("checkpoint Tick error = %v", err)
	}
}

func TestFlushDoesNotPublishBeforeCheckpointCommit(t *testing.T) {
	h, stop := newHarness(t, []graph.Channel{{Name: "output", From: "worker", To: "sink", Partitioning: graph.Partitioning{Partitions: 1}}})
	defer stop()
	w := h.worker("worker", "worker-0", nil, []string{"output"})
	w.checkpoint = &checkpointSession{}
	if err := w.Emit("output", "key", 1); err != nil {
		t.Fatal(err)
	}
	if err := w.Flush(); err != nil {
		t.Fatal(err)
	}
	if got := h.co.Metrics().Channels[0].Produced; got != 0 {
		t.Fatalf("Flush published %d records before checkpoint commit", got)
	}
	if len(w.unannounced["output"]) != 1 {
		t.Fatalf("materialized output = %+v", w.unannounced)
	}
}
