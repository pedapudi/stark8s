package sdk

import (
	"context"
	"encoding/json"
	"errors"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/pedapudi/stark8s/api/graph"
	"github.com/pedapudi/stark8s/pkg/coordinator"
	"github.com/pedapudi/stark8s/pkg/storage"
)

func TestCheckpointRecognizesCoordinatorInputAfterAddressChange(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	segments, _ := storage.NewLocal(t.TempDir())
	checkpoints, _ := storage.NewLocal(t.TempDir())
	firstSegments := httptest.NewServer(nil)
	co, err := coordinator.NewDurable(ctx, strings.TrimPrefix(firstSegments.URL, "http://"), segments, "workload/coordinator.json", "coordinator-1")
	if err != nil {
		t.Fatal(err)
	}
	if err := co.Configure([]graph.Channel{{Name: "input", To: "reduce", Partitioning: graph.Partitioning{Partitions: 1}}}); err != nil {
		t.Fatal(err)
	}
	firstSegments.Config.Handler = coordinator.SegmentHandler(co)
	control := httptest.NewServer(coordinator.Handler(co))
	if err := co.Produce("input", "", []coordinator.Record{{Key: "one", Value: 1}}); err != nil {
		t.Fatal(err)
	}
	if err := co.Seal("input"); err != nil {
		t.Fatal(err)
	}

	state := 0
	recordCalls := 0
	handlers := Handlers{
		OnRecord: func(_ context.Context, _ *Worker, _ Record) error {
			recordCalls++
			state++
			return nil
		},
		Snapshot: func(context.Context) ([]byte, error) { return json.Marshal(state) },
		Restore:  func(_ context.Context, body []byte) error { return json.Unmarshal(body, &state) },
	}
	crashed := errors.New("stop after input checkpoint")
	first := checkpointTestWorker(control.URL, firstSegments.URL, segments, checkpoints, "reduce-0", "first")
	first.Outbound = nil
	first.checkpointAfterCommit = func() error { return crashed }
	if err := first.Run(ctx, handlers); !errors.Is(err, crashed) {
		t.Fatalf("first worker: %v", err)
	}
	if recordCalls != 1 || state != 1 {
		t.Fatalf("before replacement calls=%d state=%d", recordCalls, state)
	}
	control.Close()
	firstSegments.Close()

	replacementSegments := httptest.NewServer(nil)
	co, err = coordinator.NewDurable(ctx, strings.TrimPrefix(replacementSegments.URL, "http://"), segments, "workload/coordinator.json", "coordinator-2")
	if err != nil {
		t.Fatal(err)
	}
	replacementSegments.Config.Handler = coordinator.SegmentHandler(co)
	control = httptest.NewServer(coordinator.Handler(co))
	defer control.Close()
	defer replacementSegments.Close()
	replacement := checkpointTestWorker(control.URL, replacementSegments.URL, segments, checkpoints, "reduce-0", "second")
	replacement.Outbound = nil
	done := make(chan error, 1)
	go func() { done <- replacement.Run(ctx, handlers) }()
	waitForMetric(t, co, "input", func(metric coordinator.ChannelMetrics) bool { return metric.Acknowledged == 1 })
	if recordCalls != 1 || state != 1 {
		t.Fatalf("after replacement calls=%d state=%d", recordCalls, state)
	}
	cancel()
	<-done
}

func TestCheckpointFollowsFixedOwnerAcrossPodReplacement(t *testing.T) {
	ctx := context.Background()
	checkpoints, err := storage.NewLocal(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}

	state := []byte("committed state")
	first := &Worker{Operation: "reduce", Instance: "reduce-old-pod", Incarnation: "first", CheckpointStore: checkpoints, CheckpointPrefix: "workload"}
	first.init()
	handlers := Handlers{
		Snapshot: func(context.Context) ([]byte, error) { return state, nil },
		Restore:  func(context.Context, []byte) error { return nil },
	}
	if err := first.startCheckpoint(ctx, handlers); err != nil {
		t.Fatal(err)
	}
	if err := first.commitCheckpoint(ctx, handlers, "", nil, false, false); err != nil {
		t.Fatal(err)
	}

	var restored []byte
	replacement := &Worker{Operation: "reduce", Instance: "reduce-new-pod", Incarnation: "second", CheckpointStore: checkpoints, CheckpointPrefix: "workload"}
	replacement.init()
	handlers.Restore = func(_ context.Context, body []byte) error {
		restored = append([]byte(nil), body...)
		return nil
	}
	if err := replacement.startCheckpoint(ctx, handlers); err != nil {
		t.Fatal(err)
	}
	if string(restored) != string(state) {
		t.Fatalf("replacement restored %q, want %q", restored, state)
	}
}

func TestCommittedDrainCallbackIsNotRepeatedAfterCrash(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	segments, _ := storage.NewLocal(t.TempDir())
	checkpoints, _ := storage.NewLocal(t.TempDir())
	segmentServer := httptest.NewServer(nil)
	defer segmentServer.Close()
	co := coordinator.New(strings.TrimPrefix(segmentServer.URL, "http://"))
	if err := co.Configure([]graph.Channel{
		{Name: "input", From: "source", To: "reduce", Partitioning: graph.Partitioning{Partitions: 1}},
		{Name: "output", From: "reduce", To: "sink", Durability: graph.DurabilityRetained, Partitioning: graph.Partitioning{Partitions: 1}},
	}); err != nil {
		t.Fatal(err)
	}
	segmentServer.Config.Handler = coordinator.SegmentHandler(co)
	control := httptest.NewServer(coordinator.Handler(co))
	defer control.Close()
	if err := co.Produce("input", "source", []coordinator.Record{{Key: "k", Value: 1}}); err != nil {
		t.Fatal(err)
	}
	if err := co.Seal("input"); err != nil {
		t.Fatal(err)
	}

	drainCalls := 0
	handlers := Handlers{
		OnDrain: func(_ context.Context, w *Worker) error {
			drainCalls++
			return w.Emit("output", "total", drainCalls)
		},
		Snapshot: func(context.Context) ([]byte, error) { return []byte("state"), nil },
		Restore:  func(context.Context, []byte) error { return nil },
	}
	crashed := errors.New("crash after drain checkpoint")
	commits := 0
	first := checkpointTestWorker(control.URL, segmentServer.URL, segments, checkpoints, "reduce-0", "first")
	first.DurableSegments = nil
	first.SegmentDir = t.TempDir()
	first.checkpointAfterCommit = func() error {
		commits++
		if commits == 2 {
			return crashed
		}
		return nil
	}
	if err := first.Run(ctx, handlers); !errors.Is(err, crashed) {
		t.Fatalf("first run: %v", err)
	}
	if got := channelMetric(co, "input").Acknowledged; got != 1 {
		t.Fatalf("input acknowledgements = %d, want 1", got)
	}

	replacement := checkpointTestWorker(control.URL, segmentServer.URL, segments, checkpoints, "reduce-0", "second")
	replacement.DurableSegments = nil
	replacement.SegmentDir = t.TempDir()
	done := make(chan error, 1)
	go func() { done <- replacement.Run(ctx, handlers) }()
	waitForMetric(t, co, "output", func(m coordinator.ChannelMetrics) bool { return m.Produced > 0 })
	if drainCalls != 1 {
		t.Fatalf("OnDrain called %d times, want once", drainCalls)
	}
	cancel()
	<-done
}

func TestCheckpointDoesNotRepublishGarbageCollectedOutput(t *testing.T) {
	ctx := context.Background()
	segments, _ := storage.NewLocal(t.TempDir())
	checkpoints, _ := storage.NewLocal(t.TempDir())
	coordinatorState := segments
	co, err := coordinator.NewDurable(ctx, "127.0.0.1:8090", coordinatorState, "workload/coordinator.json", "first")
	if err != nil {
		t.Fatal(err)
	}
	if err := co.Configure([]graph.Channel{{Name: "output", From: "reduce", To: "sink", Durability: graph.DurabilityEphemeral, Partitioning: graph.Partitioning{Partitions: 1}}}); err != nil {
		t.Fatal(err)
	}
	control := httptest.NewServer(coordinator.Handler(co))
	handlers := Handlers{Snapshot: func(context.Context) ([]byte, error) { return []byte("state"), nil }, Restore: func(context.Context, []byte) error { return nil }}

	first := checkpointTestWorker(control.URL, control.URL, segments, checkpoints, "reduce-0", "first")
	first.Inbound = nil
	first.init()
	if err := first.loadTopology(); err != nil {
		t.Fatal(err)
	}
	if err := first.register(); err != nil {
		t.Fatal(err)
	}
	if err := first.startCheckpoint(ctx, handlers); err != nil {
		t.Fatal(err)
	}
	if err := first.Emit("output", "k", 1); err != nil {
		t.Fatal(err)
	}
	if err := first.commitCheckpoint(ctx, handlers, "", nil, false, false); err != nil {
		t.Fatal(err)
	}
	if err := co.Register(coordinator.PodRegistration{Operation: "sink", Pod: "sink-0", Incarnation: "sink", Slots: 1}); err != nil {
		t.Fatal(err)
	}
	delivery, err := co.ConsumeSession("output", "sink", "sink-0", "sink", 1)
	if err != nil || len(delivery.Work) != 1 || len(delivery.Work[0].Segments) != 1 {
		t.Fatalf("consume output: response=%+v err=%v", delivery, err)
	}
	ref := delivery.Work[0].Segments[0]
	if err := co.AckSession("output", "sink", "sink-0", "sink", []coordinator.SegmentAck{{ID: ref.ID, Holder: ref.Holder, Pod: "sink-0"}}); err != nil {
		t.Fatal(err)
	}
	first.releaseSegments()
	if got := channelMetric(co, "output").Produced; got != 1 {
		t.Fatalf("produced before recovery = %d, want 1", got)
	}
	control.Close()
	co, err = coordinator.NewDurable(ctx, "127.0.0.1:8090", coordinatorState, "workload/coordinator.json", "second")
	if err != nil {
		t.Fatal(err)
	}
	control = httptest.NewServer(coordinator.Handler(co))
	defer control.Close()

	replacement := checkpointTestWorker(control.URL, control.URL, segments, checkpoints, "reduce-0", "second")
	replacement.Inbound = nil
	replacement.init()
	if err := replacement.loadTopology(); err != nil {
		t.Fatal(err)
	}
	if err := replacement.register(); err != nil {
		t.Fatal(err)
	}
	if err := replacement.startCheckpoint(ctx, handlers); err != nil {
		t.Fatal(err)
	}
	if got := channelMetric(co, "output").Produced; got != 1 {
		t.Fatalf("produced after recovery = %d, want 1", got)
	}
}

func checkpointTestWorker(controlURL, segmentURL string, segments, checkpoints storage.Store, instance, incarnation string) *Worker {
	return &Worker{Coordinator: controlURL, Operation: "reduce", Instance: instance, Incarnation: incarnation, Inbound: []string{"input"}, Outbound: []string{"output"}, SegmentListen: "127.0.0.1:0", DurableSegments: segments, SegmentBaseURL: segmentURL, SegmentPrefix: "workload", CheckpointStore: checkpoints, CheckpointPrefix: "workload"}
}

func channelMetric(co *coordinator.Coordinator, name string) coordinator.ChannelMetrics {
	for _, metric := range co.Metrics().Channels {
		if metric.Name == name {
			return metric
		}
	}
	return coordinator.ChannelMetrics{}
}

func waitForMetric(t *testing.T, co *coordinator.Coordinator, channel string, ready func(coordinator.ChannelMetrics) bool) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if ready(channelMetric(co, channel)) {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s metric: %+v", channel, channelMetric(co, channel))
}
