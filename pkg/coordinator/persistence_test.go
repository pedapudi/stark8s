package coordinator

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/pedapudi/stark8s/api/graph"
	"github.com/pedapudi/stark8s/pkg/storage"
)

func TestDurableCoordinatorRestoresPublicationDeliveryAndExternalRecords(t *testing.T) {
	ctx := context.Background()
	store, err := storage.NewLocal(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	co, err := NewDurable(ctx, "coordinator:8090", store, "state", "writer-1")
	if err != nil {
		t.Fatal(err)
	}
	specs := []graph.Channel{
		{Name: "work", From: "source", To: "sink", Partitioning: graph.Partitioning{Partitions: 1}},
		{Name: "results", From: "sink", Durability: graph.DurabilityRetained, Partitioning: graph.Partitioning{Partitions: 1}},
	}
	if err := co.Configure(specs); err != nil {
		t.Fatal(err)
	}
	if err := co.Register(PodRegistration{Operation: "sink", Pod: "sink-0", Slots: 1}); err != nil {
		t.Fatal(err)
	}
	ann := SegmentAnnouncement{ID: "segment-1", Holder: "objects.example/segments", Producer: "source-0", Partition: 0, Records: 2}
	if err := co.Announce("work", "source", []SegmentAnnouncement{ann}); err != nil {
		t.Fatal(err)
	}
	if _, err := co.Consume("work", "sink", "sink-0", 1); err != nil {
		t.Fatal(err)
	}
	if err := co.Produce("results", "sink", []Record{{Key: "answer", Value: 42}}); err != nil {
		t.Fatal(err)
	}
	if err := co.Seal("results"); err != nil {
		t.Fatal(err)
	}

	restored, err := NewDurable(ctx, "replacement:8090", store, "state", "writer-2")
	if err != nil {
		t.Fatal(err)
	}
	if err := co.Ack("work", []SegmentAck{{ID: "segment-1", Holder: "objects.example/segments", Pod: "sink-0"}}); !errors.Is(err, storage.ErrFenced) {
		t.Fatalf("superseded writer returned %v", err)
	}
	metrics := restored.Metrics()
	if len(metrics.Channels) != 2 {
		t.Fatalf("restored metrics: %+v", metrics)
	}
	records, next, err := restored.Records("results", "", 0, 0)
	if err != nil || next != 1 || len(records) != 1 {
		t.Fatalf("restored records=%s next=%d err=%v", mustJSON(records), next, err)
	}
	work, err := restored.Consume("work", "sink", "sink-0", 1)
	if err != nil {
		t.Fatal(err)
	}
	if len(work.Work) != 0 {
		t.Fatalf("in-flight delivery was duplicated: %+v", work)
	}
}

func mustJSON(v any) string { b, _ := json.Marshal(v); return string(b) }

type failingCASStore struct {
	storage.Store
	fail bool
}

func (s *failingCASStore) CompareAndSwap(ctx context.Context, key, version string, body []byte) (string, error) {
	if s.fail {
		return "", errors.New("checkpoint unavailable")
	}
	return s.Store.CompareAndSwap(ctx, key, version, body)
}

func TestCrashAfterExternalSegmentWriteDoesNotPublishMetadata(t *testing.T) {
	ctx := context.Background()
	local, err := storage.NewLocal(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	store := &failingCASStore{Store: local}
	co, err := NewDurable(ctx, "coordinator:8090", store, "state", "writer-1")
	if err != nil {
		t.Fatal(err)
	}
	if err := co.Configure([]graph.Channel{{Name: "input", To: "sink", Partitioning: graph.Partitioning{Partitions: 1}}}); err != nil {
		t.Fatal(err)
	}
	store.fail = true
	if err := co.Produce("input", "", []Record{{Key: "a", Value: 1}}); err == nil {
		t.Fatal("publication succeeded while checkpoint was unavailable")
	}
	if _, err := local.Get(ctx, "segments/ext-1"); err != nil {
		t.Fatalf("segment bytes were not durable before metadata commit: %v", err)
	}
	store.fail = false
	restored, err := NewDurable(ctx, "replacement:8090", store, "state", "writer-2")
	if err != nil {
		t.Fatal(err)
	}
	if m := channelMetrics(restored, "input"); m.Produced != 0 {
		t.Fatalf("uncommitted publication recovered: %+v", m)
	}
}

func TestFailedCheckpointRetriesDoNotReportSuccess(t *testing.T) {
	tests := []struct {
		name      string
		configure []graph.Channel
		mutate    func(*Coordinator) error
	}{
		{
			name:      "announcement",
			configure: []graph.Channel{{Name: "work", From: "source", To: "sink"}},
			mutate: func(co *Coordinator) error {
				return co.Announce("work", "source", []SegmentAnnouncement{{ID: "one", Holder: "worker:8090"}})
			},
		},
		{
			name:      "append",
			configure: []graph.Channel{{Name: "events", From: "source", Durability: graph.DurabilityRetained}},
			mutate: func(co *Coordinator) error {
				_, err := co.Append("events", "source", AppendBatch{AppendID: "one", Records: []Record{{Value: 1}}})
				return err
			},
		},
		{
			name:      "subscription",
			configure: []graph.Channel{{Name: "events", From: "source", Durability: graph.DurabilityRetained}},
			mutate: func(co *Coordinator) error {
				return co.SetSubscription("events", "reader", SubscriptionSpec{})
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			local, err := storage.NewLocal(t.TempDir())
			if err != nil {
				t.Fatal(err)
			}
			store := &failingCASStore{Store: local}
			co, err := NewDurable(context.Background(), "coordinator:8090", store, "state", "writer-1")
			if err != nil {
				t.Fatal(err)
			}
			if err := co.Configure(test.configure); err != nil {
				t.Fatal(err)
			}
			store.fail = true
			if err := test.mutate(co); err == nil {
				t.Fatal("mutation succeeded while checkpoint was unavailable")
			}
			store.fail = false
			if err := test.mutate(co); err == nil {
				t.Fatal("retry reported success after an uncommitted mutation")
			}
		})
	}
}

func TestCoordinatorHeldSegmentUsesReplacementAddress(t *testing.T) {
	ctx := context.Background()
	store, err := storage.NewLocal(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	co, err := NewDurable(ctx, "first:8090", store, "state", "writer-1")
	if err != nil {
		t.Fatal(err)
	}
	if err := co.Configure([]graph.Channel{{Name: "input", To: "sink", Partitioning: graph.Partitioning{Partitions: 1}}}); err != nil {
		t.Fatal(err)
	}
	if err := co.Produce("input", "", []Record{{Key: "a", Value: 1}}); err != nil {
		t.Fatal(err)
	}
	restored, err := NewDurable(ctx, "replacement:8090", store, "state", "writer-2")
	if err != nil {
		t.Fatal(err)
	}
	records, ok := restored.Segment("ext-1")
	if !ok || len(records) != 1 || records[0].Key != "a" {
		t.Fatalf("replacement segment records=%+v ok=%v", records, ok)
	}
	response, err := restored.Consume("input", "sink", "sink-0", 1)
	if err != nil {
		t.Fatal(err)
	}
	if len(response.Work) != 1 || len(response.Work[0].Segments) != 1 || response.Work[0].Segments[0].Holder != "replacement:8090" {
		t.Fatalf("replacement delivery: %+v", response.Work)
	}
}

func TestNegativeAcknowledgementSurvivesCoordinatorReplacement(t *testing.T) {
	ctx := context.Background()
	store, err := storage.NewLocal(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	co, err := NewDurable(ctx, "first:8090", store, "state", "writer-1")
	if err != nil {
		t.Fatal(err)
	}
	if err := co.Configure([]graph.Channel{{Name: "work", From: "source", To: "sink", Partitioning: graph.Partitioning{Partitions: 1}}}); err != nil {
		t.Fatal(err)
	}
	if err := co.Register(PodRegistration{Operation: "sink", Pod: "sink-0", Incarnation: "first", Slots: 1}); err != nil {
		t.Fatal(err)
	}
	announcement := SegmentAnnouncement{ID: "one", Holder: "source:8090", Records: 1}
	if err := co.Announce("work", "source", []SegmentAnnouncement{announcement}); err != nil {
		t.Fatal(err)
	}
	response, err := co.ConsumeSession("work", "sink", "sink-0", "first", 1)
	if err != nil || len(response.Work) != 1 {
		t.Fatalf("consume response=%+v err=%v", response, err)
	}
	if err := co.NackSession("work", "sink", "sink-0", "first", []SegmentAck{{ID: "one", Holder: "source:8090", Failure: "retry"}}); err != nil {
		t.Fatal(err)
	}
	restored, err := NewDurable(ctx, "replacement:8090", store, "state", "writer-2")
	if err != nil {
		t.Fatal(err)
	}
	response, err = restored.ConsumeSession("work", "sink", "sink-0", "first", 1)
	if err != nil || len(response.Work) != 1 {
		t.Fatalf("redelivery response=%+v err=%v", response, err)
	}
}

func TestExternalSubscriptionRedeliversLostResponseAfterReplacement(t *testing.T) {
	ctx := context.Background()
	store, err := storage.NewLocal(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	co, err := NewDurable(ctx, "first:8090", store, "state", "writer-1")
	if err != nil {
		t.Fatal(err)
	}
	if err := co.Configure([]graph.Channel{{Name: "events", From: "writer", Durability: graph.DurabilityRetained}}); err != nil {
		t.Fatal(err)
	}
	if err := co.SetSubscription("events", "external", SubscriptionSpec{}); err != nil {
		t.Fatal(err)
	}
	if _, err := co.Append("events", "writer", AppendBatch{AppendID: "one", Records: []Record{{Value: 1}}}); err != nil {
		t.Fatal(err)
	}
	first := consumeSubscriptionOne(t, co, "external", "stable-reader", "")
	restored, err := NewDurable(ctx, "replacement:8090", store, "state", "writer-2")
	if err != nil {
		t.Fatal(err)
	}
	retry := consumeSubscriptionOne(t, restored, "external", "stable-reader", "")
	if retry.ID != first.ID || retry.AppendID != first.AppendID || retry.Offset != first.Offset {
		t.Fatalf("retry=%+v want=%+v", retry, first)
	}
	ackSubscriptionOne(t, restored, "external", "stable-reader", "", retry)
}

func TestCoordinatorSegmentsUseCheckpointDirectory(t *testing.T) {
	ctx := context.Background()
	store, err := storage.NewLocal(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	co, err := NewDurable(ctx, "coordinator:8090", store, "workload-a/coordinator.json", "writer-1")
	if err != nil {
		t.Fatal(err)
	}
	if err := co.Configure([]graph.Channel{{Name: "input", To: "sink", Partitioning: graph.Partitioning{Partitions: 1}}}); err != nil {
		t.Fatal(err)
	}
	if err := co.Produce("input", "", []Record{{Key: "a", Value: 1}}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Get(ctx, "workload-a/segments/ext-1"); err != nil {
		t.Fatalf("scoped segment is unavailable: %v", err)
	}
	if _, err := store.Get(ctx, "segments/ext-1"); !errors.Is(err, storage.ErrNotFound) {
		t.Fatalf("unscoped segment exists or returned the wrong error: %v", err)
	}
}

func TestReplacementDeletesCommittedSegmentTombstones(t *testing.T) {
	ctx := context.Background()
	store, err := storage.NewLocal(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	co, err := NewDurable(ctx, "coordinator:8090", store, "workload-a/coordinator.json", "writer-1")
	if err != nil {
		t.Fatal(err)
	}
	spec := graph.Channel{Name: "data", From: "source", To: "sink", Partitioning: graph.Partitioning{Partitions: 1}}
	if err := co.Configure([]graph.Channel{spec}); err != nil {
		t.Fatal(err)
	}
	if err := storage.PutImmutable(ctx, store, "workload-a/segments/segment-1", []byte("payload")); err != nil {
		t.Fatal(err)
	}
	announcement := SegmentAnnouncement{ID: "segment-1", Holder: "https://objects.example/bucket", Producer: "source-0", Records: 1}
	if err := co.Announce("data", "source", []SegmentAnnouncement{announcement}); err != nil {
		t.Fatal(err)
	}
	if err := co.Register(PodRegistration{Operation: "sink", Pod: "sink-0", Slots: 1}); err != nil {
		t.Fatal(err)
	}
	response, err := co.Consume("data", "sink", "sink-0", 1)
	if err != nil || len(response.Work) != 1 || len(response.Work[0].Segments) != 1 {
		t.Fatalf("consume response=%+v err=%v", response, err)
	}
	if err := co.Ack("data", []SegmentAck{{ID: "segment-1", Holder: announcement.Holder, Pod: "sink-0"}}); err != nil {
		t.Fatal(err)
	}
	if ids, err := co.ReleasedCommitted("source-0"); err != nil || len(ids) != 1 || ids[0] != "segment-1" {
		t.Fatalf("released ids=%v err=%v", ids, err)
	}

	if _, err := NewDurable(ctx, "replacement:8090", store, "workload-a/coordinator.json", "writer-2"); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Get(ctx, "workload-a/segments/segment-1"); !errors.Is(err, storage.ErrNotFound) {
		t.Fatalf("released segment remains after replacement: %v", err)
	}
	checkpoint, err := store.Get(ctx, "workload-a/coordinator.json")
	if err != nil {
		t.Fatal(err)
	}
	var envelope storage.Checkpoint
	if err := json.Unmarshal(checkpoint.Bytes, &envelope); err != nil {
		t.Fatal(err)
	}
	if len(envelope.Tombstones) != 0 {
		t.Fatalf("cleared tombstones persisted as %v", envelope.Tombstones)
	}
}

func TestSegmentServerFailsClosedAfterCheckpointFailure(t *testing.T) {
	ctx := context.Background()
	local, err := storage.NewLocal(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	store := &failingCASStore{Store: local}
	co, err := NewDurable(ctx, "coordinator:8090", store, "state", "writer-1")
	if err != nil {
		t.Fatal(err)
	}
	if err := co.Configure([]graph.Channel{{Name: "input", To: "sink", Partitioning: graph.Partitioning{Partitions: 1}}}); err != nil {
		t.Fatal(err)
	}
	if err := co.Produce("input", "", []Record{{Key: "a", Value: 1}}); err != nil {
		t.Fatal(err)
	}
	store.fail = true
	if err := co.Seal("input"); err == nil {
		t.Fatal("seal succeeded while checkpoint was unavailable")
	}
	request := httptest.NewRequest(http.MethodGet, "/segments/ext-1", nil)
	response := httptest.NewRecorder()
	SegmentHandler(co).ServeHTTP(response, request)
	if response.Code != http.StatusServiceUnavailable {
		t.Fatalf("segment server returned %d after checkpoint failure, want %d", response.Code, http.StatusServiceUnavailable)
	}
}

func TestSynchronousAcknowledgementSurvivesCoordinatorReplacement(t *testing.T) {
	ctx := context.Background()
	store, err := storage.NewLocal(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	co, err := NewDurable(ctx, "coordinator:8090", store, "state", "writer-1")
	if err != nil {
		t.Fatal(err)
	}
	spec := graph.Channel{
		Name: "loop", From: "worker", To: "worker", Durability: graph.DurabilityRetained,
		Partitioning: graph.Partitioning{Partitions: 1},
		Feedback:     &graph.Feedback{Mode: graph.FeedbackSynchronous, MaxEpochs: 2},
	}
	if err := co.Configure([]graph.Channel{spec}); err != nil {
		t.Fatal(err)
	}
	if err := co.Register(PodRegistration{Operation: "worker", Pod: "worker-0", Slots: 1}); err != nil {
		t.Fatal(err)
	}
	announcement := SegmentAnnouncement{ID: "segment-1", Holder: "source:8090", Producer: "worker-0", Records: 1}
	if err := co.Announce("loop", "worker", []SegmentAnnouncement{announcement}); err != nil {
		t.Fatal(err)
	}
	response, err := co.Consume("loop", "worker", "worker-0", 1)
	if err != nil || len(response.Work) != 1 {
		t.Fatalf("consume response=%+v err=%v", response, err)
	}
	if err := co.Ack("loop", []SegmentAck{{ID: "segment-1", Holder: announcement.Holder, Pod: "worker-0"}}); err != nil {
		t.Fatal(err)
	}
	restored, err := NewDurable(ctx, "replacement:8090", store, "state", "writer-2")
	if err != nil {
		t.Fatal(err)
	}
	response, err = restored.Consume("loop", "worker", "worker-0", 1)
	if err != nil {
		t.Fatal(err)
	}
	if len(response.Work) != 0 {
		t.Fatalf("acknowledged segment was redelivered: %+v", response)
	}
}
