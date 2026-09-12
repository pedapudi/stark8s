package coordinator

import (
	"errors"
	"testing"
	"time"

	"github.com/pedapudi/stark8s/api/graph"
)

func registerIncarnation(t *testing.T, co *Coordinator, op, pod, incarnation string) {
	t.Helper()
	if err := co.Register(PodRegistration{Operation: op, Pod: pod, Incarnation: incarnation, Addr: pod + ":8090", Slots: 1}); err != nil {
		t.Fatal(err)
	}
}

func requireConflict(t *testing.T, err error) {
	t.Helper()
	var coordinatorError *Error
	if !errors.As(err, &coordinatorError) || coordinatorError.Status != 409 {
		t.Fatalf("got %v, want conflict", err)
	}
}

func TestProcessReplacementRequeuesDeliveryAndRejectsStaleRequests(t *testing.T) {
	co := New("self:8090")
	co.Configure([]graph.Channel{
		{Name: "input", From: "source", To: "worker"},
		{Name: "output", From: "worker", To: "sink"},
	})
	registerIncarnation(t, co, "worker", "worker-0", "first")
	id := announce(t, co, "input", "source", "source-0", 0, 0, 3)
	if err := co.AnnounceSession("output", "worker", "worker-0", "first", []SegmentAnnouncement{{ID: "held", Holder: "worker-0:8090", Producer: "worker-0", Records: 1}}); err != nil {
		t.Fatal(err)
	}
	first, err := co.ConsumeSession("input", "worker", "worker-0", "first", 1)
	if err != nil || len(first.Work) != 1 {
		t.Fatalf("initial consume: response=%+v err=%v", first, err)
	}
	if err := co.SourceDone(PodRegistration{Operation: "worker", Pod: "worker-0", Incarnation: "first"}); err != nil {
		t.Fatal(err)
	}

	registerIncarnation(t, co, "worker", "worker-0", "second")
	if got := channelMetrics(co, "output").Lost; got != 0 {
		t.Fatalf("replacement marked holder data lost: %d records", got)
	}
	second, err := co.ConsumeSession("input", "worker", "worker-0", "second", 1)
	if err != nil || len(second.Work) != 1 || second.Work[0].Segments[0].ID != id {
		t.Fatalf("replacement did not reacquire delivery: response=%+v err=%v", second, err)
	}

	requireConflict(t, co.Register(PodRegistration{Operation: "worker", Pod: "worker-0", Incarnation: "first"}))
	requireConflict(t, co.SourceDone(PodRegistration{Operation: "worker", Pod: "worker-0", Incarnation: "first"}))
	_, err = co.ConsumeSession("input", "worker", "worker-0", "first", 1)
	requireConflict(t, err)
	requireConflict(t, co.AckSession("input", "worker", "worker-0", "first", []SegmentAck{{ID: id, Holder: "source-0:8090"}}))
	requireConflict(t, co.AnnounceSession("output", "worker", "worker-0", "first", nil))
	_, err = co.ReleasedSession("worker", "worker-0", "first")
	requireConflict(t, err)

	if err := co.AckSession("input", "worker", "worker-0", "second", []SegmentAck{{ID: id, Holder: "source-0:8090"}}); err != nil {
		t.Fatal(err)
	}
	if got := channelMetrics(co, "input").InFlight; got != 0 {
		t.Fatalf("in-flight records = %d, want 0", got)
	}
}

func TestNackIsOwnedAndIdempotent(t *testing.T) {
	co := New("self:8090")
	co.Configure([]graph.Channel{{Name: "input", From: "source", To: "worker"}})
	registerIncarnation(t, co, "worker", "worker-0", "active")
	id := announce(t, co, "input", "source", "source-0", 0, 0, 2)
	if _, err := co.ConsumeSession("input", "worker", "worker-0", "active", 1); err != nil {
		t.Fatal(err)
	}
	delivery := []SegmentAck{{ID: id, Holder: "source-0:8090"}}
	requireConflict(t, co.NackSession("input", "worker", "worker-0", "stale", delivery))
	if err := co.NackSession("input", "worker", "worker-0", "active", delivery); err != nil {
		t.Fatal(err)
	}
	if err := co.NackSession("input", "worker", "worker-0", "active", delivery); err != nil {
		t.Fatal(err)
	}
	if got := channelMetrics(co, "input").Pending; got != 2 {
		t.Fatalf("pending records = %d, want 2", got)
	}
	if got := channelMetrics(co, "input").LatestDeliveryFailure; got != "" {
		t.Fatalf("unexpected failure diagnostic %q", got)
	}
	resp, err := co.ConsumeSession("input", "worker", "worker-0", "active", 10)
	if err != nil || len(resp.Work) != 1 || len(resp.Work[0].Segments) != 1 {
		t.Fatalf("redelivery: response=%+v err=%v", resp, err)
	}
}

func TestNackRecordsFailureAndDuplicateAckCountsOnce(t *testing.T) {
	now := testTime()
	co := New("self:8090")
	co.now = func() time.Time { return now }
	co.Configure([]graph.Channel{{Name: "input", From: "source", To: "worker"}})
	registerIncarnation(t, co, "worker", "worker-0", "active")
	id := announce(t, co, "input", "source", "source-0", 0, 0, 2)
	if _, err := co.ConsumeSession("input", "worker", "worker-0", "active", 1); err != nil {
		t.Fatal(err)
	}
	delivery := []SegmentAck{{ID: id, Holder: "source-0:8090", Failure: "holder timed out", RetryAfterMillis: 100}}
	if err := co.NackSession("input", "worker", "worker-0", "active", delivery); err != nil {
		t.Fatal(err)
	}
	if got := channelMetrics(co, "input").LatestDeliveryFailure; got != "holder timed out" {
		t.Fatalf("failure diagnostic = %q", got)
	}
	if resp, err := co.ConsumeSession("input", "worker", "worker-0", "active", 1); err != nil || len(resp.Work) != 0 {
		t.Fatalf("delivery ignored retry delay: response=%+v err=%v", resp, err)
	}
	now = now.Add(100 * time.Millisecond)
	if _, err := co.ConsumeSession("input", "worker", "worker-0", "active", 1); err != nil {
		t.Fatal(err)
	}
	ack := []SegmentAck{{ID: id, Holder: "source-0:8090"}}
	if err := co.AckSession("input", "worker", "worker-0", "active", ack); err != nil {
		t.Fatal(err)
	}
	if err := co.AckSession("input", "worker", "worker-0", "active", ack); err != nil {
		t.Fatal(err)
	}
	if got := channelMetrics(co, "input").Acknowledged; got != 2 {
		t.Fatalf("acknowledged records = %d, want 2", got)
	}
}

func TestBroadcastReplacementPodReusesExpiredCohortSlot(t *testing.T) {
	now := testTime()
	co := New("self:8090")
	co.now = func() time.Time { return now }
	co.Configure([]graph.Channel{{Name: "broadcast", From: "source", To: "worker", Partitioning: graph.Partitioning{Mode: graph.PartitionBroadcast}}})
	co.SetOperations([]OperationSpec{{Name: "worker", Replicas: 2}})
	registerIncarnation(t, co, "worker", "worker-a", "a")
	registerIncarnation(t, co, "worker", "worker-b", "b")
	id := announce(t, co, "broadcast", "source", "source-0", 0, 0, 1)
	if _, err := co.ConsumeSession("broadcast", "worker", "worker-a", "a", 1); err != nil {
		t.Fatal(err)
	}
	if err := co.AckSession("broadcast", "worker", "worker-a", "a", []SegmentAck{{ID: id, Holder: "source-0:8090"}}); err != nil {
		t.Fatal(err)
	}
	now = now.Add(PodTTL + time.Second)
	registerIncarnation(t, co, "worker", "worker-c", "c")
	if _, err := co.ConsumeSession("broadcast", "worker", "worker-c", "c", 1); err != nil {
		t.Fatal(err)
	}
	if err := co.AckSession("broadcast", "worker", "worker-c", "c", []SegmentAck{{ID: id, Holder: "source-0:8090"}}); err != nil {
		t.Fatal(err)
	}
	if rel := co.Released("source-0"); len(rel) != 0 {
		t.Fatalf("successive pods in one cohort slot released segment: %v", rel)
	}
	registerIncarnation(t, co, "worker", "worker-d", "d")
	if _, err := co.ConsumeSession("broadcast", "worker", "worker-d", "d", 1); err != nil {
		t.Fatal(err)
	}
	if err := co.AckSession("broadcast", "worker", "worker-d", "d", []SegmentAck{{ID: id, Holder: "source-0:8090"}}); err != nil {
		t.Fatal(err)
	}
	if rel := co.Released("source-0"); len(rel) != 1 {
		t.Fatalf("two cohort slots released segments = %v", rel)
	}
}

func testTime() time.Time { return time.Unix(1_700_000_000, 0) }

func TestBroadcastReplacementUsesExistingCohortSlot(t *testing.T) {
	co := New("self:8090")
	co.Configure([]graph.Channel{{Name: "broadcast", From: "source", To: "worker", Partitioning: graph.Partitioning{Mode: graph.PartitionBroadcast}}})
	co.SetOperations([]OperationSpec{{Name: "worker", Replicas: 2}})
	registerIncarnation(t, co, "worker", "worker-0", "first")
	registerIncarnation(t, co, "worker", "worker-1", "peer")
	id := announce(t, co, "broadcast", "source", "source-0", 0, 0, 1)
	for _, process := range []struct{ pod, incarnation string }{{"worker-0", "first"}, {"worker-1", "peer"}} {
		if _, err := co.ConsumeSession("broadcast", "worker", process.pod, process.incarnation, 1); err != nil {
			t.Fatal(err)
		}
		if process.pod == "worker-0" {
			registerIncarnation(t, co, "worker", "worker-0", "second")
		}
	}
	if err := co.AckSession("broadcast", "worker", "worker-1", "peer", []SegmentAck{{ID: id, Holder: "source-0:8090"}}); err != nil {
		t.Fatal(err)
	}
	if rel := co.Released("source-0"); len(rel) != 0 {
		t.Fatalf("released after one cohort slot acknowledged: %v", rel)
	}
	if _, err := co.ConsumeSession("broadcast", "worker", "worker-0", "second", 1); err != nil {
		t.Fatal(err)
	}
	returned := []SegmentAck{{ID: id, Holder: "source-0:8090", Failure: "temporary fetch failure"}}
	if err := co.NackSession("broadcast", "worker", "worker-0", "second", returned); err != nil {
		t.Fatal(err)
	}
	if err := co.NackSession("broadcast", "worker", "worker-0", "second", returned); err != nil {
		t.Fatal(err)
	}
	if rel := co.Released("source-0"); len(rel) != 0 {
		t.Fatalf("partial broadcast acknowledgement lost after nack: %v", rel)
	}
	if _, err := co.ConsumeSession("broadcast", "worker", "worker-0", "second", 1); err != nil {
		t.Fatal(err)
	}
	if err := co.AckSession("broadcast", "worker", "worker-0", "second", []SegmentAck{{ID: id, Holder: "source-0:8090"}}); err != nil {
		t.Fatal(err)
	}
	if rel := co.Released("source-0"); len(rel) != 1 {
		t.Fatalf("released segments = %v, want one", rel)
	}
}
