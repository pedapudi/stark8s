package coordinator

import (
	"testing"

	"github.com/pedapudi/stark8s/api/v1alpha1"
)

// A Broadcast segment must survive until every replica of the consuming
// operation has it, including replicas that have not started yet.
//
// The shape is a source feeding a Broadcast Materialized channel into a
// three-replica consumer. The consumer's pods are created together and start
// at different times, so the first one up can consume, acknowledge and drain
// before its peers have registered. Counting acknowledgements against the pod
// registry treats that as the whole operation having finished, frees the
// segment, and tells the producer to delete the bytes its other two replicas
// are about to ask for.
func TestBroadcastSegmentHeldUntilEveryReplicaAcknowledges(t *testing.T) {
	co := New("self:8090")
	co.Configure([]v1alpha1.Channel{{
		Name: "dict", From: "vocab", To: "extract",
		Partitioning: v1alpha1.Partitioning{Mode: v1alpha1.PartitionBroadcast},
		Delivery:     v1alpha1.DeliveryMaterialized,
	}})
	co.SetOperations([]OperationSpec{{Name: "vocab", Replicas: 1}, {Name: "extract", Replicas: 3}})

	register(t, co, "vocab", "vocab-0")
	announce(t, co, "dict", "vocab", "vocab-0", 0, 0, 100)
	if err := co.SourceDone(PodRegistration{Operation: "vocab", Pod: "vocab-0"}); err != nil {
		t.Fatal(err)
	}
	if err := co.Seal("dict"); err != nil {
		t.Fatal(err)
	}

	// Only the first replica has started. It reads everything it is offered,
	// acknowledges, sees the channel drained, and reports done.
	if n, _ := drain(t, co, "dict", "extract", "extract-0"); n != 100 {
		t.Fatalf("extract-0 read %d records, want 100", n)
	}
	if err := co.SourceDone(PodRegistration{Operation: "extract", Pod: "extract-0"}); err != nil {
		t.Fatal(err)
	}

	if om := operationMetrics(co, "vocab"); !om.HoldsUnconsumed {
		t.Errorf("producer freed its segment after 1 of 3 replicas acknowledged: %+v", om)
	}
	if om := operationMetrics(co, "extract"); om.Complete {
		t.Errorf("consumer reported complete after 1 of 3 replicas read it: %+v", om)
	}
	if rel := co.Released("vocab-0"); len(rel) != 0 {
		t.Errorf("producer told to delete %v while two replicas have not read it", rel)
	}

	// The remaining replicas start and read the same records.
	for _, pod := range []string{"extract-1", "extract-2"} {
		if n, _ := drain(t, co, "dict", "extract", pod); n != 100 {
			t.Fatalf("%s read %d records, want 100", pod, n)
		}
		if err := co.SourceDone(PodRegistration{Operation: "extract", Pod: pod}); err != nil {
			t.Fatal(err)
		}
	}

	if om := operationMetrics(co, "extract"); !om.Complete {
		t.Errorf("consumer not complete after every replica read it: %+v", om)
	}
	if om := operationMetrics(co, "vocab"); om.HoldsUnconsumed {
		t.Errorf("producer still holding after every replica acknowledged: %+v", om)
	}
	if rel := co.Released("vocab-0"); len(rel) != 1 {
		t.Errorf("producer not told to delete the segment: %v", rel)
	}
}

// A consumer scaled down while a Broadcast segment is outstanding must not
// pin that segment for ever waiting for replicas it no longer runs.
func TestBroadcastReleasedAfterConsumerScalesDown(t *testing.T) {
	co := New("self:8090")
	co.Configure([]v1alpha1.Channel{
		{Name: "dict", From: "vocab", To: "extract",
			Partitioning: v1alpha1.Partitioning{Mode: v1alpha1.PartitionBroadcast}},
	})
	co.SetOperations([]OperationSpec{{Name: "extract", Replicas: 3}})
	register(t, co, "vocab", "vocab-0")
	announce(t, co, "dict", "vocab", "vocab-0", 0, 0, 10)
	drain(t, co, "dict", "extract", "extract-0")
	if om := operationMetrics(co, "vocab"); !om.HoldsUnconsumed {
		t.Fatalf("released while the consumer still wanted three replicas: %+v", om)
	}
	// The controller republishes on every pass; the consumer now runs one.
	co.SetOperations([]OperationSpec{{Name: "extract", Replicas: 1}})
	if om := operationMetrics(co, "vocab"); om.HoldsUnconsumed {
		t.Errorf("still held after the consumer scaled down to its one reader: %+v", om)
	}
}

// A Broadcast channel read from outside the workload has no consuming
// operation, and looking one up would invent an operation with no name.
func TestBroadcastWithNoConsumerInventsNoOperation(t *testing.T) {
	co := New("self:8090")
	co.Configure([]v1alpha1.Channel{
		{Name: "out", From: "a", Partitioning: v1alpha1.Partitioning{Mode: v1alpha1.PartitionBroadcast}},
	})
	register(t, co, "a", "a-0")
	// Announcing rather than producing puts a segment on the channel, which
	// is what a release sweep walks.
	announce(t, co, "out", "a", "a-0", 0, 0, 1)
	for _, om := range co.Metrics().Operations {
		if om.Name == "" {
			t.Fatalf("metrics report an operation with no name: %+v", co.Metrics().Operations)
		}
	}
}
