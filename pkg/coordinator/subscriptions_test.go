package coordinator

import (
	"errors"
	"sort"
	"sync"
	"testing"

	"github.com/pedapudi/stark8s/api/graph"
)

func TestDurableSubscriptionsAdvanceIndependentlyAndReplay(t *testing.T) {
	co := New("self:8090")
	co.Configure([]graph.Channel{{Name: "events", From: "writer", To: "primary", Durability: graph.DurabilityRetained}})
	if err := co.SetSubscription("events", "primary-reader", SubscriptionSpec{Operation: "primary"}); err != nil {
		t.Fatal(err)
	}
	if err := co.SetSubscription("events", "audit-reader", SubscriptionSpec{}); err != nil {
		t.Fatal(err)
	}
	registerIncarnation(t, co, "primary", "primary-0", "first")

	first := AppendBatch{AppendID: "append-1", Records: []Record{{Key: "a", Value: "first"}}}
	second := AppendBatch{AppendID: "append-2", Records: []Record{{Key: "b", Value: "second"}}}
	if offset, err := co.Append("events", "writer", first); err != nil || offset != 0 {
		t.Fatalf("first append: offset=%d err=%v", offset, err)
	}
	if offset, err := co.Append("events", "writer", second); err != nil || offset != 1 {
		t.Fatalf("second append: offset=%d err=%v", offset, err)
	}
	if offset, err := co.Append("events", "writer", first); err != nil || offset != 0 {
		t.Fatalf("retried append: offset=%d err=%v", offset, err)
	}
	if _, err := co.Append("events", "writer", AppendBatch{AppendID: "append-1", Records: []Record{{Value: "different"}}}); err == nil {
		t.Fatal("changed retry was accepted")
	}

	primaryFirst := consumeSubscriptionOne(t, co, "primary-reader", "primary-0", "first")
	ackSubscriptionOne(t, co, "primary-reader", "primary-0", "first", primaryFirst)
	auditFirst := consumeSubscriptionOne(t, co, "audit-reader", "audit-process", "")
	if auditFirst.AppendID != primaryFirst.AppendID {
		t.Fatalf("subscribers saw different first append: %q and %q", primaryFirst.AppendID, auditFirst.AppendID)
	}
	ackSubscriptionOne(t, co, "audit-reader", "audit-process", "", auditFirst)

	primarySecond := consumeSubscriptionOne(t, co, "primary-reader", "primary-0", "first")
	registerIncarnation(t, co, "primary", "primary-0", "second")
	recovered := consumeSubscriptionOne(t, co, "primary-reader", "primary-0", "second")
	if recovered.Offset != primarySecond.Offset || recovered.ID != primarySecond.ID {
		t.Fatalf("replacement received %+v, want %+v", recovered, primarySecond)
	}
	ackSubscriptionOne(t, co, "primary-reader", "primary-0", "second", recovered)

	auditSecond := consumeSubscriptionOne(t, co, "audit-reader", "audit-process", "")
	if auditSecond.Offset != 1 {
		t.Fatalf("audit offset = %d, want 1", auditSecond.Offset)
	}
	if auditSecond.AppendID != primarySecond.AppendID {
		t.Fatalf("subscribers saw different second append: %q and %q", primarySecond.AppendID, auditSecond.AppendID)
	}
	ackSubscriptionOne(t, co, "audit-reader", "audit-process", "", auditSecond)

	if err := co.ReplaySubscription("events", "primary-reader", []PartitionPosition{{Partition: 0, Offset: 0}}); err != nil {
		t.Fatal(err)
	}
	replayed := consumeSubscriptionOne(t, co, "primary-reader", "primary-0", "second")
	if replayed.Offset != 0 || replayed.ID != primaryFirst.ID {
		t.Fatalf("replay returned %+v, want first append", replayed)
	}
	ackSubscriptionOne(t, co, "primary-reader", "primary-0", "second", replayed)

	if err := co.DeleteRetainedBefore("events", []PartitionPosition{{Partition: 0, Offset: 1}}); err != nil {
		t.Fatal(err)
	}
	var coordinatorError *Error
	err := co.ReplaySubscription("events", "audit-reader", []PartitionPosition{{Partition: 0, Offset: 0}})
	if !errors.As(err, &coordinatorError) || coordinatorError.Status != 410 {
		t.Fatalf("replay deleted position: %v", err)
	}
}

func TestConcurrentAppendsReceiveDistinctCommittedOffsets(t *testing.T) {
	co := New("self:8090")
	co.Configure([]graph.Channel{{Name: "events", From: "writer", To: "reader", Durability: graph.DurabilityRetained}})
	var wait sync.WaitGroup
	offsets := make(chan int64, 2)
	errorsFound := make(chan error, 2)
	for _, id := range []string{"left", "right"} {
		id := id
		wait.Add(1)
		go func() {
			defer wait.Done()
			offset, err := co.Append("events", "writer", AppendBatch{AppendID: id, Records: []Record{{Value: id}}})
			if err != nil {
				errorsFound <- err
				return
			}
			offsets <- offset
		}()
	}
	wait.Wait()
	close(errorsFound)
	for err := range errorsFound {
		t.Fatal(err)
	}
	close(offsets)
	var got []int
	for offset := range offsets {
		got = append(got, int(offset))
	}
	sort.Ints(got)
	if len(got) != 2 || got[0] != 0 || got[1] != 1 {
		t.Fatalf("committed offsets = %v, want [0 1]", got)
	}
}

func TestSealedRetainedChannelAcceptsOnlyCommittedAppendRetry(t *testing.T) {
	co := New("self:8090")
	if err := co.Configure([]graph.Channel{{Name: "events", From: "writer", Durability: graph.DurabilityRetained}}); err != nil {
		t.Fatal(err)
	}
	batch := AppendBatch{AppendID: "one", Records: []Record{{Value: "value"}}}
	if _, err := co.Append("events", "writer", batch); err != nil {
		t.Fatal(err)
	}
	if err := co.Seal("events"); err != nil {
		t.Fatal(err)
	}
	if offset, err := co.Append("events", "writer", batch); err != nil || offset != 0 {
		t.Fatalf("committed retry offset=%d err=%v", offset, err)
	}
	if _, err := co.Append("events", "writer", AppendBatch{AppendID: "two"}); err == nil {
		t.Fatal("new append succeeded after seal")
	}
}

func TestRetentionDeletionEndsAppendIDDeduplication(t *testing.T) {
	co := New("self:8090")
	if err := co.Configure([]graph.Channel{{Name: "events", From: "writer", Durability: graph.DurabilityRetained}}); err != nil {
		t.Fatal(err)
	}
	batch := AppendBatch{AppendID: "one", Records: []Record{{Value: "value"}}}
	if _, err := co.Append("events", "writer", batch); err != nil {
		t.Fatal(err)
	}
	if err := co.DeleteRetainedBefore("events", []PartitionPosition{{Partition: 0, Offset: 1}}); err != nil {
		t.Fatal(err)
	}
	if offset, err := co.Append("events", "writer", batch); err != nil || offset != 1 {
		t.Fatalf("append after retention offset=%d err=%v", offset, err)
	}
}

func consumeSubscriptionOne(t *testing.T, co *Coordinator, subscription, pod, incarnation string) SegmentRef {
	t.Helper()
	resp, err := co.ConsumeSubscription("events", subscription, pod, incarnation, 1)
	if err != nil {
		t.Fatal(err)
	}
	if len(resp.Work) != 1 || len(resp.Work[0].Segments) != 1 {
		t.Fatalf("subscription %q response: %+v", subscription, resp)
	}
	return resp.Work[0].Segments[0]
}

func ackSubscriptionOne(t *testing.T, co *Coordinator, subscription, pod, incarnation string, segment SegmentRef) {
	t.Helper()
	err := co.AckSubscription("events", subscription, pod, incarnation, []SubscriptionAck{{Partition: 0, Offset: segment.Offset, AppendID: segment.AppendID}})
	if err != nil {
		t.Fatal(err)
	}
}
