package exchange

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/pedapudi/stark8s/api/v1alpha1"
)

func rec(key string, epoch int32) Record {
	return Record{Key: key, Value: json.RawMessage(`1`), Epoch: epoch}
}

func drain(t *testing.T, e *Exchange, ch, op, id string) (int, *ConsumeResponse) {
	t.Helper()
	n := 0
	for {
		resp, err := e.Consume(ch, op, id, 10)
		if err != nil {
			t.Fatal(err)
		}
		if len(resp.Records) == 0 {
			return n, resp
		}
		ids := make([]uint64, 0, len(resp.Records))
		for _, r := range resp.Records {
			ids = append(ids, r.ID)
		}
		n += len(ids)
		if err := e.Ack(ch, ids); err != nil {
			t.Fatal(err)
		}
	}
}

func TestMaterializedGatesUntilSealed(t *testing.T) {
	e := New()
	e.Configure([]v1alpha1.Channel{{Name: "s", From: "a", To: "b",
		Partitioning: v1alpha1.Partitioning{Mode: v1alpha1.PartitionHash, Partitions: 4},
		Delivery:     v1alpha1.DeliveryMaterialized}})
	if err := e.Produce("s", "a", []Record{rec("x", 0), rec("y", 0)}); err != nil {
		t.Fatal(err)
	}
	if err := e.Produce("s", "zzz", []Record{rec("x", 0)}); err == nil {
		t.Fatal("foreign producer accepted")
	}
	n, resp := drain(t, e, "s", "b", "b-0")
	if n != 0 || resp.Drained {
		t.Fatalf("materialized channel delivered before seal: n=%d resp=%+v", n, resp)
	}
	if err := e.Seal("s"); err != nil {
		t.Fatal(err)
	}
	n, resp = drain(t, e, "s", "b", "b-0")
	if n != 2 || !resp.Drained {
		t.Fatalf("after seal: n=%d resp=%+v", n, resp)
	}
}

func TestHashAffinityAcrossConsumers(t *testing.T) {
	e := New()
	e.Configure([]v1alpha1.Channel{{Name: "s", From: "a", To: "b",
		Partitioning: v1alpha1.Partitioning{Mode: v1alpha1.PartitionHash, Partitions: 8}}})
	var recs []Record
	for i := 0; i < 200; i++ {
		recs = append(recs, rec(string(rune('a'+i%10)), 0))
	}
	if err := e.Produce("s", "a", recs); err != nil {
		t.Fatal(err)
	}
	// Register two consumers, then read everything; each key must land on one consumer only.
	seen := map[string]string{}
	for round := 0; round < 40; round++ {
		for _, id := range []string{"b-0", "b-1"} {
			resp, _ := e.Consume("s", "b", id, 5)
			var ids []uint64
			for _, r := range resp.Records {
				if prev, ok := seen[r.Key]; ok && prev != id {
					t.Fatalf("key %q delivered to both %s and %s", r.Key, prev, id)
				}
				seen[r.Key] = id
				ids = append(ids, r.ID)
			}
			_ = e.Ack("s", ids)
		}
	}
	if m := e.Metrics()[0]; m.Pending != 0 || m.InFlight != 0 {
		t.Fatalf("not drained: %+v", m)
	}
}

func TestExpiredConsumerRedelivers(t *testing.T) {
	e := New()
	now := time.Now()
	e.now = func() time.Time { return now }
	e.Configure([]v1alpha1.Channel{{Name: "s", From: "a", To: "b",
		Partitioning: v1alpha1.Partitioning{Mode: v1alpha1.PartitionRoundRobin, Partitions: 2}}})
	_ = e.Produce("s", "a", []Record{rec("x", 0), rec("y", 0)})
	resp, _ := e.Consume("s", "b", "dead", 10)
	if len(resp.Records) != 2 {
		t.Fatalf("want 2 records, got %d", len(resp.Records))
	}
	now = now.Add(consumerTTL + time.Second)
	n, _ := drain(t, e, "s", "b", "alive")
	if n != 2 {
		t.Fatalf("redelivery after consumer expiry: got %d", n)
	}
}

func TestFeedbackSuperstepBarrier(t *testing.T) {
	e := New()
	e.Configure([]v1alpha1.Channel{{Name: "fb", From: "r", To: "r",
		Partitioning: v1alpha1.Partitioning{Mode: v1alpha1.PartitionHash, Partitions: 2},
		Feedback:     &v1alpha1.Feedback{MaxEpochs: 3}}})
	// Epoch-1 records are held while epoch 0 is current.
	_ = e.Produce("fb", "r", []Record{rec("x", 1)})
	n, resp := drain(t, e, "fb", "r", "r-0")
	if n != 0 || !resp.Quiescent || resp.Epoch != 0 {
		t.Fatalf("epoch 0: n=%d resp=%+v", n, resp)
	}
	if err := e.EpochDone("fb", "r-0", 0); err != nil {
		t.Fatal(err)
	}
	n, resp = drain(t, e, "fb", "r", "r-0")
	if n != 1 || resp.Epoch != 1 {
		t.Fatalf("epoch 1: n=%d resp=%+v", n, resp)
	}
	_ = e.Produce("fb", "r", []Record{rec("x", 2)})
	_ = e.EpochDone("fb", "r-0", 1)
	n, resp = drain(t, e, "fb", "r", "r-0")
	if n != 1 || resp.Epoch != 2 {
		t.Fatalf("epoch 2: n=%d resp=%+v", n, resp)
	}
	// Records beyond the bound are dropped and the channel seals after the last epoch.
	_ = e.Produce("fb", "r", []Record{rec("x", 3)})
	_ = e.EpochDone("fb", "r-0", 2)
	_, resp = drain(t, e, "fb", "r", "r-0")
	if !resp.Sealed || !resp.Drained {
		t.Fatalf("loop did not terminate: %+v", resp)
	}
}

func TestBroadcastAndExternalLog(t *testing.T) {
	e := New()
	e.Configure([]v1alpha1.Channel{
		{Name: "bc", From: "a", To: "b", Partitioning: v1alpha1.Partitioning{Mode: v1alpha1.PartitionBroadcast}},
		{Name: "out", From: "a"},
	})
	_ = e.Produce("bc", "a", []Record{rec("x", 0), rec("y", 0)})
	for _, id := range []string{"b-0", "b-1"} {
		if n, _ := drain(t, e, "bc", "b", id); n != 2 {
			t.Fatalf("%s got %d broadcast records", id, n)
		}
	}
	_ = e.Produce("out", "a", []Record{rec("k", 0)})
	log, _ := e.Log("out")
	if len(log) != 1 || log[0].Key != "k" {
		t.Fatalf("external log: %+v", log)
	}
}

func TestHashChannelsIntoOneOperationAreCoPartitioned(t *testing.T) {
	e := New()
	e.Configure([]v1alpha1.Channel{
		{Name: "left", From: "a", To: "join", Partitioning: v1alpha1.Partitioning{Mode: v1alpha1.PartitionHash, Partitions: 8}},
		{Name: "right", From: "b", To: "join", Partitioning: v1alpha1.Partitioning{Mode: v1alpha1.PartitionHash, Partitions: 8}},
	})
	var recs []Record
	for i := 0; i < 64; i++ {
		recs = append(recs, rec(string(rune('a'+i%26)), 0))
	}
	_ = e.Produce("left", "a", recs)
	_ = e.Produce("right", "b", recs)
	owner := map[string]string{}
	for round := 0; round < 40; round++ {
		for _, id := range []string{"j-0", "j-1", "j-2"} {
			for _, ch := range []string{"left", "right"} {
				resp, _ := e.Consume(ch, "join", id, 4)
				var ids []uint64
				for _, r := range resp.Records {
					if prev, ok := owner[r.Key]; ok && prev != id {
						t.Fatalf("key %q on %s went to %s but earlier to %s", r.Key, ch, id, prev)
					}
					owner[r.Key] = id
					ids = append(ids, r.ID)
				}
				_ = e.Ack(ch, ids)
			}
		}
	}
	for _, m := range e.Metrics() {
		if m.Pending != 0 {
			t.Fatalf("not drained: %+v", m)
		}
	}
}
