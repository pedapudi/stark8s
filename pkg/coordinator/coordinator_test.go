package coordinator

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"github.com/pedapudi/stark8s/api/v1alpha1"
)

var segSeq int64

// announce creates one segment of n records on the channel, held by the pod.
func announce(t *testing.T, co *Coordinator, ch, op, pod string, part, epoch int32, n int64) string {
	t.Helper()
	id := fmt.Sprintf("seg-%d", atomic.AddInt64(&segSeq, 1))
	err := co.Announce(ch, op, []SegmentAnnouncement{{
		ID: id, Channel: ch, Partition: part, Epoch: epoch, Records: n,
		Holder: pod + ":8090", Producer: pod,
	}})
	if err != nil {
		t.Fatal(err)
	}
	return id
}

func register(t *testing.T, co *Coordinator, op, pod string) {
	t.Helper()
	if err := co.Register(PodRegistration{Operation: op, Pod: pod, Addr: pod + ":8090", Slots: 1}); err != nil {
		t.Fatal(err)
	}
}

// drain consumes and acknowledges everything the pod is offered; it returns
// the number of records and the last response.
func drain(t *testing.T, co *Coordinator, ch, op, pod string) (int64, *ConsumeResponse) {
	t.Helper()
	var n int64
	for {
		resp, err := co.Consume(ch, op, pod, 10)
		if err != nil {
			t.Fatal(err)
		}
		var acks []SegmentAck
		for _, w := range resp.Work {
			for _, s := range w.Segments {
				n += s.Records
				acks = append(acks, SegmentAck{ID: s.ID, Holder: s.Holder, Pod: pod})
			}
		}
		if len(acks) == 0 {
			return n, resp
		}
		if err := co.Ack(ch, acks); err != nil {
			t.Fatal(err)
		}
	}
}

func channelMetrics(co *Coordinator, name string) ChannelMetrics {
	for _, c := range co.Metrics().Channels {
		if c.Name == name {
			return c
		}
	}
	return ChannelMetrics{}
}

func operationMetrics(co *Coordinator, name string) OperationMetrics {
	for _, o := range co.Metrics().Operations {
		if o.Name == name {
			return o
		}
	}
	return OperationMetrics{}
}

func TestMaterializedGatesUntilSealed(t *testing.T) {
	co := New("self:8090")
	co.Configure([]v1alpha1.Channel{{Name: "s", From: "a", To: "b",
		Partitioning: v1alpha1.Partitioning{Mode: v1alpha1.PartitionHash, Partitions: 4},
		Delivery:     v1alpha1.DeliveryMaterialized}})
	register(t, co, "b", "b-0")
	announce(t, co, "s", "a", "a-0", 1, 0, 2)
	if err := co.Announce("s", "zzz", []SegmentAnnouncement{{ID: "x", Records: 1}}); err == nil {
		t.Fatal("foreign producer accepted")
	}
	n, resp := drain(t, co, "s", "b", "b-0")
	if n != 0 || resp.Drained {
		t.Fatalf("materialized channel delivered before seal: n=%d resp=%+v", n, resp)
	}
	if om := operationMetrics(co, "b"); om.RunnableTasks != 0 {
		t.Fatalf("gated channel counted as runnable: %+v", om)
	}
	if err := co.Seal("s"); err != nil {
		t.Fatal(err)
	}
	n, resp = drain(t, co, "s", "b", "b-0")
	if n != 2 || !resp.Drained {
		t.Fatalf("after seal: n=%d resp=%+v", n, resp)
	}
}

func TestHashChannelsIntoOneOperationAreCoPartitioned(t *testing.T) {
	co := New("self:8090")
	co.Configure([]v1alpha1.Channel{
		{Name: "left", From: "a", To: "join", Partitioning: v1alpha1.Partitioning{Mode: v1alpha1.PartitionHash, Partitions: 8}},
		{Name: "right", From: "b", To: "join", Partitioning: v1alpha1.Partitioning{Mode: v1alpha1.PartitionHash, Partitions: 8}},
	})
	pods := []string{"j-0", "j-1", "j-2"}
	for _, p := range pods {
		register(t, co, "join", p)
	}
	for p := int32(0); p < 8; p++ {
		announce(t, co, "left", "a", "a-0", p, 0, 3)
		announce(t, co, "right", "b", "b-0", p, 0, 5)
	}
	owner := map[int32]string{}
	var total int64
	for round := 0; round < 5; round++ {
		for _, pod := range pods {
			for _, ch := range []string{"left", "right"} {
				resp, err := co.Consume(ch, "join", pod, 4)
				if err != nil {
					t.Fatal(err)
				}
				var acks []SegmentAck
				for _, w := range resp.Work {
					if prev, ok := owner[w.Partition]; ok && prev != pod {
						t.Fatalf("partition %d of %s went to %s but earlier to %s", w.Partition, ch, pod, prev)
					}
					owner[w.Partition] = pod
					for _, s := range w.Segments {
						total += s.Records
						acks = append(acks, SegmentAck{ID: s.ID, Holder: s.Holder, Pod: pod})
					}
				}
				_ = co.Ack(ch, acks)
			}
		}
	}
	if total != 64 {
		t.Fatalf("consumed %d records, want 64", total)
	}
	seen := map[string]int{}
	for _, o := range owner {
		seen[o]++
	}
	if len(seen) != 3 {
		t.Fatalf("partitions not spread across pods: %v", seen)
	}
	for _, m := range co.Metrics().Channels {
		if m.Pending != 0 || m.InFlight != 0 {
			t.Fatalf("not drained: %+v", m)
		}
	}
}

func TestExpiredConsumerRedelivers(t *testing.T) {
	co := New("self:8090")
	now := time.Now()
	co.now = func() time.Time { return now }
	co.Configure([]v1alpha1.Channel{{Name: "s", From: "a", To: "b",
		Partitioning: v1alpha1.Partitioning{Mode: v1alpha1.PartitionRoundRobin, Partitions: 2}}})
	register(t, co, "a", "a-0")
	register(t, co, "b", "dead")
	announce(t, co, "s", "a", "a-0", 0, 0, 1)
	announce(t, co, "s", "a", "a-0", 1, 0, 1)
	resp, _ := co.Consume("s", "b", "dead", 10)
	if len(resp.Work) != 2 {
		t.Fatalf("want 2 partitions of work, got %+v", resp.Work)
	}
	if m := channelMetrics(co, "s"); m.InFlight != 2 || m.Pending != 0 {
		t.Fatalf("in flight: %+v", m)
	}
	now = now.Add(PodTTL + time.Second)
	register(t, co, "a", "a-0") // the holder is still alive
	register(t, co, "b", "alive")
	n, _ := drain(t, co, "s", "b", "alive")
	if n != 2 {
		t.Fatalf("redelivery after consumer expiry: got %d", n)
	}
	if m := channelMetrics(co, "s"); m.Lost != 0 {
		t.Fatalf("live holder's segments reported lost: %+v", m)
	}
}

func TestExpiredHolderLosesSegmentsWithoutBlocking(t *testing.T) {
	co := New("self:8090")
	now := time.Now()
	co.now = func() time.Time { return now }
	co.Configure([]v1alpha1.Channel{{Name: "s", From: "a", To: "b",
		Partitioning: v1alpha1.Partitioning{Mode: v1alpha1.PartitionRoundRobin, Partitions: 2}}})
	register(t, co, "a", "a-0")
	register(t, co, "b", "b-0")
	announce(t, co, "s", "a", "a-0", 0, 0, 4)
	announce(t, co, "s", "a", "a-0", 1, 0, 6)
	if om := operationMetrics(co, "a"); !om.HoldsUnconsumed {
		t.Fatalf("holder not reported: %+v", om)
	}
	// Partition 0 is fetched but not acknowledged; partition 1 is pending.
	resp, _ := co.Consume("s", "b", "b-0", 1)
	if len(resp.Work) != 1 {
		t.Fatalf("want one segment, got %+v", resp.Work)
	}
	_ = co.Seal("s")
	now = now.Add(PodTTL + time.Second)
	register(t, co, "b", "b-0")
	n, resp := drain(t, co, "s", "b", "b-0")
	if n != 0 || !resp.Drained {
		t.Fatalf("lost segments blocked the consumer: n=%d resp=%+v", n, resp)
	}
	m := channelMetrics(co, "s")
	if m.Lost != 10 || m.Pending != 0 || m.InFlight != 0 {
		t.Fatalf("loss not reported: %+v", m)
	}
	if om := operationMetrics(co, "b"); om.Complete {
		t.Fatalf("complete before the pod reported done: %+v", om)
	}
	_ = co.SourceDone(PodRegistration{Operation: "b", Pod: "b-0"})
	if om := operationMetrics(co, "b"); !om.Complete {
		t.Fatalf("consumer not complete after loss: %+v", om)
	}
}

func TestSynchronousBarrierAndTermination(t *testing.T) {
	co := New("self:8090")
	co.Configure([]v1alpha1.Channel{{Name: "fb", From: "r", To: "r",
		Partitioning: v1alpha1.Partitioning{Mode: v1alpha1.PartitionHash, Partitions: 2},
		Feedback:     &v1alpha1.Feedback{MaxEpochs: 3}}})
	register(t, co, "r", "r-0")
	// Epoch-1 segments are held while epoch 0 is current.
	announce(t, co, "fb", "r", "r-0", 0, 1, 1)
	n, resp := drain(t, co, "fb", "r", "r-0")
	if n != 0 || !resp.Quiescent || resp.Epoch != 0 || resp.Mode != v1alpha1.FeedbackSynchronous {
		t.Fatalf("epoch 0: n=%d resp=%+v", n, resp)
	}
	if m := channelMetrics(co, "fb"); m.Pending != 1 {
		t.Fatalf("held segment not counted as pending: %+v", m)
	}
	if err := co.EpochDone("fb", "r-0", 0); err != nil {
		t.Fatal(err)
	}
	n, resp = drain(t, co, "fb", "r", "r-0")
	if n != 1 || resp.Epoch != 1 {
		t.Fatalf("epoch 1: n=%d resp=%+v", n, resp)
	}
	announce(t, co, "fb", "r", "r-0", 1, 2, 1)
	if err := co.Announce("fb", "r", []SegmentAnnouncement{{ID: "late", Records: 1, Epoch: 0, Holder: "r-0:8090"}}); err == nil {
		t.Fatal("segment behind the current epoch accepted")
	}
	_ = co.EpochDone("fb", "r-0", 1)
	n, resp = drain(t, co, "fb", "r", "r-0")
	if n != 1 || resp.Epoch != 2 {
		t.Fatalf("epoch 2: n=%d resp=%+v", n, resp)
	}
	// Segments beyond the bound are dropped and the channel seals after the last epoch.
	announce(t, co, "fb", "r", "r-0", 0, 3, 1)
	_ = co.EpochDone("fb", "r-0", 2)
	_, resp = drain(t, co, "fb", "r", "r-0")
	if !resp.Sealed || !resp.Drained {
		t.Fatalf("loop did not terminate: %+v", resp)
	}
	_ = co.SourceDone(PodRegistration{Operation: "r", Pod: "r-0"})
	if om := operationMetrics(co, "r"); !om.Complete || om.HoldsUnconsumed {
		t.Fatalf("loop operation after termination: %+v", om)
	}
}

func TestSynchronousBarrierWaitsForEveryPod(t *testing.T) {
	co := New("self:8090")
	co.Configure([]v1alpha1.Channel{{Name: "fb", From: "r", To: "r",
		Partitioning: v1alpha1.Partitioning{Mode: v1alpha1.PartitionHash, Partitions: 2},
		Feedback:     &v1alpha1.Feedback{MaxEpochs: 2}}})
	register(t, co, "r", "r-0")
	register(t, co, "r", "r-1")
	drain(t, co, "fb", "r", "r-0")
	drain(t, co, "fb", "r", "r-1")
	_ = co.EpochDone("fb", "r-0", 0)
	if _, resp := drain(t, co, "fb", "r", "r-0"); resp.Epoch != 0 {
		t.Fatalf("barrier advanced with one pod outstanding: %+v", resp)
	}
	_ = co.EpochDone("fb", "r-1", 0)
	if _, resp := drain(t, co, "fb", "r", "r-0"); resp.Epoch != 1 {
		t.Fatalf("barrier did not advance: %+v", resp)
	}
}

func TestAsynchronousLoopDeliversImmediatelyAndTerminates(t *testing.T) {
	co := New("self:8090")
	co.Configure([]v1alpha1.Channel{
		{Name: "in", From: "src", To: "agent", Partitioning: v1alpha1.Partitioning{Mode: v1alpha1.PartitionHash, Partitions: 2}},
		{Name: "loop", From: "agent", To: "agent",
			Partitioning: v1alpha1.Partitioning{Mode: v1alpha1.PartitionHash, Partitions: 2},
			Feedback:     &v1alpha1.Feedback{Mode: v1alpha1.FeedbackAsynchronous, MaxEpochs: 3, Overflow: "spill"}},
		{Name: "spill", From: "agent"},
	})
	register(t, co, "src", "src-0")
	register(t, co, "agent", "ag-0")
	announce(t, co, "in", "src", "src-0", 0, 0, 1)
	_ = co.Seal("in")
	// Records of several epochs are delivered at once: no barrier.
	announce(t, co, "loop", "agent", "ag-0", 0, 1, 1)
	announce(t, co, "loop", "agent", "ag-0", 1, 2, 1)
	resp, _ := co.Consume("loop", "agent", "ag-0", 10)
	if resp.Mode != v1alpha1.FeedbackAsynchronous || resp.MaxEpochs != 3 || resp.Quiescent {
		t.Fatalf("async consume: %+v", resp)
	}
	got := 0
	var acks []SegmentAck
	for _, w := range resp.Work {
		for _, s := range w.Segments {
			got++
			acks = append(acks, SegmentAck{ID: s.ID, Holder: s.Holder, Pod: "ag-0"})
		}
	}
	if got != 2 {
		t.Fatalf("want both epochs delivered, got %d", got)
	}
	if resp.Sealed {
		t.Fatal("loop sealed while the input was not drained")
	}
	// Diverted records land on the overflow channel; dropped ones are counted.
	if err := co.Produce("spill", "agent", []Record{{Key: "k", Value: 1, Epoch: 3}}); err != nil {
		t.Fatal(err)
	}
	if err := co.Announce("loop", "agent", []SegmentAnnouncement{{Overflowed: 2}}); err != nil {
		t.Fatal(err)
	}
	if m := channelMetrics(co, "loop"); m.Overflowed != 2 {
		t.Fatalf("overflow not counted: %+v", m)
	}
	_ = co.Ack("loop", acks)
	drain(t, co, "in", "agent", "ag-0")
	// Inputs are drained and the cycle is quiet: the loop terminates.
	_, resp = drain(t, co, "loop", "agent", "ag-0")
	if !resp.Sealed || !resp.Drained {
		t.Fatalf("async loop did not terminate: %+v", resp)
	}
	_ = co.SourceDone(PodRegistration{Operation: "agent", Pod: "ag-0"})
	if om := operationMetrics(co, "agent"); !om.Complete {
		t.Fatalf("agent not complete: %+v", om)
	}
	recs, _, _ := co.Records("spill", "", 0, 0)
	if len(recs) != 1 || recs[0].Epoch != 3 {
		t.Fatalf("overflow records: %+v", recs)
	}
}

func TestBroadcastDeliversToEveryPod(t *testing.T) {
	co := New("self:8090")
	co.Configure([]v1alpha1.Channel{
		{Name: "bc", From: "a", To: "b", Partitioning: v1alpha1.Partitioning{Mode: v1alpha1.PartitionBroadcast}},
	})
	register(t, co, "a", "a-0")
	register(t, co, "b", "b-0")
	register(t, co, "b", "b-1")
	announce(t, co, "bc", "a", "a-0", 0, 0, 2)
	if n, _ := drain(t, co, "bc", "b", "b-0"); n != 2 {
		t.Fatalf("b-0 got %d broadcast records", n)
	}
	if om := operationMetrics(co, "a"); !om.HoldsUnconsumed {
		t.Fatalf("segment released before every pod acknowledged: %+v", om)
	}
	if n, _ := drain(t, co, "bc", "b", "b-1"); n != 2 {
		t.Fatalf("b-1 got %d broadcast records", n)
	}
	if om := operationMetrics(co, "a"); om.HoldsUnconsumed {
		t.Fatalf("segment not released after every pod acknowledged: %+v", om)
	}
	if rel := co.Released("a-0"); len(rel) != 1 {
		t.Fatalf("released: %v", rel)
	}
}

func TestExternalProduceAndKeyFilteredLongPoll(t *testing.T) {
	co := New("self:8090")
	co.Configure([]v1alpha1.Channel{
		{Name: "in", To: "w", Partitioning: v1alpha1.Partitioning{Mode: v1alpha1.PartitionHash, Partitions: 4}},
		{Name: "out", From: "w"},
	})
	register(t, co, "w", "w-0")
	// External producer: records become coordinator-held segments.
	if err := co.Produce("in", "", []Record{{Key: "a", Value: 1}, {Key: "b", Value: 2}, {Key: "a", Value: 3}}); err != nil {
		t.Fatal(err)
	}
	resp, _ := co.Consume("in", "w", "w-0", 10)
	var n int64
	for _, w := range resp.Work {
		for _, s := range w.Segments {
			if s.Holder != "self:8090" {
				t.Fatalf("holder %q, want the coordinator", s.Holder)
			}
			recs, ok := co.Segment(s.ID)
			if !ok {
				t.Fatalf("segment %s not served", s.ID)
			}
			for _, r := range recs {
				if HashPartition(r.Key, 4) != int(w.Partition) {
					t.Fatalf("record %q in partition %d", r.Key, w.Partition)
				}
			}
			n += s.Records
		}
	}
	if n != 3 {
		t.Fatalf("external records delivered: %d", n)
	}

	// External consumer: key-filtered long poll wakes on produce.
	done := make(chan []Record)
	go func() {
		recs, _, _ := co.Records("out", "x", 0, 5*time.Second)
		done <- recs
	}()
	time.Sleep(50 * time.Millisecond)
	if err := co.Produce("out", "w", []Record{{Key: "y", Value: 1}}); err != nil {
		t.Fatal(err)
	}
	if err := co.Produce("out", "w", []Record{{Key: "x", Value: 2}}); err != nil {
		t.Fatal(err)
	}
	select {
	case recs := <-done:
		if len(recs) != 1 || recs[0].Key != "x" {
			t.Fatalf("filtered poll: %+v", recs)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("long poll did not wake")
	}
	recs, next, _ := co.Records("out", "", 0, 0)
	if len(recs) != 2 || next != 2 {
		t.Fatalf("all records: %+v next=%d", recs, next)
	}
	recs, _, _ = co.Records("out", "", next, 0)
	if len(recs) != 0 {
		t.Fatalf("records after offset: %+v", recs)
	}
	if _, _, err := co.Records("in", "", 0, 0); err == nil {
		t.Fatal("records of a consumed channel served from the coordinator")
	}
}

func TestHoldsUnconsumedAndCompleteTransitions(t *testing.T) {
	co := New("self:8090")
	now := time.Now()
	co.now = func() time.Time { return now }
	co.Configure([]v1alpha1.Channel{
		{Name: "s", From: "src", To: "b", Partitioning: v1alpha1.Partitioning{Mode: v1alpha1.PartitionRoundRobin, Partitions: 2}},
		{Name: "kept", From: "src", To: "c", Durability: v1alpha1.DurabilityRetained,
			Partitioning: v1alpha1.Partitioning{Mode: v1alpha1.PartitionRoundRobin, Partitions: 1}},
	})
	register(t, co, "src", "src-0")
	register(t, co, "src", "src-1")
	if om := operationMetrics(co, "src"); om.Complete || om.HoldsUnconsumed || om.LivePods != 2 {
		t.Fatalf("fresh source: %+v", om)
	}
	announce(t, co, "s", "src", "src-0", 0, 0, 3)
	announce(t, co, "kept", "src", "src-0", 0, 0, 3)
	if om := operationMetrics(co, "src"); !om.HoldsUnconsumed {
		t.Fatalf("source holding an unacknowledged segment: %+v", om)
	}
	// Complete needs source-done from every live pod.
	_ = co.SourceDone(PodRegistration{Operation: "src", Pod: "src-0"})
	if om := operationMetrics(co, "src"); om.Complete {
		t.Fatalf("complete with one pod outstanding: %+v", om)
	}
	_ = co.SourceDone(PodRegistration{Operation: "src", Pod: "src-1"})
	if om := operationMetrics(co, "src"); !om.Complete || !om.HoldsUnconsumed {
		t.Fatalf("source done: %+v", om)
	}
	if om := operationMetrics(co, "b"); om.RunnableTasks != 1 || om.Complete {
		t.Fatalf("consumer before seal: %+v", om)
	}
	_ = co.Seal("s")
	_ = co.Seal("kept")
	register(t, co, "b", "b-0")
	register(t, co, "c", "c-0")
	drain(t, co, "s", "b", "b-0")
	if om := operationMetrics(co, "b"); om.Complete || om.RunnableTasks != 0 {
		t.Fatalf("consumer drained but not yet done: %+v", om)
	}
	_ = co.SourceDone(PodRegistration{Operation: "b", Pod: "b-0"})
	if om := operationMetrics(co, "b"); !om.Complete {
		t.Fatalf("consumer after drain and done: %+v", om)
	}
	if om := operationMetrics(co, "src"); om.HoldsUnconsumed {
		t.Fatalf("Retained segment counted as unconsumed hold: %+v", om)
	}
	if rel := co.Released("src-0"); len(rel) != 1 {
		t.Fatalf("Ephemeral segment not released once: %v", rel)
	}
	if rel := co.Released("src-0"); len(rel) != 0 {
		t.Fatalf("released segment reported twice: %v", rel)
	}
	drain(t, co, "kept", "c", "c-0")
	if rel := co.Released("src-0"); len(rel) != 0 {
		t.Fatalf("Retained segment offered for deletion: %v", rel)
	}
	// A source pod that goes silent stops counting.
	register(t, co, "src", "src-2")
	if om := operationMetrics(co, "src"); om.Complete {
		t.Fatalf("new pod without source-done: %+v", om)
	}
	now = now.Add(PodTTL / 2)
	register(t, co, "src", "src-0")
	register(t, co, "src", "src-1")
	now = now.Add(PodTTL/2 + time.Second)
	register(t, co, "src", "src-0")
	if om := operationMetrics(co, "src"); !om.Complete || om.LivePods != 2 {
		t.Fatalf("after expiry of the silent pod: %+v", om)
	}
}

func TestPendingByPartitionAndHTTPRoundTrip(t *testing.T) {
	co := New("self:8090")
	srv := httptest.NewServer(Handler(co))
	defer srv.Close()
	body, _ := json.Marshal([]v1alpha1.Channel{{Name: "s", From: "a", To: "b",
		Partitioning: v1alpha1.Partitioning{Mode: v1alpha1.PartitionHash, Partitions: 3}}})
	req, _ := http.NewRequest("PUT", srv.URL+PathTopology, bytesReader(body))
	if resp, err := http.DefaultClient.Do(req); err != nil || resp.StatusCode != 204 {
		t.Fatalf("topology: %v %v", err, resp)
	}
	body, _ = json.Marshal([]SegmentAnnouncement{{ID: "s1", Partition: 2, Records: 7, Holder: "a-0:8090", Producer: "a-0"}})
	req, _ = http.NewRequest("POST", srv.URL+PathChannels+"/s"+SuffixSegments, bytesReader(body))
	req.Header.Set(OperationHeader, "a")
	if resp, err := http.DefaultClient.Do(req); err != nil || resp.StatusCode != 204 {
		t.Fatalf("announce: %v %v", err, resp)
	}
	resp, err := http.Get(srv.URL + PathMetrics)
	if err != nil {
		t.Fatal(err)
	}
	var m Metrics
	_ = json.NewDecoder(resp.Body).Decode(&m)
	if len(m.Channels) != 1 || m.Channels[0].Pending != 7 || len(m.Channels[0].PendingByPartition) != 3 || m.Channels[0].PendingByPartition[2] != 7 {
		t.Fatalf("metrics: %+v", m)
	}
	req, _ = http.NewRequest("GET", srv.URL+PathChannels+"/s"+SuffixConsume+"?pod=b-0&max="+strconv.Itoa(5), nil)
	req.Header.Set(OperationHeader, "b")
	resp, _ = http.DefaultClient.Do(req)
	var cr ConsumeResponse
	_ = json.NewDecoder(resp.Body).Decode(&cr)
	if len(cr.Work) != 1 || cr.Work[0].Partition != 2 || cr.Work[0].Segments[0].ID != "s1" {
		t.Fatalf("consume: %+v", cr)
	}
	req, _ = http.NewRequest("GET", srv.URL+PathChannels+"/s"+SuffixConsume+"?pod=b-0", nil)
	req.Header.Set(OperationHeader, "zzz")
	if resp, _ := http.DefaultClient.Do(req); resp.StatusCode != 403 {
		t.Fatalf("foreign consumer status %d", resp.StatusCode)
	}
}

func bytesReader(b []byte) *bytes.Reader { return bytes.NewReader(b) }
