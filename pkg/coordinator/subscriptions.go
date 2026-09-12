package coordinator

import (
	"fmt"
	"reflect"
	"sort"
	"time"

	"github.com/pedapudi/stark8s/api/graph"
)

// Append stores one batch in the order in which the coordinator accepts it.
// AppendID makes a retry idempotent while the batch remains in retained
// history. Explicit retention deletion also deletes its append receipt.
func (co *Coordinator) Append(channelName, operation string, batch AppendBatch) (int64, error) {
	return co.AppendSession(channelName, operation, "", "", batch)
}

// AppendSession stores one batch after verifying an internal writer process.
func (co *Coordinator) AppendSession(channelName, operation, podName, incarnation string, batch AppendBatch) (int64, error) {
	co.mu.Lock()
	defer co.mu.Unlock()
	if err := co.requireDurableWriterLocked(); err != nil {
		return 0, err
	}
	c, err := co.get(channelName)
	if err != nil {
		return 0, err
	}
	if c.spec.Durability != graph.DurabilityRetained {
		return 0, errf(400, "channel %q is not retained", channelName)
	}
	if !co.mayProduce(c, operation) {
		return 0, errf(403, "operation %q may not produce on channel %q", operation, channelName)
	}
	if podName != "" {
		if _, err := co.requireIncarnation(operation, podName, incarnation); err != nil {
			return 0, err
		}
	}
	if batch.AppendID == "" {
		return 0, errf(400, "appendId is required")
	}
	if batch.Partition < 0 || int(batch.Partition) >= c.partitions() {
		return 0, errf(400, "partition %d out of range", batch.Partition)
	}
	if prior := c.appendIDs[batch.AppendID]; prior != nil {
		if prior.part != batch.Partition || !sameRecords(prior.data, batch.Records) {
			return 0, errf(409, "append %q already committed with different records", batch.AppendID)
		}
		return prior.offset, nil
	}
	if c.sealed {
		return 0, errf(409, "channel %q is sealed", channelName)
	}
	co.nextID++
	id := fmt.Sprintf("coordinator-%d", co.nextID)
	s := &segment{id: id, holder: co.selfAddr, channel: channelName, part: batch.Partition, records: int64(len(batch.Records)), appendID: batch.AppendID, data: append([]Record(nil), batch.Records...), delivered: map[string]bool{}, acked: map[string]bool{}, retryAfter: map[string]time.Time{}}
	co.index(c, s)
	if err := co.commitLocked(); err != nil {
		return 0, err
	}
	return s.offset, nil
}

func sameRecords(a, b []Record) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i].Key != b[i].Key || a[i].Epoch != b[i].Epoch || !reflect.DeepEqual(a[i].Value, b[i].Value) {
			return false
		}
	}
	return true
}

// SetSubscription creates a named durable cursor. Repeating the same binding
// is idempotent; changing its operation is rejected.
func (co *Coordinator) SetSubscription(channelName, name string, spec SubscriptionSpec) error {
	if name == "" {
		return errf(400, "subscription name is required")
	}
	co.mu.Lock()
	defer co.mu.Unlock()
	if err := co.requireDurableWriterLocked(); err != nil {
		return err
	}
	c, err := co.get(channelName)
	if err != nil {
		return err
	}
	if c.spec.Durability != graph.DurabilityRetained {
		return errf(400, "channel %q is not retained", channelName)
	}
	if prior := c.subscriptions[name]; prior != nil {
		if prior.operation != spec.Operation {
			return errf(409, "subscription %q is already bound to operation %q", name, prior.operation)
		}
		return nil
	}
	positions := append([]int64(nil), c.historyBase...)
	c.subscriptions[name] = &subscription{name: name, operation: spec.Operation, positions: positions, inflight: map[int32]subscriptionDelivery{}}
	return co.commitLocked()
}

// ConsumeSubscription returns at most one ordered append per assigned
// partition. Replicas of one operation share the subscription positions.
func (co *Coordinator) ConsumeSubscription(channelName, name, podName, incarnation string, max int) (*ConsumeResponse, error) {
	co.mu.Lock()
	defer co.mu.Unlock()
	c, err := co.get(channelName)
	if err != nil {
		return nil, err
	}
	sub := c.subscriptions[name]
	if sub == nil {
		return nil, errf(404, "subscription %q not found", name)
	}
	owner := deliveryKey(podName, incarnation)
	partitions := make([]int, c.partitions())
	for i := range partitions {
		partitions[i] = i
	}
	if sub.operation != "" {
		if _, err := co.requireIncarnation(sub.operation, podName, incarnation); err != nil {
			return nil, err
		}
		partitions = c.assigned(co.op(sub.operation), podName)
	} else if podName == "" {
		return nil, errf(400, "reader is required")
	}
	if max <= 0 {
		max = 100
	}
	resp := &ConsumeResponse{Sealed: c.sealed}
	for _, p := range partitions {
		if len(resp.Work) >= max {
			break
		}
		position := sub.positions[p]
		if position < c.historyBase[p] {
			return nil, errf(410, "subscription %q position %d on partition %d is no longer retained", name, position, p)
		}
		if delivery, busy := sub.inflight[int32(p)]; busy {
			// External readers use podName as a stable reader identity. Returning
			// the same delivery lets that reader recover a lost response.
			if sub.operation == "" && delivery.owner == owner {
				index := delivery.offset - c.historyBase[p]
				if index >= 0 && index < int64(len(c.history[p])) {
					entry := c.history[p][index]
					resp.Work = append(resp.Work, PartitionWork{Partition: int32(p), Segments: []SegmentRef{ref(entry)}})
				}
			}
			continue
		}
		index := position - c.historyBase[p]
		if index >= int64(len(c.history[p])) {
			continue
		}
		entry := c.history[p][index]
		sub.inflight[int32(p)] = subscriptionDelivery{offset: position, appendID: entry.appendID, owner: owner}
		resp.Work = append(resp.Work, PartitionWork{Partition: int32(p), Segments: []SegmentRef{ref(entry)}})
	}
	resp.Drained = c.sealed && subscriptionDrained(c, sub)
	if len(resp.Work) > 0 {
		if err := co.commitLocked(); err != nil {
			return nil, err
		}
	}
	return resp, nil
}

func subscriptionDrained(c *channel, sub *subscription) bool {
	if len(sub.inflight) != 0 {
		return false
	}
	for p, position := range sub.positions {
		if position < c.historyBase[p]+int64(len(c.history[p])) {
			return false
		}
	}
	return true
}

// AckSubscription commits an append only when the active reader owns it.
func (co *Coordinator) AckSubscription(channelName, name, podName, incarnation string, acks []SubscriptionAck) error {
	co.mu.Lock()
	defer co.mu.Unlock()
	c, err := co.get(channelName)
	if err != nil {
		return err
	}
	sub := c.subscriptions[name]
	if sub == nil {
		return errf(404, "subscription %q not found", name)
	}
	if sub.operation != "" {
		if _, err := co.requireIncarnation(sub.operation, podName, incarnation); err != nil {
			return err
		}
	}
	owner := deliveryKey(podName, incarnation)
	for _, ack := range acks {
		delivery, ok := sub.inflight[ack.Partition]
		if !ok || delivery.owner != owner || delivery.offset != ack.Offset || delivery.appendID != ack.AppendID {
			continue
		}
		delete(sub.inflight, ack.Partition)
		sub.positions[ack.Partition] = ack.Offset + 1
	}
	return co.commitLocked()
}

// ReplaySubscription moves a named cursor to retained absolute positions.
func (co *Coordinator) ReplaySubscription(channelName, name string, positions []PartitionPosition) error {
	co.mu.Lock()
	defer co.mu.Unlock()
	c, err := co.get(channelName)
	if err != nil {
		return err
	}
	sub := c.subscriptions[name]
	if sub == nil {
		return errf(404, "subscription %q not found", name)
	}
	if len(sub.inflight) != 0 {
		return errf(409, "subscription %q has deliveries in flight", name)
	}
	for _, position := range positions {
		if position.Partition < 0 || int(position.Partition) >= len(sub.positions) {
			return errf(400, "partition %d out of range", position.Partition)
		}
		end := c.historyBase[position.Partition] + int64(len(c.history[position.Partition]))
		if position.Offset < c.historyBase[position.Partition] || position.Offset > end {
			return errf(410, "position %d on partition %d is unavailable", position.Offset, position.Partition)
		}
	}
	for _, position := range positions {
		sub.positions[position.Partition] = position.Offset
	}
	return co.commitLocked()
}

// DeleteRetainedBefore removes retained history and append receipts before
// absolute positions. Append ID deduplication begins again at that boundary.
func (co *Coordinator) DeleteRetainedBefore(channelName string, positions []PartitionPosition) error {
	co.mu.Lock()
	defer co.mu.Unlock()
	c, err := co.get(channelName)
	if err != nil {
		return err
	}
	if c.spec.Durability != graph.DurabilityRetained {
		return errf(400, "channel %q is not retained", channelName)
	}
	for _, position := range positions {
		if position.Partition < 0 || int(position.Partition) >= len(c.history) {
			return errf(400, "partition %d out of range", position.Partition)
		}
		p := int(position.Partition)
		end := c.historyBase[p] + int64(len(c.history[p]))
		if position.Offset < c.historyBase[p] || position.Offset > end {
			return errf(400, "retention position %d on partition %d is out of range", position.Offset, p)
		}
	}
	for _, position := range positions {
		p := int(position.Partition)
		count := int(position.Offset - c.historyBase[p])
		for _, entry := range c.history[p][:count] {
			delete(c.appendIDs, entry.appendID)
			entry.data = nil
			entry.retentionDeleted = true
			entry.released = true
			if entry.producer == "" {
				delete(c.all, entry.key())
			}
		}
		c.history[p] = append([]*segment(nil), c.history[p][count:]...)
		c.historyBase[p] = position.Offset
	}
	return co.commitLocked()
}

func (co *Coordinator) subscriptionNames(c *channel) []string {
	names := make([]string, 0, len(c.subscriptions))
	for name := range c.subscriptions {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}
