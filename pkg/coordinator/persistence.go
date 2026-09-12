package coordinator

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"time"

	"github.com/pedapudi/stark8s/api/graph"
)

type persistedCoordinator struct {
	NextID     uint64                        `json:"nextID"`
	Channels   map[string]persistedChannel   `json:"channels"`
	Operations map[string]persistedOperation `json:"operations"`
}

type persistedOperation struct {
	Pods            map[string]persistedPod    `json:"pods"`
	Owners          map[string]string          `json:"owners"`
	Pinned          map[string]bool            `json:"pinned,omitempty"`
	EpochDone       map[string]int32           `json:"epochDone,omitempty"`
	Replicas        int32                      `json:"replicas"`
	Retired         map[string]map[string]bool `json:"retired,omitempty"`
	NextCohortSlot  int                        `json:"nextCohortSlot"`
	FreeCohortSlots []int                      `json:"freeCohortSlots,omitempty"`
	Completed       bool                       `json:"completed"`
}

type persistedPod struct {
	Operation   string    `json:"operation"`
	Address     string    `json:"address"`
	Slots       int32     `json:"slots"`
	LastSeen    time.Time `json:"lastSeen"`
	Done        bool      `json:"done"`
	Incarnation string    `json:"incarnation,omitempty"`
	CohortSlot  int       `json:"cohortSlot"`
}

type persistedChannel struct {
	Spec                  graph.Channel                    `json:"spec"`
	Sealed                bool                             `json:"sealed"`
	Produced              int64                            `json:"produced"`
	Overflowed            int64                            `json:"overflowed"`
	Lost                  int64                            `json:"lost"`
	Acknowledged          int64                            `json:"acknowledged"`
	LatestDeliveryFailure string                           `json:"latestDeliveryFailure,omitempty"`
	Epoch                 int32                            `json:"epoch"`
	FiniteEpochs          bool                             `json:"finiteEpochs,omitempty"`
	MaxEpochs             int32                            `json:"maxEpochs,omitempty"`
	ProductionClosed      map[int32]bool                   `json:"productionClosed,omitempty"`
	RoundRobin            uint64                           `json:"roundRobin"`
	Segments              map[string]persistedSegment      `json:"segments"`
	Queues                [][]string                       `json:"queues"`
	Log                   []string                         `json:"log"`
	Held                  []string                         `json:"held"`
	Cursor                map[string]int                   `json:"cursor"`
	EpochDone             map[string]int32                 `json:"epochDone"`
	Records               []Record                         `json:"records"`
	AppendIndex           bool                             `json:"appendIndex,omitempty"`
	All                   []string                         `json:"all,omitempty"`
	History               [][]string                       `json:"history,omitempty"`
	HistoryBase           []int64                          `json:"historyBase,omitempty"`
	Subscriptions         map[string]persistedSubscription `json:"subscriptions,omitempty"`
}

type persistedSubscription struct {
	Name      string                              `json:"name"`
	Operation string                              `json:"operation,omitempty"`
	Positions []int64                             `json:"positions"`
	InFlight  map[int32]persistedSubscriptionWork `json:"inFlight,omitempty"`
}

type persistedSubscriptionWork struct {
	Offset   int64  `json:"offset"`
	AppendID string `json:"appendID"`
	Owner    string `json:"owner"`
}

type persistedSegment struct {
	ID, Holder, Producer, Operation, Channel string
	Partition, Epoch                         int32
	Records, Bytes                           int64
	Durable                                  bool
	Task                                     TaskID
	Delivered, Acked                         map[string]bool
	RetryAfter                               map[string]time.Time
	AppendID                                 string
	Offset                                   int64
	RetentionDeleted                         bool
	Lost, Released, Reported                 bool
	Data                                     []Record
}

func (co *Coordinator) commitLocked() error {
	if co.failed != nil {
		return co.failed
	}
	if co.durable == nil {
		return nil
	}
	body, err := json.Marshal(co.snapshot())
	if err != nil {
		return err
	}
	err = co.durable.Commit(context.Background(), body, co.tombstones())
	if err != nil {
		co.failed = fmt.Errorf("durable checkpoint failed; restart required: %w", err)
	}
	return co.failed
}

func (co *Coordinator) requireDurableWriterLocked() error {
	return co.failed
}

func (co *Coordinator) tombstones() []string {
	var out []string
	for key := range co.tombstoneKeys {
		out = append(out, key)
	}
	for _, c := range co.channels {
		if c.spec.Durability == graph.DurabilityRetained {
			continue
		}
		for _, s := range c.all {
			if s.released {
				out = append(out, s.id)
			}
		}
	}
	sort.Strings(out)
	return out
}

func (co *Coordinator) snapshot() persistedCoordinator {
	out := persistedCoordinator{NextID: co.nextID, Channels: map[string]persistedChannel{}, Operations: map[string]persistedOperation{}}
	for name, o := range co.ops {
		po := persistedOperation{Pods: map[string]persistedPod{}, Owners: o.owner, Pinned: o.pinned, EpochDone: o.epochDone, Replicas: o.replicas, Retired: o.retired, NextCohortSlot: o.nextCohortSlot, FreeCohortSlots: o.freeCohortSlots, Completed: o.completed}
		for id, p := range o.pods {
			po.Pods[id] = persistedPod{Operation: p.op, Address: p.addr, Slots: p.slots, LastSeen: p.lastSeen, Done: p.done, Incarnation: p.incarnation, CohortSlot: p.cohortSlot}
		}
		out.Operations[name] = po
	}
	for name, c := range co.channels {
		pc := persistedChannel{Spec: c.spec, Sealed: c.sealed, Produced: c.produced, Overflowed: c.overflowed, Lost: c.lost, Acknowledged: c.acknowledged, LatestDeliveryFailure: c.latestDeliveryFailure, Epoch: c.epoch, FiniteEpochs: c.finiteEpochs, MaxEpochs: c.maxEpochs, ProductionClosed: c.productionClosed, RoundRobin: c.rr, Segments: map[string]persistedSegment{}, Cursor: c.cursor, EpochDone: c.epochDone, Records: c.records, AppendIndex: true, HistoryBase: c.historyBase, Subscriptions: map[string]persistedSubscription{}}
		for appendID, s := range c.appendIDs {
			pc.Segments[appendID] = persistedSegment{ID: s.id, Holder: s.holder, Producer: s.producer, Operation: s.op, Channel: s.channel, Partition: s.part, Epoch: s.epoch, Records: s.records, Bytes: s.bytes, Durable: s.durable, Task: s.task, Delivered: s.delivered, Acked: s.acked, RetryAfter: s.retryAfter, AppendID: s.appendID, Offset: s.offset, RetentionDeleted: s.retentionDeleted, Lost: s.lost, Released: s.released, Reported: s.reported, Data: s.data}
		}
		for _, s := range c.all {
			pc.All = append(pc.All, s.appendID)
		}
		pc.Queues = make([][]string, len(c.queues))
		for i, q := range c.queues {
			for _, s := range q {
				pc.Queues[i] = append(pc.Queues[i], s.appendID)
			}
		}
		for _, s := range c.log {
			pc.Log = append(pc.Log, s.appendID)
		}
		for _, s := range c.held {
			pc.Held = append(pc.Held, s.appendID)
		}
		pc.History = make([][]string, len(c.history))
		for partition, entries := range c.history {
			for _, s := range entries {
				pc.History[partition] = append(pc.History[partition], s.appendID)
			}
		}
		for subName, sub := range c.subscriptions {
			inFlight := map[int32]persistedSubscriptionWork{}
			for partition, delivery := range sub.inflight {
				inFlight[partition] = persistedSubscriptionWork{Offset: delivery.offset, AppendID: delivery.appendID, Owner: delivery.owner}
			}
			pc.Subscriptions[subName] = persistedSubscription{Name: sub.name, Operation: sub.operation, Positions: sub.positions, InFlight: inFlight}
		}
		out.Channels[name] = pc
	}
	return out
}

func (co *Coordinator) restore(body []byte) error {
	var in persistedCoordinator
	if err := json.Unmarshal(body, &in); err != nil {
		return fmt.Errorf("decode coordinator state: %w", err)
	}
	co.nextID = in.NextID
	co.ops = map[string]*operation{}
	co.channels = map[string]*channel{}
	for name, po := range in.Operations {
		o := &operation{pods: map[string]*pod{}, owner: po.Owners, pinned: po.Pinned, epochDone: po.EpochDone, replicas: po.Replicas, retired: po.Retired, nextCohortSlot: po.NextCohortSlot, freeCohortSlots: po.FreeCohortSlots, completed: po.Completed}
		if o.owner == nil {
			o.owner = map[string]string{}
		}
		if o.retired == nil {
			o.retired = map[string]map[string]bool{}
		}
		if o.pinned == nil {
			o.pinned = map[string]bool{}
		}
		if o.epochDone == nil {
			o.epochDone = map[string]int32{}
		}
		for id, pp := range po.Pods {
			o.pods[id] = &pod{name: id, op: pp.Operation, addr: pp.Address, slots: pp.Slots, lastSeen: pp.LastSeen, done: pp.Done, incarnation: pp.Incarnation, cohortSlot: pp.CohortSlot}
		}
		co.ops[name] = o
	}
	for name, pc := range in.Channels {
		c := &channel{spec: pc.Spec, sealed: pc.Sealed, produced: pc.Produced, overflowed: pc.Overflowed, lost: pc.Lost, acknowledged: pc.Acknowledged, latestDeliveryFailure: pc.LatestDeliveryFailure, epoch: pc.Epoch, finiteEpochs: pc.FiniteEpochs, maxEpochs: pc.MaxEpochs, productionClosed: pc.ProductionClosed, rr: pc.RoundRobin, cursor: pc.Cursor, epochDone: pc.EpochDone, records: pc.Records, historyBase: pc.HistoryBase, all: map[string]*segment{}, inflight: map[string]*segment{}, appendIDs: map[string]*segment{}, subscriptions: map[string]*subscription{}}
		if c.productionClosed == nil {
			c.productionClosed = map[int32]bool{}
		}
		if c.cursor == nil {
			c.cursor = map[string]int{}
		}
		if c.epochDone == nil {
			c.epochDone = map[string]int32{}
		}
		all := map[string]bool{}
		for _, appendID := range pc.All {
			all[appendID] = true
		}
		legacyAll := !pc.AppendIndex
		for key, ps := range pc.Segments {
			s := &segment{id: ps.ID, holder: ps.Holder, producer: ps.Producer, op: ps.Operation, channel: ps.Channel, part: ps.Partition, epoch: ps.Epoch, records: ps.Records, bytes: ps.Bytes, durable: ps.Durable || durableHolder(ps.Holder), task: ps.Task, delivered: ps.Delivered, acked: ps.Acked, retryAfter: ps.RetryAfter, appendID: ps.AppendID, offset: ps.Offset, retentionDeleted: ps.RetentionDeleted, lost: ps.Lost, released: ps.Released, reported: ps.Reported, data: ps.Data}
			if s.appendID == "" {
				s.appendID = key
			}
			if s.delivered == nil {
				s.delivered = map[string]bool{}
			}
			if s.acked == nil {
				s.acked = map[string]bool{}
			}
			if s.retryAfter == nil {
				s.retryAfter = map[string]time.Time{}
			}
			c.appendIDs[s.appendID] = s
			if legacyAll || all[s.appendID] {
				c.all[s.key()] = s
			}
			if len(s.delivered) > 0 {
				c.inflight[s.key()] = s
			}
		}
		c.history = make([][]*segment, len(pc.History))
		if len(c.history) == 0 {
			c.history = make([][]*segment, c.partitions())
		}
		if len(c.historyBase) == 0 {
			c.historyBase = make([]int64, c.partitions())
		}
		for partition, ids := range pc.History {
			for _, appendID := range ids {
				if s := c.appendIDs[appendID]; s != nil {
					c.history[partition] = append(c.history[partition], s)
				}
			}
		}
		for subName, ps := range pc.Subscriptions {
			inFlight := map[int32]subscriptionDelivery{}
			for partition, delivery := range ps.InFlight {
				inFlight[partition] = subscriptionDelivery{offset: delivery.Offset, appendID: delivery.AppendID, owner: delivery.Owner}
			}
			c.subscriptions[subName] = &subscription{name: ps.Name, operation: ps.Operation, positions: ps.Positions, inflight: inFlight}
		}
		c.queues = make([][]*segment, len(pc.Queues))
		for i, keys := range pc.Queues {
			for _, key := range keys {
				if s := c.appendIDs[key]; s != nil {
					c.queues[i] = append(c.queues[i], s)
				}
			}
		}
		for _, key := range pc.Log {
			if s := c.appendIDs[key]; s != nil {
				c.log = append(c.log, s)
			}
		}
		for _, key := range pc.Held {
			if s := c.appendIDs[key]; s != nil {
				c.held = append(c.held, s)
			}
		}
		co.channels[name] = c
	}
	return nil
}
