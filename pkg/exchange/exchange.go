// Package exchange implements the channel runtime: a partitioned,
// in-memory message exchange that hosts every channel of one workload.
//
// The exchange is the single place where channel semantics are enforced:
// partitioning across consumer replicas, Materialized versus Pipelined
// delivery, sealing when a producer completes, retention, and the
// bulk-synchronous epoch barrier that gives feedback channels (cycles) a
// well-defined meaning. Workers talk to it over HTTP through the SDK.
//
// State lives in memory only; a restart of the exchange loses every
// unconsumed record. That is acceptable for a local, single-node runtime
// and is the first thing to replace for production use.
package exchange

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"sort"
	"sync"
	"time"

	"github.com/pedapudi/stark8s/api/v1alpha1"
)

// Record is one unit of information on a channel.
type Record struct {
	ID    uint64          `json:"id,omitempty"`
	Key   string          `json:"key"`
	Value json.RawMessage `json:"value"`
	// Epoch is the superstep the record belongs to. Zero outside loops.
	Epoch int32 `json:"epoch"`
}

// ConsumeResponse is what a consumer receives on each poll.
type ConsumeResponse struct {
	Records []Record `json:"records"`
	// Sealed: the producer has completed; no further records will arrive.
	Sealed bool `json:"sealed"`
	// Drained: sealed and nothing pending or in flight on any partition.
	// A Drain-completion consumer may exit once every inbound channel is drained.
	Drained bool `json:"drained"`
	// Epoch is the channel's current superstep (feedback channels only).
	Epoch int32 `json:"epoch"`
	// Quiescent: feedback channel with no pending or in-flight records at
	// the current epoch. The consumer should finish the epoch and report
	// epoch-done so the barrier can advance.
	Quiescent bool `json:"quiescent"`
	// MaxEpochs echoes the channel's loop bound so workers can act on the
	// final epoch (for example emit results).
	MaxEpochs int32 `json:"maxEpochs,omitempty"`
}

// Metrics is the per-channel view the controller uses for status and scaling.
type Metrics struct {
	Name      string `json:"name"`
	From      string `json:"from"`
	To        string `json:"to"`
	Sealed    bool   `json:"sealed"`
	Pending   int64  `json:"pending"`
	InFlight  int64  `json:"inFlight"`
	Produced  int64  `json:"produced"`
	Consumers int    `json:"consumers"`
	Epoch     int32  `json:"epoch"`
}

const consumerTTL = 20 * time.Second

// consumer is one replica of a consuming operation. Liveness is tracked per
// operation because a worker polls all of its inbound channels together.
type consumer struct {
	lastSeen time.Time
}

// group is the consumer-side state shared by every channel into one
// operation. Hash partition ownership lives here so that partition p of
// every hash channel into the operation is owned by the same replica: two
// inputs hashed with the same partition count are co-partitioned, which a
// join or a loop vertex holding per-key state depends on.
type group struct {
	consumers map[string]*consumer
	// owner maps "partitions/p" -> consumer id.
	owner map[string]string
}

func (g *group) liveIDs() []string {
	ids := make([]string, 0, len(g.consumers))
	for id := range g.consumers {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	return ids
}

func ownerKey(partitions, p int) string { return fmt.Sprintf("%d/%d", partitions, p) }

type inflight struct {
	rec       Record
	consumer  string
	partition int
}

type channel struct {
	spec v1alpha1.Channel

	sealed   bool
	produced int64
	rr       uint64
	epoch    int32

	// queues holds pending records per partition (Hash, RoundRobin).
	queues [][]Record
	// log is the retained record log used by Broadcast delivery and by
	// Retained durability / external channels.
	log []Record
	// held holds feedback records for epochs later than the current one.
	held []Record

	// cursor is each consumer's broadcast read position.
	cursor map[string]int
	// epochDone is the last epoch each consumer reported finished.
	epochDone map[string]int32
	inflight  map[uint64]inflight
}

// Exchange hosts the channels of one workload.
type Exchange struct {
	mu       sync.Mutex
	channels map[string]*channel
	// groups is keyed by consuming operation name (empty for external).
	groups map[string]*group
	nextID uint64
	now    func() time.Time
}

// New returns an empty exchange.
func New() *Exchange {
	return &Exchange{channels: map[string]*channel{}, groups: map[string]*group{}, now: time.Now}
}

func (e *Exchange) group(op string) *group {
	g, ok := e.groups[op]
	if !ok {
		g = &group{consumers: map[string]*consumer{}, owner: map[string]string{}}
		e.groups[op] = g
	}
	return g
}

// Configure declares channels. Existing channels keep their state so the
// controller can call this on every reconcile; new channels are created
// and channels absent from the list are left untouched.
func (e *Exchange) Configure(specs []v1alpha1.Channel) {
	e.mu.Lock()
	defer e.mu.Unlock()
	for _, s := range specs {
		if s.Partitioning.Partitions <= 0 {
			s.Partitioning.Partitions = 1
		}
		if s.Partitioning.Mode == "" {
			s.Partitioning.Mode = v1alpha1.PartitionRoundRobin
		}
		if s.To == "" {
			s.Durability = v1alpha1.DurabilityRetained
		}
		if c, ok := e.channels[s.Name]; ok {
			c.spec = s
			continue
		}
		c := &channel{
			spec:      s,
			cursor:    map[string]int{},
			epochDone: map[string]int32{},
			inflight:  map[uint64]inflight{},
		}
		c.queues = make([][]Record, s.Partitioning.Partitions)
		e.channels[s.Name] = c
	}
}

// Error carries an HTTP-style status.
type Error struct {
	Status int
	Msg    string
}

func (e *Error) Error() string { return e.Msg }

func errf(status int, format string, a ...any) error {
	return &Error{Status: status, Msg: fmt.Sprintf(format, a...)}
}

func (e *Exchange) get(name string) (*channel, error) {
	c, ok := e.channels[name]
	if !ok {
		return nil, errf(404, "channel %q not found", name)
	}
	return c, nil
}

// Produce appends records. operation must be the channel's producer.
func (e *Exchange) Produce(name, operation string, recs []Record) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	c, err := e.get(name)
	if err != nil {
		return err
	}
	if c.spec.From != "" && c.spec.From != operation {
		return errf(403, "operation %q may not produce on channel %q (producer is %q)", operation, name, c.spec.From)
	}
	if c.sealed {
		return errf(409, "channel %q is sealed", name)
	}
	for _, r := range recs {
		e.nextID++
		r.ID = e.nextID
		if c.spec.Feedback != nil {
			if r.Epoch < c.epoch {
				return errf(400, "record epoch %d is behind channel epoch %d", r.Epoch, c.epoch)
			}
			if r.Epoch >= c.spec.Feedback.MaxEpochs {
				// Beyond the loop bound: the loop terminates, the record is dropped.
				continue
			}
			if r.Epoch > c.epoch {
				c.held = append(c.held, r)
				c.produced++
				continue
			}
		}
		c.enqueue(r)
	}
	return nil
}

func (c *channel) enqueue(r Record) {
	c.produced++
	switch {
	case c.spec.To == "" || c.spec.Partitioning.Mode == v1alpha1.PartitionBroadcast:
		c.log = append(c.log, r)
	case c.spec.Partitioning.Mode == v1alpha1.PartitionHash:
		h := fnv.New32a()
		h.Write([]byte(r.Key))
		p := int(h.Sum32() % uint32(len(c.queues)))
		c.queues[p] = append(c.queues[p], r)
	default:
		p := int(c.rr % uint64(len(c.queues)))
		c.rr++
		c.queues[p] = append(c.queues[p], r)
	}
}

// Seal marks the channel complete: no further records will be produced.
func (e *Exchange) Seal(name string) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	c, err := e.get(name)
	if err != nil {
		return err
	}
	c.sealed = true
	c.held = nil
	return nil
}

// expire drops consumers of an operation that stopped polling and returns
// their in-flight records on every channel into that operation to the
// queues (at-least-once delivery).
func (e *Exchange) expire(op string) {
	g := e.group(op)
	cut := e.now().Add(-consumerTTL)
	for id, cons := range g.consumers {
		if cons.lastSeen.After(cut) {
			continue
		}
		delete(g.consumers, id)
		for k, o := range g.owner {
			if o == id {
				delete(g.owner, k)
			}
		}
		for _, c := range e.channels {
			if c.spec.To != op {
				continue
			}
			delete(c.cursor, id)
			delete(c.epochDone, id)
			for rid, f := range c.inflight {
				if f.consumer != id {
					continue
				}
				delete(c.inflight, rid)
				if f.partition >= 0 {
					c.queues[f.partition] = append([]Record{f.rec}, c.queues[f.partition]...)
				}
			}
		}
	}
}

// assigned returns the partitions the consumer may read.
//
// RoundRobin partitions have no key affinity so they are rebalanced freely
// across live consumers. Hash partitions carry key affinity so ownership is
// sticky and shared across the operation's channels: a partition stays with
// its first owner until that owner expires.
func (c *channel) assigned(g *group, id string) []int {
	ids := g.liveIDs()
	idx := sort.SearchStrings(ids, id)
	var out []int
	n := len(c.queues)
	if c.spec.Partitioning.Mode == v1alpha1.PartitionHash {
		for p := 0; p < n; p++ {
			k := ownerKey(n, p)
			if _, ok := g.owner[k]; ok {
				continue
			}
			best, bestN := "", int(^uint(0)>>1)
			for _, cid := range ids {
				cnt := 0
				for _, o := range g.owner {
					if o == cid {
						cnt++
					}
				}
				if cnt < bestN {
					best, bestN = cid, cnt
				}
			}
			g.owner[k] = best
		}
		for p := 0; p < n; p++ {
			if g.owner[ownerKey(n, p)] == id {
				out = append(out, p)
			}
		}
		return out
	}
	for p := 0; p < n; p++ {
		if p%len(ids) == idx {
			out = append(out, p)
		}
	}
	return out
}

func (c *channel) pending() int64 {
	var n int64
	for _, q := range c.queues {
		n += int64(len(q))
	}
	if c.spec.Partitioning.Mode == v1alpha1.PartitionBroadcast {
		for _, cur := range c.cursor {
			n += int64(len(c.log) - cur)
		}
	}
	return n
}

// Consume delivers up to max records to the named consumer instance.
func (e *Exchange) Consume(name, operation, id string, max int) (*ConsumeResponse, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	c, err := e.get(name)
	if err != nil {
		return nil, err
	}
	if c.spec.To != "" && c.spec.To != operation {
		return nil, errf(403, "operation %q may not consume channel %q (consumer is %q)", operation, name, c.spec.To)
	}
	g := e.group(c.spec.To)
	cons, ok := g.consumers[id]
	if !ok {
		cons = &consumer{}
		g.consumers[id] = cons
	}
	cons.lastSeen = e.now()
	if _, ok := c.cursor[id]; !ok {
		c.cursor[id] = 0
		c.epochDone[id] = -1
	}
	e.expire(c.spec.To)

	resp := &ConsumeResponse{Sealed: c.sealed, Epoch: c.epoch}
	if c.spec.Feedback != nil {
		resp.MaxEpochs = c.spec.Feedback.MaxEpochs
	}
	gate := c.spec.Delivery == v1alpha1.DeliveryMaterialized && !c.sealed
	if !gate {
		if c.spec.Partitioning.Mode == v1alpha1.PartitionBroadcast {
			for c.cursor[id] < len(c.log) && len(resp.Records) < max {
				r := c.log[c.cursor[id]]
				c.cursor[id]++
				c.inflight[r.ID] = inflight{rec: r, consumer: id, partition: -1}
				resp.Records = append(resp.Records, r)
			}
		} else {
			for _, p := range c.assigned(g, id) {
				for len(c.queues[p]) > 0 && len(resp.Records) < max {
					r := c.queues[p][0]
					c.queues[p] = c.queues[p][1:]
					c.inflight[r.ID] = inflight{rec: r, consumer: id, partition: p}
					resp.Records = append(resp.Records, r)
				}
			}
		}
	}
	quiet := c.pending() == 0 && len(c.inflight) == 0
	resp.Drained = c.sealed && quiet
	resp.Quiescent = c.spec.Feedback != nil && !c.sealed && quiet
	return resp, nil
}

// Ack marks records processed. Retained channels keep them in the log.
func (e *Exchange) Ack(name string, ids []uint64) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	c, err := e.get(name)
	if err != nil {
		return err
	}
	for _, id := range ids {
		f, ok := c.inflight[id]
		if !ok {
			continue
		}
		delete(c.inflight, id)
		if c.spec.Durability == v1alpha1.DurabilityRetained && c.spec.Partitioning.Mode != v1alpha1.PartitionBroadcast {
			c.log = append(c.log, f.rec)
		}
	}
	return nil
}

// EpochDone records that a consumer finished the given epoch of a feedback
// channel. When every live consumer has finished it and the channel is
// quiescent, the barrier advances: held records for the next epoch are
// released, or the channel is sealed when the loop bound is reached.
func (e *Exchange) EpochDone(name, id string, epoch int32) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	c, err := e.get(name)
	if err != nil {
		return err
	}
	if c.spec.Feedback == nil {
		return errf(400, "channel %q is not a feedback channel", name)
	}
	if _, ok := c.epochDone[id]; !ok {
		return errf(400, "unknown consumer %q", id)
	}
	if epoch > c.epochDone[id] {
		c.epochDone[id] = epoch
	}
	e.expire(c.spec.To)
	if c.sealed || epoch != c.epoch {
		return nil
	}
	for _, done := range c.epochDone {
		if done < c.epoch {
			return nil
		}
	}
	if c.pending() != 0 || len(c.inflight) != 0 {
		return nil
	}
	next := c.epoch + 1
	if next >= c.spec.Feedback.MaxEpochs {
		c.sealed = true
		c.held = nil
		return nil
	}
	c.epoch = next
	var rest []Record
	for _, r := range c.held {
		if r.Epoch == next {
			c.produced-- // enqueue counts it again
			c.enqueue(r)
		} else {
			rest = append(rest, r)
		}
	}
	c.held = rest
	return nil
}

// Log returns retained records of a channel (external or Retained channels).
func (e *Exchange) Log(name string) ([]Record, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	c, err := e.get(name)
	if err != nil {
		return nil, err
	}
	out := make([]Record, len(c.log))
	copy(out, c.log)
	return out, nil
}

// Metrics reports every channel.
func (e *Exchange) Metrics() []Metrics {
	e.mu.Lock()
	defer e.mu.Unlock()
	names := make([]string, 0, len(e.channels))
	for n := range e.channels {
		names = append(names, n)
	}
	sort.Strings(names)
	out := make([]Metrics, 0, len(names))
	for _, n := range names {
		c := e.channels[n]
		e.expire(c.spec.To)
		out = append(out, Metrics{
			Name: n, From: c.spec.From, To: c.spec.To,
			Sealed: c.sealed, Pending: c.pending() + int64(len(c.held)),
			InFlight: int64(len(c.inflight)), Produced: c.produced,
			Consumers: len(c.cursor), Epoch: c.epoch,
		})
	}
	return out
}
