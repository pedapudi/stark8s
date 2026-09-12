package coordinator

import (
	"context"
	"encoding/json"
	"fmt"
	"hash/fnv"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/pedapudi/stark8s/api/graph"
	"github.com/pedapudi/stark8s/pkg/storage"
)

// PodTTL is how long a pod may stay silent before it is considered gone.
// A gone pod loses its partition ownership, its fetched-but-unacknowledged
// segments are returned to the pending set, and the segments it was holding
// are marked lost.
const PodTTL = 20 * time.Second

// Error carries an HTTP-style status.
type Error struct {
	Status int
	Msg    string
}

func (e *Error) Error() string { return e.Msg }

func errf(status int, format string, a ...any) error {
	return &Error{Status: status, Msg: fmt.Sprintf(format, a...)}
}

// pod is one registered worker pod.
type pod struct {
	name        string
	op          string
	addr        string
	slots       int32
	lastSeen    time.Time
	done        bool
	incarnation string
	cohortSlot  int
}

// operation is the per-operation state: its pods and, for its consumers,
// hash partition ownership shared across every hash channel into it so that
// partition p of every such channel (with equal partition count) is owned by
// the same pod. Two inputs hashed with the same partition count are then
// co-partitioned, which a join or a loop vertex holding per-key state relies
// on.
type operation struct {
	pods      map[string]*pod
	epochDone map[string]int32
	// owner maps "partitions/p" -> pod name.
	owner map[string]string
	// pinned holds the owner keys whose owner has actually been handed a
	// segment. A consumer accumulates in-memory state for the partitions it
	// has processed, so a pinned partition stays with its owner for as long
	// as that owner is alive; an unpinned one carries no state anywhere and
	// is redistributed freely as the pod pool grows.
	pinned map[string]bool
	// replicas is the replica count the controller last published for this
	// operation. Zero means the controller has not published one yet.
	replicas int32
	// retired records replaced process incarnations so a delayed heartbeat
	// cannot replace the active process again.
	retired         map[string]map[string]bool
	nextCohortSlot  int
	freeCohortSlots []int
	// completed latches completion so an operation scaled to zero stays
	// complete. It is set once inputs are drained and every live pod has
	// reported done, and cleared as soon as an inbound channel has runnable
	// work again, so late input restarts the operation.
	completed bool
}

func (o *operation) liveIDs() []string {
	ids := make([]string, 0, len(o.pods))
	for id := range o.pods {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	return ids
}

func ownerKey(partitions, p int) string { return fmt.Sprintf("%d/%d", partitions, p) }

// balanceHash spreads the partitions of the operation's hash channels with
// partition count n over its live pods.
//
// A partition that has been handed work is pinned: its owner may hold state
// derived from the records it processed, so moving it would silently corrupt
// the result and it is left where it is until the owner expires. Every other
// partition carries no state anywhere, so it is (re)assigned here, least
// loaded pod first, counting pinned partitions as load already carried. A
// pod that registers after the first one therefore still receives a share
// instead of finding every partition taken.
//
// The result is a function of the live pods and the pinned partitions alone,
// so repeated calls that change neither return the same assignment and
// ownership does not flap. Balancing is per partition count because the
// assignment for one count must not depend on the assignment for another,
// which is what makes that fixed point reachable; two hash channels with
// equal partition counts into one operation share these keys and so stay
// co-partitioned.
func (o *operation) balanceHash(n int) {
	ids := o.liveIDs()
	if len(ids) == 0 {
		return
	}
	live := make(map[string]bool, len(ids))
	for _, id := range ids {
		live[id] = true
	}
	load := make(map[string]int, len(ids))
	var free []int
	for p := 0; p < n; p++ {
		k := ownerKey(n, p)
		owner, owned := o.owner[k]
		if owned && !live[owner] {
			delete(o.owner, k)
			delete(o.pinned, k)
			owned = false
		}
		if owned && o.pinned[k] {
			load[owner]++
			continue
		}
		free = append(free, p)
	}
	for _, p := range free {
		best := ids[0]
		for _, id := range ids[1:] {
			if load[id] < load[best] {
				best = id
			}
		}
		o.owner[ownerKey(n, p)] = best
		load[best]++
	}
}

// segment is the coordinator's record of one announced segment.
type segment struct {
	id       string
	holder   string
	producer string
	// op is the operation whose pod holds the segment (empty when the
	// coordinator holds it).
	op       string
	channel  string
	part     int32
	epoch    int32
	records  int64
	bytes    int64
	durable  bool
	task     TaskID
	appendID string
	offset   int64
	// delivered is the set of consumer pods that fetched (or were told to
	// fetch) the segment and have not acknowledged it yet.
	delivered map[string]bool
	// acked is the set of consumer pods that acknowledged it.
	acked map[string]bool
	// retryAfter delays a returned delivery for the process that returned it.
	retryAfter map[string]time.Time
	// lost: the holder expired before every consumer acknowledged it.
	lost bool
	// released: nothing needs the segment any more (fully acknowledged,
	// dropped at a seal, or lost). Ephemeral released segments are reported
	// to the holder for deletion once and then forgotten.
	released         bool
	retentionDeleted bool
	reported         bool
	// data is set for segments the coordinator itself holds (external
	// producers).
	data []Record
}

func (s *segment) key() string { return s.holder + "/" + s.id }

type channel struct {
	spec graph.Channel

	sealed                bool
	produced              int64
	overflowed            int64
	lost                  int64
	acknowledged          int64
	latestDeliveryFailure string
	epoch                 int32
	finiteEpochs          bool
	maxEpochs             int32
	productionClosed      map[int32]bool
	rr                    uint64

	// queues holds pending (undelivered) segments per partition for Hash
	// and RoundRobin channels.
	queues [][]*segment
	// log is every segment of a Broadcast channel in arrival order; each
	// consumer pod reads it through its own cursor.
	log    []*segment
	cursor map[string]int
	// inflight holds delivered-but-unacknowledged segments keyed by
	// segment key.
	inflight map[string]*segment
	// held holds Synchronous feedback segments of epochs later than the
	// current one.
	held []*segment
	// epochDone is the last epoch each consumer pod reported finished.
	epochDone map[string]int32
	// records is the retained record log of a channel with no consumer.
	records       []Record
	history       [][]*segment
	historyBase   []int64
	appendIDs     map[string]*segment
	subscriptions map[string]*subscription
	// all indexes every segment of the channel that has not been forgotten.
	all map[string]*segment
}

type subscription struct {
	name      string
	operation string
	positions []int64
	inflight  map[int32]subscriptionDelivery
}

type subscriptionDelivery struct {
	offset   int64
	appendID string
	owner    string
}

func (c *channel) partitions() int { return int(c.spec.Partitioning.Partitions) }

func (c *channel) external() bool { return c.spec.To == "" }

func (c *channel) broadcast() bool {
	return c.spec.Partitioning.Mode == graph.PartitionBroadcast
}

func (c *channel) feedbackMode() graph.FeedbackMode {
	if c.spec.Feedback == nil {
		return ""
	}
	if c.spec.Feedback.Mode == graph.FeedbackAsynchronous {
		return graph.FeedbackAsynchronous
	}
	return graph.FeedbackSynchronous
}

func (c *channel) synchronous() bool { return c.feedbackMode() == graph.FeedbackSynchronous }

// Coordinator holds the control-plane state of one workload.
type Coordinator struct {
	mu       sync.Mutex
	channels map[string]*channel
	ops      map[string]*operation
	// selfAddr is host:port of the coordinator's own segment server, used as
	// holder of segments stored for external producers.
	selfAddr string
	nextID   uint64
	now      func() time.Time
	// wake is closed and replaced whenever a retained record log grows, to
	// release long-polling readers.
	wake          chan struct{}
	durable       *storage.Writer
	tombstoneKeys map[string]bool
	failed        error
}

func (co *Coordinator) failure() error {
	co.mu.Lock()
	defer co.mu.Unlock()
	return co.failed
}

// NewDurable claims the coordinator checkpoint and restores its last state.
// Claiming a checkpoint fences a previous coordinator writer.
func NewDurable(ctx context.Context, selfAddr string, store storage.Store, key, writerID string) (*Coordinator, error) {
	w, checkpoint, err := storage.Claim(ctx, store, key, writerID)
	if err != nil {
		return nil, err
	}
	co := New(selfAddr)
	co.durable = w
	co.tombstoneKeys = map[string]bool{}
	if len(checkpoint.State) > 0 {
		if err := co.restore(checkpoint.State); err != nil {
			return nil, err
		}
		co.rebindHeldSegments(selfAddr)
		co.selfAddr = selfAddr
	}
	for _, id := range checkpoint.Tombstones {
		if err := w.Delete(ctx, "segments/"+id); err != nil {
			return nil, fmt.Errorf("delete released segment %q: %w", id, err)
		}
	}
	if len(checkpoint.Tombstones) > 0 {
		if err := co.commitLocked(); err != nil {
			return nil, fmt.Errorf("clear released segment tombstones: %w", err)
		}
	}
	return co, nil
}

// rebindHeldSegments makes coordinator-owned records available from a
// replacement coordinator address.
func (co *Coordinator) rebindHeldSegments(selfAddr string) {
	for _, c := range co.channels {
		for key, s := range c.all {
			if s.data == nil || s.holder == selfAddr {
				continue
			}
			delete(c.all, key)
			if c.inflight[key] == s {
				delete(c.inflight, key)
				c.inflight[selfAddr+"/"+s.id] = s
			}
			s.holder = selfAddr
			c.all[s.key()] = s
		}
	}
}

// New returns an empty coordinator whose own segment server is reachable at
// selfAddr (host:port).
func New(selfAddr string) *Coordinator {
	return &Coordinator{
		channels:      map[string]*channel{},
		ops:           map[string]*operation{},
		selfAddr:      selfAddr,
		now:           time.Now,
		wake:          make(chan struct{}),
		tombstoneKeys: map[string]bool{},
	}
}

func (co *Coordinator) op(name string) *operation {
	o, ok := co.ops[name]
	if !ok {
		o = &operation{pods: map[string]*pod{}, owner: map[string]string{}, pinned: map[string]bool{}, retired: map[string]map[string]bool{}, epochDone: map[string]int32{}}
		co.ops[name] = o
	}
	return o
}

func (co *Coordinator) get(name string) (*channel, error) {
	c, ok := co.channels[name]
	if !ok {
		return nil, errf(404, "channel %q not found", name)
	}
	return c, nil
}

// Configure declares channels. Existing channels keep their state so the
// controller can call this on every reconcile; new channels are created and
// channels absent from the list are left untouched.
func (co *Coordinator) Configure(specs []graph.Channel) error {
	co.mu.Lock()
	defer co.mu.Unlock()
	for _, s := range specs {
		if s.Partitioning.Partitions <= 0 {
			s.Partitioning.Partitions = 1
		}
		if s.Partitioning.Mode == "" {
			s.Partitioning.Mode = graph.PartitionRoundRobin
		}
		if s.To == "" {
			s.Durability = graph.DurabilityRetained
		}
		if s.Feedback != nil && s.Feedback.Mode == "" {
			s.Feedback.Mode = graph.FeedbackSynchronous
		}
		if c, ok := co.channels[s.Name]; ok {
			c.spec = s
			continue
		}
		c := &channel{
			spec:             s,
			cursor:           map[string]int{},
			inflight:         map[string]*segment{},
			epochDone:        map[string]int32{},
			all:              map[string]*segment{},
			productionClosed: map[int32]bool{},
			appendIDs:        map[string]*segment{},
			subscriptions:    map[string]*subscription{},
		}
		c.queues = make([][]*segment, s.Partitioning.Partitions)
		c.history = make([][]*segment, s.Partitioning.Partitions)
		c.historyBase = make([]int64, s.Partitioning.Partitions)
		co.channels[s.Name] = c
		if s.From != "" {
			co.op(s.From)
		}
		if s.To != "" {
			co.op(s.To)
		}
	}
	co.markFiniteEpochChannels()
	return co.commitLocked()
}

// markFiniteEpochChannels marks every channel in a graph component containing
// Synchronous feedback. Feedback contributes records to the next scalar epoch;
// every other edge preserves the epoch.
func (co *Coordinator) markFiniteEpochChannels() {
	finiteOps := map[string]bool{}
	for _, c := range co.channels {
		if c.synchronous() {
			finiteOps[c.spec.From], finiteOps[c.spec.To] = true, true
			c.productionClosed[0] = true // loop input for epoch zero comes from non-feedback edges
		}
	}
	for changed := true; changed; {
		changed = false
		for _, c := range co.channels {
			if finiteOps[c.spec.From] || finiteOps[c.spec.To] {
				if c.spec.From != "" && !finiteOps[c.spec.From] {
					finiteOps[c.spec.From], changed = true, true
				}
				if c.spec.To != "" && !finiteOps[c.spec.To] {
					finiteOps[c.spec.To], changed = true, true
				}
			}
		}
	}
	for _, c := range co.channels {
		c.finiteEpochs = finiteOps[c.spec.From] || finiteOps[c.spec.To]
		if c.finiteEpochs {
			c.maxEpochs = co.componentMaxEpochs(c)
		}
	}
}

func (co *Coordinator) componentMaxEpochs(start *channel) int32 {
	seen := map[string]bool{}
	if start.spec.From != "" {
		seen[start.spec.From] = true
	}
	if start.spec.To != "" {
		seen[start.spec.To] = true
	}
	for changed := true; changed; {
		changed = false
		for _, c := range co.channels {
			if seen[c.spec.From] || seen[c.spec.To] {
				if c.spec.From != "" && !seen[c.spec.From] {
					seen[c.spec.From], changed = true, true
				}
				if c.spec.To != "" && !seen[c.spec.To] {
					seen[c.spec.To], changed = true, true
				}
			}
		}
	}
	var max int32
	for _, c := range co.channels {
		if c.synchronous() && seen[c.spec.From] && c.spec.Feedback.MaxEpochs > max {
			max = c.spec.Feedback.MaxEpochs
		}
	}
	return max
}

// SetOperations records the replica count the controller wants for each
// operation. A Broadcast channel is finished with only when every replica of
// its consumer has acknowledged it, and this is where that number comes from.
func (co *Coordinator) SetOperations(specs []OperationSpec) {
	co.mu.Lock()
	defer co.mu.Unlock()
	for _, s := range specs {
		if s.Name == "" {
			continue
		}
		co.op(s.Name).replicas = s.Replicas
	}
}

// Topology returns the declared channels.
func (co *Coordinator) Topology() []graph.Channel {
	co.mu.Lock()
	defer co.mu.Unlock()
	out := make([]graph.Channel, 0, len(co.channels))
	for _, n := range co.channelNames() {
		out = append(out, co.channels[n].spec)
	}
	return out
}

func (co *Coordinator) channelNames() []string {
	names := make([]string, 0, len(co.channels))
	for n := range co.channels {
		names = append(names, n)
	}
	sort.Strings(names)
	return names
}

// --- pods -----------------------------------------------------------------

// Register records a pod as alive. It is also the heartbeat.
func (co *Coordinator) Register(reg PodRegistration) error {
	if reg.Operation == "" || reg.Pod == "" {
		return errf(400, "operation and pod are required")
	}
	co.mu.Lock()
	defer co.mu.Unlock()
	co.expireAllExcept(reg.Operation, reg.Pod)
	o := co.op(reg.Operation)
	if retired := o.retired[reg.Pod]; reg.Incarnation != "" && retired[reg.Incarnation] {
		return errf(409, "incarnation %q for pod %q has been replaced", reg.Incarnation, reg.Pod)
	}
	if p := o.pods[reg.Pod]; p != nil && p.incarnation != reg.Incarnation {
		if p.incarnation != "" && reg.Incarnation == "" {
			return errf(409, "pod %q requires an incarnation", reg.Pod)
		}
		if p.incarnation != "" {
			if o.retired[reg.Pod] == nil {
				o.retired[reg.Pod] = map[string]bool{}
			}
			o.retired[reg.Pod][p.incarnation] = true
		}
		co.returnConsumerDeliveries(reg.Operation, o, reg.Pod, p.incarnation, false)
		p.done = false
		p.incarnation = reg.Incarnation
		o.epochDone[reg.Pod] = -1
		o.completed = false
	}
	p := co.touch(reg.Operation, reg.Pod, reg.Addr, reg.Slots)
	p.incarnation = reg.Incarnation
	if _, ok := o.epochDone[p.name]; !ok {
		o.epochDone[p.name] = -1
	}
	return co.commitLocked()
}

// touch refreshes a pod's liveness, creating it when unknown.
func (co *Coordinator) touch(opName, podName, addr string, slots int32) *pod {
	o := co.op(opName)
	p, ok := o.pods[podName]
	if !ok {
		slot := o.nextCohortSlot
		if len(o.freeCohortSlots) > 0 {
			slot = o.freeCohortSlots[0]
			o.freeCohortSlots = o.freeCohortSlots[1:]
		} else {
			o.nextCohortSlot++
		}
		p = &pod{name: podName, op: opName, cohortSlot: slot}
		o.pods[podName] = p
	}
	if addr != "" {
		p.addr = addr
	}
	if slots > 0 {
		p.slots = slots
	}
	p.lastSeen = co.now()
	return p
}

// SourceDone records that a pod has emitted everything it will emit: a
// source pod after its source ran, a consumer pod after it drained.
func (co *Coordinator) SourceDone(reg PodRegistration) error {
	if reg.Operation == "" || reg.Pod == "" {
		return errf(400, "operation and pod are required")
	}
	co.mu.Lock()
	defer co.mu.Unlock()
	p, err := co.requireIncarnation(reg.Operation, reg.Pod, reg.Incarnation)
	if err != nil {
		return err
	}
	p.done = true
	if !co.hasInbound(reg.Operation) {
		if err := co.recordOperationEpochDone(reg.Operation, reg.Pod, 0); err != nil {
			return err
		}
	}
	return co.commitLocked()
}

func (co *Coordinator) requireIncarnation(opName, podName, incarnation string) (*pod, error) {
	p := co.op(opName).pods[podName]
	if p == nil {
		return nil, errf(409, "pod %q is not registered", podName)
	}
	if p.incarnation != incarnation {
		return nil, errf(409, "incarnation for pod %q is no longer active", podName)
	}
	return p, nil
}

// Released returns the IDs of Ephemeral segments held by the pod that no
// consumer needs any more, and forgets them.
func (co *Coordinator) Released(podName string) []string {
	out, _ := co.ReleasedSession("", podName, "")
	return out
}

// ReleasedCommitted is the legacy durable deletion API. Session-aware callers
// use ReleasedSession so a replaced process cannot delete segment bytes.
func (co *Coordinator) ReleasedCommitted(podName string) ([]string, error) {
	return co.ReleasedSession("", podName, "")
}

// ReleasedSession returns deletable segments after verifying the holder's
// active process. Process replacement does not discard holder state because
// the pod's segment volume may survive a container restart.
func (co *Coordinator) ReleasedSession(opName, podName, incarnation string) ([]string, error) {
	co.mu.Lock()
	defer co.mu.Unlock()
	if opName != "" {
		if _, err := co.requireIncarnation(opName, podName, incarnation); err != nil {
			return nil, err
		}
	}
	co.expireAll()
	var out []string
	type removal struct {
		channel *channel
		key     string
		segment *segment
	}
	var removals []removal
	for _, c := range co.channels {
		for k, s := range c.all {
			if c.spec.Durability == graph.DurabilityRetained && !s.retentionDeleted {
				continue
			}
			if s.producer != podName || !s.released || s.data != nil {
				continue
			}
			out = append(out, s.id)
			co.tombstoneKeys[s.id] = true
			removals = append(removals, removal{c, k, s})
			delete(c.all, k)
		}
	}
	sort.Strings(out)
	if out == nil {
		out = []string{}
	}
	if err := co.commitLocked(); err != nil {
		for _, r := range removals {
			r.channel.all[r.key] = r.segment
			delete(co.tombstoneKeys, r.segment.id)
		}
		return nil, err
	}
	return out, nil
}

// expireAll drops every pod that stopped heartbeating.
func (co *Coordinator) expireAll() {
	co.expireAllExcept("", "")
}

func (co *Coordinator) expireAllExcept(skipOperation, skipPod string) {
	cut := co.now().Add(-PodTTL)
	for opName, o := range co.ops {
		for id, p := range o.pods {
			if opName == skipOperation && id == skipPod {
				continue
			}
			if p.lastSeen.After(cut) {
				continue
			}
			delete(o.pods, id)
			o.freeCohortSlots = append(o.freeCohortSlots, p.cohortSlot)
			sort.Ints(o.freeCohortSlots)
			co.expireConsumer(opName, o, id)
			co.expireHolder(id)
		}
	}
}

// expireConsumer releases partition ownership of a gone consumer pod and
// returns its unacknowledged segments on every channel into its operation to
// the pending set (at-least-once delivery).
func (co *Coordinator) expireConsumer(opName string, o *operation, id string) {
	co.returnConsumerDeliveries(opName, o, id, "", true)
}

// returnConsumerDeliveries releases ownership and returns unfinished work for
// one process. A process replacement passes its incarnation; expiry returns
// every delivery for the pod.
func (co *Coordinator) returnConsumerDeliveries(opName string, o *operation, id, incarnation string, all bool) {
	for k, owner := range o.owner {
		if owner == id {
			delete(o.owner, k)
			delete(o.pinned, k)
		}
	}
	for _, c := range co.channels {
		for _, sub := range c.subscriptions {
			if sub.operation != opName {
				continue
			}
			for partition, delivery := range sub.inflight {
				if deliveryPod(delivery.owner) == id && (all || delivery.owner == deliveryKey(id, incarnation)) {
					delete(sub.inflight, partition)
				}
			}
		}
		if c.spec.To != opName {
			continue
		}
		delete(c.cursor, id)
		delete(c.epochDone, id)
		delivery := deliveryKey(id, incarnation)
		for key, s := range c.inflight {
			owned := s.delivered[delivery]
			if all && !owned {
				for k := range s.delivered {
					if deliveryPod(k) == id {
						delivery = k
						owned = true
						break
					}
				}
			}
			if !owned {
				continue
			}
			delete(s.delivered, delivery)
			if c.broadcast() {
				if len(s.delivered) == 0 {
					delete(c.inflight, key)
				}
				continue
			}
			delete(c.inflight, key)
			if !s.lost {
				c.queues[s.part] = append([]*segment{s}, c.queues[s.part]...)
			}
		}
	}
	delete(o.epochDone, id)
}

func deliveryKey(podName, incarnation string) string { return podName + "\x00" + incarnation }
func deliveryPod(key string) string {
	for i := range key {
		if key[i] == 0 {
			return key[:i]
		}
	}
	return key
}

// expireHolder marks every segment a gone pod was holding as lost unless it
// was already released. A lost segment is removed from the pending and
// in-flight sets so the consuming operation is not blocked; the loss is
// reported in ChannelMetrics.Lost.
//
// Limitation: the producing task is not re-executed. Recovering the lost
// records requires either Retained input to the producer and a replay, or a
// lineage re-execution, neither of which the coordinator drives.
func (co *Coordinator) expireHolder(podName string) {
	for _, c := range co.channels {
		for _, s := range c.all {
			if s.producer != podName || s.durable || s.released || s.lost {
				continue
			}
			co.markLost(c, s)
		}
	}
}

func (co *Coordinator) markLost(c *channel, s *segment) {
	s.lost = true
	s.released = true
	c.lost += s.records
	delete(c.inflight, s.key())
	s.delivered = map[string]bool{}
	if !c.broadcast() {
		c.queues[s.part] = removeSegment(c.queues[s.part], s)
	}
	c.held = removeSegment(c.held, s)
}

func removeSegment(list []*segment, s *segment) []*segment {
	out := list[:0]
	for _, x := range list {
		if x != s {
			out = append(out, x)
		}
	}
	return out
}

// --- producing ------------------------------------------------------------

// mayProduce says whether an operation may announce segments on a channel:
// the declared producer, anyone when the producer is external, or the
// operation of an Asynchronous loop diverting records to the channel named
// as the loop's Overflow.
func (co *Coordinator) mayProduce(c *channel, opName string) bool {
	if c.spec.From == "" || c.spec.From == opName {
		return true
	}
	for _, other := range co.channels {
		if other.spec.Feedback != nil && other.spec.Feedback.Overflow == c.spec.Name && other.spec.From == opName {
			return true
		}
	}
	return false
}

// Announce indexes segments produced on the channel.
func (co *Coordinator) Announce(name, opName string, anns []SegmentAnnouncement) error {
	return co.AnnounceSession(name, opName, "", "", anns)
}

// AnnounceSession indexes segments after verifying the producing process.
func (co *Coordinator) AnnounceSession(name, opName, podName, incarnation string, anns []SegmentAnnouncement) error {
	co.mu.Lock()
	defer co.mu.Unlock()
	if err := co.requireDurableWriterLocked(); err != nil {
		return err
	}
	c, err := co.get(name)
	if err != nil {
		return err
	}
	if !co.mayProduce(c, opName) {
		return errf(403, "operation %q may not produce on channel %q (producer is %q)", opName, name, c.spec.From)
	}
	if podName != "" {
		if _, err := co.requireIncarnation(opName, podName, incarnation); err != nil {
			return err
		}
	}
	hasNew := false
	for _, a := range anns {
		if a.ID == "" {
			hasNew = true
			continue
		}
		if a.Channel != "" && a.Channel != name {
			return errf(400, "segment %q announced for channel %q on channel %q", a.ID, a.Channel, name)
		}
		appendID := a.AppendID
		if appendID == "" {
			appendID = a.Holder + "/" + a.ID
		}
		if prior := c.appendIDs[appendID]; prior != nil && !sameAppend(prior, a, name) {
			return errf(409, "append %q already committed with different metadata", appendID)
		}
		if c.appendIDs[appendID] != nil {
			continue
		}
		hasNew = true
		if c.sealed {
			return errf(409, "channel %q is sealed", name)
		}
		if c.finiteEpochs && a.Epoch < c.epoch {
			return errf(400, "segment epoch %d is behind channel epoch %d", a.Epoch, c.epoch)
		}
		if !c.broadcast() && (a.Partition < 0 || int(a.Partition) >= c.partitions()) {
			return errf(400, "partition %d out of range for channel %q", a.Partition, name)
		}
	}
	if !hasNew {
		return nil
	}
	for _, a := range anns {
		if a.ID == "" {
			c.overflowed += a.Overflowed
			continue
		}
		appendID := a.AppendID
		if appendID == "" {
			appendID = a.Holder + "/" + a.ID
		}
		if c.appendIDs[appendID] != nil {
			continue
		}
		c.overflowed += a.Overflowed
		s := &segment{
			id: a.ID, holder: a.Holder, producer: a.Producer, channel: name,
			part: a.Partition, epoch: a.Epoch, records: a.Records, bytes: a.Bytes, durable: a.Durable || durableHolder(a.Holder), task: a.Task,
			appendID: appendID, delivered: map[string]bool{}, acked: map[string]bool{}, retryAfter: map[string]time.Time{},
		}
		if a.Producer == "" {
			s.producer = opName
		}
		s.op = opName
		if s.op == "" {
			s.op = c.spec.From
		}
		co.index(c, s)
	}
	co.settle()
	return co.commitLocked()
}

// index adds a segment to the channel's structures.
func (co *Coordinator) index(c *channel, s *segment) {
	if s.appendID == "" {
		s.appendID = s.producer + "/" + s.id
	}
	if _, dup := c.all[s.key()]; dup {
		return
	}
	c.all[s.key()] = s
	c.appendIDs[s.appendID] = s
	partition := int(s.part)
	if c.broadcast() {
		partition = 0
	}
	s.offset = c.historyBase[partition] + int64(len(c.history[partition]))
	c.history[partition] = append(c.history[partition], s)
	c.produced += s.records
	if c.spec.Feedback != nil && s.epoch >= c.spec.Feedback.MaxEpochs {
		// Beyond the loop bound (Synchronous loops): the record set is
		// dropped; the holder may delete it.
		c.produced -= s.records
		s.released = true
		return
	}
	if c.finiteEpochs && s.epoch > c.epoch {
		c.held = append(c.held, s)
		return
	}
	c.enqueue(s)
}

func sameAppend(s *segment, a SegmentAnnouncement, channel string) bool {
	producer := a.Producer
	if producer == "" {
		producer = s.op
	}
	return s.id == a.ID && s.holder == a.Holder && s.producer == producer && s.channel == channel && s.part == a.Partition && s.epoch == a.Epoch && s.records == a.Records && s.bytes == a.Bytes && s.durable == (a.Durable || durableHolder(a.Holder)) && s.task == a.Task
}

func durableHolder(holder string) bool {
	return strings.HasPrefix(holder, "http://") || strings.HasPrefix(holder, "https://")
}

func (c *channel) enqueue(s *segment) {
	if c.broadcast() {
		c.log = append(c.log, s)
		return
	}
	c.queues[s.part] = append(c.queues[s.part], s)
}

// partitionOf computes the partition of a record the way producer pods do.
func (c *channel) partitionOf(r Record) int32 {
	switch c.spec.Partitioning.Mode {
	case graph.PartitionBroadcast:
		return 0
	case graph.PartitionHash:
		return int32(HashPartition(r.Key, c.partitions()))
	default:
		p := int32(c.rr % uint64(c.partitions()))
		c.rr++
		return p
	}
}

// HashPartition is the partition of key under Hash partitioning. Producer
// pods and the coordinator must agree on it.
func HashPartition(key string, partitions int) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() % uint32(partitions))
}

// Produce stores records on the coordinator. For a channel with no consumer
// they go to the retained log; otherwise (external producer) they become
// segments served by the coordinator's own segment server.
func (co *Coordinator) Produce(name, opName string, recs []Record) error {
	co.mu.Lock()
	defer co.mu.Unlock()
	c, err := co.get(name)
	if err != nil {
		return err
	}
	if !co.mayProduce(c, opName) {
		return errf(403, "operation %q may not produce on channel %q (producer is %q)", opName, name, c.spec.From)
	}
	if c.sealed {
		return errf(409, "channel %q is sealed", name)
	}
	if c.external() {
		c.records = append(c.records, recs...)
		c.produced += int64(len(recs))
		close(co.wake)
		co.wake = make(chan struct{})
		return co.commitLocked()
	}
	type pe struct {
		p int32
		e int32
	}
	groups := map[pe][]Record{}
	var order []pe
	for _, r := range recs {
		k := pe{c.partitionOf(r), r.Epoch}
		if _, ok := groups[k]; !ok {
			order = append(order, k)
		}
		groups[k] = append(groups[k], r)
	}
	for _, k := range order {
		if c.finiteEpochs && k.e < c.epoch {
			return errf(400, "record epoch %d is behind channel epoch %d", k.e, c.epoch)
		}
	}
	for _, k := range order {
		co.nextID++
		id := fmt.Sprintf("ext-%d", co.nextID)
		if co.durable != nil {
			body, marshalErr := json.Marshal(groups[k])
			if marshalErr != nil {
				return marshalErr
			}
			if putErr := co.durable.PutImmutable(context.Background(), "segments/"+id, body); putErr != nil {
				return putErr
			}
		}
		s := &segment{
			id: id, appendID: "coordinator/" + id, holder: co.selfAddr, producer: "coordinator",
			channel: name, part: k.p, epoch: k.e, records: int64(len(groups[k])),
			delivered: map[string]bool{}, acked: map[string]bool{}, data: groups[k],
		}
		co.index(c, s)
	}
	co.settle()
	return co.commitLocked()
}

// Segment returns the records of a segment the coordinator holds.
func (co *Coordinator) Segment(id string) ([]Record, bool) {
	co.mu.Lock()
	defer co.mu.Unlock()
	key := co.selfAddr + "/" + id
	for _, c := range co.channels {
		if s, ok := c.all[key]; ok && s.data != nil {
			return s.data, true
		}
	}
	return nil, false
}

// Seal marks the channel complete: no further segments will be announced.
func (co *Coordinator) Seal(name string) error {
	co.mu.Lock()
	defer co.mu.Unlock()
	c, err := co.get(name)
	if err != nil {
		return err
	}
	co.seal(c)
	co.settle()
	return co.commitLocked()
}

func (co *Coordinator) seal(c *channel) {
	c.sealed = true
	c.productionClosed[c.epoch] = true
	if c.synchronous() {
		for _, s := range c.held {
			s.released = true
		}
		c.held = nil
	}
	close(co.wake)
	co.wake = make(chan struct{})
}

// --- consuming ------------------------------------------------------------

// assigned returns the partitions the pod may read.
//
// RoundRobin partitions have no key affinity so they are rebalanced freely
// across live pods. Hash partitions carry key affinity so ownership is
// sticky and shared across the operation's channels, but only once the owner
// has been handed work for the partition: see balanceHash.
func (c *channel) assigned(o *operation, id string) []int {
	var out []int
	n := c.partitions()
	if c.spec.Partitioning.Mode == graph.PartitionHash {
		o.balanceHash(n)
		for p := 0; p < n; p++ {
			if o.owner[ownerKey(n, p)] == id {
				out = append(out, p)
			}
		}
		return out
	}
	ids := o.liveIDs()
	if len(ids) == 0 {
		return nil
	}
	idx := sort.SearchStrings(ids, id)
	for p := 0; p < n; p++ {
		if p%len(ids) == idx {
			out = append(out, p)
		}
	}
	return out
}

// pendingByPartition counts records in undelivered segments of the current
// epoch. For Broadcast channels partition 0 carries the sum over consumer
// pods of what each has not yet read (or everything when no pod is known).
func (c *channel) pendingByPartition() []int64 {
	if c.broadcast() {
		var n int64
		if len(c.cursor) == 0 {
			for _, s := range c.log {
				if !s.lost && !s.released {
					n += s.records
				}
			}
			return []int64{n}
		}
		for _, cur := range c.cursor {
			for _, s := range c.log[cur:] {
				if !s.lost && !s.released {
					n += s.records
				}
			}
		}
		return []int64{n}
	}
	out := make([]int64, len(c.queues))
	for p, q := range c.queues {
		for _, s := range q {
			out[p] += s.records
		}
	}
	return out
}

func (c *channel) pending() int64 {
	var n int64
	for _, v := range c.pendingByPartition() {
		n += v
	}
	return n
}

func (c *channel) heldRecords() int64 {
	var n int64
	for _, s := range c.held {
		n += s.records
	}
	return n
}

func (c *channel) inflightRecords() int64 {
	var n int64
	for _, s := range c.inflight {
		if c.broadcast() {
			n += s.records * int64(len(s.delivered))
		} else {
			n += s.records
		}
	}
	return n
}

// quiet: nothing pending or in flight at the current epoch.
func (c *channel) quiet() bool { return c.pending() == 0 && len(c.inflight) == 0 }

func (c *channel) gated() bool {
	if c.spec.Delivery != graph.DeliveryMaterialized {
		return false
	}
	if c.finiteEpochs {
		return !c.productionClosed[c.epoch]
	}
	return !c.sealed
}

// Consume returns up to max segments pending on the partitions the pod owns.
func (co *Coordinator) Consume(name, opName, podName string, max int) (*ConsumeResponse, error) {
	return co.ConsumeSession(name, opName, podName, "", max)
}

// ConsumeSession returns work after verifying the consuming process.
func (co *Coordinator) ConsumeSession(name, opName, podName, incarnation string, max int) (*ConsumeResponse, error) {
	co.mu.Lock()
	defer co.mu.Unlock()
	c, err := co.get(name)
	if err != nil {
		return nil, err
	}
	if c.external() {
		return nil, errf(400, "channel %q has no consuming operation; read it with GET records", name)
	}
	if opName != "" && c.spec.To != opName {
		return nil, errf(403, "operation %q may not consume channel %q (consumer is %q)", opName, name, c.spec.To)
	}
	if podName == "" {
		return nil, errf(400, "pod is required")
	}
	before, snapshotErr := json.Marshal(co.snapshot())
	if snapshotErr != nil {
		return nil, snapshotErr
	}
	o := co.op(c.spec.To)
	p, err := co.requireIncarnation(c.spec.To, podName, incarnation)
	if err != nil {
		// Legacy callers historically registered implicitly through consume.
		if incarnation != "" || o.pods[podName] != nil {
			return nil, err
		}
		p = co.touch(c.spec.To, podName, "", 0)
	}
	p.lastSeen = co.now()
	if _, ok := c.cursor[podName]; !ok {
		c.cursor[podName] = 0
		c.epochDone[podName] = -1
	}
	co.expireAll()
	co.settle()

	resp := &ConsumeResponse{Sealed: c.sealed, Epoch: c.epoch, Mode: c.feedbackMode(), FiniteEpochs: c.finiteEpochs, ProductionClosed: c.productionClosed[c.epoch]}
	if c.finiteEpochs {
		resp.MaxEpochs = c.maxEpochs
	} else if c.spec.Feedback != nil {
		resp.MaxEpochs = c.spec.Feedback.MaxEpochs
	}
	n := 0
	if !c.gated() {
		if c.broadcast() {
			work := PartitionWork{Partition: 0}
			for c.cursor[podName] < len(c.log) && n < max {
				s := c.log[c.cursor[podName]]
				if co.now().Before(s.retryAfter[deliveryKey(podName, incarnation)]) {
					break
				}
				c.cursor[podName]++
				if s.lost || s.released || s.acked[cohortKey(p)] {
					continue
				}
				s.delivered[deliveryKey(podName, incarnation)] = true
				c.inflight[s.key()] = s
				work.Segments = append(work.Segments, ref(s))
				n++
			}
			if len(work.Segments) > 0 {
				resp.Work = append(resp.Work, work)
			}
		} else {
			for _, p := range c.assigned(o, podName) {
				work := PartitionWork{Partition: int32(p)}
				for len(c.queues[p]) > 0 && n < max {
					s := c.queues[p][0]
					if co.now().Before(s.retryAfter[deliveryKey(podName, incarnation)]) {
						break
					}
					c.queues[p] = c.queues[p][1:]
					s.delivered[deliveryKey(podName, incarnation)] = true
					c.inflight[s.key()] = s
					work.Segments = append(work.Segments, ref(s))
					n++
				}
				if len(work.Segments) > 0 {
					// The pod is about to process records of this partition
					// and may keep state derived from them, so its ownership
					// stops being provisional here.
					if c.spec.Partitioning.Mode == graph.PartitionHash {
						o.pinned[ownerKey(c.partitions(), p)] = true
					}
					resp.Work = append(resp.Work, work)
				}
			}
		}
	}
	quiet := c.quiet()
	resp.Drained = c.sealed && quiet
	resp.Quiescent = c.finiteEpochs && resp.ProductionClosed && quiet
	if err := co.commitLocked(); err != nil {
		if restoreErr := co.restore(before); restoreErr != nil {
			return nil, fmt.Errorf("commit failed: %v; rollback failed: %w", err, restoreErr)
		}
		return nil, err
	}
	return resp, nil
}

func ref(s *segment) SegmentRef {
	return SegmentRef{ID: s.id, AppendID: s.appendID, Holder: s.holder, Records: s.records, Bytes: s.bytes, Epoch: s.epoch, Offset: s.offset}
}

// Ack marks segments processed by the acknowledging pods.
func (co *Coordinator) Ack(name string, acks []SegmentAck) error {
	return co.AckSession(name, "", "", "", acks)
}

// AckSession marks deliveries processed by the active process.
func (co *Coordinator) AckSession(name, opName, podName, incarnation string, acks []SegmentAck) error {
	co.mu.Lock()
	defer co.mu.Unlock()
	c, err := co.get(name)
	if err != nil {
		return err
	}
	var p *pod
	if podName != "" {
		p, err = co.requireIncarnation(opName, podName, incarnation)
		if err != nil {
			return err
		}
	}
	for _, a := range acks {
		ackPod := a.Pod
		if podName != "" {
			ackPod = podName
		}
		acknowledgingPod := p
		if acknowledgingPod == nil {
			acknowledgingPod = co.op(c.spec.To).pods[ackPod]
		}
		s, ok := c.all[a.Holder+"/"+a.ID]
		if !ok || s.lost {
			continue
		}
		owned := deliveryKey(ackPod, incarnation)
		if !s.delivered[owned] {
			continue
		}
		delete(s.delivered, owned)
		delete(s.retryAfter, owned)
		acknowledger := ackPod
		if c.broadcast() && acknowledgingPod != nil {
			acknowledger = cohortKey(acknowledgingPod)
		}
		if !s.acked[acknowledger] {
			c.acknowledged += s.records
		}
		s.acked[acknowledger] = true
		if len(s.delivered) == 0 {
			delete(c.inflight, s.key())
		}
		if !c.broadcast() {
			// One acknowledgement is definitive on a partitioned channel: the
			// segment went to exactly one replica. A Broadcast segment needs
			// one from every replica, which settle decides.
			co.release(c, s)
		}
	}
	co.settle()
	return co.commitLocked()
}

func cohortKey(p *pod) string { return fmt.Sprintf("slot:%d", p.cohortSlot) }

// NackSession returns specified unfinished deliveries owned by the active
// process. Repeating a successful request has no effect.
func (co *Coordinator) NackSession(name, opName, podName, incarnation string, acks []SegmentAck) error {
	co.mu.Lock()
	defer co.mu.Unlock()
	c, err := co.get(name)
	if err != nil {
		return err
	}
	if _, err := co.requireIncarnation(opName, podName, incarnation); err != nil {
		return err
	}
	owned := deliveryKey(podName, incarnation)
	for _, a := range acks {
		s := c.all[a.Holder+"/"+a.ID]
		if s == nil || !s.delivered[owned] {
			continue
		}
		delete(s.delivered, owned)
		if a.RetryAfterMillis > 0 {
			if s.retryAfter == nil {
				s.retryAfter = map[string]time.Time{}
			}
			s.retryAfter[owned] = co.now().Add(time.Duration(a.RetryAfterMillis) * time.Millisecond)
		}
		if a.Failure != "" {
			c.latestDeliveryFailure = a.Failure
		}
		if len(s.delivered) == 0 {
			delete(c.inflight, s.key())
		}
		if c.broadcast() {
			c.cursor[podName] = 0
		} else if !s.lost && !s.released && !containsSegment(c.queues[s.part], s) {
			c.queues[s.part] = append([]*segment{s}, c.queues[s.part]...)
		}
	}
	co.settle()
	return co.commitLocked()
}

func containsSegment(list []*segment, want *segment) bool {
	for _, s := range list {
		if s == want {
			return true
		}
	}
	return false
}

// release marks a segment as needed by nobody and drops the copy the
// coordinator holds for an external producer.
func (co *Coordinator) release(c *channel, s *segment) {
	s.released = true
	if s.data != nil && c.spec.Durability != graph.DurabilityRetained {
		s.data = nil
		delete(c.all, s.key())
	}
}

// releaseBroadcast releases the segments of a Broadcast channel that every
// replica of the consuming operation has acknowledged. The last
// acknowledgement usually decides this, and a replica count that falls to
// meet the acknowledgements already in hand also does, so the decision is
// taken on a sweep rather than at the moment of an acknowledgement.
func (co *Coordinator) releaseBroadcast(c *channel) {
	if !c.broadcast() || c.external() {
		return
	}
	for _, s := range c.all {
		if !s.lost && !s.released && co.consumedByEveryReplica(c, s) {
			co.release(c, s)
		}
	}
}

// broadcastFullyConsumed reports whether every replica of the consuming
// operation has acknowledged every segment of the channel.
func (co *Coordinator) broadcastFullyConsumed(c *channel) bool {
	for _, s := range c.log {
		if !s.lost && !s.released && !co.consumedByEveryReplica(c, s) {
			return false
		}
	}
	return true
}

// consumedByEveryReplica reports whether every replica of the consuming
// operation has acknowledged the segment.
//
// Every replica receives every record on a Broadcast channel, so a segment is
// finished with only when they all have it. The registry of pods does not
// answer that question: a replica that has not started yet has acknowledged
// nothing and is invisible here, so counting acknowledgements against the
// registry treats a partly-started operation as a finished one. The replica
// count the controller publishes is the missing number.
//
// The count of live pods is a floor, so an operation running more pods than
// the controller last published is still handled, and a consumer with no
// published count behaves as it did before the count existed. The controller
// republishes on every pass, so a consumer that has been scaled down stops
// waiting for the replicas it no longer has one pass later. A channel whose
// consumer has neither a published count nor a live pod keeps its segments,
// because nothing has read them.
func (co *Coordinator) consumedByEveryReplica(c *channel, s *segment) bool {
	o := co.op(c.spec.To)
	want := int(o.replicas)
	if want < len(o.pods) {
		want = len(o.pods)
	}
	if want == 0 {
		return false
	}
	return len(s.acked) >= want
}

// EpochDone records completion of the consuming operation's scalar epoch.
// It remains channel-scoped for wire compatibility with existing workers.
func (co *Coordinator) EpochDone(name, podName string, epoch int32) error {
	return co.EpochDoneSession(name, "", podName, "", epoch)
}

// EpochDoneSession records completion after verifying the active process.
func (co *Coordinator) EpochDoneSession(name, opName, podName, incarnation string, epoch int32) error {
	co.mu.Lock()
	defer co.mu.Unlock()
	c, err := co.get(name)
	if err != nil {
		return err
	}
	if !c.synchronous() {
		return errf(400, "channel %q is not a Synchronous feedback channel", name)
	}
	if opName != "" {
		if _, err := co.requireIncarnation(opName, podName, incarnation); err != nil {
			return err
		}
	}
	if _, ok := c.epochDone[podName]; !ok {
		return errf(400, "unknown consumer pod %q", podName)
	}
	if epoch > c.epochDone[podName] {
		c.epochDone[podName] = epoch
	}
	co.touch(c.spec.To, podName, "", 0)
	if err := co.recordOperationEpochDone(c.spec.To, podName, epoch); err != nil {
		return err
	}
	return co.commitLocked()
}

// OperationEpochDone records that a worker finished all callbacks for an
// epoch and published every buffered output produced by those callbacks.
func (co *Coordinator) OperationEpochDone(opName, podName string, epoch int32) error {
	return co.OperationEpochDoneSession(opName, podName, "", epoch)
}

// OperationEpochDoneSession records completion after verifying the active
// worker process.
func (co *Coordinator) OperationEpochDoneSession(opName, podName, incarnation string, epoch int32) error {
	co.mu.Lock()
	defer co.mu.Unlock()
	if opName == "" || podName == "" {
		return errf(400, "operation and pod are required")
	}
	p, err := co.requireIncarnation(opName, podName, incarnation)
	if err != nil {
		return err
	}
	p.lastSeen = co.now()
	co.expireAllExcept(opName, podName)
	if err := co.recordOperationEpochDone(opName, podName, epoch); err != nil {
		return err
	}
	return co.commitLocked()
}

func (co *Coordinator) recordOperationEpochDone(opName, podName string, epoch int32) error {
	o := co.op(opName)
	if _, ok := o.epochDone[podName]; !ok {
		return errf(400, "unknown worker pod %q", podName)
	}
	if epoch <= o.epochDone[podName] {
		return nil
	}
	for _, c := range co.channels {
		if c.spec.To != opName || !c.finiteEpochs {
			continue
		}
		if c.epoch != epoch || !c.productionClosed[epoch] || !c.quiet() {
			return errf(425, "operation %q still has open or unconsumed input at epoch %d", opName, epoch)
		}
	}
	if epoch > o.epochDone[podName] {
		o.epochDone[podName] = epoch
	}
	required := o.replicas
	if live := int32(len(o.pods)); required < live {
		required = live
	}
	var done int32
	for id, p := range o.pods {
		if p.lastSeen.After(co.now().Add(-PodTTL)) && o.epochDone[id] >= epoch {
			done++
		}
	}
	if required == 0 || done < required {
		return nil
	}
	for _, c := range co.channels {
		if c.spec.From != opName || !c.finiteEpochs {
			continue
		}
		outEpoch := epoch
		if c.synchronous() {
			outEpoch++
		}
		if c.synchronous() && outEpoch >= c.spec.Feedback.MaxEpochs {
			co.seal(c)
			continue
		}
		c.productionClosed[outEpoch] = true
	}
	for _, c := range co.channels {
		if c.spec.To == opName && c.finiteEpochs && c.epoch == epoch {
			co.advanceEpoch(c)
		}
	}
	return nil
}

func (co *Coordinator) advanceEpoch(c *channel) {
	c.epoch++
	if c.sealed {
		c.productionClosed[c.epoch] = true
	}
	var rest []*segment
	for _, s := range c.held {
		if s.epoch == c.epoch {
			c.enqueue(s)
		} else {
			rest = append(rest, s)
		}
	}
	c.held = rest
}

func (co *Coordinator) hasInbound(opName string) bool {
	for _, c := range co.channels {
		if c.spec.To == opName {
			return true
		}
	}
	return false
}

// --- Asynchronous loop termination ----------------------------------------

// settle releases Broadcast segments that every replica has taken, and seals
// every Asynchronous feedback channel whose loop can produce nothing more:
// every channel feeding the cycle from outside is sealed and drained, and
// every channel inside the cycle is quiet. Records still being processed are
// in flight on some channel inside the cycle, so a quiet cycle with sealed
// inputs is finished.
func (co *Coordinator) settle() {
	for _, c := range co.channels {
		co.releaseBroadcast(c)
		if c.sealed || c.feedbackMode() != graph.FeedbackAsynchronous {
			continue
		}
		members := co.cycleMembers(c)
		done := true
		for _, other := range co.channels {
			if other == c || !members[other.spec.To] {
				continue
			}
			if members[other.spec.From] {
				if other.spec.Feedback == nil && !other.quiet() {
					done = false
				}
				continue
			}
			if !other.sealed || !other.quiet() || other.heldRecords() > 0 {
				done = false
			}
		}
		if done && c.quiet() {
			co.seal(c)
		}
	}
}

// cycleMembers returns the operations on the cycle closed by feedback
// channel c: those reachable from its consumer that can reach its producer
// over non-feedback channels.
func (co *Coordinator) cycleMembers(c *channel) map[string]bool {
	forward := map[string]bool{c.spec.To: true}
	backward := map[string]bool{c.spec.From: true}
	for changed := true; changed; {
		changed = false
		for _, ch := range co.channels {
			if ch.spec.Feedback != nil || ch.spec.From == "" || ch.spec.To == "" {
				continue
			}
			if forward[ch.spec.From] && !forward[ch.spec.To] {
				forward[ch.spec.To] = true
				changed = true
			}
			if backward[ch.spec.To] && !backward[ch.spec.From] {
				backward[ch.spec.From] = true
				changed = true
			}
		}
	}
	members := map[string]bool{c.spec.To: true, c.spec.From: true}
	for op := range forward {
		if backward[op] {
			members[op] = true
		}
	}
	return members
}

// --- external readers -----------------------------------------------------

// Records returns retained records of a channel with no consumer from log
// offset after, filtered by key when key is non-empty. It waits up to wait
// for new records when none match, and returns the offset to resume from.
func (co *Coordinator) Records(name, key string, after int, wait time.Duration) ([]Record, int, error) {
	deadline := time.Now().Add(wait)
	for {
		co.mu.Lock()
		c, err := co.get(name)
		if err != nil {
			co.mu.Unlock()
			return nil, 0, err
		}
		if !c.external() {
			co.mu.Unlock()
			return nil, 0, errf(400, "channel %q is consumed by operation %q; its records live on worker pods", name, c.spec.To)
		}
		if after < 0 {
			after = 0
		}
		if after > len(c.records) {
			after = len(c.records)
		}
		out := []Record{}
		for _, r := range c.records[after:] {
			if key == "" || r.Key == key {
				out = append(out, r)
			}
		}
		next := len(c.records)
		wake := co.wake
		sealed := c.sealed
		co.mu.Unlock()
		if len(out) > 0 || sealed || wait <= 0 {
			return out, next, nil
		}
		remaining := time.Until(deadline)
		if remaining <= 0 {
			return out, next, nil
		}
		select {
		case <-wake:
		case <-time.After(remaining):
			return out, next, nil
		}
	}
}

// --- metrics --------------------------------------------------------------

// Metrics reports every channel and operation.
func (co *Coordinator) Metrics() Metrics {
	co.mu.Lock()
	defer co.mu.Unlock()
	co.expireAll()
	co.settle()
	m := Metrics{Channels: []ChannelMetrics{}, Operations: []OperationMetrics{}}
	for _, n := range co.channelNames() {
		c := co.channels[n]
		m.Channels = append(m.Channels, ChannelMetrics{
			Name: n, From: c.spec.From, To: c.spec.To, Sealed: c.sealed,
			Pending: c.pending() + c.heldRecords(), InFlight: c.inflightRecords(),
			Produced: c.produced, Epoch: c.epoch, Overflowed: c.overflowed, Lost: c.lost,
			ProductionClosed: c.productionClosed[c.epoch],
			Acknowledged:     c.acknowledged, LatestDeliveryFailure: c.latestDeliveryFailure,
			PendingByPartition: c.pendingByPartition(),
		})
	}
	names := make([]string, 0, len(co.ops))
	for n := range co.ops {
		names = append(names, n)
	}
	sort.Strings(names)
	for _, n := range names {
		m.Operations = append(m.Operations, co.operationMetrics(n))
	}
	return m
}

// operationMetrics derives an operation's state from the channels rather
// than latching it. An operation that consumes channels is complete when
// every inbound channel is sealed, drained, and holds nothing, and no
// partition still has runnable work; live pods are not required, so an
// operation that has been scaled to zero stays complete, and new input on
// an inbound channel makes it incomplete again. A source (no inbound
// channels) is complete when it has a live pod and every live pod has
// reported that it will emit nothing more.
func (co *Coordinator) operationMetrics(name string) OperationMetrics {
	o := co.op(name)
	om := OperationMetrics{Name: name, LivePods: int32(len(o.pods))}
	complete := true
	hasInbound := false
	runnable := map[string]bool{}
	for _, c := range co.channels {
		if c.spec.To == name {
			hasInbound = true
			if !(c.sealed && c.quiet() && c.heldRecords() == 0) {
				complete = false
			}
			// A Broadcast channel is drained for one replica once that
			// replica has read it, so quiescence alone would let the first
			// replica up finish the whole operation on its own.
			if c.broadcast() && !co.broadcastFullyConsumed(c) {
				complete = false
			}
			if !c.gated() {
				for p, v := range c.pendingByPartition() {
					if v == 0 {
						continue
					}
					k := c.spec.Name + "/" + fmt.Sprint(p)
					if c.spec.Partitioning.Mode == graph.PartitionHash {
						k = "hash/" + ownerKey(c.partitions(), p)
					}
					runnable[k] = true
				}
			}
		}
		if c.spec.Durability != graph.DurabilityRetained {
			for _, s := range c.all {
				if s.data == nil && !s.durable && !s.released && s.op == name {
					om.HoldsUnconsumed = true
				}
			}
		}
	}
	om.RunnableTasks = int32(len(runnable))
	if len(runnable) > 0 {
		complete = false
	}
	if !hasInbound {
		// A source has no inbound channels; it is done producing when its
		// pods say so.
		complete = complete && len(o.pods) > 0
	}
	// Completion also requires that every live pod has reported done, so an
	// operation with an OnDrain step is not torn down before that step has
	// produced its output.
	if len(o.pods) == 0 && !o.completed {
		complete = false
	}
	if int32(len(o.pods)) < o.replicas && !o.completed {
		complete = false
	}
	for _, p := range o.pods {
		if !p.done {
			complete = false
		}
	}
	if complete {
		o.completed = true
	} else if len(runnable) > 0 {
		// Late input on an inbound channel restarts a completed operation.
		o.completed = false
	}
	om.Complete = o.completed
	return om
}
