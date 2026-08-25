package coordinator

import (
	"fmt"
	"hash/fnv"
	"sort"
	"sync"
	"time"

	"github.com/pedapudi/stark8s/api/v1alpha1"
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
	name     string
	op       string
	addr     string
	slots    int32
	lastSeen time.Time
	done     bool
}

// operation is the per-operation state: its pods and, for its consumers,
// hash partition ownership shared across every hash channel into it so that
// partition p of every such channel (with equal partition count) is owned by
// the same pod. Two inputs hashed with the same partition count are then
// co-partitioned, which a join or a loop vertex holding per-key state relies
// on.
type operation struct {
	pods map[string]*pod
	// owner maps "partitions/p" -> pod name.
	owner map[string]string
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

// segment is the coordinator's record of one announced segment.
type segment struct {
	id       string
	holder   string
	producer string
	// op is the operation whose pod holds the segment (empty when the
	// coordinator holds it).
	op      string
	channel string
	part    int32
	epoch   int32
	records int64
	bytes   int64
	task    TaskID
	// delivered is the set of consumer pods that fetched (or were told to
	// fetch) the segment and have not acknowledged it yet.
	delivered map[string]bool
	// acked is the set of consumer pods that acknowledged it.
	acked map[string]bool
	// lost: the holder expired before every consumer acknowledged it.
	lost bool
	// released: nothing needs the segment any more (fully acknowledged,
	// dropped at a seal, or lost). Ephemeral released segments are reported
	// to the holder for deletion once and then forgotten.
	released bool
	reported bool
	// data is set for segments the coordinator itself holds (external
	// producers).
	data []Record
}

func (s *segment) key() string { return s.holder + "/" + s.id }

type channel struct {
	spec v1alpha1.Channel

	sealed     bool
	produced   int64
	overflowed int64
	lost       int64
	epoch      int32
	rr         uint64

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
	records []Record
	// all indexes every segment of the channel that has not been forgotten.
	all map[string]*segment
}

func (c *channel) partitions() int { return int(c.spec.Partitioning.Partitions) }

func (c *channel) external() bool { return c.spec.To == "" }

func (c *channel) broadcast() bool {
	return c.spec.Partitioning.Mode == v1alpha1.PartitionBroadcast
}

func (c *channel) feedbackMode() v1alpha1.FeedbackMode {
	if c.spec.Feedback == nil {
		return ""
	}
	if c.spec.Feedback.Mode == v1alpha1.FeedbackAsynchronous {
		return v1alpha1.FeedbackAsynchronous
	}
	return v1alpha1.FeedbackSynchronous
}

func (c *channel) synchronous() bool { return c.feedbackMode() == v1alpha1.FeedbackSynchronous }

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
	wake chan struct{}
}

// New returns an empty coordinator whose own segment server is reachable at
// selfAddr (host:port).
func New(selfAddr string) *Coordinator {
	return &Coordinator{
		channels: map[string]*channel{},
		ops:      map[string]*operation{},
		selfAddr: selfAddr,
		now:      time.Now,
		wake:     make(chan struct{}),
	}
}

func (co *Coordinator) op(name string) *operation {
	o, ok := co.ops[name]
	if !ok {
		o = &operation{pods: map[string]*pod{}, owner: map[string]string{}}
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
func (co *Coordinator) Configure(specs []v1alpha1.Channel) {
	co.mu.Lock()
	defer co.mu.Unlock()
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
		if s.Feedback != nil && s.Feedback.Mode == "" {
			s.Feedback.Mode = v1alpha1.FeedbackSynchronous
		}
		if c, ok := co.channels[s.Name]; ok {
			c.spec = s
			continue
		}
		c := &channel{
			spec:      s,
			cursor:    map[string]int{},
			inflight:  map[string]*segment{},
			epochDone: map[string]int32{},
			all:       map[string]*segment{},
		}
		c.queues = make([][]*segment, s.Partitioning.Partitions)
		co.channels[s.Name] = c
		if s.From != "" {
			co.op(s.From)
		}
		if s.To != "" {
			co.op(s.To)
		}
	}
}

// Topology returns the declared channels.
func (co *Coordinator) Topology() []v1alpha1.Channel {
	co.mu.Lock()
	defer co.mu.Unlock()
	out := make([]v1alpha1.Channel, 0, len(co.channels))
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
	co.touch(reg.Operation, reg.Pod, reg.Addr, reg.Slots)
	co.expireAll()
	return nil
}

// touch refreshes a pod's liveness, creating it when unknown.
func (co *Coordinator) touch(opName, podName, addr string, slots int32) *pod {
	o := co.op(opName)
	p, ok := o.pods[podName]
	if !ok {
		p = &pod{name: podName, op: opName}
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
	co.touch(reg.Operation, reg.Pod, reg.Addr, reg.Slots).done = true
	return nil
}

// Released returns the IDs of Ephemeral segments held by the pod that no
// consumer needs any more, and forgets them.
func (co *Coordinator) Released(podName string) []string {
	co.mu.Lock()
	defer co.mu.Unlock()
	co.expireAll()
	var out []string
	for _, c := range co.channels {
		if c.spec.Durability == v1alpha1.DurabilityRetained {
			continue
		}
		for k, s := range c.all {
			if s.producer != podName || !s.released || s.data != nil {
				continue
			}
			out = append(out, s.id)
			delete(c.all, k)
		}
	}
	sort.Strings(out)
	if out == nil {
		out = []string{}
	}
	return out
}

// expireAll drops every pod that stopped heartbeating.
func (co *Coordinator) expireAll() {
	cut := co.now().Add(-PodTTL)
	for opName, o := range co.ops {
		for id, p := range o.pods {
			if p.lastSeen.After(cut) {
				continue
			}
			delete(o.pods, id)
			co.expireConsumer(opName, o, id)
			co.expireHolder(id)
		}
	}
}

// expireConsumer releases partition ownership of a gone consumer pod and
// returns its unacknowledged segments on every channel into its operation to
// the pending set (at-least-once delivery).
func (co *Coordinator) expireConsumer(opName string, o *operation, id string) {
	for k, owner := range o.owner {
		if owner == id {
			delete(o.owner, k)
		}
	}
	for _, c := range co.channels {
		if c.spec.To != opName {
			continue
		}
		delete(c.cursor, id)
		delete(c.epochDone, id)
		for key, s := range c.inflight {
			if !s.delivered[id] {
				continue
			}
			delete(s.delivered, id)
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
			if s.producer != podName || s.released || s.lost {
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
	for _, a := range anns {
		if a.ID == "" {
			continue
		}
		if a.Channel != "" && a.Channel != name {
			return errf(400, "segment %q announced for channel %q on channel %q", a.ID, a.Channel, name)
		}
		if c.synchronous() && a.Epoch < c.epoch {
			return errf(400, "segment epoch %d is behind channel epoch %d", a.Epoch, c.epoch)
		}
		if !c.broadcast() && (a.Partition < 0 || int(a.Partition) >= c.partitions()) {
			return errf(400, "partition %d out of range for channel %q", a.Partition, name)
		}
	}
	for _, a := range anns {
		c.overflowed += a.Overflowed
		if a.ID == "" {
			continue
		}
		s := &segment{
			id: a.ID, holder: a.Holder, producer: a.Producer, channel: name,
			part: a.Partition, epoch: a.Epoch, records: a.Records, bytes: a.Bytes, task: a.Task,
			delivered: map[string]bool{}, acked: map[string]bool{},
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
	return nil
}

// index adds a segment to the channel's structures.
func (co *Coordinator) index(c *channel, s *segment) {
	if _, dup := c.all[s.key()]; dup {
		return
	}
	c.all[s.key()] = s
	c.produced += s.records
	if c.spec.Feedback != nil && s.epoch >= c.spec.Feedback.MaxEpochs {
		// Beyond the loop bound (Synchronous loops): the record set is
		// dropped; the holder may delete it.
		c.produced -= s.records
		s.released = true
		return
	}
	if c.synchronous() && s.epoch > c.epoch {
		c.held = append(c.held, s)
		return
	}
	c.enqueue(s)
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
	case v1alpha1.PartitionBroadcast:
		return 0
	case v1alpha1.PartitionHash:
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
		return nil
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
		if c.synchronous() && k.e < c.epoch {
			return errf(400, "record epoch %d is behind channel epoch %d", k.e, c.epoch)
		}
	}
	for _, k := range order {
		co.nextID++
		s := &segment{
			id: fmt.Sprintf("ext-%d", co.nextID), holder: co.selfAddr, producer: "coordinator",
			channel: name, part: k.p, epoch: k.e, records: int64(len(groups[k])),
			delivered: map[string]bool{}, acked: map[string]bool{}, data: groups[k],
		}
		co.index(c, s)
	}
	co.settle()
	return nil
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
	return nil
}

func (co *Coordinator) seal(c *channel) {
	c.sealed = true
	for _, s := range c.held {
		s.released = true
	}
	c.held = nil
	close(co.wake)
	co.wake = make(chan struct{})
}

// --- consuming ------------------------------------------------------------

// assigned returns the partitions the pod may read.
//
// RoundRobin partitions have no key affinity so they are rebalanced freely
// across live pods. Hash partitions carry key affinity so ownership is
// sticky and shared across the operation's channels: a partition stays with
// its first owner until that owner expires.
func (c *channel) assigned(o *operation, id string) []int {
	ids := o.liveIDs()
	idx := sort.SearchStrings(ids, id)
	var out []int
	n := c.partitions()
	if c.spec.Partitioning.Mode == v1alpha1.PartitionHash {
		for p := 0; p < n; p++ {
			k := ownerKey(n, p)
			if _, ok := o.owner[k]; ok {
				continue
			}
			best, bestN := "", int(^uint(0)>>1)
			for _, cid := range ids {
				cnt := 0
				for _, owner := range o.owner {
					if owner == cid {
						cnt++
					}
				}
				if cnt < bestN {
					best, bestN = cid, cnt
				}
			}
			o.owner[k] = best
		}
		for p := 0; p < n; p++ {
			if o.owner[ownerKey(n, p)] == id {
				out = append(out, p)
			}
		}
		return out
	}
	if len(ids) == 0 {
		return nil
	}
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
				if !s.lost {
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
	return c.spec.Delivery == v1alpha1.DeliveryMaterialized && !c.sealed
}

// Consume returns up to max segments pending on the partitions the pod owns.
func (co *Coordinator) Consume(name, opName, podName string, max int) (*ConsumeResponse, error) {
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
	o := co.op(c.spec.To)
	co.touch(c.spec.To, podName, "", 0)
	if _, ok := c.cursor[podName]; !ok {
		c.cursor[podName] = 0
		c.epochDone[podName] = -1
	}
	co.expireAll()
	co.settle()

	resp := &ConsumeResponse{Sealed: c.sealed, Epoch: c.epoch, Mode: c.feedbackMode()}
	if c.spec.Feedback != nil {
		resp.MaxEpochs = c.spec.Feedback.MaxEpochs
	}
	n := 0
	if !c.gated() {
		if c.broadcast() {
			work := PartitionWork{Partition: 0}
			for c.cursor[podName] < len(c.log) && n < max {
				s := c.log[c.cursor[podName]]
				c.cursor[podName]++
				if s.lost || s.acked[podName] {
					continue
				}
				s.delivered[podName] = true
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
					c.queues[p] = c.queues[p][1:]
					s.delivered[podName] = true
					c.inflight[s.key()] = s
					work.Segments = append(work.Segments, ref(s))
					n++
				}
				if len(work.Segments) > 0 {
					resp.Work = append(resp.Work, work)
				}
			}
		}
	}
	quiet := c.quiet()
	resp.Drained = c.sealed && quiet
	resp.Quiescent = c.synchronous() && !c.sealed && quiet
	return resp, nil
}

func ref(s *segment) SegmentRef {
	return SegmentRef{ID: s.id, Holder: s.holder, Records: s.records, Epoch: s.epoch}
}

// Ack marks segments processed by the acknowledging pods.
func (co *Coordinator) Ack(name string, acks []SegmentAck) error {
	co.mu.Lock()
	defer co.mu.Unlock()
	c, err := co.get(name)
	if err != nil {
		return err
	}
	o := co.op(c.spec.To)
	for _, a := range acks {
		s, ok := c.all[a.Holder+"/"+a.ID]
		if !ok || s.lost {
			continue
		}
		delete(s.delivered, a.Pod)
		s.acked[a.Pod] = true
		if len(s.delivered) == 0 {
			delete(c.inflight, s.key())
		}
		if c.broadcast() {
			done := len(o.pods) > 0
			for id := range o.pods {
				if !s.acked[id] {
					done = false
				}
			}
			if done {
				s.released = true
			}
		} else {
			s.released = true
		}
		if s.released && s.data != nil && c.spec.Durability != v1alpha1.DurabilityRetained {
			s.data = nil
			delete(c.all, s.key())
		}
	}
	co.settle()
	return nil
}

// EpochDone records that a pod finished the given epoch of a Synchronous
// feedback channel. When every live consumer pod has finished it and the
// channel is quiet, the barrier advances: held segments for the next epoch
// are released, or the channel is sealed when the loop bound is reached.
func (co *Coordinator) EpochDone(name, podName string, epoch int32) error {
	co.mu.Lock()
	defer co.mu.Unlock()
	c, err := co.get(name)
	if err != nil {
		return err
	}
	if !c.synchronous() {
		return errf(400, "channel %q is not a Synchronous feedback channel", name)
	}
	if _, ok := c.epochDone[podName]; !ok {
		return errf(400, "unknown consumer pod %q", podName)
	}
	if epoch > c.epochDone[podName] {
		c.epochDone[podName] = epoch
	}
	co.touch(c.spec.To, podName, "", 0)
	co.expireAll()
	if c.sealed || epoch != c.epoch {
		return nil
	}
	for _, done := range c.epochDone {
		if done < c.epoch {
			return nil
		}
	}
	if !c.quiet() {
		return nil
	}
	next := c.epoch + 1
	if next >= c.spec.Feedback.MaxEpochs {
		co.seal(c)
		return nil
	}
	c.epoch = next
	var rest []*segment
	for _, s := range c.held {
		if s.epoch == next {
			c.enqueue(s)
		} else {
			rest = append(rest, s)
		}
	}
	c.held = rest
	return nil
}

// --- Asynchronous loop termination ----------------------------------------

// settle seals every Asynchronous feedback channel whose loop can produce
// nothing more: every channel feeding the cycle from outside is sealed and
// drained, and every channel inside the cycle is quiet. Records still being
// processed are in flight on some channel inside the cycle, so a quiet cycle
// with sealed inputs is finished.
func (co *Coordinator) settle() {
	for _, c := range co.channels {
		if c.sealed || c.feedbackMode() != v1alpha1.FeedbackAsynchronous {
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

func (co *Coordinator) operationMetrics(name string) OperationMetrics {
	o := co.op(name)
	om := OperationMetrics{Name: name, LivePods: int32(len(o.pods))}
	complete := true
	runnable := map[string]bool{}
	for _, c := range co.channels {
		if c.spec.To == name {
			if !(c.sealed && c.quiet() && c.heldRecords() == 0) {
				complete = false
			}
			if !c.gated() {
				for p, v := range c.pendingByPartition() {
					if v == 0 {
						continue
					}
					k := c.spec.Name + "/" + fmt.Sprint(p)
					if c.spec.Partitioning.Mode == v1alpha1.PartitionHash {
						k = "hash/" + ownerKey(c.partitions(), p)
					}
					runnable[k] = true
				}
			}
		}
		if c.spec.Durability != v1alpha1.DurabilityRetained {
			for _, s := range c.all {
				if s.data == nil && !s.released && s.op == name {
					om.HoldsUnconsumed = true
				}
			}
		}
	}
	om.RunnableTasks = int32(len(runnable))
	if len(o.pods) == 0 {
		complete = false
	}
	for _, p := range o.pods {
		if !p.done {
			complete = false
		}
	}
	om.Complete = complete
	return om
}
