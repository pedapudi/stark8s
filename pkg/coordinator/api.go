// Package coordinator defines the control-plane protocol of a workload: the
// HTTP paths and JSON types exchanged between the controller, the
// coordinator server, and worker pods.
//
// Execution model. Records never pass through the coordinator. A producer
// pod writes records into local segments, one per (channel, partition,
// epoch) per flush, serves them over HTTP on the segment port, and
// announces each segment's location to the coordinator. A consumer pod asks
// the coordinator which partitions it owns and which segments are pending
// for them, fetches those segments directly from the holder pods, processes
// them, and acknowledges them. The coordinator holds only metadata:
// topology, pod registry, partition ownership per consuming operation,
// segment index, seals, and loop epochs.
//
// The one exception is external channels (no producer or no consumer):
// their records are small and are stored on the coordinator itself, which
// also serves them on the segment API.
package coordinator

import "github.com/pedapudi/stark8s/api/v1alpha1"

// Ports and paths.
const (
	// ControlPort is where the coordinator listens.
	ControlPort = 8080
	// SegmentPort is where every worker pod (and the coordinator) serves
	// segments: GET /segments/{id} returns the segment's records as JSON.
	SegmentPort = 8090

	// OperationHeader names the calling operation. When token verification
	// is enabled the coordinator derives the operation from the bearer
	// token's ServiceAccount instead and rejects a mismatching header.
	OperationHeader = "X-Stark8s-Operation"
	// RecordsNextHeader on a GET records response is the log offset following
	// the last record scanned, to pass as `after` on the next call.
	RecordsNextHeader = "X-Stark8s-Next"

	PathTopology = "/topology" // PUT  []v1alpha1.Channel; GET -> []v1alpha1.Channel (pods read partitioning and feedback settings)
	PathMetrics  = "/metrics"  // GET  Metrics
	PathHealth   = "/healthz"  // GET
	// PathEditor serves the graph editor, which draws the workload and
	// overlays what the coordinator observes. It reads PathTopology once for
	// the graph and polls PathMetrics for the overlay, so viewing a running
	// workload needs a port-forward and nothing else. The route is read-only
	// and is registered for GET alone.
	PathEditor   = "/editor"        // GET  text/html
	PathRegister = "/pods/register" // POST PodRegistration (also the heartbeat; repeat every 5s)
	// PathSourceDone: the pod has emitted everything it will emit. Source
	// pods post it after their Source handler; Drain pods post it after
	// their inbound channels drained and their final flush completed. An
	// operation is Complete only when every live pod has posted it.
	PathSourceDone = "/pods/source-done" // POST PodRegistration
	// PathReleased lists segments a holder pod may delete: Ephemeral segments
	// it announced that every consumer has acknowledged. The coordinator
	// forgets each segment once it has been listed.
	PathReleased = "/pods/released" // GET ?pod= -> []string (segment IDs)
	// Channel-scoped paths are PathChannels + "/" + name + suffix.
	PathChannels    = "/channels"
	SuffixSegments  = "/segments"   // POST []SegmentAnnouncement (producer)
	SuffixConsume   = "/consume"    // GET ?pod=&max= -> ConsumeResponse
	SuffixAck       = "/ack"        // POST []SegmentAck
	SuffixSeal      = "/seal"       // POST
	SuffixEpochDone = "/epoch-done" // POST ?pod=&epoch=  (Synchronous feedback)
	// GET records returns []Record from offset `after` in the channel's retained
	// log (filtered by key when given), long-polling up to `wait` for new
	// records; the response header RecordsNextHeader carries the offset to
	// pass as `after` on the next call.
	SuffixRecords     = "/records"    // POST []Record (external producer); GET ?key=&after=&wait= (external consumer)
	SuffixOperationsD = "/operations" // reserved
)

// Record is one unit of information on a channel.
type Record struct {
	Key   string `json:"key"`
	Value any    `json:"value"`
	// Epoch is the loop iteration the record belongs to. Zero outside loops.
	Epoch int32 `json:"epoch"`
}

// PodRegistration identifies a worker pod. Sent on start and as heartbeat.
type PodRegistration struct {
	Operation string `json:"operation"`
	Pod       string `json:"pod"`
	// Addr is host:port of the pod's segment server.
	Addr string `json:"addr"`
	// Slots is the pod's concurrent partition capacity.
	Slots int32 `json:"slots"`
}

// TaskID identifies the unit of work that produced a segment: the consuming
// side's (channel, partition, epoch) that the producer pod was processing,
// or the producer pod's own identity for source operations. It lets the
// coordinator deduplicate re-executed work.
type TaskID struct {
	Channel   string `json:"channel"`
	Partition int32  `json:"partition"`
	Epoch     int32  `json:"epoch"`
	Attempt   int32  `json:"attempt"`
}

// SegmentAnnouncement tells the coordinator where a segment lives.
type SegmentAnnouncement struct {
	// ID is unique per holder pod; the coordinator qualifies it with Addr.
	ID        string `json:"id"`
	Channel   string `json:"channel"`
	Partition int32  `json:"partition"`
	Epoch     int32  `json:"epoch"`
	Records   int64  `json:"records"`
	Bytes     int64  `json:"bytes"`
	Holder    string `json:"holder"` // host:port of the segment server
	Producer  string `json:"producer"`
	Task      TaskID `json:"task"`
	// Overflowed counts records the producer dropped at the loop bound of
	// an Asynchronous feedback channel (no Overflow channel declared). An
	// announcement with an empty ID and Overflowed > 0 reports drops only.
	Overflowed int64 `json:"overflowed,omitempty"`
}

// SegmentRef is a segment a consumer should fetch.
type SegmentRef struct {
	ID      string `json:"id"`
	Holder  string `json:"holder"`
	Records int64  `json:"records"`
	Epoch   int32  `json:"epoch"`
}

// PartitionWork is the pending work for one partition owned by the caller.
type PartitionWork struct {
	Partition int32        `json:"partition"`
	Segments  []SegmentRef `json:"segments"`
}

// ConsumeResponse is what a consumer receives on each poll of one channel.
type ConsumeResponse struct {
	Work []PartitionWork `json:"work"`
	// Sealed: the producer has completed; no further segments will arrive.
	Sealed bool `json:"sealed"`
	// Drained: sealed and no pending or in-flight segments on any partition.
	Drained bool `json:"drained"`
	// Epoch is the current superstep (Synchronous feedback only).
	Epoch int32 `json:"epoch"`
	// Quiescent: Synchronous feedback with nothing pending or in flight at the
	// current epoch; the consumer should finish the epoch and report it.
	Quiescent bool                  `json:"quiescent"`
	MaxEpochs int32                 `json:"maxEpochs,omitempty"`
	Mode      v1alpha1.FeedbackMode `json:"mode,omitempty"`
}

// SegmentAck marks a fetched segment as processed by the calling pod.
type SegmentAck struct {
	ID     string `json:"id"`
	Holder string `json:"holder"`
	Pod    string `json:"pod"`
}

// ChannelMetrics reports one channel.
type ChannelMetrics struct {
	Name       string `json:"name"`
	From       string `json:"from"`
	To         string `json:"to"`
	Sealed     bool   `json:"sealed"`
	Pending    int64  `json:"pending"`  // records in unconsumed segments
	InFlight   int64  `json:"inFlight"` // records in fetched, unacknowledged segments
	Produced   int64  `json:"produced"`
	Epoch      int32  `json:"epoch"`
	Overflowed int64  `json:"overflowed"`
	Lost       int64  `json:"lost"`
	// PendingByPartition supports skew detection by a planner.
	PendingByPartition []int64 `json:"pendingByPartition,omitempty"`
}

// OperationMetrics reports one operation, as the controller needs it.
type OperationMetrics struct {
	Name string `json:"name"`
	// RunnableTasks is the number of owned-or-unowned partitions with pending
	// segments (per current epoch for Synchronous loops).
	RunnableTasks int32 `json:"runnableTasks"`
	// Complete: every inbound channel is sealed and drained, nothing is in
	// flight, and every live registered pod (at least one) has reported
	// done on PathSourceDone.
	Complete bool `json:"complete"`
	// HoldsUnconsumed: pods of this operation hold Ephemeral segments that
	// consumers have not acknowledged. The controller must keep the pods.
	HoldsUnconsumed bool `json:"holdsUnconsumed"`
	// LivePods is the number of registered, unexpired pods.
	LivePods int32 `json:"livePods"`
}

// Metrics is the coordinator's full report.
type Metrics struct {
	Channels   []ChannelMetrics   `json:"channels"`
	Operations []OperationMetrics `json:"operations"`
}

// Environment variables injected into operation pods.
const (
	EnvCoordinator = "STARK8S_COORDINATOR" // http://<workload>-coordinator:8080
	EnvWorkload    = "STARK8S_WORKLOAD"
	EnvOperation   = "STARK8S_OPERATION"
	EnvInstance    = "STARK8S_INSTANCE" // pod name
	EnvPodIP       = "STARK8S_POD_IP"
	EnvSlots       = "STARK8S_SLOTS"
	EnvInbound     = "STARK8S_INBOUND"
	EnvOutbound    = "STARK8S_OUTBOUND"
	EnvFeedback    = "STARK8S_FEEDBACK"     // inbound feedback channels
	EnvFeedbackOut = "STARK8S_FEEDBACK_OUT" // outbound feedback channels
	EnvSegmentDir  = "STARK8S_SEGMENT_DIR"  // local segment store; default /var/lib/stark8s/segments
)
