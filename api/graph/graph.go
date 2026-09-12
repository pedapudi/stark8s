// Package graph is the vocabulary a Workload uses to describe its dataflow:
// the channels between operations, how records are partitioned across them,
// when they are delivered, whether they are kept, and how a cycle iterates.
//
// It imports nothing. That is why it exists apart from api/v1alpha1: the
// coordinator, the worker SDK and every worker binary need this vocabulary and
// nothing else, while the Workload resource that carries it needs Kubernetes
// types for the pod template and object metadata. Keeping them apart means a
// worker pod does not link a Kubernetes client in order to read a partition
// count.
//
// +kubebuilder:object:generate=true
package graph

// PartitioningMode says how records on a channel are split across the
// consuming operation's replicas.
type PartitioningMode string

const (
	// PartitionHash routes each record by hash of its key. Records with equal
	// keys always reach the same consumer replica.
	PartitionHash PartitioningMode = "Hash"
	// PartitionRoundRobin spreads records evenly with no key affinity.
	PartitionRoundRobin PartitioningMode = "RoundRobin"
	// PartitionBroadcast delivers every record to every consumer replica.
	PartitionBroadcast PartitioningMode = "Broadcast"
)

// Delivery says when records become visible to the consumer.
type Delivery string

const (
	// DeliveryPipelined delivers records as soon as they are produced. The
	// consumer may start before the producer finishes.
	DeliveryPipelined Delivery = "Pipelined"
	// DeliveryMaterialized withholds all records until the channel is sealed
	// (the producer has completed). This is a stage barrier: the consumer is
	// not even started until the producer finishes.
	DeliveryMaterialized Delivery = "Materialized"
)

// Durability says what happens to records after they are consumed.
type Durability string

const (
	// DurabilityEphemeral discards records once acknowledged.
	DurabilityEphemeral Durability = "Ephemeral"
	// DurabilityRetained keeps records after acknowledgement so the channel
	// can be replayed or read externally (for example a result channel).
	DurabilityRetained Durability = "Retained"
)

// CombineMode names an associative, commutative function that a producer
// applies to records sharing a key before they are put on the wire.
type CombineMode string

const (
	// CombineSum adds the values. Records must be numbers.
	CombineSum CombineMode = "Sum"
	// CombineMin keeps the smallest value. Records must be numbers.
	CombineMin CombineMode = "Min"
	// CombineMax keeps the largest value. Records must be numbers.
	CombineMax CombineMode = "Max"
	// CombineCount replaces the values with how many were emitted for the
	// key, so the emitted value is ignored and may be null.
	CombineCount CombineMode = "Count"
)

// Idempotent reports whether applying a combined record more than once yields
// the same result. Delivery is at-least-once, so a consumer that applies the
// channel's own function to an idempotent mode needs no deduplication; Sum and
// Count are not idempotent and a redelivered record double-counts.
func (c CombineMode) Idempotent() bool {
	return c == CombineMin || c == CombineMax
}

// Partitioning describes how a channel splits records across consumers.
type Partitioning struct {
	// +kubebuilder:default=RoundRobin
	// +kubebuilder:validation:Enum=Hash;RoundRobin;Broadcast
	Mode PartitioningMode `json:"mode,omitempty"`
	// Partitions is the number of partitions for Hash and RoundRobin modes.
	// The consuming operation should not have more replicas than partitions.
	// +kubebuilder:default=8
	// +kubebuilder:validation:Minimum=1
	Partitions int32 `json:"partitions,omitempty"`
}

// FeedbackMode says how a loop is synchronised.
type FeedbackMode string

const (
	// FeedbackSynchronous runs the loop as bulk-synchronous supersteps: one
	// global epoch, and epoch e+1 is delivered only after every consumer has
	// finished epoch e. Suited to iterative algorithms such as PageRank.
	FeedbackSynchronous FeedbackMode = "Synchronous"
	// FeedbackAsynchronous runs the loop without a barrier. The epoch is
	// carried per record and incremented each time the record crosses the
	// feedback channel, so each key iterates on its own schedule. Suited to
	// agent loops where each conversation is an independent thread.
	FeedbackAsynchronous FeedbackMode = "Asynchronous"
)

// Feedback marks a channel as closing a cycle and defines the loop it drives.
// Records on a feedback channel carry an epoch.
type Feedback struct {
	// +kubebuilder:default=Synchronous
	// +kubebuilder:validation:Enum=Synchronous;Asynchronous
	Mode FeedbackMode `json:"mode,omitempty"`
	// MaxEpochs bounds the loop. Synchronous: when the consuming operation
	// finishes epoch MaxEpochs-1 the channel is sealed. Asynchronous: a
	// record whose epoch reaches MaxEpochs is diverted to Overflow, or
	// dropped and counted when Overflow is empty.
	// +kubebuilder:validation:Minimum=1
	MaxEpochs int32 `json:"maxEpochs"`
	// Overflow names a channel that receives records exceeding MaxEpochs on
	// an Asynchronous loop. It must be a channel with no consumer or one
	// consumed by another operation.
	Overflow string `json:"overflow,omitempty"`
}

// Channel is a directed information flow between two operations. It is the
// only kind of edge in the graph.
type Channel struct {
	Name string `json:"name"`
	// From is the producing operation. Empty means records are produced from
	// outside the workload through the exchange API.
	From string `json:"from,omitempty"`
	// To is the consuming operation. Empty means records are read from outside
	// the workload through the exchange API; such channels are always retained.
	To string `json:"to,omitempty"`
	// +kubebuilder:default={mode:RoundRobin,partitions:8}
	Partitioning Partitioning `json:"partitioning,omitempty"`
	// +kubebuilder:default=Pipelined
	// +kubebuilder:validation:Enum=Pipelined;Materialized
	Delivery Delivery `json:"delivery,omitempty"`
	// +kubebuilder:default=Ephemeral
	// +kubebuilder:validation:Enum=Ephemeral;Retained
	Durability Durability `json:"durability,omitempty"`
	// Combine names a function the producer applies to records sharing a key
	// before they are put on the wire, so a stage that emits one record per
	// fact ships one record per key instead. It is the map-side half of a
	// reduce-by-key: the consumer still aggregates across producers.
	//
	// The function must be associative and commutative, because the producer
	// applies it to whatever subset of a key's records happen to be buffered
	// together. Empty means no combining.
	// +kubebuilder:validation:Enum=Sum;Min;Max;Count
	Combine CombineMode `json:"combine,omitempty"`
	// Feedback is set on the channel that closes a cycle.
	Feedback *Feedback `json:"feedback,omitempty"`
}
