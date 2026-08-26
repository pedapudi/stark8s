// Package v1alpha1 defines the Workload API: a directed graph of operations
// connected by channels, where each operation owns an independently scaled
// pool of pods and each channel describes how information flows between two
// operations.
package v1alpha1

import (
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// Completion says when an operation is finished.
type Completion string

const (
	// CompletionDrain: the operation finishes once every inbound channel is
	// sealed and it has consumed everything assigned to it. Batch semantics;
	// realised as a Job.
	CompletionDrain Completion = "Drain"
	// CompletionNever: the operation runs until the workload is deleted.
	// Streaming semantics; realised as a Deployment.
	CompletionNever Completion = "Never"
)

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

// HorizontalScaling bounds replica count and names the signals that drive it.
type HorizontalScaling struct {
	// +kubebuilder:default=1
	// +kubebuilder:validation:Minimum=0
	Min int32 `json:"min,omitempty"`
	// +kubebuilder:default=1
	// +kubebuilder:validation:Minimum=1
	Max int32 `json:"max,omitempty"`
	// CPUUtilizationPercent creates a HorizontalPodAutoscaler on CPU. Only
	// applies to operations with completion Never. Zero disables it.
	CPUUtilizationPercent int32 `json:"cpuUtilizationPercent,omitempty"`
}

// VerticalScalingMode selects VerticalPodAutoscaler behaviour.
type VerticalScalingMode string

const (
	VerticalOff     VerticalScalingMode = "Off"
	VerticalInitial VerticalScalingMode = "Initial"
	VerticalAuto    VerticalScalingMode = "Auto"
)

// VerticalScaling requests a VerticalPodAutoscaler for the operation. It is
// ignored when the VPA API is not installed in the cluster.
type VerticalScaling struct {
	// +kubebuilder:default=Off
	// +kubebuilder:validation:Enum=Off;Initial;Auto
	Mode VerticalScalingMode `json:"mode,omitempty"`
}

// Scaling groups the horizontal and vertical policies of an operation.
type Scaling struct {
	// +kubebuilder:default={min:1,max:1}
	Horizontal HorizontalScaling `json:"horizontal,omitempty"`
	Vertical   *VerticalScaling  `json:"vertical,omitempty"`
}

// Operation is a vertex of the graph: one logical computation backed by its
// own pool of pods.
type Operation struct {
	Name string `json:"name"`
	// +kubebuilder:default=Drain
	// +kubebuilder:validation:Enum=Drain;Never
	Completion Completion `json:"completion,omitempty"`
	// Template is the pod template for this operation's replicas. The
	// controller injects the STARK8S_* environment variables that the SDK
	// reads to discover the exchange and this operation's channels.
	Template corev1.PodTemplateSpec `json:"template"`
	// +kubebuilder:default={horizontal:{min:1,max:1}}
	Scaling Scaling `json:"scaling,omitempty"`
	// Slots is how many partitions one replica processes concurrently. The
	// controller sizes the replica count as runnable tasks divided by slots.
	// +kubebuilder:default=1
	// +kubebuilder:validation:Minimum=1
	Slots int32 `json:"slots,omitempty"`
}

// CoordinatorSpec configures the per-workload coordinator that tracks
// partition ownership, segment locations, seals, and epochs.
type CoordinatorSpec struct {
	// Image of the coordinator server. Defaults to the controller's own image.
	Image string `json:"image,omitempty"`
}

// WorkloadSpec is the graph.
type WorkloadSpec struct {
	Operations  []Operation     `json:"operations"`
	Channels    []Channel       `json:"channels,omitempty"`
	Coordinator CoordinatorSpec `json:"coordinator,omitempty"`
}

// OperationPhase is the lifecycle state of one operation.
type OperationPhase string

const (
	// OperationWaiting: an inbound Materialized channel is not yet sealed.
	OperationWaiting   OperationPhase = "Waiting"
	OperationRunning   OperationPhase = "Running"
	OperationSucceeded OperationPhase = "Succeeded"
	OperationFailed    OperationPhase = "Failed"
)

// OperationStatus reports one operation.
type OperationStatus struct {
	Name     string         `json:"name"`
	Phase    OperationPhase `json:"phase"`
	Replicas int32          `json:"replicas"`
	Ready    int32          `json:"ready"`
	// RunnableTasks is the number of partitions with unconsumed input.
	RunnableTasks int32 `json:"runnableTasks,omitempty"`
	// HoldsUnconsumed: the operation's pods still hold segments that a
	// consumer has not fetched, so they are kept after completion.
	HoldsUnconsumed bool `json:"holdsUnconsumed,omitempty"`
}

// ChannelStatus reports one channel, from coordinator metrics.
type ChannelStatus struct {
	Name     string `json:"name"`
	Sealed   bool   `json:"sealed"`
	Pending  int64  `json:"pending"`
	InFlight int64  `json:"inFlight"`
	Produced int64  `json:"produced"`
	// Epoch is the current superstep of a Synchronous feedback channel.
	Epoch int32 `json:"epoch,omitempty"`
	// Overflowed counts records diverted or dropped at the loop bound.
	Overflowed int64 `json:"overflowed,omitempty"`
	// Lost counts segments whose holder pod expired before consumption.
	Lost int64 `json:"lost,omitempty"`
}

// WorkloadPhase is the lifecycle state of the whole graph.
type WorkloadPhase string

const (
	WorkloadPending   WorkloadPhase = "Pending"
	WorkloadRunning   WorkloadPhase = "Running"
	WorkloadSucceeded WorkloadPhase = "Succeeded"
	WorkloadFailed    WorkloadPhase = "Failed"
)

// WorkloadStatus is the observed state of the graph.
type WorkloadStatus struct {
	Phase      WorkloadPhase     `json:"phase,omitempty"`
	Message    string            `json:"message,omitempty"`
	Operations []OperationStatus `json:"operations,omitempty"`
	Channels   []ChannelStatus   `json:"channels,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:shortName=wl
// +kubebuilder:printcolumn:name="Phase",type=string,JSONPath=`.status.phase`
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`

// Workload is a directed graph of operations connected by channels.
type Workload struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   WorkloadSpec   `json:"spec,omitempty"`
	Status WorkloadStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true

// WorkloadList is a list of Workloads.
type WorkloadList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []Workload `json:"items"`
}

func init() {
	SchemeBuilder.Register(&Workload{}, &WorkloadList{})
}

// OperationByName returns the named operation, or nil.
func (s *WorkloadSpec) OperationByName(name string) *Operation {
	for i := range s.Operations {
		if s.Operations[i].Name == name {
			return &s.Operations[i]
		}
	}
	return nil
}

// Inbound returns the channels consumed by the named operation.
func (s *WorkloadSpec) Inbound(op string) []Channel {
	var out []Channel
	for _, c := range s.Channels {
		if c.To == op {
			out = append(out, c)
		}
	}
	return out
}

// Outbound returns the channels produced by the named operation.
func (s *WorkloadSpec) Outbound(op string) []Channel {
	var out []Channel
	for _, c := range s.Channels {
		if c.From == op {
			out = append(out, c)
		}
	}
	return out
}
