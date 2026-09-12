// Package v1alpha1 defines the Workload API: a directed graph of operations
// connected by channels, where each operation owns an independently scaled
// pool of pods and each channel describes how information flows between two
// operations.
package v1alpha1

import (
	"github.com/pedapudi/stark8s/api/graph"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
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

// HorizontalScaling bounds replica count and names the signals that drive it.
type HorizontalScaling struct {
	// +kubebuilder:default=1
	// +kubebuilder:validation:Minimum=0
	Min int32 `json:"min,omitempty"`
	// +kubebuilder:default=1
	// +kubebuilder:validation:Minimum=1
	Max int32 `json:"max,omitempty"`
	// CPUUtilizationPercent is retained for compatibility. Automatic horizontal
	// scaling is disabled because operation pods may hold local state or output.
	CPUUtilizationPercent int32 `json:"cpuUtilizationPercent,omitempty"`
}

// VerticalScalingMode selects VerticalPodAutoscaler behaviour.
type VerticalScalingMode string

const (
	VerticalOff     VerticalScalingMode = "Off"
	VerticalInitial VerticalScalingMode = "Initial"
	VerticalAuto    VerticalScalingMode = "Auto"
)

// VerticalScaling requests initial resource sizing for a Never operation when
// the VerticalPodAutoscaler API is installed. Auto is retained for
// compatibility but disabled because it can evict a stateful pod.
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

// EgressDestination names somewhere outside the workload that an operation is
// allowed to open connections to.
type EgressDestination string

const (
	// EgressMetadata is the instance metadata server on its link-local
	// address, over plain HTTP. It is how a pod obtains an identity token for
	// the account it runs as, which everything else outside the cluster tends
	// to need first.
	EgressMetadata EgressDestination = "Metadata"
	// EgressInternet is HTTPS to addresses outside the cluster. The private
	// ranges are excluded, so this grants no reach to other pods or services.
	EgressInternet EgressDestination = "Internet"
)

// EgressRule grants one operation access to one destination outside the
// workload.
//
// It is a struct holding a single field rather than a bare destination so
// that a narrower grant can be expressed later, by adding an explicit CIDR or
// a port list here, without changing the shape of anything already written.
type EgressRule struct {
	// To names the destination.
	// +kubebuilder:validation:Enum=Metadata;Internet
	To EgressDestination `json:"to"`
}

// SegmentStorage sizes the local volume that holds the segments an
// operation's pods produce. It is per pod, not per operation: every replica
// gets a volume this size and requests this much disk, so an operation that
// produces a total spread over N replicas needs roughly a total/N here, and
// the cluster is asked for Size times the replica count.
//
// How much has to fit depends on the outbound channels:
//
//   - Ephemeral: a segment is deleted once every consumer has acknowledged
//     it, so the volume has to hold the peak unacknowledged output. On a
//     Materialized channel that is the replica's whole output, since the
//     consumer is not started until the channel seals.
//   - Retained with a consumer: the coordinator never releases retained
//     segments back to their producer, so the volume has to hold everything
//     the replica produces for as long as its pod runs.
//   - No consumer at all: nothing reaches this volume. The coordinator forces
//     such a channel to Retained and the worker posts its records to the
//     coordinator instead of writing a segment, so Size does nothing for an
//     operation whose only output is a terminal channel.
//
// Sizing the volume is a scheduling statement, not a durability one. Segments
// live and die with the pod holding them, so a completed producer of retained
// internal segments remains running.
//
// Declaring it is what tells the scheduler the pods need disk. Left unset the
// volume is a bare emptyDir with no size, and its capacity is whatever the
// cluster's defaults allow.
type SegmentStorage struct {
	// Size is the capacity of the segment volume. The controller sets it as
	// the volume's sizeLimit and as a floor under the first container's
	// ephemeral-storage request, which is what the scheduler places the pod
	// by. No limit is set: a pod's ephemeral-storage limit is charged the
	// volume together with every container's writable layer and logs, so a
	// limit equal to Size would evict the pod before the volume filled. A
	// template whose containers do add up to a limit keeps it, and one that
	// does not exceed Size is rejected rather than raised.
	Size resource.Quantity `json:"size"`
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
	// Slots is how many partitions one replica processes concurrently.
	// +kubebuilder:default=1
	// +kubebuilder:validation:Minimum=1
	Slots int32 `json:"slots,omitempty"`
	// TickInterval makes the operation run on a clock as well as on its
	// input: every replica calls its Tick handler this often, between passes
	// over its inbound channels. It suits an operation that polls a feed, a
	// queue or an API on a schedule while taking what to poll for from a
	// channel. Leave it unset for an operation driven only by records.
	//
	// The handler runs on the same goroutine as record processing, so this is
	// a floor on the period rather than a guarantee: a long batch of records
	// delays the next tick.
	TickInterval *metav1.Duration `json:"tickInterval,omitempty"`
	// Egress lists what this operation may reach outside the workload.
	//
	// The default is nothing. Operation pods are otherwise allowed to reach
	// only the coordinator, DNS, and the segment port of pods in the same
	// workload, which is what keeps one operation off another operation's
	// channels. An operation that has to read a feed, call an API or fetch an
	// identity token says so here, and the grant applies to that operation
	// alone; its neighbours are unaffected.
	//
	// Declaring it per operation rather than per workload keeps the grant
	// visible in the graph, so a reader can see which vertices reach outside
	// and a reviewer sees the widening in the same change as the code that
	// needs it.
	Egress []EgressRule `json:"egress,omitempty"`
	// Segments sizes the local volume this operation's pods keep their
	// produced segments in. A pod template that declares its own volume named
	// stark8s-segments sizes it instead, and the two cannot both be set.
	Segments *SegmentStorage `json:"segments,omitempty"`
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
	Channels    []graph.Channel `json:"channels,omitempty"`
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
	Reason   string         `json:"reason,omitempty"`
	Message  string         `json:"message,omitempty"`
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
	// Acknowledged counts records accepted as processed by consumers.
	Acknowledged int64 `json:"acknowledged,omitempty"`
	// Epoch is the current superstep of a Synchronous feedback channel.
	Epoch int32 `json:"epoch,omitempty"`
	// Overflowed counts records diverted or dropped at the loop bound.
	Overflowed int64 `json:"overflowed,omitempty"`
	// Lost counts records whose holder pod expired before consumption.
	Lost int64 `json:"lost,omitempty"`
	// LatestDeliveryFailure preserves the most recent failed segment fetch.
	LatestDeliveryFailure string `json:"latestDeliveryFailure,omitempty"`
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
	Reason     string            `json:"reason,omitempty"`
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
func (s *WorkloadSpec) Inbound(op string) []graph.Channel {
	var out []graph.Channel
	for _, c := range s.Channels {
		if c.To == op {
			out = append(out, c)
		}
	}
	return out
}

// Outbound returns the channels produced by the named operation.
func (s *WorkloadSpec) Outbound(op string) []graph.Channel {
	var out []graph.Channel
	for _, c := range s.Channels {
		if c.From == op {
			out = append(out, c)
		}
	}
	return out
}
