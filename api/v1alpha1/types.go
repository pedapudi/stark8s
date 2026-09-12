// Package v1alpha1 defines the Workload API: a directed graph of operations
// connected by channels, where each operation owns an independently scaled
// pool of pods and each channel describes how information flows between two
// operations.
package v1alpha1

import (
	"github.com/pedapudi/stark8s/api/graph"
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
