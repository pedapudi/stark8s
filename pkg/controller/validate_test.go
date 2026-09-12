package controller

import (
	"testing"

	"github.com/pedapudi/stark8s/api/graph"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"

	"github.com/pedapudi/stark8s/api/v1alpha1"
)

func spec(chans ...graph.Channel) *v1alpha1.WorkloadSpec {
	return &v1alpha1.WorkloadSpec{
		Operations: []v1alpha1.Operation{{Name: "a"}, {Name: "b"}, {Name: "c"}},
		Channels:   chans,
	}
}

func TestValidateRejectsCycleWithoutFeedback(t *testing.T) {
	s := spec(
		graph.Channel{Name: "ab", From: "a", To: "b"},
		graph.Channel{Name: "bc", From: "b", To: "c"},
		graph.Channel{Name: "ca", From: "c", To: "a"},
	)
	if err := Validate(s); err == nil {
		t.Fatal("cycle without feedback accepted")
	}
	s.Channels[2].Feedback = &graph.Feedback{MaxEpochs: 5}
	if err := Validate(s); err != nil {
		t.Fatal(err)
	}
	s.Channels[2].Feedback.Mode = graph.FeedbackAsynchronous
	if err := Validate(s); err != nil {
		t.Fatalf("asynchronous feedback on a cycle rejected: %v", err)
	}
}

func TestValidateRejectsUnknownOperation(t *testing.T) {
	if err := Validate(spec(graph.Channel{Name: "x", From: "a", To: "zz"})); err == nil {
		t.Fatal("unknown consumer accepted")
	}
}

func TestValidateOverflowMustBeDeclared(t *testing.T) {
	loop := graph.Channel{Name: "loop", From: "a", To: "a",
		Feedback: &graph.Feedback{Mode: graph.FeedbackAsynchronous, MaxEpochs: 3, Overflow: "spill"}}
	if err := Validate(spec(loop)); err == nil {
		t.Fatal("undeclared overflow channel accepted")
	}
	if err := Validate(spec(loop, graph.Channel{Name: "spill", From: "a"})); err != nil {
		t.Fatalf("declared overflow channel rejected: %v", err)
	}
	loop.Feedback.Overflow = "loop"
	if err := Validate(spec(loop)); err == nil {
		t.Fatal("overflow onto the feedback channel itself accepted")
	}
}

func TestValidateSlots(t *testing.T) {
	s := spec()
	s.Operations[0].Slots = -1
	if err := Validate(s); err == nil {
		t.Fatal("negative slots accepted")
	}
	s.Operations[0].Slots = 0
	if err := Validate(s); err != nil {
		t.Fatalf("unset slots rejected: %v", err)
	}
	s.Operations[0].Slots = 4
	if err := Validate(s); err != nil {
		t.Fatalf("slots 4 rejected: %v", err)
	}
}

func TestValidateObjectStore(t *testing.T) {
	tests := []struct {
		name     string
		endpoint string
		secret   string
	}{
		{name: "empty endpoint", secret: "credentials"},
		{name: "missing scheme", endpoint: "objects.example/bucket", secret: "credentials"},
		{name: "unsupported scheme", endpoint: "file:///bucket", secret: "credentials"},
		{name: "userinfo", endpoint: "https://key@objects.example/bucket", secret: "credentials"},
		{name: "query", endpoint: "https://objects.example/bucket?token=value", secret: "credentials"},
		{name: "fragment", endpoint: "https://objects.example/bucket#part", secret: "credentials"},
		{name: "empty credentials secret", endpoint: "https://objects.example/bucket"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			spec := v1alpha1.WorkloadSpec{Coordinator: v1alpha1.CoordinatorSpec{ObjectStore: &v1alpha1.ObjectStoreSpec{Endpoint: test.endpoint, CredentialsSecret: test.secret}}}
			if err := Validate(&spec); err == nil {
				t.Fatal("invalid object store was accepted")
			}
		})
	}
	valid := v1alpha1.WorkloadSpec{Coordinator: v1alpha1.CoordinatorSpec{ObjectStore: &v1alpha1.ObjectStoreSpec{Endpoint: "https://objects.example/bucket", CredentialsSecret: "credentials"}}}
	if err := Validate(&valid); err != nil {
		t.Fatalf("valid object store rejected: %v", err)
	}
}

func TestDesiredReplicas(t *testing.T) {
	op := func(name string, slots, min, max int32) *v1alpha1.Operation {
		return &v1alpha1.Operation{Name: name, Slots: slots, Scaling: v1alpha1.Scaling{Horizontal: v1alpha1.HorizontalScaling{Min: min, Max: max}}}
	}
	cases := []struct {
		name string
		op   *v1alpha1.Operation
		want int32
	}{
		{"membership uses maximum before work", op("staged", 2, 1, 10), 10},
		{"unset maximum uses one", op("staged", 1, 0, 0), 1},
	}
	for _, c := range cases {
		if got := desiredReplicas(c.op); got != c.want {
			t.Errorf("%s: got %d, want %d", c.name, got, c.want)
		}
	}
}

func TestValidateSegmentsSize(t *testing.T) {
	s := spec()
	s.Operations[0].Segments = &v1alpha1.SegmentStorage{Size: resource.MustParse("0")}
	if err := Validate(s); err == nil {
		t.Fatal("zero segments.size accepted")
	}
	s.Operations[0].Segments.Size = resource.MustParse("-1Gi")
	if err := Validate(s); err == nil {
		t.Fatal("negative segments.size accepted")
	}
	// A quantity with no unit suffix is bytes, so this asks for a 50-byte
	// volume the kubelet evicts the pod over before it holds anything.
	s.Operations[0].Segments.Size = resource.MustParse("50")
	if err := Validate(s); err == nil {
		t.Fatal("segments.size of 50 bytes accepted")
	}
	s.Operations[0].Segments.Size = resource.MustParse("50Gi")
	if err := Validate(s); err != nil {
		t.Fatal(err)
	}
	// A pod budget that does not exceed the size the volume may reach evicts
	// the pod before it fills, since the budget is charged the writable layers
	// and logs too. Raising the limit to fit is not the controller's call.
	s.Operations[0].Template = container()
	c := &s.Operations[0].Template.Spec.Containers[0]
	c.Resources.Limits = corev1.ResourceList{corev1.ResourceEphemeralStorage: resource.MustParse("10Gi")}
	if err := Validate(s); err == nil {
		t.Fatal("segments.size above the pod's ephemeral-storage limit accepted")
	}
	// Equal leaves nothing for the writable layers and logs, so it is refused
	// for the same reason rather than sitting just inside the boundary.
	c.Resources.Limits[corev1.ResourceEphemeralStorage] = resource.MustParse("50Gi")
	if err := Validate(s); err == nil {
		t.Fatal("segments.size equal to the pod's ephemeral-storage limit accepted")
	}
	c.Resources.Limits[corev1.ResourceEphemeralStorage] = resource.MustParse("100Gi")
	if err := Validate(s); err != nil {
		t.Fatalf("segments.size under the pod's limit rejected: %v", err)
	}
	// The budget is the sum over the containers, and a container naming no
	// limit contributes nothing, so a sidecar alone can cap the pod.
	c.Resources.Limits = nil
	s.Operations[0].Template.Spec.Containers = append(s.Operations[0].Template.Spec.Containers, corev1.Container{
		Name:      "sidecar",
		Resources: corev1.ResourceRequirements{Limits: corev1.ResourceList{corev1.ResourceEphemeralStorage: resource.MustParse("1Gi")}},
	})
	if err := Validate(s); err == nil {
		t.Fatal("segments.size above a sidecar's ephemeral-storage limit accepted")
	}
	s.Operations[0].Template = container()
	// The controller leaves a pre-declared segment volume alone, so asking
	// for both is a contradiction rather than an override.
	s.Operations[0].Template.Spec.Volumes = []corev1.Volume{{Name: segmentVolumeName}}
	if err := Validate(s); err == nil {
		t.Fatal("segments.size alongside a pre-declared segment volume accepted")
	}
	s.Operations[0].Segments = nil
	if err := Validate(s); err != nil {
		t.Fatalf("pre-declared segment volume on its own rejected: %v", err)
	}
}
