package controller

import (
	"testing"

	"github.com/pedapudi/stark8s/api/v1alpha1"
)

func spec(chans ...v1alpha1.Channel) *v1alpha1.WorkloadSpec {
	return &v1alpha1.WorkloadSpec{
		Operations: []v1alpha1.Operation{{Name: "a"}, {Name: "b"}, {Name: "c"}},
		Channels:   chans,
	}
}

func TestValidateRejectsCycleWithoutFeedback(t *testing.T) {
	s := spec(
		v1alpha1.Channel{Name: "ab", From: "a", To: "b"},
		v1alpha1.Channel{Name: "bc", From: "b", To: "c"},
		v1alpha1.Channel{Name: "ca", From: "c", To: "a"},
	)
	if err := Validate(s); err == nil {
		t.Fatal("cycle without feedback accepted")
	}
	s.Channels[2].Feedback = &v1alpha1.Feedback{MaxEpochs: 5}
	if err := Validate(s); err != nil {
		t.Fatal(err)
	}
}

func TestValidateRejectsUnknownOperation(t *testing.T) {
	if err := Validate(spec(v1alpha1.Channel{Name: "x", From: "a", To: "zz"})); err == nil {
		t.Fatal("unknown consumer accepted")
	}
}

func TestDesiredReplicas(t *testing.T) {
	op := &v1alpha1.Operation{Scaling: v1alpha1.Scaling{Horizontal: v1alpha1.HorizontalScaling{Min: 1, Max: 4, TargetBacklogPerReplica: 100}}}
	if got := desiredReplicas(op, 0, 0); got != 1 {
		t.Fatalf("idle: %d", got)
	}
	if got := desiredReplicas(op, 1, 250); got != 3 {
		t.Fatalf("backlog 250: %d", got)
	}
	if got := desiredReplicas(op, 1, 10000); got != 4 {
		t.Fatalf("clamped: %d", got)
	}
	if got := desiredReplicas(op, 3, 0); got != 3 {
		t.Fatalf("never below current: %d", got)
	}
}
