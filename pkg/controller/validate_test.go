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
	s.Channels[2].Feedback.Mode = v1alpha1.FeedbackAsynchronous
	if err := Validate(s); err != nil {
		t.Fatalf("asynchronous feedback on a cycle rejected: %v", err)
	}
}

func TestValidateRejectsUnknownOperation(t *testing.T) {
	if err := Validate(spec(v1alpha1.Channel{Name: "x", From: "a", To: "zz"})); err == nil {
		t.Fatal("unknown consumer accepted")
	}
}

func TestValidateOverflowMustBeDeclared(t *testing.T) {
	loop := v1alpha1.Channel{Name: "loop", From: "a", To: "a",
		Feedback: &v1alpha1.Feedback{Mode: v1alpha1.FeedbackAsynchronous, MaxEpochs: 3, Overflow: "spill"}}
	if err := Validate(spec(loop)); err == nil {
		t.Fatal("undeclared overflow channel accepted")
	}
	if err := Validate(spec(loop, v1alpha1.Channel{Name: "spill", From: "a"})); err != nil {
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

func TestDesiredReplicas(t *testing.T) {
	s := &v1alpha1.WorkloadSpec{
		Operations: []v1alpha1.Operation{{Name: "src"}, {Name: "piped"}, {Name: "staged"}},
		Channels: []v1alpha1.Channel{
			{Name: "p", From: "src", To: "piped", Delivery: v1alpha1.DeliveryPipelined},
			{Name: "m", From: "piped", To: "staged", Delivery: v1alpha1.DeliveryMaterialized},
		},
	}
	op := func(name string, slots, min, max int32) *v1alpha1.Operation {
		return &v1alpha1.Operation{Name: name, Slots: slots, Scaling: v1alpha1.Scaling{Horizontal: v1alpha1.HorizontalScaling{Min: min, Max: max}}}
	}
	cases := []struct {
		name     string
		op       *v1alpha1.Operation
		runnable int32
		want     int32
	}{
		{"ceil of runnable over slots", op("staged", 2, 1, 10), 5, 3},
		{"slots default to one", op("staged", 0, 1, 10), 5, 5},
		{"clamped to max", op("staged", 1, 1, 4), 100, 4},
		{"raised to min", op("staged", 1, 3, 4), 1, 3},
		{"idle staged consumer uses min", op("staged", 1, 0, 4), 0, 0},
		{"idle source runs at least one", op("src", 1, 0, 4), 0, 1},
		{"idle pipelined consumer runs at least one", op("piped", 1, 0, 4), 0, 1},
		{"idle with min two", op("staged", 1, 2, 4), 0, 2},
	}
	for _, c := range cases {
		if got := desiredReplicas(s, c.op, c.runnable); got != c.want {
			t.Errorf("%s: got %d, want %d", c.name, got, c.want)
		}
	}
}
