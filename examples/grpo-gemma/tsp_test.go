package main

import "testing"

func TestRewardStaircase(t *testing.T) {
	cs := instance("slot-0", 0, 8)
	ref := reference(cs)
	perfect := ""
	for i, v := range ref {
		if i > 0 {
			perfect += ","
		}
		perfect += string(rune('0' + v))
	}
	if r := reward(cs, perfect); r < 0.99 {
		t.Errorf("reference tour scored %.3f, want ~1.0", r)
	}
	if r := reward(cs, "0,1,2,3,4,5,6,7"); r < 0.3 || r > 1.0 {
		t.Errorf("valid tour scored %.3f, want in (0.3,1.0]", r)
	}
	if r := reward(cs, "0,1,2,3,4,5,6"); r != 0 {
		t.Errorf("short permutation scored %.3f, want 0", r)
	}
	if r := reward(cs, "no tour here"); r != 0 {
		t.Errorf("prose scored %.3f, want 0", r)
	}
	// A model that reasons out loud must not be punished for stray integers.
	if r := reward(cs, "City 3 is closest at distance 25.\nAnswer: 0,1,2,3,4,5,6,7"); r == 0 {
		t.Error("reasoning before the answer should still parse")
	}
}

func TestFlatGroupHasNoGradient(t *testing.T) {
	for _, a := range advantages([]float64{0.5, 0.5, 0.5}) {
		if a != 0 {
			t.Errorf("flat group gave advantage %v", a)
		}
	}
	m := advantages([]float64{0.2, 0.5, 0.9})
	if m[0] >= 0 || m[2] <= 0 {
		t.Errorf("mixed group not signed: %v", m)
	}
}
