package main

import "testing"

func TestUnfenceKeepsTheProof(t *testing.T) {
	want := "theorem t : 1 + 1 = 2 := by norm_num"
	for _, s := range []string{
		want,
		"```lean\n" + want + "\n```",
		"```\n" + want + "\n```",
		"  " + want + "  ",
	} {
		if got := unfence(s); got != want {
			t.Errorf("unfence(%q) = %q", s, got)
		}
	}
}

func TestFirstLineIsBounded(t *testing.T) {
	if got := firstLine("a\nb\nc"); got != "a" {
		t.Errorf("got %q", got)
	}
	long := make([]byte, 400)
	for i := range long {
		long[i] = 'x'
	}
	if got := firstLine(string(long)); len(got) != 160 {
		t.Errorf("len %d, want 160", len(got))
	}
	if firstLine("   ") != "" {
		t.Error("blank should be empty")
	}
}
