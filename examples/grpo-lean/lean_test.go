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

func TestClassifyOrdersTheWaysACandidateIsWrong(t *testing.T) {
	for _, c := range []struct {
		name   string
		text   string
		exitOK bool
		want   float64
		cat    string
	}{
		{"clean compile", "", true, 1.0, "proved"},
		{"sorry type-checks but proves nothing", "warning: declaration uses 'sorry'", true, 0.1, "other-error"},
		{"well-formed tactic leaves goals", "error: unsolved goals\n⊢ 0 ≤ x ^ 2", false, 0.4, "unsolved"},
		{"invented lemma name", "error: unknown identifier 'nonneg_of_square'", false, 0.1, "unknown-lemma"},
		{"not Lean at all", "error: unexpected token ':='; expected term", false, 0.0, "parse-error"},
	} {
		got, cat := classify(c.text, c.exitOK)
		if got != c.want || cat != c.cat {
			t.Errorf("%s: classify() = %v, %q; want %v, %q", c.name, got, cat, c.want, c.cat)
		}
	}
}

// A binary reward is what makes GRPO train on nothing here, so the tiers must
// stay ordered: any mistake that collapses two of them reintroduces the
// degenerate groups the grading exists to avoid.
func TestClassifyTiersAreStrictlyOrdered(t *testing.T) {
	proved, _ := classify("", true)
	unsolved, _ := classify("error: unsolved goals", false)
	unknown, _ := classify("error: unknown constant 'foo'", false)
	parse, _ := classify("error: unexpected token", false)
	if !(proved > unsolved && unsolved > unknown && unknown > parse) {
		t.Errorf("tiers not ordered: proved=%v unsolved=%v unknown=%v parse=%v",
			proved, unsolved, unknown, parse)
	}
}
