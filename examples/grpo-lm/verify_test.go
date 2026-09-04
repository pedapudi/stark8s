package main

import (
	"fmt"
	"strings"
	"testing"
)

func TestConstraintsDecideByReading(t *testing.T) {
	cases := []struct {
		c    constraint
		text string
		want bool
	}{
		{constraint{"bullets", "3"}, "- one\n- two\n- three", true},
		{constraint{"bullets", "3"}, "- one\n- two", false},
		{constraint{"bullets", "2"}, "* one\n* two", true},
		{constraint{"lines", "2"}, "alpha\n\nbeta\n", true},
		{constraint{"maxwords", "4"}, "one two three", true},
		{constraint{"maxwords", "2"}, "one two three", false},
		{constraint{"minwords", "3"}, "one two three", true},
		{constraint{"minwords", "4"}, "one two three", false},
		{constraint{"endswith", "Thank you."}, "Here it is. Thank you.  ", true},
		{constraint{"endswith", "Thank you."}, "Thank you. Here it is.", false},
		{constraint{"startswith", "Answer:"}, "  Answer: 42", true},
		{constraint{"avoid", "very"}, "It is VERY good", false},
		{constraint{"avoid", "very"}, "It is good", true},
		{constraint{"json", "name,city"}, `{"name":"a","city":"b"}`, true},
		{constraint{"json", "name,city"}, `{"name":"a"}`, false},
		{constraint{"json", "name,city"}, `{"name":"a","city":"b","extra":1}`, false},
		{constraint{"json", "name,city"}, `not json`, false},
		{constraint{"uppercase", ""}, "Hello There World", true},
		{constraint{"uppercase", ""}, "Hello there World", false},
		{constraint{"uppercase", ""}, "", false},
		{constraint{"bullets", "notanumber"}, "- one", false},
	}
	for _, tc := range cases {
		if got := tc.c.check(tc.text); got != tc.want {
			t.Errorf("%s on %q = %v, want %v", tc.c, tc.text, got, tc.want)
		}
	}
}

// The reward has to be able to land strictly between 0 and 1, or a group has
// no spread and GRPO has no gradient.
func TestScoreGivesPartialCredit(t *testing.T) {
	tk := task{
		ID:          "t",
		Instruction: "Describe a city.",
		Constraints: []constraint{
			{"bullets", "3"},
			{"maxwords", "20"},
			{"endswith", "Done."},
		},
	}
	for _, tc := range []struct {
		name string
		text string
		want float64
	}{
		{"all three", "- a\n- b\n- c Done.", 1},
		{"two of three", "- a\n- b\n- c", 2.0 / 3.0},
		{"one of three", "a very long answer that runs well past the twenty word budget " +
			"and keeps going on and on and on past it Done.", 1.0 / 3.0},
		// Fails all three: no bullets, well over the word budget, wrong ending.
		{"none", "this answer has no bullet points at all and it deliberately runs " +
			"far past the twenty word budget so that the word constraint fails too", 0},
	} {
		got, detail := tk.score(tc.text)
		if got != tc.want {
			t.Errorf("%s: score %.3f, want %.3f (%v)", tc.name, got, tc.want, detail)
		}
		if len(detail) != len(tk.Constraints) {
			t.Errorf("%s: %d detail entries, want %d", tc.name, len(detail), len(tk.Constraints))
		}
	}
}

// Every constraint a generated instance can carry must be one the checker
// implements, or a prompt would ask for something that silently always scores
// zero. Instances are generated, so this samples the generator rather than
// enumerating a fixed set.
func TestGeneratedInstancesAreCheckable(t *testing.T) {
	seen := map[string]bool{}
	for i := 0; i < 200; i++ {
		tk := instance(fmt.Sprintf("probe-%d", i))
		if len(tk.Constraints) == 0 {
			t.Fatalf("%s: no constraints", tk.ID)
		}
		if tk.prompt() == "" {
			t.Fatalf("%s: empty prompt", tk.ID)
		}
		for _, c := range tk.Constraints {
			seen[c.Kind] = true
			if !anySatisfiable(c) {
				t.Errorf("%s: constraint %s is never satisfiable — unknown kind?", tk.ID, c)
			}
		}
	}
	for _, want := range []string{"exactwords", "avoid", "include"} {
		if !seen[want] {
			t.Errorf("generator never produced a %q constraint", want)
		}
	}
}

// The held-out set must not overlap anything the run trains on, or the
// measurement is of memorization.
func TestHeldOutIsDisjointFromTraining(t *testing.T) {
	h := map[string]bool{}
	for _, tk := range held(32) {
		h[tk.ID] = true
	}
	for step := int32(0); step < 200; step++ {
		for _, tk := range trainBatch(step, 16) {
			if h[tk.ID] {
				t.Fatalf("training instance %s is also in the held-out set", tk.ID)
			}
		}
	}
}

// Grading the word count is what keeps a group's rewards spread out. If a
// near miss scored the same as a wild miss, the only signal left would be the
// two binary constraints.
func TestWordCountIsGradedByDistance(t *testing.T) {
	c := constraint{"exactwords", "20"}
	exact := strings.Repeat("alpha ", 20)
	near := strings.Repeat("alpha ", 18)
	far := strings.Repeat("alpha ", 5)
	e, n, f := c.partial(exact), c.partial(near), c.partial(far)
	if e != 1 {
		t.Errorf("exact = %v, want 1", e)
	}
	if !(e > n && n > f) {
		t.Errorf("not ordered by distance: exact=%v near=%v far=%v", e, n, f)
	}
}

// A word must match as a word. Substring matching would fail an avoid("the")
// constraint on the word "theatre", which the model could never diagnose.
func TestAvoidMatchesWholeWordsOnly(t *testing.T) {
	c := constraint{"avoid", "the"}
	if !c.check("A theatre opened on Tuesday") {
		t.Error("avoid(the) rejected a sentence containing only \"theatre\"")
	}
	if c.check("A shop on the corner") {
		t.Error("avoid(the) accepted a sentence containing \"the\"")
	}
	if !c.check("Grandstand seating, thereabouts") {
		t.Error("avoid(the) rejected words that merely contain the letters")
	}
}

func anySatisfiable(c constraint) bool {
	probes := []string{
		"- a\n- b\n- c", "- a\n- b", "a b c", "Answer: 42",
		`{"name":"a","city":"b"}`, "Hello There", "short Done.",
		"one\ntwo", "x", "Answer: done",
	}
	// Give endswith/startswith/avoid/json a probe built from their own argument.
	switch c.Kind {
	case "endswith":
		probes = append(probes, "text "+c.Arg)
	case "startswith":
		probes = append(probes, c.Arg+" text")
	case "json":
		obj := "{"
		for i, k := range splitCSV(c.Arg) {
			if i > 0 {
				obj += ","
			}
			obj += `"` + k + `":1`
		}
		probes = append(probes, obj+"}")
	case "bullets", "lines":
		b := ""
		n := atoiOr(c.Arg, 0)
		for i := 0; i < n; i++ {
			if i > 0 {
				b += "\n"
			}
			b += "- item"
		}
		probes = append(probes, b)
	case "include":
		probes = append(probes, "some text "+c.Arg+" more text")
	case "minwords", "exactwords":
		s := ""
		for i := 0; i < atoiOr(c.Arg, 0); i++ {
			s += "word "
		}
		probes = append(probes, s)
	}
	for _, p := range probes {
		if c.check(p) {
			return true
		}
	}
	return false
}

// Calibration on the real model found eight of eight completions returning
// correct JSON inside a ```json fence and scoring zero. The wrapper is not
// the answer.
func TestFencedJSONCounts(t *testing.T) {
	c := constraint{"json", "name,city"}
	for _, s := range []string{
		"```json\n{\"name\":\"a\",\"city\":\"b\"}\n```",
		"```\n{\"name\":\"a\",\"city\":\"b\"}\n```",
		"{\"name\":\"a\",\"city\":\"b\"}",
	} {
		if !c.check(s) {
			t.Errorf("fenced JSON rejected: %q", s)
		}
	}
	if c.check("```json\n{\"name\":\"a\"}\n```") {
		t.Error("fence stripping must not excuse a missing key")
	}
}

func TestExactWords(t *testing.T) {
	c := constraint{"exactwords", "3"}
	if !c.check("one two three") || c.check("one two") || c.check("one two three four") {
		t.Error("exactwords is not exact")
	}
}
