package main

import "testing"

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

// Every constraint the task set uses must be one the checker implements, or a
// prompt would ask for something that silently always scores zero.
func TestTaskSetIsCheckable(t *testing.T) {
	for _, id := range sortedTaskIDs(tasks) {
		tk := tasks[id]
		if len(tk.Constraints) == 0 {
			t.Errorf("%s: no constraints", id)
		}
		for _, c := range tk.Constraints {
			// A constraint the checker does not know always returns false;
			// prove each kind can be satisfied by something.
			if !anySatisfiable(c) {
				t.Errorf("%s: constraint %s is never satisfiable — unknown kind?", id, c)
			}
		}
		if tk.prompt() == "" {
			t.Errorf("%s: empty prompt", id)
		}
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
	case "minwords":
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
