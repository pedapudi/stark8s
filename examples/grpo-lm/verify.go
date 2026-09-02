package main

import (
	"encoding/json"
	"fmt"
	"math"
	"sort"
	"strconv"
	"strings"
)

// The reward is a program, not a model.
//
// A constraint is something a checker can decide by reading the completion:
// how many bullets it has, whether it stayed under a word budget, whether it
// parses as JSON with the keys that were asked for. Nothing here calls a
// judge, so the reward cannot drift, cannot be gamed by pleasing a scorer, and
// costs no accelerator.
//
// This is also what makes the task suitable for a small model at all. GRPO's
// gradient comes from variance *within* a group: if every completion scores
// the same, the standard deviation is zero, every advantage is zero and the
// update is exactly nothing. A task the model always fails produces no signal
// for the same reason a task it always passes does. Scoring the fraction of
// constraints met, rather than all-or-nothing, keeps a group's rewards spread
// out and keeps the gradient alive.
type constraint struct {
	Kind string `json:"kind"`
	Arg  string `json:"arg,omitempty"`
}

func (c constraint) String() string {
	if c.Arg == "" {
		return c.Kind
	}
	return c.Kind + ":" + c.Arg
}

// check reports whether one completion satisfies one constraint.
func (c constraint) check(text string) bool {
	switch c.Kind {
	case "bullets":
		n, err := strconv.Atoi(c.Arg)
		if err != nil {
			return false
		}
		return countBullets(text) == n

	case "lines":
		n, err := strconv.Atoi(c.Arg)
		if err != nil {
			return false
		}
		return len(nonEmptyLines(text)) == n

	case "maxwords":
		n, err := strconv.Atoi(c.Arg)
		if err != nil {
			return false
		}
		return len(strings.Fields(text)) <= n

	case "minwords":
		n, err := strconv.Atoi(c.Arg)
		if err != nil {
			return false
		}
		return len(strings.Fields(text)) >= n

	case "endswith":
		return strings.HasSuffix(strings.TrimSpace(text), c.Arg)

	case "startswith":
		return strings.HasPrefix(strings.TrimSpace(text), c.Arg)

	case "avoid":
		return !strings.Contains(strings.ToLower(text), strings.ToLower(c.Arg))

	case "json":
		var m map[string]json.RawMessage
		if err := json.Unmarshal([]byte(strings.TrimSpace(text)), &m); err != nil {
			return false
		}
		want := strings.Split(c.Arg, ",")
		if len(m) != len(want) {
			return false
		}
		for _, k := range want {
			if _, ok := m[strings.TrimSpace(k)]; !ok {
				return false
			}
		}
		return true

	case "uppercase":
		// Every word begins with a capital.
		for _, w := range strings.Fields(text) {
			r := []rune(w)
			if len(r) > 0 && !strings.ContainsRune("ABCDEFGHIJKLMNOPQRSTUVWXYZ", r[0]) {
				return false
			}
		}
		return len(strings.Fields(text)) > 0
	}
	return false
}

func countBullets(text string) int {
	n := 0
	for _, l := range nonEmptyLines(text) {
		t := strings.TrimSpace(l)
		if strings.HasPrefix(t, "- ") || strings.HasPrefix(t, "* ") {
			n++
		}
	}
	return n
}

func nonEmptyLines(text string) []string {
	var out []string
	for _, l := range strings.Split(text, "\n") {
		if strings.TrimSpace(l) != "" {
			out = append(out, l)
		}
	}
	return out
}

// task is one prompt and the constraints its completion has to satisfy.
type task struct {
	ID          string       `json:"id"`
	Instruction string       `json:"instruction"`
	Constraints []constraint `json:"constraints"`
}

// prompt renders what the model is asked. The constraints are stated in the
// prompt as well as checked, because the point is to teach the model to
// follow instructions it can read, not to guess a hidden rubric.
func (t task) prompt() string {
	var b strings.Builder
	b.WriteString(t.Instruction)
	b.WriteString("\n\nFollow every requirement:\n")
	for _, c := range t.Constraints {
		b.WriteString("- ")
		b.WriteString(describe(c))
		b.WriteString("\n")
	}
	return b.String()
}

func describe(c constraint) string {
	switch c.Kind {
	case "bullets":
		return fmt.Sprintf("use exactly %s bullet points, each starting with \"- \"", c.Arg)
	case "lines":
		return fmt.Sprintf("write exactly %s lines", c.Arg)
	case "maxwords":
		return fmt.Sprintf("use at most %s words in total", c.Arg)
	case "minwords":
		return fmt.Sprintf("use at least %s words in total", c.Arg)
	case "endswith":
		return fmt.Sprintf("end with exactly: %s", c.Arg)
	case "startswith":
		return fmt.Sprintf("begin with exactly: %s", c.Arg)
	case "avoid":
		return fmt.Sprintf("never use the word %q", c.Arg)
	case "json":
		return fmt.Sprintf("reply with a JSON object having exactly these keys: %s", c.Arg)
	case "uppercase":
		return "capitalise the first letter of every word"
	}
	return c.String()
}

// score is the fraction of the task's constraints the completion satisfies,
// and the per-constraint detail behind it. The detail is carried through the
// graph so a reader can see which requirement the model is still failing,
// which is the thing worth watching during a run.
func (t task) score(text string) (float64, map[string]bool) {
	if len(t.Constraints) == 0 {
		return 0, nil
	}
	detail := map[string]bool{}
	hit := 0.0
	for _, c := range t.Constraints {
		ok := c.check(text)
		detail[c.String()] = ok
		if ok {
			hit++
		}
	}
	return hit / float64(len(t.Constraints)), detail
}

// sortedTaskIDs keeps every operation's iteration order stable, so a run does
// not depend on Go's map ordering.
func sortedTaskIDs(m map[string]task) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

// advantages centres one group's rewards and scales them by their spread.
// This is the whole of GRPO's baseline: the group mean stands in for a value
// network, which is why there is no critic anywhere in the graph.
//
// It is the same function as in examples/grpo. Examples in this repository are
// self-contained, and this one is four lines of definition.
func advantages(rewards []float64) []float64 {
	n := float64(len(rewards))
	mean := 0.0
	for _, r := range rewards {
		mean += r / n
	}
	varsum := 0.0
	for _, r := range rewards {
		varsum += (r - mean) * (r - mean)
	}
	std := math.Sqrt(varsum / n)
	out := make([]float64, len(rewards))
	for i, r := range rewards {
		if std < 1e-8 {
			// Every completion scored the same, so the group says nothing
			// about which is better and contributes no gradient. The learner
			// counts these: a run where every group is flat is learning
			// nothing, however healthy it looks.
			out[i] = 0
			continue
		}
		out[i] = (r - mean) / std
	}
	return out
}
