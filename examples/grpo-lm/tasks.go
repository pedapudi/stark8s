package main

import "strconv"

// The task set. Small on purpose: the point is a working post-training loop,
// not a benchmark. Every constraint is one the checker in verify.go decides by
// reading the completion.
//
// The constraints are chosen so a small instruction-tuned model gets some of
// them right and some wrong. That is the requirement GRPO actually imposes: a
// task the model always fails gives a group with no spread, which gives a zero
// standard deviation, which gives zero advantages and no gradient at all. The
// run would look healthy and learn nothing.
var tasks = map[string]task{
	"summary": {
		ID:          "summary",
		Instruction: "Summarise why a small team might prefer a managed database.",
		Constraints: []constraint{
			{"bullets", "3"},
			{"maxwords", "27"},
			{"avoid", "and"},
		},
	},
	"record": {
		ID:          "record",
		Instruction: "Give the details of a fictional public library.",
		Constraints: []constraint{
			{"json", "name,city,founded,motto"},
			{"maxwords", "16"},
			{"avoid", "library"},
		},
	},
	"headline": {
		ID:          "headline",
		Instruction: "Write a headline for an article about urban cycling.",
		Constraints: []constraint{
			{"lines", "1"},
			{"uppercase", ""},
			{"avoid", "bikes"},
			{"avoid", "now"},
		},
	},
	"reply": {
		ID:          "reply",
		Instruction: "Reply to a customer asking when their order will arrive.",
		Constraints: []constraint{
			{"startswith", "Answer:"},
			{"endswith", "Thank you."},
			{"maxwords", "27"},
			{"avoid", "order"},
		},
	},
}

func splitCSV(s string) []string {
	var out []string
	cur := ""
	for _, r := range s {
		if r == ',' {
			out = append(out, cur)
			cur = ""
			continue
		}
		cur += string(r)
	}
	if cur != "" {
		out = append(out, cur)
	}
	return out
}

func atoiOr(s string, d int) int {
	n, err := strconv.Atoi(s)
	if err != nil {
		return d
	}
	return n
}
