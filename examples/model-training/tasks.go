package main

import (
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"strconv"
	"strings"
)

// Instances are generated, not enumerated.
//
// A fixed pool of tasks cannot show that anything was learned. Measured on
// this model: training on a fixed pool moved train reward 0.805 -> 0.861 while
// held-out reward fell 0.845 -> 0.831, which is what fitting the pool looks
// like. Drawing a fresh instance every step instead removes the thing to
// memorize, but then the reward compares different problems each step and
// instance difficulty swamps the signal; a run that way fell 0.813 -> 0.742
// and there was no way to tell that from harder draws.
//
// So: fresh instances to train on, and a fixed disjoint set to measure on.
// Neither alone answers the question.

// subjects are the things to write about. They carry no difficulty of their
// own; the constraints are what the model has to satisfy.
var subjects = []string{
	"a neighborhood bakery", "a public library", "a bicycle repair shop",
	"a rooftop garden", "a night bus route", "a community radio station",
	"a swimming pool", "a hardware store", "a botanical garden",
	"a train station cafe", "a repair cafe", "a food market",
	"a music venue", "a bookshop", "a climbing gym", "a pottery studio",
	"a tool library", "a weather station", "a lighthouse", "a ferry terminal",
	"a seed bank", "a print workshop", "a youth center", "a dog park",
	"a planetarium", "a tram depot", "a farmers market", "a language school",
	"a bird sanctuary", "a stationery shop", "a cheese counter", "a tea room",
}

// banned are ordinary words a writer would otherwise reach for, so the
// avoid-constraint bites without being impossible.
var banned = []string{
	"the", "and", "very", "good", "great", "place", "people", "you",
	"local", "best", "new", "make", "help", "offer", "provide", "space",
}

// required are terms the completion must contain. They are unrelated to the
// subject on purpose: including one is a deliberate act, not an accident of
// writing about the topic.
var required = []string{
	"Tuesday", "seventeen", "quietly", "granite", "lantern", "compass",
	"velvet", "harbor", "amber", "thistle", "copper", "meadow",
}

func seedOf(tag string) uint64 {
	h := sha256.Sum256([]byte(tag))
	return binary.BigEndian.Uint64(h[:8])
}

// instance builds one task from a tag. The same tag always gives the same
// task, which is what makes the held-out set a fixed set and makes any
// training instance reproducible from its name alone.
//
// Every instance carries the same three constraint kinds so the per-constraint
// rates are comparable across instances and across steps. What varies is the
// word budget, the banned word, the required term and the subject.
func instance(tag string) task {
	s := seedOf(tag)
	pick := func(shift uint, n int) int { return int((s >> shift) % uint64(n)) }

	subject := subjects[pick(0, len(subjects))]
	words := 14 + pick(8, 13) // 14..26 words
	avoid := banned[pick(16, len(banned))]
	need := required[pick(24, len(required))]

	return task{
		ID:          tag,
		Instruction: fmt.Sprintf("Write one sentence describing %s.", subject),
		Constraints: []constraint{
			{"exactwords", strconv.Itoa(words)},
			{"avoid", avoid},
			{"include", need},
		},
	}
}

// held is the measurement set: fixed, disjoint from anything trained on, and
// scored with identical instances at every evaluation so that a change in the
// number is a change in the policy rather than a change in the problems.
func held(n int) []task {
	out := make([]task, n)
	for i := range out {
		out[i] = instance(fmt.Sprintf("held-%03d", i))
	}
	return out
}

// trainTag names the instance for one slot at one step. The slot is what the
// batch channel partitions across rollout replicas; the step is what makes the
// instance behind it new every time.
func trainTag(step int32, slot int) string {
	return fmt.Sprintf("train-%d-%d", step, slot)
}

// trainBatch draws instances no evaluation ever sees.
func trainBatch(step int32, slots int) []task {
	out := make([]task, slots)
	for i := range out {
		out[i] = instance(trainTag(step, i))
	}
	return out
}

func splitCSV(s string) []string {
	var out []string
	for _, p := range strings.Split(s, ",") {
		if p != "" {
			out = append(out, p)
		}
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
