package main

import (
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"math"
	"math/rand"
	"strings"
)

// The instance is derived from (slot, step), never stored.
//
// `slot` is the partition key: it is what `prompts` emits once and what a
// rollout replica owns for the whole run, so the Hash partition on `scored`
// still routes a slot's completions to one advantage replica. `step` makes the
// instance fresh every round, so there is nothing to memorise. The graph does
// not change.
func seedOf(slot string, step int32) int64 {
	h := sha256.Sum256([]byte(fmt.Sprintf("%s:%d", slot, step)))
	return int64(binary.BigEndian.Uint64(h[:8]))
}

type city struct{ X, Y int }

func instance(slot string, step int32, n int) []city {
	rng := rand.New(rand.NewSource(seedOf(slot, step)))
	out := make([]city, n)
	for i := range out {
		out[i] = city{rng.Intn(100), rng.Intn(100)}
	}
	return out
}

func prompt(cs []city) string {
	var b strings.Builder
	b.WriteString("Visit every city exactly once and return to the start.\n\nCities (index: x y):\n")
	for i, c := range cs {
		fmt.Fprintf(&b, "%d: %d %d\n", i, c.X, c.Y)
	}
	fmt.Fprintf(&b, "\nReply with only the visiting order: %d comma-separated indices, "+
		"each used exactly once.", len(cs))
	return b.String()
}

func dist(a, b city) float64 {
	dx, dy := float64(a.X-b.X), float64(a.Y-b.Y)
	return math.Sqrt(dx*dx + dy*dy)
}

func tourLen(cs []city, t []int) float64 {
	s := 0.0
	for i := range t {
		s += dist(cs[t[i]], cs[t[(i+1)%len(t)]])
	}
	return s
}

// reference is nearest neighbour plus 2-opt: a strong baseline, not a claim of
// optimality, which is why the reward is capped at 1 rather than allowed to
// exceed it.
func reference(cs []city) []int {
	n := len(cs)
	left := map[int]bool{}
	for i := 1; i < n; i++ {
		left[i] = true
	}
	t := []int{0}
	for len(left) > 0 {
		best, bd := -1, math.Inf(1)
		for c := range left {
			if d := dist(cs[t[len(t)-1]], cs[c]); d < bd {
				best, bd = c, d
			}
		}
		t = append(t, best)
		delete(left, best)
	}
	for improved := true; improved; {
		improved = false
		for i := 1; i < n-1; i++ {
			for j := i + 1; j < n; j++ {
				a := append([]int{}, t[:i]...)
				for k := j; k >= i; k-- {
					a = append(a, t[k])
				}
				a = append(a, t[j+1:]...)
				if tourLen(cs, a) < tourLen(cs, t) {
					t, improved = a, true
				}
			}
		}
	}
	return t
}

// parseTour is deliberately lenient about surrounding prose but strict about
// the permutation: every city exactly once, or the completion scores zero.
func parseTour(text string, n int) ([]int, bool) {
	var nums []int
	cur := ""
	flush := func() {
		if cur != "" {
			v := 0
			fmt.Sscanf(cur, "%d", &v)
			nums = append(nums, v)
			cur = ""
		}
	}
	for _, r := range text {
		if r >= '0' && r <= '9' {
			cur += string(r)
		} else {
			flush()
		}
	}
	flush()
	if len(nums) < n {
		return nil, false
	}
	// The answer is the last n integers, so a model that reasons out loud is
	// not punished for the numbers inside its reasoning.
	nums = nums[len(nums)-n:]
	seen := make([]bool, n)
	for _, v := range nums {
		if v < 0 || v >= n || seen[v] {
			return nil, false
		}
		seen[v] = true
	}
	return nums, true
}

// reward is a staircase, because emitting a valid permutation is the first
// thing to learn and a binary reward would give a group no spread.
func reward(cs []city, text string) float64 {
	t, ok := parseTour(text, len(cs))
	if !ok {
		return 0
	}
	return 0.3 + 0.7*math.Min(1.0, tourLen(cs, reference(cs))/tourLen(cs, t))
}

// advantages centres one group's rewards: GRPO's baseline, and the reason no
// critic appears anywhere in the graph.
func advantages(rs []float64) []float64 {
	n := float64(len(rs))
	mean := 0.0
	for _, r := range rs {
		mean += r / n
	}
	v := 0.0
	for _, r := range rs {
		v += (r - mean) * (r - mean)
	}
	sd := math.Sqrt(v / n)
	out := make([]float64, len(rs))
	if sd < 1e-8 {
		return out // a flat group says nothing and contributes no gradient
	}
	for i, r := range rs {
		out[i] = (r - mean) / sd
	}
	return out
}
