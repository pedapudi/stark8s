package main

import (
	"hash/fnv"
	"math"
	"math/rand"
)

// The policy is conditioned on the prompt, because that is what a policy is:
// theta[q][t][v] scores token v at position t of a completion for prompt q. A
// completion is one token per position, so a rollout is T independent draws
// and the log-probability of a completion is the sum of the T
// log-probabilities.
//
// Conditioning is not a detail that can be dropped for a small example. One
// table shared across prompts that want different answers has no setting that
// satisfies them: the gradients cancel and the reward never moves. A language
// model conditions by reading the prompt; here the prompt indexes the table.
//
// This is the smallest policy class that exercises every part of GRPO — group
// baseline, importance ratio, clipping, KL to a reference — with gradients in
// closed form, so the example needs no autodiff and no accelerator. Swapping
// in a language model changes what sample and step do, and changes nothing
// about the graph.
type policy struct {
	T, V  int
	Theta map[string][]float64 // prompt -> T*V, row-major
}

func newPolicy(t, v int, prompts []string) *policy {
	p := &policy{T: t, V: v, Theta: map[string][]float64{}}
	for _, q := range prompts {
		p.Theta[q] = make([]float64, t*v)
	}
	return p
}

func (p *policy) clone() *policy {
	q := &policy{T: p.T, V: p.V, Theta: map[string][]float64{}}
	for k, row := range p.Theta {
		cp := make([]float64, len(row))
		copy(cp, row)
		q.Theta[k] = cp
	}
	return q
}

// probs returns the distribution at position t of a completion for prompt q.
func (p *policy) probs(q string, t int) []float64 {
	row := p.Theta[q][t*p.V : (t+1)*p.V]
	max := math.Inf(-1)
	for _, x := range row {
		if x > max {
			max = x
		}
	}
	out, sum := make([]float64, p.V), 0.0
	for i, x := range row {
		out[i] = math.Exp(x - max)
		sum += out[i]
	}
	for i := range out {
		out[i] /= sum
	}
	return out
}

// sample draws a completion. The stream is seeded from the prompt, the step
// and the index within the group, so which replica happens to run a rollout
// cannot change what it draws: the run is reproducible at any pod count.
func (p *policy) sample(prompt string, step int32, index int) ([]int, float64) {
	h := fnv.New64a()
	_, _ = h.Write([]byte(prompt))
	seed := int64(h.Sum64()) ^ int64(step)*1_000_003 ^ int64(index)*7919
	rng := rand.New(rand.NewSource(seed))

	tokens, lp := make([]int, p.T), 0.0
	for t := 0; t < p.T; t++ {
		pr := p.probs(prompt, t)
		u, acc, pick := rng.Float64(), 0.0, p.V-1
		for v, q := range pr {
			if acc += q; u < acc {
				pick = v
				break
			}
		}
		tokens[t] = pick
		lp += math.Log(pr[pick])
	}
	return tokens, lp
}

// klTo is the exact KL divergence from this policy to ref, summed over
// positions. The policy is categorical, so this is a sum rather than the
// sampled estimator a language model would need.
func (p *policy) klTo(ref *policy) float64 {
	kl := 0.0
	for q := range p.Theta {
		for t := 0; t < p.T; t++ {
			a, b := p.probs(q, t), ref.probs(q, t)
			for v := range a {
				if a[v] > 0 {
					kl += a[v] * math.Log(a[v]/b[v])
				}
			}
		}
	}
	return kl
}

// sampleRec is one completion with the advantage its group gave it and the
// log-probability the policy had when it was drawn.
type sampleRec struct {
	Prompt  string  `json:"prompt"`
	Tokens  []int   `json:"tokens"`
	OldLogP float64 `json:"oldLogP"`
	Adv     float64 `json:"adv"`
}

// step applies one GRPO update in place and returns the mean clipped
// objective and the KL to the reference.
//
// For each sample the importance ratio is exp(logp - oldLogP), and the
// surrogate is the smaller of ratio*A and clip(ratio, 1±eps)*A. With one
// gradient step per batch of rollouts the ratio starts at exactly 1 and the
// clip is inactive; it is written out in full because that stops being true
// the moment a second inner step is taken.
//
// Every gradient here is analytic. For a categorical position,
// d logP(o) / d theta[t][v] = 1[v == o_t] - p_t(v), and the KL term
// differentiates to beta * p_t(v) * (log(p_t(v)/pref_t(v)) - KL_t).
func (p *policy) step(batch []sampleRec, ref *policy, lr, eps, beta float64) (obj, kl float64) {
	if len(batch) == 0 {
		return 0, p.klTo(ref)
	}
	grad := map[string][]float64{}
	probs := map[string][][]float64{}
	for q := range p.Theta {
		grad[q] = make([]float64, len(p.Theta[q]))
		rows := make([][]float64, p.T)
		for t := range rows {
			rows[t] = p.probs(q, t)
		}
		probs[q] = rows
	}
	// Each prompt's parameters see only that prompt's group, so the step is
	// normalised per group rather than over the whole batch. Dividing by the
	// total would make the effective step size depend on how many prompts
	// happen to be batched together, which is not a property of the algorithm.
	count := map[string]float64{}
	for _, s := range batch {
		count[s.Prompt]++
	}
	n := float64(len(batch))

	for _, s := range batch {
		pr, ok := probs[s.Prompt]
		if !ok {
			continue
		}
		lp := 0.0
		for t, tok := range s.Tokens {
			lp += math.Log(pr[t][tok])
		}
		ratio := math.Exp(lp - s.OldLogP)
		clipped := math.Min(math.Max(ratio, 1-eps), 1+eps)
		// The surrogate is a min, so only one branch carries gradient; when
		// the clipped branch wins, the ratio is held constant and this term
		// contributes nothing.
		use, live := ratio*s.Adv, true
		if c := clipped * s.Adv; c < use {
			use, live = c, false
		}
		obj += use / n
		if !live {
			continue
		}
		// d/dtheta [ratio * A] = ratio * A * d logP
		coef := ratio * s.Adv / count[s.Prompt]
		g := grad[s.Prompt]
		for t, tok := range s.Tokens {
			row := t * p.V
			for v := 0; v < p.V; v++ {
				ind := 0.0
				if v == tok {
					ind = 1
				}
				g[row+v] += coef * (ind - pr[t][v])
			}
		}
	}

	// KL pulls every prompt's distribution back toward the reference.
	for q, rows := range probs {
		g := grad[q]
		for t := 0; t < p.T; t++ {
			a, b := rows[t], ref.probs(q, t)
			klT := 0.0
			for v := range a {
				if a[v] > 0 {
					klT += a[v] * math.Log(a[v]/b[v])
				}
			}
			kl += klT
			row := t * p.V
			for v := range a {
				g[row+v] -= beta * a[v] * (math.Log(a[v]/b[v]) - klT)
			}
		}
	}

	// Ascend the surrogate.
	for q, g := range grad {
		row := p.Theta[q]
		for i := range row {
			row[i] += lr * g[i]
		}
	}
	return obj, kl
}

// advantages centres the rewards of one group and scales them by their spread.
// This is the whole of GRPO's baseline: the group mean stands in for a value
// network, which is why there is no critic anywhere in the graph.
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
			// A group whose samples all scored the same says nothing about
			// which is better, so it contributes no gradient.
			out[i] = 0
			continue
		}
		out[i] = (r - mean) / std
	}
	return out
}
