// Command grpo runs Group Relative Policy Optimization (GRPO) as a cyclic
// workload. The policy is a table of logits, one row per prompt and one
// column per action, so the whole loop runs without a model server; the
// graph is the point.
//
//	prompts  (source) emits one record per prompt, keyed by the prompt.
//	sampler  holds the prompts that hash to it. For every policy version it
//	         receives it draws a group of actions per prompt and emits each
//	         sample twice: to reward, to be scored, and to trainer, to be
//	         trained on. Version 0 is the uniform policy, so sampling starts
//	         as soon as the prompts arrive.
//	reward   scores a sample: 1 when the action is the prompt's target,
//	         otherwise 0. It is the only operation that knows the target.
//	trainer  joins samples with rewards by (prompt, version, index). When
//	         every group of a version is complete it takes a fixed number of
//	         clipped policy-gradient steps with group-normalised advantages
//	         and a penalty toward the initial policy, broadcasts the new
//	         version on weights, and writes the mean reward to metrics. The
//	         version produced after the last step is diverted to final-policy
//	         by the loop bound.
//
// Sampling is seeded by (prompt, version, index), so the run is
// deterministic whichever replica draws a sample and in whatever order
// records arrive. All state a replica holds is rebuilt from the records it
// receives: a sampler holds prompts and the latest policy, the trainer holds
// the policy and the incomplete batches.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"math"
	"math/rand/v2"
	"sort"
	"strconv"
	"strings"

	"github.com/pedapudi/stark8s/pkg/sdk"
)

// Config is the shape of the learning problem and of the optimiser. Every
// operation is started with the same values.
type Config struct {
	// Prompts is the number of prompts. The trainer needs it to know when a
	// version's batch is complete.
	Prompts int
	// Actions is the number of actions the policy chooses between.
	Actions int
	// Group is the number of samples drawn per prompt per version.
	Group int
	// Iterations is the number of gradient steps taken on one batch. Only
	// after the first does the probability ratio depart from one, so the
	// clip is inactive with Iterations = 1.
	Iterations int
	// Rate is the step size.
	Rate float64
	// Clip bounds the probability ratio at 1 ± Clip.
	Clip float64
	// KL weights the penalty toward the initial policy.
	KL float64
}

// Sample is one drawn action with the log-probability the policy assigned
// to it at the time. The log-probability travels with the sample so the
// trainer can form the probability ratio against a later version.
type Sample struct {
	Prompt  string  `json:"prompt"`
	Version int32   `json:"version"`
	Index   int     `json:"index"`
	Action  int     `json:"action"`
	LogProb float64 `json:"logProb"`
}

// Score is the reward for one sample, identified the way the sample is.
type Score struct {
	Version int32   `json:"version"`
	Index   int     `json:"index"`
	Reward  float64 `json:"reward"`
}

// Policy is one version of the parameters: a row of logits per prompt.
type Policy struct {
	Version int32                `json:"version"`
	Logits  map[string][]float64 `json:"logits"`
}

// Metric is what the trainer reports after each step.
type Metric struct {
	Version    int32   `json:"version"`
	MeanReward float64 `json:"meanReward"`
}

// promptName names prompt i. targetOf inverts it and returns the action
// that earns the reward; only the prompts source and the reward operation
// call it.
func promptName(i int) string { return "p" + strconv.Itoa(i) }

func targetOf(prompt string, actions int) (int, error) {
	i, err := strconv.Atoi(strings.TrimPrefix(prompt, "p"))
	if err != nil {
		return 0, fmt.Errorf("prompt %q is not p<n>: %w", prompt, err)
	}
	return (i*5 + 1) % actions, nil
}

// softmax turns a row of logits into probabilities.
func softmax(logits []float64) []float64 {
	max := math.Inf(-1)
	for _, l := range logits {
		max = math.Max(max, l)
	}
	p := make([]float64, len(logits))
	var z float64
	for i, l := range logits {
		p[i] = math.Exp(l - max)
		z += p[i]
	}
	for i := range p {
		p[i] /= z
	}
	return p
}

// draw samples one action from probabilities p with r.
func draw(p []float64, r *rand.Rand) int {
	u := r.Float64()
	for i, pi := range p {
		u -= pi
		if u < 0 {
			return i
		}
	}
	return len(p) - 1
}

// seed makes sampling a function of (prompt, version, index) alone.
func seed(prompt string, version int32, index int) *rand.Rand {
	var h uint64 = 1469598103934665603
	for _, b := range []byte(prompt) {
		h = (h ^ uint64(b)) * 1099511628211
	}
	return rand.New(rand.NewPCG(h, uint64(version)<<32|uint64(index)))
}

// sampleGroup draws the group for one prompt under one policy version.
func sampleGroup(cfg Config, prompt string, version int32, logits []float64) []Sample {
	p := softmax(logits)
	out := make([]Sample, cfg.Group)
	for g := range out {
		a := draw(p, seed(prompt, version, g))
		out[g] = Sample{Prompt: prompt, Version: version, Index: g, Action: a, LogProb: math.Log(p[a])}
	}
	return out
}

// --- the trainer ------------------------------------------------------------

// group is one prompt's samples and rewards for one version, filled in as
// records arrive in either order. Indexing by sample index makes a
// redelivered record overwrite rather than duplicate.
type group struct {
	samples map[int]Sample
	rewards map[int]float64
}

func (g *group) complete(n int) bool { return len(g.samples) == n && len(g.rewards) == n }

// advantages returns the group-normalised advantage of each sample. A group
// whose rewards are all equal has no signal and gets zero advantages.
func (g *group) advantages(n int) []float64 {
	var mean float64
	for i := 0; i < n; i++ {
		mean += g.rewards[i]
	}
	mean /= float64(n)
	var variance float64
	for i := 0; i < n; i++ {
		d := g.rewards[i] - mean
		variance += d * d
	}
	std := math.Sqrt(variance / float64(n))
	adv := make([]float64, n)
	if std == 0 {
		return adv
	}
	for i := 0; i < n; i++ {
		adv[i] = (g.rewards[i] - mean) / std
	}
	return adv
}

// step updates one prompt's logits from its completed group. It takes
// cfg.Iterations gradient steps of the clipped surrogate objective, with a
// penalty toward the reference row, and returns the new row.
//
// For a sample with advantage A drawn at log-probability q, the ratio is
// r = π(a)/exp(q). The surrogate is min(rA, clip(r)A), so the gradient
// flows only while the unclipped term is the smaller: r ≤ 1+Clip for A > 0
// and r ≥ 1−Clip for A < 0. The penalty is the estimator
// ρ − log ρ − 1 with ρ = πref(a)/π(a), whose gradient with respect to the
// logits is (1 − ρ) ∇log π(a). ∇log π(a) for a softmax row is
// onehot(a) − π.
func step(cfg Config, logits, ref []float64, g *group) []float64 {
	row := append([]float64(nil), logits...)
	adv := g.advantages(cfg.Group)
	for it := 0; it < cfg.Iterations; it++ {
		p := softmax(row)
		pref := softmax(ref)
		grad := make([]float64, len(row))
		for i := 0; i < cfg.Group; i++ {
			s := g.samples[i]
			a := s.Action
			ratio := p[a] / math.Exp(s.LogProb)
			scale := 0.0
			if (adv[i] >= 0 && ratio <= 1+cfg.Clip) || (adv[i] < 0 && ratio >= 1-cfg.Clip) {
				scale = ratio * adv[i]
			}
			rho := pref[a] / p[a]
			scale -= cfg.KL * (1 - rho)
			for k := range grad {
				d := -p[k]
				if k == a {
					d += 1
				}
				grad[k] += scale * d / float64(cfg.Group)
			}
		}
		for k := range row {
			row[k] += cfg.Rate * grad[k]
		}
	}
	return row
}

// trainer holds the current policy and the batches still being joined.
type trainer struct {
	cfg     Config
	policy  Policy
	ref     []float64
	batches map[int32]map[string]*group
}

func newTrainer(cfg Config) *trainer {
	t := &trainer{cfg: cfg, ref: make([]float64, cfg.Actions), batches: map[int32]map[string]*group{}}
	t.policy = Policy{Version: 0, Logits: map[string][]float64{}}
	for i := 0; i < cfg.Prompts; i++ {
		t.policy.Logits[promptName(i)] = make([]float64, cfg.Actions)
	}
	return t
}

func (t *trainer) group(version int32, prompt string) *group {
	b := t.batches[version]
	if b == nil {
		b = map[string]*group{}
		t.batches[version] = b
	}
	g := b[prompt]
	if g == nil {
		g = &group{samples: map[int]Sample{}, rewards: map[int]float64{}}
		b[prompt] = g
	}
	return g
}

// onRecord joins one sample or score into its group and steps when the
// version's batch is complete. The record that completes the batch carries
// the batch's version as its epoch, so the weights the step emits are
// stamped with the next version by the worker library.
func (t *trainer) onRecord(ctx context.Context, w *sdk.Worker, r sdk.Record) error {
	var version int32
	switch r.Channel {
	case "samples":
		var s Sample
		if err := json.Unmarshal(r.Value, &s); err != nil {
			return err
		}
		version = s.Version
		t.group(version, r.Key).samples[s.Index] = s
	case "rewards":
		var s Score
		if err := json.Unmarshal(r.Value, &s); err != nil {
			return err
		}
		version = s.Version
		t.group(version, r.Key).rewards[s.Index] = s.Reward
	default:
		return fmt.Errorf("unexpected channel %s", r.Channel)
	}
	batch := t.batches[version]
	if len(batch) < t.cfg.Prompts {
		return nil
	}
	for _, g := range batch {
		if !g.complete(t.cfg.Group) {
			return nil
		}
	}
	if version != t.policy.Version {
		return fmt.Errorf("batch for version %d completed while the policy is at version %d", version, t.policy.Version)
	}
	if r.Epoch != version {
		return fmt.Errorf("record of version %d arrived with epoch %d", version, r.Epoch)
	}
	return t.stepBatch(w, batch)
}

func (t *trainer) stepBatch(w *sdk.Worker, batch map[string]*group) error {
	var total float64
	next := Policy{Version: t.policy.Version + 1, Logits: map[string][]float64{}}
	for prompt, g := range batch {
		next.Logits[prompt] = step(t.cfg, t.policy.Logits[prompt], t.ref, g)
		for i := 0; i < t.cfg.Group; i++ {
			total += g.rewards[i]
		}
	}
	mean := total / float64(t.cfg.Prompts*t.cfg.Group)
	log.Printf("op=%s version=%d meanReward=%.3f", w.Operation, t.policy.Version, mean)
	if err := w.Emit("metrics", strconv.Itoa(int(t.policy.Version)), Metric{Version: t.policy.Version, MeanReward: mean}); err != nil {
		return err
	}
	delete(t.batches, t.policy.Version)
	t.policy = next
	return w.Emit("weights", "policy", next)
}

// --- the sampler ------------------------------------------------------------

// sampler holds its prompts and the latest policy it has received.
type sampler struct {
	cfg     Config
	prompts []string
	policy  Policy
}

func newSampler(cfg Config) *sampler {
	return &sampler{cfg: cfg, policy: Policy{Version: 0, Logits: map[string][]float64{}}}
}

func (s *sampler) onRecord(ctx context.Context, w *sdk.Worker, r sdk.Record) error {
	switch r.Channel {
	case "prompt-set":
		s.prompts = append(s.prompts, r.Key)
		sort.Strings(s.prompts)
		return s.emit(w, r.Key)
	case "weights":
		var p Policy
		if err := json.Unmarshal(r.Value, &p); err != nil {
			return err
		}
		if p.Version != r.Epoch {
			return fmt.Errorf("policy version %d arrived with epoch %d", p.Version, r.Epoch)
		}
		if p.Version <= s.policy.Version {
			return nil
		}
		s.policy = p
		log.Printf("op=%s pod=%s version=%d prompts=%d", w.Operation, w.Instance, p.Version, len(s.prompts))
		for _, prompt := range s.prompts {
			if err := s.emit(w, prompt); err != nil {
				return err
			}
		}
		return nil
	default:
		return fmt.Errorf("unexpected channel %s", r.Channel)
	}
}

// emit draws the group for one prompt under the current version and sends
// every sample to both consumers. A channel has one consuming operation, so
// a record two operations need is emitted once per channel.
func (s *sampler) emit(w *sdk.Worker, prompt string) error {
	logits := s.policy.Logits[prompt]
	if logits == nil {
		logits = make([]float64, s.cfg.Actions)
	}
	for _, sample := range sampleGroup(s.cfg, prompt, s.policy.Version, logits) {
		if err := w.Emit("rollouts", prompt, sample); err != nil {
			return err
		}
		if err := w.Emit("samples", prompt, sample); err != nil {
			return err
		}
	}
	return nil
}

// --- the reward -------------------------------------------------------------

func rewardRecord(cfg Config) func(context.Context, *sdk.Worker, sdk.Record) error {
	return func(ctx context.Context, w *sdk.Worker, r sdk.Record) error {
		var s Sample
		if err := json.Unmarshal(r.Value, &s); err != nil {
			return err
		}
		target, err := targetOf(r.Key, cfg.Actions)
		if err != nil {
			return err
		}
		reward := 0.0
		if s.Action == target {
			reward = 1
		}
		return w.Emit("rewards", r.Key, Score{Version: s.Version, Index: s.Index, Reward: reward})
	}
}

// --- the prompts source -----------------------------------------------------

func sourcePrompts(cfg Config) func(context.Context, *sdk.Worker) error {
	return func(ctx context.Context, w *sdk.Worker) error {
		for i := 0; i < cfg.Prompts; i++ {
			target, err := targetOf(promptName(i), cfg.Actions)
			if err != nil {
				return err
			}
			if err := w.Emit("prompt-set", promptName(i), map[string]int{"target": target}); err != nil {
				return err
			}
		}
		return nil
	}
}

// handlers builds the callbacks for one operation.
func handlers(cfg Config, op string) (sdk.Handlers, error) {
	var h sdk.Handlers
	switch op {
	case "prompts":
		h.Source = sourcePrompts(cfg)
	case "sampler":
		h.OnRecord = newSampler(cfg).onRecord
	case "reward":
		h.OnRecord = rewardRecord(cfg)
	case "trainer":
		h.OnRecord = newTrainer(cfg).onRecord
	default:
		return h, fmt.Errorf("unknown operation %q", op)
	}
	return h, nil
}

func main() {
	var cfg Config
	flag.IntVar(&cfg.Prompts, "prompts", 8, "number of prompts")
	flag.IntVar(&cfg.Actions, "actions", 6, "number of actions")
	flag.IntVar(&cfg.Group, "group", 8, "samples per prompt per version")
	flag.IntVar(&cfg.Iterations, "iterations", 3, "gradient steps per batch")
	flag.Float64Var(&cfg.Rate, "rate", 0.5, "step size")
	flag.Float64Var(&cfg.Clip, "clip", 0.2, "probability-ratio clip")
	flag.Float64Var(&cfg.KL, "kl", 0.01, "weight of the penalty toward the initial policy")
	flag.Parse()
	if flag.NArg() != 1 {
		log.Fatal("usage: grpo [flags] prompts|sampler|reward|trainer")
	}
	w, err := sdk.FromEnv()
	if err != nil {
		log.Fatal(err)
	}
	h, err := handlers(cfg, flag.Arg(0))
	if err != nil {
		log.Fatal(err)
	}
	if err := w.Run(context.Background(), h); err != nil {
		log.Fatal(err)
	}
}
