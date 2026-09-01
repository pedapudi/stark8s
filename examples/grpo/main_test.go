package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http/httptest"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/pedapudi/stark8s/api/v1alpha1"
	"github.com/pedapudi/stark8s/pkg/coordinator"
	"github.com/pedapudi/stark8s/pkg/sdk"
)

// The tests share one configuration. The first runs the learning loop in a
// single goroutine with no coordinator, to show the optimiser converges on
// its own. The second runs the whole workload in one process: a real
// coordinator on an httptest server, two sampler pods, two reward pods and
// one trainer pod exchanging real segments over real HTTP. The only thing
// standing in for the controller is sealing a channel when its producing
// operation completes, which is what the reconciler does for non-feedback
// channels.

var testConfig = Config{Prompts: 8, Actions: 6, Group: 8, Iterations: 3, Rate: 0.5, Clip: 0.2, KL: 0.01}

const (
	testSteps = 30
	// A trained row must put at least this much probability on the target.
	wantAccuracy = 0.9
)

// score is what the reward operation computes, inlined for the in-process
// loop.
func score(t *testing.T, cfg Config, s Sample) float64 {
	t.Helper()
	target, err := targetOf(s.Prompt, cfg.Actions)
	if err != nil {
		t.Fatal(err)
	}
	if s.Action == target {
		return 1
	}
	return 0
}

// accuracy returns the probability the policy assigns to each prompt's
// target.
func accuracy(t *testing.T, cfg Config, p Policy) map[string]float64 {
	t.Helper()
	out := map[string]float64{}
	for i := 0; i < cfg.Prompts; i++ {
		prompt := promptName(i)
		target, err := targetOf(prompt, cfg.Actions)
		if err != nil {
			t.Fatal(err)
		}
		row := p.Logits[prompt]
		if len(row) != cfg.Actions {
			t.Fatalf("policy row for %s has %d logits, want %d", prompt, len(row), cfg.Actions)
		}
		out[prompt] = softmax(row)[target]
	}
	return out
}

func TestOptimiserConvergesInProcess(t *testing.T) {
	cfg := testConfig
	tr := newTrainer(cfg)
	first, last := -1.0, -1.0
	for v := int32(0); v < testSteps; v++ {
		batch := map[string]*group{}
		var total float64
		for i := 0; i < cfg.Prompts; i++ {
			prompt := promptName(i)
			g := &group{samples: map[int]Sample{}, rewards: map[int]float64{}}
			for _, s := range sampleGroup(cfg, prompt, v, tr.policy.Logits[prompt]) {
				g.samples[s.Index] = s
				g.rewards[s.Index] = score(t, cfg, s)
				total += g.rewards[s.Index]
			}
			batch[prompt] = g
		}
		mean := total / float64(cfg.Prompts*cfg.Group)
		if first < 0 {
			first = mean
		}
		last = mean
		next := Policy{Version: v + 1, Logits: map[string][]float64{}}
		for prompt, g := range batch {
			next.Logits[prompt] = step(cfg, tr.policy.Logits[prompt], tr.ref, g)
		}
		tr.policy = next
	}
	for prompt, acc := range accuracy(t, cfg, tr.policy) {
		if acc < wantAccuracy {
			t.Errorf("%s: probability on the target is %.3f after %d steps, want at least %.2f", prompt, acc, testSteps, wantAccuracy)
		}
	}
	if last <= first {
		t.Errorf("mean reward went from %.3f to %.3f", first, last)
	}
	t.Logf("mean reward %.3f -> %.3f", first, last)
}

func TestSamplingIsDeterministic(t *testing.T) {
	cfg := testConfig
	logits := []float64{0.3, -0.2, 0, 0.9, 0.1, -1}
	a := sampleGroup(cfg, "p3", 7, logits)
	b := sampleGroup(cfg, "p3", 7, logits)
	if fmt.Sprint(a) != fmt.Sprint(b) {
		t.Fatalf("the same (prompt, version) drew different groups:\n%v\n%v", a, b)
	}
	c := sampleGroup(cfg, "p3", 8, logits)
	if fmt.Sprint(a) == fmt.Sprint(c) {
		t.Fatalf("versions 7 and 8 drew the same group: %v", a)
	}
}

func TestEqualRewardsGiveNoSignal(t *testing.T) {
	cfg := testConfig
	g := &group{samples: map[int]Sample{}, rewards: map[int]float64{}}
	for i := 0; i < cfg.Group; i++ {
		g.samples[i] = Sample{Prompt: "p0", Index: i, Action: i % cfg.Actions, LogProb: -1.79}
		g.rewards[i] = 1
	}
	for _, a := range g.advantages(cfg.Group) {
		if a != 0 {
			t.Fatalf("advantages of an all-equal group: %v", g.advantages(cfg.Group))
		}
	}
}

// --- harness ----------------------------------------------------------------

type harness struct {
	t    *testing.T
	co   *coordinator.Coordinator
	ctrl *httptest.Server
	seg  *httptest.Server
	ctx  context.Context
	wg   sync.WaitGroup
}

func newHarness(t *testing.T, specs []v1alpha1.Channel) (*harness, context.CancelFunc) {
	t.Helper()
	seg := httptest.NewServer(nil)
	co := coordinator.New(strings.TrimPrefix(seg.URL, "http://"))
	seg.Config.Handler = coordinator.SegmentHandler(co)
	co.Configure(specs)
	ctrl := httptest.NewServer(coordinator.Handler(co))
	ctx, cancel := context.WithCancel(context.Background())
	h := &harness{t: t, co: co, ctrl: ctrl, seg: seg, ctx: ctx}
	return h, func() {
		cancel()
		h.wg.Wait()
		ctrl.Close()
		seg.Close()
	}
}

// run starts one pod of an operation. inbound and outbound are its channels
// and feedbackIn and feedbackOut are the subset of those that are feedback
// edges, as the controller injects them into a pod's environment.
func (h *harness) run(op, instance string, inbound, outbound, feedbackIn, feedbackOut []string, hs sdk.Handlers) {
	w := &sdk.Worker{
		Coordinator:   h.ctrl.URL,
		Operation:     op,
		Instance:      instance,
		Inbound:       inbound,
		Outbound:      outbound,
		SegmentDir:    h.t.TempDir(),
		SegmentListen: "127.0.0.1:0",
	}
	w.SetFeedback(feedbackIn, feedbackOut)
	h.wg.Add(1)
	go func() {
		defer h.wg.Done()
		if err := w.Run(h.ctx, hs); err != nil && h.ctx.Err() == nil {
			h.t.Errorf("%s: %v", instance, err)
		}
	}()
}

func (h *harness) waitComplete(op string) {
	h.t.Helper()
	deadline := time.Now().Add(90 * time.Second)
	for time.Now().Before(deadline) {
		for _, om := range h.co.Metrics().Operations {
			if om.Name == op && om.Complete {
				return
			}
		}
		time.Sleep(50 * time.Millisecond)
	}
	h.t.Fatalf("operation %s did not complete: %+v", op, h.co.Metrics())
}

func (h *harness) seal(name string) {
	h.t.Helper()
	if err := h.co.Seal(name); err != nil {
		h.t.Fatal(err)
	}
}

func (h *harness) channel(name string) coordinator.ChannelMetrics {
	h.t.Helper()
	for _, cm := range h.co.Metrics().Channels {
		if cm.Name == name {
			return cm
		}
	}
	h.t.Fatalf("no channel %q", name)
	return coordinator.ChannelMetrics{}
}

// --- the workload -----------------------------------------------------------

func TestWorkloadTrainsThePolicy(t *testing.T) {
	cfg := testConfig
	hash := func(n int32) v1alpha1.Partitioning {
		return v1alpha1.Partitioning{Mode: v1alpha1.PartitionHash, Partitions: n}
	}
	h, stop := newHarness(t, []v1alpha1.Channel{
		{Name: "prompt-set", From: "prompts", To: "sampler", Partitioning: hash(8), Delivery: v1alpha1.DeliveryMaterialized},
		{Name: "rollouts", From: "sampler", To: "reward", Partitioning: hash(8)},
		{Name: "samples", From: "sampler", To: "trainer", Partitioning: hash(8)},
		{Name: "rewards", From: "reward", To: "trainer", Partitioning: hash(8)},
		{Name: "weights", From: "trainer", To: "sampler",
			Partitioning: v1alpha1.Partitioning{Mode: v1alpha1.PartitionBroadcast, Partitions: 1},
			Feedback:     &v1alpha1.Feedback{Mode: v1alpha1.FeedbackAsynchronous, MaxEpochs: testSteps, Overflow: "final-policy"}},
		{Name: "final-policy", From: "trainer", Durability: v1alpha1.DurabilityRetained},
		{Name: "metrics", From: "trainer", Durability: v1alpha1.DurabilityRetained},
	})
	defer stop()

	var mu sync.Mutex
	// versionsSeen[pod] is the policy versions that sampler pod received.
	versionsSeen := map[string][]int32{}
	// rewardsByPod[pod] counts the samples each reward pod scored.
	rewardsByPod := map[string]int{}

	h.run("prompts", "prompts-0", nil, []string{"prompt-set"}, nil, nil,
		sdk.Handlers{Source: sourcePrompts(cfg)})

	for i := 0; i < 2; i++ {
		pod := fmt.Sprintf("sampler-%d", i)
		s := newSampler(cfg)
		h.run("sampler", pod, []string{"prompt-set", "weights"}, []string{"rollouts", "samples"}, []string{"weights"}, nil,
			sdk.Handlers{OnRecord: func(ctx context.Context, w *sdk.Worker, r sdk.Record) error {
				if r.Channel == "weights" {
					mu.Lock()
					versionsSeen[pod] = append(versionsSeen[pod], r.Epoch)
					mu.Unlock()
				}
				return s.onRecord(ctx, w, r)
			}})
	}
	for i := 0; i < 2; i++ {
		pod := fmt.Sprintf("reward-%d", i)
		score := rewardRecord(cfg)
		h.run("reward", pod, []string{"rollouts"}, []string{"rewards"}, nil, nil,
			sdk.Handlers{OnRecord: func(ctx context.Context, w *sdk.Worker, r sdk.Record) error {
				mu.Lock()
				rewardsByPod[pod]++
				mu.Unlock()
				return score(ctx, w, r)
			}})
	}
	h.run("trainer", "trainer-0", []string{"samples", "rewards"}, []string{"weights", "final-policy", "metrics"}, nil, []string{"weights"},
		sdk.Handlers{OnRecord: newTrainer(cfg).onRecord})

	// The controller's role: seal an operation's non-feedback outbound
	// channels once it is complete. "weights" is a feedback channel and is
	// sealed by the coordinator when the loop can produce nothing more.
	h.waitComplete("prompts")
	h.seal("prompt-set")
	h.waitComplete("sampler")
	h.seal("rollouts")
	h.seal("samples")
	h.waitComplete("reward")
	h.seal("rewards")
	h.waitComplete("trainer")

	// --- the policy the loop bound diverted is trained ---

	recs, _, err := h.co.Records("final-policy", "", 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(recs) != 1 {
		t.Fatalf("final-policy holds %d records, want 1", len(recs))
	}
	raw, _ := json.Marshal(recs[0].Value)
	var final Policy
	if err := json.Unmarshal(raw, &final); err != nil {
		t.Fatal(err)
	}
	if final.Version != testSteps || recs[0].Epoch != testSteps {
		t.Errorf("final policy is version %d with epoch %d, want %d: the loop did not end at the bound", final.Version, recs[0].Epoch, testSteps)
	}
	for prompt, acc := range accuracy(t, cfg, final) {
		if acc < wantAccuracy {
			t.Errorf("%s: probability on the target is %.3f, want at least %.2f", prompt, acc, wantAccuracy)
		}
	}

	// --- the reward rose, and every step reported it ---

	recs, _, err = h.co.Records("metrics", "", 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(recs) != testSteps {
		t.Fatalf("metrics holds %d records, want one per step (%d)", len(recs), testSteps)
	}
	byVersion := map[int32]float64{}
	for _, r := range recs {
		raw, _ := json.Marshal(r.Value)
		var m Metric
		if err := json.Unmarshal(raw, &m); err != nil {
			t.Fatal(err)
		}
		byVersion[m.Version] = m.MeanReward
	}
	if byVersion[testSteps-1] <= byVersion[0] {
		t.Errorf("mean reward went from %.3f at version 0 to %.3f at version %d", byVersion[0], byVersion[testSteps-1], testSteps-1)
	}
	t.Logf("mean reward %.3f -> %.3f over %d steps", byVersion[0], byVersion[testSteps-1], testSteps)

	// --- Broadcast reached every sampler for every version below the bound ---

	mu.Lock()
	defer mu.Unlock()
	if len(versionsSeen) != 2 {
		t.Fatalf("%d sampler pods received weights, want 2: %v", len(versionsSeen), versionsSeen)
	}
	var want []string
	for v := int32(1); v < testSteps; v++ {
		want = append(want, strconv.Itoa(int(v)))
	}
	for pod, versions := range versionsSeen {
		sort.Slice(versions, func(i, j int) bool { return versions[i] < versions[j] })
		var got []string
		for _, v := range versions {
			got = append(got, strconv.Itoa(int(v)))
		}
		if strings.Join(got, ",") != strings.Join(want, ",") {
			t.Errorf("%s received versions %v, want 1..%d", pod, versions, testSteps-1)
		}
	}

	// --- the reward pool shared the scoring ---

	total := 0
	for _, n := range rewardsByPod {
		total += n
	}
	if want := cfg.Prompts * cfg.Group * testSteps; total < want {
		t.Errorf("reward pods scored %d samples, want at least %d", total, want)
	}
	for pod, n := range rewardsByPod {
		if n == 0 {
			t.Errorf("%s scored nothing", pod)
		}
	}

	// --- the loop ended at the bound ---

	if cm := h.channel("weights"); !cm.Sealed || cm.Produced != testSteps-1 {
		t.Errorf("weights: sealed=%v produced=%d, want sealed with %d versions", cm.Sealed, cm.Produced, testSteps-1)
	}
}
