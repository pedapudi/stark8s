package main

import (
	"context"
	"encoding/json"
	"fmt"
	"math/rand"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/pedapudi/stark8s/api/v1alpha1"
	"github.com/pedapudi/stark8s/pkg/coordinator"
	"github.com/pedapudi/stark8s/pkg/sdk"
)

// The graph is tested on CPU against a stub model, the way a workload with a
// networked dependency is tested against a fake. What is real here is
// everything the example is about: the coordinator, the segments over HTTP,
// the Hash partition that forms a group, the Broadcast feedback edge carrying
// the checkpoint reference, the two counted barriers, and the epoch
// arithmetic that ends the run. What is stubbed is the only part that needs
// an accelerator.
const (
	testGroup = 8
	testSteps = 6
)

func specs() []v1alpha1.Channel {
	return []v1alpha1.Channel{
		{Name: "batch", From: "prompts", To: "rollout",
			Partitioning: v1alpha1.Partitioning{Mode: v1alpha1.PartitionHash, Partitions: 8}},
		{Name: "completions", From: "rollout", To: "reward",
			Partitioning: v1alpha1.Partitioning{Mode: v1alpha1.PartitionRoundRobin, Partitions: 8}},
		{Name: "scored", From: "reward", To: "advantage",
			Partitioning: v1alpha1.Partitioning{Mode: v1alpha1.PartitionHash, Partitions: 8}},
		{Name: "advantages", From: "advantage", To: "learner",
			Partitioning: v1alpha1.Partitioning{Mode: v1alpha1.PartitionHash, Partitions: 1}},
		{Name: "weights", From: "learner", To: "rollout",
			Partitioning: v1alpha1.Partitioning{Mode: v1alpha1.PartitionBroadcast, Partitions: 1},
			Feedback:     &v1alpha1.Feedback{Mode: v1alpha1.FeedbackAsynchronous, MaxEpochs: testSteps}},
		{Name: "metrics", From: "learner"},
	}
}

// stubModel stands in for the generator and the trainer. It improves with each
// checkpoint it is told to load, so the reward is expected to rise — which is
// what lets the test assert that a checkpoint reference actually travelled the
// feedback edge and was acted on, rather than merely being emitted.
type stubModel struct {
	mu      sync.Mutex
	skill   float64 // 0 = ignores every constraint, 1 = satisfies all
	loaded  []string
	trained int
}

func (m *stubModel) Generate(ctx context.Context, prompt string, n int, seed int64) ([]string, error) {
	m.mu.Lock()
	skill := m.skill
	m.mu.Unlock()
	// Recover which task this prompt belongs to, so the stub can produce
	// something the real checker will actually score.
	var tk task
	for _, id := range sortedTaskIDs(tasks) {
		if tasks[id].prompt() == prompt {
			tk = tasks[id]
		}
	}
	rng := rand.New(rand.NewSource(seed))
	out := make([]string, n)
	for i := range out {
		// Each completion satisfies each constraint with probability `skill`,
		// so a group has spread at every skill level except 0 and 1.
		good := map[string]bool{}
		for _, c := range tk.Constraints {
			good[c.String()] = rng.Float64() < skill
		}
		out[i] = compose(tk, good)
	}
	return out, nil
}

func (m *stubModel) Load(ctx context.Context, uri string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.loaded = append(m.loaded, uri)
	return nil
}

func (m *stubModel) Step(ctx context.Context, batch []trainSample, step int32) (stepResult, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.trained++
	// A real trainer moves the weights; this one moves the number that stands
	// in for them, so a loaded checkpoint has an observable effect.
	m.skill += 0.14
	if m.skill > 1 {
		m.skill = 1
	}
	return stepResult{
		Checkpoint: fmt.Sprintf("memory://ckpt/step-%03d", step),
		Objective:  0,
		KL:         float64(step) * 0.01,
	}, nil
}

// compose writes a completion that satisfies exactly the constraints marked
// good, so the stub exercises the real checker rather than a shortcut.
func compose(tk task, good map[string]bool) string {
	body := "Answer: the order ships on Tuesday and arrives within three working days after that"
	bullets, lines := 0, 0
	suffix, prefix := "", ""
	jsonKeys := ""
	forceLong, forceShort, avoid, upper := false, false, "", false
	for _, c := range tk.Constraints {
		ok := good[c.String()]
		switch c.Kind {
		case "bullets":
			if ok {
				bullets = atoiOr(c.Arg, 0)
			} else {
				bullets = atoiOr(c.Arg, 0) + 1
			}
		case "lines":
			if ok {
				lines = atoiOr(c.Arg, 0)
			} else {
				lines = atoiOr(c.Arg, 0) + 1
			}
		case "maxwords":
			forceLong = !ok
		case "minwords":
			forceShort = !ok
		case "endswith":
			if ok {
				suffix = c.Arg
			}
		case "startswith":
			if ok {
				prefix = c.Arg
			}
		case "avoid":
			if !ok {
				avoid = c.Arg
			}
		case "json":
			if ok {
				jsonKeys = c.Arg
			}
		case "uppercase":
			upper = ok
		}
	}
	if jsonKeys != "" {
		parts := []string{}
		for _, k := range splitCSV(jsonKeys) {
			parts = append(parts, fmt.Sprintf("%q:%q", strings.TrimSpace(k), "x"))
		}
		return "{" + strings.Join(parts, ",") + "}"
	}
	var b strings.Builder
	if prefix != "" {
		b.WriteString(prefix + " ")
	}
	switch {
	case bullets > 0:
		for i := 0; i < bullets; i++ {
			if i > 0 {
				b.WriteString("\n")
			}
			b.WriteString("- point")
		}
	case lines > 0:
		for i := 0; i < lines; i++ {
			if i > 0 {
				b.WriteString("\n")
			}
			b.WriteString("Cycling Lanes Open Downtown")
		}
	case upper:
		b.WriteString("Cycling Lanes Open Downtown Today")
	default:
		b.WriteString(body)
	}
	if forceShort {
		return strings.Fields(b.String())[0]
	}
	if forceLong {
		b.WriteString(strings.Repeat(" filler", 80))
	}
	if avoid != "" {
		b.WriteString(" " + avoid)
	}
	if suffix != "" {
		b.WriteString(" " + suffix)
	}
	return b.String()
}

type rig struct {
	t   *testing.T
	co  *coordinator.Coordinator
	srv *httptest.Server
	ctx context.Context
	wg  sync.WaitGroup
}

func newRig(t *testing.T) (*rig, func()) {
	t.Helper()
	seg := httptest.NewServer(nil)
	co := coordinator.New(strings.TrimPrefix(seg.URL, "http://"))
	seg.Config.Handler = coordinator.SegmentHandler(co)
	co.Configure(specs())
	srv := httptest.NewServer(coordinator.Handler(co))
	ctx, cancel := context.WithCancel(context.Background())
	r := &rig{t: t, co: co, srv: srv, ctx: ctx}
	return r, func() { cancel(); r.wg.Wait(); srv.Close(); seg.Close() }
}

func (r *rig) run(op, instance string, in, out []string, fbIn, fbOut []string, h sdk.Handlers) {
	w := &sdk.Worker{
		Coordinator: r.srv.URL, Operation: op, Instance: instance,
		Inbound: in, Outbound: out,
		SegmentDir: r.t.TempDir(), SegmentListen: "127.0.0.1:0",
	}
	if len(fbIn) > 0 || len(fbOut) > 0 {
		w.SetFeedback(fbIn, fbOut)
	}
	r.wg.Add(1)
	go func() {
		defer r.wg.Done()
		if err := w.Run(r.ctx, h); err != nil && r.ctx.Err() == nil {
			r.t.Errorf("%s: %v", instance, err)
		}
	}()
}

func TestGraphTrainsAgainstAStubModel(t *testing.T) {
	r, stop := newRig(t)
	defer stop()
	model := &stubModel{skill: 0.15}

	r.run("prompts", "prompts-0", nil, []string{"batch"}, nil, nil,
		handlers("prompts", testGroup, model, model))
	for i := 0; i < 2; i++ {
		r.run("rollout", fmt.Sprintf("rollout-%d", i),
			[]string{"batch", "weights"}, []string{"completions"},
			[]string{"weights"}, nil,
			handlers("rollout", testGroup, model, model))
		r.run("reward", fmt.Sprintf("reward-%d", i),
			[]string{"completions"}, []string{"scored"}, nil, nil,
			handlers("reward", testGroup, model, model))
		r.run("advantage", fmt.Sprintf("advantage-%d", i),
			[]string{"scored"}, []string{"advantages"}, nil, nil,
			handlers("advantage", testGroup, model, model))
	}
	r.run("learner", "learner-0", []string{"advantages"},
		[]string{"weights", "metrics"}, nil, []string{"weights"},
		handlers("learner", testGroup, model, model))

	deadline := time.Now().Add(120 * time.Second)
	for time.Now().Before(deadline) {
		model.mu.Lock()
		done := model.trained >= testSteps
		model.mu.Unlock()
		if done {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}

	model.mu.Lock()
	trained, loaded := model.trained, append([]string(nil), model.loaded...)
	model.mu.Unlock()

	// The engine ends the loop: an update at epoch e emits a checkpoint
	// stamped e+1, and maxEpochs drops the last one.
	if trained != testSteps {
		t.Fatalf("trainer steps = %d, want %d", trained, testSteps)
	}
	// Every rollout replica loaded every checkpoint but the dropped one, which
	// is what proves the Broadcast feedback edge carried the reference and
	// that the rollout acted on it.
	if want := (testSteps - 1) * 2; len(loaded) != want {
		t.Errorf("checkpoint loads = %d, want %d (%v)", len(loaded), want, loaded)
	}

	recs, _, err := r.co.Records("metrics", "", 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(recs) != testSteps {
		t.Fatalf("metrics = %d, want %d", len(recs), testSteps)
	}
	first, last := metricAt(t, recs, 0), metricAt(t, recs, len(recs)-1)
	if !(last.RewardMean > first.RewardMean) {
		t.Errorf("reward did not rise: %.3f -> %.3f", first.RewardMean, last.RewardMean)
	}
	if last.Checkpoint == "" {
		t.Error("last metric carries no checkpoint reference")
	}
	if len(last.PerTask) != len(tasks) {
		t.Errorf("perTask has %d entries, want %d", len(last.PerTask), len(tasks))
	}
	t.Logf("reward %.3f -> %.3f over %d steps; %d checkpoint loads; final degenerate groups %d/%d",
		first.RewardMean, last.RewardMean, testSteps, len(loaded), last.Degenerate, len(tasks))
}

// A group whose completions all score the same must contribute no gradient,
// and must be counted, because a run where that is always true learns nothing
// while looking healthy.
func TestFlatGroupIsCountedAndContributesNothing(t *testing.T) {
	for _, rs := range [][]float64{{1, 1, 1, 1}, {0, 0, 0, 0}} {
		for i, a := range advantages(rs) {
			if a != 0 {
				t.Errorf("rewards %v: advantage[%d] = %v, want 0", rs, i, a)
			}
		}
	}
	mixed := advantages([]float64{0, 0.5, 1})
	if mixed[0] >= 0 || mixed[2] <= 0 {
		t.Errorf("mixed group did not produce signed advantages: %v", mixed)
	}
}

func metricAt(t *testing.T, recs []coordinator.Record, i int) metric {
	t.Helper()
	b, err := json.Marshal(recs[i].Value)
	if err != nil {
		t.Fatal(err)
	}
	var m metric
	if err := json.Unmarshal(b, &m); err != nil {
		t.Fatal(err)
	}
	return m
}
