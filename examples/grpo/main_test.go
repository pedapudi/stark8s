package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/pedapudi/stark8s/api/v1alpha1"
	"github.com/pedapudi/stark8s/pkg/coordinator"
	"github.com/pedapudi/stark8s/pkg/sdk"
)

// The whole workload in one process against a real coordinator: real
// segments over real HTTP, the real Broadcast feedback channel, and the two
// application-level barriers doing the work Materialized delivery cannot.
const (
	testGroup = 8
	testSteps = 24 // = maxEpochs on the weights channel
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
		{Name: "checkpoints", From: "learner"},
	}
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

func (r *rig) worker(op, instance string, in, out []string) *sdk.Worker {
	return &sdk.Worker{
		Coordinator: r.srv.URL, Operation: op, Instance: instance,
		Inbound: in, Outbound: out,
		SegmentDir: r.t.TempDir(), SegmentListen: "127.0.0.1:0",
	}
}

func (r *rig) run(w *sdk.Worker, h sdk.Handlers) {
	r.wg.Add(1)
	go func() {
		defer r.wg.Done()
		if err := w.Run(r.ctx, h); err != nil && r.ctx.Err() == nil {
			r.t.Errorf("%s: %v", w.Instance, err)
		}
	}()
}

func TestGRPOLearnsTheTasks(t *testing.T) {
	r, stop := newRig(t)
	defer stop()

	// prompts
	r.run(r.worker("prompts", "prompts-0", nil, []string{"batch"}), sdk.Handlers{
		Source: func(ctx context.Context, w *sdk.Worker) error {
			for _, task := range sortedTasks() {
				if err := w.Emit("batch", task, tasks[task]); err != nil {
					return err
				}
			}
			return nil
		},
	})

	// rollout: two replicas, each owning whichever tasks hash to it
	for i := 0; i < 2; i++ {
		w := r.worker("rollout", fmt.Sprintf("rollout-%d", i), []string{"batch", "weights"}, []string{"completions"})
		w.SetFeedback([]string{"weights"}, nil)
		pol := newPolicy(positions, vocab, sortedTasks())
		owned := map[string]bool{}
		draw := func(w *sdk.Worker, task string) error {
			for i := 0; i < testGroup; i++ {
				tokens, lp := pol.sample(task, w.Epoch(), i)
				if err := w.Emit("completions", task, completion{
					Task: task, Index: i, Tokens: tokens, OldLogP: lp}); err != nil {
					return err
				}
			}
			return nil
		}
		r.run(w, sdk.Handlers{OnRecord: func(ctx context.Context, w *sdk.Worker, rec sdk.Record) error {
			switch rec.Channel {
			case "batch":
				owned[rec.Key] = true
				return draw(w, rec.Key)
			case "weights":
				if err := json.Unmarshal(rec.Value, &pol.Theta); err != nil {
					return err
				}
				for _, task := range sortedKeys(owned) {
					if err := draw(w, task); err != nil {
						return err
					}
				}
			}
			return nil
		}})
	}

	// reward: stateless, two replicas
	for i := 0; i < 2; i++ {
		r.run(r.worker("reward", fmt.Sprintf("reward-%d", i), []string{"completions"}, []string{"scored"}),
			sdk.Handlers{OnRecord: func(ctx context.Context, w *sdk.Worker, rec sdk.Record) error {
				var c completion
				if err := json.Unmarshal(rec.Value, &c); err != nil {
					return err
				}
				c.Reward = reward(c.Task, c.Tokens)
				return w.Emit("scored", c.Task, c)
			}})
	}

	// advantage: the group barrier, two replicas
	type gkey struct {
		step int32
		task string
	}
	for i := 0; i < 2; i++ {
		open := map[gkey][]completion{}
		r.run(r.worker("advantage", fmt.Sprintf("advantage-%d", i), []string{"scored"}, []string{"advantages"}),
			sdk.Handlers{OnRecord: func(ctx context.Context, w *sdk.Worker, rec sdk.Record) error {
				var c completion
				if err := json.Unmarshal(rec.Value, &c); err != nil {
					return err
				}
				k := gkey{rec.Epoch, c.Task}
				open[k] = append(open[k], c)
				if len(open[k]) < testGroup {
					return nil
				}
				cs := open[k]
				delete(open, k)
				rewards := make([]float64, len(cs))
				for i, c := range cs {
					rewards[i] = c.Reward
				}
				adv := advantages(rewards)
				g := group{Task: c.Task, Samples: make([]sampleRec, len(cs))}
				for i, c := range cs {
					g.Samples[i] = sampleRec{Prompt: c.Task, Tokens: c.Tokens, OldLogP: c.OldLogP, Adv: adv[i]}
				}
				return w.Emit("advantages", c.Task, g)
			}})
	}

	// learner: the step barrier and the single owner of theta
	lw := r.worker("learner", "learner-0", []string{"advantages"},
		[]string{"weights", "metrics", "checkpoints"})
	lw.SetFeedback(nil, []string{"weights"})
	pol := newPolicy(positions, vocab, sortedTasks())
	ref := pol.clone()
	open := map[int32][]group{}
	var mu sync.Mutex
	var updates int
	r.run(lw, sdk.Handlers{OnRecord: func(ctx context.Context, w *sdk.Worker, rec sdk.Record) error {
		var g group
		if err := json.Unmarshal(rec.Value, &g); err != nil {
			return err
		}
		open[rec.Epoch] = append(open[rec.Epoch], g)
		if len(open[rec.Epoch]) < len(tasks) {
			return nil
		}
		groups := open[rec.Epoch]
		delete(open, rec.Epoch)
		var batch []sampleRec
		mean, n := 0.0, 0.0
		for _, g := range groups {
			for _, s := range g.Samples {
				batch = append(batch, s)
				mean += reward(g.Task, s.Tokens)
				n++
			}
		}
		obj, kl := pol.step(batch, ref, 0.5, 0.2, 0.001)
		mu.Lock()
		updates++
		mu.Unlock()
		if err := w.Emit("metrics", fmt.Sprintf("step-%03d", rec.Epoch),
			metric{Step: rec.Epoch, RewardMean: mean / n, Objective: obj, KL: kl}); err != nil {
			return err
		}
		return w.Emit("weights", "theta", pol.Theta)
	}})

	// The loop ends itself: the update at the last step emits a weights record
	// stamped maxEpochs, which the engine drops.
	deadline := time.Now().Add(90 * time.Second)
	for time.Now().Before(deadline) {
		mu.Lock()
		done := updates >= testSteps
		mu.Unlock()
		if done {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	mu.Lock()
	got := updates
	mu.Unlock()
	if got != testSteps {
		t.Fatalf("updates = %d, want %d", got, testSteps)
	}

	// Every step reported a metric, and the reward rose.
	recs, _, err := r.co.Records("metrics", "", 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(recs) != testSteps {
		t.Fatalf("metrics records = %d, want %d", len(recs), testSteps)
	}
	first, last := metricAt(t, recs, 0), metricAt(t, recs, len(recs)-1)
	if !(last.RewardMean > first.RewardMean) {
		t.Errorf("reward did not rise: step %d %.3f -> step %d %.3f",
			first.Step, first.RewardMean, last.Step, last.RewardMean)
	}
	if last.RewardMean < 0.85 {
		t.Errorf("final reward %.3f, want >= 0.85", last.RewardMean)
	}
	t.Logf("reward %.3f -> %.3f over %d steps, final KL %.3f",
		first.RewardMean, last.RewardMean, testSteps, last.KL)

	// The closed form: for every task, at every position, the learned policy
	// puts its mass on the token that task asked for. This is what the run is
	// checked against, in place of eyeballing a reward curve.
	for _, task := range sortedTasks() {
		want := tasks[task]
		for pos, wantTok := range want {
			pr := pol.probs(task, pos)
			best := 0
			for v := range pr {
				if pr[v] > pr[best] {
					best = v
				}
			}
			if best != wantTok {
				t.Errorf("%s position %d: argmax %d, want %d (%v)", task, pos, best, wantTok, pr)
			}
			if pr[wantTok] < 0.5 {
				t.Errorf("%s position %d: p(%d) = %.3f, want >= 0.5", task, pos, wantTok, pr[wantTok])
			}
		}
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
