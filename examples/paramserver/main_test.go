package main

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"net/http/httptest"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/pedapudi/stark8s/api/v1alpha1"
	"github.com/pedapudi/stark8s/pkg/coordinator"
	"github.com/pedapudi/stark8s/pkg/sdk"
)

// The test runs the whole workload in one process: a real coordinator on an
// httptest server, one server pod and three worker pods exchanging real
// segments over real HTTP. The only thing standing in for the controller is
// sealing a channel when its producing operation completes, which is what
// the reconciler does for non-feedback channels.

const (
	testShards = 4
	testRounds = 40
	testRate   = 1.4
	// Gradient descent on this problem at this rate contracts the error by
	// a factor of 0.51 a round, so forty rounds leave about 6e-12 of the
	// initial 3.05. The tolerance is well over a hundred times that.
	tolerance = 1e-9
)

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
// edges, exactly as the controller injects them into a pod's environment.
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

// --- the test ---------------------------------------------------------------

func TestParameterServerTrains(t *testing.T) {
	h, stop := newHarness(t, []v1alpha1.Channel{
		{Name: "shards", From: "data", To: "worker",
			Partitioning: v1alpha1.Partitioning{Mode: v1alpha1.PartitionHash, Partitions: testShards},
			Delivery:     v1alpha1.DeliveryMaterialized},
		{Name: "weights", From: "server", To: "worker",
			Partitioning: v1alpha1.Partitioning{Mode: v1alpha1.PartitionBroadcast, Partitions: 1},
			Feedback:     &v1alpha1.Feedback{Mode: v1alpha1.FeedbackAsynchronous, MaxEpochs: testRounds + 1}},
		{Name: "gradients", From: "worker", To: "server",
			Partitioning: v1alpha1.Partitioning{Mode: v1alpha1.PartitionHash, Partitions: 1}},
		{Name: "checkpoints", From: "server", Durability: v1alpha1.DurabilityRetained},
	})
	defer stop()

	var mu sync.Mutex
	// gradientsPerRound[r] is the set of shard keys the server saw for round r.
	gradientsPerRound := map[int32]map[string]bool{}
	// weightsSeen[pod] is the rounds that pod received on the Broadcast channel.
	weightsSeen := map[string][]int32{}

	h.run("data", "data-0", nil, []string{"shards"}, nil, nil,
		sdk.Handlers{Source: sourceShards(testShards)})

	srv := newServer(testShards, testRate)
	h.run("server", "server-0", []string{"gradients"}, []string{"weights", "checkpoints"}, nil, []string{"weights"},
		sdk.Handlers{
			OnRecord: func(ctx context.Context, w *sdk.Worker, r sdk.Record) error {
				mu.Lock()
				if gradientsPerRound[r.Epoch] == nil {
					gradientsPerRound[r.Epoch] = map[string]bool{}
				}
				gradientsPerRound[r.Epoch][r.Key] = true
				mu.Unlock()
				return srv.onGradient(ctx, w, r)
			},
			OnDrain: srv.onDrain,
		})

	for i := 0; i < 3; i++ {
		pod := fmt.Sprintf("worker-%d", i)
		wk := newWorker()
		h.run("worker", pod, []string{"shards", "weights"}, []string{"gradients"}, []string{"weights"}, nil,
			sdk.Handlers{
				OnRecord: func(ctx context.Context, w *sdk.Worker, r sdk.Record) error {
					if r.Channel == "weights" {
						mu.Lock()
						weightsSeen[pod] = append(weightsSeen[pod], r.Epoch)
						mu.Unlock()
					}
					return wk.onRecord(ctx, w, r)
				},
			})
	}

	// The controller's role: seal an operation's non-feedback outbound
	// channels once it is complete. "weights" is a feedback channel and is
	// sealed by the coordinator when the loop can produce nothing more.
	h.waitComplete("data")
	if err := h.co.Seal("shards"); err != nil {
		t.Fatal(err)
	}
	h.waitComplete("worker")
	if err := h.co.Seal("gradients"); err != nil {
		t.Fatal(err)
	}
	h.waitComplete("server")

	// --- the weights converge to the closed-form least-squares solution ---

	want := closedForm()
	if math.Abs(want[0]-1) > 1e-12 {
		t.Errorf("closed-form intercept is %.15f, want exactly 1: the dataset is not what the comment claims", want[0])
	}
	final := checkpoint(t, h, "final")
	if len(final.W) != params {
		t.Fatalf("final checkpoint has %d parameters: %+v", len(final.W), final)
	}
	for j := range want {
		if math.Abs(final.W[j]-want[j]) > tolerance {
			t.Errorf("w[%d] = %.12f, closed form %.12f, off by %.3g (tolerance %g)",
				j, final.W[j], want[j], math.Abs(final.W[j]-want[j]), tolerance)
		}
	}
	t.Logf("learned %v, closed form %v", final.W, want)

	// --- Broadcast reached every worker, every round ---

	mu.Lock()
	defer mu.Unlock()
	if len(weightsSeen) != 3 {
		t.Fatalf("%d worker pods received weights, want 3: %v", len(weightsSeen), weightsSeen)
	}
	var allRounds []int32
	for r := int32(1); r <= testRounds; r++ {
		allRounds = append(allRounds, r)
	}
	for pod, rounds := range weightsSeen {
		sort.Slice(rounds, func(i, j int) bool { return rounds[i] < rounds[j] })
		if fmt.Sprint(rounds) != fmt.Sprint(allRounds) {
			t.Errorf("%s received rounds %v, want %v", pod, rounds, allRounds)
		}
	}

	// --- one hash partition concentrated every gradient on the one server ---

	// The shard keys do not agree on a partition when there is more than
	// one, so their arrival together is the partition count doing the work
	// and not an accident of the keys.
	spread := map[int]bool{}
	for s := 0; s < testShards; s++ {
		spread[coordinator.HashPartition(shardID(s), testShards)] = true
	}
	if len(spread) < 2 {
		t.Errorf("the %d shard keys all hash to one partition of %d; the concentration assertion proves nothing", testShards, testShards)
	}
	// Round 0 is registration and rounds 1..testRounds are training, so the
	// server should see every shard in every one of them.
	if len(gradientsPerRound) != testRounds+1 {
		t.Errorf("server saw %d rounds of gradients, want %d", len(gradientsPerRound), testRounds+1)
	}
	for r := int32(0); r <= testRounds; r++ {
		if got := len(gradientsPerRound[r]); got != testShards {
			t.Errorf("round %d: server received %d gradients, want %d (%v)", r, got, testShards, gradientsPerRound[r])
		}
	}
	if cm := h.channel("gradients"); cm.Produced != int64(testShards*(testRounds+1)) {
		t.Errorf("gradients produced %d records, want %d", cm.Produced, testShards*(testRounds+1))
	}

	// --- the loop terminated at the bound ---

	cm := h.channel("weights")
	if !cm.Sealed {
		t.Errorf("weights channel not sealed: %+v", cm)
	}
	if cm.Produced != testRounds {
		t.Errorf("weights produced %d records, want %d (one per round)", cm.Produced, testRounds)
	}
	// The vector the server computes after the last round is stamped with
	// maxEpochs, so the engine drops it and counts it. That is the loop
	// bound taking effect.
	if cm.Overflowed != 1 {
		t.Errorf("weights overflowed %d records at the loop bound, want 1", cm.Overflowed)
	}
	if got := checkpoint(t, h, fmt.Sprintf("round-%d", testRounds)); got.Round != testRounds {
		t.Errorf("last round checkpoint is round %d, want %d", got.Round, testRounds)
	}
}

// checkpoint reads one record of the retained "checkpoints" channel, the way
// a reader outside the workload would.
func checkpoint(t *testing.T, h *harness, key string) Weights {
	t.Helper()
	recs, _, err := h.co.Records("checkpoints", key, 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(recs) != 1 {
		t.Fatalf("checkpoints: %d records for key %q, want 1", len(recs), key)
	}
	b, err := json.Marshal(recs[0].Value)
	if err != nil {
		t.Fatal(err)
	}
	var w Weights
	if err := json.Unmarshal(b, &w); err != nil {
		t.Fatal(err)
	}
	return w
}

// closedForm solves the two-by-two normal equations for the training set,
// independently of anything the workload computes.
func closedForm() []float64 {
	var sx, sy, sxx, sxy float64
	n := float64(samples)
	for i := 0; i < samples; i++ {
		x, y := point(i)
		sx += x
		sy += y
		sxx += x * x
		sxy += x * y
	}
	det := n*sxx - sx*sx
	return []float64{(sxx*sy - sx*sxy) / det, (n*sxy - sx*sy) / det}
}

// TestSynchronousFeedbackDoesNotWaitForAnotherOperation records why the
// weights channel is an Asynchronous feedback channel and not a Synchronous
// one, which for a training loop would be the obvious choice.
//
// A Synchronous feedback channel is a barrier: the consumer runs OnEpochEnd
// when the channel is quiescent at the current epoch, reports the epoch
// finished, and the coordinator releases the next one. Quiescent means
// nothing pending and nothing in flight — which is also true of a channel
// whose producer has not sent anything yet. When the producer is the same
// operation, as in examples/pagerank, that cannot happen: the pod fills the
// channel in OnEpochEnd and only then reports the epoch finished. When the
// producer is a different operation there is nothing holding the barrier,
// and the consumer runs through every epoch to the bound on its own.
//
// This test starts the consumer of such a channel and never starts the
// producer at all.
func TestSynchronousFeedbackDoesNotWaitForAnotherOperation(t *testing.T) {
	const maxEpochs = 3
	h, stop := newHarness(t, []v1alpha1.Channel{
		{Name: "gradients", From: "worker", To: "server",
			Partitioning: v1alpha1.Partitioning{Mode: v1alpha1.PartitionHash, Partitions: 1},
			Feedback:     &v1alpha1.Feedback{Mode: v1alpha1.FeedbackSynchronous, MaxEpochs: maxEpochs}},
	})
	defer stop()

	var mu sync.Mutex
	var ran []int32
	h.run("server", "server-0", []string{"gradients"}, nil, []string{"gradients"}, nil, sdk.Handlers{
		OnEpochEnd: func(ctx context.Context, w *sdk.Worker, epoch int32) error {
			mu.Lock()
			ran = append(ran, epoch)
			mu.Unlock()
			return nil
		},
	})
	h.waitComplete("server")

	mu.Lock()
	defer mu.Unlock()
	if fmt.Sprint(ran) != "[0 1 2]" {
		t.Errorf("consumer ran epochs %v, want [0 1 2]: the barrier is expected to run to the bound unaided", ran)
	}
	if cm := h.channel("gradients"); !cm.Sealed || cm.Produced != 0 {
		t.Errorf("gradients: %+v, want sealed with nothing produced", cm)
	}
}
