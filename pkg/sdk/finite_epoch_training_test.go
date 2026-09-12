package sdk

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/pedapudi/stark8s/api/graph"
	"github.com/pedapudi/stark8s/pkg/coordinator"
)

func TestFiniteEpochTrainingPipelineHandlesEmptyAndDelayedOutput(t *testing.T) {
	const epochs = 4
	h, stop := newHarness(t, []graph.Channel{
		{Name: "initial", From: "seed", To: "producer", Delivery: graph.DeliveryMaterialized,
			Partitioning: graph.Partitioning{Mode: graph.PartitionHash, Partitions: 1}},
		{Name: "candidates", From: "producer", To: "score", Delivery: graph.DeliveryMaterialized,
			Partitioning: graph.Partitioning{Mode: graph.PartitionRoundRobin, Partitions: 2}},
		{Name: "scores", From: "score", To: "learner", Delivery: graph.DeliveryMaterialized,
			Partitioning: graph.Partitioning{Mode: graph.PartitionHash, Partitions: 1}},
		{Name: "feedback", From: "learner", To: "producer", Delivery: graph.DeliveryMaterialized,
			Partitioning: graph.Partitioning{Mode: graph.PartitionBroadcast, Partitions: 1},
			Feedback:     &graph.Feedback{Mode: graph.FeedbackSynchronous, MaxEpochs: epochs}},
	})
	defer stop()
	h.co.SetOperations([]coordinator.OperationSpec{
		{Name: "seed", Replicas: 1},
		{Name: "producer", Replicas: 1},
		{Name: "score", Replicas: 2},
		{Name: "learner", Replicas: 1},
	})

	producer := h.worker("producer", "producer-0", []string{"initial", "feedback"}, []string{"candidates"})
	producer.SetFeedback([]string{"feedback"}, nil)
	h.run(producer, Handlers{
		OnRecord: func(context.Context, *Worker, Record) error { return nil },
		OnEpochEnd: func(ctx context.Context, worker *Worker, epoch int32) error {
			values := map[int32][]int{0: {1, 2}, 1: {}, 2: {3}, 3: {4}}[epoch]
			if epoch == 2 {
				time.Sleep(100 * time.Millisecond)
			}
			for index, value := range values {
				if err := worker.Emit("candidates", fmt.Sprintf("%d-%d", epoch, index), value); err != nil {
					return err
				}
			}
			return nil
		},
	})

	for index := 0; index < 2; index++ {
		h.run(h.worker("score", fmt.Sprintf("score-%d", index), []string{"candidates"}, []string{"scores"}), Handlers{
			OnRecord: func(ctx context.Context, worker *Worker, record Record) error {
				var value int
				if err := json.Unmarshal(record.Value, &value); err != nil {
					return err
				}
				return worker.Emit("scores", record.Key, value*value)
			},
		})
	}

	var mu sync.Mutex
	byEpoch := map[int32]int{}
	var completed []int
	learner := h.worker("learner", "learner-0", []string{"scores"}, []string{"feedback"})
	learner.SetFeedback(nil, []string{"feedback"})
	h.run(learner, Handlers{
		OnRecord: func(ctx context.Context, worker *Worker, record Record) error {
			var value int
			if err := json.Unmarshal(record.Value, &value); err != nil {
				return err
			}
			mu.Lock()
			byEpoch[record.Epoch] += value
			mu.Unlock()
			return nil
		},
		OnEpochEnd: func(ctx context.Context, worker *Worker, epoch int32) error {
			mu.Lock()
			completed = append(completed, byEpoch[epoch])
			mu.Unlock()
			return worker.Emit("feedback", "weights", epoch+1)
		},
	})

	h.run(h.worker("seed", "seed-0", nil, []string{"initial"}), Handlers{
		Source: func(ctx context.Context, worker *Worker) error {
			return worker.Emit("initial", "start", true)
		},
	})
	h.waitComplete("seed")
	if err := h.co.Seal("initial"); err != nil {
		t.Fatal(err)
	}
	started := time.Now()
	h.waitComplete("producer")
	if err := h.co.Seal("candidates"); err != nil {
		t.Fatal(err)
	}
	h.waitComplete("score")
	if err := h.co.Seal("scores"); err != nil {
		t.Fatal(err)
	}
	h.waitComplete("learner")
	if elapsed := time.Since(started); elapsed < 100*time.Millisecond {
		t.Fatalf("pipeline completed in %v before delayed epoch output", elapsed)
	}
	mu.Lock()
	defer mu.Unlock()
	want := []int{5, 0, 9, 16}
	if !reflect.DeepEqual(completed, want) {
		t.Fatalf("epoch results = %v, want %v", completed, want)
	}
}
