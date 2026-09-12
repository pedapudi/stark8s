package sdk

import (
	"context"
	"encoding/json"
	"errors"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/pedapudi/stark8s/api/graph"
	"github.com/pedapudi/stark8s/pkg/coordinator"
	"github.com/pedapudi/stark8s/pkg/etl"
	"github.com/pedapudi/stark8s/pkg/storage"
)

func TestETLReaderAndReducerRecoverAtCheckpointBoundaries(t *testing.T) {
	ctx := context.Background()
	files, _ := storage.NewLocal(t.TempDir())
	segments, _ := storage.NewLocal(t.TempDir())
	checkpoints, _ := storage.NewLocal(t.TempDir())
	inputs := map[string][]byte{
		"inputs/a.jsonl": []byte("{\"region\":\"north\",\"amount\":3,\"status\":\"settled\"}\n{\"region\":\"south\",\"amount\":8,\"status\":\"void\"}\n"),
		"inputs/b.jsonl": []byte("{\"region\":\"south\",\"amount\":4,\"status\":\"settled\"}\n{\"region\":\"north\",\"amount\":2,\"status\":\"settled\"}\n"),
	}
	var inputVersions []etl.Input
	for key, body := range inputs {
		if err := files.PutImmutable(ctx, key, body); err != nil {
			t.Fatal(err)
		}
		object, _ := files.Get(ctx, key)
		inputVersions = append(inputVersions, etl.Input{Key: key, Version: object.Version})
	}
	splits, err := etl.Plan(ctx, files, inputVersions)
	if err != nil {
		t.Fatal(err)
	}

	segmentServer := httptest.NewServer(nil)
	defer segmentServer.Close()
	co := coordinator.New(strings.TrimPrefix(segmentServer.URL, "http://"))
	if err := co.Configure([]graph.Channel{
		{Name: "splits", From: "source", To: "transform", Partitioning: graph.Partitioning{Mode: graph.PartitionHash, Partitions: 1}},
		{Name: "values", From: "transform", To: "aggregate", Partitioning: graph.Partitioning{Mode: graph.PartitionHash, Partitions: 1}},
	}); err != nil {
		t.Fatal(err)
	}
	segmentServer.Config.Handler = coordinator.SegmentHandler(co)
	control := httptest.NewServer(coordinator.Handler(co))
	defer control.Close()
	sourceCtx, stopSource := context.WithCancel(ctx)
	source := etlCheckpointWorker(control.URL, segmentServer.URL, segments, nil, "source", "source-0", "first", nil, []string{"splits"})
	sourceDone := make(chan error, 1)
	go func() {
		sourceDone <- source.Run(sourceCtx, Handlers{Source: func(_ context.Context, worker *Worker) error {
			for _, split := range splits {
				if err := worker.Emit("splits", split.ID, split); err != nil {
					return err
				}
			}
			return nil
		}})
	}()
	waitForOperation(t, co, "source")
	stopSource()
	<-sourceDone
	if err := co.Seal("splits"); err != nil {
		t.Fatal(err)
	}

	transform := Handlers{
		OnRecord: func(ctx context.Context, worker *Worker, record Record) error {
			var split etl.Split
			if err := json.Unmarshal(record.Value, &split); err != nil {
				return err
			}
			mapped, err := etl.TransformJSONLines(ctx, files, split, func(line json.RawMessage) (string, int64, bool, error) {
				var value struct {
					Region string `json:"region"`
					Amount int64  `json:"amount"`
					Status string `json:"status"`
				}
				err := json.Unmarshal(line, &value)
				return value.Region, value.Amount, value.Status == "settled", err
			})
			if err != nil {
				return err
			}
			for _, value := range mapped {
				if err := worker.Emit("values", value.Key, value.Value); err != nil {
					return err
				}
			}
			return nil
		},
		Snapshot: func(context.Context) ([]byte, error) { return []byte("{}"), nil },
		Restore:  func(context.Context, []byte) error { return nil },
	}
	crashed := errors.New("reader stopped after checkpoint commit")
	firstReader := etlCheckpointWorker(control.URL, segmentServer.URL, segments, checkpoints, "transform", "reader-old", "first", []string{"splits"}, []string{"values"})
	firstReader.checkpointAfterCommit = func() error { return crashed }
	if err := firstReader.Run(ctx, transform); !errors.Is(err, crashed) {
		t.Fatalf("first reader run: %v", err)
	}
	if got := channelMetric(co, "values").Produced; got != 0 {
		t.Fatalf("values visible before reader recovery = %d, want 0", got)
	}
	readerCtx, stopReader := context.WithCancel(ctx)
	replacementReader := etlCheckpointWorker(control.URL, segmentServer.URL, segments, checkpoints, "transform", "reader-old", "second", []string{"splits"}, []string{"values"})
	readerDone := make(chan error, 1)
	go func() { readerDone <- replacementReader.Run(readerCtx, transform) }()
	waitForOperation(t, co, "transform")
	stopReader()
	<-readerDone
	if got := channelMetric(co, "values").Produced; got != 3 {
		t.Fatalf("mapped values after reader recovery = %d, want 3", got)
	}
	if err := co.Seal("values"); err != nil {
		t.Fatal(err)
	}

	state := map[string]int64{}
	var output []etl.OutputFile
	aggregate := Handlers{
		OnRecord: func(_ context.Context, _ *Worker, record Record) error {
			var value int64
			if err := json.Unmarshal(record.Value, &value); err != nil {
				return err
			}
			state[record.Key] += value
			return nil
		},
		OnDrain: func(ctx context.Context, _ *Worker) error {
			records := make([]etl.KeyValue, 0, len(state))
			for key, value := range state {
				records = append(records, etl.KeyValue{Key: key, Value: value})
			}
			file, err := etl.WritePartition(ctx, files, "sales", "attempt-a", 0, records)
			if err == nil {
				output = []etl.OutputFile{file}
			}
			return err
		},
		Snapshot: func(context.Context) ([]byte, error) { return json.Marshal(state) },
		Restore: func(_ context.Context, body []byte) error {
			return json.Unmarshal(body, &state)
		},
	}
	crashed = errors.New("reducer stopped after input acknowledgement")
	firstReducer := etlCheckpointWorker(control.URL, segmentServer.URL, segments, checkpoints, "aggregate", "reducer-old", "first", []string{"values"}, nil)
	firstReducer.checkpointAfterAck = func() error { return crashed }
	if err := firstReducer.Run(ctx, aggregate); !errors.Is(err, crashed) {
		t.Fatalf("first reducer run: %v", err)
	}
	if len(output) != 0 {
		t.Fatal("reducer published output before recovery")
	}
	state = map[string]int64{}
	reducerCtx, stopReducer := context.WithCancel(ctx)
	replacementReducer := etlCheckpointWorker(control.URL, segmentServer.URL, segments, checkpoints, "aggregate", "reducer-old", "second", []string{"values"}, nil)
	reducerDone := make(chan error, 1)
	go func() { reducerDone <- replacementReducer.Run(reducerCtx, aggregate) }()
	waitForOperation(t, co, "aggregate")
	stopReducer()
	<-reducerDone
	if len(output) != 1 {
		t.Fatalf("output files = %d, want 1", len(output))
	}
	if _, err := etl.Publish(ctx, files, "sales", "attempt-a", 1, output); err != nil {
		t.Fatal(err)
	}
	object, err := files.Get(ctx, output[0].Key)
	if err != nil {
		t.Fatal(err)
	}
	var result []etl.KeyValue
	for _, line := range strings.Split(strings.TrimSpace(string(object.Bytes)), "\n") {
		var record etl.KeyValue
		if err := json.Unmarshal([]byte(line), &record); err != nil {
			t.Fatal(err)
		}
		result = append(result, record)
	}
	want := []etl.KeyValue{{Key: "north", Value: 5}, {Key: "south", Value: 4}}
	if !reflect.DeepEqual(result, want) {
		t.Fatalf("recovered result = %#v, want %#v", result, want)
	}
}

func etlCheckpointWorker(controlURL, segmentURL string, segments, checkpoints storage.Store, operation, instance, incarnation string, inbound, outbound []string) *Worker {
	return &Worker{Coordinator: controlURL, Operation: operation, Instance: instance, Incarnation: incarnation, Inbound: inbound, Outbound: outbound, SegmentListen: "127.0.0.1:0", DurableSegments: segments, SegmentBaseURL: segmentURL, SegmentPrefix: "etl", CheckpointStore: checkpoints, CheckpointPrefix: "etl"}
}

func waitForOperation(t *testing.T, co *coordinator.Coordinator, operation string) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		for _, metric := range co.Metrics().Operations {
			if metric.Name == operation && metric.Complete {
				return
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("operation %s did not complete: %+v", operation, co.Metrics())
}
