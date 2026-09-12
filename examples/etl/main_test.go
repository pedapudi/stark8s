package main

import (
	"context"
	"encoding/json"
	"reflect"
	"testing"

	"github.com/pedapudi/stark8s/pkg/coordinator"
	"github.com/pedapudi/stark8s/pkg/sdk"
	"github.com/pedapudi/stark8s/pkg/storage"
)

func TestAggregateSnapshotRestoresRunningSums(t *testing.T) {
	store, err := storage.NewLocal(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	first := aggregateHandlers(store, "attempt-a")
	for _, record := range []sdk.Record{
		{Key: "north", Value: json.RawMessage("3")},
		{Key: "south", Value: json.RawMessage("4")},
	} {
		if err := first.OnRecord(context.Background(), nil, record); err != nil {
			t.Fatal(err)
		}
	}
	snapshot, err := first.Snapshot(context.Background())
	if err != nil {
		t.Fatal(err)
	}

	restored := aggregateHandlers(store, "attempt-a")
	if err := restored.Restore(context.Background(), snapshot); err != nil {
		t.Fatal(err)
	}
	if err := restored.OnRecord(context.Background(), nil, sdk.Record{Key: "north", Value: json.RawMessage("2")}); err != nil {
		t.Fatal(err)
	}
	got, err := restored.Snapshot(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	var state map[int]map[string]int64
	if err := json.Unmarshal(got, &state); err != nil {
		t.Fatal(err)
	}
	want := map[int]map[string]int64{}
	northPartition := coordinator.HashPartition("north", outputPartitions)
	southPartition := coordinator.HashPartition("south", outputPartitions)
	want[northPartition] = map[string]int64{"north": 5}
	if southPartition == northPartition {
		want[southPartition]["south"] = 4
	} else {
		want[southPartition] = map[string]int64{"south": 4}
	}
	if !reflect.DeepEqual(state, want) {
		t.Fatalf("restored state = %#v, want %#v", state, want)
	}
}
