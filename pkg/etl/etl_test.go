package etl

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"testing"

	"github.com/pedapudi/stark8s/pkg/storage"
)

type sale struct {
	Region string `json:"region"`
	Amount int64  `json:"amount"`
	Keep   bool   `json:"keep"`
}

func saleMapper(line json.RawMessage) (string, int64, bool, error) {
	var value sale
	err := json.Unmarshal(line, &value)
	return value.Region, value.Amount, value.Keep, err
}

func TestReplayMatchesReferenceAndPublishesAtomically(t *testing.T) {
	ctx := context.Background()
	store, err := storage.NewLocal(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	inputs := map[string][]byte{
		"inputs/north.jsonl": []byte("{\"region\":\"north\",\"amount\":3,\"keep\":true}\n{\"region\":\"south\",\"amount\":9,\"keep\":false}\n"),
		"inputs/south.jsonl": []byte("{\"region\":\"south\",\"amount\":4,\"keep\":true}\n{\"region\":\"north\",\"amount\":2,\"keep\":true}\n"),
	}
	var planned []Input
	for key, body := range inputs {
		if err := store.PutImmutable(ctx, key, body); err != nil {
			t.Fatal(err)
		}
		object, _ := store.Get(ctx, key)
		planned = append(planned, Input{Key: key, Version: object.Version})
	}
	splits, err := Plan(ctx, store, planned)
	if err != nil {
		t.Fatal(err)
	}

	// A reader can fail after processing part of a split. The partial return is
	// discarded, and replaying the immutable split produces the reference.
	calls := 0
	_, err = TransformJSONLines(ctx, store, splits[0], func(line json.RawMessage) (string, int64, bool, error) {
		calls++
		if calls == 2 {
			return "", 0, false, errors.New("reader stopped")
		}
		return saleMapper(line)
	})
	if err == nil {
		t.Fatal("reader failure was not returned")
	}
	first, err := TransformJSONLines(ctx, store, splits[0], saleMapper)
	if err != nil {
		t.Fatal(err)
	}
	replayed, err := TransformJSONLines(ctx, store, splits[0], saleMapper)
	if err != nil || !reflect.DeepEqual(first, replayed) {
		t.Fatalf("reader replay changed output: %v, %v", replayed, err)
	}
	var mapped []KeyValue
	for _, split := range splits {
		records, err := TransformJSONLines(ctx, store, split, saleMapper)
		if err != nil {
			t.Fatal(err)
		}
		mapped = append(mapped, records...)
	}
	reference := []KeyValue{{Key: "north", Value: 5}, {Key: "south", Value: 4}}
	if got := Aggregate(mapped); !reflect.DeepEqual(got, reference) {
		t.Fatalf("aggregate = %#v, want %#v", got, reference)
	}

	part0, err := WritePartition(ctx, store, "sales", "attempt-a", 0, reference[:1])
	if err != nil {
		t.Fatal(err)
	}
	if _, err := Open(ctx, store, "sales"); !errors.Is(err, storage.ErrNotFound) {
		t.Fatalf("partial output became visible: %v", err)
	}
	// A reducer restart repeats its full partition. Immutable writes accept
	// the same bytes at the same attempt key.
	if replayedPart, err := WritePartition(ctx, store, "sales", "attempt-a", 0, reference[:1]); err != nil || replayedPart != part0 {
		t.Fatalf("partition replay = %#v, %v", replayedPart, err)
	}
	part1, err := WritePartition(ctx, store, "sales", "attempt-a", 1, reference[1:])
	if err != nil {
		t.Fatal(err)
	}
	manifest, err := Publish(ctx, store, "sales", "attempt-a", 2, []OutputFile{part1, part0})
	if err != nil {
		t.Fatal(err)
	}
	opened, err := Open(ctx, store, "sales")
	if err != nil || !reflect.DeepEqual(opened, manifest) {
		t.Fatalf("opened manifest = %#v, %v", opened, err)
	}
	var output []KeyValue
	for _, file := range opened.Files {
		object, err := store.Get(ctx, file.Key)
		if err != nil {
			t.Fatal(err)
		}
		scanner := bufio.NewScanner(bytes.NewReader(object.Bytes))
		for scanner.Scan() {
			var record KeyValue
			if err := json.Unmarshal(scanner.Bytes(), &record); err != nil {
				t.Fatal(err)
			}
			output = append(output, record)
		}
	}
	if !reflect.DeepEqual(output, reference) {
		t.Fatalf("committed output = %#v, want %#v", output, reference)
	}
	if _, err := Publish(ctx, store, "sales", "attempt-a", 2, []OutputFile{part0, part1}); err != nil {
		t.Fatalf("manifest retry: %v", err)
	}
}

func TestChangedInputVersionAndIncompleteOutputFail(t *testing.T) {
	ctx := context.Background()
	store, _ := storage.NewLocal(t.TempDir())
	if err := store.PutImmutable(ctx, "inputs/data.jsonl", []byte("{}\n")); err != nil {
		t.Fatal(err)
	}
	if _, err := Plan(ctx, store, []Input{{Key: "inputs/data.jsonl", Version: "stale"}}); err == nil {
		t.Fatal("Plan accepted a stale input version")
	}
	object, _ := store.Get(ctx, "inputs/data.jsonl")
	if _, err := Plan(ctx, store, []Input{{Key: "inputs/data.jsonl", Version: object.Version}, {Key: "inputs/data.jsonl", Version: object.Version}}); err == nil {
		t.Fatal("Plan accepted a duplicate input key")
	}
	if _, err := Publish(ctx, store, "result", "attempt-a", 2, nil); err == nil {
		t.Fatal("Publish accepted missing partitions")
	}
	if _, err := Publish(ctx, store, "result", "attempt-a", 0, nil); err == nil {
		t.Fatal("Publish accepted a non-positive partition count")
	}
}
