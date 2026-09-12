// Package etl provides replayable file splits and committed file output for
// bounded map, filter, and keyed-aggregation workloads.
package etl

import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"

	"github.com/pedapudi/stark8s/pkg/storage"
)

// Input names an immutable object version. One input object is one split.
type Input struct {
	Key     string `json:"key"`
	Version string `json:"version"`
}

// Split is a stable assignment that always resolves to the same input bytes.
type Split struct {
	ID      string `json:"id"`
	Key     string `json:"key"`
	Version string `json:"version"`
}

// KeyValue is the intermediate and output record supported by the helper.
type KeyValue struct {
	Key   string `json:"key"`
	Value int64  `json:"value"`
}

// Mapper combines record parsing, filtering, and mapping. Returning keep=false
// drops the input record.
type Mapper func(json.RawMessage) (key string, value int64, keep bool, err error)

// OutputFile describes one immutable output partition.
type OutputFile struct {
	Partition int    `json:"partition"`
	Key       string `json:"key"`
	Version   string `json:"version"`
	Records   int    `json:"records"`
}

// Manifest is the atomically published description of a complete dataset.
type Manifest struct {
	Dataset string       `json:"dataset"`
	Attempt string       `json:"attempt"`
	Files   []OutputFile `json:"files"`
}

// Plan validates input versions and returns stable splits in key order.
func Plan(ctx context.Context, store storage.Store, inputs []Input) ([]Split, error) {
	sorted := append([]Input(nil), inputs...)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i].Key < sorted[j].Key })
	splits := make([]Split, 0, len(sorted))
	for index, input := range sorted {
		if index > 0 && sorted[index-1].Key == input.Key {
			return nil, fmt.Errorf("duplicate input key %q", input.Key)
		}
		object, err := store.Get(ctx, input.Key)
		if err != nil {
			return nil, fmt.Errorf("read input %q: %w", input.Key, err)
		}
		if object.Version != input.Version {
			return nil, fmt.Errorf("input %q version changed: have %s, want %s", input.Key, object.Version, input.Version)
		}
		sum := sha256.Sum256([]byte(input.Key + "\x00" + input.Version))
		splits = append(splits, Split{ID: hex.EncodeToString(sum[:16]), Key: input.Key, Version: input.Version})
	}
	return splits, nil
}

// TransformJSONLines revalidates a split and maps each non-empty JSON line.
func TransformJSONLines(ctx context.Context, store storage.Store, split Split, mapper Mapper) ([]KeyValue, error) {
	object, err := store.Get(ctx, split.Key)
	if err != nil {
		return nil, err
	}
	if object.Version != split.Version {
		return nil, fmt.Errorf("split %s input version changed", split.ID)
	}
	var output []KeyValue
	scanner := bufio.NewScanner(bytes.NewReader(object.Bytes))
	scanner.Buffer(make([]byte, 64*1024), 16*1024*1024)
	for scanner.Scan() {
		line := bytes.TrimSpace(scanner.Bytes())
		if len(line) == 0 {
			continue
		}
		key, value, keep, err := mapper(json.RawMessage(append([]byte(nil), line...)))
		if err != nil {
			return nil, fmt.Errorf("split %s: %w", split.ID, err)
		}
		if keep {
			output = append(output, KeyValue{Key: key, Value: value})
		}
	}
	if err := scanner.Err(); err != nil {
		return nil, err
	}
	return output, nil
}

// Aggregate sums values by key and returns records in key order.
func Aggregate(records []KeyValue) []KeyValue {
	totals := make(map[string]int64)
	for _, record := range records {
		totals[record.Key] += record.Value
	}
	result := make([]KeyValue, 0, len(totals))
	for key, value := range totals {
		result = append(result, KeyValue{Key: key, Value: value})
	}
	sort.Slice(result, func(i, j int) bool { return result[i].Key < result[j].Key })
	return result
}

// WritePartition writes canonical JSON Lines under an attempt-specific key.
// An identical retry succeeds; different bytes for the same attempt conflict.
func WritePartition(ctx context.Context, store storage.Store, dataset, attempt string, partition int, records []KeyValue) (OutputFile, error) {
	if err := validateComponent("dataset", dataset); err != nil {
		return OutputFile{}, err
	}
	if err := validateComponent("attempt", attempt); err != nil {
		return OutputFile{}, err
	}
	if partition < 0 {
		return OutputFile{}, fmt.Errorf("partition must be non-negative")
	}
	records = append([]KeyValue(nil), records...)
	sort.Slice(records, func(i, j int) bool {
		if records[i].Key == records[j].Key {
			return records[i].Value < records[j].Value
		}
		return records[i].Key < records[j].Key
	})
	var body bytes.Buffer
	encoder := json.NewEncoder(&body)
	for _, record := range records {
		if err := encoder.Encode(record); err != nil {
			return OutputFile{}, err
		}
	}
	key := fmt.Sprintf("datasets/%s/attempts/%s/part-%05d.jsonl", dataset, attempt, partition)
	if err := storage.PutImmutable(ctx, store, key, body.Bytes()); err != nil {
		return OutputFile{}, err
	}
	object, err := store.Get(ctx, key)
	if err != nil {
		return OutputFile{}, err
	}
	return OutputFile{Partition: partition, Key: key, Version: object.Version, Records: len(records)}, nil
}

// Publish makes a complete dataset visible after every required partition is
// present with the recorded immutable version. The first valid attempt wins.
func Publish(ctx context.Context, store storage.Store, dataset, attempt string, partitionCount int, files []OutputFile) (Manifest, error) {
	if err := validateComponent("dataset", dataset); err != nil {
		return Manifest{}, err
	}
	if err := validateComponent("attempt", attempt); err != nil {
		return Manifest{}, err
	}
	if partitionCount <= 0 {
		return Manifest{}, fmt.Errorf("partition count must be positive")
	}
	if len(files) != partitionCount {
		return Manifest{}, fmt.Errorf("have %d output partitions, want %d", len(files), partitionCount)
	}
	files = append([]OutputFile(nil), files...)
	sort.Slice(files, func(i, j int) bool { return files[i].Partition < files[j].Partition })
	seen := make(map[int]bool, len(files))
	for partition, file := range files {
		if seen[file.Partition] {
			return Manifest{}, fmt.Errorf("duplicate output partition %d", file.Partition)
		}
		seen[file.Partition] = true
		if file.Partition != partition {
			return Manifest{}, fmt.Errorf("missing output partition %d", partition)
		}
		expectedKey := fmt.Sprintf("datasets/%s/attempts/%s/part-%05d.jsonl", dataset, attempt, partition)
		if file.Key != expectedKey {
			return Manifest{}, fmt.Errorf("output partition %d has key %q, want %q", partition, file.Key, expectedKey)
		}
		object, err := store.Get(ctx, file.Key)
		if err != nil {
			return Manifest{}, fmt.Errorf("read output partition %d: %w", partition, err)
		}
		if object.Version != file.Version {
			return Manifest{}, fmt.Errorf("output partition %d version changed", partition)
		}
	}
	manifest := Manifest{Dataset: dataset, Attempt: attempt, Files: files}
	body, err := json.Marshal(manifest)
	if err != nil {
		return Manifest{}, err
	}
	key := fmt.Sprintf("datasets/%s/complete.json", dataset)
	if _, err = store.CompareAndSwap(ctx, key, "", body); err == nil {
		return manifest, nil
	}
	if !errors.Is(err, storage.ErrConflict) {
		return Manifest{}, err
	}
	existing, getErr := store.Get(ctx, key)
	if getErr == nil && bytes.Equal(existing.Bytes, body) {
		return manifest, nil
	}
	return Manifest{}, err
}

// Open reads a published manifest and verifies every referenced partition.
func Open(ctx context.Context, store storage.Store, dataset string) (Manifest, error) {
	if err := validateComponent("dataset", dataset); err != nil {
		return Manifest{}, err
	}
	object, err := store.Get(ctx, fmt.Sprintf("datasets/%s/complete.json", dataset))
	if err != nil {
		return Manifest{}, err
	}
	var manifest Manifest
	if err := json.Unmarshal(object.Bytes, &manifest); err != nil {
		return Manifest{}, err
	}
	if manifest.Dataset != dataset {
		return Manifest{}, fmt.Errorf("completion manifest names dataset %q", manifest.Dataset)
	}
	for _, file := range manifest.Files {
		part, err := store.Get(ctx, file.Key)
		if err != nil || part.Version != file.Version {
			return Manifest{}, fmt.Errorf("published partition %d is unavailable or changed", file.Partition)
		}
	}
	return manifest, nil
}

func validateComponent(name, value string) error {
	if value == "" || value == "." || value == ".." || strings.ContainsAny(value, "/\\") {
		return fmt.Errorf("invalid %s %q", name, value)
	}
	return nil
}
