// Command etl runs a bounded JSON Lines aggregation over immutable files.
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"strconv"

	"github.com/pedapudi/stark8s/pkg/coordinator"
	"github.com/pedapudi/stark8s/pkg/etl"
	"github.com/pedapudi/stark8s/pkg/sdk"
	"github.com/pedapudi/stark8s/pkg/storage"
)

const (
	dataset          = "regional-sales"
	outputPartitions = 2
)

type sale struct {
	Region string `json:"region"`
	Amount int64  `json:"amount"`
	Status string `json:"status"`
}

func main() {
	if len(os.Args) != 2 {
		log.Fatal("usage: etl read|transform|aggregate|publish")
	}
	storeDir := os.Getenv("ETL_STORE_DIR")
	if storeDir == "" {
		log.Fatal("ETL_STORE_DIR must name the mounted durable directory")
	}
	store, err := storage.NewLocal(storeDir)
	if err != nil {
		log.Fatal(err)
	}
	worker, err := sdk.FromEnv()
	if err != nil {
		log.Fatal(err)
	}
	attempt := os.Getenv("ETL_ATTEMPT")
	if attempt == "" {
		log.Fatal("ETL_ATTEMPT must be stable across a full-stage retry")
	}

	var handlers sdk.Handlers
	switch os.Args[1] {
	case "read":
		handlers.Source = func(ctx context.Context, worker *sdk.Worker) error {
			object, err := store.Get(ctx, "inputs/manifest.json")
			if err != nil {
				return err
			}
			var inputs []etl.Input
			if err := json.Unmarshal(object.Bytes, &inputs); err != nil {
				return err
			}
			splits, err := etl.Plan(ctx, store, inputs)
			if err != nil {
				return err
			}
			for _, split := range splits {
				if err := worker.Emit("splits", split.ID, split); err != nil {
					return err
				}
			}
			return nil
		}
	case "transform":
		handlers.OnRecord = func(ctx context.Context, worker *sdk.Worker, record sdk.Record) error {
			var split etl.Split
			if err := json.Unmarshal(record.Value, &split); err != nil {
				return err
			}
			mapped, err := etl.TransformJSONLines(ctx, store, split, func(line json.RawMessage) (string, int64, bool, error) {
				var value sale
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
		}
		handlers.Snapshot = func(context.Context) ([]byte, error) { return []byte("{}"), nil }
		handlers.Restore = func(_ context.Context, body []byte) error {
			var state struct{}
			return json.Unmarshal(body, &state)
		}
	case "aggregate":
		handlers = aggregateHandlers(store, attempt)
	case "publish":
		files := make([]etl.OutputFile, 0, outputPartitions)
		handlers.OnRecord = func(ctx context.Context, worker *sdk.Worker, record sdk.Record) error {
			var file etl.OutputFile
			if err := json.Unmarshal(record.Value, &file); err != nil {
				return err
			}
			files = append(files, file)
			return nil
		}
		handlers.OnDrain = func(ctx context.Context, worker *sdk.Worker) error {
			manifest, err := etl.Publish(ctx, store, dataset, attempt, outputPartitions, files)
			if err != nil {
				return err
			}
			return worker.Emit("completed", dataset, manifest)
		}
	default:
		log.Fatal(fmt.Sprintf("unknown operation %q", os.Args[1]))
	}
	if err := worker.Run(context.Background(), handlers); err != nil {
		log.Fatal(err)
	}
}

func aggregateHandlers(store storage.Store, attempt string) sdk.Handlers {
	partitions := map[int]map[string]int64{}
	return sdk.Handlers{
		OnRecord: func(ctx context.Context, worker *sdk.Worker, record sdk.Record) error {
			var value int64
			if err := json.Unmarshal(record.Value, &value); err != nil {
				return err
			}
			partition := coordinator.HashPartition(record.Key, outputPartitions)
			if partitions[partition] == nil {
				partitions[partition] = map[string]int64{}
			}
			partitions[partition][record.Key] += value
			return nil
		},
		OnDrain: func(ctx context.Context, worker *sdk.Worker) error {
			for partition := 0; partition < outputPartitions; partition++ {
				values := make([]etl.KeyValue, 0, len(partitions[partition]))
				for key, value := range partitions[partition] {
					values = append(values, etl.KeyValue{Key: key, Value: value})
				}
				file, err := etl.WritePartition(ctx, store, dataset, attempt, partition, values)
				if err != nil {
					return err
				}
				if err := worker.Emit("output-files", strconv.Itoa(partition), file); err != nil {
					return err
				}
			}
			return nil
		},
		Snapshot: func(context.Context) ([]byte, error) {
			return json.Marshal(partitions)
		},
		Restore: func(_ context.Context, body []byte) error {
			return json.Unmarshal(body, &partitions)
		},
	}
}
