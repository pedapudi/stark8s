package sdk

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/pedapudi/stark8s/pkg/coordinator"
	"github.com/pedapudi/stark8s/pkg/storage"
)

func TestS3IntegrationWorkerDataUsesOperationPrefix(t *testing.T) {
	endpoint := os.Getenv("STARK8S_S3_TEST_ENDPOINT")
	credentials := storage.S3Credentials{AccessKey: os.Getenv("STARK8S_S3_TEST_ACCESS_KEY"), SecretKey: os.Getenv("STARK8S_S3_TEST_SECRET_KEY"), SessionToken: os.Getenv("STARK8S_S3_TEST_SESSION_TOKEN"), Region: os.Getenv("STARK8S_S3_TEST_REGION")}
	if endpoint == "" || credentials.AccessKey == "" || credentials.SecretKey == "" || credentials.Region == "" {
		t.Skip("set the STARK8S_S3_TEST_* connection variables")
	}
	prefix := strings.Trim(os.Getenv("STARK8S_S3_TEST_PREFIX"), "/")
	if prefix == "" {
		prefix = fmt.Sprintf("integration/%d", time.Now().UTC().UnixNano())
	}
	store, err := storage.NewS3(endpoint, credentials, &http.Client{Timeout: 10 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	workerPrefix := prefix + "/operations/worker-data"
	body, _ := json.Marshal([]wireRecord{{Key: "key", Value: json.RawMessage(`"value"`)}})
	if err := storage.PutImmutable(context.Background(), store, workerPrefix+"/segments/output-1", body); err != nil {
		t.Fatal(err)
	}
	w := &Worker{DurableSegments: store, SegmentPrefix: workerPrefix}
	records, err := w.fetch(context.Background(), coordinator.SegmentRef{ID: "output-1"})
	if err != nil || len(records) != 1 || records[0].Key != "key" {
		t.Fatalf("worker-prefixed fetch: records=%+v err=%v", records, err)
	}
}
