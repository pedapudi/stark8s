package coordinator

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/pedapudi/stark8s/api/graph"
	"github.com/pedapudi/stark8s/pkg/storage"
)

func TestS3IntegrationExternalInputUsesCoordinatorPrefix(t *testing.T) {
	endpoint, credentials, prefix := s3IntegrationConfig(t)
	store, err := storage.NewS3(endpoint, credentials, &http.Client{Timeout: 10 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	checkpointKey := prefix + "/coordinator/coordinator.json"
	co, err := NewDurable(ctx, "coordinator.invalid:8090", store, checkpointKey, "integration-writer")
	if err != nil {
		t.Fatal(err)
	}
	co.Configure([]graph.Channel{{Name: "external-input", To: "worker", Durability: graph.DurabilityRetained}})
	if err := co.Produce("external-input", "", []Record{{Key: "key", Value: "value"}}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Get(ctx, prefix+"/coordinator/segments/ext-1"); err != nil {
		t.Fatalf("coordinator-prefixed external input: %v", err)
	}
	if _, err := store.Get(ctx, "segments/ext-1"); err == nil {
		t.Fatal("external input also appeared outside the coordinator prefix")
	}
}

func s3IntegrationConfig(t *testing.T) (string, storage.S3Credentials, string) {
	t.Helper()
	endpoint := os.Getenv("STARK8S_S3_TEST_ENDPOINT")
	credentials := storage.S3Credentials{AccessKey: os.Getenv("STARK8S_S3_TEST_ACCESS_KEY"), SecretKey: os.Getenv("STARK8S_S3_TEST_SECRET_KEY"), SessionToken: os.Getenv("STARK8S_S3_TEST_SESSION_TOKEN"), Region: os.Getenv("STARK8S_S3_TEST_REGION")}
	if endpoint == "" || credentials.AccessKey == "" || credentials.SecretKey == "" || credentials.Region == "" {
		t.Skip("set the STARK8S_S3_TEST_* connection variables")
	}
	prefix := strings.Trim(os.Getenv("STARK8S_S3_TEST_PREFIX"), "/")
	if prefix == "" {
		prefix = fmt.Sprintf("integration/%d", time.Now().UTC().UnixNano())
	}
	return endpoint, credentials, prefix
}
