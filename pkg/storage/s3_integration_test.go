package storage

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/url"
	"os"
	"strings"
	"testing"
	"time"
)

// TestS3Integration exercises the signed adapter against an actual
// S3-compatible service. It is opt-in because it writes retained objects.
//
// STARK8S_S3_TEST_ENDPOINT=http://127.0.0.1:9000/test-bucket \
// STARK8S_S3_TEST_ACCESS_KEY=... STARK8S_S3_TEST_SECRET_KEY=... \
// STARK8S_S3_TEST_REGION=us-east-1 go test ./pkg/storage ./pkg/coordinator \
// ./pkg/sdk -run S3Integration -v
func TestS3Integration(t *testing.T) {
	endpoint := os.Getenv("STARK8S_S3_TEST_ENDPOINT")
	accessKey := os.Getenv("STARK8S_S3_TEST_ACCESS_KEY")
	secretKey := os.Getenv("STARK8S_S3_TEST_SECRET_KEY")
	region := os.Getenv("STARK8S_S3_TEST_REGION")
	if endpoint == "" || accessKey == "" || secretKey == "" || region == "" {
		t.Skip("set STARK8S_S3_TEST_ENDPOINT, STARK8S_S3_TEST_ACCESS_KEY, STARK8S_S3_TEST_SECRET_KEY, and STARK8S_S3_TEST_REGION")
	}
	store, err := NewS3(endpoint, S3Credentials{AccessKey: accessKey, SecretKey: secretKey, SessionToken: os.Getenv("STARK8S_S3_TEST_SESSION_TOKEN"), Region: region}, &http.Client{Timeout: 10 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	prefix := strings.Trim(os.Getenv("STARK8S_S3_TEST_PREFIX"), "/")
	if prefix == "" {
		prefix = "integration/" + time.Now().UTC().Format("20060102T150405.000000000")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	immutableKey := prefix + "/immutable/path with + and %/object.json"
	if err := PutImmutable(ctx, store, immutableKey, []byte("same")); err != nil {
		t.Fatal(err)
	}
	if err := PutImmutable(ctx, store, immutableKey, []byte("same")); err != nil {
		t.Fatalf("identical immutable retry: %v", err)
	}
	if err := PutImmutable(ctx, store, immutableKey, []byte("different")); !errors.Is(err, ErrConflict) {
		t.Fatalf("mismatched immutable retry: got %v, want conflict", err)
	}
	assertPrivateSignedGet(t, ctx, endpoint, store, immutableKey, "same")

	casKey := prefix + "/coordinator/checkpoint.json"
	firstVersion, err := store.CompareAndSwap(ctx, casKey, "", []byte("first"))
	if err != nil {
		t.Fatal(err)
	}
	if firstVersion == "" {
		t.Fatal("conditional create returned an empty ETag")
	}
	secondVersion, err := store.CompareAndSwap(ctx, casKey, firstVersion, []byte("second"))
	if err != nil {
		t.Fatal(err)
	}
	if secondVersion == "" || secondVersion == firstVersion {
		t.Fatalf("replacement ETag = %q after %q", secondVersion, firstVersion)
	}
	if _, err := store.CompareAndSwap(ctx, casKey, firstVersion, []byte("stale")); !errors.Is(err, ErrConflict) {
		t.Fatalf("stale ETag: got %v, want conflict", err)
	}

	writerKey := prefix + "/coordinator/writer.json"
	firstWriter, _, err := Claim(ctx, store, writerKey, "writer-one")
	if err != nil {
		t.Fatal(err)
	}
	if err := firstWriter.Commit(ctx, json.RawMessage(`{"value":1}`), nil); err != nil {
		t.Fatal(err)
	}
	secondWriter, recovered, err := Claim(ctx, store, writerKey, "writer-two")
	if err != nil {
		t.Fatal(err)
	}
	if string(recovered.State) != `{"value":1}` {
		t.Fatalf("replacement recovered %s", recovered.State)
	}
	if err := firstWriter.Commit(ctx, json.RawMessage(`{"value":2}`), nil); !errors.Is(err, ErrFenced) {
		t.Fatalf("superseded writer: got %v, want fenced", err)
	}
	if err := secondWriter.Commit(ctx, json.RawMessage(`{"value":3}`), nil); err != nil {
		t.Fatal(err)
	}

	for key, body := range map[string]string{
		prefix + "/coordinator/segments/external-input":           "external",
		prefix + "/operations/worker-data/segments/worker-output": "worker",
	} {
		if err := PutImmutable(ctx, store, key, []byte(body)); err != nil {
			t.Fatalf("write %s: %v", key, err)
		}
		object, err := store.Get(ctx, key)
		if err != nil || string(object.Bytes) != body {
			t.Fatalf("read %s: body=%q err=%v", key, object.Bytes, err)
		}
	}
	t.Logf("retained test object prefix: %s", prefix)
}

func assertPrivateSignedGet(t *testing.T, ctx context.Context, endpoint string, store Store, key, want string) {
	t.Helper()
	base, err := url.Parse(strings.TrimRight(endpoint, "/"))
	if err != nil {
		t.Fatal(err)
	}
	base.Path += "/" + key
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, base.String(), nil)
	if err != nil {
		t.Fatal(err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	_, _ = io.Copy(io.Discard, resp.Body)
	resp.Body.Close()
	if resp.StatusCode != http.StatusForbidden {
		t.Fatalf("anonymous GET returned %s, want 403 Forbidden", resp.Status)
	}
	object, err := store.Get(ctx, key)
	if err != nil || string(object.Bytes) != want {
		t.Fatalf("signed GET: body=%q err=%v", object.Bytes, err)
	}
}
