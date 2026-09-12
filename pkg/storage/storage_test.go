package storage

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestLocalImmutableAndCompareAndSwap(t *testing.T) {
	store, err := NewLocal(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	if err := PutImmutable(ctx, store, "segments/a", []byte("one")); err != nil {
		t.Fatal(err)
	}
	if err := PutImmutable(ctx, store, "segments/a", []byte("one")); err != nil {
		t.Fatal(err)
	}
	if err := PutImmutable(ctx, store, "segments/a", []byte("two")); !errors.Is(err, ErrConflict) {
		t.Fatalf("got %v, want conflict", err)
	}
	v1, err := store.CompareAndSwap(ctx, "state", "", []byte("one"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.CompareAndSwap(ctx, "state", "", []byte("two")); !errors.Is(err, ErrConflict) {
		t.Fatalf("got %v, want conflict", err)
	}
	if _, err := store.CompareAndSwap(ctx, "state", v1, []byte("two")); err != nil {
		t.Fatal(err)
	}
}

func TestLocalCompareAndSwapFencesAcrossInstances(t *testing.T) {
	directory := t.TempDir()
	first, err := NewLocal(directory)
	if err != nil {
		t.Fatal(err)
	}
	second, err := NewLocal(directory)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	version, err := first.CompareAndSwap(ctx, "state", "", []byte("initial"))
	if err != nil {
		t.Fatal(err)
	}
	start := make(chan struct{})
	errorsByWriter := make(chan error, 2)
	var ready sync.WaitGroup
	ready.Add(2)
	for index, store := range []*Local{first, second} {
		index, store := index, store
		go func() {
			ready.Done()
			<-start
			_, err := store.CompareAndSwap(ctx, "state", version, []byte(fmt.Sprintf("writer-%d", index)))
			errorsByWriter <- err
		}()
	}
	ready.Wait()
	close(start)
	var succeeded, conflicted int
	for range 2 {
		switch err := <-errorsByWriter; {
		case err == nil:
			succeeded++
		case errors.Is(err, ErrConflict):
			conflicted++
		default:
			t.Fatalf("compare-and-swap returned %v", err)
		}
	}
	if succeeded != 1 || conflicted != 1 {
		t.Fatalf("successful writes=%d conflicts=%d, want one of each", succeeded, conflicted)
	}
}

func TestLocalCompareAndSwapStopsWaitingWhenContextEnds(t *testing.T) {
	directory := t.TempDir()
	first, err := NewLocal(directory)
	if err != nil {
		t.Fatal(err)
	}
	second, err := NewLocal(directory)
	if err != nil {
		t.Fatal(err)
	}
	unlock, err := first.lockKey(context.Background(), "state")
	if err != nil {
		t.Fatal(err)
	}
	defer unlock()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if _, err := second.CompareAndSwap(ctx, "state", "", []byte("value")); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("compare-and-swap returned %v, want context deadline", err)
	}
}

func TestHTTPStoresUseBoundedRequestTimeouts(t *testing.T) {
	httpStore, err := NewHTTP("http://objects.example/bucket", nil)
	if err != nil {
		t.Fatal(err)
	}
	if httpStore.client.Timeout != defaultRequestTimeout {
		t.Fatalf("HTTP timeout=%v want=%v", httpStore.client.Timeout, defaultRequestTimeout)
	}
	s3Store, err := NewS3("http://objects.example/bucket", S3Credentials{AccessKey: "access", SecretKey: "secret", Region: "test-1"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if s3Store.client.Timeout != defaultRequestTimeout {
		t.Fatalf("S3 timeout=%v want=%v", s3Store.client.Timeout, defaultRequestTimeout)
	}
	finite := &http.Client{Timeout: 20 * time.Millisecond}
	finiteStore, err := NewHTTP("http://objects.example/bucket", finite)
	if err != nil {
		t.Fatal(err)
	}
	if finiteStore.client.Timeout != finite.Timeout {
		t.Fatalf("finite timeout=%v want=%v", finiteStore.client.Timeout, finite.Timeout)
	}
}

func TestHTTPStoreCancelsStalledRequest(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		<-r.Context().Done()
	}))
	defer server.Close()
	store, err := NewHTTP(server.URL, &http.Client{Timeout: 20 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	started := time.Now()
	_, err = store.Get(context.Background(), "stalled")
	if err == nil {
		t.Fatal("stalled request succeeded")
	}
	if elapsed := time.Since(started); elapsed > time.Second {
		t.Fatalf("stalled request took %v", elapsed)
	}
}

func TestS3SignsConditionalObjectWrite(t *testing.T) {
	var authorization, condition, path string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		authorization, condition, path = r.Header.Get("Authorization"), r.Header.Get("If-None-Match"), r.URL.Path
		w.Header().Set("ETag", `"v1"`)
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()
	store, err := NewS3(server.URL+"/bucket", S3Credentials{AccessKey: "access", SecretKey: "secret", Region: "test-1"}, server.Client())
	if err != nil {
		t.Fatal(err)
	}
	if err := store.PutImmutable(context.Background(), "segments/a", []byte("body")); err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(authorization, "AWS4-HMAC-SHA256 Credential=access/") {
		t.Fatalf("missing signature: %q", authorization)
	}
	if condition != "*" || path != "/bucket/segments/a" {
		t.Fatalf("condition=%q path=%q", condition, path)
	}
}

func TestCheckpointReplacementFencesPreviousWriter(t *testing.T) {
	store, err := NewLocal(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	first, _, err := Claim(ctx, store, "checkpoint", "first")
	if err != nil {
		t.Fatal(err)
	}
	if err := first.Commit(ctx, json.RawMessage(`{"accepted":1}`), nil); err != nil {
		t.Fatal(err)
	}
	second, recovered, err := Claim(ctx, store, "checkpoint", "second")
	if err != nil {
		t.Fatal(err)
	}
	if string(recovered.State) != `{"accepted":1}` {
		t.Fatalf("recovered %s", recovered.State)
	}
	if err := first.Commit(ctx, json.RawMessage(`{"accepted":2}`), nil); !errors.Is(err, ErrFenced) {
		t.Fatalf("got %v, want fenced", err)
	}
	if err := second.Commit(ctx, json.RawMessage(`{"accepted":3}`), []string{"segments/a"}); err != nil {
		t.Fatal(err)
	}
}

func TestLocalWriterLockRejectsConcurrentOwner(t *testing.T) {
	store, err := NewLocal(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	lock, err := store.LockWriter()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.LockWriter(); err == nil {
		t.Fatal("second writer acquired lock")
	}
	if err := lock.Close(); err != nil {
		t.Fatal(err)
	}
	if next, err := store.LockWriter(); err != nil {
		t.Fatal(err)
	} else {
		next.Close()
	}
}
