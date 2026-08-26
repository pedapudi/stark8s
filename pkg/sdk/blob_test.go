package sdk

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/rand"
	"net"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/pedapudi/stark8s/api/v1alpha1"
)

// payload returns n deterministic pseudorandom bytes and their digest. The
// content is incompressible enough that a truncated or reordered transfer
// changes the digest.
func payload(seed int64, n int) ([]byte, string) {
	b := make([]byte, n)
	rand.New(rand.NewSource(seed)).Read(b)
	sum := sha256.Sum256(b)
	return b, hex.EncodeToString(sum[:])
}

// digestOf streams r through sha256 without materialising it, the way a
// consumer of a large blob is meant to read one.
func digestOf(r io.Reader) (string, int64, error) {
	h := sha256.New()
	n, err := io.Copy(h, r)
	return hex.EncodeToString(h.Sum(nil)), n, err
}

// TestBlobRoundTrip moves a payload far larger than any record through a
// channel and checks it arrives byte for byte, and that the blob is deleted
// once the segment carrying its handle is released.
func TestBlobRoundTrip(t *testing.T) {
	h, stop := newHarness(t, []v1alpha1.Channel{
		{Name: "docs", From: "src", To: "sink"},
		{Name: "digests", From: "sink"},
	})
	defer stop()

	const size = 6 << 20 // 6 MiB: far past anything that belongs in a record
	body, want := payload(1, size)

	src := h.worker("src", "src-0", nil, []string{"docs"})
	h.run(src, Handlers{
		Source: func(ctx context.Context, w *Worker) error {
			// An ordinary record on the same channel, to prove the two mix.
			if err := w.Emit("docs", "note", "plain"); err != nil {
				return err
			}
			return w.EmitBlob("docs", "big", bytes.NewReader(body))
		},
	})

	var mu sync.Mutex
	var gotDigest string
	var gotSize int64
	plain := 0
	h.run(h.worker("sink", "sink-0", []string{"docs"}, []string{"digests"}), Handlers{
		OnRecord: func(ctx context.Context, w *Worker, r Record) error {
			handle, ok := r.Blob()
			if !ok {
				mu.Lock()
				plain++
				mu.Unlock()
				return nil
			}
			rc, n, err := w.OpenBlob(r)
			if err != nil {
				return err
			}
			defer rc.Close()
			d, copied, err := digestOf(rc)
			if err != nil {
				return err
			}
			mu.Lock()
			gotDigest, gotSize = d, copied
			mu.Unlock()
			if n != int64(size) || handle.Size != int64(size) {
				return fmt.Errorf("declared size %d/%d, want %d", n, handle.Size, size)
			}
			return w.Emit("digests", r.Key, d)
		},
	})

	h.waitComplete("src")
	if err := h.co.Seal("docs"); err != nil {
		t.Fatal(err)
	}
	h.waitComplete("sink")

	mu.Lock()
	defer mu.Unlock()
	if gotDigest != want || gotSize != int64(size) {
		t.Fatalf("blob arrived as %d bytes %s, want %d bytes %s", gotSize, gotDigest, size, want)
	}
	if plain != 1 {
		t.Fatalf("plain records seen: %d, want 1", plain)
	}
	recs, _, _ := h.co.Records("digests", "", 0, 0)
	if len(recs) != 1 || recs[0].Value != want {
		t.Fatalf("digests: %+v", recs)
	}

	// Lifetime: the sink has acknowledged the segment, so the coordinator
	// releases it and the producer deletes the blob with it.
	waitForBlobsGone(t, src)
}

// waitForBlobsGone drives the release path the heartbeat normally drives and
// waits for the worker's segment directory to hold no blob files.
func waitForBlobsGone(t *testing.T, w *Worker) {
	t.Helper()
	deadline := time.Now().Add(15 * time.Second)
	for {
		w.releaseSegments()
		left := blobFiles(t, w.store.dir)
		if len(left) == 0 {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("blobs still on disk after release: %v", left)
		}
		time.Sleep(50 * time.Millisecond)
	}
}

func blobFiles(t *testing.T, dir string) []string {
	t.Helper()
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	var out []string
	for _, e := range entries {
		if strings.HasSuffix(e.Name(), ".blob") {
			out = append(out, e.Name())
		}
	}
	return out
}

// TestBlobBroadcastReachesEveryReplica checks that a blob referenced by a
// broadcast record is fetched by every consumer replica, and that it survives
// until the last of them has acknowledged the carrying segment.
func TestBlobBroadcastReachesEveryReplica(t *testing.T) {
	h, stop := newHarness(t, []v1alpha1.Channel{
		{Name: "model", From: "trainer", To: "scorer",
			Partitioning: v1alpha1.Partitioning{Mode: v1alpha1.PartitionBroadcast}},
		{Name: "seen", From: "scorer"},
	})
	defer stop()

	const size = 2 << 20
	body, want := payload(2, size)
	replicas := []string{"scorer-0", "scorer-1", "scorer-2"}

	// Every replica must be registered before the blob is announced, or a
	// late replica would never be delivered the broadcast segment.
	var mu sync.Mutex
	got := map[string]string{}
	for _, inst := range replicas {
		inst := inst
		h.run(h.worker("scorer", inst, []string{"model"}, []string{"seen"}), Handlers{
			OnRecord: func(ctx context.Context, w *Worker, r Record) error {
				rc, _, err := w.OpenBlob(r)
				if err != nil {
					return err
				}
				defer rc.Close()
				d, n, err := digestOf(rc)
				if err != nil {
					return err
				}
				if n != int64(size) {
					return fmt.Errorf("%s read %d bytes, want %d", inst, n, size)
				}
				mu.Lock()
				got[inst] = d
				mu.Unlock()
				return w.Emit("seen", inst, d)
			},
		})
	}
	waitForPods(t, h, "scorer", len(replicas))

	src := h.worker("trainer", "trainer-0", nil, []string{"model"})
	h.run(src, Handlers{
		Source: func(ctx context.Context, w *Worker) error {
			return w.EmitBlob("model", "weights", bytes.NewReader(body))
		},
	})
	h.waitComplete("trainer")
	if err := h.co.Seal("model"); err != nil {
		t.Fatal(err)
	}
	h.waitComplete("scorer")

	mu.Lock()
	for _, inst := range replicas {
		if got[inst] != want {
			t.Errorf("%s got digest %q, want %q", inst, got[inst], want)
		}
	}
	mu.Unlock()
	if t.Failed() {
		t.FailNow()
	}
	// Released only once every replica acknowledged.
	waitForBlobsGone(t, src)
}

// waitForPods blocks until the coordinator knows of n live pods of the
// operation.
func waitForPods(t *testing.T, h *harness, op string, n int) {
	t.Helper()
	deadline := time.Now().Add(20 * time.Second)
	for time.Now().Before(deadline) {
		for _, om := range h.co.Metrics().Operations {
			if om.Name == op && int(om.LivePods) >= n {
				return
			}
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("operation %s did not reach %d live pods", op, n)
}

// TestOpenBlobOnPlainRecord reports a clear error instead of panicking or
// inventing a fetch.
func TestOpenBlobOnPlainRecord(t *testing.T) {
	w := &Worker{Instance: "w-0", SegmentDir: t.TempDir(), SegmentListen: "127.0.0.1:0"}
	w.init()
	for _, value := range []string{`42`, `"a string"`, `null`, `{"key":"value"}`, `{"stark8sBlob":""}`, ``} {
		rec := Record{Channel: "docs", Key: "k", Value: json.RawMessage(value)}
		if _, ok := rec.Blob(); ok {
			t.Fatalf("value %s reported as a blob handle", value)
		}
		rc, n, err := w.OpenBlob(rec)
		if !errors.Is(err, ErrNotBlob) {
			t.Fatalf("value %s: err %v, want ErrNotBlob", value, err)
		}
		if rc != nil || n != 0 {
			t.Fatalf("value %s: returned a reader", value)
		}
		if !strings.Contains(err.Error(), "docs") || !strings.Contains(err.Error(), "k") {
			t.Fatalf("error does not name the record: %v", err)
		}
	}
}

// TestOpenBlobUnreachableHolderFailsPromptly is the failure contract: a blob
// whose holder is gone produces an error naming the blob and the holder,
// quickly, rather than blocking the worker.
func TestOpenBlobUnreachableHolderFailsPromptly(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := ln.Addr().String()
	ln.Close() // nothing listens there any more, as after a pod is deleted

	w := &Worker{Instance: "w-0", SegmentDir: t.TempDir(), SegmentListen: "127.0.0.1:0"}
	w.init()
	handle, _ := json.Marshal(BlobHandle{Blob: "blob-42", Holder: addr, Size: 10})
	rec := Record{Channel: "docs", Key: "k", Value: handle}

	done := make(chan error, 1)
	start := time.Now()
	go func() {
		_, _, err := w.OpenBlob(rec)
		done <- err
	}()
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("unreachable holder returned no error")
		}
		if errors.Is(err, ErrNotBlob) {
			t.Fatalf("wrong error kind: %v", err)
		}
		if !strings.Contains(err.Error(), "blob-42") || !strings.Contains(err.Error(), addr) {
			t.Fatalf("error names neither blob nor holder: %v", err)
		}
		t.Logf("failed in %v: %v", time.Since(start), err)
	case <-time.After(30 * time.Second):
		t.Fatal("OpenBlob hung on an unreachable holder")
	}
}

// TestBlobMissingOnHolderFailsWithoutRetrying covers the holder that is alive
// but no longer has the blob, which is what a consumer reading past the
// lifetime of the carrying segment would see.
func TestBlobMissingOnHolderFailsWithoutRetrying(t *testing.T) {
	w := &Worker{Instance: "w-0", SegmentDir: t.TempDir(), SegmentListen: "127.0.0.1:0"}
	w.init()
	if err := w.serveSegments(); err != nil {
		t.Fatal(err)
	}
	handle, _ := json.Marshal(BlobHandle{Blob: "gone", Holder: w.addr, Size: 1})
	_, _, err := w.OpenBlob(Record{Channel: "docs", Key: "k", Value: handle})
	if err == nil || !strings.Contains(err.Error(), "gone") || !strings.Contains(err.Error(), "404") {
		t.Fatalf("missing blob: %v", err)
	}
}

// TestBlobLifetimeAcrossSegments checks the rule that a blob outlives every
// segment referring to it, not just the first one released.
func TestBlobLifetimeAcrossSegments(t *testing.T) {
	s, err := openStore(t.TempDir(), "pod-x")
	if err != nil {
		t.Fatal(err)
	}
	id, n, err := s.writeBlob("pod-x", strings.NewReader("payload"))
	if err != nil || n != 7 {
		t.Fatalf("writeBlob: %s %d %v", id, n, err)
	}
	exists := func() bool {
		_, err := os.Stat(s.blobPath(id))
		return err == nil
	}
	// Unbound: no segment references it yet, so nothing may collect it.
	s.releaseBlobs("seg-a")
	if !exists() {
		t.Fatal("unbound blob was deleted")
	}
	s.bind("seg-a", []string{id})
	s.bind("seg-b", []string{id})
	s.remove("seg-a")
	if !exists() {
		t.Fatal("blob deleted while seg-b still refers to it")
	}
	s.remove("seg-b")
	if exists() {
		t.Fatal("blob outlived its last segment")
	}
	// Releasing again is harmless.
	s.remove("seg-b")
}

// TestEmitBlobDroppedAtLoopBound checks that a blob whose record is discarded
// at the bound of an asynchronous loop is discarded with it, rather than
// staying on disk with nothing left to reference it.
func TestEmitBlobDroppedAtLoopBound(t *testing.T) {
	h, stop := newHarness(t, []v1alpha1.Channel{
		{Name: "turns", From: "agent", To: "agent",
			Feedback: &v1alpha1.Feedback{Mode: v1alpha1.FeedbackAsynchronous, MaxEpochs: 1}},
	})
	defer stop()
	w := h.worker("agent", "agent-0", []string{"turns"}, []string{"turns"})
	w.SetFeedback([]string{"turns"}, []string{"turns"})
	if err := w.serveSegments(); err != nil {
		t.Fatal(err)
	}
	if err := w.EmitBlob("turns", "k", strings.NewReader("payload")); err != nil {
		t.Fatal(err)
	}
	if left := blobFiles(t, w.store.dir); len(left) != 0 {
		t.Fatalf("blob of a dropped record left on disk: %v", left)
	}
	if err := w.Flush(); err != nil {
		t.Fatal(err)
	}
	for _, cm := range h.co.Metrics().Channels {
		if cm.Name == "turns" && cm.Overflowed != 1 {
			t.Fatalf("turns: %+v", cm)
		}
	}
}

// TestEmitBlobRejectsUndeclaredChannel leaves nothing on disk when the emit
// cannot succeed.
func TestEmitBlobRejectsUndeclaredChannel(t *testing.T) {
	h, stop := newHarness(t, []v1alpha1.Channel{{Name: "docs", From: "src", To: "sink"}})
	defer stop()
	w := h.worker("src", "src-0", nil, []string{"docs"})
	if err := w.serveSegments(); err != nil {
		t.Fatal(err)
	}
	if err := w.EmitBlob("nope", "k", strings.NewReader("x")); err == nil {
		t.Fatal("EmitBlob accepted an undeclared channel")
	}
	if left := blobFiles(t, w.store.dir); len(left) != 0 {
		t.Fatalf("failed EmitBlob left files behind: %v", left)
	}
}
