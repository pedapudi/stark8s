package sdk

// Pass-by-reference payloads.
//
// The transport is already reference-based: a producer writes its records to
// a file on its own disk, announces only the location to the coordinator, and
// consumers fetch pod to pod. What was not reference-based is the payload.
// Emit JSON-marshals every value into the segment, and a consumer decodes the
// whole segment into memory before the first record is handled, so a large
// value is encoded, written, read and decoded in full and buffered whole on
// both sides.
//
// EmitBlob streams a payload into its own file next to the segments and emits
// an ordinary, tiny record whose value is a BlobHandle. OpenBlob streams the
// bytes back from the holder. Neither side ever holds the payload in memory,
// and the payload is never JSON-encoded.
//
// LIFETIME RULE. A blob lives exactly as long as the segment carrying the
// record that references it.
//
//   - The blob file is written before the referencing record is buffered, so
//     the blob exists by the time any consumer can learn of it.
//   - At flush time the blob is bound to the segment the record landed in.
//   - The blob is deleted when the coordinator reports that segment released,
//     which it does only after every consumer that was delivered the segment
//     has acknowledged it. For a Broadcast channel that means every consumer
//     replica.
//
// The consequence for application code: OpenBlob is guaranteed to succeed for
// the duration of the OnRecord call that received the record, because the
// worker acknowledges the segment only after OnRecord returns. A reader kept
// past the return of OnRecord may find the blob already deleted. An HTTP read
// already in progress is not truncated by a release -- the holder keeps the
// file open for the length of the copy, and on a POSIX filesystem the data
// survives the unlink until that descriptor closes -- but a fetch started
// after the release fails with a 404.
//
// A blob referenced from more than one of the producer's own segments is
// deleted only when every one of those segments has been released. That
// bookkeeping is local to the producing pod; docs/design.md sets out what
// this feature deliberately is not.

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

// blobRoute is the path the segment server serves blobs on, alongside
// /segments/{id}.
const blobRoute = "/blobs/"

// ErrNotBlob is returned by OpenBlob for a record that carries no blob
// handle, so callers can tell "this is an ordinary record" from "the blob
// could not be fetched".
var ErrNotBlob = errors.New("record carries no blob handle")

// BlobHandle is the value of a record that refers to a blob. It is ordinary
// JSON: a worker built against an older SDK sees a small object rather than
// failing, and a channel carrying blob records is a channel like any other.
type BlobHandle struct {
	// Blob is the blob's id, unique within the holder.
	Blob string `json:"stark8sBlob"`
	// Holder is host:port of the segment server holding the bytes. It is the
	// producing pod, never the coordinator.
	Holder string `json:"holder"`
	// Size is the payload length in bytes.
	Size int64 `json:"size"`
}

// Blob returns the record's blob handle. ok is false for every record that is
// not a handle, which is every record produced by Emit.
func (r Record) Blob() (BlobHandle, bool) {
	var h BlobHandle
	if len(r.Value) == 0 || r.Value[0] != '{' {
		return BlobHandle{}, false
	}
	if err := json.Unmarshal(r.Value, &h); err != nil {
		return BlobHandle{}, false
	}
	if h.Blob == "" || h.Holder == "" {
		return BlobHandle{}, false
	}
	return h, true
}

// --- producing ------------------------------------------------------------

// EmitBlob streams r into this pod's segment directory as its own file and
// emits one record on the channel whose value is a BlobHandle pointing at it.
// The payload is never JSON-encoded and never held in memory.
//
// Apart from the payload the record is an ordinary one: it is partitioned by
// key, stamped with the epoch, buffered, and flushed inside a segment exactly
// like a record from Emit. The blob's lifetime is bound to that segment; see
// the lifetime rule at the top of this file.
//
// EmitBlob reads r to EOF before it returns. It does not close r.
func (w *Worker) EmitBlob(channel, key string, r io.Reader) error {
	w.init()
	// Validate the channel before writing anything, so a misspelled name does
	// not leave a file behind.
	if _, err := w.spec(channel); err != nil {
		return err
	}
	if w.store == nil {
		if err := w.serveSegments(); err != nil {
			return err
		}
	}
	id, size, err := w.store.writeBlob(w.Instance, r)
	if err != nil {
		return fmt.Errorf("writing blob for channel %s key %s: %w", channel, key, err)
	}
	h := BlobHandle{Blob: id, Holder: w.addr, Size: size}
	if err := w.emit(channel, key, h, id); err != nil {
		w.store.dropBlob(id)
		return err
	}
	return nil
}

// --- consuming ------------------------------------------------------------

// OpenBlob streams the payload of a blob record from the pod holding it. The
// caller must close the reader. The returned size is the payload length.
//
// It returns ErrNotBlob for a record that carries no handle, and an error
// naming the blob and its holder when the holder is unreachable or no longer
// has the blob. It never blocks indefinitely: connecting and receiving the
// response header are bounded, only the body transfer is not.
func (w *Worker) OpenBlob(rec Record) (io.ReadCloser, int64, error) {
	w.init()
	h, ok := rec.Blob()
	if !ok {
		return nil, 0, fmt.Errorf("channel %s key %s: %w", rec.Channel, rec.Key, ErrNotBlob)
	}
	return w.openBlob(h)
}

func (w *Worker) openBlob(h BlobHandle) (io.ReadCloser, int64, error) {
	url := "http://" + h.Holder + blobRoute + h.Blob
	var err error
	// Bounded retry, in the spirit of the segment fetch: a few quick attempts
	// absorb a restarting listener, but an unreachable holder must fail
	// promptly rather than hang, because the record cannot be processed
	// without its payload and the segment is redelivered if this pod dies.
	for attempt := 0; attempt < 3; attempt++ {
		if attempt > 0 {
			time.Sleep(time.Duration(attempt) * 200 * time.Millisecond)
		}
		var resp *http.Response
		resp, err = w.blobClient.Get(url)
		if err != nil {
			continue
		}
		if resp.StatusCode == http.StatusOK {
			return resp.Body, contentLength(resp, h.Size), nil
		}
		msg, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
		resp.Body.Close()
		err = fmt.Errorf("%s: %s", resp.Status, strings.TrimSpace(string(msg)))
		if resp.StatusCode == http.StatusNotFound {
			// The blob is gone, not slow to answer: the segment that owned it
			// was released, or the holder never had it. Retrying cannot help.
			break
		}
	}
	return nil, 0, fmt.Errorf("blob %s on holder %s: %w", h.Blob, h.Holder, err)
}

func contentLength(resp *http.Response, fallback int64) int64 {
	if resp.ContentLength >= 0 {
		return resp.ContentLength
	}
	return fallback
}

// newBlobClient returns the client used for blob transfers. It deliberately
// has no overall timeout: a blob may be large and take arbitrarily long to
// stream. Liveness is bounded where it matters instead, at connect and at the
// response header, so a dead holder fails in seconds.
func newBlobClient() *http.Client {
	return &http.Client{
		Transport: &http.Transport{
			DialContext:           (&net.Dialer{Timeout: 5 * time.Second}).DialContext,
			ResponseHeaderTimeout: 10 * time.Second,
			IdleConnTimeout:       30 * time.Second,
		},
	}
}

// --- store ----------------------------------------------------------------

// blobRef tracks which of this pod's segments still reference a blob.
type blobRef struct {
	// segments are the segment ids that carry a record referencing the blob.
	segments map[string]bool
	// bound is set once at least one segment has claimed the blob. An unbound
	// blob is never collected: its record has not been flushed yet, or it went
	// to a channel with no consuming operation and so has no segment to die
	// with.
	bound bool
}

func (s *segmentStore) blobPath(id string) string { return filepath.Join(s.dir, id+".blob") }

// writeBlob streams r into a new blob file and returns its id and size. The
// payload passes through an io.Copy buffer and is never held whole.
func (s *segmentStore) writeBlob(instance string, r io.Reader) (string, int64, error) {
	s.mu.Lock()
	s.seq++
	id := fmt.Sprintf("%s-%d-%d", instance, time.Now().UnixNano(), s.seq)
	s.mu.Unlock()

	tmp := s.blobPath(id) + ".tmp"
	f, err := os.OpenFile(tmp, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o644)
	if err != nil {
		return "", 0, err
	}
	n, err := io.Copy(f, r)
	if cerr := f.Close(); err == nil {
		err = cerr
	}
	if err == nil {
		err = os.Rename(tmp, s.blobPath(id))
	}
	if err != nil {
		_ = os.Remove(tmp)
		return "", 0, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.blobs == nil {
		s.blobs = map[string]*blobRef{}
	}
	s.blobs[id] = &blobRef{segments: map[string]bool{}}
	return id, n, nil
}

// bind makes the segment an owner of each blob: the blob outlives the segment
// and is deleted when its last owning segment is released.
func (s *segmentStore) bind(segment string, blobs []string) {
	if len(blobs) == 0 {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.segBlobs == nil {
		s.segBlobs = map[string][]string{}
	}
	for _, id := range blobs {
		b := s.blobs[id]
		if b == nil {
			continue
		}
		b.bound = true
		b.segments[segment] = true
	}
	s.segBlobs[segment] = append(s.segBlobs[segment], blobs...)
}

// releaseBlobs drops the segment's claim on its blobs and deletes those no
// other segment of this pod still references.
func (s *segmentStore) releaseBlobs(segment string) {
	s.mu.Lock()
	ids := s.segBlobs[segment]
	delete(s.segBlobs, segment)
	var dead []string
	for _, id := range ids {
		b := s.blobs[id]
		if b == nil {
			continue
		}
		delete(b.segments, segment)
		if b.bound && len(b.segments) == 0 {
			delete(s.blobs, id)
			dead = append(dead, id)
		}
	}
	s.mu.Unlock()
	for _, id := range dead {
		_ = os.Remove(s.blobPath(id))
	}
}

// dropBlob deletes a blob no record will ever reference: its record was
// dropped at a loop bound, or emitting it failed.
func (s *segmentStore) dropBlob(id string) {
	s.mu.Lock()
	delete(s.blobs, id)
	s.mu.Unlock()
	_ = os.Remove(s.blobPath(id))
}

// serveBlob streams a blob from disk. It never reads the payload into memory.
// A release during the copy is harmless: the descriptor is already open and
// the bytes survive the unlink until it closes.
func (s *segmentStore) serveBlob(rw http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if id == "" || strings.ContainsAny(id, "/\\") {
		http.Error(rw, "bad blob id", 400)
		return
	}
	f, err := os.Open(s.blobPath(id))
	if err != nil {
		http.Error(rw, "blob not found", 404)
		return
	}
	defer f.Close()
	rw.Header().Set("Content-Type", "application/octet-stream")
	if fi, err := f.Stat(); err == nil {
		rw.Header().Set("Content-Length", strconv.FormatInt(fi.Size(), 10))
	}
	_, _ = io.Copy(rw, f)
}
