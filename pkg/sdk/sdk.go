// Package sdk is the worker-side client for operations.
//
// A worker discovers its place in the graph from the environment variables
// the controller injects into every operation pod (the Env* constants in
// package coordinator): the coordinator URL, the operation and pod names,
// the pod IP, the slot count, the inbound and outbound channel names, which
// of them are feedback edges, and the local segment directory.
//
// Records never pass through the coordinator. Emit buffers records per
// (channel, partition, epoch); Flush writes each buffer to a local segment
// file, serves it over HTTP on the segment port, and announces it to the
// coordinator. Run polls the coordinator for segments pending on the
// partitions this pod owns, fetches them from the holder pods, calls
// OnRecord for each record, and acknowledges them. Records for a channel
// with no consuming operation are posted to the coordinator instead, which
// keeps them for external readers.
//
// A large payload can bypass the record encoding entirely: EmitBlob streams
// it into a file of its own beside the segments and emits a small record
// referring to it, and OpenBlob streams it back from the holder. See blob.go
// for the rule binding a blob's lifetime to its carrying segment.
//
// Run implements the completion and loop protocols so application code only
// handles records. A worker never exits on its own: once its work is done
// it keeps heartbeating and serving the segments it holds until ctx ends,
// because consumers may still need them and the pod runs under a
// Deployment.
package sdk

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/pedapudi/stark8s/api/graph"
	"github.com/pedapudi/stark8s/pkg/coordinator"
	"github.com/pedapudi/stark8s/pkg/storage"
)

// Record is a consumed record with its source channel.
type Record struct {
	Channel string
	Key     string
	Value   json.RawMessage
	Epoch   int32
}

// wireRecord is coordinator.Record with the value kept as raw JSON.
type wireRecord struct {
	Key   string          `json:"key"`
	Value json.RawMessage `json:"value"`
	Epoch int32           `json:"epoch"`
	// blob is the id of the blob this record refers to, for a record from
	// EmitBlob. It is producer-local bookkeeping: it stays out of the segment
	// (the handle in Value is what travels) and binds the blob's lifetime to
	// the segment this record is flushed into.
	blob string
}

// Handlers are the application callbacks. All are optional.
type Handlers struct {
	// Snapshot returns all application state needed to resume after the
	// current input segment. Restore installs bytes from the last committed
	// checkpoint before record processing starts. They must be supplied
	// together.
	Snapshot func(context.Context) ([]byte, error)
	Restore  func(context.Context, []byte) error
	// Source runs once for operations with no inbound channels. It should
	// emit everything and return; the worker then reports source-done and
	// idles.
	Source func(ctx context.Context, w *Worker) error
	// OnRecord is called for each consumed record.
	OnRecord func(ctx context.Context, w *Worker, r Record) error
	// OnEpochEnd is called when every inbound Synchronous feedback channel
	// is quiescent at the given epoch, before the barrier advances. Emit
	// next-epoch records here. It is called at most once per epoch.
	OnEpochEnd func(ctx context.Context, w *Worker, epoch int32) error
	// OnDrain is called once when every inbound channel that has a producing
	// operation is drained. Emit final results here; the worker then reports
	// done and idles.
	OnDrain func(ctx context.Context, w *Worker) error
	// Tick is called on an interval, for operations that have work to do on a
	// clock as well as on their input: polling a feed, a queue or an API,
	// where a channel supplies what to poll for.
	//
	// It runs on the goroutine that consumes records, between passes over the
	// inbound channels, so it never overlaps OnRecord and the two may share
	// state without a lock. That is deliberate. The emit buffers are a plain
	// map keyed by channel, partition and epoch, and Worker.epoch is a field
	// rewritten for every record consumed, so a Tick running anywhere else
	// would race with record processing on both.
	//
	// The cost of that choice is that a slow Tick delays record processing by
	// its own duration, and a slow batch of records delays Tick. The interval
	// is a floor on the period, never a guarantee.
	//
	// Tick needs Worker.TickInterval set. It is only reached by the loop that
	// polls inbound channels, so an operation with no inbound channels runs
	// Source instead and never ticks; Run rejects that combination rather
	// than letting the handler sit there uncalled.
	Tick func(ctx context.Context, w *Worker) error
}

// bufKey identifies one output buffer: a segment in the making.
type bufKey struct {
	channel   string
	partition int32
	epoch     int32
}

// Thresholds at which an output buffer becomes a segment. Whichever trips
// first wins.
const (
	// flushRecords caps a segment's record count. The per-record and
	// per-segment costs are what dominate for the small records most
	// operations emit, so this is the threshold that usually decides.
	flushRecords = 500
	// flushBytes caps a buffer's accumulated encoded size, so an operation
	// emitting large values cannot hold an unbounded amount of memory while
	// waiting for the 500th record. 4 MiB is small enough that a pod with a
	// few dozen live buffers stays well inside a normal container limit, and
	// large enough that it never fires for records under ~8 KiB, which
	// leaves the record threshold in charge of the common case.
	flushBytes                  = 4 << 20
	defaultMaxRecordBytes       = 8 << 20
	defaultMaxBufferedBytes     = 64 << 20
	defaultMaxFetchedBytes      = 64 << 20
	defaultMaxFetchedRecords    = 64 * flushRecords
	defaultMaxSegmentStoreBytes = 1 << 30
)

// Worker is one pod of an operation.
type Worker struct {
	Coordinator string
	Workload    string
	Operation   string
	Instance    string
	// Incarnation identifies this process lifetime within Instance. It is
	// generated during initialization when the caller leaves it empty.
	Incarnation string
	PodIP       string
	Slots       int32
	Inbound     []string
	Outbound    []string
	// SegmentDir is where this pod's segments are written.
	SegmentDir string
	// SegmentListen is the address the segment server listens on; default
	// ":8090". Tests may set "127.0.0.1:0" to run several workers in one
	// process.
	SegmentListen string
	// TickInterval is how often Handlers.Tick is called. Zero leaves the
	// operation driven entirely by its records.
	TickInterval         time.Duration
	MaxRecordBytes       int64
	MaxBufferedBytes     int64
	MaxFetchedBytes      int64
	MaxFetchedRecords    int
	MaxSegmentStoreBytes int64
	runContext           context.Context
	// DurableSegments stores immutable JSON segments outside the producer
	// pod. SegmentBaseURL is the HTTP location from which consumers read the
	// same keys. SegmentPrefix scopes keys for this workload.
	DurableSegments storage.Store
	SegmentBaseURL  string
	SegmentPrefix   string
	// CheckpointStore enables atomic state, input-position, and output-manifest
	// checkpoints. It is initially supported for fixed-ownership finite work.
	CheckpointStore      storage.Store
	CheckpointPrefix     string
	CollectiveRank       int
	CollectiveSize       int
	CollectiveAttempt    string
	CollectiveRendezvous string
	CollectiveCheckpoint string

	feedback    map[string]bool
	feedbackOut map[string]bool

	client *http.Client
	// blobClient transfers blob payloads; unlike client it has no overall
	// timeout, because a blob may be arbitrarily large.
	blobClient *http.Client
	specs      map[string]graph.Channel

	buffers map[bufKey][]wireRecord
	// combineIndex maps a buffered record key to its slot in buffers, for
	// channels that declare a Combine function.
	combineIndex  map[bufKey]map[string]int
	bufBytes      map[bufKey]int
	bufferedBytes int64
	order         []bufKey
	rr            map[string]uint64
	unannounced   map[string][]coordinator.SegmentAnnouncement
	overflowed    map[string]int64

	epoch    int32
	maxEpoch int32
	lastDone int32
	// syncLoop: an inbound feedback channel runs in Synchronous mode, so
	// the epoch comes from the coordinator rather than from records.
	syncLoop bool
	// task is the unit of work being processed; announced segments carry it.
	task coordinator.TaskID

	addr                  string
	store                 *segmentStore
	segmentSeq            uint64
	checkpoint            *checkpointSession
	checkpointAfterCommit func() error
	checkpointAfterAck    func() error
}

// FromEnv builds a Worker from the injected environment.
func FromEnv() (*Worker, error) {
	slots, _ := strconv.Atoi(os.Getenv(coordinator.EnvSlots))
	rank, _ := strconv.Atoi(os.Getenv(coordinator.EnvCollectiveRank))
	groupSize, _ := strconv.Atoi(os.Getenv(coordinator.EnvCollectiveSize))
	w := &Worker{
		Coordinator:    os.Getenv(coordinator.EnvCoordinator),
		Workload:       os.Getenv(coordinator.EnvWorkload),
		Operation:      os.Getenv(coordinator.EnvOperation),
		Instance:       os.Getenv(coordinator.EnvInstance),
		PodIP:          os.Getenv(coordinator.EnvPodIP),
		Slots:          int32(slots),
		Inbound:        split(os.Getenv(coordinator.EnvInbound)),
		Outbound:       split(os.Getenv(coordinator.EnvOutbound)),
		SegmentDir:     os.Getenv(coordinator.EnvSegmentDir),
		SegmentBaseURL: os.Getenv(coordinator.EnvObjectEndpoint),
		SegmentPrefix:  os.Getenv(coordinator.EnvObjectPrefix),
		CollectiveRank: rank, CollectiveSize: groupSize,
		CollectiveAttempt: os.Getenv(coordinator.EnvCollectiveAttempt), CollectiveRendezvous: os.Getenv(coordinator.EnvCollectiveRendezvous),
		CollectiveCheckpoint: os.Getenv(coordinator.EnvCollectiveCheckpoint),
	}
	if w.SegmentBaseURL != "" {
		store, err := storage.NewS3(w.SegmentBaseURL, storage.S3Credentials{AccessKey: os.Getenv(coordinator.EnvObjectAccessKey), SecretKey: os.Getenv(coordinator.EnvObjectSecretKey), SessionToken: os.Getenv(coordinator.EnvObjectSessionToken), Region: os.Getenv(coordinator.EnvObjectRegion)}, nil)
		if err != nil {
			return nil, err
		}
		w.DurableSegments = store
		if enabled, _ := strconv.ParseBool(os.Getenv(coordinator.EnvCheckpoint)); enabled {
			w.CheckpointStore = store
			w.CheckpointPrefix = w.SegmentPrefix
		}
	}
	w.init()
	var err error
	if w.MaxRecordBytes, err = byteLimitFromEnv(coordinator.EnvMaxRecordBytes, w.MaxRecordBytes); err != nil {
		return nil, err
	}
	if w.MaxBufferedBytes, err = byteLimitFromEnv(coordinator.EnvMaxBufferedBytes, w.MaxBufferedBytes); err != nil {
		return nil, err
	}
	if w.MaxFetchedBytes, err = byteLimitFromEnv(coordinator.EnvMaxFetchedBytes, w.MaxFetchedBytes); err != nil {
		return nil, err
	}
	if n, parseErr := byteLimitFromEnv(coordinator.EnvMaxFetchedRecords, int64(w.MaxFetchedRecords)); parseErr != nil {
		return nil, parseErr
	} else {
		w.MaxFetchedRecords = int(n)
	}
	if w.MaxSegmentStoreBytes, err = byteLimitFromEnv(coordinator.EnvMaxSegmentStoreBytes, w.MaxSegmentStoreBytes); err != nil {
		return nil, err
	}
	for _, f := range split(os.Getenv(coordinator.EnvFeedback)) {
		w.feedback[f] = true
	}
	for _, f := range split(os.Getenv(coordinator.EnvFeedbackOut)) {
		w.feedbackOut[f] = true
	}
	if v := strings.TrimSpace(os.Getenv(coordinator.EnvTickInterval)); v != "" {
		d, err := time.ParseDuration(v)
		if err != nil {
			return nil, fmt.Errorf("%s=%q: %w", coordinator.EnvTickInterval, v, err)
		}
		if d < 0 {
			return nil, fmt.Errorf("%s=%q: the tick interval must not be negative", coordinator.EnvTickInterval, v)
		}
		w.TickInterval = d
	}
	if w.Coordinator == "" || w.Operation == "" {
		return nil, fmt.Errorf("%s and %s must be set", coordinator.EnvCoordinator, coordinator.EnvOperation)
	}
	if w.Instance == "" {
		w.Instance, _ = os.Hostname()
	}
	return w, nil
}

// CollectiveIOEnabled reports whether this process may use graph input and
// output. Fixed collectives assign graph I/O to rank zero.
func (w *Worker) CollectiveIOEnabled() bool { return w.CollectiveSize == 0 || w.CollectiveRank == 0 }

func byteLimitFromEnv(name string, fallback int64) (int64, error) {
	v := strings.TrimSpace(os.Getenv(name))
	if v == "" {
		return fallback, nil
	}
	n, err := strconv.ParseInt(v, 10, 64)
	if err != nil || n <= 0 {
		return 0, fmt.Errorf("%s=%q: must be a positive integer", name, v)
	}
	return n, nil
}

// init fills defaults so a Worker built by hand (tests) works like one from
// FromEnv.
func (w *Worker) init() {
	if w.Incarnation == "" {
		var value [16]byte
		if _, err := rand.Read(value[:]); err != nil {
			panic(fmt.Sprintf("generate worker incarnation: %v", err))
		}
		w.Incarnation = hex.EncodeToString(value[:])
	}
	if w.MaxRecordBytes <= 0 {
		w.MaxRecordBytes = defaultMaxRecordBytes
	}
	if w.MaxBufferedBytes <= 0 {
		w.MaxBufferedBytes = defaultMaxBufferedBytes
	}
	if w.MaxFetchedBytes <= 0 {
		w.MaxFetchedBytes = defaultMaxFetchedBytes
	}
	if w.MaxFetchedRecords <= 0 {
		w.MaxFetchedRecords = defaultMaxFetchedRecords
	}
	if w.MaxSegmentStoreBytes <= 0 {
		w.MaxSegmentStoreBytes = defaultMaxSegmentStoreBytes
	}
	if w.feedback == nil {
		w.feedback = map[string]bool{}
	}
	if w.feedbackOut == nil {
		w.feedbackOut = map[string]bool{}
	}
	if w.client == nil {
		w.client = &http.Client{Timeout: 60 * time.Second}
	}
	if w.blobClient == nil {
		w.blobClient = newBlobClient()
	}
	if w.buffers == nil {
		w.buffers = map[bufKey][]wireRecord{}
		w.bufBytes = map[bufKey]int{}
		w.rr = map[string]uint64{}
		w.unannounced = map[string][]coordinator.SegmentAnnouncement{}
		w.overflowed = map[string]int64{}
		w.lastDone = -1
	}
	if w.Slots <= 0 {
		w.Slots = 1
	}
	if w.SegmentDir == "" {
		w.SegmentDir = "/var/lib/stark8s/segments"
	}
	if w.SegmentListen == "" {
		w.SegmentListen = fmt.Sprintf(":%d", coordinator.SegmentPort)
	}
}

// SetFeedback declares which inbound and outbound channels are feedback
// edges, for workers not built by FromEnv.
func (w *Worker) SetFeedback(inbound, outbound []string) {
	w.init()
	for _, f := range inbound {
		w.feedback[f] = true
	}
	for _, f := range outbound {
		w.feedbackOut[f] = true
	}
}

func split(s string) []string {
	if s == "" {
		return nil
	}
	var out []string
	for _, p := range strings.Split(s, ",") {
		if p = strings.TrimSpace(p); p != "" {
			out = append(out, p)
		}
	}
	return out
}

// Epoch is the current loop iteration (zero outside loops). In a
// Synchronous loop it is the superstep; in an Asynchronous loop it is the
// epoch of the record being processed.
func (w *Worker) Epoch() int32 { return w.epoch }

// MaxEpochs is the loop bound of the inbound feedback channel, or zero.
func (w *Worker) MaxEpochs() int32 { return w.maxEpoch }

// --- segment store --------------------------------------------------------

// segmentStore keeps this pod's segments as JSON files, one per segment, and
// the blobs those segments refer to as files of their own.
type segmentStore struct {
	dir       string
	mu        sync.Mutex
	seq       uint64
	maxBytes  int64
	usedBytes int64
	// blobs maps a blob id to the segments that still reference it, and
	// segBlobs is the reverse index. Both are producer-local; see blob.go for
	// the lifetime rule they implement.
	blobs    map[string]*blobRef
	segBlobs map[string][]string
}

var errStorageCapacity = errors.New("segment store capacity exceeded")

func openStore(dir, instance string) (*segmentStore, error) {
	if err := os.MkdirAll(dir, 0o755); err == nil {
		if f, err := os.CreateTemp(dir, ".probe"); err == nil {
			f.Close()
			os.Remove(f.Name())
			return &segmentStore{dir: dir}, nil
		}
	}
	fallback := filepath.Join(os.TempDir(), "stark8s-segments-"+instance)
	if err := os.MkdirAll(fallback, 0o755); err != nil {
		return nil, fmt.Errorf("segment directory %s is not writable and %s cannot be created: %w", dir, fallback, err)
	}
	log.Printf("segment directory %s is not writable; using %s", dir, fallback)
	return &segmentStore{dir: fallback}, nil
}

func (s *segmentStore) path(id string) string { return filepath.Join(s.dir, id+".json") }

// write stores the records as a new segment and returns its ID and size.
func (s *segmentStore) write(instance string, recs []wireRecord) (string, int64, error) {
	s.mu.Lock()
	s.seq++
	id := fmt.Sprintf("%s-%d-%d", instance, time.Now().UnixNano(), s.seq)
	s.mu.Unlock()
	body, err := json.Marshal(recs)
	if err != nil {
		return "", 0, err
	}
	s.mu.Lock()
	if s.maxBytes > 0 && s.usedBytes+int64(len(body)) > s.maxBytes {
		used := s.usedBytes
		s.mu.Unlock()
		return "", 0, fmt.Errorf("%w: writing %d bytes at %d of %d bytes", errStorageCapacity, len(body), used, s.maxBytes)
	}
	s.mu.Unlock()
	tmp := s.path(id) + ".tmp"
	if err := os.WriteFile(tmp, body, 0o644); err != nil {
		return "", 0, err
	}
	if err := os.Rename(tmp, s.path(id)); err != nil {
		_ = os.Remove(tmp)
		return "", 0, err
	}
	s.mu.Lock()
	s.usedBytes += int64(len(body))
	s.mu.Unlock()
	return id, int64(len(body)), nil
}

func (s *segmentStore) serve(rw http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if id == "" || strings.ContainsAny(id, "/\\") {
		http.Error(rw, "bad segment id", 400)
		return
	}
	f, err := os.Open(s.path(id))
	if err != nil {
		http.Error(rw, "segment not found", 404)
		return
	}
	defer f.Close()
	rw.Header().Set("Content-Type", "application/json")
	_, _ = io.Copy(rw, f)
}

// remove deletes a released segment and, with it, the blobs no other segment
// of this pod still refers to.
func (s *segmentStore) remove(id string) {
	path := s.path(id)
	if info, err := os.Stat(path); err == nil {
		if os.Remove(path) == nil {
			s.mu.Lock()
			s.usedBytes -= info.Size()
			s.mu.Unlock()
		}
	}
	s.releaseBlobs(id)
}

// --- emitting -------------------------------------------------------------

// Emit buffers a record for an outbound channel.
//
// The partition is computed the way the coordinator expects: hash of the
// key for Hash channels, round robin otherwise; Broadcast channels and
// channels with no consumer have a single partition. Records to a feedback
// channel are stamped with the epoch after the current one; when that
// reaches the bound of an Asynchronous loop the record is diverted to the
// loop's Overflow channel, or dropped and counted when none is declared.
// Full buffers are flushed as segments; Flush sends the rest.
func (w *Worker) Emit(channel, key string, value any) error {
	if !w.CollectiveIOEnabled() {
		return fmt.Errorf("collective rank %d cannot emit graph records; rank zero owns graph I/O", w.CollectiveRank)
	}
	return w.emit(channel, key, value, "")
}

// ValidateEmit checks an output without changing buffers or counters. It lets
// adapters validate a complete callback reply before emitting any of it.
func (w *Worker) ValidateEmit(channel, key string, value any) error {
	w.init()
	if _, err := w.spec(channel); err != nil {
		return err
	}
	body, err := json.Marshal(value)
	if err != nil {
		return err
	}
	if int64(len(key)+len(body)) > w.MaxRecordBytes {
		return fmt.Errorf("channel %q record is %d bytes; limit is %d: use EmitBlob for large payloads", channel, len(key)+len(body), w.MaxRecordBytes)
	}
	if w.DurableSegments != nil {
		var handle BlobHandle
		if len(body) > 0 && body[0] == '{' && json.Unmarshal(body, &handle) == nil && handle.Blob != "" {
			return errors.New("blob handles cannot be written to durable segments because their payload is pod-local")
		}
	}
	return nil
}

// emit is Emit with the id of the blob the value refers to, empty for an
// ordinary record. The id travels with the buffered record so that flushing
// can bind the blob to the segment it lands in.
func (w *Worker) emit(channel, key string, value any, blob string) error {
	w.init()
	if err := w.ValidateEmit(channel, key, value); err != nil {
		return err
	}
	spec, err := w.spec(channel)
	if err != nil {
		return err
	}
	b, err := json.Marshal(value)
	if err != nil {
		return err
	}
	epoch := w.epoch
	if w.feedbackOut[channel] {
		epoch = w.epoch + 1
		if spec.Feedback != nil && spec.Feedback.Mode == graph.FeedbackAsynchronous && epoch >= spec.Feedback.MaxEpochs {
			if spec.Feedback.Overflow == "" {
				w.overflowed[channel]++
				if blob != "" && w.store != nil {
					// The record is dropped at the loop bound, so nothing will
					// ever reference the blob.
					w.store.dropBlob(blob)
				}
				return nil
			}
			return w.buffer(spec.Feedback.Overflow, key, b, epoch, blob)
		}
	}
	return w.buffer(channel, key, b, epoch, blob)
}

func (w *Worker) buffer(channel, key string, value json.RawMessage, epoch int32, blob string) error {
	spec, err := w.spec(channel)
	if err != nil {
		return err
	}
	recordBytes := len(key) + len(value)
	if int64(recordBytes) > w.MaxRecordBytes {
		return fmt.Errorf("channel %q record is %d bytes; limit is %d: use EmitBlob for large payloads", channel, recordBytes, w.MaxRecordBytes)
	}
	if err := w.reserveBufferBytes(int64(recordBytes)); err != nil {
		return err
	}
	var p int32
	switch {
	case spec.To == "" || spec.Partitioning.Mode == graph.PartitionBroadcast:
		p = 0
	case spec.Partitioning.Mode == graph.PartitionHash:
		p = int32(coordinator.HashPartition(key, int(spec.Partitioning.Partitions)))
	default:
		p = int32(w.rr[channel] % uint64(spec.Partitioning.Partitions))
		w.rr[channel]++
	}
	k := bufKey{channel, p, epoch}
	if _, ok := w.buffers[k]; !ok {
		w.order = append(w.order, k)
	}
	if spec.Combine != "" {
		if blob != "" {
			return fmt.Errorf("channel %q combines numeric records and cannot carry blobs", channel)
		}
		if err := w.combine(k, spec.Combine, key, value, epoch); err != nil {
			return err
		}
		// The buffer now holds one record per distinct key, so the flush
		// threshold bounds distinct keys rather than facts. That is the point:
		// a stage emitting many records per key ships far fewer.
		if len(w.buffers[k]) >= flushRecords || w.bufBytes[k] >= flushBytes {
			return w.flushBuffer(k)
		}
		return nil
	}
	w.buffers[k] = append(w.buffers[k], wireRecord{Key: key, Value: value, Epoch: epoch, blob: blob})
	w.bufBytes[k] += len(key) + len(value)
	w.bufferedBytes += int64(recordBytes)
	if len(w.buffers[k]) >= flushRecords || w.bufBytes[k] >= flushBytes {
		return w.flushBuffer(k)
	}
	return nil
}

func (w *Worker) reserveBufferBytes(n int64) error {
	for w.bufferedBytes+n > w.MaxBufferedBytes {
		var oldest bufKey
		found := false
		for len(w.order) > 0 {
			oldest, w.order = w.order[0], w.order[1:]
			if len(w.buffers[oldest]) > 0 {
				found = true
				break
			}
		}
		if !found {
			return fmt.Errorf("record needs %d buffered bytes; aggregate limit is %d", n, w.MaxBufferedBytes)
		}
		if err := w.flushBuffer(oldest); err != nil {
			return fmt.Errorf("freeing buffered output at %d of %d bytes: %w", w.bufferedBytes, w.MaxBufferedBytes, err)
		}
	}
	return nil
}

// combine folds a record into the buffered record for the same key, applying
// the channel's function. The buffered slice stays the source of truth so that
// flushBuffer, the byte accounting and the ordering guarantees all keep
// working unchanged; combineIndex only says where in it each key lives.
func (w *Worker) combine(k bufKey, mode graph.CombineMode, key string, value json.RawMessage, epoch int32) error {
	if w.combineIndex == nil {
		w.combineIndex = map[bufKey]map[string]int{}
	}
	idx, ok := w.combineIndex[k]
	if !ok {
		idx = map[string]int{}
		w.combineIndex[k] = idx
	}

	if mode == graph.CombineCount {
		// Count ignores the emitted value, so it is the one mode that accepts
		// a null and the one that cannot fail on a non-numeric record.
		if at, seen := idx[key]; seen {
			var n float64
			if err := json.Unmarshal(w.buffers[k][at].Value, &n); err != nil {
				return fmt.Errorf("combine Count on channel %q key %q: %w", k.channel, key, err)
			}
			return w.setCombined(k, at, n+1)
		}
		idx[key] = len(w.buffers[k])
		b, _ := json.Marshal(1)
		w.buffers[k] = append(w.buffers[k], wireRecord{Key: key, Value: b, Epoch: epoch})
		w.bufBytes[k] += len(key) + len(b)
		w.bufferedBytes += int64(len(key) + len(b))
		return nil
	}

	var incoming float64
	if err := json.Unmarshal(value, &incoming); err != nil {
		return fmt.Errorf("combine %s on channel %q key %q: value must be a number: %w", mode, k.channel, key, err)
	}
	at, seen := idx[key]
	if !seen {
		idx[key] = len(w.buffers[k])
		w.buffers[k] = append(w.buffers[k], wireRecord{Key: key, Value: value, Epoch: epoch})
		w.bufBytes[k] += len(key) + len(value)
		w.bufferedBytes += int64(len(key) + len(value))
		return nil
	}
	var held float64
	if err := json.Unmarshal(w.buffers[k][at].Value, &held); err != nil {
		return fmt.Errorf("combine %s on channel %q key %q: %w", mode, k.channel, key, err)
	}
	switch mode {
	case graph.CombineSum:
		held += incoming
	case graph.CombineMin:
		if incoming < held {
			held = incoming
		}
	case graph.CombineMax:
		if incoming > held {
			held = incoming
		}
	default:
		return fmt.Errorf("channel %q: unknown combine mode %q", k.channel, mode)
	}
	return w.setCombined(k, at, held)
}

// setCombined rewrites a buffered record in place, keeping the byte accounting
// consistent with the new encoding.
func (w *Worker) setCombined(k bufKey, at int, v float64) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	w.bufBytes[k] += len(b) - len(w.buffers[k][at].Value)
	w.bufferedBytes += int64(len(b) - len(w.buffers[k][at].Value))
	w.buffers[k][at].Value = b
	return nil
}

// spec returns the declared channel, loading the topology on first use.
func (w *Worker) spec(channel string) (graph.Channel, error) {
	if w.specs == nil {
		if err := w.loadTopology(); err != nil {
			return graph.Channel{}, err
		}
	}
	s, ok := w.specs[channel]
	if !ok {
		return graph.Channel{}, fmt.Errorf("channel %q is not declared in the topology", channel)
	}
	return s, nil
}

func (w *Worker) loadTopology() error {
	var specs []graph.Channel
	if err := w.do("GET", coordinator.PathTopology, nil, &specs); err != nil {
		return err
	}
	m := map[string]graph.Channel{}
	for _, s := range specs {
		if s.Partitioning.Partitions <= 0 {
			s.Partitioning.Partitions = 1
		}
		m[s.Name] = s
	}
	w.specs = m
	return nil
}

// flushBuffer turns one buffer into a local segment (or posts it to the
// coordinator for a channel with no consumer) and queues its announcement.
func (w *Worker) flushBuffer(k bufKey) error {
	recs := w.buffers[k]
	if len(recs) == 0 {
		delete(w.buffers, k)
		delete(w.combineIndex, k)
		delete(w.bufBytes, k)
		return nil
	}
	spec, err := w.spec(k.channel)
	if err != nil {
		return err
	}
	var blobs []string
	for _, r := range recs {
		if r.blob != "" {
			blobs = append(blobs, r.blob)
		}
	}
	if spec.To == "" {
		if w.checkpoint != nil {
			return errors.New("checkpointed output to an external channel requires a transactional or idempotent sink")
		}
		body, _ := json.Marshal(recs)
		if err := w.do("POST", coordinator.PathChannels+"/"+k.channel+coordinator.SuffixRecords, body, nil); err != nil {
			return err
		}
		// A channel with no consuming operation has no segment and so no
		// release signal: blobs referenced from it stay bound to nothing and
		// live until the pod does, which is what an external reader of a
		// retained record log needs.
		delete(w.buffers, k)
		w.bufferedBytes -= int64(w.bufBytes[k])
		delete(w.combineIndex, k)
		delete(w.bufBytes, k)
		return nil
	}
	if w.DurableSegments == nil && w.store == nil {
		if err := w.serveSegments(); err != nil {
			return err
		}
	}
	var id string
	var size int64
	var holder string
	if w.DurableSegments != nil {
		w.segmentSeq++
		id = fmt.Sprintf("%s-%d-%d", w.Instance, time.Now().UnixNano(), w.segmentSeq)
		body, marshalErr := json.Marshal(recs)
		if marshalErr != nil {
			return marshalErr
		}
		if err := storage.PutImmutable(context.Background(), w.DurableSegments, w.segmentKey(id), body); err != nil {
			return err
		}
		size, holder = int64(len(body)), strings.TrimRight(w.SegmentBaseURL, "/")
		if holder == "" {
			return errors.New("SegmentBaseURL is required with DurableSegments")
		}
	} else {
		var err error
		id, size, err = w.store.write(w.Instance, recs)
		if err != nil {
			return err
		}
		holder = w.addr
	}
	// Bind before announcing: from the moment a consumer can learn of the
	// segment, releasing it must also release its blobs.
	w.store.bind(id, blobs)
	delete(w.buffers, k)
	w.bufferedBytes -= int64(w.bufBytes[k])
	delete(w.combineIndex, k)
	delete(w.bufBytes, k)
	w.unannounced[k.channel] = append(w.unannounced[k.channel], coordinator.SegmentAnnouncement{
		ID: id, Channel: k.channel, Partition: k.partition, Epoch: k.epoch,
		Records: int64(len(recs)), Bytes: size, Holder: holder, Producer: w.Instance, Task: w.task,
	})
	return nil
}

// Flush writes every buffer as a segment and announces all segments not yet
// announced, together with the count of records dropped at a loop bound.
// It is safe to retry: a failed announcement stays queued.
func (w *Worker) Flush() error {
	if err := w.materialize(); err != nil {
		return err
	}
	if w.checkpoint != nil {
		return nil
	}
	return w.publish()
}

func (w *Worker) materialize() error {
	w.init()
	var keys []bufKey
	for _, k := range w.order {
		if _, ok := w.buffers[k]; ok {
			keys = append(keys, k)
		}
	}
	w.order = nil
	for _, k := range keys {
		if err := w.flushBuffer(k); err != nil {
			w.order = append(w.order, k)
			return err
		}
	}
	for ch, n := range w.overflowed {
		if n > 0 {
			w.unannounced[ch] = append(w.unannounced[ch], coordinator.SegmentAnnouncement{Channel: ch, Overflowed: n})
			w.overflowed[ch] = 0
		}
	}
	return nil
}

func (w *Worker) publish() error {
	for ch, anns := range w.unannounced {
		body, _ := json.Marshal(anns)
		if err := w.do("POST", fmt.Sprintf("%s/%s%s?pod=%s", coordinator.PathChannels, ch, coordinator.SuffixSegments, w.Instance), body, nil); err != nil {
			return err
		}
		delete(w.unannounced, ch)
	}
	return nil
}

// --- coordinator client ---------------------------------------------------

func (w *Worker) do(method, path string, body []byte, out any) error {
	var rd io.Reader
	if body != nil {
		rd = bytes.NewReader(body)
	}
	req, err := http.NewRequest(method, w.Coordinator+path, rd)
	if err != nil {
		return err
	}
	req.Header.Set(coordinator.OperationHeader, w.Operation)
	req.Header.Set(coordinator.IncarnationHeader, w.Incarnation)
	req.Header.Set("Content-Type", "application/json")
	resp, err := w.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusConflict {
		return errSealed
	}
	if resp.StatusCode >= 300 {
		msg, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("%s %s: %s: %s", method, path, resp.Status, strings.TrimSpace(string(msg)))
	}
	if out != nil {
		return json.NewDecoder(resp.Body).Decode(out)
	}
	return nil
}

func (w *Worker) registration() coordinator.PodRegistration {
	return coordinator.PodRegistration{Operation: w.Operation, Pod: w.Instance, Incarnation: w.Incarnation, Addr: w.addr, Slots: w.Slots}
}

func (w *Worker) register() error {
	body, _ := json.Marshal(w.registration())
	return w.do("POST", coordinator.PathRegister, body, nil)
}

// reportDone tells the coordinator this pod will emit nothing more.
func (w *Worker) reportDone() error {
	body, _ := json.Marshal(w.registration())
	return w.do("POST", coordinator.PathSourceDone, body, nil)
}

func (w *Worker) consume(ch string, max int) (*coordinator.ConsumeResponse, error) {
	var resp coordinator.ConsumeResponse
	err := w.do("GET", fmt.Sprintf("%s/%s%s?pod=%s&max=%d", coordinator.PathChannels, ch, coordinator.SuffixConsume, w.Instance, max), nil, &resp)
	return &resp, err
}

func (w *Worker) ack(ch string, acks []coordinator.SegmentAck) error {
	body, _ := json.Marshal(acks)
	return w.do("POST", fmt.Sprintf("%s/%s%s?pod=%s", coordinator.PathChannels, ch, coordinator.SuffixAck, w.Instance), body, nil)
}

func (w *Worker) nack(ch string, deliveries []coordinator.SegmentAck) error {
	body, _ := json.Marshal(deliveries)
	return w.do("POST", fmt.Sprintf("%s/%s%s?pod=%s", coordinator.PathChannels, ch, coordinator.SuffixNack, w.Instance), body, nil)
}

// fetch reads a segment from its holder.
func (w *Worker) fetch(ctx context.Context, ref coordinator.SegmentRef) ([]wireRecord, error) {
	if ref.Bytes > w.MaxFetchedBytes {
		return nil, fmt.Errorf("fetch segment %s: declared size %d exceeds fetched-byte limit %d", ref.ID, ref.Bytes, w.MaxFetchedBytes)
	}
	if ref.Records > int64(w.MaxFetchedRecords) {
		return nil, fmt.Errorf("fetch segment %s: declared records %d exceeds fetched-record limit %d", ref.ID, ref.Records, w.MaxFetchedRecords)
	}
	if w.DurableSegments != nil {
		return w.fetchDurable(ctx, ref)
	}
	holder := strings.TrimRight(ref.Holder, "/")
	var segmentURL string
	if strings.HasPrefix(holder, "http://") || strings.HasPrefix(holder, "https://") {
		segmentURL = holder + "/" + w.segmentKey(ref.ID)
	} else {
		segmentURL = "http://" + holder + "/segments/" + ref.ID
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, segmentURL, nil)
	if err != nil {
		return nil, err
	}
	resp, err := w.client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		return nil, fmt.Errorf("fetch %s from %s: %s", ref.ID, ref.Holder, resp.Status)
	}
	limited := &io.LimitedReader{R: resp.Body, N: w.MaxFetchedBytes + 1}
	recs, err := decodeSegment(limited, ref.ID, w.MaxFetchedRecords)
	if err != nil {
		if limited.N <= 0 {
			return nil, fmt.Errorf("fetch segment %s: encoded body exceeds fetched-byte limit %d", ref.ID, w.MaxFetchedBytes)
		}
		return nil, err
	}
	if limited.N <= 0 {
		return nil, fmt.Errorf("fetch segment %s: encoded body exceeds fetched-byte limit %d", ref.ID, w.MaxFetchedBytes)
	}
	return recs, nil
}

func (w *Worker) fetchDurable(ctx context.Context, ref coordinator.SegmentRef) ([]wireRecord, error) {
	getter, ok := w.DurableSegments.(storage.BoundedGetter)
	if !ok {
		return nil, fmt.Errorf("fetch segment %s: durable store does not support bounded reads", ref.ID)
	}
	object, err := getter.GetBounded(ctx, w.segmentKey(ref.ID), w.MaxFetchedBytes)
	if err != nil {
		return nil, fmt.Errorf("fetch %s from durable storage: %w", ref.ID, err)
	}
	return decodeSegment(bytes.NewReader(object.Bytes), ref.ID, w.MaxFetchedRecords)
}

func decodeSegment(r io.Reader, id string, maxRecords int) ([]wireRecord, error) {
	decoder := json.NewDecoder(r)
	token, err := decoder.Token()
	if err != nil {
		return nil, err
	}
	if delimiter, ok := token.(json.Delim); !ok || delimiter != '[' {
		return nil, fmt.Errorf("fetch segment %s: body must be a JSON array", id)
	}
	records := make([]wireRecord, 0, min(maxRecords, 256))
	for decoder.More() {
		if len(records) >= maxRecords {
			return nil, fmt.Errorf("fetch segment %s: decoded records exceed limit %d", id, maxRecords)
		}
		var record wireRecord
		if err := decoder.Decode(&record); err != nil {
			return nil, err
		}
		records = append(records, record)
	}
	if _, err := decoder.Token(); err != nil {
		return nil, err
	}
	var extra any
	if err := decoder.Decode(&extra); err != io.EOF {
		if err == nil {
			return nil, fmt.Errorf("fetch segment %s: response contains more than one JSON value", id)
		}
		return nil, fmt.Errorf("fetch segment %s: trailing response data: %w", id, err)
	}
	return records, nil
}

// releaseSegments deletes the local segments the coordinator has released.
func (w *Worker) releaseSegments() {
	if w.store == nil && w.DurableSegments == nil {
		return
	}
	var ids []string
	if err := w.do("GET", coordinator.PathReleased+"?pod="+w.Instance, nil, &ids); err != nil {
		log.Printf("released segments: %v", err)
		return
	}
	for _, id := range ids {
		if w.DurableSegments != nil {
			if err := w.DurableSegments.Delete(context.Background(), w.segmentKey(id)); err != nil {
				log.Printf("delete released segment %s: %v", id, err)
			}
		} else {
			w.store.remove(id)
		}
	}
}

func (w *Worker) segmentKey(id string) string {
	prefix := strings.Trim(w.SegmentPrefix, "/")
	if prefix == "" {
		return "segments/" + id
	}
	return prefix + "/segments/" + id
}

// serveSegments opens the local store and starts the segment server.
func (w *Worker) serveSegments() error {
	if w.store != nil {
		return nil
	}
	store, err := openStore(w.SegmentDir, w.Instance)
	if err != nil {
		return err
	}
	store.maxBytes = w.MaxSegmentStoreBytes
	ln, err := net.Listen("tcp", w.SegmentListen)
	if err != nil {
		return fmt.Errorf("segment server: %w", err)
	}
	port := ln.Addr().(*net.TCPAddr).Port
	host := w.PodIP
	if host == "" {
		host = ln.Addr().(*net.TCPAddr).IP.String()
		if host == "::" || host == "0.0.0.0" {
			host, _ = os.Hostname()
		}
	}
	w.addr = net.JoinHostPort(host, strconv.Itoa(port))
	mux := http.NewServeMux()
	mux.HandleFunc("GET /segments/{id}", store.serve)
	mux.HandleFunc("GET "+blobRoute+"{id}", store.serveBlob)
	go func() { _ = http.Serve(ln, mux) }()
	w.store = store
	return nil
}

// --- running --------------------------------------------------------------

// Consume polling. When a poll finds nothing the worker sleeps, doubling the
// sleep from pollFloor up to a ceiling.
const (
	pollFloor = 100 * time.Millisecond
	// pollCeiling applies outside a Synchronous loop, where a pod that finds
	// nothing is idle because its producers are slow and there is nothing
	// for it to react to promptly.
	pollCeiling = 2 * time.Second
	// barrierPollCeiling applies while an inbound Synchronous feedback
	// channel is driving the epoch. There every pod is idle by definition
	// while it waits at the barrier, so pollCeiling saturates and the pod
	// can then sleep for up to another 2s after the barrier releases -- dead
	// time comparable to a whole superstep, on every superstep.
	//
	// 250ms bounds that wakeup delay while keeping the request rate modest:
	// one consume per inbound channel per pod per 250ms, so 4 requests per
	// second per channel from each pod. A 200-pod operation with two inbound
	// channels asks the coordinator for at most 1600 consumes/s, each a
	// short critical section under the coordinator's single mutex. Polling
	// harder would trade a shared bottleneck for latency the barrier does
	// not actually have.
	barrierPollCeiling = 250 * time.Millisecond
)

// Fetching a segment from its holder. The coordinator has no way to hand a
// delivered segment back to the pending queue on request, so a consumer that
// cannot fetch one has only two options: retry, or stop. It retries this
// many times, doubling the wait from fetchRetryFloor to fetchRetryCeiling --
// about 5s in total, enough to ride out a holder restart or a network blip
// -- and then fails.
const (
	fetchAttempts     = 6
	fetchRetryFloor   = 200 * time.Millisecond
	fetchRetryCeiling = 2 * time.Second
	fetchRetryBudget  = 6 * time.Second
)

// Run executes the worker: it registers, starts heartbeats and the segment
// server, loads the topology, and then drives the source or the
// poll/process/ack loop. When the work is done it idles until ctx ends,
// returning nil; handler errors abort the worker.
func (w *Worker) Run(ctx context.Context, h Handlers) error {
	w.init()
	w.runContext = ctx
	if w.CheckpointStore != nil && h.Tick != nil {
		return errors.New("checkpointed operations cannot use Tick because tick state and output are outside an input checkpoint boundary")
	}
	if !w.CollectiveIOEnabled() {
		if h.Source == nil {
			return fmt.Errorf("collective rank %d requires a Source handler", w.CollectiveRank)
		}
		return h.Source(ctx, w)
	}
	// A handler combination that can never fire is a programming mistake, so
	// it is caught here, before the worker touches the network. Tick is
	// driven by the loop that polls inbound channels, and an operation with
	// none of those runs Source instead and reaches that loop never.
	if h.Tick != nil && len(w.Inbound) == 0 {
		return fmt.Errorf("operation %s has a Tick handler and no inbound channels: Tick is driven by the loop that polls inbound channels, so give the operation a channel to consume or move the work into Source", w.Operation)
	}
	if err := w.serveSegments(); err != nil {
		return err
	}
	if err := w.retry(ctx, w.loadTopology); err != nil {
		return err
	}
	if err := w.retry(ctx, w.register); err != nil {
		return err
	}
	if err := w.startCheckpoint(ctx, h); err != nil {
		return err
	}
	if w.checkpoint != nil && w.checkpoint.current.Complete {
		if err := w.retry(ctx, w.reportDone); err != nil {
			return err
		}
		return w.idle(ctx)
	}
	if w.checkpoint != nil && w.checkpoint.current.EpochCallbackComplete {
		for ch := range w.feedback {
			ch := ch
			if err := w.retry(ctx, func() error {
				return w.do("POST", fmt.Sprintf("%s/%s%s?pod=%s&epoch=%d", coordinator.PathChannels, ch, coordinator.SuffixEpochDone, w.Instance, w.epoch), nil, nil)
			}); err != nil {
				return err
			}
		}
		w.lastDone = w.epoch
	}
	go w.heartbeat(ctx)

	if len(w.Inbound) == 0 {
		if h.Source == nil {
			return fmt.Errorf("operation %s has no inbound channels and no Source handler", w.Operation)
		}
		w.task = coordinator.TaskID{Channel: w.Instance}
		if err := h.Source(ctx, w); err != nil {
			return err
		}
		var flushErr error
		if w.checkpoint != nil {
			flushErr = w.commitCheckpoint(ctx, h, "", nil, true, false)
		} else {
			flushErr = w.retry(ctx, w.Flush)
		}
		if flushErr != nil && !errors.Is(flushErr, errSealed) {
			return flushErr
		} else if flushErr != nil {
			log.Printf("%s: output channel already sealed; source output was produced by an earlier pod", w.Instance)
			w.buffers = map[bufKey][]wireRecord{}
			w.bufBytes = map[bufKey]int{}
		}
		if err := w.retry(ctx, w.reportDone); err != nil {
			return err
		}
		log.Printf("%s: source complete", w.Instance)
		if w.CollectiveSize > 0 {
			return nil
		}
		return w.idle(ctx)
	}

	backoff := pollFloor
	// observedEpoch is the last epoch this pod saw, so that a barrier
	// release puts the poll rate back at the floor even when the pod then
	// finds no work of its own in the new superstep.
	observedEpoch := int32(-1)

	ticking := h.Tick != nil && w.TickInterval > 0
	if h.Tick != nil && !ticking {
		log.Printf("%s: a Tick handler is set but the tick interval is zero, so Tick is never called", w.Instance)
	}
	nextTick := time.Now().Add(w.TickInterval)

	for ctx.Err() == nil {
		progressed := false
		allDrained := true
		allQuiet := true
		for _, ch := range w.Inbound {
			resp, err := w.consume(ch, 50)
			if err != nil {
				log.Printf("consume %s: %v", ch, err)
				time.Sleep(time.Second)
				allDrained, allQuiet = false, false
				continue
			}
			if w.feedback[ch] {
				w.maxEpoch = resp.MaxEpochs
				if resp.Mode != graph.FeedbackAsynchronous {
					w.syncLoop = true
					w.epoch = resp.Epoch
				}
			}
			if resp.FiniteEpochs {
				w.syncLoop = true
				w.epoch = resp.Epoch
				w.maxEpoch = resp.MaxEpochs
			}
			for _, work := range resp.Work {
				for _, seg := range work.Segments {
					if w.checkpoint != nil && w.checkpoint.covers(ch, seg) {
						acks := []coordinator.SegmentAck{{ID: seg.ID, Holder: seg.Holder, Pod: w.Instance}}
						if err := w.retry(ctx, func() error { return w.ack(ch, acks) }); err != nil {
							return err
						}
						progressed = true
						continue
					}
					recs, err := w.fetchRetry(ctx, seg)
					if err != nil {
						if ctx.Err() != nil {
							return ctx.Err()
						}
						failure := fmt.Sprintf("fetch segment %s from %s: %v", seg.ID, seg.Holder, err)
						delivery := []coordinator.SegmentAck{{ID: seg.ID, Holder: seg.Holder, Pod: w.Instance, Failure: failure, RetryAfterMillis: 500}}
						if nackErr := w.nack(ch, delivery); nackErr != nil {
							return fmt.Errorf("%s; return delivery: %w", failure, nackErr)
						}
						return fmt.Errorf("%s; delivery returned", failure)
					}
					progressed = true
					w.task = coordinator.TaskID{Channel: ch, Partition: work.Partition, Epoch: seg.Epoch}
					for _, r := range recs {
						if !w.syncLoop {
							// Outside a Synchronous loop the epoch is carried
							// by the records themselves so pipelines inside a
							// cycle propagate it.
							w.epoch = r.Epoch
						}
						if h.OnRecord != nil {
							if err := h.OnRecord(ctx, w, Record{Channel: ch, Key: r.Key, Value: r.Value, Epoch: r.Epoch}); err != nil {
								return err
							}
						}
					}
					acks := []coordinator.SegmentAck{{ID: seg.ID, Holder: seg.Holder, Pod: w.Instance}}
					if w.checkpoint != nil {
						if err := w.commitCheckpoint(ctx, h, ch, acks, false, false); err != nil {
							return err
						}
						continue
					}
					if err := w.retry(ctx, w.Flush); err != nil {
						return err
					}
					if err := w.retry(ctx, func() error { return w.ack(ch, acks) }); err != nil {
						return err
					}
				}
			}
			if !resp.Drained {
				allDrained = false
			}
			if resp.FiniteEpochs && !resp.Quiescent {
				allQuiet = false
			} else if w.feedback[ch] && resp.Mode != graph.FeedbackAsynchronous && !resp.Quiescent {
				allQuiet = false
			} else if !resp.FiniteEpochs && !w.feedback[ch] && !resp.Drained {
				// A loop cannot end an epoch while non-loop inputs still flow.
				allQuiet = false
			}
		}
		if w.epoch != observedEpoch {
			observedEpoch = w.epoch
			backoff = pollFloor
		}

		// Tick runs here: on this goroutine, between passes over the inbound
		// channels, and never inside one. It is placed before the progressed
		// check so that a busy operation still ticks; putting it after would
		// starve the clock exactly when the input is heaviest.
		if ticking && !time.Now().Before(nextTick) {
			if err := h.Tick(ctx, w); err != nil {
				return err
			}
			if err := w.retry(ctx, w.Flush); err != nil {
				return err
			}
			// Measured from the end of the handler, so a Tick that runs longer
			// than the interval does not come due again the instant it returns.
			nextTick = time.Now().Add(w.TickInterval)
		}
		if progressed {
			backoff = pollFloor
			continue
		}
		if allDrained {
			if h.OnDrain != nil {
				if err := h.OnDrain(ctx, w); err != nil {
					return err
				}
			}
			if w.checkpoint != nil {
				if err := w.commitCheckpoint(ctx, h, "", nil, true, false); err != nil {
					return err
				}
			} else if err := w.retry(ctx, w.Flush); err != nil {
				return err
			}
			if err := w.retry(ctx, w.reportDone); err != nil {
				return err
			}
			log.Printf("%s: drained", w.Instance)
			if w.CollectiveSize > 0 {
				return nil
			}
			return w.idle(ctx)
		}
		if w.syncLoop && allQuiet && w.lastDone < w.epoch {
			if h.OnEpochEnd != nil {
				if err := h.OnEpochEnd(ctx, w, w.epoch); err != nil {
					return err
				}
			}
			if w.checkpoint != nil {
				if err := w.commitCheckpoint(ctx, h, "", nil, false, true); err != nil {
					return err
				}
			} else if err := w.retry(ctx, w.Flush); err != nil {
				return err
			}
			if err := w.retry(ctx, func() error {
				return w.do("POST", fmt.Sprintf("%s/%s%s?pod=%s&epoch=%d", coordinator.PathOperations, w.Operation, coordinator.SuffixEpochDone, w.Instance, w.epoch), nil, nil)
			}); err != nil {
				return err
			}
			w.lastDone = w.epoch
			continue
		}
		ceiling := pollCeiling
		if w.syncLoop {
			ceiling = barrierPollCeiling
		}
		if backoff > ceiling {
			backoff = ceiling
		}
		// The idle backoff must not outrun the clock. Without this cap a
		// worker that has settled at the two-second ceiling would serve a
		// hundred-millisecond tick interval two seconds late.
		wait := backoff
		if wait > ceiling {
			wait = ceiling
		}
		if ticking {
			if d := time.Until(nextTick); d < wait {
				wait = d
			}
		}
		if wait > 0 {
			time.Sleep(wait)
		}
		if backoff < ceiling {
			backoff *= 2
		}
	}
	return ctx.Err()
}

// fetchRetry fetches a segment, riding out a transient failure of its holder
// for a bounded number of attempts.
func (w *Worker) fetchRetry(ctx context.Context, seg coordinator.SegmentRef) ([]wireRecord, error) {
	retryCtx, cancel := context.WithTimeout(ctx, fetchRetryBudget)
	defer cancel()
	wait := fetchRetryFloor
	var err error
	for i := 0; i < fetchAttempts && retryCtx.Err() == nil; i++ {
		var recs []wireRecord
		if recs, err = w.fetch(retryCtx, seg); err == nil {
			return recs, nil
		}
		log.Printf("fetch segment %s from %s (attempt %d of %d): %v", seg.ID, seg.Holder, i+1, fetchAttempts, err)
		if i == fetchAttempts-1 {
			break
		}
		select {
		case <-retryCtx.Done():
		case <-time.After(wait):
		}
		if wait < fetchRetryCeiling {
			wait *= 2
		}
	}
	if ctx.Err() != nil {
		return nil, ctx.Err()
	}
	return nil, fmt.Errorf("fetch segment %s from %s: giving up after %d attempts: %w", seg.ID, seg.Holder, fetchAttempts, err)
}

// idle keeps the pod alive after its work is done: heartbeats continue,
// held segments stay served, and released ones are deleted, until ctx ends.
func (w *Worker) idle(ctx context.Context) error {
	<-ctx.Done()
	return nil
}

// heartbeat re-registers every 5s and deletes released segments.
func (w *Worker) heartbeat(ctx context.Context) {
	t := time.NewTicker(5 * time.Second)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			if err := w.register(); err != nil {
				log.Printf("heartbeat: %v", err)
			}
			w.releaseSegments()
		}
	}
}

// errSealed is returned by a produce call on a sealed channel. A restarted
// source pod sees it when the operation already completed; the work is
// not repeated.
var errSealed = errors.New("channel is sealed")

func (w *Worker) retry(ctx context.Context, f func() error) error {
	var err error
	for i := 0; i < 30 && ctx.Err() == nil; i++ {
		if err = f(); err == nil {
			return nil
		}
		if errors.Is(err, errStorageCapacity) {
			return err
		}
		if errors.Is(err, errSealed) {
			return err
		}
		log.Printf("coordinator call failed (attempt %d): %v", i+1, err)
		time.Sleep(time.Second)
	}
	return err
}
