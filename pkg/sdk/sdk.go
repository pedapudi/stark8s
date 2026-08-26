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
// Run implements the completion and loop protocols so application code only
// handles records. A worker never exits on its own: once its work is done
// it keeps heartbeating and serving the segments it holds until ctx ends,
// because consumers may still need them and the pod runs under a
// Deployment.
package sdk

import (
	"bytes"
	"context"
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

	"github.com/pedapudi/stark8s/api/v1alpha1"
	"github.com/pedapudi/stark8s/pkg/coordinator"
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
}

// Handlers are the application callbacks. All are optional.
type Handlers struct {
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

// Worker is one pod of an operation.
type Worker struct {
	Coordinator string
	Workload    string
	Operation   string
	Instance    string
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
	TickInterval time.Duration

	feedback    map[string]bool
	feedbackOut map[string]bool

	client *http.Client
	specs  map[string]v1alpha1.Channel

	buffers     map[bufKey][]wireRecord
	order       []bufKey
	rr          map[string]uint64
	unannounced map[string][]coordinator.SegmentAnnouncement
	overflowed  map[string]int64

	epoch    int32
	maxEpoch int32
	lastDone int32
	// syncLoop: an inbound feedback channel runs in Synchronous mode, so
	// the epoch comes from the coordinator rather than from records.
	syncLoop bool
	// task is the unit of work being processed; announced segments carry it.
	task coordinator.TaskID

	addr  string
	store *segmentStore
}

// FromEnv builds a Worker from the injected environment.
func FromEnv() (*Worker, error) {
	slots, _ := strconv.Atoi(os.Getenv(coordinator.EnvSlots))
	w := &Worker{
		Coordinator: os.Getenv(coordinator.EnvCoordinator),
		Workload:    os.Getenv(coordinator.EnvWorkload),
		Operation:   os.Getenv(coordinator.EnvOperation),
		Instance:    os.Getenv(coordinator.EnvInstance),
		PodIP:       os.Getenv(coordinator.EnvPodIP),
		Slots:       int32(slots),
		Inbound:     split(os.Getenv(coordinator.EnvInbound)),
		Outbound:    split(os.Getenv(coordinator.EnvOutbound)),
		SegmentDir:  os.Getenv(coordinator.EnvSegmentDir),
	}
	w.init()
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

// init fills defaults so a Worker built by hand (tests) works like one from
// FromEnv.
func (w *Worker) init() {
	if w.feedback == nil {
		w.feedback = map[string]bool{}
	}
	if w.feedbackOut == nil {
		w.feedbackOut = map[string]bool{}
	}
	if w.client == nil {
		w.client = &http.Client{Timeout: 60 * time.Second}
	}
	if w.buffers == nil {
		w.buffers = map[bufKey][]wireRecord{}
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

// segmentStore keeps this pod's segments as JSON files, one per segment.
type segmentStore struct {
	dir string
	mu  sync.Mutex
	seq uint64
}

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
	tmp := s.path(id) + ".tmp"
	if err := os.WriteFile(tmp, body, 0o644); err != nil {
		return "", 0, err
	}
	if err := os.Rename(tmp, s.path(id)); err != nil {
		return "", 0, err
	}
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

func (s *segmentStore) remove(id string) { _ = os.Remove(s.path(id)) }

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
	w.init()
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
		if spec.Feedback != nil && spec.Feedback.Mode == v1alpha1.FeedbackAsynchronous && epoch >= spec.Feedback.MaxEpochs {
			if spec.Feedback.Overflow == "" {
				w.overflowed[channel]++
				return nil
			}
			return w.buffer(spec.Feedback.Overflow, key, b, epoch)
		}
	}
	return w.buffer(channel, key, b, epoch)
}

func (w *Worker) buffer(channel, key string, value json.RawMessage, epoch int32) error {
	spec, err := w.spec(channel)
	if err != nil {
		return err
	}
	var p int32
	switch {
	case spec.To == "" || spec.Partitioning.Mode == v1alpha1.PartitionBroadcast:
		p = 0
	case spec.Partitioning.Mode == v1alpha1.PartitionHash:
		p = int32(coordinator.HashPartition(key, int(spec.Partitioning.Partitions)))
	default:
		p = int32(w.rr[channel] % uint64(spec.Partitioning.Partitions))
		w.rr[channel]++
	}
	k := bufKey{channel, p, epoch}
	if _, ok := w.buffers[k]; !ok {
		w.order = append(w.order, k)
	}
	w.buffers[k] = append(w.buffers[k], wireRecord{Key: key, Value: value, Epoch: epoch})
	if len(w.buffers[k]) >= 500 {
		return w.flushBuffer(k)
	}
	return nil
}

// spec returns the declared channel, loading the topology on first use.
func (w *Worker) spec(channel string) (v1alpha1.Channel, error) {
	if w.specs == nil {
		if err := w.loadTopology(); err != nil {
			return v1alpha1.Channel{}, err
		}
	}
	s, ok := w.specs[channel]
	if !ok {
		return v1alpha1.Channel{}, fmt.Errorf("channel %q is not declared in the topology", channel)
	}
	return s, nil
}

func (w *Worker) loadTopology() error {
	var specs []v1alpha1.Channel
	if err := w.do("GET", coordinator.PathTopology, nil, &specs); err != nil {
		return err
	}
	m := map[string]v1alpha1.Channel{}
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
		return nil
	}
	spec, err := w.spec(k.channel)
	if err != nil {
		return err
	}
	if spec.To == "" {
		body, _ := json.Marshal(recs)
		if err := w.do("POST", coordinator.PathChannels+"/"+k.channel+coordinator.SuffixRecords, body, nil); err != nil {
			return err
		}
		delete(w.buffers, k)
		return nil
	}
	if w.store == nil {
		if err := w.serveSegments(); err != nil {
			return err
		}
	}
	id, size, err := w.store.write(w.Instance, recs)
	if err != nil {
		return err
	}
	delete(w.buffers, k)
	w.unannounced[k.channel] = append(w.unannounced[k.channel], coordinator.SegmentAnnouncement{
		ID: id, Channel: k.channel, Partition: k.partition, Epoch: k.epoch,
		Records: int64(len(recs)), Bytes: size, Holder: w.addr, Producer: w.Instance, Task: w.task,
	})
	return nil
}

// Flush writes every buffer as a segment and announces all segments not yet
// announced, together with the count of records dropped at a loop bound.
// It is safe to retry: a failed announcement stays queued.
func (w *Worker) Flush() error {
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
	for ch, anns := range w.unannounced {
		body, _ := json.Marshal(anns)
		if err := w.do("POST", coordinator.PathChannels+"/"+ch+coordinator.SuffixSegments, body, nil); err != nil {
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
	return coordinator.PodRegistration{Operation: w.Operation, Pod: w.Instance, Addr: w.addr, Slots: w.Slots}
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
	return w.do("POST", coordinator.PathChannels+"/"+ch+coordinator.SuffixAck, body, nil)
}

// fetch reads a segment from its holder.
func (w *Worker) fetch(ref coordinator.SegmentRef) ([]wireRecord, error) {
	resp, err := w.client.Get("http://" + ref.Holder + "/segments/" + ref.ID)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		return nil, fmt.Errorf("fetch %s from %s: %s", ref.ID, ref.Holder, resp.Status)
	}
	var recs []wireRecord
	if err := json.NewDecoder(resp.Body).Decode(&recs); err != nil {
		return nil, err
	}
	return recs, nil
}

// releaseSegments deletes the local segments the coordinator has released.
func (w *Worker) releaseSegments() {
	if w.store == nil {
		return
	}
	var ids []string
	if err := w.do("GET", coordinator.PathReleased+"?pod="+w.Instance, nil, &ids); err != nil {
		log.Printf("released segments: %v", err)
		return
	}
	for _, id := range ids {
		w.store.remove(id)
	}
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
	go func() { _ = http.Serve(ln, mux) }()
	w.store = store
	return nil
}

// --- running --------------------------------------------------------------

// Run executes the worker: it registers, starts heartbeats and the segment
// server, loads the topology, and then drives the source or the
// poll/process/ack loop. When the work is done it idles until ctx ends,
// returning nil; handler errors abort the worker.
func (w *Worker) Run(ctx context.Context, h Handlers) error {
	w.init()
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
	go w.heartbeat(ctx)

	if len(w.Inbound) == 0 {
		if h.Source == nil {
			return fmt.Errorf("operation %s has no inbound channels and no Source handler", w.Operation)
		}
		w.task = coordinator.TaskID{Channel: w.Instance}
		if err := h.Source(ctx, w); err != nil {
			return err
		}
		if err := w.retry(ctx, w.Flush); err != nil && !errors.Is(err, errSealed) {
			return err
		} else if err != nil {
			log.Printf("%s: output channel already sealed; source output was produced by an earlier pod", w.Instance)
			w.buffers = map[bufKey][]wireRecord{}
		}
		if err := w.retry(ctx, w.reportDone); err != nil {
			return err
		}
		log.Printf("%s: source complete", w.Instance)
		return w.idle(ctx)
	}

	// external is the set of inbound channels that no operation produces
	// into. They are fed from outside the workload through the records API,
	// and bounded counts the rest.
	//
	// The distinction decides when this operation is finished. A channel with
	// no producing operation is never sealed, because sealing is what the
	// controller does when a producing operation completes and there is no
	// producer here to complete. A channel reports drained only once it is
	// sealed, so an external channel reports drained never. Counting it in
	// the drain test would hold the operation open for a seal that cannot
	// come: OnDrain would never run, the operation would never report done,
	// its outbound channels would never be sealed, and every consumer behind
	// a Materialized edge downstream would wait forever. So completion is
	// decided by the channels that have a producer, and an external topic
	// left open forever does not by itself keep the operation running.
	//
	// An operation whose inbound channels are ALL external is a different
	// case, and it does not finish. There is no bounded input to wait for and
	// nothing can ever say the outside world has stopped sending, so the
	// operation stays in this loop consuming and ticking until its context
	// ends. Draining it instead would be worse: it would report done before
	// processing anything and quietly discard the stream it exists to serve.
	// Such an operation cannot feed a Materialized channel, because that edge
	// waits on a seal that requires a completion which will never happen.
	// Validate rejects that graph rather than letting it hang.
	//
	// The epoch barrier below is left alone. A Synchronous loop that also
	// takes an external side input still refuses to advance while that input
	// is unsealed, which is the behaviour this change does not attempt to
	// fix; no example combines the two.
	external := map[string]bool{}
	bounded := 0
	for _, ch := range w.Inbound {
		spec, err := w.spec(ch)
		if err != nil {
			return err
		}
		if spec.From == "" {
			external[ch] = true
			continue
		}
		bounded++
	}
	if len(external) > 0 {
		log.Printf("%s: %d of %d inbound channels have no producing operation; completion follows the other %d",
			w.Instance, len(external), len(w.Inbound), bounded)
	}

	ticking := h.Tick != nil && w.TickInterval > 0
	if h.Tick != nil && !ticking {
		log.Printf("%s: a Tick handler is set but the tick interval is zero, so Tick is never called", w.Instance)
	}
	nextTick := time.Now().Add(w.TickInterval)

	backoff := 100 * time.Millisecond
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
				if resp.Mode != v1alpha1.FeedbackAsynchronous {
					w.syncLoop = true
					w.epoch = resp.Epoch
				}
			}
			for _, work := range resp.Work {
				for _, seg := range work.Segments {
					recs, err := w.fetch(seg)
					if err != nil {
						// The segment stays in flight; it is redelivered if
						// this pod expires or marked lost if the holder does.
						log.Printf("fetch segment %s: %v", seg.ID, err)
						continue
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
					if err := w.retry(ctx, w.Flush); err != nil {
						return err
					}
					acks := []coordinator.SegmentAck{{ID: seg.ID, Holder: seg.Holder, Pod: w.Instance}}
					if err := w.retry(ctx, func() error { return w.ack(ch, acks) }); err != nil {
						return err
					}
				}
			}
			if !resp.Drained && !external[ch] {
				allDrained = false
			}
			if w.feedback[ch] && resp.Mode != v1alpha1.FeedbackAsynchronous && !resp.Quiescent {
				allQuiet = false
			}
			if !w.feedback[ch] && !resp.Drained {
				// A loop cannot end an epoch while non-loop inputs still flow.
				allQuiet = false
			}
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
			backoff = 100 * time.Millisecond
			continue
		}
		if allDrained && bounded > 0 {
			if h.OnDrain != nil {
				if err := h.OnDrain(ctx, w); err != nil {
					return err
				}
			}
			if err := w.retry(ctx, w.Flush); err != nil {
				return err
			}
			if err := w.retry(ctx, w.reportDone); err != nil {
				return err
			}
			log.Printf("%s: drained", w.Instance)
			return w.idle(ctx)
		}
		if w.syncLoop && allQuiet && w.lastDone < w.epoch {
			if h.OnEpochEnd != nil {
				if err := h.OnEpochEnd(ctx, w, w.epoch); err != nil {
					return err
				}
			}
			if err := w.retry(ctx, w.Flush); err != nil {
				return err
			}
			for ch := range w.feedback {
				ch := ch
				if err := w.retry(ctx, func() error {
					return w.do("POST", fmt.Sprintf("%s/%s%s?pod=%s&epoch=%d", coordinator.PathChannels, ch, coordinator.SuffixEpochDone, w.Instance, w.epoch), nil, nil)
				}); err != nil {
					return err
				}
			}
			w.lastDone = w.epoch
			continue
		}
		// The idle backoff must not outrun the clock. Without this cap a
		// worker that has settled at the two-second ceiling would serve a
		// hundred-millisecond tick interval two seconds late.
		wait := backoff
		if ticking {
			if d := time.Until(nextTick); d < wait {
				wait = d
			}
		}
		if wait > 0 {
			time.Sleep(wait)
		}
		if backoff < 2*time.Second {
			backoff *= 2
		}
	}
	return ctx.Err()
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
		if errors.Is(err, errSealed) {
			return err
		}
		log.Printf("coordinator call failed (attempt %d): %v", i+1, err)
		time.Sleep(time.Second)
	}
	return err
}
