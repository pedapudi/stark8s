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
	// OnDrain is called once when every inbound channel is drained. Emit
	// final results here; the worker then reports done and idles.
	OnDrain func(ctx context.Context, w *Worker) error
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
		if err := w.retry(ctx, w.Flush); err != nil {
			return err
		}
		if err := w.retry(ctx, w.reportDone); err != nil {
			return err
		}
		log.Printf("%s: source complete", w.Instance)
		return w.idle(ctx)
	}

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
			if !resp.Drained {
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
		if progressed {
			backoff = 100 * time.Millisecond
			continue
		}
		if allDrained {
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
		time.Sleep(backoff)
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

func (w *Worker) retry(ctx context.Context, f func() error) error {
	var err error
	for i := 0; i < 30 && ctx.Err() == nil; i++ {
		if err = f(); err == nil {
			return nil
		}
		log.Printf("coordinator call failed (attempt %d): %v", i+1, err)
		time.Sleep(time.Second)
	}
	return err
}
