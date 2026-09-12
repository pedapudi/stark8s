package runtime

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"sync"

	"github.com/pedapudi/stark8s/pkg/sdk"
)

const maxMessageBytes = 8 << 20

type call struct {
	inv   Invocation
	reply chan Reply
}

// Server owns one application session and at most one active invocation.
type Server struct {
	mu           sync.Mutex
	session      string
	ready        bool
	current      *call
	changed      chan struct{}
	checkpointed bool
	fatal        error
	failures     chan error
}

// NewServer creates an idle local protocol server.
func NewServer() *Server { return &Server{changed: make(chan struct{}), failures: make(chan error, 1)} }

// ConfigureCheckpointing makes replacement application sessions fail the
// worker attempt. A fresh worker must restore the last committed snapshot
// before it can safely replay input into a replacement application process.
func (s *Server) ConfigureCheckpointing(enabled bool) {
	s.mu.Lock()
	s.checkpointed = enabled
	s.mu.Unlock()
}

// Failures reports a terminal protocol failure that requires worker restart.
func (s *Server) Failures() <-chan error { return s.failures }

func id() (string, error) {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", err
	}
	return hex.EncodeToString(b[:]), nil
}

func (s *Server) signal() {
	close(s.changed)
	s.changed = make(chan struct{})
}

// ServeHTTP implements the local runtime protocol.
func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	switch {
	case r.Method == http.MethodGet && r.URL.Path == PathHealth:
		w.WriteHeader(http.StatusNoContent)
	case r.Method == http.MethodPost && r.URL.Path == PathConnect:
		s.connect(w)
	case r.Method == http.MethodPost && r.URL.Path == PathReady:
		s.setReady(w, r)
	case r.Method == http.MethodGet && r.URL.Path == PathInvocations:
		s.next(w, r)
	case r.Method == http.MethodPost && r.URL.Path == PathReplies:
		s.complete(w, r)
	default:
		http.NotFound(w, r)
	}
}

func (s *Server) connect(w http.ResponseWriter) {
	sid, err := id()
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	s.mu.Lock()
	if s.fatal != nil {
		err := s.fatal
		s.mu.Unlock()
		http.Error(w, err.Error(), http.StatusServiceUnavailable)
		return
	}
	if s.checkpointed && s.session != "" {
		err := errors.New("checkpointed application session was replaced; restart the worker to restore committed state")
		s.fatal = err
		if s.current != nil {
			s.current.reply <- Reply{Error: err.Error()}
			s.current = nil
		}
		s.signal()
		s.failures <- err
		s.mu.Unlock()
		http.Error(w, err.Error(), http.StatusConflict)
		return
	}
	s.session, s.ready = sid, false
	s.signal()
	s.mu.Unlock()
	writeJSON(w, session{SessionID: sid})
}

func decode(w http.ResponseWriter, r *http.Request, dst any) bool {
	r.Body = http.MaxBytesReader(w, r.Body, maxMessageBytes)
	decoder := json.NewDecoder(r.Body)
	if err := decoder.Decode(dst); err != nil {
		http.Error(w, "invalid JSON", http.StatusBadRequest)
		return false
	}
	if err := decoder.Decode(&struct{}{}); err != io.EOF {
		http.Error(w, "request body must contain one JSON value", http.StatusBadRequest)
		return false
	}
	return true
}

func (s *Server) setReady(w http.ResponseWriter, r *http.Request) {
	var got session
	if !decode(w, r, &got) {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if got.SessionID == "" || got.SessionID != s.session {
		http.Error(w, "stale session", http.StatusConflict)
		return
	}
	s.ready = true
	s.signal()
	w.WriteHeader(http.StatusNoContent)
}

func (s *Server) next(w http.ResponseWriter, r *http.Request) {
	sid := r.URL.Query().Get("sessionId")
	for {
		s.mu.Lock()
		if s.fatal != nil {
			err := s.fatal
			s.mu.Unlock()
			http.Error(w, err.Error(), http.StatusServiceUnavailable)
			return
		}
		if sid == "" || sid != s.session {
			s.mu.Unlock()
			http.Error(w, "stale session", http.StatusConflict)
			return
		}
		if s.ready && s.current != nil {
			inv := s.current.inv
			s.mu.Unlock()
			writeJSON(w, inv)
			return
		}
		changed := s.changed
		s.mu.Unlock()
		select {
		case <-r.Context().Done():
			return
		case <-changed:
		}
	}
}

func (s *Server) complete(w http.ResponseWriter, r *http.Request) {
	var reply Reply
	if !decode(w, r, &reply) {
		return
	}
	s.mu.Lock()
	if reply.SessionID == "" || reply.SessionID != s.session {
		s.mu.Unlock()
		http.Error(w, "stale session", http.StatusConflict)
		return
	}
	if s.current == nil || reply.InvocationID != s.current.inv.ID {
		s.mu.Unlock()
		http.Error(w, "stale invocation", http.StatusConflict)
		return
	}
	c := s.current
	s.current = nil
	s.signal()
	s.mu.Unlock()
	c.reply <- reply
	w.WriteHeader(http.StatusNoContent)
}

func writeJSON(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(v)
}

// WaitReady waits until an application session completes its readiness
// handshake. Workers must not register or claim work before this succeeds.
func (s *Server) WaitReady(ctx context.Context) error {
	for {
		s.mu.Lock()
		if s.fatal != nil {
			err := s.fatal
			s.mu.Unlock()
			return err
		}
		if s.ready {
			s.mu.Unlock()
			return nil
		}
		changed := s.changed
		s.mu.Unlock()
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-changed:
		}
	}
}

// Invoke delivers one callback. A replacement session receives the same
// unfinished invocation, while replies from the replaced session are rejected.
func (s *Server) Invoke(ctx context.Context, inv Invocation) (Reply, error) {
	if err := s.WaitReady(ctx); err != nil {
		return Reply{}, err
	}
	invocationID, err := id()
	if err != nil {
		return Reply{}, err
	}
	inv.ID = invocationID
	c := &call{inv: inv, reply: make(chan Reply, 1)}
	s.mu.Lock()
	if s.current != nil {
		s.mu.Unlock()
		return Reply{}, errors.New("an invocation is already active")
	}
	s.current = c
	s.signal()
	s.mu.Unlock()
	select {
	case <-ctx.Done():
		s.mu.Lock()
		if s.current == c {
			s.current = nil
			s.signal()
		}
		s.mu.Unlock()
		return Reply{}, ctx.Err()
	case reply := <-c.reply:
		if reply.Error != "" {
			return reply, errors.New(reply.Error)
		}
		return reply, nil
	}
}

// Handlers adapts local invocations to the existing serial SDK worker.
func (s *Server) Handlers() sdk.Handlers {
	call := func(ctx context.Context, w *sdk.Worker, inv Invocation) error {
		return callThroughServer(ctx, s, w, inv)
	}
	return sdk.Handlers{
		Source: func(ctx context.Context, w *sdk.Worker) error {
			return call(ctx, w, Invocation{Kind: "source"})
		},
		OnRecord: func(ctx context.Context, w *sdk.Worker, r sdk.Record) error {
			return call(ctx, w, Invocation{Kind: "record", Record: &Record{Channel: r.Channel, Key: r.Key, Value: r.Value, Epoch: r.Epoch}})
		},
		OnEpochEnd: func(ctx context.Context, w *sdk.Worker, epoch int32) error {
			return call(ctx, w, Invocation{Kind: "epoch", Epoch: epoch})
		},
		OnDrain: func(ctx context.Context, w *sdk.Worker) error {
			return call(ctx, w, Invocation{Kind: "drain"})
		},
	}
}

// HandlersFor adds checkpoint callbacks only when the worker is configured
// for checkpointing. The base callback set remains valid without a store.
func (s *Server) HandlersFor(w *sdk.Worker) sdk.Handlers {
	h := s.Handlers()
	if w.TickInterval > 0 {
		h.Tick = func(ctx context.Context, w *sdk.Worker) error {
			return callThroughServer(ctx, s, w, Invocation{Kind: "tick"})
		}
	}
	if w.CheckpointStore == nil {
		return h
	}
	h.Snapshot = func(ctx context.Context) ([]byte, error) {
		reply, err := s.Invoke(ctx, Invocation{Kind: "snapshot"})
		if err != nil {
			return nil, err
		}
		if len(reply.State) == 0 || !json.Valid(reply.State) {
			return nil, errors.New("snapshot reply requires JSON state")
		}
		return append([]byte(nil), reply.State...), nil
	}
	h.Restore = func(ctx context.Context, state []byte) error {
		if !json.Valid(state) {
			return errors.New("checkpoint state is not JSON")
		}
		_, err := s.Invoke(ctx, Invocation{Kind: "restore", Data: append(json.RawMessage(nil), state...)})
		return err
	}
	return h
}

func callThroughServer(ctx context.Context, s *Server, w *sdk.Worker, inv Invocation) error {
	inv.Epoch, inv.MaxEpoch = w.Epoch(), w.MaxEpochs()
	reply, err := s.Invoke(ctx, inv)
	if err != nil {
		return err
	}
	for _, out := range reply.Outputs {
		if err := w.ValidateEmit(out.Channel, out.Key, out.Value); err != nil {
			return fmt.Errorf("invocation %s output: %w", reply.InvocationID, err)
		}
	}
	for _, out := range reply.Outputs {
		if err := w.Emit(out.Channel, out.Key, out.Value); err != nil {
			return err
		}
	}
	return nil
}
