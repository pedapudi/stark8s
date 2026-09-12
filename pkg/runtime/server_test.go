package runtime

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/pedapudi/stark8s/api/graph"
	"github.com/pedapudi/stark8s/pkg/coordinator"
	"github.com/pedapudi/stark8s/pkg/sdk"
	"github.com/pedapudi/stark8s/pkg/storage"
)

func request(t *testing.T, client *http.Client, method, url string, in, out any) int {
	t.Helper()
	var body bytes.Buffer
	if in != nil {
		if err := json.NewEncoder(&body).Encode(in); err != nil {
			t.Fatal(err)
		}
	}
	req, err := http.NewRequest(method, url, &body)
	if err != nil {
		t.Fatal(err)
	}
	resp, err := client.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if out != nil && resp.StatusCode < 300 {
		if err := json.NewDecoder(resp.Body).Decode(out); err != nil {
			t.Fatal(err)
		}
	}
	return resp.StatusCode
}

func TestGoAndPythonClientsRunSourceThroughDrain(t *testing.T) {
	for _, language := range []string{"go", "python"} {
		t.Run(language, func(t *testing.T) {
			segmentServer := httptest.NewServer(nil)
			co := coordinator.New(strings.TrimPrefix(segmentServer.URL, "http://"))
			segmentServer.Config.Handler = coordinator.SegmentHandler(co)
			defer segmentServer.Close()
			if err := co.Configure([]graph.Channel{
				{Name: "input", From: "source", To: "sink", Partitioning: graph.Partitioning{Partitions: 1}},
				{Name: "output", From: "sink", Partitioning: graph.Partitioning{Partitions: 1}},
			}); err != nil {
				t.Fatal(err)
			}
			co.SetOperations([]coordinator.OperationSpec{{Name: "source", Replicas: 1}, {Name: "sink", Replicas: 1}})
			control := httptest.NewServer(coordinator.Handler(co))
			defer control.Close()
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			sourceWorker := &sdk.Worker{Coordinator: control.URL, Operation: "source", Instance: "source-0", Outbound: []string{"input"}, SegmentDir: t.TempDir(), SegmentListen: "127.0.0.1:0"}
			sinkWorker := &sdk.Worker{Coordinator: control.URL, Operation: "sink", Instance: "sink-0", Inbound: []string{"input"}, Outbound: []string{"output"}, SegmentDir: t.TempDir(), SegmentListen: "127.0.0.1:0"}
			sourceRuntime, sinkRuntime := NewServer(), NewServer()
			sourceHTTP, sinkHTTP := httptest.NewServer(sourceRuntime), httptest.NewServer(sinkRuntime)
			defer sourceHTTP.Close()
			defer sinkHTTP.Close()
			var apps []*exec.Cmd
			if language == "go" {
				go (&Client{BaseURL: sourceHTTP.URL, HTTP: sourceHTTP.Client()}).Run(ctx, Adapters{Source: func(context.Context, Invocation) ([]Output, error) {
					return []Output{{Channel: "input", Key: "key", Value: json.RawMessage("21")}}, nil
				}})
				go (&Client{BaseURL: sinkHTTP.URL, HTTP: sinkHTTP.Client()}).Run(ctx, Adapters{OnRecord: func(_ context.Context, inv Invocation) ([]Output, error) {
					return []Output{{Channel: "output", Key: inv.Record.Key, Value: json.RawMessage("42")}}, nil
				}})
			} else {
				_, file, _, _ := runtime.Caller(0)
				script := filepath.Join(filepath.Dir(file), "..", "..", "clients", "python", "handler_fixture.py")
				apps = []*exec.Cmd{exec.CommandContext(ctx, "python3", script, sourceHTTP.URL, "source"), exec.CommandContext(ctx, "python3", script, sinkHTTP.URL, "sink")}
				for _, app := range apps {
					if err := app.Start(); err != nil {
						t.Fatal(err)
					}
				}
			}
			errs := make(chan error, 2)
			go func() { errs <- sourceWorker.Run(ctx, sourceRuntime.HandlersFor(sourceWorker)) }()
			go func() { errs <- sinkWorker.Run(ctx, sinkRuntime.HandlersFor(sinkWorker)) }()
			deadline := time.Now().Add(5 * time.Second)
			for time.Now().Before(deadline) {
				if co.Metrics().Channels[1].Produced == 1 {
					break
				}
				time.Sleep(10 * time.Millisecond)
			}
			if got := co.Metrics().Channels[1].Produced; got != 1 {
				t.Fatalf("output records = %d", got)
			}
			cancel()
			<-errs
			<-errs
			for _, app := range apps {
				_ = app.Wait()
			}
		})
	}
}

func connect(t *testing.T, ts *httptest.Server) string {
	t.Helper()
	var sess session
	if got := request(t, ts.Client(), http.MethodPost, ts.URL+PathConnect, struct{}{}, &sess); got != http.StatusOK {
		t.Fatalf("connect returned %d", got)
	}
	if got := request(t, ts.Client(), http.MethodPost, ts.URL+PathReady, sess, nil); got != http.StatusNoContent {
		t.Fatalf("ready returned %d", got)
	}
	return sess.SessionID
}

func TestReconnectRedeliversAndRejectsStaleReply(t *testing.T) {
	s := NewServer()
	ts := httptest.NewServer(s)
	defer ts.Close()
	first := connect(t, ts)
	replies := make(chan Reply, 1)
	errs := make(chan error, 1)
	go func() {
		reply, err := s.Invoke(context.Background(), Invocation{Kind: "record"})
		replies <- reply
		errs <- err
	}()
	var before Invocation
	request(t, ts.Client(), http.MethodGet, ts.URL+PathInvocations+"?sessionId="+first, nil, &before)
	second := connect(t, ts)
	var after Invocation
	request(t, ts.Client(), http.MethodGet, ts.URL+PathInvocations+"?sessionId="+second, nil, &after)
	if before.ID == "" || after.ID != before.ID {
		t.Fatalf("redelivered invocation IDs %q and %q", before.ID, after.ID)
	}
	stale := Reply{SessionID: first, InvocationID: before.ID}
	if got := request(t, ts.Client(), http.MethodPost, ts.URL+PathReplies, stale, nil); got != http.StatusConflict {
		t.Fatalf("stale reply returned %d", got)
	}
	want := Reply{SessionID: second, InvocationID: after.ID}
	if got := request(t, ts.Client(), http.MethodPost, ts.URL+PathReplies, want, nil); got != http.StatusNoContent {
		t.Fatalf("current reply returned %d", got)
	}
	if err := <-errs; err != nil {
		t.Fatal(err)
	}
	if got := <-replies; got.InvocationID != after.ID {
		t.Fatalf("reply invocation %q", got.InvocationID)
	}
}

func TestCheckpointedReconnectRequiresWorkerRestart(t *testing.T) {
	s := NewServer()
	s.ConfigureCheckpointing(true)
	ts := httptest.NewServer(s)
	defer ts.Close()
	connect(t, ts)
	if got := request(t, ts.Client(), http.MethodPost, ts.URL+PathConnect, struct{}{}, nil); got != http.StatusConflict {
		t.Fatalf("replacement connect returned %d", got)
	}
	select {
	case err := <-s.Failures():
		if err == nil || !strings.Contains(err.Error(), "restore committed state") {
			t.Fatalf("replacement failure = %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("replacement did not fail the worker attempt")
	}
}

func TestDecodeRejectsTrailingJSON(t *testing.T) {
	ts := httptest.NewServer(NewServer())
	defer ts.Close()
	req, err := http.NewRequest(http.MethodPost, ts.URL+PathReady, strings.NewReader(`{"sessionId":"x"} {}`))
	if err != nil {
		t.Fatal(err)
	}
	resp, err := ts.Client().Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("trailing JSON returned %d", resp.StatusCode)
	}
}

func TestApplicationErrorFailsInvocation(t *testing.T) {
	s := NewServer()
	ts := httptest.NewServer(s)
	defer ts.Close()
	sid := connect(t, ts)
	errResult := make(chan error, 1)
	go func() {
		_, err := s.Invoke(context.Background(), Invocation{Kind: "source"})
		errResult <- err
	}()
	var inv Invocation
	request(t, ts.Client(), http.MethodGet, ts.URL+PathInvocations+"?sessionId="+sid, nil, &inv)
	reply := Reply{SessionID: sid, InvocationID: inv.ID, Error: "application failed"}
	if got := request(t, ts.Client(), http.MethodPost, ts.URL+PathReplies, reply, nil); got != http.StatusNoContent {
		t.Fatalf("error reply returned %d", got)
	}
	if err := <-errResult; err == nil || err.Error() != "application failed" {
		t.Fatalf("invocation error %v", err)
	}
}

func TestWaitReadyBlocksUntilHandshake(t *testing.T) {
	s := NewServer()
	ts := httptest.NewServer(s)
	defer ts.Close()
	done := make(chan error, 1)
	go func() { done <- s.WaitReady(context.Background()) }()
	select {
	case err := <-done:
		t.Fatalf("WaitReady returned before handshake: %v", err)
	case <-time.After(20 * time.Millisecond):
	}
	connect(t, ts)
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("WaitReady did not observe handshake")
	}
}

func TestHandlersIncludeTickAndOptionalCheckpoints(t *testing.T) {
	s := NewServer()
	plain := s.HandlersFor(&sdk.Worker{})
	if plain.Tick != nil || plain.Snapshot != nil || plain.Restore != nil {
		t.Fatalf("plain handlers: Tick=%v Snapshot=%v Restore=%v", plain.Tick != nil, plain.Snapshot != nil, plain.Restore != nil)
	}
	ticking := s.HandlersFor(&sdk.Worker{TickInterval: time.Second})
	if ticking.Tick == nil {
		t.Fatal("configured Tick callback is absent")
	}
	store, err := storage.NewLocal(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	checkpointed := s.HandlersFor(&sdk.Worker{CheckpointStore: store})
	if checkpointed.Tick != nil || checkpointed.Snapshot == nil || checkpointed.Restore == nil {
		t.Fatalf("checkpoint handlers: Tick=%v Snapshot=%v Restore=%v", checkpointed.Tick != nil, checkpointed.Snapshot != nil, checkpointed.Restore != nil)
	}
}

func TestGoAndPythonHandlersReturnTheSameOutput(t *testing.T) {
	for _, language := range []string{"go", "python"} {
		t.Run(language, func(t *testing.T) {
			s := NewServer()
			ts := httptest.NewServer(s)
			defer ts.Close()
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			done := make(chan error, 1)
			if language == "go" {
				client := &Client{BaseURL: ts.URL, HTTP: ts.Client()}
				go func() {
					done <- client.Run(ctx, Adapters{OnRecord: func(_ context.Context, inv Invocation) ([]Output, error) {
						var n int
						_ = json.Unmarshal(inv.Record.Value, &n)
						value, _ := json.Marshal(n * 2)
						return []Output{{Channel: "output", Key: inv.Record.Key, Value: value}}, nil
					}})
				}()
			} else {
				_, file, _, _ := runtime.Caller(0)
				script := filepath.Join(filepath.Dir(file), "..", "..", "clients", "python", "handler_fixture.py")
				cmd := exec.CommandContext(ctx, "python3", script, ts.URL)
				go func() { done <- cmd.Run() }()
			}
			deadline := time.Now().Add(5 * time.Second)
			for {
				s.mu.Lock()
				ready := s.ready
				s.mu.Unlock()
				if ready {
					break
				}
				if time.Now().After(deadline) {
					t.Fatal("client did not become ready")
				}
				time.Sleep(time.Millisecond)
			}
			value, _ := json.Marshal(21)
			reply, err := s.Invoke(ctx, Invocation{Kind: "record", Record: &Record{Channel: "input", Key: "k", Value: value}})
			if err != nil {
				t.Fatal(err)
			}
			if len(reply.Outputs) != 1 || string(reply.Outputs[0].Value) != "42" || reply.Outputs[0].Key != "k" {
				t.Fatalf("outputs %+v", reply.Outputs)
			}
			if _, err := s.Invoke(ctx, Invocation{Kind: "drain"}); err != nil {
				t.Fatalf("optional drain callback: %v", err)
			}
			cancel()
			select {
			case <-done:
			case <-time.After(2 * time.Second):
				t.Fatal("client did not stop")
			}
		})
	}
}
