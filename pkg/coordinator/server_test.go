package coordinator

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/pedapudi/stark8s/api/v1alpha1"
	"github.com/pedapudi/stark8s/web"
)

// editorServer starts the control API over a small workload and returns the
// server. The channel named "editor" is there on purpose: it is the name that
// would collide with the editor's own route if the route were a prefix.
func editorServer(t *testing.T) (*Coordinator, *httptest.Server) {
	t.Helper()
	co := New("coordinator:8090")
	co.Configure([]v1alpha1.Channel{
		{Name: "in", To: "map", Partitioning: v1alpha1.Partitioning{Mode: v1alpha1.PartitionRoundRobin, Partitions: 2}},
		{Name: "shuffle", From: "map", To: "reduce", Partitioning: v1alpha1.Partitioning{Mode: v1alpha1.PartitionHash, Partitions: 4}, Delivery: v1alpha1.DeliveryMaterialized},
		{Name: "editor", From: "reduce", Durability: v1alpha1.DurabilityRetained},
	})
	srv := httptest.NewServer(Handler(co))
	t.Cleanup(srv.Close)
	return co, srv
}

func get(t *testing.T, url string) (*http.Response, []byte) {
	t.Helper()
	resp, err := http.Get(url)
	if err != nil {
		t.Fatal(err)
	}
	body, err := io.ReadAll(resp.Body)
	resp.Body.Close()
	if err != nil {
		t.Fatal(err)
	}
	return resp, body
}

// TestEditorRouteServesThePage: the coordinator serves the editor itself, so
// that viewing a running workload needs a port-forward and nothing else. The
// bytes must be the embedded file exactly, because a page that is rewritten in
// flight is a page that can be broken in flight.
func TestEditorRouteServesThePage(t *testing.T) {
	_, srv := editorServer(t)
	resp, body := get(t, srv.URL+PathEditor)

	if resp.StatusCode != 200 {
		t.Fatalf("GET %s returned %d", PathEditor, resp.StatusCode)
	}
	if ct := resp.Header.Get("Content-Type"); ct != "text/html; charset=utf-8" {
		t.Errorf("content type %q, want text/html; charset=utf-8", ct)
	}
	if !bytes.Equal(body, web.Editor) {
		t.Errorf("the served page is %d bytes and the embedded file is %d", len(body), len(web.Editor))
	}
	// A sanity check on the embed itself: an empty or truncated asset would
	// still compare equal above.
	if !bytes.Contains(body, []byte("<!doctype html>")) && !bytes.Contains(body, []byte("<!DOCTYPE html>")) {
		t.Errorf("the served page does not begin like an HTML document")
	}
	if len(body) < 10000 {
		t.Errorf("the served page is %d bytes, which is too small to be the editor", len(body))
	}
}

// TestEditorRouteCannotShadowAChannel is the reason the route is a literal
// path. Channel routes are /channels/{c}/..., so a channel may legitimately be
// called "editor", and adding the page must not make that channel unreachable.
func TestEditorRouteCannotShadowAChannel(t *testing.T) {
	_, srv := editorServer(t)

	// Produce onto the channel named "editor" and read it back through the
	// records API, the whole way round over HTTP.
	rec, err := json.Marshal([]Record{{Key: "k", Value: "v"}})
	if err != nil {
		t.Fatal(err)
	}
	url := srv.URL + PathChannels + "/editor" + SuffixRecords
	req, err := http.NewRequest(http.MethodPost, url, bytes.NewReader(rec))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Content-Type", "application/json")
	// The channel declares a producer, so the caller has to name it. Without
	// this the coordinator answers 403 and the test would read a refusal as a
	// shadowed route.
	req.Header.Set(OperationHeader, "reduce")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != 204 {
		t.Fatalf("POST %s returned %d, want 204: the editor route has shadowed the channel", url, resp.StatusCode)
	}

	resp, body := get(t, url)
	if resp.StatusCode != 200 {
		t.Fatalf("GET %s returned %d", url, resp.StatusCode)
	}
	var got []Record
	if err := json.Unmarshal(body, &got); err != nil {
		t.Fatalf("GET %s did not return records: %v (%s)", url, err, truncate(body))
	}
	if len(got) != 1 || got[0].Key != "k" {
		t.Errorf("GET %s returned %+v, want the one record produced", url, got)
	}

	// And the page is still served at its own path.
	resp, body = get(t, srv.URL+PathEditor)
	if resp.StatusCode != 200 || !bytes.Contains(body, []byte("<html")) {
		t.Errorf("GET %s returned %d and %d bytes", PathEditor, resp.StatusCode, len(body))
	}
}

// TestEditorRouteIsReadOnly: serving a page widens the surface, so the route
// answers GET and nothing else. Anything that changed state through it would
// be reachable by whoever can reach the page.
func TestEditorRouteIsReadOnly(t *testing.T) {
	_, srv := editorServer(t)
	for _, method := range []string{http.MethodPost, http.MethodPut, http.MethodDelete, http.MethodPatch} {
		req, err := http.NewRequest(method, srv.URL+PathEditor, strings.NewReader("x"))
		if err != nil {
			t.Fatal(err)
		}
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		resp.Body.Close()
		if resp.StatusCode == 200 {
			t.Errorf("%s %s returned 200; the route must answer GET alone", method, PathEditor)
		}
	}
}

// TestEditorInputsAreServed pins the two endpoints the page reads, because the
// page is now a consumer of both: the graph comes from the topology once and
// the overlay from the metrics on an interval.
func TestEditorInputsAreServed(t *testing.T) {
	co, srv := editorServer(t)
	register(t, co, "map", "map-0")
	announce(t, co, "shuffle", "map", "map-0", 0, 0, 7)

	resp, body := get(t, srv.URL+PathTopology)
	if resp.StatusCode != 200 {
		t.Fatalf("GET %s returned %d", PathTopology, resp.StatusCode)
	}
	var topo []v1alpha1.Channel
	if err := json.Unmarshal(body, &topo); err != nil {
		t.Fatalf("the topology is not a channel list: %v (%s)", err, truncate(body))
	}
	if len(topo) != 3 {
		t.Fatalf("the topology holds %d channels, want 3", len(topo))
	}
	// The page builds its operation list out of the producer and consumer
	// names, so those two fields carry the graph and must survive the round
	// trip through JSON.
	byName := map[string]v1alpha1.Channel{}
	for _, c := range topo {
		byName[c.Name] = c
	}
	sh, ok := byName["shuffle"]
	if !ok {
		t.Fatal("the topology has no channel named shuffle")
	}
	if sh.From != "map" || sh.To != "reduce" {
		t.Errorf("shuffle reads from %q to %q, want map to reduce", sh.From, sh.To)
	}
	if sh.Partitioning.Mode != v1alpha1.PartitionHash || sh.Partitioning.Partitions != 4 {
		t.Errorf("shuffle carries partitioning %+v", sh.Partitioning)
	}
	if sh.Delivery != v1alpha1.DeliveryMaterialized {
		t.Errorf("shuffle carries delivery %q", sh.Delivery)
	}

	resp, body = get(t, srv.URL+PathMetrics)
	if resp.StatusCode != 200 {
		t.Fatalf("GET %s returned %d", PathMetrics, resp.StatusCode)
	}
	var m Metrics
	if err := json.Unmarshal(body, &m); err != nil {
		t.Fatalf("the metrics are not a Metrics: %v (%s)", err, truncate(body))
	}
	if len(m.Channels) != 3 {
		t.Errorf("the metrics report %d channels, want 3", len(m.Channels))
	}
	var found bool
	for _, c := range m.Channels {
		if c.Name != "shuffle" {
			continue
		}
		found = true
		// The counters the overlay draws under an edge.
		if c.Produced != 7 || c.Pending != 7 {
			t.Errorf("shuffle reports produced %d pending %d, want 7 and 7", c.Produced, c.Pending)
		}
	}
	if !found {
		t.Error("the metrics report no channel named shuffle")
	}
	// The operation figures the overlay draws under a node. livePods is the
	// one the page shows, and it is the count of pods registered here rather
	// than any replica count the controller asked for.
	var mapOp *OperationMetrics
	for i := range m.Operations {
		if m.Operations[i].Name == "map" {
			mapOp = &m.Operations[i]
		}
	}
	if mapOp == nil {
		t.Fatal("the metrics report no operation named map")
	}
	if mapOp.LivePods != 1 {
		t.Errorf("map reports %d live pods, want 1", mapOp.LivePods)
	}
}

func truncate(b []byte) string {
	if len(b) > 200 {
		return string(b[:200]) + "..."
	}
	return string(b)
}
