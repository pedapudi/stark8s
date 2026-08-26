package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/pedapudi/stark8s/api/v1alpha1"
	"github.com/pedapudi/stark8s/pkg/coordinator"
	"github.com/pedapudi/stark8s/pkg/sdk"
)

// channels mirrors workload.yaml.
func channels() []v1alpha1.Channel {
	return []v1alpha1.Channel{
		{Name: "calib", From: "plan", To: "scan",
			Partitioning: v1alpha1.Partitioning{Mode: v1alpha1.PartitionBroadcast, Partitions: 1}},
		{Name: "tiles", From: "plan", To: "scan",
			Partitioning: v1alpha1.Partitioning{Mode: v1alpha1.PartitionRoundRobin, Partitions: 8}},
		{Name: "hits", From: "scan", To: "reduce",
			Partitioning: v1alpha1.Partitioning{Mode: v1alpha1.PartitionHash, Partitions: 4},
			Delivery:     v1alpha1.DeliveryMaterialized},
		{Name: "regions", From: "reduce"},
	}
}

type harness struct {
	t   *testing.T
	co  *coordinator.Coordinator
	srv *httptest.Server
	ctx context.Context
	wg  sync.WaitGroup
}

func newHarness(t *testing.T) (*harness, func()) {
	t.Helper()
	seg := httptest.NewServer(nil)
	co := coordinator.New(strings.TrimPrefix(seg.URL, "http://"))
	seg.Config.Handler = coordinator.SegmentHandler(co)
	co.Configure(channels())
	srv := httptest.NewServer(coordinator.Handler(co))
	ctx, cancel := context.WithCancel(context.Background())
	h := &harness{t: t, co: co, srv: srv, ctx: ctx}
	return h, func() {
		cancel()
		h.wg.Wait()
		srv.Close()
		seg.Close()
	}
}

func (h *harness) run(op, instance string, inbound, outbound []string, hs sdk.Handlers) {
	w := &sdk.Worker{
		Coordinator:   h.srv.URL,
		Operation:     op,
		Instance:      instance,
		Inbound:       inbound,
		Outbound:      outbound,
		SegmentDir:    h.t.TempDir(),
		SegmentListen: "127.0.0.1:0",
	}
	h.wg.Add(1)
	go func() {
		defer h.wg.Done()
		if err := w.Run(h.ctx, hs); err != nil && h.ctx.Err() == nil {
			h.t.Errorf("%s: %v", instance, err)
		}
	}()
}

func (h *harness) waitComplete(op string) {
	h.t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		for _, om := range h.co.Metrics().Operations {
			if om.Name == op && om.Complete {
				return
			}
		}
		time.Sleep(20 * time.Millisecond)
	}
	h.t.Fatalf("operation %s did not complete: %+v", op, h.co.Metrics())
}

// TestTileScanFansOutAndGathers runs the whole graph in one process against
// a real coordinator and checks the reduction against the planted signal.
func TestTileScanFansOutAndGathers(t *testing.T) {
	h, stop := newHarness(t)
	defer stop()

	// One replica of the source, or the tile list would be produced twice.
	h.run("plan", "plan-0", nil, []string{"calib", "tiles"}, planHandlers())
	// Three scan replicas share sixteen tiles and each receives the whole
	// broadcast calibration table.
	for _, inst := range []string{"scan-0", "scan-1", "scan-2"} {
		h.run("scan", inst, []string{"calib", "tiles"}, []string{"hits"}, scanHandlers())
	}
	for _, inst := range []string{"reduce-0", "reduce-1"} {
		h.run("reduce", inst, []string{"hits"}, []string{"regions"}, reduceHandlers())
	}

	// The controller's role: seal an operation's outputs once it completes.
	h.waitComplete("plan")
	for _, ch := range []string{"calib", "tiles"} {
		if err := h.co.Seal(ch); err != nil {
			t.Fatal(err)
		}
	}
	h.waitComplete("scan")
	if err := h.co.Seal("hits"); err != nil {
		t.Fatal(err)
	}
	h.waitComplete("reduce")

	recs, _, err := h.co.Records("regions", "", 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	got := map[string]bandSummary{}
	for _, r := range recs {
		var b bandSummary
		raw, err := json.Marshal(r.Value)
		if err != nil {
			t.Fatal(err)
		}
		if err := json.Unmarshal(raw, &b); err != nil {
			t.Fatal(err)
		}
		if _, dup := got[r.Key]; dup {
			t.Fatalf("band %s summarised twice: %+v", r.Key, recs)
		}
		got[r.Key] = b
	}

	// Hand-checked expectation. The grid is 32x32 split into four column
	// bands of eight columns, so each band covers 32*8 = 256 cells; every
	// cell must be scanned exactly once. The detected cells are exactly the
	// planted anomalies, because every amplitude is at least 30 and the
	// baseline variation is at most 4, against a threshold of 20.
	want := map[string]bandSummary{
		"band-0": {Scanned: 256, Hits: 2, Cells: []string{"3,1", "5,4"}},
		"band-1": {Scanned: 256, Hits: 1, Cells: []string{"9,10"}},
		"band-2": {Scanned: 256, Hits: 2, Cells: []string{"17,19", "20,22"}},
		"band-3": {Scanned: 256, Hits: 2, Cells: []string{"28,30", "31,31"}},
	}
	if len(got) != len(want) {
		t.Fatalf("got %d band summaries, want %d: %+v", len(got), len(want), got)
	}
	for name, w := range want {
		g, ok := got[name]
		if !ok {
			t.Fatalf("no summary for %s: %+v", name, got)
		}
		if g.Scanned != w.Scanned || g.Hits != w.Hits || fmt.Sprint(g.Cells) != fmt.Sprint(w.Cells) {
			t.Errorf("%s = %+v, want %+v", name, g, w)
		}
	}

	// Every anomaly was found and nothing else was, across all bands.
	total := 0
	for _, g := range got {
		total += g.Hits
	}
	if total != len(anomalies) {
		t.Errorf("found %d cells, want the %d planted", total, len(anomalies))
	}

	// One record per tile on the shuffle, not one per cell, and nothing
	// left behind on any channel.
	for _, cm := range h.co.Metrics().Channels {
		switch cm.Name {
		case "hits":
			if cm.Produced != int64(len(tiles())) {
				t.Errorf("hits carried %d records, want one per tile (%d)", cm.Produced, len(tiles()))
			}
		case "tiles":
			if cm.Produced != int64(len(tiles())) {
				t.Errorf("tiles carried %d records, want %d", cm.Produced, len(tiles()))
			}
		}
		if cm.Name != "regions" && (cm.Pending != 0 || cm.InFlight != 0 || cm.Lost != 0) {
			t.Errorf("channel %s not drained: %+v", cm.Name, cm)
		}
	}
}

// TestScanBuffersTilesUntilCalibrationArrives pins the ordering hazard the
// example works around: nothing orders one inbound channel against another,
// so a tile can be delivered before the broadcast table it needs.
func TestScanBuffersTilesUntilCalibrationArrives(t *testing.T) {
	hs := scanHandlers()
	w := &sdk.Worker{Operation: "scan", Instance: "scan-0"}
	ctx := context.Background()

	tileJSON, err := json.Marshal(tile{Row0: 0, Row1: tileSize, Col0: 0, Col1: tileSize})
	if err != nil {
		t.Fatal(err)
	}
	// A tile before the table: buffered, and emitting nothing means Emit is
	// never called, so the nil Worker plumbing is never exercised.
	if err := hs.OnRecord(ctx, w, sdk.Record{Channel: "tiles", Value: tileJSON}); err != nil {
		t.Fatalf("tile before calibration: %v", err)
	}
	if err := hs.OnDrain(ctx, w); err == nil {
		t.Fatal("drain with an unserviced tile should fail")
	}
}
