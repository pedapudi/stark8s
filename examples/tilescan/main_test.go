package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http/httptest"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/pedapudi/stark8s/api/v1alpha1"
	"github.com/pedapudi/stark8s/pkg/coordinator"
	"github.com/pedapudi/stark8s/pkg/sdk"
)

// allPlantedCells is every cell of the array that carries an added signal,
// in the order the summaries sort them. It is written out here rather than
// derived from the anomalies table in main.go, so that a change to that table
// makes this test fail instead of following it.
var allPlantedCells = []string{"17,19", "20,22", "28,30", "3,1", "31,31", "5,4", "9,10"}

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

	// Every planted cell was found across all bands and nothing else was.
	// The comparison is against the literal list rather than against the
	// implementation's own table of planted cells, so the test can disagree
	// with the code it is checking.
	var found []string
	for _, g := range got {
		found = append(found, g.Cells...)
	}
	sort.Strings(found)
	if fmt.Sprint(found) != fmt.Sprint(allPlantedCells) {
		t.Errorf("the run found %v, want %v", found, allPlantedCells)
	}

	// The shuffle carried one record per tile, and nothing was left behind on
	// any channel.
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

// trueProfile is the calibration table the driver shares.
func trueProfile() []int {
	profile := make([]int, gridCols)
	for c := range profile {
		profile[c] = background(c)
	}
	return profile
}

// cellsOf lists a tile result's cells in a comparable order.
func cellsOf(res tileResult) []string {
	out := make([]string, 0, len(res.Hits))
	for _, h := range res.Hits {
		out = append(out, cellKey(h.Row, h.Col))
	}
	sort.Strings(out)
	return out
}

// TestScanTileUsesTheSharedTable pins the example's headline claim: the answer
// depends on the calibration table the driver shared.
//
// The shipped fixture cannot show this on its own. Its background is a
// function of the column, so a scan that ignored the shared table and
// recomputed the background itself would produce exactly the same seven
// cells, and every assertion about the shipped numbers would still pass. The
// check therefore feeds tables that are deliberately wrong in three different
// ways and requires the result to follow each one.
func TestScanTileUsesTheSharedTable(t *testing.T) {
	full := tile{Row0: 0, Row1: gridRows, Col0: 0, Col1: gridCols}

	// The real table. The residual left over is the planted signal plus at
	// most four of baseline variation, so exactly the planted cells clear the
	// threshold of 20.
	if got := cellsOf(scanTile(full, trueProfile())); fmt.Sprint(got) != fmt.Sprint(allPlantedCells) {
		t.Errorf("with the shared table the scan found %v, want %v", got, allPlantedCells)
	}

	// A table of zeros leaves the whole background in every residual, so
	// every cell clears the threshold.
	zero := make([]int, gridCols)
	if res := scanTile(full, zero); len(res.Hits) != gridRows*gridCols {
		t.Errorf("with a table of zeros the scan reported %d cells, want all %d",
			len(res.Hits), gridRows*gridCols)
	}

	// A table reading higher than the largest planted signal hides
	// everything.
	high := trueProfile()
	for c := range high {
		high[c] += 100
	}
	if res := scanTile(full, high); len(res.Hits) != 0 {
		t.Errorf("with a table above every signal the scan reported %d cells, want none", len(res.Hits))
	}

	// A table wrong in one column only. Reading column 7 low by the full
	// threshold promotes all 32 of its cells and leaves the rest alone, so
	// this also shows the scan indexes the table by column.
	skewed := trueProfile()
	skewed[7] -= threshold
	got := cellsOf(scanTile(full, skewed))
	if len(got) != len(allPlantedCells)+gridRows {
		t.Errorf("with column 7 read low the scan found %d cells, want %d",
			len(got), len(allPlantedCells)+gridRows)
	}
	for _, cell := range got {
		if !strings.HasSuffix(cell, ",7") && !contains(allPlantedCells, cell) {
			t.Errorf("cell %s was reported, but only column 7 and the planted cells should be", cell)
		}
	}
}

func contains(list []string, s string) bool {
	for _, v := range list {
		if v == s {
			return true
		}
	}
	return false
}

// TestScanHoldsTilesUntilTheTableArrives pins the ordering hazard the example
// works around. Nothing orders one inbound channel against another, so a tile
// can be delivered before the broadcast table it needs.
//
// Holding the tile is only half of it. A scanner that held tiles and then
// dropped them when the table arrived would lose that work with no error
// anywhere, and the missing cells would look like cells that were scanned and
// found clean, so the release is checked too.
func TestScanHoldsTilesUntilTheTableArrives(t *testing.T) {
	var s scanner
	first := tile{Row0: 0, Row1: tileSize, Col0: 0, Col1: tileSize}
	second := tile{Row0: 0, Row1: tileSize, Col0: tileSize, Col1: 2 * tileSize}

	if ready := s.take(first); len(ready) != 0 {
		t.Fatalf("a tile arriving before the table was ready to scan: %v", ready)
	}
	if ready := s.take(second); len(ready) != 0 {
		t.Fatalf("a second tile arriving before the table was ready to scan: %v", ready)
	}

	ready := s.calibrate(trueProfile())
	if len(ready) != 2 || ready[0] != first || ready[1] != second {
		t.Fatalf("the table released %v, want both held tiles in arrival order", ready)
	}
	if n := len(s.pending); n != 0 {
		t.Errorf("%d tiles are still held after the table arrived", n)
	}

	// With the table in hand a tile is scanned straight away.
	if ready := s.take(first); len(ready) != 1 || ready[0] != first {
		t.Errorf("after the table arrived a tile released %v, want just that tile", ready)
	}
}

// TestScanDrainFailsOnUnservicedTiles: a replica that runs out of input while
// still holding tiles never received the table, and its share of the scan is
// missing from the summary.
func TestScanDrainFailsOnUnservicedTiles(t *testing.T) {
	hs := scanHandlers()
	w := &sdk.Worker{Operation: "scan", Instance: "scan-0"}
	ctx := context.Background()

	tileJSON, err := json.Marshal(tile{Row0: 0, Row1: tileSize, Col0: 0, Col1: tileSize})
	if err != nil {
		t.Fatal(err)
	}
	// The tile is held, so nothing is emitted and the worker's channel
	// plumbing is never touched.
	if err := hs.OnRecord(ctx, w, sdk.Record{Channel: "tiles", Value: tileJSON}); err != nil {
		t.Fatalf("tile before calibration: %v", err)
	}
	if err := hs.OnDrain(ctx, w); err == nil {
		t.Fatal("drain with an unserviced tile should fail")
	}
}

// TestTilesCoverTheGridOnceWithinOneBand pins the relation the reduction key
// depends on. A tile is labelled by the band of its first column, so a tile
// that spanned two bands would file half its cells under the wrong one. That
// holds only while a band is at least as wide as a tile and the two divide
// the grid evenly, which is a relation between four constants and nothing
// else checks it.
func TestTilesCoverTheGridOnceWithinOneBand(t *testing.T) {
	all := tiles()
	if want := (gridRows / tileSize) * (gridCols / tileSize); len(all) != want {
		t.Fatalf("tiles() returned %d tiles, want %d", len(all), want)
	}
	covered := map[string]int{}
	for _, tl := range all {
		if first, last := bandOf(tl.Col0), bandOf(tl.Col1-1); first != last {
			t.Errorf("tile %+v spans bands %s to %s, so its label covers cells it does not own", tl, first, last)
		}
		for r := tl.Row0; r < tl.Row1; r++ {
			for c := tl.Col0; c < tl.Col1; c++ {
				covered[cellKey(r, c)]++
			}
		}
	}
	if len(covered) != gridRows*gridCols {
		t.Errorf("the tiling covers %d cells, want the whole %dx%d grid", len(covered), gridRows, gridCols)
	}
	for cell, n := range covered {
		if n != 1 {
			t.Errorf("cell %s is covered by %d tiles, want exactly 1", cell, n)
		}
	}
}
