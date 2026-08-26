// Command tilescan scans a large two-dimensional array in chunks and reduces
// the per-chunk results into a summary.
//
// Programs that hand work to a pool of workers and then collect the answers
// are usually written with futures and a shared object store. A driver holds
// the array, computes a calibration table once, submits one task per chunk,
// waits for every task, and reassembles what comes back. Ray and Dask are the
// runtimes people write this against, and it looks like this:
//
//	shared = put(background_profile)        # one copy in the object store
//	jobs   = [scan.remote(tile, shared) for tile in tiles]
//	hits   = concat(get(jobs))              # the gather
//	report = summarise(hits)
//
// The same shape here is three operations and four channels:
//
//	plan   (source)  emits the tile descriptors on "tiles" and the shared
//	                 calibration table on "calib", a Broadcast channel that
//	                 every scan replica receives in full. A descriptor is a
//	                 rectangle of coordinates, so the array itself never
//	                 crosses a channel.
//	scan             reads its own chunk of the array, subtracts the
//	                 calibration background, and emits one result record per
//	                 tile listing the cells whose residual exceeds the
//	                 detection threshold, keyed by column band.
//	reduce           gathers the per-tile results for the bands it owns and
//	                 writes one summary per band to "regions", an externally
//	                 readable channel.
//
// # Four things the translation has to replace
//
// The driver's get(jobs) becomes two properties of a channel. The Hash
// partitioning of "hits" decides which reducer sees which result. The drain
// of that channel decides when the reduction is complete. No list of futures
// exists anywhere, and no process waits on one.
//
// put(shared) becomes a Broadcast channel, and it costs more here. Ray hands
// every task a pointer into a shared-memory object store, so one copy serves
// a whole node. A Broadcast channel gives each consuming replica its own
// copy, and this model has no pass-by-reference to fall back on. For a small
// table the difference is invisible. For a large one it is the first thing to
// measure.
//
// Task size becomes record size. Cost here is charged per record, so one
// record carries a whole tile: 16 tile results cost 16 records, where one
// record per cell would cost 1024. Deciding how much work rides in a record
// is the same decision as deciding how much work goes in a Ray task.
//
// Input readiness becomes the handler's problem. A Ray task names its inputs
// and does not start until they exist. Here scan consumes two channels,
// nothing orders one against the other, and a tile can arrive before the
// calibration table it needs. The scanner type below holds such tiles and
// scans them once the table lands.
//
// The operation is selected by the first argument: plan, scan or reduce.
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"sort"

	"github.com/pedapudi/stark8s/pkg/sdk"
)

// The array being scanned. Its cells are computed from their coordinates, so
// the example needs no input file. A real scan would open a raster, a matrix
// shard or a table range at this point and read rows [Row0,Row1) x
// [Col0,Col1) of it. That is what shipping coordinates buys: the worker
// fetches its own chunk, and the array never travels.
const (
	gridRows = 32
	gridCols = 32
	// tileSize is the side of one chunk. The grid divides into
	// (gridRows/tileSize) * (gridCols/tileSize) = 16 tiles.
	tileSize = 8
	// bandCols is the width of one output band, which is the reduction key.
	// It is set equal to tileSize so that every tile falls entirely within one
	// band, because a tile is labelled by the band of its first column.
	// TestTilesCoverTheGridOnceWithinOneBand checks that relation holds.
	bandCols = 8
	// threshold is the residual above which a cell is reported.
	threshold = 20
)

// tile is a unit of work: a half-open rectangle of the array.
type tile struct {
	Row0, Row1 int
	Col0, Col1 int
}

// hit is one detected cell.
type hit struct {
	Row      int `json:"row"`
	Col      int `json:"col"`
	Residual int `json:"residual"`
}

// tileResult is what one scan task returns: the whole tile in one record.
type tileResult struct {
	Scanned int   `json:"scanned"`
	Hits    []hit `json:"hits"`
}

// bandSummary is the reduction over all tiles of one column band.
type bandSummary struct {
	Scanned int      `json:"scanned"`
	Hits    int      `json:"hits"`
	Cells   []string `json:"cells"`
}

// anomalies are the cells that carry an added signal. The smallest amplitude
// here is 30 and the baseline variation added by value() never exceeds 4, so
// against a threshold of 20 the detected set is exactly these seven cells,
// whatever the tiling.
var anomalies = map[string]int{
	"3,1":   50,
	"5,4":   80,
	"9,10":  30,
	"17,19": 60,
	"20,22": 45,
	"28,30": 70,
	"31,31": 90,
}

func cellKey(r, c int) string { return fmt.Sprintf("%d,%d", r, c) }

// background is the calibration profile: the expected value of a cell as a
// function of its column. The driver computes it once and shares it with
// every task.
func background(c int) int { return 100 + 10*(c%4) }

// value reads one cell of the array.
func value(r, c int) int {
	v := background(c) + (r*7+c*13)%5
	return v + anomalies[cellKey(r, c)]
}

func bandOf(c int) string { return fmt.Sprintf("band-%d", c/bandCols) }

// tiles enumerates the chunks the driver fans out over.
func tiles() []tile {
	var out []tile
	for r := 0; r < gridRows; r += tileSize {
		for c := 0; c < gridCols; c += tileSize {
			out = append(out, tile{Row0: r, Row1: r + tileSize, Col0: c, Col1: c + tileSize})
		}
	}
	return out
}

// planHandlers is the driver: it publishes the shared table and the work
// items, then has nothing left to do. It must run with exactly one replica,
// since every replica of a source operation runs Source independently.
func planHandlers() sdk.Handlers {
	return sdk.Handlers{
		Source: func(ctx context.Context, w *sdk.Worker) error {
			profile := make([]int, gridCols)
			for c := range profile {
				profile[c] = background(c)
			}
			if err := w.Emit("calib", "background", profile); err != nil {
				return err
			}
			for i, t := range tiles() {
				if err := w.Emit("tiles", fmt.Sprint(i), t); err != nil {
					return err
				}
			}
			return nil
		},
	}
}

// scanTile is the work itself: subtract the shared calibration background
// from every cell of the chunk and report the cells that stand out.
//
// The profile arrives as a parameter, which makes the answer a function of
// the table the driver shared. That matters for testing. This fixture has a
// background that any worker could recompute from the column index alone, so
// a scan that ignored the shared table would still produce the same seven
// cells and nothing in the shipped numbers would give it away. Taking the
// table as an argument lets a test hand over one that is deliberately wrong
// and require the result to follow it.
func scanTile(t tile, profile []int) tileResult {
	res := tileResult{Scanned: (t.Row1 - t.Row0) * (t.Col1 - t.Col0)}
	for r := t.Row0; r < t.Row1; r++ {
		for c := t.Col0; c < t.Col1; c++ {
			if d := value(r, c) - profile[c]; d >= threshold {
				res.Hits = append(res.Hits, hit{Row: r, Col: c, Residual: d})
			}
		}
	}
	return res
}

// scanner is the state one replica of the task pool keeps: the shared
// calibration table once it has arrived, and any tiles that arrived first.
type scanner struct {
	profile []int
	pending []tile
}

// calibrate records the shared table and returns every tile that was waiting
// for it.
func (s *scanner) calibrate(profile []int) []tile {
	s.profile = profile
	held := s.pending
	s.pending = nil
	return held
}

// take records a tile and returns the tiles that can be scanned now. Until
// the calibration table arrives there is nothing to subtract, so the tile is
// held and comes back out of calibrate.
func (s *scanner) take(t tile) []tile {
	if s.profile == nil {
		s.pending = append(s.pending, t)
		return nil
	}
	return []tile{t}
}

// scanHandlers is one fan-out task per record on "tiles".
func scanHandlers() sdk.Handlers {
	var s scanner
	return sdk.Handlers{
		OnRecord: func(ctx context.Context, w *sdk.Worker, r sdk.Record) error {
			var ready []tile
			switch r.Channel {
			case "calib":
				var profile []int
				if err := json.Unmarshal(r.Value, &profile); err != nil {
					return err
				}
				ready = s.calibrate(profile)
			case "tiles":
				var t tile
				if err := json.Unmarshal(r.Value, &t); err != nil {
					return err
				}
				ready = s.take(t)
			default:
				return nil
			}
			for _, t := range ready {
				// One record carries the whole tile. The scheduler charges per
				// record, so the tile is the unit of work.
				if err := w.Emit("hits", bandOf(t.Col0), scanTile(t, s.profile)); err != nil {
					return err
				}
			}
			return nil
		},
		OnDrain: func(ctx context.Context, w *sdk.Worker) error {
			// Reaching the end of the input while still holding tiles means the
			// calibration table never arrived, and this replica's share of the
			// scan is missing from the answer. Failing here is the only way to
			// say so, because a summary that is short reads exactly like a
			// summary of cells that were scanned and found clean.
			if n := len(s.pending); n > 0 {
				return fmt.Errorf("drained with %d tiles still waiting for the calibration table", n)
			}
			return nil
		},
	}
}

// reduceHandlers is the gather. "hits" is Hash-partitioned on the band, so
// every result for a band reaches the same replica and the summary needs no
// second reduction.
func reduceHandlers() sdk.Handlers {
	bands := map[string]*bandSummary{}
	return sdk.Handlers{
		OnRecord: func(ctx context.Context, w *sdk.Worker, r sdk.Record) error {
			var res tileResult
			if err := json.Unmarshal(r.Value, &res); err != nil {
				return err
			}
			b := bands[r.Key]
			if b == nil {
				b = &bandSummary{}
				bands[r.Key] = b
			}
			b.Scanned += res.Scanned
			b.Hits += len(res.Hits)
			for _, h := range res.Hits {
				b.Cells = append(b.Cells, cellKey(h.Row, h.Col))
			}
			return nil
		},
		OnDrain: func(ctx context.Context, w *sdk.Worker) error {
			for name, b := range bands {
				sort.Strings(b.Cells)
				if err := w.Emit("regions", name, b); err != nil {
					return err
				}
				log.Printf("%s scanned=%d hits=%d cells=%v", name, b.Scanned, b.Hits, b.Cells)
			}
			return nil
		},
	}
}

func main() {
	if len(os.Args) < 2 {
		log.Fatal("usage: tilescan plan|scan|reduce")
	}
	w, err := sdk.FromEnv()
	if err != nil {
		log.Fatal(err)
	}
	var h sdk.Handlers
	switch os.Args[1] {
	case "plan":
		h = planHandlers()
	case "scan":
		h = scanHandlers()
	case "reduce":
		h = reduceHandlers()
	default:
		log.Fatalf("unknown operation %q", os.Args[1])
	}
	if err := w.Run(context.Background(), h); err != nil {
		log.Fatal(err)
	}
}
