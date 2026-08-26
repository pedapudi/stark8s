// Command tilescan is a fan-out/gather workload: a chunked scan of a large
// two-dimensional array, followed by a reduction over the chunk results.
//
// This is the commonest shape of a task-parallel program written against a
// futures-and-object-store runtime such as Ray or Dask. The serial version
// is a driver that holds a big array, chops it into chunks, submits one
// task per chunk with a shared calibration table, waits for all of them,
// and reassembles the answers:
//
//	shared = put(background_profile)        # one copy in the object store
//	jobs   = [scan.remote(tile, shared) for tile in tiles]
//	hits   = concat(get(jobs))              # the gather
//	report = summarise(hits)
//
//	plan   (source)  emits the tile descriptors — coordinates, not data —
//	                 on "tiles", and the shared calibration table on
//	                 "calib", a Broadcast channel that every scan replica
//	                 receives in full.
//	scan             reads its own chunk of the array, subtracts the
//	                 calibration background, and emits one result record
//	                 per tile listing the cells whose residual exceeds the
//	                 detection threshold, keyed by column band.
//	reduce           gathers the per-tile results for the bands it owns and
//	                 writes one summary per band to "regions", an
//	                 externally readable channel.
//
// Three things about the translation are worth noticing, because they are
// what the futures model gives you for free and this model does not.
//
// The driver's `get(jobs)` is replaced by two things at once: the Hash
// partitioning of "hits", which decides which reducer sees which result,
// and the drain of that channel, which decides when the reduction is
// complete. There is no list of futures anywhere and no process that waits
// on one.
//
// `put(shared)` becomes a Broadcast channel. Ray hands every task a pointer
// into a shared-memory object store, so the shared table costs one copy per
// node; a Broadcast channel copies it once per consuming replica. For a
// small table that is free and for a large one it is not, and there is no
// pass-by-reference in this model to fall back on.
//
// A tile is emitted as one record, not one record per cell. Cost here is
// per record, so the granularity of a record is the granularity of the
// scheduler: 16 tile results cost 16 records, while 1024 per-cell records
// would cost 1024. In Ray the same choice appears as task granularity.
//
// Because scan consumes two channels and no ordering holds between them, a
// tile can arrive before the calibration table does. The scan handler
// therefore buffers tiles until the table has arrived. A Ray task cannot
// have this problem: it names its inputs and does not start without them.
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

// The array being scanned. It is generated from its coordinates rather than
// read from storage so the example needs no input file; a real scan would
// read rows [Row0,Row1) x [Col0,Col1) of a raster, a matrix shard or a
// table range here, which is the point of shipping coordinates instead of
// data.
const (
	gridRows = 32
	gridCols = 32
	// tileSize is the side of one chunk. The grid divides into
	// (gridRows/tileSize) * (gridCols/tileSize) = 16 tiles.
	tileSize = 8
	// bandCols is the width of one output band: the reduction key. It equals
	// tileSize so that every tile falls entirely within one band.
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

// anomalies are the cells that carry an added signal. Every amplitude is
// far above the threshold, and the baseline variation below is far under
// it, so the detected set is exactly this set whatever the tiling.
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

// scanHandlers is one fan-out task per record on "tiles". Tiles that arrive
// before the calibration table are held until it does.
func scanHandlers() sdk.Handlers {
	var profile []int
	var pending []tile

	scan := func(w *sdk.Worker, t tile) error {
		res := tileResult{Scanned: (t.Row1 - t.Row0) * (t.Col1 - t.Col0)}
		for r := t.Row0; r < t.Row1; r++ {
			for c := t.Col0; c < t.Col1; c++ {
				if d := value(r, c) - profile[c]; d >= threshold {
					res.Hits = append(res.Hits, hit{Row: r, Col: c, Residual: d})
				}
			}
		}
		// One record for the whole tile: the scheduler's unit of work is the
		// record, so a tile must not be emitted cell by cell.
		return w.Emit("hits", bandOf(t.Col0), res)
	}

	return sdk.Handlers{
		OnRecord: func(ctx context.Context, w *sdk.Worker, r sdk.Record) error {
			switch r.Channel {
			case "calib":
				if err := json.Unmarshal(r.Value, &profile); err != nil {
					return err
				}
				held := pending
				pending = nil
				for _, t := range held {
					if err := scan(w, t); err != nil {
						return err
					}
				}
				return nil
			case "tiles":
				var t tile
				if err := json.Unmarshal(r.Value, &t); err != nil {
					return err
				}
				if profile == nil {
					pending = append(pending, t)
					return nil
				}
				return scan(w, t)
			}
			return nil
		},
		OnDrain: func(ctx context.Context, w *sdk.Worker) error {
			if len(pending) > 0 {
				return fmt.Errorf("drained with %d tiles still waiting for the calibration table", len(pending))
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
