package sdk

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/pedapudi/stark8s/api/graph"
	"github.com/pedapudi/stark8s/pkg/coordinator"
)

// TestSlowReaderPreservesEveryRecord qualifies the current serial handler
// contract under backpressure. The elapsed-time assertion establishes that
// acknowledgement waits for record handling to finish.
func TestSlowReaderPreservesEveryRecord(t *testing.T) {
	const (
		records = 120
		delay   = 2 * time.Millisecond
	)
	h, stop := newHarness(t, []graph.Channel{
		{Name: "input", From: "source", To: "reader", Partitioning: graph.Partitioning{Mode: graph.PartitionRoundRobin, Partitions: 8}},
		{Name: "count", From: "reader"},
	})
	defer stop()

	h.run(h.worker("source", "source-0", nil, []string{"input"}), Handlers{
		Source: func(_ context.Context, w *Worker) error {
			for i := 0; i < records; i++ {
				if err := w.Emit("input", fmt.Sprint(i), i); err != nil {
					return err
				}
			}
			return nil
		},
	})
	var seen atomic.Int64
	started := time.Now()
	h.run(h.worker("reader", "reader-0", []string{"input"}, []string{"count"}), Handlers{
		OnRecord: func(_ context.Context, _ *Worker, _ Record) error {
			time.Sleep(delay)
			seen.Add(1)
			return nil
		},
		OnDrain: func(_ context.Context, w *Worker) error {
			return w.Emit("count", "records", seen.Load())
		},
	})
	h.waitComplete("source")
	if err := h.co.Seal("input"); err != nil {
		t.Fatal(err)
	}
	h.waitComplete("reader")

	if got := seen.Load(); got != records {
		t.Fatalf("slow reader handled %d records, want %d", got, records)
	}
	if elapsed := time.Since(started); elapsed < records*delay {
		t.Fatalf("reader completed in %s, before %s of handler work could finish", elapsed, records*delay)
	}
	for _, metric := range h.co.Metrics().Channels {
		if metric.Name == "input" && (metric.Pending != 0 || metric.InFlight != 0 || metric.Lost != 0) {
			t.Fatalf("input did not drain cleanly: %+v", metric)
		}
	}
}

type requestCounter struct {
	base  http.RoundTripper
	calls atomic.Int64
}

func (c *requestCounter) RoundTrip(request *http.Request) (*http.Response, error) {
	c.calls.Add(1)
	return c.base.RoundTrip(request)
}

func BenchmarkEmitAndFlush(b *testing.B) {
	cases := []struct {
		name       string
		records    int
		valueBytes int
		partitions int32
	}{
		{name: "small_records", records: 83886, valueBytes: 100, partitions: 1},
		{name: "large_payloads", records: 83, valueBytes: 100000, partitions: 1},
		{name: "many_partitions", records: 10000, valueBytes: 100, partitions: 1024},
	}
	for _, benchmark := range cases {
		b.Run(benchmark.name, func(b *testing.B) {
			payload := strings.Repeat("x", benchmark.valueBytes)
			var totalRequests, totalDisk int64
			started := time.Now()
			for iteration := 0; iteration < b.N; iteration++ {
				segmentServer := httptest.NewServer(nil)
				co := coordinator.New(strings.TrimPrefix(segmentServer.URL, "http://"))
				segmentServer.Config.Handler = coordinator.SegmentHandler(co)
				co.Configure([]graph.Channel{{
					Name: "records", From: "source", To: "sink",
					Partitioning: graph.Partitioning{Mode: graph.PartitionRoundRobin, Partitions: benchmark.partitions},
				}})
				apiServer := httptest.NewServer(coordinator.Handler(co))
				directory := b.TempDir()
				counter := &requestCounter{base: http.DefaultTransport}
				worker := &Worker{
					Coordinator: apiServer.URL, Operation: "source", Instance: fmt.Sprintf("source-%d", iteration),
					Outbound: []string{"records"}, SegmentDir: directory, SegmentListen: "127.0.0.1:0",
					client: &http.Client{Transport: counter, Timeout: 60 * time.Second},
				}
				worker.init()
				if err := worker.loadTopology(); err != nil {
					b.Fatal(err)
				}
				if err := worker.register(); err != nil {
					b.Fatal(err)
				}
				for record := 0; record < benchmark.records; record++ {
					if err := worker.Emit("records", fmt.Sprint(record), payload); err != nil {
						b.Fatal(err)
					}
				}
				if err := worker.Flush(); err != nil {
					b.Fatal(err)
				}
				for _, metric := range co.Metrics().Channels {
					if metric.Name == "records" && metric.Produced != int64(benchmark.records) {
						b.Fatalf("produced %d records, want %d", metric.Produced, benchmark.records)
					}
				}
				err := filepath.Walk(directory, func(_ string, info os.FileInfo, err error) error {
					if err == nil && !info.IsDir() {
						totalDisk += info.Size()
					}
					return err
				})
				if err != nil {
					b.Fatal(err)
				}
				totalRequests += counter.calls.Load()
				apiServer.Close()
				segmentServer.Close()
			}
			elapsed := time.Since(started).Seconds()
			totalRecords := float64(benchmark.records * b.N)
			b.ReportMetric(totalRecords/elapsed, "records/s")
			b.ReportMetric(float64(benchmark.records*benchmark.valueBytes*b.N)/(1<<20)/elapsed, "MiB/s")
			b.ReportMetric(float64(totalRequests)/float64(b.N), "requests/op")
			b.ReportMetric(float64(totalDisk)/float64(b.N)/(1<<20), "disk-MiB/op")
		})
	}
}
