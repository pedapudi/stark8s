package coordinator

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"sort"
	"strconv"
	"time"

	"github.com/pedapudi/stark8s/api/graph"
	"github.com/pedapudi/stark8s/web"
)

// Handler exposes the coordinator's control API (the paths declared in
// api.go) over HTTP. Serve it on ControlPort.
func Handler(co *Coordinator) http.Handler {
	return HandlerForGraph(co, "")
}

// HandlerForGraph exposes the coordinator API and labels exported metrics
// with the workload graph name.
func HandlerForGraph(co *Coordinator, graphName string) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("PUT "+PathTopology, func(w http.ResponseWriter, r *http.Request) {
		var specs []graph.Channel
		if err := json.NewDecoder(r.Body).Decode(&specs); err != nil {
			http.Error(w, err.Error(), 400)
			return
		}
		co.Configure(specs)
		w.WriteHeader(204)
	})
	mux.HandleFunc("GET "+PathTopology, func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, co.Topology())
	})
	mux.HandleFunc("PUT "+PathOperations, func(w http.ResponseWriter, r *http.Request) {
		var specs []OperationSpec
		if err := json.NewDecoder(r.Body).Decode(&specs); err != nil {
			http.Error(w, err.Error(), 400)
			return
		}
		co.SetOperations(specs)
		w.WriteHeader(204)
	})
	mux.HandleFunc("GET "+PathMetrics, func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, co.Metrics())
	})
	mux.HandleFunc("GET "+PathPrometheusMetrics, func(w http.ResponseWriter, r *http.Request) {
		writePrometheus(w, graphName, co.Metrics())
	})
	mux.HandleFunc("GET "+PathHealth, func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(204)
	})
	// The editor. A literal path, so it cannot shadow the channel routes
	// below: a channel named "editor" is addressed at /channels/editor/... and
	// is unaffected. Only GET is registered, and the handler reads nothing
	// from the request, so serving the page adds no way to change anything.
	mux.HandleFunc("GET "+PathEditor, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		// The page carries the workload's observed state, which changes while
		// it is open, so an intermediary must not hold on to a copy.
		w.Header().Set("Cache-Control", "no-store")
		_, _ = w.Write(web.Editor)
	})
	mux.HandleFunc("POST "+PathRegister, func(w http.ResponseWriter, r *http.Request) {
		var reg PodRegistration
		if err := json.NewDecoder(r.Body).Decode(&reg); err != nil {
			http.Error(w, err.Error(), 400)
			return
		}
		fail(w, co.Register(reg))
	})
	mux.HandleFunc("POST "+PathSourceDone, func(w http.ResponseWriter, r *http.Request) {
		var reg PodRegistration
		if err := json.NewDecoder(r.Body).Decode(&reg); err != nil {
			http.Error(w, err.Error(), 400)
			return
		}
		fail(w, co.SourceDone(reg))
	})
	mux.HandleFunc("POST "+PathOperations+"/{op}"+SuffixEpochDone, func(w http.ResponseWriter, r *http.Request) {
		epoch, _ := strconv.Atoi(r.URL.Query().Get("epoch"))
		fail(w, co.OperationEpochDoneSession(r.PathValue("op"), r.URL.Query().Get("pod"), r.Header.Get(IncarnationHeader), int32(epoch)))
	})
	mux.HandleFunc("GET "+PathReleased, func(w http.ResponseWriter, r *http.Request) {
		out, err := co.ReleasedSession(r.Header.Get(OperationHeader), r.URL.Query().Get("pod"), r.Header.Get(IncarnationHeader))
		if err != nil {
			fail(w, err)
			return
		}
		writeJSON(w, out)
	})
	ch := PathChannels + "/{c}"
	mux.HandleFunc("POST "+ch+SuffixSegments, func(w http.ResponseWriter, r *http.Request) {
		var anns []SegmentAnnouncement
		if err := json.NewDecoder(r.Body).Decode(&anns); err != nil {
			http.Error(w, err.Error(), 400)
			return
		}
		fail(w, co.AnnounceSession(r.PathValue("c"), r.Header.Get(OperationHeader), r.URL.Query().Get("pod"), r.Header.Get(IncarnationHeader), anns))
	})
	mux.HandleFunc("GET "+ch+SuffixConsume, func(w http.ResponseWriter, r *http.Request) {
		max, _ := strconv.Atoi(r.URL.Query().Get("max"))
		if max <= 0 {
			max = 100
		}
		resp, err := co.ConsumeSession(r.PathValue("c"), r.Header.Get(OperationHeader), r.URL.Query().Get("pod"), r.Header.Get(IncarnationHeader), max)
		if err != nil {
			fail(w, err)
			return
		}
		writeJSON(w, resp)
	})
	mux.HandleFunc("POST "+ch+SuffixAck, func(w http.ResponseWriter, r *http.Request) {
		var acks []SegmentAck
		if err := json.NewDecoder(r.Body).Decode(&acks); err != nil {
			http.Error(w, err.Error(), 400)
			return
		}
		fail(w, co.AckSession(r.PathValue("c"), r.Header.Get(OperationHeader), r.URL.Query().Get("pod"), r.Header.Get(IncarnationHeader), acks))
	})
	mux.HandleFunc("POST "+ch+SuffixNack, func(w http.ResponseWriter, r *http.Request) {
		var acks []SegmentAck
		if err := json.NewDecoder(r.Body).Decode(&acks); err != nil {
			http.Error(w, err.Error(), 400)
			return
		}
		fail(w, co.NackSession(r.PathValue("c"), r.Header.Get(OperationHeader), r.URL.Query().Get("pod"), r.Header.Get(IncarnationHeader), acks))
	})
	mux.HandleFunc("POST "+ch+SuffixSeal, func(w http.ResponseWriter, r *http.Request) {
		fail(w, co.Seal(r.PathValue("c")))
	})
	mux.HandleFunc("POST "+ch+SuffixEpochDone, func(w http.ResponseWriter, r *http.Request) {
		epoch, _ := strconv.Atoi(r.URL.Query().Get("epoch"))
		fail(w, co.EpochDoneSession(r.PathValue("c"), r.Header.Get(OperationHeader), r.URL.Query().Get("pod"), r.Header.Get(IncarnationHeader), int32(epoch)))
	})
	mux.HandleFunc("POST "+ch+SuffixRecords, func(w http.ResponseWriter, r *http.Request) {
		var recs []Record
		if err := json.NewDecoder(r.Body).Decode(&recs); err != nil {
			http.Error(w, err.Error(), 400)
			return
		}
		fail(w, co.Produce(r.PathValue("c"), r.Header.Get(OperationHeader), recs))
	})
	mux.HandleFunc("GET "+ch+SuffixRecords, func(w http.ResponseWriter, r *http.Request) {
		q := r.URL.Query()
		after, _ := strconv.Atoi(q.Get("after"))
		wait, _ := time.ParseDuration(q.Get("wait"))
		if wait > 5*time.Minute {
			wait = 5 * time.Minute
		}
		recs, next, err := co.Records(r.PathValue("c"), q.Get("key"), after, wait)
		if err != nil {
			fail(w, err)
			return
		}
		w.Header().Set(RecordsNextHeader, strconv.Itoa(next))
		writeJSON(w, recs)
	})
	return mux
}

func writePrometheus(w http.ResponseWriter, graph string, metrics Metrics) {
	w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
	channels := append([]ChannelMetrics(nil), metrics.Channels...)
	operations := append([]OperationMetrics(nil), metrics.Operations...)
	sort.Slice(channels, func(i, j int) bool { return channels[i].Name < channels[j].Name })
	sort.Slice(operations, func(i, j int) bool { return operations[i].Name < operations[j].Name })
	channelMetrics := []struct {
		name  string
		help  string
		kind  string
		value func(ChannelMetrics) int64
	}{
		{"stark8s_channel_produced_records_total", "Records produced on a channel.", "counter", func(m ChannelMetrics) int64 { return m.Produced }},
		{"stark8s_channel_acknowledged_records_total", "Consumer record acknowledgements accepted for a channel.", "counter", func(m ChannelMetrics) int64 { return m.Acknowledged }},
		{"stark8s_channel_pending_records", "Records waiting for delivery on a channel.", "gauge", func(m ChannelMetrics) int64 { return m.Pending }},
		{"stark8s_channel_in_flight_records", "Delivered records awaiting acknowledgement on a channel.", "gauge", func(m ChannelMetrics) int64 { return m.InFlight }},
		{"stark8s_channel_lost_records_total", "Records lost with an unavailable segment holder.", "counter", func(m ChannelMetrics) int64 { return m.Lost }},
		{"stark8s_channel_overflowed_records_total", "Records diverted or dropped at a feedback bound.", "counter", func(m ChannelMetrics) int64 { return m.Overflowed }},
		{"stark8s_channel_committed_epoch", "Current committed synchronous feedback epoch.", "gauge", func(m ChannelMetrics) int64 { return int64(m.Epoch) }},
	}
	for _, metric := range channelMetrics {
		fmt.Fprintf(w, "# HELP %s %s\n# TYPE %s %s\n", metric.name, metric.help, metric.name, metric.kind)
		for _, channel := range channels {
			fmt.Fprintf(w, "%s{graph=%q,channel=%q} %d\n", metric.name, graph, channel.Name, metric.value(channel))
		}
	}
	for _, metric := range []struct {
		name  string
		help  string
		value func(OperationMetrics) int64
	}{
		{"stark8s_operation_runnable_tasks", "Runnable partition tasks for an operation.", func(m OperationMetrics) int64 { return int64(m.RunnableTasks) }},
		{"stark8s_operation_live_pods", "Registered live pods for an operation.", func(m OperationMetrics) int64 { return int64(m.LivePods) }},
	} {
		fmt.Fprintf(w, "# HELP %s %s\n# TYPE %s gauge\n", metric.name, metric.help, metric.name)
		for _, operation := range operations {
			fmt.Fprintf(w, "%s{graph=%q,operation=%q} %d\n", metric.name, graph, operation.Name, metric.value(operation))
		}
	}
}

// SegmentHandler serves the segments the coordinator holds for external
// producers: GET /segments/{id} -> []Record. Serve it on SegmentPort.
func SegmentHandler(co *Coordinator) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /segments/{id}", func(w http.ResponseWriter, r *http.Request) {
		recs, ok := co.Segment(r.PathValue("id"))
		if !ok {
			http.Error(w, "segment not found", 404)
			return
		}
		writeJSON(w, recs)
	})
	return mux
}

func writeJSON(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(v)
}

func fail(w http.ResponseWriter, err error) {
	if err == nil {
		w.WriteHeader(204)
		return
	}
	var ce *Error
	if errors.As(err, &ce) {
		http.Error(w, ce.Msg, ce.Status)
		return
	}
	http.Error(w, err.Error(), 500)
}
