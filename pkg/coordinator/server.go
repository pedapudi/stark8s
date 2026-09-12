package coordinator

import (
	"encoding/json"
	"errors"
	"net/http"
	"strconv"
	"time"

	"github.com/pedapudi/stark8s/api/v1alpha1"
	"github.com/pedapudi/stark8s/web"
)

// Handler exposes the coordinator's control API (the paths declared in
// api.go) over HTTP. Serve it on ControlPort.
func Handler(co *Coordinator) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("PUT "+PathTopology, func(w http.ResponseWriter, r *http.Request) {
		var specs []v1alpha1.Channel
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
	mux.HandleFunc("GET "+PathMetrics, func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, co.Metrics())
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
	mux.HandleFunc("GET "+PathReleased, func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, co.Released(r.URL.Query().Get("pod")))
	})
	ch := PathChannels + "/{c}"
	mux.HandleFunc("POST "+ch+SuffixSegments, func(w http.ResponseWriter, r *http.Request) {
		var anns []SegmentAnnouncement
		if err := json.NewDecoder(r.Body).Decode(&anns); err != nil {
			http.Error(w, err.Error(), 400)
			return
		}
		fail(w, co.Announce(r.PathValue("c"), r.Header.Get(OperationHeader), anns))
	})
	mux.HandleFunc("GET "+ch+SuffixConsume, func(w http.ResponseWriter, r *http.Request) {
		max, _ := strconv.Atoi(r.URL.Query().Get("max"))
		if max <= 0 {
			max = 100
		}
		resp, err := co.Consume(r.PathValue("c"), r.Header.Get(OperationHeader), r.URL.Query().Get("pod"), max)
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
		fail(w, co.Ack(r.PathValue("c"), acks))
	})
	mux.HandleFunc("POST "+ch+SuffixSeal, func(w http.ResponseWriter, r *http.Request) {
		fail(w, co.Seal(r.PathValue("c")))
	})
	mux.HandleFunc("POST "+ch+SuffixEpochDone, func(w http.ResponseWriter, r *http.Request) {
		epoch, _ := strconv.Atoi(r.URL.Query().Get("epoch"))
		fail(w, co.EpochDone(r.PathValue("c"), r.URL.Query().Get("pod"), int32(epoch)))
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
