package exchange

import (
	"encoding/json"
	"errors"
	"net/http"
	"strconv"

	"github.com/pedapudi/stark8s/api/v1alpha1"
)

// OperationHeader carries the calling operation's name. The exchange uses it
// to enforce that only the declared producer writes and only the declared
// consumer reads a channel. The controller's NetworkPolicy restricts which
// pods can reach the exchange at all; this header restricts which channels
// those pods may touch.
const OperationHeader = "X-Stark8s-Operation"

// Handler exposes the exchange over HTTP.
//
//	PUT  /topology                           body: []v1alpha1.Channel
//	GET  /metrics                            -> []Metrics
//	POST /channels/{c}/records               body: []Record (produce)
//	GET  /channels/{c}/consume?consumer=&max= -> ConsumeResponse
//	POST /channels/{c}/ack                   body: []uint64
//	POST /channels/{c}/seal
//	POST /channels/{c}/epoch-done?consumer=&epoch=
//	GET  /channels/{c}/log                   -> []Record (retained records)
func Handler(e *Exchange) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("PUT /topology", func(w http.ResponseWriter, r *http.Request) {
		var specs []v1alpha1.Channel
		if err := json.NewDecoder(r.Body).Decode(&specs); err != nil {
			http.Error(w, err.Error(), 400)
			return
		}
		e.Configure(specs)
		w.WriteHeader(204)
	})
	mux.HandleFunc("GET /metrics", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, e.Metrics())
	})
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(204)
	})
	mux.HandleFunc("POST /channels/{c}/records", func(w http.ResponseWriter, r *http.Request) {
		var recs []Record
		if err := json.NewDecoder(r.Body).Decode(&recs); err != nil {
			http.Error(w, err.Error(), 400)
			return
		}
		fail(w, e.Produce(r.PathValue("c"), r.Header.Get(OperationHeader), recs))
	})
	mux.HandleFunc("GET /channels/{c}/consume", func(w http.ResponseWriter, r *http.Request) {
		max, _ := strconv.Atoi(r.URL.Query().Get("max"))
		if max <= 0 {
			max = 100
		}
		resp, err := e.Consume(r.PathValue("c"), r.Header.Get(OperationHeader), r.URL.Query().Get("consumer"), max)
		if err != nil {
			fail(w, err)
			return
		}
		writeJSON(w, resp)
	})
	mux.HandleFunc("POST /channels/{c}/ack", func(w http.ResponseWriter, r *http.Request) {
		var ids []uint64
		if err := json.NewDecoder(r.Body).Decode(&ids); err != nil {
			http.Error(w, err.Error(), 400)
			return
		}
		fail(w, e.Ack(r.PathValue("c"), ids))
	})
	mux.HandleFunc("POST /channels/{c}/seal", func(w http.ResponseWriter, r *http.Request) {
		fail(w, e.Seal(r.PathValue("c")))
	})
	mux.HandleFunc("POST /channels/{c}/epoch-done", func(w http.ResponseWriter, r *http.Request) {
		epoch, _ := strconv.Atoi(r.URL.Query().Get("epoch"))
		fail(w, e.EpochDone(r.PathValue("c"), r.URL.Query().Get("consumer"), int32(epoch)))
	})
	mux.HandleFunc("GET /channels/{c}/log", func(w http.ResponseWriter, r *http.Request) {
		recs, err := e.Log(r.PathValue("c"))
		if err != nil {
			fail(w, err)
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
	var xe *Error
	if errors.As(err, &xe) {
		http.Error(w, xe.Msg, xe.Status)
		return
	}
	http.Error(w, err.Error(), 500)
}
