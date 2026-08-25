// Package sdk is the worker-side client for operations.
//
// A worker discovers its place in the graph from environment variables the
// controller injects into every operation pod:
//
//	STARK8S_EXCHANGE   base URL of the workload's exchange
//	STARK8S_OPERATION  this operation's name
//	STARK8S_INSTANCE   this replica's identity (the pod name)
//	STARK8S_INBOUND    comma-separated inbound channel names
//	STARK8S_OUTBOUND   comma-separated outbound channel names
//	STARK8S_FEEDBACK   comma-separated inbound channels that are feedback edges
//	STARK8S_FEEDBACK_OUT comma-separated outbound channels that are feedback edges
//
// Run drives the poll/process/ack loop and implements the completion and
// superstep protocols so application code only handles records.
package sdk

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/pedapudi/stark8s/pkg/exchange"
)

// Record is a consumed record with its source channel.
type Record struct {
	Channel string
	Key     string
	Value   json.RawMessage
	Epoch   int32
}

// Handlers are the application callbacks. All are optional.
type Handlers struct {
	// Source runs once for operations with no inbound channels. It should
	// emit everything and return; the worker then exits.
	Source func(ctx context.Context, w *Worker) error
	// OnRecord is called for each consumed record.
	OnRecord func(ctx context.Context, w *Worker, r Record) error
	// OnEpochEnd is called when every inbound feedback channel is quiescent
	// at the given epoch, before the barrier advances. Emit next-epoch
	// records here. It is called at most once per epoch.
	OnEpochEnd func(ctx context.Context, w *Worker, epoch int32) error
	// OnDrain is called once when every inbound channel is drained, just
	// before the worker exits. Emit final results here.
	OnDrain func(ctx context.Context, w *Worker) error
}

// Worker is one replica of an operation.
type Worker struct {
	Exchange    string
	Operation   string
	Instance    string
	Inbound     []string
	Outbound    []string
	feedback    map[string]bool
	feedbackOut map[string]bool

	client   *http.Client
	buffers  map[string][]exchange.Record
	epoch    int32
	maxEpoch int32
	lastDone int32
}

// FromEnv builds a Worker from the injected environment.
func FromEnv() (*Worker, error) {
	w := &Worker{
		Exchange:    os.Getenv("STARK8S_EXCHANGE"),
		Operation:   os.Getenv("STARK8S_OPERATION"),
		Instance:    os.Getenv("STARK8S_INSTANCE"),
		Inbound:     split(os.Getenv("STARK8S_INBOUND")),
		Outbound:    split(os.Getenv("STARK8S_OUTBOUND")),
		feedback:    map[string]bool{},
		feedbackOut: map[string]bool{},
		client:      &http.Client{Timeout: 30 * time.Second},
		buffers:     map[string][]exchange.Record{},
		lastDone:    -1,
	}
	for _, f := range split(os.Getenv("STARK8S_FEEDBACK")) {
		w.feedback[f] = true
	}
	for _, f := range split(os.Getenv("STARK8S_FEEDBACK_OUT")) {
		w.feedbackOut[f] = true
	}
	if w.Exchange == "" || w.Operation == "" {
		return nil, fmt.Errorf("STARK8S_EXCHANGE and STARK8S_OPERATION must be set")
	}
	if w.Instance == "" {
		w.Instance, _ = os.Hostname()
	}
	return w, nil
}

func split(s string) []string {
	if s == "" {
		return nil
	}
	var out []string
	for _, p := range strings.Split(s, ",") {
		if p = strings.TrimSpace(p); p != "" {
			out = append(out, p)
		}
	}
	return out
}

// Epoch is the current superstep (zero outside loops).
func (w *Worker) Epoch() int32 { return w.epoch }

// MaxEpochs is the loop bound of the inbound feedback channel, or zero.
func (w *Worker) MaxEpochs() int32 { return w.maxEpoch }

// Emit buffers a record for an outbound channel. Records to a feedback
// channel are stamped with the next epoch; all others with the current one.
// Buffers are flushed after each batch and before exit.
func (w *Worker) Emit(channel, key string, value any) error {
	b, err := json.Marshal(value)
	if err != nil {
		return err
	}
	epoch := w.epoch
	if w.feedbackOut[channel] {
		epoch = w.epoch + 1
	}
	w.buffers[channel] = append(w.buffers[channel], exchange.Record{Key: key, Value: b, Epoch: epoch})
	if len(w.buffers[channel]) >= 500 {
		return w.flush(channel)
	}
	return nil
}

func (w *Worker) flush(channel string) error {
	recs := w.buffers[channel]
	if len(recs) == 0 {
		return nil
	}
	delete(w.buffers, channel)
	body, _ := json.Marshal(recs)
	return w.do("POST", "/channels/"+channel+"/records", body, nil)
}

// Flush sends all buffered records.
func (w *Worker) Flush() error {
	for ch := range w.buffers {
		if err := w.flush(ch); err != nil {
			return err
		}
	}
	return nil
}

func (w *Worker) do(method, path string, body []byte, out any) error {
	var rd io.Reader
	if body != nil {
		rd = bytes.NewReader(body)
	}
	req, err := http.NewRequest(method, w.Exchange+path, rd)
	if err != nil {
		return err
	}
	req.Header.Set(exchange.OperationHeader, w.Operation)
	req.Header.Set("Content-Type", "application/json")
	resp, err := w.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 300 {
		msg, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("%s %s: %s: %s", method, path, resp.Status, strings.TrimSpace(string(msg)))
	}
	if out != nil {
		return json.NewDecoder(resp.Body).Decode(out)
	}
	return nil
}

func (w *Worker) consume(ch string, max int) (*exchange.ConsumeResponse, error) {
	var resp exchange.ConsumeResponse
	err := w.do("GET", fmt.Sprintf("/channels/%s/consume?consumer=%s&max=%d", ch, w.Instance, max), nil, &resp)
	return &resp, err
}

func (w *Worker) ack(ch string, ids []uint64) error {
	body, _ := json.Marshal(ids)
	return w.do("POST", "/channels/"+ch+"/ack", body, nil)
}

// Run executes the worker until its completion condition is met or ctx ends.
// Transient exchange errors are retried; handler errors abort the worker.
func (w *Worker) Run(ctx context.Context, h Handlers) error {
	if len(w.Inbound) == 0 {
		if h.Source == nil {
			return fmt.Errorf("operation %s has no inbound channels and no Source handler", w.Operation)
		}
		if err := h.Source(ctx, w); err != nil {
			return err
		}
		return w.retry(ctx, w.Flush)
	}
	backoff := 100 * time.Millisecond
	for ctx.Err() == nil {
		progressed := false
		allDrained := true
		allQuiet := len(w.feedback) > 0
		for _, ch := range w.Inbound {
			resp, err := w.consume(ch, 200)
			if err != nil {
				log.Printf("consume %s: %v", ch, err)
				time.Sleep(time.Second)
				allDrained, allQuiet = false, false
				continue
			}
			if w.feedback[ch] {
				w.epoch = resp.Epoch
				w.maxEpoch = resp.MaxEpochs
			}
			if len(resp.Records) > 0 {
				progressed = true
				ids := make([]uint64, 0, len(resp.Records))
				for _, r := range resp.Records {
					if len(w.feedback) == 0 {
						// Outside a loop the epoch is carried by the records
						// themselves so pipelines inside a cycle propagate it.
						w.epoch = r.Epoch
					}
					if h.OnRecord != nil {
						if err := h.OnRecord(ctx, w, Record{Channel: ch, Key: r.Key, Value: r.Value, Epoch: r.Epoch}); err != nil {
							return err
						}
					}
					ids = append(ids, r.ID)
				}
				if err := w.retry(ctx, w.Flush); err != nil {
					return err
				}
				if err := w.retry(ctx, func() error { return w.ack(ch, ids) }); err != nil {
					return err
				}
			}
			if !resp.Drained {
				allDrained = false
			}
			if w.feedback[ch] && !resp.Quiescent {
				allQuiet = false
			}
			if !w.feedback[ch] && !resp.Drained {
				// A loop cannot end an epoch while non-loop inputs still flow.
				allQuiet = false
			}
		}
		if progressed {
			backoff = 100 * time.Millisecond
			continue
		}
		if allDrained {
			if h.OnDrain != nil {
				if err := h.OnDrain(ctx, w); err != nil {
					return err
				}
			}
			return w.retry(ctx, w.Flush)
		}
		if allQuiet && w.lastDone < w.epoch {
			if h.OnEpochEnd != nil {
				if err := h.OnEpochEnd(ctx, w, w.epoch); err != nil {
					return err
				}
			}
			if err := w.retry(ctx, w.Flush); err != nil {
				return err
			}
			for ch := range w.feedback {
				ch := ch
				if err := w.retry(ctx, func() error {
					return w.do("POST", fmt.Sprintf("/channels/%s/epoch-done?consumer=%s&epoch=%d", ch, w.Instance, w.epoch), nil, nil)
				}); err != nil {
					return err
				}
			}
			w.lastDone = w.epoch
			continue
		}
		time.Sleep(backoff)
		if backoff < 2*time.Second {
			backoff *= 2
		}
	}
	return ctx.Err()
}

func (w *Worker) retry(ctx context.Context, f func() error) error {
	var err error
	for i := 0; i < 30 && ctx.Err() == nil; i++ {
		if err = f(); err == nil {
			return nil
		}
		log.Printf("exchange call failed (attempt %d): %v", i+1, err)
		time.Sleep(time.Second)
	}
	return err
}
