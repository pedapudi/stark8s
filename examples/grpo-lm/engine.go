package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"
)

// The SDK is Go and a language model is not. A worker links the SDK, and the
// thing that generates or trains runs beside it in the same pod behind a
// localhost HTTP contract. The worker stays the SDK client and keeps the loop
// protocol; the sidecar only has to answer two requests.
//
// The seam is an interface so the whole graph can be tested on CPU against a
// deterministic stub, the way examples/newsdesk tests against a fake model.
// The networked implementations below are the only ones that need a GPU.
type generator interface {
	// Generate draws n completions for one prompt under the checkpoint the
	// worker was last told to load.
	Generate(ctx context.Context, prompt string, n int, seed int64) ([]string, error)
	// Load points the generator at a new checkpoint. It is separate from
	// Generate because it is the expensive call: it is where a rollout pod
	// spends its time between steps.
	Load(ctx context.Context, uri string) error
}

type trainer interface {
	// Step applies one GRPO update and returns the URI of the checkpoint it
	// wrote, plus the objective and KL it measured.
	Step(ctx context.Context, batch []trainSample, step int32) (stepResult, error)
}

// trainSample is one completion with the advantage its group gave it. The
// tokens are not carried: the trainer re-tokenises the text, because the
// tokenizer belongs to the model and not to the graph.
type trainSample struct {
	Prompt     string  `json:"prompt"`
	Completion string  `json:"completion"`
	Advantage  float64 `json:"advantage"`
}

type stepResult struct {
	Checkpoint string  `json:"checkpoint"`
	Objective  float64 `json:"objective"`
	KL         float64 `json:"kl"`
}

// --- the sidecar clients --------------------------------------------------

type httpGenerator struct {
	base   string
	client *http.Client
}

func newHTTPGenerator(base string) *httpGenerator {
	return &httpGenerator{base: base, client: &http.Client{Timeout: 10 * time.Minute}}
}

func (g *httpGenerator) Generate(ctx context.Context, prompt string, n int, seed int64) ([]string, error) {
	var out struct {
		Completions []string `json:"completions"`
	}
	err := postJSON(ctx, g.client, g.base+"/generate", map[string]any{
		"prompt": prompt, "n": n, "seed": seed,
	}, &out)
	return out.Completions, err
}

func (g *httpGenerator) Load(ctx context.Context, uri string) error {
	return postJSON(ctx, g.client, g.base+"/load", map[string]any{"checkpoint": uri}, nil)
}

type httpTrainer struct {
	base   string
	client *http.Client
}

func newHTTPTrainer(base string) *httpTrainer {
	return &httpTrainer{base: base, client: &http.Client{Timeout: 30 * time.Minute}}
}

func (t *httpTrainer) Step(ctx context.Context, batch []trainSample, step int32) (stepResult, error) {
	var out stepResult
	err := postJSON(ctx, t.client, t.base+"/step", map[string]any{
		"step": step, "samples": batch,
	}, &out)
	return out, err
}

func postJSON(ctx context.Context, c *http.Client, url string, body any, out any) error {
	b, err := json.Marshal(body)
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, "POST", url, bytes.NewReader(b))
	if err != nil {
		return err
	}
	req.Header.Set("content-type", "application/json")
	resp, err := c.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode/100 != 2 {
		msg, _ := io.ReadAll(io.LimitReader(resp.Body, 2048))
		return fmt.Errorf("%s: %s: %s", url, resp.Status, bytes.TrimSpace(msg))
	}
	if out == nil {
		_, _ = io.Copy(io.Discard, resp.Body)
		return nil
	}
	return json.NewDecoder(resp.Body).Decode(out)
}
