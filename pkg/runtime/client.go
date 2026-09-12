package runtime

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
)

// Handler processes one invocation and returns its output records.
type Handler func(context.Context, Invocation) ([]Output, error)

// SnapshotHandler returns JSON application state or a JSON document naming
// framework-owned durable objects.
type SnapshotHandler func(context.Context, Invocation) (json.RawMessage, error)

// Adapters names each supported callback independently.
type Adapters struct {
	Source   Handler
	OnRecord Handler
	OnEpoch  Handler
	OnDrain  Handler
	OnTick   Handler
	Snapshot SnapshotHandler
	Restore  Handler
}

// Client runs application callbacks against a local runtime.
type Client struct {
	BaseURL string
	HTTP    *http.Client
}

func (c *Client) do(ctx context.Context, method, path string, in, out any) error {
	var body bytes.Buffer
	if in != nil {
		if err := json.NewEncoder(&body).Encode(in); err != nil {
			return err
		}
	}
	req, err := http.NewRequestWithContext(ctx, method, c.BaseURL+path, &body)
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	client := c.HTTP
	if client == nil {
		client = http.DefaultClient
	}
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 300 {
		return fmt.Errorf("%s %s: %s", method, path, resp.Status)
	}
	if out != nil {
		return json.NewDecoder(resp.Body).Decode(out)
	}
	return nil
}

// Run connects, declares readiness, and processes callbacks serially.
func (c *Client) Run(ctx context.Context, a Adapters) error {
	var sess session
	if err := c.do(ctx, http.MethodPost, PathConnect, struct{}{}, &sess); err != nil {
		return err
	}
	if err := c.do(ctx, http.MethodPost, PathReady, sess, nil); err != nil {
		return err
	}
	for {
		var inv Invocation
		path := PathInvocations + "?sessionId=" + url.QueryEscape(sess.SessionID)
		if err := c.do(ctx, http.MethodGet, path, nil, &inv); err != nil {
			return err
		}
		reply := Reply{SessionID: sess.SessionID, InvocationID: inv.ID}
		if inv.Kind == "snapshot" {
			if a.Snapshot == nil {
				reply.Error = "no handler for snapshot"
			} else {
				state, err := a.Snapshot(ctx, inv)
				reply.State = state
				if err != nil {
					reply.Error = err.Error()
				} else if !json.Valid(state) {
					reply.Error = "snapshot state must be JSON"
				}
			}
		} else {
			h := map[string]Handler{"source": a.Source, "record": a.OnRecord, "epoch": a.OnEpoch, "drain": a.OnDrain, "tick": a.OnTick, "restore": a.Restore}[inv.Kind]
			if h == nil && (inv.Kind == "epoch" || inv.Kind == "drain") {
				reply.Outputs = nil
			} else if h == nil {
				reply.Error = "no handler for " + inv.Kind
			} else {
				outputs, err := h(ctx, inv)
				reply.Outputs = outputs
				if err != nil {
					reply.Error = err.Error()
				}
			}
		}
		if err := c.do(ctx, http.MethodPost, PathReplies, reply, nil); err != nil {
			return err
		}
	}
}
