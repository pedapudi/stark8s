// Package runtime connects the Go worker to an application over local HTTP.
package runtime

import (
	"encoding/json"
)

const (
	PathHealth      = "/healthz"
	PathConnect     = "/v1/connect"
	PathReady       = "/v1/ready"
	PathInvocations = "/v1/invocations"
	PathReplies     = "/v1/replies"
)

// Invocation is one serial application callback.
type Invocation struct {
	ID       string          `json:"invocationId"`
	Kind     string          `json:"kind"`
	Record   *Record         `json:"record,omitempty"`
	Epoch    int32           `json:"epoch,omitempty"`
	MaxEpoch int32           `json:"maxEpochs,omitempty"`
	Data     json.RawMessage `json:"data,omitempty"`
}

// Record is the language-neutral form of an SDK record.
type Record struct {
	Channel string          `json:"channel"`
	Key     string          `json:"key"`
	Value   json.RawMessage `json:"value"`
	Epoch   int32           `json:"epoch"`
}

// Output is a record emitted by an application callback.
type Output struct {
	Channel string          `json:"channel"`
	Key     string          `json:"key"`
	Value   json.RawMessage `json:"value"`
}

// Reply completes an invocation and may emit records.
type Reply struct {
	SessionID    string          `json:"sessionId"`
	InvocationID string          `json:"invocationId"`
	Outputs      []Output        `json:"outputs,omitempty"`
	State        json.RawMessage `json:"state,omitempty"`
	Error        string          `json:"error,omitempty"`
}

type session struct {
	SessionID string `json:"sessionId"`
}
