package sdk

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/pedapudi/stark8s/pkg/coordinator"
	"github.com/pedapudi/stark8s/pkg/storage"
)

type checkpointInput struct {
	Channel string                 `json:"channel"`
	Ack     coordinator.SegmentAck `json:"ack"`
}

type checkpointManifest struct {
	Boundary              uint64                                       `json:"boundary"`
	Complete              bool                                         `json:"complete,omitempty"`
	EpochCallbackComplete bool                                         `json:"epochCallbackComplete,omitempty"`
	Epoch                 int32                                        `json:"epoch"`
	StateKey              string                                       `json:"stateKey"`
	Inputs                []checkpointInput                            `json:"inputs"`
	Outputs               map[string][]coordinator.SegmentAnnouncement `json:"outputs"`
}

type checkpointSession struct {
	writer  *storage.Writer
	store   storage.Store
	prefix  string
	current checkpointManifest
	covered map[string]bool
}

func (w *Worker) checkpointKey() string {
	prefix := strings.Trim(w.CheckpointPrefix, "/")
	if prefix == "" {
		prefix = strings.Trim(w.SegmentPrefix, "/")
	}
	if prefix != "" {
		prefix += "/"
	}
	return prefix + "checkpoints/" + w.Operation + ".json"
}

func (w *Worker) startCheckpoint(ctx context.Context, h Handlers) error {
	if w.CheckpointStore == nil {
		if h.Snapshot != nil || h.Restore != nil {
			return errors.New("Snapshot and Restore require CheckpointStore")
		}
		return nil
	}
	if h.Snapshot == nil || h.Restore == nil {
		return errors.New("CheckpointStore requires both Snapshot and Restore")
	}
	writer, previous, err := storage.Claim(ctx, w.CheckpointStore, w.checkpointKey(), w.Incarnation)
	if err != nil {
		return err
	}
	s := &checkpointSession{writer: writer, store: w.CheckpointStore, prefix: strings.TrimSuffix(w.checkpointKey(), ".json"), covered: map[string]bool{}}
	w.checkpoint = s
	if len(previous.State) == 0 {
		return nil
	}
	if err := json.Unmarshal(previous.State, &s.current); err != nil {
		return fmt.Errorf("decode operation checkpoint: %w", err)
	}
	state, err := s.store.Get(ctx, s.current.StateKey)
	if err != nil {
		return fmt.Errorf("read checkpoint state: %w", err)
	}
	if err := h.Restore(ctx, state.Bytes); err != nil {
		return fmt.Errorf("restore checkpoint state: %w", err)
	}
	w.epoch = s.current.Epoch
	for _, in := range s.current.Inputs {
		s.covered[inputKey(in.Channel, in.Ack)] = true
	}
	w.unannounced = cloneOutputs(s.current.Outputs)
	return w.publish()
}

func inputKey(channel string, ack coordinator.SegmentAck) string {
	if ack.AppendID != "" {
		return channel + "\x00append\x00" + ack.AppendID
	}
	return channel + "\x00" + ack.Holder + "\x00" + ack.ID
}

func (s *checkpointSession) covers(channel string, seg coordinator.SegmentRef) bool {
	if seg.AppendID != "" && s.covered[inputKey(channel, coordinator.SegmentAck{ID: seg.ID, AppendID: seg.AppendID})] {
		return true
	}
	return s.covered[inputKey(channel, coordinator.SegmentAck{ID: seg.ID, Holder: seg.Holder})]
}

func cloneOutputs(in map[string][]coordinator.SegmentAnnouncement) map[string][]coordinator.SegmentAnnouncement {
	out := make(map[string][]coordinator.SegmentAnnouncement, len(in))
	for channel, announcements := range in {
		out[channel] = append([]coordinator.SegmentAnnouncement(nil), announcements...)
	}
	return out
}

func (w *Worker) commitCheckpoint(ctx context.Context, h Handlers, channel string, acks []coordinator.SegmentAck, complete, epochCallbackComplete bool) error {
	if err := w.materialize(); err != nil {
		return err
	}
	state, err := h.Snapshot(ctx)
	if err != nil {
		return fmt.Errorf("snapshot state: %w", err)
	}
	next := checkpointManifest{Boundary: w.checkpoint.current.Boundary + 1, Complete: complete, EpochCallbackComplete: epochCallbackComplete, Epoch: w.epoch, Outputs: cloneOutputs(w.checkpoint.current.Outputs)}
	for channel, announcements := range w.unannounced {
		next.Outputs[channel] = append(next.Outputs[channel], announcements...)
	}
	next.Inputs = append(next.Inputs, w.checkpoint.current.Inputs...)
	for _, ack := range acks {
		next.Inputs = append(next.Inputs, checkpointInput{Channel: channel, Ack: ack})
	}
	next.StateKey = fmt.Sprintf("%s/state-%d-%s", w.checkpoint.prefix, next.Boundary, w.Incarnation)
	if err := storage.PutImmutable(ctx, w.CheckpointStore, next.StateKey, state); err != nil {
		return err
	}
	body, err := json.Marshal(next)
	if err != nil {
		return err
	}
	if err := w.checkpoint.writer.Commit(ctx, body, nil); err != nil {
		return err
	}
	if w.checkpointAfterCommit != nil {
		if err := w.checkpointAfterCommit(); err != nil {
			return err
		}
	}
	w.checkpoint.current = next
	for _, in := range next.Inputs {
		w.checkpoint.covered[inputKey(in.Channel, in.Ack)] = true
	}
	if err := w.publish(); err != nil {
		return err
	}
	if len(acks) == 0 {
		return nil
	}
	if err := w.retry(ctx, func() error { return w.ack(channel, acks) }); err != nil {
		return err
	}
	if w.checkpointAfterAck != nil {
		return w.checkpointAfterAck()
	}
	return nil
}
