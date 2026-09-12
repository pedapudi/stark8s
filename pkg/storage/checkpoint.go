package storage

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"path"
)

// Checkpoint is the durable coordinator metadata envelope. State contains a
// complete coordinator snapshot. Tombstones retain segment identities whose
// deletion eligibility has committed but whose bytes may still exist.
type Checkpoint struct {
	Generation uint64          `json:"generation"`
	Writer     string          `json:"writer"`
	State      json.RawMessage `json:"state"`
	Tombstones []string        `json:"tombstones,omitempty"`
}

// Writer owns conditional commits to one checkpoint object.
type Writer struct {
	store   Store
	key     string
	id      string
	version string
	value   Checkpoint
}

// PutImmutable stores bytes in the same backend before metadata refers to them.
func (w *Writer) PutImmutable(ctx context.Context, key string, body []byte) error {
	return PutImmutable(ctx, w.store, w.scopedKey(key), body)
}

// Delete removes an object in the checkpoint's directory.
func (w *Writer) Delete(ctx context.Context, key string) error {
	return w.store.Delete(ctx, w.scopedKey(key))
}

func (w *Writer) scopedKey(key string) string {
	directory := path.Dir(w.key)
	if directory == "." {
		return key
	}
	return directory + "/" + key
}

// Claim replaces any previous writer and returns the last committed state.
// A previous Writer subsequently receives ErrFenced.
func Claim(ctx context.Context, store Store, key, id string) (*Writer, Checkpoint, error) {
	if id == "" {
		return nil, Checkpoint{}, fmt.Errorf("writer identity is required")
	}
	obj, err := store.Get(ctx, key)
	var cp Checkpoint
	var oldVersion string
	if err == nil {
		if err := json.Unmarshal(obj.Bytes, &cp); err != nil {
			return nil, Checkpoint{}, fmt.Errorf("decode checkpoint: %w", err)
		}
		oldVersion = obj.Version
	} else if !errors.Is(err, ErrNotFound) {
		return nil, Checkpoint{}, err
	}
	previous := cp
	cp.Generation++
	cp.Writer = id
	body, err := json.Marshal(cp)
	if err != nil {
		return nil, Checkpoint{}, err
	}
	newVersion, err := store.CompareAndSwap(ctx, key, oldVersion, body)
	if err != nil {
		return nil, Checkpoint{}, err
	}
	return &Writer{store: store, key: key, id: id, version: newVersion, value: cp}, previous, nil
}

// Commit synchronously replaces the checkpoint if this writer still owns it.
func (w *Writer) Commit(ctx context.Context, state json.RawMessage, tombstones []string) error {
	current, err := w.store.Get(ctx, w.key)
	if err != nil {
		return err
	}
	var stored Checkpoint
	if err := json.Unmarshal(current.Bytes, &stored); err != nil {
		return fmt.Errorf("decode checkpoint: %w", err)
	}
	if stored.Writer != w.id || stored.Generation != w.value.Generation || current.Version != w.version {
		return ErrFenced
	}
	next := stored
	next.Generation++
	next.State = append(json.RawMessage(nil), state...)
	next.Tombstones = append([]string(nil), tombstones...)
	body, err := json.Marshal(next)
	if err != nil {
		return err
	}
	newVersion, err := w.store.CompareAndSwap(ctx, w.key, w.version, body)
	if errors.Is(err, ErrConflict) {
		return ErrFenced
	}
	if err != nil {
		return err
	}
	w.version, w.value = newVersion, next
	return nil
}
