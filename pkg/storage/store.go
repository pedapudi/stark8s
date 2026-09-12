// Package storage provides durable immutable blobs and fenced metadata commits.
package storage

import (
	"bytes"
	"context"
	"errors"
)

var (
	// ErrNotFound means that the requested object does not exist.
	ErrNotFound = errors.New("object not found")
	// ErrConflict means that an immutable key has different contents or a
	// conditional write used a stale version.
	ErrConflict = errors.New("storage conflict")
	// ErrFenced means that a replacement writer has claimed the checkpoint.
	ErrFenced = errors.New("writer fenced")
)

// Object is a stored byte sequence and the version required to replace it.
type Object struct {
	Bytes   []byte
	Version string
}

// Store persists immutable segment bytes and conditionally updated metadata.
// Keys use slash-separated paths and must not begin with a slash.
type Store interface {
	PutImmutable(context.Context, string, []byte) error
	Get(context.Context, string) (Object, error)
	CompareAndSwap(context.Context, string, string, []byte) (string, error)
	Delete(context.Context, string) error
}

// BoundedGetter reads an object while limiting the bytes allocated for its
// body. Callers handling untrusted or workload-sized objects should require
// this interface instead of calling Store.Get and checking the length later.
type BoundedGetter interface {
	GetBounded(context.Context, string, int64) (Object, error)
}

// ErrObjectTooLarge means an object exceeded the caller's byte limit.
var ErrObjectTooLarge = errors.New("object exceeds byte limit")

// PutImmutable accepts an identical retry and rejects different bytes under
// the same key.
func PutImmutable(ctx context.Context, store Store, key string, body []byte) error {
	err := store.PutImmutable(ctx, key, body)
	if !errors.Is(err, ErrConflict) {
		return err
	}
	got, getErr := store.Get(ctx, key)
	if getErr == nil && bytes.Equal(got.Bytes, body) {
		return nil
	}
	return err
}
