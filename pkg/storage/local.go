package storage

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"time"
)

// Local stores objects below one directory. Rename and directory fsync make
// acknowledged writes survive a process or machine crash on filesystems that
// implement the usual fsync contract.
type Local struct {
	dir string
	mu  sync.Mutex
}

func NewLocal(dir string) (*Local, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	return &Local{dir: dir}, nil
}

func (s *Local) path(key string) (string, error) {
	clean := filepath.Clean(filepath.FromSlash(key))
	if key == "" || filepath.IsAbs(clean) || clean == ".." || strings.HasPrefix(clean, ".."+string(filepath.Separator)) {
		return "", fmt.Errorf("invalid storage key %q", key)
	}
	return filepath.Join(s.dir, clean), nil
}

func version(body []byte) string {
	sum := sha256.Sum256(body)
	return hex.EncodeToString(sum[:])
}

func (s *Local) PutImmutable(ctx context.Context, key string, body []byte) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	p, err := s.path(key)
	if err != nil {
		return err
	}
	if old, err := os.ReadFile(p); err == nil {
		if version(old) == version(body) {
			return nil
		}
		return ErrConflict
	} else if !os.IsNotExist(err) {
		return err
	}
	return writeImmutable(p, body)
}

func (s *Local) Get(ctx context.Context, key string) (Object, error) {
	return s.GetBounded(ctx, key, -1)
}

// GetBounded checks the file size before allocating its body. A negative
// limit preserves the Store.Get behavior for small metadata objects.
func (s *Local) GetBounded(ctx context.Context, key string, maxBytes int64) (Object, error) {
	if err := ctx.Err(); err != nil {
		return Object{}, err
	}
	p, err := s.path(key)
	if err != nil {
		return Object{}, err
	}
	f, err := os.Open(p)
	if os.IsNotExist(err) {
		return Object{}, ErrNotFound
	}
	if err != nil {
		return Object{}, err
	}
	defer f.Close()
	if maxBytes >= 0 {
		info, err := f.Stat()
		if err != nil {
			return Object{}, err
		}
		if info.Size() > maxBytes {
			return Object{}, fmt.Errorf("%w: object is %d bytes, limit is %d", ErrObjectTooLarge, info.Size(), maxBytes)
		}
	}
	reader := io.Reader(f)
	if maxBytes >= 0 {
		reader = io.LimitReader(f, maxBytes+1)
	}
	body, err := io.ReadAll(reader)
	if err == nil && maxBytes >= 0 && int64(len(body)) > maxBytes {
		return Object{}, fmt.Errorf("%w: body exceeds %d bytes", ErrObjectTooLarge, maxBytes)
	}
	if err != nil {
		return Object{}, err
	}
	return Object{Bytes: body, Version: version(body)}, nil
}

func (s *Local) CompareAndSwap(ctx context.Context, key, oldVersion string, body []byte) (string, error) {
	if err := ctx.Err(); err != nil {
		return "", err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	p, err := s.path(key)
	if err != nil {
		return "", err
	}
	unlock, err := s.lockKey(ctx, key)
	if err != nil {
		return "", err
	}
	defer unlock()
	old, err := os.ReadFile(p)
	switch {
	case os.IsNotExist(err) && oldVersion != "":
		return "", ErrConflict
	case err == nil && version(old) != oldVersion:
		return "", ErrConflict
	case err != nil && !os.IsNotExist(err):
		return "", err
	}
	if err := writeFile(p, body); err != nil {
		return "", err
	}
	return version(body), nil
}

func (s *Local) lockKey(ctx context.Context, key string) (func(), error) {
	sum := sha256.Sum256([]byte(key))
	directory := filepath.Join(s.dir, ".locks")
	if err := os.MkdirAll(directory, 0o755); err != nil {
		return nil, err
	}
	f, err := os.OpenFile(filepath.Join(directory, hex.EncodeToString(sum[:])+".lock"), os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		return nil, err
	}
	for {
		if err := syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err == nil {
			return func() {
				_ = syscall.Flock(int(f.Fd()), syscall.LOCK_UN)
				_ = f.Close()
			}, nil
		} else if err != syscall.EWOULDBLOCK && err != syscall.EAGAIN {
			_ = f.Close()
			return nil, err
		}
		select {
		case <-ctx.Done():
			_ = f.Close()
			return nil, ctx.Err()
		case <-time.After(10 * time.Millisecond):
		}
	}
}

func (s *Local) Delete(ctx context.Context, key string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	p, err := s.path(key)
	if err != nil {
		return err
	}
	if err := os.Remove(p); err != nil && !os.IsNotExist(err) {
		return err
	}
	return syncDir(filepath.Dir(p))
}

func writeFile(path string, body []byte) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	f, err := os.CreateTemp(filepath.Dir(path), ".write-*")
	if err != nil {
		return err
	}
	tmp := f.Name()
	defer os.Remove(tmp)
	if _, err = f.Write(body); err == nil {
		err = f.Sync()
	}
	if closeErr := f.Close(); err == nil {
		err = closeErr
	}
	if err != nil {
		return err
	}
	if err := os.Rename(tmp, path); err != nil {
		return err
	}
	return syncDir(filepath.Dir(path))
}

func writeImmutable(path string, body []byte) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	f, err := os.CreateTemp(filepath.Dir(path), ".write-*")
	if err != nil {
		return err
	}
	tmp := f.Name()
	defer os.Remove(tmp)
	if _, err = f.Write(body); err == nil {
		err = f.Sync()
	}
	if closeErr := f.Close(); err == nil {
		err = closeErr
	}
	if err != nil {
		return err
	}
	if err := os.Link(tmp, path); err != nil {
		if os.IsExist(err) {
			old, readErr := os.ReadFile(path)
			if readErr == nil && version(old) == version(body) {
				return nil
			}
			return ErrConflict
		}
		return err
	}
	return syncDir(filepath.Dir(path))
}

func syncDir(path string) error {
	d, err := os.Open(path)
	if err != nil {
		return err
	}
	defer d.Close()
	return d.Sync()
}

// WriterLock holds an advisory process lock until Close. A replacement local
// coordinator cannot start while the previous process still owns the lock.
type WriterLock struct{ file *os.File }

func (s *Local) LockWriter() (*WriterLock, error) {
	f, err := os.OpenFile(filepath.Join(s.dir, ".writer.lock"), os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		return nil, err
	}
	if err := syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		f.Close()
		return nil, fmt.Errorf("coordinator writer already active: %w", err)
	}
	return &WriterLock{file: f}, nil
}

func (l *WriterLock) Close() error {
	if l == nil || l.file == nil {
		return nil
	}
	err := syscall.Flock(int(l.file.Fd()), syscall.LOCK_UN)
	closeErr := l.file.Close()
	l.file = nil
	if err != nil {
		return err
	}
	return closeErr
}
