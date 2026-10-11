package pebblestore

import (
	"context"
	"fmt"

	"github.com/cockroachdb/pebble/v2"

	"github.com/lotannauo/toposhift/internal/store"
	"github.com/lotannauo/toposhift/internal/store/pebblekv"
)

// ClusterName returns the cluster name the store records: the name it was started
// with, with set false when it was started with none. present is false when the
// store records nothing, as a new store and one written before this key existed
// do. The value is read as stored, so a recorded name is returned even if it is
// longer than [store.MaxClusterNameLen]; one that cannot be decoded is an error
// that wraps [pebblekv.ErrValue]. After Close it returns [store.ErrClosed].
func (s *Store) ClusterName() (name string, set, present bool, err error) {
	if s.closed.Load() {
		return "", false, false, closedError("ClusterName")
	}
	raw, err := s.kv.GetMeta(metaKey(metaClusterName))
	if err != nil {
		return "", false, false, fmt.Errorf("pebblestore: ClusterName: %w", err)
	}
	if raw == nil {
		return "", false, false, nil
	}
	name, set, err = store.DecodeClusterName(raw)
	if err != nil {
		return "", false, false, fmt.Errorf("pebblestore: ClusterName: %w: %w", err, pebblekv.ErrValue)
	}
	return name, set, true, nil
}

// RecordClusterName records the cluster name the store is started with, or with
// set false that none is, in a synced commit of that key alone. A name that does
// not pass [store.CheckClusterNameLimit] is refused with an error that wraps
// [store.ErrInvalid], and nothing is written. After Close it returns
// [store.ErrClosed]. It is not part of [store.Store]. It takes the lock Write and
// Retain take, so it waits for one in progress.
//
// A commit that fails says nothing about whether the key landed. The key is read
// back by [Store.ClusterName] after the store is reopened, and recording the same
// value again is harmless.
func (s *Store) RecordClusterName(ctx context.Context, name string, set bool) error {
	if s.closed.Load() {
		return closedError("RecordClusterName")
	}
	if err := ctx.Err(); err != nil {
		return contextError("RecordClusterName", err)
	}
	if set {
		if err := store.CheckClusterNameLimit(name); err != nil {
			return fmt.Errorf("pebblestore: RecordClusterName: %w", err)
		}
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.failed != nil {
		return s.failedError("RecordClusterName")
	}
	b := s.kv.NewBatch()
	defer func() { _ = b.Close() }()
	if err := b.Set(metaKey(metaClusterName), store.AppendClusterName(nil, name, set), nil); err != nil {
		return fmt.Errorf("pebblestore: RecordClusterName: %w", err)
	}
	if err := s.kv.Apply(b, pebble.Sync); err != nil {
		return fmt.Errorf("pebblestore: RecordClusterName: %w", err)
	}
	return nil
}
