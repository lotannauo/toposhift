package memstore

import (
	"context"
	"fmt"

	"github.com/lotannauo/toposhift/internal/store"
)

// clusterNameMeta is the cluster name a store records: see [Store.RecordClusterName].
type clusterNameMeta struct {
	name    string
	set     bool
	present bool
}

// ClusterName returns the cluster name the store records, with set false when it
// was started with none, and present false when it records nothing. It is the
// equivalent of the pebblestore method of the same name, and is not persisted: a
// memstore starts empty. After Close it returns [store.ErrClosed].
func (s *Store) ClusterName() (name string, set, present bool, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return "", false, false, closedError("ClusterName")
	}
	return s.clusterName.name, s.clusterName.set, s.clusterName.present, nil
}

// RecordClusterName records the cluster name the store is started with, or with
// set false that none is. A name that does not pass [store.CheckClusterNameLimit]
// is refused with an error that wraps [store.ErrInvalid], and nothing is recorded.
// After Close it returns [store.ErrClosed].
func (s *Store) RecordClusterName(ctx context.Context, name string, set bool) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return closedError("RecordClusterName")
	}
	if err := ctx.Err(); err != nil {
		return contextError("RecordClusterName", err)
	}
	if set {
		if err := store.CheckClusterNameLimit(name); err != nil {
			return fmt.Errorf("memstore: RecordClusterName: %w", err)
		}
	} else {
		name = ""
	}
	s.clusterName = clusterNameMeta{name: name, set: set, present: true}
	return nil
}
