package mcp

import (
	"context"
	"sync"
	"time"

	"github.com/lotannauo/toposhift/internal/identity"
	"github.com/lotannauo/toposhift/internal/store"
)

// blockStore blocks every read until release is closed or the read's context
// ends, and says when a read has started.
type blockStore struct {
	*spyStore
	release chan struct{}
	entered chan struct{}
	once    sync.Once
}

func newBlockStore(st store.Store) *blockStore {
	return &blockStore{spyStore: &spyStore{Store: st}, release: make(chan struct{}), entered: make(chan struct{})}
}

func (b *blockStore) block(ctx context.Context) error {
	b.once.Do(func() { close(b.entered) })
	select {
	case <-b.release:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (b *blockStore) Neighbors(ctx context.Context, fp identity.Fingerprint, d store.Direction, t time.Time, sc store.Scope) ([]store.Neighbor, error) {
	if err := b.block(ctx); err != nil {
		return nil, err
	}
	return b.spyStore.Neighbors(ctx, fp, d, t, sc)
}

func (b *blockStore) Alive(ctx context.Context, fp identity.Fingerprint, t time.Time, sc store.Scope) (bool, error) {
	if err := b.block(ctx); err != nil {
		return false, err
	}
	return b.spyStore.Alive(ctx, fp, t, sc)
}

func (b *blockStore) Window(ctx context.Context, fp identity.Fingerprint, d store.Direction, from, to time.Time, sc store.Scope) ([]store.Record, error) {
	if err := b.block(ctx); err != nil {
		return nil, err
	}
	return b.spyStore.Window(ctx, fp, d, from, to, sc)
}

func (b *blockStore) EntityWindow(ctx context.Context, fp identity.Fingerprint, from, to time.Time, sc store.Scope) ([]store.Record, error) {
	if err := b.block(ctx); err != nil {
		return nil, err
	}
	return b.spyStore.EntityWindow(ctx, fp, from, to, sc)
}
