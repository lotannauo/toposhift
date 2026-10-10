package pebblekv_test

import (
	"context"
	"maps"
	"testing"
	"time"

	"github.com/cockroachdb/pebble/v2/vfs"

	"github.com/lotannauo/toposhift/bench/spike/pebblekv"
	rootkv "github.com/lotannauo/toposhift/internal/store/pebblekv"
)

// A database opened by the root module and then wrapped describes itself as the
// same database opened by this package does, and the waits and the snapshot work
// on it.
func TestWrapDescribesAsOpenDoes(t *testing.T) {
	t.Parallel()
	cfg := pebblekv.Config{Schema: pebblekv.SchemaDefault, Tuning: pebblekv.TinyTuning(), SettleRetention: true}
	cfg.FS = vfs.NewMem()
	own, err := pebblekv.Open("own", pebblekv.BytewiseLayout, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = own.Close() }()
	cfg.FS = vfs.NewMem()
	base, err := rootkv.Open("wrapped", pebblekv.BytewiseLayout, pebblekv.Quiet(cfg))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = base.Close() }()
	wrapped := pebblekv.Wrap(base)
	want, err := own.Describe()
	if err != nil {
		t.Fatal(err)
	}
	got, err := wrapped.Describe()
	if err != nil {
		t.Fatal(err)
	}
	if !maps.Equal(got, want) {
		t.Errorf("Describe of a wrapped database:\n got %v\nwant %v", got, want)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	if err := wrapped.CompactAll(ctx); err != nil {
		t.Fatal(err)
	}
	if s := wrapped.Snapshot(); s.CompactionsInProgress != 0 {
		t.Errorf("a compaction is in progress after CompactAll: %+v", s)
	}
}
