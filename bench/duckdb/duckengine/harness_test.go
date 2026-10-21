package duckengine

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/lotannauo/toposhift/bench/duckdb/duckmodel"
	"github.com/lotannauo/toposhift/bench/spike/engine"
	"github.com/lotannauo/toposhift/bench/spike/runner"
	"github.com/lotannauo/toposhift/internal/identity"
)

// fileEngine is an engine whose reads are SQL over files, for the conformance checks,
// which write and read in turn. Files that were built take no write, so it keeps the
// records it is given, and on the first read after a change builds the files from them
// as a build of the stream would: the retention applied once, at the horizon the stream
// ends with. Writes and retentions are checked and sequenced by the model, which
// conformance has already held to the contract, so what these checks hold to account is
// the reads.
type fileEngine struct {
	t    testing.TB
	arm  Arm
	mut  sqlMutant
	root string

	mu      sync.Mutex
	model   *duckmodel.Model
	recs    []engine.Record
	horizon time.Time
	dirty   bool
	builds  int
	db      *DB
	bdb     *DB
	reader  *Reader
}

var _ engine.Engine = (*fileEngine)(nil)

func newFileEngine(t testing.TB, arm Arm, mut sqlMutant) *fileEngine {
	t.Helper()
	e := &fileEngine{t: t, arm: arm, mut: mut, root: t.TempDir(), model: duckmodel.New(duckmodel.ArmRecords), dirty: true}
	var err error
	if e.db, err = Open(context.Background(), Config{Mode: ModeRead, TempDir: filepath.Join(e.root, "tmp-read")}); err != nil {
		t.Fatal(err)
	}
	if e.bdb, err = Open(context.Background(), Config{Mode: ModeBuild, TempDir: filepath.Join(e.root, "tmp-build")}); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { e.Close() })
	return e
}

func (e *fileEngine) Write(batch []engine.Record) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	if err := e.model.Write(batch); err != nil {
		return err
	}
	for _, r := range batch {
		r.Payload = slices.Clone(r.Payload)
		e.recs = append(e.recs, r)
	}
	e.dirty = true
	return nil
}

func (e *fileEngine) Retain(h time.Time) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	if err := e.model.Retain(h); err != nil {
		return err
	}
	if h.After(e.horizon) {
		e.horizon = h
		e.dirty = true
	}
	return nil
}

func (e *fileEngine) LastSeq() uint64 { return e.model.LastSeq() }

// ready builds the files if the stream has changed since they were, and returns the
// reader. It returns nil for a stream with nothing in it, which no file can hold.
func (e *fileEngine) ready() (*Reader, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if !e.dirty {
		return e.reader, nil
	}
	if e.reader != nil {
		e.reader.Close()
		e.reader = nil
	}
	e.dirty = false
	if len(e.recs) == 0 {
		return nil, nil
	}
	e.builds++
	ctx := context.Background()
	dir := filepath.Join(e.root, fmt.Sprintf("build-%d", e.builds))
	recs, horizon := e.recs, e.horizon
	src := Source{Label: "harness", Horizon: horizon, Replay: func(_ context.Context, sink runner.Sink) (string, error) {
		for i := 0; i < len(recs); i += 64 {
			batch := slices.Clone(recs[i:min(i+64, len(recs))])
			if err := sink.Write(batch); err != nil {
				return "", err
			}
		}
		if !horizon.IsZero() {
			if err := sink.Retain(horizon); err != nil {
				return "", err
			}
		}
		return "harness", nil
	}}
	// With a stream whose horizon is past every record there is still the baselines.
	if _, err := Build(ctx, e.bdb, dir, src, []Arm{e.arm}, BuildOptions{VerifyModel: e.arm == ArmI}); err != nil {
		return nil, err
	}
	m, err := ReadManifest(dir)
	if err != nil {
		return nil, err
	}
	if e.reader, err = openReader(ctx, e.db, dir, e.arm, m, false, e.mut); err != nil {
		return nil, err
	}
	return e.reader, nil
}

func (e *fileEngine) Neighbors(fp identity.Fingerprint, dir engine.Direction, t time.Time, sc engine.Scope) ([]engine.Neighbor, error) {
	if err := sc.Validate(); err != nil {
		return nil, err
	}
	r, err := e.ready()
	if err != nil || r == nil {
		return nil, err
	}
	return r.Neighbors(fp, dir, t, sc)
}

func (e *fileEngine) NeighborsBatch(fps []identity.Fingerprint, dir engine.Direction, t time.Time, sc engine.Scope) ([][]engine.Neighbor, error) {
	if err := sc.Validate(); err != nil {
		return nil, err
	}
	r, err := e.ready()
	if err != nil {
		return nil, err
	}
	if r == nil {
		return make([][]engine.Neighbor, len(fps)), nil
	}
	return r.NeighborsBatch(fps, dir, t, sc)
}

func (e *fileEngine) Alive(fp identity.Fingerprint, t time.Time, sc engine.Scope) (bool, error) {
	if err := sc.Validate(); err != nil {
		return false, err
	}
	r, err := e.ready()
	if err != nil || r == nil {
		return false, err
	}
	return r.Alive(fp, t, sc)
}

func (e *fileEngine) Window(fp identity.Fingerprint, dir engine.Direction, from, to time.Time, sc engine.Scope) ([]engine.Record, error) {
	if err := sc.Validate(); err != nil {
		return nil, err
	}
	r, err := e.ready()
	if err != nil || r == nil {
		return nil, err
	}
	return r.Window(fp, dir, from, to, sc)
}

func (e *fileEngine) Size() (int64, error) { return e.model.Size() }

func (e *fileEngine) Close() error {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.reader != nil {
		e.reader.Close()
		e.reader = nil
	}
	var err error
	if e.db != nil {
		err = e.db.Close()
		e.db = nil
	}
	if e.bdb != nil {
		err = errors.Join(err, e.bdb.Close())
		e.bdb = nil
	}
	return err
}
