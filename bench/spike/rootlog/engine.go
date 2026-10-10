package rootlog

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/lotannauo/toposhift/bench/spike/engine"
	"github.com/lotannauo/toposhift/bench/spike/pebblekv"
	"github.com/lotannauo/toposhift/internal/catalog"
	"github.com/lotannauo/toposhift/internal/identity"
	"github.com/lotannauo/toposhift/internal/lifecycle"
	"github.com/lotannauo/toposhift/internal/store"
	"github.com/lotannauo/toposhift/internal/store/pebblestore"
)

// Options says how an engine is opened.
type Options struct {
	// Config is the database's, as the runner chose it. Pebble's messages are
	// dropped unless it names a logger, as the spike's other engines drop them.
	pebblekv.Config
	// Recorder receives the store's counts and samples (see
	// [pebblestore.Recorder]). Nil discards them.
	Recorder engine.Recorder
	// Checkpoints says when interleaved checkpoints are written. The zero value
	// writes none, stated outright to the store: it is not the store's default.
	Checkpoints pebblestore.CheckpointOptions
}

// Engine is layout L through the root module's store. It implements
// [engine.Engine] and the optional parts of a measurement the runner uses.
type Engine struct {
	s   *pebblestore.Store
	ins *pebblestore.Instrument
	// kv gives the hooks of a measurement the database the store owns. It is never
	// closed: Close goes through the store.
	kv   *pebblekv.KV
	ckpt pebblestore.CheckpointOptions
}

var (
	_ engine.Engine        = (*Engine)(nil)
	_ engine.Settler       = (*Engine)(nil)
	_ engine.Quiescer      = (*Engine)(nil)
	_ engine.ColdStarter   = (*Engine)(nil)
	_ engine.Canonicalizer = (*Engine)(nil)
	_ engine.Statser       = (*Engine)(nil)
	_ engine.Describer     = (*Engine)(nil)
	_ engine.LayerSizer    = (*Engine)(nil)
	_ engine.Breakdowner   = (*Engine)(nil)
	_ engine.Checkpointer  = (*Engine)(nil)
)

// Open opens the store under dir, new or as an earlier one left it. The lifecycle
// policy is the zero one: no boot key.
func Open(dir string, opts Options) (*Engine, error) {
	ckpt := opts.Checkpoints // a copy: its address is the store's explicit policy
	so := pebblestore.Options{
		Config:      pebblekv.Quiet(opts.Config),
		Policy:      lifecycle.Policy{},
		Checkpoints: &ckpt,
	}
	if opts.Recorder != nil { // a nil engine.Recorder must stay a nil recorder, not a typed one
		so.Recorder = opts.Recorder
	}
	s, err := pebblestore.Open(dir, so)
	if err != nil {
		return nil, mapError(err)
	}
	ins := s.Instrument()
	return &Engine{s: s, ins: ins, kv: pebblekv.Wrap(ins.KV()), ckpt: opts.Checkpoints}, nil
}

// mapError makes an error of the store match the engine's errors as well as the
// store's.
func mapError(err error) error {
	switch {
	case err == nil:
		return nil
	case errors.Is(err, store.ErrBeforeHorizon):
		return fmt.Errorf("%w: %w", err, engine.ErrBeforeHorizon)
	case errors.Is(err, store.ErrInvalid):
		return fmt.Errorf("%w: %w", err, engine.ErrInvalid)
	}
	return err
}

func toStore(r engine.Record) store.Record {
	return store.Record{
		Layer:     r.Layer,
		Subject:   store.Subject{Kind: store.SubjectKind(r.Subject.Kind), A: r.Subject.A, B: r.Subject.B, Relation: r.Subject.Relation},
		Producer:  r.Producer,
		EventTime: r.EventTime,
		Seq:       r.Seq,
		Kind:      r.Kind,
		TTL:       r.TTL,
		Through:   r.Through,
		Payload:   r.Payload,
		// Boot is empty and EventTimeBasis is unknown: the spike's records carry neither.
	}
}

func fromStore(r store.Record) engine.Record {
	return engine.Record{
		Layer:     r.Layer,
		Subject:   engine.Subject{Kind: engine.SubjectKind(r.Subject.Kind), A: r.Subject.A, B: r.Subject.B, Relation: r.Subject.Relation},
		Producer:  r.Producer,
		EventTime: r.EventTime,
		Seq:       r.Seq,
		Kind:      r.Kind,
		TTL:       r.TTL,
		Through:   r.Through,
		Payload:   r.Payload,
	}
}

func scopeOf(s engine.Scope) store.Scope { return store.Scope{Layer: s.Layer, AsOf: s.AsOf} }

func neighborsFrom(ns []store.Neighbor) []engine.Neighbor {
	if len(ns) == 0 {
		return nil
	}
	out := make([]engine.Neighbor, len(ns))
	for i, n := range ns {
		out[i] = engine.Neighbor{Peer: n.Peer, Relation: n.Relation}
	}
	return out
}

// Write implements [engine.Engine].
func (e *Engine) Write(batch []engine.Record) error {
	recs := make([]store.Record, len(batch))
	for i, r := range batch {
		recs[i] = toStore(r)
	}
	return mapError(e.s.Write(context.Background(), recs))
}

// LastSeq implements [engine.Engine].
func (e *Engine) LastSeq() uint64 { return e.s.LastSeq() }

// Neighbors implements [engine.Engine].
func (e *Engine) Neighbors(fp identity.Fingerprint, dir engine.Direction, t time.Time, s engine.Scope) ([]engine.Neighbor, error) {
	ns, err := e.s.Neighbors(context.Background(), fp, store.Direction(dir), t, scopeOf(s))
	if err != nil {
		return nil, mapError(err)
	}
	return neighborsFrom(ns), nil
}

// NeighborsBatch implements [engine.Engine].
func (e *Engine) NeighborsBatch(fps []identity.Fingerprint, dir engine.Direction, t time.Time, s engine.Scope) ([][]engine.Neighbor, error) {
	all, err := e.s.NeighborsBatch(context.Background(), fps, store.Direction(dir), t, scopeOf(s))
	if err != nil {
		return nil, mapError(err)
	}
	out := make([][]engine.Neighbor, len(all))
	for i, ns := range all {
		out[i] = neighborsFrom(ns)
	}
	return out, nil
}

// Alive implements [engine.Engine]. The store is opened with no boot key, so it
// never reports a quarantine.
func (e *Engine) Alive(fp identity.Fingerprint, t time.Time, s engine.Scope) (bool, error) {
	ok, err := e.s.Alive(context.Background(), fp, t, scopeOf(s))
	return ok, mapError(err)
}

// Window implements [engine.Engine].
func (e *Engine) Window(fp identity.Fingerprint, dir engine.Direction, from, to time.Time, s engine.Scope) ([]engine.Record, error) {
	rs, err := e.s.Window(context.Background(), fp, store.Direction(dir), from, to, scopeOf(s))
	if err != nil {
		return nil, mapError(err)
	}
	if len(rs) == 0 {
		return nil, nil
	}
	out := make([]engine.Record, len(rs))
	for i, r := range rs {
		out[i] = fromStore(r)
	}
	return out, nil
}

// Retain implements [engine.Engine].
func (e *Engine) Retain(horizon time.Time) error {
	return mapError(e.s.Retain(context.Background(), horizon))
}

// LastRetain is how the last call of Retain spent its time; see
// [pebblestore.Instrument.LastRetain].
func (e *Engine) LastRetain() (work, flush, settle time.Duration, deadlineHit bool) {
	return e.ins.LastRetain()
}

// Size implements [engine.Engine].
func (e *Engine) Size() (int64, error) { return e.ins.KV().Size() }

// Close implements [engine.Engine].
func (e *Engine) Close() error { return e.s.Close() }

// Settle implements [engine.Settler].
func (e *Engine) Settle() error { return e.ins.KV().Settle() }

// Quiesce implements [engine.Quiescer].
func (e *Engine) Quiesce(ctx context.Context) error { return e.kv.Quiesce(ctx) }

// CompactAll implements [engine.Quiescer].
func (e *Engine) CompactAll(ctx context.Context) error { return e.kv.CompactAll(ctx) }

// ColdStart implements [engine.ColdStarter].
func (e *Engine) ColdStart() { e.kv.ColdStart() }

// Canonicalize implements [engine.Canonicalizer]. The store's own sequence number,
// horizons and the rest of its state are keys, rewritten with the others.
func (e *Engine) Canonicalize(ctx context.Context) error { return e.kv.Canonicalize(ctx) }

// Stats implements [engine.Statser].
func (e *Engine) Stats() map[string]int64 { return e.kv.Snapshot().Flat() }

// Describe implements [engine.Describer]: the database's description, with the
// layout, the store it was built through and the checkpoint policy.
func (e *Engine) Describe() (map[string]string, error) {
	d, err := e.kv.Describe()
	if err != nil {
		return nil, err
	}
	d["layout"] = "L"
	d["store"] = "root"
	d["checkpoints"] = "off"
	if cp := e.ckpt; cp.On {
		d["checkpoints"] = fmt.Sprintf("kmin=%d alpha=%g lag=%s", cp.KMin, cp.Alpha, cp.Lag)
	}
	return d, nil
}

// SizeByLayer implements [engine.LayerSizer].
func (e *Engine) SizeByLayer() (map[catalog.Layer]int64, error) { return e.ins.SizeByLayer() }

// Breakdown implements [engine.Breakdowner].
func (e *Engine) Breakdown() (map[engine.Part]int64, error) {
	parts, err := e.ins.Breakdown()
	if err != nil {
		return nil, err
	}
	out := make(map[engine.Part]int64, len(parts))
	for p, n := range parts {
		out[engine.Part{Layer: p.Layer, Kind: p.Kind}] = n
	}
	return out, nil
}

// CheckpointEdges implements [engine.Checkpointer].
func (e *Engine) CheckpointEdges(layer catalog.Layer, fp identity.Fingerprint, dir engine.Direction, c time.Time) error {
	return mapError(e.ins.CheckpointEdges(layer, fp, store.Direction(dir), c))
}

// CheckpointEntity implements [engine.Checkpointer].
func (e *Engine) CheckpointEntity(layer catalog.Layer, fp identity.Fingerprint, c time.Time) error {
	return mapError(e.ins.CheckpointEntity(layer, fp, c))
}
