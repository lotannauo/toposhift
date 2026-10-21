package vmquery

import (
	"errors"
	"fmt"
	"time"

	"github.com/lotannauo/toposhift/bench/spike/engine"
	"github.com/lotannauo/toposhift/bench/spike/runner"
	"github.com/lotannauo/toposhift/bench/victoria/vmmodel"
	"github.com/lotannauo/toposhift/internal/identity"
)

// Evaluate answers an expression at a time from a sealed store, as the server is assumed
// to: each term is the last sample of each matching series within the term's window,
// (t-W, t] [U] (j), at t truncated to the millisecond [U] (i); or joins the terms, a
// series that two terms both give being given once. The answer has the series' labels
// and the value 1.
func Evaluate(st *vmmodel.Store, sem vmmodel.Semantics, e Expr, at time.Time) ([]Result, error) {
	atMs := at.UnixMilli()
	var out []Result
	seen := map[string]struct{}{}
	for _, t := range e {
		sel := map[int]struct{}{}
		var order []int
		for _, v := range t.Values {
			for _, i := range st.Lookup(t.Label, v) {
				if _, dup := sel[i]; !dup {
					sel[i] = struct{}{}
					order = append(order, i)
				}
			}
		}
		for _, i := range order {
			s := st.At(i)
			if s.Metric() != t.Metric || s.Get(vmmodel.LabelLayer) != t.Layer.String() ||
				s.Get(vmmodel.LabelProducer) != string(t.Producer) ||
				(t.Type != "" && s.Get(vmmodel.LabelType) != string(t.Type)) {
				continue
			}
			if !st.Last(i, atMs-t.Window.Milliseconds(), atMs, sem) {
				continue
			}
			if _, dup := seen[s.Key()]; dup {
				continue
			}
			seen[s.Key()] = struct{}{}
			labels := map[string]string{}
			for _, l := range s.Labels() {
				labels[l.Name] = l.Value
			}
			out = append(out, Result{Labels: labels, Value: "1"})
		}
	}
	return out, nil
}

// ErrReadOnly is returned by the model engine's Write.
var ErrReadOnly = errors.New("vmquery: the model engine is read-only")

// ModelEngine is the reads of a metrics store as the model assumes them to be: an
// engine.Engine whose Neighbors, NeighborsBatch and Alive are the rewritten queries
// evaluated over a store, and whose Window and reads as of an old token are refused with
// the typed error of the capability table. It holds no records, so it is written
// nothing, and answers nothing it was not asked in the store's terms: a question it
// cannot express is never answered from the latest state instead.
type ModelEngine struct {
	reg     *vmmodel.Registry
	store   *vmmodel.Store
	sem     vmmodel.Semantics
	lastSeq uint64
}

var _ engine.Engine = (*ModelEngine)(nil)

// NewModelEngine returns the engine over a sealed store. lastSeq is the sequence number
// of the last record the store was written from.
func NewModelEngine(reg *vmmodel.Registry, st *vmmodel.Store, sem vmmodel.Semantics, lastSeq uint64) *ModelEngine {
	return &ModelEngine{reg: reg, store: st, sem: sem, lastSeq: lastSeq}
}

// Write refuses: the engine reads a store written elsewhere.
func (*ModelEngine) Write([]engine.Record) error { return ErrReadOnly }

// LastSeq is the sequence number the store was written through.
func (m *ModelEngine) LastSeq() uint64 { return m.lastSeq }

// Retain does nothing: the store keeps everything it is given.
func (*ModelEngine) Retain(time.Time) error { return nil }

// Size is not known to a model.
func (*ModelEngine) Size() (int64, error) {
	return 0, errors.New("vmquery: a model of the store has no size")
}

// Close does nothing.
func (*ModelEngine) Close() error { return nil }

// asOf is the query's snapshot token: Latest, or the last the store holds, which is the
// same snapshot. Any earlier token cannot be read.
func (m *ModelEngine) latest(s engine.Scope) uint64 {
	if s.AsOf >= m.lastSeq {
		return engine.Latest
	}
	return s.AsOf
}

func (m *ModelEngine) ask(op runner.Op, fps []identity.Fingerprint, d engine.Direction, t time.Time, s engine.Scope) (Plan, []Result, error) {
	q := runner.Query{Group: "model", Op: op, Layer: s.Layer, Dir: d, Fps: fps, Age: "instant", At: t, AsOf: m.latest(s)}
	p, err := Rewrite(m.reg, q)
	if err != nil {
		return Plan{}, nil, err
	}
	rs, err := Evaluate(m.store, m.sem, p.Expr, p.At)
	return p, rs, err
}

// Neighbors implements engine.Engine.
func (m *ModelEngine) Neighbors(fp identity.Fingerprint, d engine.Direction, t time.Time, s engine.Scope) ([]engine.Neighbor, error) {
	_, rs, err := m.ask(runner.OpNeighbors, []identity.Fingerprint{fp}, d, t, s)
	if err != nil {
		return nil, err
	}
	return Neighbors(rs, fp, d)
}

// NeighborsBatch implements engine.Engine.
func (m *ModelEngine) NeighborsBatch(fps []identity.Fingerprint, d engine.Direction, t time.Time, s engine.Scope) ([][]engine.Neighbor, error) {
	_, rs, err := m.ask(runner.OpBatch, fps, d, t, s)
	if err != nil {
		return nil, err
	}
	return NeighborsBatch(rs, fps, d)
}

// Alive implements engine.Engine.
func (m *ModelEngine) Alive(fp identity.Fingerprint, t time.Time, s engine.Scope) (bool, error) {
	_, rs, err := m.ask(runner.OpAlive, []identity.Fingerprint{fp}, 0, t, s)
	if err != nil {
		return false, err
	}
	return Alive(rs, fp)
}

// Window is refused: the capability table has it as a row.
func (m *ModelEngine) Window(fp identity.Fingerprint, d engine.Direction, from, to time.Time, s engine.Scope) ([]engine.Record, error) {
	q := runner.Query{Group: "model", Op: runner.OpWindow, Layer: s.Layer, Dir: d, Fps: []identity.Fingerprint{fp}, Age: "window", From: from, To: to, AsOf: s.AsOf}
	_, err := Rewrite(m.reg, q)
	if err == nil {
		err = fmt.Errorf("vmquery: a window was rewritten")
	}
	return nil, err
}
