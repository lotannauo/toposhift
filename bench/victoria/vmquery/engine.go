package vmquery

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/lotannauo/toposhift/bench/spike/engine"
	"github.com/lotannauo/toposhift/bench/spike/runner"
	"github.com/lotannauo/toposhift/bench/victoria/vmmodel"
	"github.com/lotannauo/toposhift/internal/identity"
)

// Backend answers a rewritten read as a metrics store does: the series of the vector
// an instant query gives. The model of the server is one ([ModelBackend]) and the server
// over HTTP is another (package vmengine).
type Backend interface {
	Instant(ctx context.Context, p Plan) ([]Result, error)
}

// ErrReadOnly is returned by the engine's Write.
var ErrReadOnly = errors.New("vmquery: the engine is read-only")

// Engine is the reads of a metrics store as an engine.Engine: its Neighbors,
// NeighborsBatch and Alive are the rewritten queries put to a [Backend], and its Window
// and reads as of an old token are refused with the typed error of the capability
// table. It holds no records, so it is written nothing, and answers nothing it was not
// asked in the store's terms: a question it cannot express is never answered from the
// latest state instead.
type Engine struct {
	reg     *vmmodel.Registry
	be      Backend
	lastSeq uint64
}

var _ engine.Engine = (*Engine)(nil)

// NewEngine returns the engine over a backend. lastSeq is the sequence number of the
// last record the store was written from.
func NewEngine(reg *vmmodel.Registry, be Backend, lastSeq uint64) *Engine {
	return &Engine{reg: reg, be: be, lastSeq: lastSeq}
}

// Write refuses: the engine reads a store written elsewhere.
func (*Engine) Write([]engine.Record) error { return ErrReadOnly }

// LastSeq is the sequence number the store was written through.
func (e *Engine) LastSeq() uint64 { return e.lastSeq }

// Retain does nothing: the store keeps everything it is given.
func (*Engine) Retain(time.Time) error { return nil }

// Size is not known to the engine: what a store holds is read from the store.
func (*Engine) Size() (int64, error) {
	return 0, errors.New("vmquery: the engine has no size")
}

// Close does nothing.
func (*Engine) Close() error { return nil }

// latest is the query's snapshot token: Latest, or the last the store holds, which is the
// same snapshot. Any earlier token cannot be read.
func (e *Engine) latest(s engine.Scope) uint64 {
	if s.AsOf >= e.lastSeq {
		return engine.Latest
	}
	return s.AsOf
}

func (e *Engine) ask(op runner.Op, fps []identity.Fingerprint, d engine.Direction, t time.Time, s engine.Scope) ([]Result, error) {
	q := runner.Query{Group: "engine", Op: op, Layer: s.Layer, Dir: d, Fps: fps, Age: "instant", At: t, AsOf: e.latest(s)}
	p, err := Rewrite(e.reg, q)
	if err != nil {
		return nil, err
	}
	return e.be.Instant(context.Background(), p)
}

// Neighbors implements engine.Engine.
func (e *Engine) Neighbors(fp identity.Fingerprint, d engine.Direction, t time.Time, s engine.Scope) ([]engine.Neighbor, error) {
	rs, err := e.ask(runner.OpNeighbors, []identity.Fingerprint{fp}, d, t, s)
	if err != nil {
		return nil, err
	}
	return Neighbors(rs, fp, d)
}

// NeighborsBatch implements engine.Engine.
func (e *Engine) NeighborsBatch(fps []identity.Fingerprint, d engine.Direction, t time.Time, s engine.Scope) ([][]engine.Neighbor, error) {
	rs, err := e.ask(runner.OpBatch, fps, d, t, s)
	if err != nil {
		return nil, err
	}
	return NeighborsBatch(rs, fps, d)
}

// Alive implements engine.Engine.
func (e *Engine) Alive(fp identity.Fingerprint, t time.Time, s engine.Scope) (bool, error) {
	rs, err := e.ask(runner.OpAlive, []identity.Fingerprint{fp}, 0, t, s)
	if err != nil {
		return false, err
	}
	return Alive(rs, fp)
}

// Window is refused: the capability table has it as a row.
func (e *Engine) Window(fp identity.Fingerprint, d engine.Direction, from, to time.Time, s engine.Scope) ([]engine.Record, error) {
	q := runner.Query{Group: "engine", Op: runner.OpWindow, Layer: s.Layer, Dir: d, Fps: []identity.Fingerprint{fp}, Age: "window", From: from, To: to, AsOf: s.AsOf}
	_, err := Rewrite(e.reg, q)
	if err == nil {
		err = fmt.Errorf("vmquery: a window was rewritten")
	}
	return nil, err
}
