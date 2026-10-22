package vmrun

import (
	"context"
	"errors"
	"fmt"

	"github.com/lotannauo/toposhift/bench/spike/engine"
	"github.com/lotannauo/toposhift/bench/victoria/vmgate"
	"github.com/lotannauo/toposhift/bench/victoria/vmmodel"
)

// ErrInvalid marks a read that makes the arm INVALID.
var ErrInvalid = errors.New("vmrun: the read is INVALID")

// ReadReport is the first gate on a store: every question of the input that a metrics
// store can be asked, put to it and sorted against the reference.
type ReadReport struct {
	Format int
	Env    Env
	Input  Ref
	// Gate is the sorting by family: asked, refused (a row of the capability table),
	// boundary, reads of a deleted fact, compared and equal.
	Gate *vmgate.Report
	// NotCompared are the families where the questions that were neither refused nor
	// compared exceed half of those the store can be asked: "not compared at sample
	// resolution", not a pass.
	NotCompared []string
	// Invalid is why the arm is INVALID, empty if it is not: a compared answer differs from
	// the reference's, the sorting does not account for every question, or nothing was
	// compared.
	Invalid string `json:",omitempty"`
}

// Read asks every question of the input of the engine and sorts the answers: refused as a
// capability row, boundary instant, read of a deleted fact, or compared. A compared answer
// that differs from the reference's makes the arm INVALID; no counter of an INVALID arm is
// to be read. The engine is the store's (over HTTP, or the model of it).
func Read(ctx context.Context, in *Input, e engine.Engine, env Env) (*ReadReport, error) {
	rep := &ReadReport{Format: ReportFormat, Env: env, Input: in.Ref()}
	qs, err := in.Queries()
	if err != nil {
		return nil, err
	}
	ref, held, err := in.Worlds(ctx)
	if err != nil {
		return nil, err
	}
	g := &vmgate.Gate{Reg: in.Registry, Ref: ref, Store: held, Sem: vmmodel.Semantics{}}
	gate, err := g.Run(qs, e)
	if err != nil {
		return nil, err
	}
	rep.Gate = gate
	rep.NotCompared = gate.NotCompared()
	switch {
	case gate.Wrong > 0:
		rep.Invalid = fmt.Sprintf("%d compared answers differ from the reference's", gate.Wrong)
	case gate.Accounting() != nil:
		rep.Invalid = gate.Accounting().Error()
	case gate.Total().Asked != len(qs):
		rep.Invalid = fmt.Sprintf("%d questions asked of %d", gate.Total().Asked, len(qs))
	case gate.Total().Compared == 0:
		rep.Invalid = "no question was compared"
	}
	if rep.Invalid != "" {
		return rep, fmt.Errorf("%w: %s", ErrInvalid, rep.Invalid)
	}
	return rep, nil
}
