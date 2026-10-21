package vmquery

import (
	"context"
	"time"

	"github.com/lotannauo/toposhift/bench/victoria/vmmodel"
)

// Evaluate answers an expression at a time from a sealed store, as the server is assumed
// to: each term is the last sample of each matching series within the term's window,
// (t-W, t] (assumption (j)), at t truncated to the millisecond (assumption (i)); or joins the terms, a
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

// ModelBackend is the model of the server as a [Backend]: it evaluates a plan's
// expression over a sealed store with the semantics assumed of a window.
type ModelBackend struct {
	Store *vmmodel.Store
	Sem   vmmodel.Semantics
}

// Instant implements Backend.
func (m ModelBackend) Instant(_ context.Context, p Plan) ([]Result, error) {
	return Evaluate(m.Store, m.Sem, p.Expr, p.At)
}

// NewModelEngine returns the reads of a metrics store as the model assumes them to be:
// an [Engine] over the model of the server for a sealed store. lastSeq is the sequence
// number of the last record the store was written from.
func NewModelEngine(reg *vmmodel.Registry, st *vmmodel.Store, sem vmmodel.Semantics, lastSeq uint64) *Engine {
	return NewEngine(reg, ModelBackend{Store: st, Sem: sem}, lastSeq)
}
