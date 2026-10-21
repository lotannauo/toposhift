package doltengine

import (
	"cmp"
	"errors"
	"fmt"
	"slices"
	"testing"
	"time"

	"pgregory.net/rapid"

	"github.com/lotannauo/toposhift/bench/dolt/doltmodel"
	"github.com/lotannauo/toposhift/bench/spike/conformance"
	"github.com/lotannauo/toposhift/bench/spike/engine"
	"github.com/lotannauo/toposhift/bench/spike/workload"
	"github.com/lotannauo/toposhift/internal/catalog"
	"github.com/lotannauo/toposhift/internal/lifecycle"
)

// mem is the tables a writer's plans make, kept in memory and changed as the statements
// of a batch and of a retention change them: INSERT adds, REPLACE replaces by key, and a
// retention deletes. It is the other side of the comparison with a [doltmodel.Model] that
// was given the same stream, with no server in between.
type mem struct {
	lastSeq int64
	horizon int64
	pastEnd bool

	log      []doltmodel.LogRow
	entLog   []doltmodel.EntLogRow
	state    map[string]doltmodel.StateRow
	entState map[string]doltmodel.EntStateRow
}

func newMem() *mem {
	return &mem{lastSeq: doltmodel.EncodeSeq(0), state: map[string]doltmodel.StateRow{}, entState: map[string]doltmodel.EntStateRow{}}
}

func stateKey(r doltmodel.StateRow) string {
	return fmt.Sprintf("%d|%d|%q|%q|%q|%q", r.Layer, r.Dir, r.A, r.Peer, r.Rel, r.Producer)
}

func (m *mem) apply(p *batchPlan) {
	m.log = append(m.log, p.log...)
	m.entLog = append(m.entLog, p.entLog...)
	for _, r := range p.state {
		m.state[stateKey(r)] = r
	}
	for _, r := range p.entState {
		m.entState[fmt.Sprintf("%q|%q", r.Fp, r.Producer)] = r
	}
	m.lastSeq = doltmodel.EncodeSeq(p.last)
}

// retain does what the retention's statements do: the log keeps, of each key's rows before
// the horizon, the newest; the state loses the rows whose deadline is at or before it.
func (m *mem) retain(h int64, pastEnd bool) {
	if pastEnd {
		h = 1<<63 - 1
	}
	m.horizon, m.pastEnd = max(m.horizon, h), m.pastEnd || pastEnd
	for k, r := range m.state {
		if !r.Open && r.DeadlineNs <= h {
			delete(m.state, k)
		}
	}
	for k, r := range m.entState {
		if !r.Open && r.DeadlineNs <= h {
			delete(m.entState, k)
		}
	}
	before := func(ns int64) bool { return pastEnd || ns < h }
	newest := map[string]doltmodel.LogRow{}
	for _, r := range m.log {
		k := fmt.Sprintf("%d|%d|%q|%q|%q|%q", r.Layer, r.Dir, r.A, r.Peer, r.Rel, r.Producer)
		if o, ok := newest[k]; before(r.EventNs) && (!ok || cmp.Or(cmp.Compare(r.EventNs, o.EventNs), cmp.Compare(r.Seq, o.Seq)) > 0) {
			newest[k] = r
		}
	}
	kept := m.log[:0]
	for _, r := range m.log {
		k := fmt.Sprintf("%d|%d|%q|%q|%q|%q", r.Layer, r.Dir, r.A, r.Peer, r.Rel, r.Producer)
		if before(r.EventNs) && newest[k] != r {
			continue
		}
		kept = append(kept, r)
	}
	m.log = kept
	entNewest := map[string]doltmodel.EntLogRow{}
	for _, r := range m.entLog {
		k := fmt.Sprintf("%q|%q", r.Fp, r.Producer)
		if o, ok := entNewest[k]; before(r.EventNs) && (!ok || cmp.Or(cmp.Compare(r.EventNs, o.EventNs), cmp.Compare(r.Seq, o.Seq)) > 0) {
			entNewest[k] = r
		}
	}
	entKept := m.entLog[:0]
	for _, r := range m.entLog {
		if before(r.EventNs) && entNewest[fmt.Sprintf("%q|%q", r.Fp, r.Producer)] != r {
			continue
		}
		entKept = append(entKept, r)
	}
	m.entLog = entKept
}

func (m *mem) tables() doltmodel.Tables {
	t := doltmodel.Tables{Meta: doltmodel.Meta{LastSeq: m.lastSeq, HorizonNs: m.horizon, PastEnd: m.pastEnd, SchemaVersion: doltmodel.SchemaVersion}}
	t.Log = slices.Clone(m.log)
	slices.SortFunc(t.Log, func(a, b doltmodel.LogRow) int {
		return cmp.Or(cmp.Compare(a.Layer, b.Layer), cmp.Compare(a.Dir, b.Dir), cmp.Compare(a.A, b.A), cmp.Compare(a.Peer, b.Peer),
			cmp.Compare(a.Rel, b.Rel), cmp.Compare(a.Producer, b.Producer), cmp.Compare(a.EventNs, b.EventNs), cmp.Compare(a.Seq, b.Seq))
	})
	t.EntLog = slices.Clone(m.entLog)
	slices.SortFunc(t.EntLog, func(a, b doltmodel.EntLogRow) int {
		return cmp.Or(cmp.Compare(a.Fp, b.Fp), cmp.Compare(a.Producer, b.Producer), cmp.Compare(a.EventNs, b.EventNs), cmp.Compare(a.Seq, b.Seq))
	})
	for _, r := range m.state {
		t.State = append(t.State, r)
	}
	slices.SortFunc(t.State, func(a, b doltmodel.StateRow) int {
		return cmp.Or(cmp.Compare(a.Layer, b.Layer), cmp.Compare(a.Dir, b.Dir), cmp.Compare(a.A, b.A), cmp.Compare(a.Peer, b.Peer),
			cmp.Compare(a.Rel, b.Rel), cmp.Compare(a.Producer, b.Producer))
	})
	for _, r := range m.entState {
		t.EntState = append(t.EntState, r)
	}
	slices.SortFunc(t.EntState, func(a, b doltmodel.EntStateRow) int {
		return cmp.Or(cmp.Compare(a.Fp, b.Fp), cmp.Compare(a.Producer, b.Producer))
	})
	return t
}

// planner is a writer driven the way the engine drives it: plan a batch, apply it, commit
// it to the writer's memory; and for a retention, move the horizon and expire the state.
type planner struct {
	w      *writer
	m      *mem
	counts doltmodel.Counts
}

func newPlanner(f *faults) *planner { return &planner{w: newWriter(f), m: newMem()} }

func (pl *planner) write(batch []engine.Record) error {
	p, refused, err := pl.w.plan(batch)
	if err != nil {
		if refused {
			pl.counts.PayloadRefusals++
		}
		return err
	}
	pl.m.apply(p)
	pl.w.commit(p)
	pl.counts.Records += p.counts.Records
	pl.counts.LogRows += p.counts.LogRows
	pl.counts.StateWrites += p.counts.StateWrites
	return nil
}

func (pl *planner) retain(horizon time.Time) {
	past := horizon.After(engine.MaxEventTime)
	h := int64(0)
	switch {
	case past:
		h = 1<<63 - 1
	case horizon.After(engine.MinEventTime):
		h = doltmodel.Nanos(horizon)
	}
	changed := false
	if past && !pl.w.pastEnd {
		pl.w.pastEnd, changed = true, true
	}
	if h > pl.w.horizon {
		pl.w.horizon, changed = h, true
	}
	if !changed {
		return
	}
	pl.m.retain(h, pl.w.pastEnd)
	pl.w.expire(h)
}

// sameAsModel compares what the planner has made with the model's tables and counters.
func (pl *planner) sameAsModel(m *doltmodel.Model) error {
	got, want := pl.m.tables(), m.Tables()
	if d := got.FirstDifference(want); d != "" {
		return errors.New(d)
	}
	c := m.Counts()
	if pl.counts.Records != c.Records || pl.counts.LogRows != c.LogRows || pl.counts.StateWrites != c.StateWrites || pl.counts.PayloadRefusals != c.PayloadRefusals {
		return fmt.Errorf("planned counts %+v, the model's %+v", pl.counts, c)
	}
	// What the writer remembers is the state table's rows, one per key.
	if n := len(pl.w.newest); n != len(want.State)/2+len(want.EntState) && !pl.w.f.stateKeepsLast {
		var edges int
		for _, r := range want.State {
			if r.Dir == int(engine.Forward) {
				edges++
			}
		}
		if n != edges+len(want.EntState) {
			return fmt.Errorf("the writer remembers %d state keys, the tables hold %d", n, edges+len(want.EntState))
		}
	}
	return nil
}

// driveBoth writes a workload to a model and to a planner in batches, with retentions at the
// given fractions of the period, and compares them after every batch.
func driveBoth(cfg workload.Config, batchSize int, retainAt []float64, f *faults) error {
	g, err := workload.New(cfg)
	if err != nil {
		return err
	}
	m, pl := doltmodel.New(doltmodel.WithPayloadCapacity(doltmodel.RegisteredPayloadCapacity)), newPlanner(f)
	var horizon time.Time
	next := 0
	span := g.End().Sub(g.Start())
	for {
		recs := g.Batch(batchSize)
		if len(recs) == 0 {
			break
		}
		kept := recs[:0]
		for _, r := range recs {
			if !r.EventTime.Before(horizon) {
				kept = append(kept, r)
			}
		}
		if len(kept) == 0 {
			continue
		}
		if err := m.Write(slices.Clone(kept)); err != nil {
			return err
		}
		if err := pl.write(slices.Clone(kept)); err != nil {
			return err
		}
		if err := pl.sameAsModel(m); err != nil {
			return fmt.Errorf("after the batch ending at seq %d: %w", kept[len(kept)-1].Seq, err)
		}
		if next < len(retainAt) && !kept[len(kept)-1].EventTime.Before(g.Start().Add(time.Duration(retainAt[next]*float64(span)))) {
			horizon = g.Start().Add(time.Duration(retainAt[next] * float64(span)))
			next++
			if err := m.Retain(horizon); err != nil {
				return err
			}
			pl.retain(horizon)
			g.SetHorizon(horizon)
			if err := pl.sameAsModel(m); err != nil {
				return fmt.Errorf("after the retention at %v: %w", horizon, err)
			}
		}
	}
	return nil
}

// TestThePlannedRowsAreTheModelsTables: the rows a writer plans, applied as the statements
// apply them, are the rows of a model given the same stream, batch after batch, through
// retentions: the late records that change no state, ties at one instant, runs extended
// through a later time, deletes, entities, and sequence numbers across 2^63.
func TestThePlannedRowsAreTheModelsTables(t *testing.T) {
	cfgs := conformance.Configs()
	for i, cfg := range cfgs {
		if conformance.Trimmed() && i != len(cfgs)-1 {
			continue // as the conformance test's trimmed run does: one workload, the last
		}
		for _, retain := range [][]float64{nil, {0.3, 0.7}} {
			t.Run(fmt.Sprintf("config %d retain %v", i, retain), func(t *testing.T) {
				t.Parallel()
				if err := driveBoth(cfg, 40, retain, &faults{}); err != nil {
					t.Fatal(err)
				}
			})
		}
	}
}

// TestPlanningDefectsAreCaught: the defects of the writer's planning make rows that are not
// the model's.
func TestPlanningDefectsAreCaught(t *testing.T) {
	cfgs := conformance.Configs()
	for _, c := range []struct {
		name string
		f    faults
		cfg  workload.Config
	}{
		{FaultStateKeepsLast, faults{stateKeepsLast: true}, cfgs[1]},
		{FaultTieByEvent, faults{tieByEvent: true}, cfgs[1]},
		{FaultRetainDropsOpenState, faults{retainDropsOpenState: true}, cfgs[0]},
	} {
		t.Run(c.name, func(t *testing.T) {
			if err := driveBoth(c.cfg, 40, []float64{0.3, 0.7}, &c.f); err == nil {
				t.Fatalf("a writer that %s made the model's rows", c.name)
			}
		})
	}
}

// TestValidationAgreesWithTheModel: the same batch, after the same writes, is refused by the
// planner for the reason the model refuses it, and a batch the model accepts is accepted.
func TestValidationAgreesWithTheModel(t *testing.T) {
	big := observe(edge1, 10, "a", 100, 0)
	big.Payload = make([]byte, doltmodel.RegisteredPayloadCapacity+1)
	long := observe(edge1, 10, "a", 100, 0)
	long.Producer = lifecycle.Producer(string(make([]byte, doltmodel.MaxProducerLen+1)))
	otherLayer := observe(edge1, 10, "a", 100, 0)
	otherLayer.Layer = catalog.L3
	wrongEntityLayer := existence(pod1, 10, "a", 100, 0)
	wrongEntityLayer.Layer = catalog.L3
	twoLayers := []engine.Record{observe(edge3, 10, "a", 100, 0), observe(edge3, 11, "a", 101, 0)}
	twoLayers[1].Layer = catalog.L3
	pastEnd := engine.MaxEventTime.Add(time.Nanosecond)

	for _, c := range []struct {
		name  string
		setup func(w both)
		batch []engine.Record
	}{
		{"a valid batch", nil, []engine.Record{observe(edge1, 10, "a", 100, 0), existence(pod1, 11, "a", 100, 0)}},
		{"an empty producer", nil, []engine.Record{func() engine.Record { r := observe(edge1, 10, "a", 100, 0); r.Producer = ""; return r }()}},
		{"an event time before 1970", nil, []engine.Record{func() engine.Record { r := observe(edge1, 10, "a", 0, 0); r.EventTime = time.Unix(-1, 0); return r }()}},
		{"a delete with a TTL", nil, []engine.Record{func() engine.Record { r := del(edge1, 10, "a", 100); r.TTL = time.Second; return r }()}},
		{"an entity in the wrong layer", nil, []engine.Record{wrongEntityLayer}},
		{"a payload over the column", nil, []engine.Record{big}},
		{"a payload of exactly the column", nil, []engine.Record{func() engine.Record {
			r := observe(edge1, 10, "a", 100, 0)
			r.Payload = make([]byte, doltmodel.RegisteredPayloadCapacity)
			return r
		}()}},
		{"a producer over the column", nil, []engine.Record{long}},
		{"a repeated sequence number", func(w both) {
			_ = w.write([]engine.Record{observe(edge2, 10, "a", 100, 0)})
		}, []engine.Record{observe(edge1, 10, "a", 100, 0)}},
		{"a sequence number that falls in the batch", nil, []engine.Record{observe(edge1, 11, "a", 100, 0), observe(edge2, 10, "a", 100, 0)}},
		{"a subject in a second layer", func(w both) {
			_ = w.write([]engine.Record{observe(edge1, 9, "a", 100, 0)})
		}, []engine.Record{otherLayer}},
		{"a subject in two layers in one batch", nil, twoLayers},
		{"a record before the horizon", func(w both) {
			w.retain(at(200))
		}, []engine.Record{observe(edge1, 10, "a", 100, 0)}},
		{"a record at the horizon", func(w both) {
			w.retain(at(200))
		}, []engine.Record{observe(edge1, 10, "a", 200, 0)}},
		{"a record when the horizon is past the end", func(w both) {
			w.retain(pastEnd)
		}, []engine.Record{observe(edge1, 10, "a", 100, 0)}},
		{"a record at the last instant when the horizon is past the end", func(w both) {
			w.retain(pastEnd)
		}, []engine.Record{func() engine.Record { r := observe(edge1, 10, "a", 0, 0); r.EventTime = engine.MaxEventTime; return r }()}},
	} {
		t.Run(c.name, func(t *testing.T) {
			m := doltmodel.New(doltmodel.WithPayloadCapacity(doltmodel.RegisteredPayloadCapacity))
			pl := newPlanner(&faults{})
			if c.setup != nil {
				c.setup(modelAndPlanner{m, pl})
			}
			merr := m.Write(slices.Clone(c.batch))
			perr := pl.write(slices.Clone(c.batch))
			for _, class := range []error{engine.ErrInvalid, engine.ErrBeforeHorizon} {
				if errors.Is(merr, class) != errors.Is(perr, class) {
					t.Errorf("the model: %v; the planner: %v (%v)", merr, perr, class)
				}
			}
			if (merr == nil) != (perr == nil) {
				t.Errorf("the model: %v; the planner: %v", merr, perr)
			}
			if err := pl.sameAsModel(m); err != nil {
				t.Error(err)
			}
		})
	}
}

// both is what a case's setup does its writes and retentions through.
type both interface {
	write([]engine.Record) error
	retain(time.Time)
}

// modelAndPlanner does one write or retention to both.
type modelAndPlanner struct {
	m  *doltmodel.Model
	pl *planner
}

func (b modelAndPlanner) write(batch []engine.Record) error {
	if err := b.m.Write(slices.Clone(batch)); err != nil {
		return err
	}
	return b.pl.write(slices.Clone(batch))
}

func (b modelAndPlanner) retain(h time.Time) {
	_ = b.m.Retain(h)
	b.pl.retain(h)
}

// genRecord draws one record of a small universe, as the model's own property test does:
// two pods, a node, two relations, three producers, a minute of instants that share, deletes,
// runs and entity records.
func genRecord(t *rapid.T, label string, floorSecs int64) engine.Record {
	edges := []engine.Subject{edge1, edge2, edge3}
	subject := rapid.SampledFrom(append(edges, engine.EntitySubject(pod1), engine.EntitySubject(node1))).Draw(t, label+" subject")
	producer := rapid.SampledFrom([]lifecycle.Producer{"a", "b", "c"}).Draw(t, label+" producer")
	secs := rapid.Int64Range(max(floorSecs, 0), 60).Draw(t, label+" secs")
	r := engine.Record{Layer: catalog.L2, Subject: subject, Producer: producer, EventTime: at(secs), Kind: lifecycle.Observe, Payload: []byte{1}}
	if subject.Kind == engine.SubjectEntity {
		e, _ := catalog.Default().Entity(subject.A.Type())
		r.Layer = e.Layer()
	}
	switch rapid.IntRange(0, 5).Draw(t, label+" shape") {
	case 0:
		r.Kind, r.Payload = lifecycle.Delete, nil
	case 1:
		r.TTL = time.Duration(rapid.Int64Range(1, 30).Draw(t, label+" ttl")) * time.Second
	case 2:
		r.TTL = time.Duration(rapid.Int64Range(1, 30).Draw(t, label+" ttl")) * time.Second
		r.Through = at(secs + rapid.Int64Range(0, 20).Draw(t, label+" through"))
	}
	return r
}

// TestRandomStreamsPlanTheModelsTables is the same comparison on random streams of a tiny
// universe, with retentions.
func TestRandomStreamsPlanTheModelsTables(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		m := doltmodel.New(doltmodel.WithPayloadCapacity(doltmodel.RegisteredPayloadCapacity))
		pl := newPlanner(&faults{})
		seq := uint64(rapid.SampledFrom([]uint64{0, 1<<32 - 3, 1<<63 - 3}).Draw(t, "first seq"))
		var horizon time.Time
		for b := range rapid.IntRange(1, 8).Draw(t, "batches") {
			n := rapid.IntRange(1, 6).Draw(t, "batch size")
			var batch []engine.Record
			for i := range n {
				r := genRecord(t, fmt.Sprintf("record %d.%d", b, i), doltmodel.Nanos(horizon)/int64(time.Second))
				seq++
				r.Seq = seq
				batch = append(batch, r)
			}
			if err := m.Write(slices.Clone(batch)); err != nil {
				t.Fatal(err)
			}
			if err := pl.write(slices.Clone(batch)); err != nil {
				t.Fatal(err)
			}
			if rapid.IntRange(0, 3).Draw(t, "retain") == 0 {
				h := at(rapid.Int64Range(10, 60).Draw(t, "horizon"))
				if h.After(horizon) {
					horizon = h
				}
				_ = m.Retain(h)
				pl.retain(h)
			}
			if err := pl.sameAsModel(m); err != nil {
				t.Fatal(err)
			}
		}
	})
}
