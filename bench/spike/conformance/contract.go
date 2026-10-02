package conformance

import (
	"errors"
	"fmt"
	"slices"
	"time"

	"github.com/lotannauo/toposhift/bench/spike/engine"
	"github.com/lotannauo/toposhift/bench/spike/workload"
	"github.com/lotannauo/toposhift/internal/catalog"
	"github.com/lotannauo/toposhift/internal/identity"
	"github.com/lotannauo/toposhift/internal/lifecycle"
)

// CheckWriteContract checks that an engine refuses records out of sequence and
// records its validation rules say are invalid, that a refused batch is
// refused whole (its valid records are not stored and their sequence numbers
// are not consumed), and that the retention horizon only moves forward. It
// leaves the engine with a retention horizon, so use a fresh engine.
func CheckWriteContract(cand engine.Engine) error {
	g, err := workload.New(workload.Tiny())
	if err != nil {
		return err
	}
	recs := g.Batch(12)
	if cand.LastSeq() != 0 {
		return fmt.Errorf("a new engine has LastSeq %d, want 0", cand.LastSeq())
	}
	if err := cand.Write(cloneRecords(recs[:10])); err != nil {
		return fmt.Errorf("a valid batch was refused: %w", err)
	}
	if got, want := cand.LastSeq(), recs[9].Seq; got != want {
		return fmt.Errorf("LastSeq = %d after a batch ending at seq %d", got, want)
	}
	if err := cand.Write(cloneRecords(recs[:1])); !errors.Is(err, engine.ErrInvalid) {
		return fmt.Errorf("a repeated seq must be refused with ErrInvalid, got %w", orNil(err))
	}

	good := recs[10]
	broken := map[string]func(engine.Record) engine.Record{
		"an event time before 1970": func(r engine.Record) engine.Record {
			r.EventTime = time.Date(1969, 1, 1, 0, 0, 0, 0, time.UTC)
			return r
		},
		"an empty producer": func(r engine.Record) engine.Record { r.Producer = ""; return r },
		"an unset layer":    func(r engine.Record) engine.Record { r.Layer = 0; return r },
		"an unset kind":     func(r engine.Record) engine.Record { r.Kind = 0; return r },
		"an entity in a layer other than its type's": func(r engine.Record) engine.Record {
			// Any entity will do: its layer is moved to a different, valid one.
			r.Subject = engine.EntitySubject(r.Subject.A)
			e, _ := catalog.Default().Entity(r.Subject.A.Type())
			r.Layer = e.Layer()%catalog.L3 + 1
			r.TTL, r.Through, r.Payload = 0, time.Time{}, nil
			r.Kind = lifecycle.Observe
			return r
		},
		"a delete with a TTL": func(r engine.Record) engine.Record {
			r.Kind, r.Payload, r.TTL = lifecycle.Delete, nil, time.Minute
			return r
		},
	}
	names := make([]string, 0, len(broken))
	for name := range broken {
		names = append(names, name)
	}
	slices.Sort(names)
	for _, name := range names {
		bad := broken[name](recs[11])
		// A valid record followed by an invalid one: the whole batch is refused.
		if err := cand.Write(cloneRecords([]engine.Record{good, bad})); !errors.Is(err, engine.ErrInvalid) {
			return fmt.Errorf("a batch with %s must be refused with ErrInvalid, got %w", name, orNil(err))
		}
	}
	if got, want := cand.LastSeq(), recs[9].Seq; got != want {
		return fmt.Errorf("refused batches moved LastSeq to %d, want %d", got, want)
	}
	// If any refused batch had stored its valid record, that record's sequence
	// number would now be used and this would fail.
	if err := cand.Write(cloneRecords([]engine.Record{good})); err != nil {
		return fmt.Errorf("a refused batch left its valid record behind: %w", err)
	}

	// The horizon only moves forward: a later, earlier-dated Retain must not
	// bring back the window between the two.
	start := recs[0].EventTime
	if err := cand.Retain(start.Add(2 * time.Hour)); err != nil {
		return fmt.Errorf("retaining: %w", err)
	}
	if err := cand.Retain(start.Add(time.Hour)); err != nil {
		return fmt.Errorf("an earlier Retain must be accepted and ignored: %w", err)
	}
	between := recs[11]
	between.Seq, between.EventTime = recs[11].Seq+1000, start.Add(90*time.Minute)
	if err := cand.Write(cloneRecords([]engine.Record{between})); !errors.Is(err, engine.ErrBeforeHorizon) {
		return fmt.Errorf("after Retain(+2h) then Retain(+1h), a record at +90m must still be refused with ErrBeforeHorizon, got %w", orNil(err))
	}
	if got, want := cand.LastSeq(), good.Seq; got != want {
		return fmt.Errorf("a retention or a refused record moved LastSeq to %d, want %d", got, want)
	}

	// A batch that mixes a record before the horizon with valid ones is refused
	// whole: the valid ones are not stored and their sequence numbers stay
	// unused. The horizon is checked against engine state, so an engine can
	// notice it only after it has already started on the valid records.
	fresh := recs[11]
	fresh.Seq, fresh.EventTime = good.Seq+1, start.Add(3*time.Hour)
	stale := recs[11]
	stale.Seq, stale.EventTime = good.Seq+2, start.Add(90*time.Minute)
	later := recs[11]
	later.Seq, later.EventTime = good.Seq+3, start.Add(4*time.Hour)
	if err := cand.Write(cloneRecords([]engine.Record{fresh, stale, later})); !errors.Is(err, engine.ErrBeforeHorizon) {
		return fmt.Errorf("a batch with a record before the horizon must be refused with ErrBeforeHorizon, got %w", orNil(err))
	}
	if got := cand.LastSeq(); got != good.Seq {
		return fmt.Errorf("a refused batch moved LastSeq to %d, want %d", got, good.Seq)
	}
	if err := cand.Write(cloneRecords([]engine.Record{fresh, later})); err != nil {
		return fmt.Errorf("a refused batch left some of its records behind: %w", err)
	}
	if got := cand.LastSeq(); got != later.Seq {
		return fmt.Errorf("LastSeq = %d after the batch ending at seq %d", got, later.Seq)
	}
	return nil
}

// orNil makes "accepted" print as an error value.
func orNil(err error) error {
	if err == nil {
		return errors.New("no error")
	}
	return err
}

// CheckReadContract checks that an engine refuses a read whose scope names no
// layer, and answers a read at any instant, even one far outside the range
// event times can take, without error. Use a fresh engine.
func CheckReadContract(cand engine.Engine) error {
	g, err := workload.New(workload.Tiny())
	if err != nil {
		return err
	}
	recs := g.Batch(5)
	if err := cand.Write(cloneRecords(recs)); err != nil {
		return fmt.Errorf("a valid batch was refused: %w", err)
	}
	fp := recs[0].Subject.A
	at := recs[0].EventTime
	for _, sc := range []engine.Scope{{}, {AsOf: engine.Latest}, {Layer: catalog.L3 + 1, AsOf: engine.Latest}} {
		if _, err := cand.Neighbors(fp, engine.Forward, at, sc); !errors.Is(err, engine.ErrInvalid) {
			return fmt.Errorf("reading Neighbors with scope %+v must fail with ErrInvalid, got %w", sc, orNil(err))
		}
		if _, err := cand.NeighborsBatch([]identity.Fingerprint{fp}, engine.Forward, at, sc); !errors.Is(err, engine.ErrInvalid) {
			return fmt.Errorf("NeighborsBatch with scope %+v must fail with ErrInvalid, got %w", sc, orNil(err))
		}
		if _, err := cand.Alive(fp, at, sc); !errors.Is(err, engine.ErrInvalid) {
			return fmt.Errorf("reading Alive with scope %+v must fail with ErrInvalid, got %w", sc, orNil(err))
		}
		if _, err := cand.Window(fp, engine.Forward, at, at.Add(time.Hour), sc); !errors.Is(err, engine.ErrInvalid) {
			return fmt.Errorf("reading Window with scope %+v must fail with ErrInvalid, got %w", sc, orNil(err))
		}
	}
	// Any instant may be asked about, however far outside the range of event
	// times.
	for _, t := range []time.Time{{}, time.Date(9999, 12, 31, 0, 0, 0, 0, time.UTC), engine.MinEventTime.Add(-time.Nanosecond), engine.MaxEventTime.Add(time.Nanosecond)} {
		sc := engine.Current(recs[0].Layer)
		if _, err := cand.Alive(fp, t, sc); err != nil {
			return fmt.Errorf("reading Alive at %s: %w", t.Format(time.RFC3339Nano), err)
		}
		if _, err := cand.Neighbors(fp, engine.Forward, t, sc); err != nil {
			return fmt.Errorf("reading Neighbors at %s: %w", t.Format(time.RFC3339Nano), err)
		}
		if _, err := cand.Window(fp, engine.Forward, t, t.Add(time.Hour), sc); err != nil {
			return fmt.Errorf("reading Window from %s: %w", t.Format(time.RFC3339Nano), err)
		}
	}
	return nil
}
