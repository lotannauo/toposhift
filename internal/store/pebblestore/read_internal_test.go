package pebblestore

import (
	"errors"
	"slices"
	"testing"
	"time"

	"github.com/cockroachdb/pebble/v2"

	"github.com/lotannauo/toposhift/internal/catalog"
	"github.com/lotannauo/toposhift/internal/identity"
	"github.com/lotannauo/toposhift/internal/lifecycle"
	"github.com/lotannauo/toposhift/internal/store"
	"github.com/lotannauo/toposhift/internal/store/memstore"
	"github.com/lotannauo/toposhift/internal/store/storetest"
)

func writeTiny(t *testing.T, stores ...store.Store) []store.Record {
	t.Helper()
	cfg := storetest.Tiny()
	cfg.Duration = 15 * time.Minute
	cfg.Runs = true
	cfg.ConfirmProbability, cfg.ConfirmTTL = 0.3, 3*time.Minute
	g, err := storetest.NewGenerator(cfg)
	if err != nil {
		t.Fatal(err)
	}
	recs := g.All()
	for i := 0; i < len(recs); i += 50 {
		for _, s := range stores {
			if err := s.Write(bg, cloneAll(recs[i:min(i+50, len(recs))])); err != nil {
				t.Fatal(err)
			}
		}
	}
	return recs
}

// The parts of Breakdown add up to the bytes of every data key and value, counted
// straight off the database: nothing is counted twice and nothing is left out,
// including the baseline and, where the store writes them, the checkpoints.
func TestBreakdownAddsUpToTheBytesOnDisk(t *testing.T) {
	t.Parallel()
	for name, ckpt := range map[string]*CheckpointOptions{
		"without checkpoints": {},
		"with checkpoints":    {On: true, KMin: 4, Alpha: 1, Lag: time.Nanosecond},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			s := openMem(t, Options{Checkpoints: ckpt})
			recs := writeTiny(t, s)
			// Before any retention the stream's run extensions are there to be counted.
			if parts, err := s.Instrument().Breakdown(); err != nil || !hasKind(parts, partExtension) {
				t.Fatalf("no extension bytes in a stream with runs: %v, %v", parts, err)
			}
			if err := s.Retain(bg, recs[len(recs)/2].EventTime); err != nil {
				t.Fatal(err)
			}
			if err := s.kv.Settle(); err != nil {
				t.Fatal(err)
			}

			lo, hi := dataBounds()
			it, err := s.kv.NewIter(&pebble.IterOptions{LowerBound: lo, UpperBound: hi})
			if err != nil {
				t.Fatal(err)
			}
			var raw int64
			for ok := it.First(); ok; ok = it.Next() {
				raw += int64(len(it.Key()) + len(it.Value()))
			}
			_ = it.Close()

			parts, err := s.Instrument().Breakdown()
			if err != nil {
				t.Fatal(err)
			}
			var sum int64
			kinds := map[string]bool{}
			for p, n := range parts {
				sum += n
				kinds[p.Kind] = true
			}
			if sum != raw || raw == 0 {
				t.Errorf("the parts add up to %d bytes, the keys and values to %d", sum, raw)
			}
			for _, k := range []string{partBaseline, partObserve, partPayloadForward, partPayloadReverse, partPayloadEntity} {
				if !kinds[k] {
					t.Errorf("no %s bytes after a retention: %v", k, kinds)
				}
			}
			if kinds[partCheckpoint] != ckpt.On {
				t.Errorf("checkpoint bytes present = %v, with the policy on = %v", kinds[partCheckpoint], ckpt.On)
			}
		})
	}
}

func hasKind(parts map[Part]int64, kind string) bool {
	for p, n := range parts {
		if p.Kind == kind && n > 0 {
			return true
		}
	}
	return false
}

// Every iterator statistic is recorded, under the read that made it, for each
// kind of read. A read that forgot to report would show up as a missing name.
func TestEveryKindOfReadReportsItsIteratorStatistics(t *testing.T) {
	t.Parallel()
	rec := newMemRecorder()
	s := openMem(t, Options{Recorder: rec})
	recs := writeTiny(t, s)
	if err := s.kv.Settle(); err != nil {
		t.Fatal(err)
	}
	clear(rec.counters)
	clear(rec.samples)
	r := recs[len(recs)-1]
	at := r.EventTime
	fp := r.Subject.A
	scope := store.Current(r.Layer)
	if _, err := s.Neighbors(bg, fp, store.Forward, at, scope); err != nil {
		t.Fatal(err)
	}
	if _, err := s.NeighborsBatch(bg, []identity.Fingerprint{fp, fp}, store.Forward, at, scope); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Alive(bg, fp, at, scope); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Window(bg, fp, store.Forward, at.Add(-time.Hour), at.Add(time.Nanosecond), scope); err != nil {
		t.Fatal(err)
	}
	if _, err := s.EntityWindow(bg, fp, at.Add(-time.Hour), at.Add(time.Nanosecond), scope); err != nil {
		t.Fatal(err)
	}
	for _, op := range []string{"neighbors", "batch", "alive", "window", "entitywindow"} {
		if rec.counters["read."+op+".reads"] != 1 {
			t.Errorf("read.%s.reads = %d, want 1", op, rec.counters["read."+op+".reads"])
		}
		for _, name := range []string{"block_bytes", "points", "seeks", "steps", "internal_seeks", "internal_steps"} {
			if len(rec.samples["read."+op+"."+name]) != 1 {
				t.Errorf("read.%s.%s was not sampled once", op, name)
			}
		}
	}
}

// EntityWindow gives the existence records of the entity and never an edge's, and
// agrees with the reference on every window, token and bound.
func TestEntityWindowAgreesWithTheReference(t *testing.T) {
	t.Parallel()
	s := openMem(t, Options{})
	ref, err := memstore.Open(memstore.Options{})
	if err != nil {
		t.Fatal(err)
	}
	recs := writeTiny(t, s, ref)
	g, err := storetest.NewGenerator(func() storetest.Config { c := storetest.Tiny(); c.Duration = 15 * time.Minute; return c }())
	if err != nil {
		t.Fatal(err)
	}
	first, last := recs[0].EventTime, recs[len(recs)-1].EventTime
	checked := 0
	for _, fp := range g.Entities() {
		layer := recs[0].Layer
		for _, r := range recs {
			if r.Subject.Kind == store.SubjectEntity && r.Subject.A == fp {
				layer = r.Layer
				break
			}
		}
		for _, tok := range []uint64{0, uint64(len(recs) / 2), store.Latest} {
			sc := store.Scope{Layer: layer, AsOf: tok}
			for _, w := range [][2]time.Time{
				{first.Add(-time.Hour), last.Add(time.Hour)},
				{first, last},
				{first.Add(time.Minute), first.Add(5 * time.Minute)},
				{last, last},
				{last.Add(time.Nanosecond), last},
				{last, last.Add(time.Nanosecond)},
			} {
				want, err := ref.EntityWindow(bg, fp, w[0], w[1], sc)
				if err != nil {
					t.Fatal(err)
				}
				got, err := s.EntityWindow(bg, fp, w[0], w[1], sc)
				if err != nil || !equalRecords(got, want) {
					t.Fatalf("EntityWindow(%s, %v, %v, asOf %d) = %v, %v; the reference says %v", fp, w[0], w[1], tok, got, err, want)
				}
				for _, r := range got {
					if r.Subject.Kind != store.SubjectEntity || r.Subject.A != fp {
						t.Fatalf("EntityWindow returned %v", r.Subject)
					}
				}
				checked += len(got)
			}
		}
	}
	if checked == 0 {
		t.Fatal("no entity record was ever returned; the test checks nothing")
	}
}

func equalRecords(a, b []store.Record) bool {
	return slices.EqualFunc(a, b, func(x, y store.Record) bool {
		return x.Layer == y.Layer && x.Subject == y.Subject && x.Producer == y.Producer && x.EventTime.Equal(y.EventTime) &&
			x.Seq == y.Seq && x.Kind == y.Kind && x.TTL == y.TTL && x.Through.Equal(y.Through) && slices.Equal(x.Payload, y.Payload) &&
			x.Boot == y.Boot && x.EventTimeBasis == y.EventTimeBasis
	})
}

// The basis of a record's event time, and its boot, come back from Window and
// EntityWindow exactly.
func TestWindowsReturnTheBasisAndTheBoot(t *testing.T) {
	t.Parallel()
	host := fingerprintOf(catalog.Host, 0x60)
	rack := fingerprintOf(catalog.Rack, 0x70)
	s := openMem(t, Options{})
	var recs []store.Record
	seq := uint64(0)
	for basis := store.BasisUnknown; basis <= store.BasisProducerEvent; basis++ {
		seq++
		recs = append(recs, store.Record{
			Layer: catalog.L1, Subject: store.EntitySubject(host), Producer: "agent", EventTime: t0.Add(time.Duration(seq) * time.Second),
			Seq: seq, Kind: lifecycle.Observe, Boot: "boot-1", Payload: []byte("h"), EventTimeBasis: basis,
		})
		seq++
		recs = append(recs, store.Record{
			Layer: catalog.L1, Subject: store.EdgeSubject(host, rack, catalog.LocatedIn), Producer: "agent", EventTime: t0.Add(time.Duration(seq) * time.Second),
			Seq: seq, Kind: lifecycle.Observe, Payload: []byte("e"), EventTimeBasis: basis,
		})
	}
	if err := s.Write(bg, cloneAll(recs)); err != nil {
		t.Fatal(err)
	}
	sc := store.Current(catalog.L1)
	ents, err := s.EntityWindow(bg, host, t0, t0.Add(time.Hour), sc)
	if err != nil {
		t.Fatal(err)
	}
	edges, err := s.Window(bg, host, store.Forward, t0, t0.Add(time.Hour), sc)
	if err != nil {
		t.Fatal(err)
	}
	var wantEnts, wantEdges []store.Record
	for _, r := range recs {
		if r.Subject.Kind == store.SubjectEntity {
			wantEnts = append(wantEnts, r)
		} else {
			wantEdges = append(wantEdges, r)
		}
	}
	if !equalRecords(ents, wantEnts) || !equalRecords(edges, wantEdges) {
		t.Errorf("windows returned\n %v\n %v\nwant\n %v\n %v", ents, edges, wantEnts, wantEdges)
	}
	// A retention carries the basis in the baseline's entries, and the fold of what
	// is left is the reference's.
	if err := s.Retain(bg, t0.Add(time.Hour)); err != nil {
		t.Fatal(err)
	}
	if ok, err := s.Alive(bg, host, t0.Add(time.Hour), sc); err != nil || !ok {
		t.Errorf("Alive after the retention = %v, %v", ok, err)
	}
}

// Arguments are checked before the horizon, and a token above LastSeq that is not
// Latest reads the whole snapshot.
func TestReadsCheckArgumentsBeforeTheHorizonAndTolerateALargeToken(t *testing.T) {
	t.Parallel()
	p := newPair(t, Options{})
	p.write(edgeRecord(1, "p", t0, lifecycle.Observe, 0))
	p.retain(t0.Add(time.Minute))
	before := t0.Add(-time.Hour)
	if _, err := p.s.Neighbors(bg, podFP, 0, before, store.Current(catalog.L2)); errors.Is(err, store.ErrBeforeHorizon) || !errors.Is(err, store.ErrInvalid) {
		t.Errorf("a bad direction before the horizon = %v, want ErrInvalid and not ErrBeforeHorizon", err)
	}
	if _, err := p.s.Alive(bg, podFP, before, store.Scope{AsOf: store.Latest}); errors.Is(err, store.ErrBeforeHorizon) || !errors.Is(err, store.ErrInvalid) {
		t.Errorf("no layer before the horizon = %v, want ErrInvalid and not ErrBeforeHorizon", err)
	}
	if _, err := p.s.NeighborsBatch(bg, []identity.Fingerprint{podFP, {}}, store.Forward, before, store.Current(catalog.L2)); errors.Is(err, store.ErrBeforeHorizon) || !errors.Is(err, store.ErrInvalid) {
		t.Errorf("a zero fingerprint before the horizon = %v, want ErrInvalid and not ErrBeforeHorizon", err)
	}
	for name, run := range map[string]func() error{
		"Neighbors": func() error {
			_, err := p.s.Neighbors(bg, podFP, store.Forward, before, store.Current(catalog.L2))
			return err
		},
		"Alive": func() error { _, err := p.s.Alive(bg, podFP, before, store.Current(catalog.L2)); return err },
		"Window": func() error {
			_, err := p.s.Window(bg, podFP, store.Forward, before, t0, store.Current(catalog.L2))
			return err
		},
		"EntityWindow": func() error { _, err := p.s.EntityWindow(bg, podFP, before, t0, store.Current(catalog.L2)); return err },
		"a token below the horizon's seq": func() error {
			_, err := p.s.Neighbors(bg, podFP, store.Forward, t0.Add(time.Hour), store.Scope{Layer: catalog.L2, AsOf: 0})
			return err
		},
	} {
		if err := run(); !errors.Is(err, store.ErrBeforeHorizon) {
			t.Errorf("%s before the horizon = %v, want ErrBeforeHorizon", name, err)
		}
	}
	p.same([]identity.Fingerprint{podFP, nodeFP}, []time.Time{t0.Add(time.Hour)}, []uint64{1, 99, store.Latest})
}
