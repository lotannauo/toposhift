package doltengine_test

import (
	"fmt"
	"maps"
	"slices"
	"testing"
	"time"

	"github.com/lotannauo/toposhift/bench/dolt/doltengine"
	"github.com/lotannauo/toposhift/bench/dolt/doltmodel"
	"github.com/lotannauo/toposhift/bench/spike/engine"
	"github.com/lotannauo/toposhift/bench/spike/oracle"
	"github.com/lotannauo/toposhift/bench/spike/workload"
	"github.com/lotannauo/toposhift/internal/catalog"
	"github.com/lotannauo/toposhift/internal/identity"
)

// A frontier of the commit a batch made is the largest event time written so far, which the
// loader computes; the diagnostic reading is held to the model's ([doltmodel.CommitTime]), to
// the reference engine at the instants where it must be right, and to the reference at
// the instants where it must be wrong.

// framed writes batches to an engine and gives the frontier of each commit.
type framed struct {
	e         *doltengine.Engine
	frontier  int64
	frontiers []int64
}

func newFramed(e *doltengine.Engine) *framed { return &framed{e: e, frontier: doltmodel.NoFrontier} }

func (f *framed) write(t testing.TB, batch []engine.Record) {
	t.Helper()
	if err := f.e.Write(clone(batch)); err != nil {
		t.Fatal(err)
	}
	for _, r := range batch {
		f.frontier = max(f.frontier, doltmodel.Nanos(r.EventTime))
	}
	if head, ok := f.e.Head(); ok {
		for len(f.frontiers) < head.Ordinal {
			f.frontiers = append(f.frontiers, f.frontier)
		}
	}
}

func (f *framed) diag(t testing.TB) *doltengine.CommitTime {
	t.Helper()
	d, err := doltengine.NewCommitTime(f.e, f.frontiers)
	if err != nil {
		t.Fatal(err)
	}
	return d
}

func wrongAnswers(t *testing.T, e engine.Engine, ora *oracle.Oracle, fps []identity.Fingerprint, instants []time.Time) (wrong, asked int) {
	t.Helper()
	sc := engine.Current(catalog.L2)
	for _, instant := range instants {
		for _, fp := range fps {
			for _, dir := range []engine.Direction{engine.Forward, engine.Reverse} {
				got, err := e.Neighbors(fp, dir, instant, sc)
				if err != nil {
					t.Fatal(err)
				}
				want, _ := ora.Neighbors(fp, dir, instant, sc)
				asked++
				if !slices.Equal(got, want) {
					wrong++
				}
			}
			got, err := e.Alive(fp, instant, sc)
			if err != nil {
				t.Fatal(err)
			}
			want, _ := ora.Alive(fp, instant, sc)
			asked++
			if got != want {
				wrong++
			}
		}
	}
	return wrong, asked
}

// inOrderStream is a stream whose event times rise strictly with the sequence numbers, in
// batches of two: every record is on time, and no batch shares an instant with the next.
func inOrderStream() [][]engine.Record {
	return [][]engine.Record{
		{observe(edge1, 1, "a", 100, 60), existence(pod1, 2, "a", 101, 0)},
		{observe(edge2, 3, "a", 110, 0), del(edge1, 4, "a", 120)},
		{observe(edge1, 5, "a", 130, 20), existence(node1, 6, "a", 135, 0)},
		{del(edge2, 7, "a", 140), observe(edge3, 8, "b", 150, 10)},
		{existence(pod1, 9, "a", 160, 5), observe(edge2, 10, "a", 170, 0)},
	}
}

func TestTheDiagnosticOnAServerIsRightOnAnInOrderStreamReadAtBatchFrontiers(t *testing.T) {
	f := newFramed(openEngine(t, doltmodel.ArmCommit))
	ora := oracle.New()
	fps := []identity.Fingerprint{pod1, pod2, node1}
	var instants []time.Time
	for _, batch := range inOrderStream() {
		f.write(t, batch)
		if err := ora.Write(clone(batch)); err != nil {
			t.Fatal(err)
		}
		instants = append(instants, doltmodel.FromNanos(f.frontier))
		if wrong, asked := wrongAnswers(t, f.diag(t), ora, fps, instants[len(instants)-1:]); wrong != 0 || asked == 0 {
			t.Fatalf("after batch %d: %d of %d answers wrong at its frontier", len(instants), wrong, asked)
		}
	}
	d := f.diag(t)
	if wrong, asked := wrongAnswers(t, d, ora, fps, instants); wrong != 0 {
		t.Fatalf("%d of %d answers wrong at the frontiers, as of the end", wrong, asked)
	}
	if p := d.Paths(); p["neighbors/commit-time"] == 0 || p["alive/commit-time"] == 0 || p["neighbors/as-of-alone"] != 0 || p["neighbors/needed-log"] != 0 {
		t.Fatalf("the diagnostic read the wrong tables: %v", p)
	}
}

// lateDelete writes a delete from 150 after a batch with an event time of 200 and returns how
// many of the answers at 170 the diagnostic gets wrong, and how many the design itself does.
// The delete is in a commit whose frontier is 200, so the commit-time reading of 170 finds the
// commit before it, and an edge that was already gone.
func lateDelete(t *testing.T, mutate func(*doltengine.CommitTime)) (diagnostic, design int) {
	t.Helper()
	f := newFramed(openEngine(t, doltmodel.ArmCommit))
	ora := oracle.New()
	for _, batch := range [][]engine.Record{
		{observe(edge1, 1, "a", 100, 0)},
		{observe(edge2, 2, "a", 200, 0)},
		{del(edge1, 3, "a", 150)}, // late: from before the batch before it
	} {
		f.write(t, batch)
		if err := ora.Write(clone(batch)); err != nil {
			t.Fatal(err)
		}
	}
	d := f.diag(t)
	if mutate != nil {
		mutate(d)
	}
	fps, instants := []identity.Fingerprint{node1, pod1, pod2}, []time.Time{at(170)}
	diagnostic, _ = wrongAnswers(t, d, ora, fps, instants)
	design, _ = wrongAnswers(t, f.e, ora, fps, instants)
	return diagnostic, design
}

func wantTwoWrong(diagnostic, design int) error {
	if diagnostic != 2 || design != 0 {
		return fmt.Errorf("the diagnostic gave %d wrong answers (want 2) and the design %d (want 0)", diagnostic, design)
	}
	return nil
}

func TestTheDiagnosticOnAServerIsWrongWhenARecordArrivesLate(t *testing.T) {
	if err := wantTwoWrong(lateDelete(t, nil)); err != nil {
		t.Fatal(err)
	}
}

// TestTheDiagnosticThatReadsTheDesignWayIsCaught: the diagnostic must read commits and not the
// design's rule, or its count is zero and says nothing.
func TestTheDiagnosticThatReadsTheDesignWayIsCaught(t *testing.T) {
	if err := wantTwoWrong(lateDelete(t, func(d *doltengine.CommitTime) { d.Break(doltengine.FaultDiagUsesLog) })); err == nil {
		t.Fatal("a diagnostic that answers as the design does passed the check")
	}
}

// TestTheDiagnosticOnAServerIsTheModelsOnAStreamWithLateRecords: on a generated stream with
// heavy lateness the diagnostic on the server gives, for every neighbors, batched and existence
// read at instants across the stream, as of the newest token and as of each batch boundary,
// the answer the model's diagnostic gives, and counts the reads as it does; and it is wrong
// somewhere, as the model's is.
func TestTheDiagnosticOnAServerIsTheModelsOnAStreamWithLateRecords(t *testing.T) {
	cfg := workload.Tiny()
	cfg.Seed = 7
	cfg.LateProbability, cfg.LateMeanDelay = 0.4, 5*time.Minute
	g, err := workload.New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	f := newFramed(openEngine(t, doltmodel.ArmCommit))
	md := doltmodel.NewCommitTime(doltmodel.WithPayloadCapacity(doltmodel.RegisteredPayloadCapacity))
	ora := oracle.New()
	var all []engine.Record
	for {
		batch := g.Batch(100)
		if len(batch) == 0 {
			break
		}
		f.write(t, batch)
		if err := md.Write(clone(batch)); err != nil {
			t.Fatal(err)
		}
		if err := ora.Write(clone(batch)); err != nil {
			t.Fatal(err)
		}
		all = append(all, batch...)
	}
	d := f.diag(t)
	if got, want := d.LastSeq(), md.LastSeq(); got != want {
		t.Fatalf("seq %d, model %d", got, want)
	}
	var fps []identity.Fingerprint
	layers := map[identity.Fingerprint]bool{}
	for _, r := range all {
		for _, fp := range []identity.Fingerprint{r.Subject.A, r.Subject.B} {
			if !fp.IsZero() && r.Layer == catalog.L2 && !layers[fp] {
				layers[fp] = true
				fps = append(fps, fp)
			}
		}
	}
	doltmodel.SortFingerprints(fps)
	fps = fps[:min(len(fps), 24)]
	start, end := all[0].EventTime, all[0].EventTime
	for _, r := range all {
		if r.EventTime.Before(start) {
			start = r.EventTime
		}
		if r.EventTime.After(end) {
			end = r.EventTime
		}
	}
	var instants []time.Time
	for at := start.Add(-time.Second); !at.After(end.Add(time.Minute)); at = at.Add(47 * time.Second) {
		instants = append(instants, at)
	}
	tokens := []uint64{engine.Latest}
	for i, c := range f.e.Commits() {
		if i%7 == 0 {
			tokens = append(tokens, c.Token)
		}
	}
	for _, token := range tokens {
		sc := engine.Scope{Layer: catalog.L2, AsOf: token}
		for _, instant := range instants {
			for _, dir := range []engine.Direction{engine.Forward, engine.Reverse} {
				got, err := d.Neighbors(fps[0], dir, instant, sc)
				if err != nil {
					t.Fatal(err)
				}
				want, _ := md.Neighbors(fps[0], dir, instant, sc)
				if !slices.Equal(got, want) {
					t.Fatalf("neighbors %s %s at %s as of %d: %v, the model %v", fps[0], dir, instant, token, got, want)
				}
			}
			gb, err := d.NeighborsBatch(fps, engine.Reverse, instant, sc)
			if err != nil {
				t.Fatal(err)
			}
			wb, _ := md.NeighborsBatch(fps, engine.Reverse, instant, sc)
			for i := range gb {
				if !slices.Equal(gb[i], wb[i]) {
					t.Fatalf("batch list %d at %s as of %d: %v, the model %v", i, instant, token, gb[i], wb[i])
				}
			}
			for _, fp := range fps {
				ga, err := d.Alive(fp, instant, sc)
				if err != nil {
					t.Fatal(err)
				}
				if wa, _ := md.Alive(fp, instant, sc); ga != wa {
					t.Fatalf("alive %s at %s as of %d: %v, the model %v", fp, instant, token, ga, wa)
				}
			}
		}
	}
	if got, want := d.Paths(), md.Paths(); !maps.Equal(got, want) {
		t.Fatalf("the reads were counted differently:\n the server %v\n the model  %v", got, want)
	}
	wrong, asked := wrongAnswers(t, d, ora, fps, instants)
	if wrong == 0 {
		t.Fatalf("0 of %d answers wrong on a stream with late records", asked)
	}
	if real, _ := wrongAnswers(t, f.e, ora, fps, instants); real != 0 {
		t.Fatalf("the design gave %d wrong answers", real)
	}
}

// TestTheDiagnosticOnAServerHoldsARecordUntilItsDeadlineAndNotAt: a record with a TTL holds
// the edge up to its deadline and not at it, as the reference says, at the instant before the
// deadline and at it.
func TestTheDiagnosticOnAServerHoldsARecordUntilItsDeadlineAndNotAt(t *testing.T) {
	f := newFramed(openEngine(t, doltmodel.ArmCommit))
	ora := oracle.New()
	batch := []engine.Record{observe(edge1, 1, "a", 100, 10), existence(pod1, 2, "a", 100, 10)}
	f.write(t, batch)
	if err := ora.Write(clone(batch)); err != nil {
		t.Fatal(err)
	}
	d := f.diag(t)
	sc := engine.Current(catalog.L2)
	for _, tc := range []struct {
		at   int64
		held bool
	}{{99, false}, {100, true}, {109, true}, {110, false}, {111, false}} {
		ns, err := d.Neighbors(pod1, engine.Forward, at(tc.at), sc)
		if err != nil {
			t.Fatal(err)
		}
		alive, err := d.Alive(pod1, at(tc.at), sc)
		if err != nil {
			t.Fatal(err)
		}
		want, _ := ora.Neighbors(pod1, engine.Forward, at(tc.at), sc)
		if tc.at >= 100 { // the frontier is 100: before it the diagnostic knows nothing
			if (len(ns) == 1) != tc.held || alive != tc.held || len(want) != len(ns) {
				t.Errorf("at %d: %d neighbors and alive %v, want held %v (reference %d neighbors)", tc.at, len(ns), alive, tc.held, len(want))
			}
		}
	}
}
