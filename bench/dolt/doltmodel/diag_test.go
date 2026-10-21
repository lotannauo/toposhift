package doltmodel_test

import (
	"fmt"
	"slices"
	"testing"
	"time"

	"github.com/lotannauo/toposhift/bench/dolt/doltmodel"
	"github.com/lotannauo/toposhift/bench/spike/engine"
	"github.com/lotannauo/toposhift/bench/spike/oracle"
	"github.com/lotannauo/toposhift/bench/spike/workload"
	"github.com/lotannauo/toposhift/internal/catalog"
	"github.com/lotannauo/toposhift/internal/identity"
	"github.com/lotannauo/toposhift/internal/lifecycle"
)

// wrongAnswers counts the neighbor lists and existence answers an engine gives at the
// instants, as of the newest token, that differ from the reference engine's.
func wrongAnswers(t *testing.T, e engine.Engine, ora *oracle.Oracle, fps []identity.Fingerprint, instants []time.Time) (wrong, asked int) {
	t.Helper()
	sc := engine.Current(catalog.L2)
	for _, at := range instants {
		for _, fp := range fps {
			for _, dir := range []engine.Direction{engine.Forward, engine.Reverse} {
				got, err := e.Neighbors(fp, dir, at, sc)
				if err != nil {
					t.Fatal(err)
				}
				want, err := ora.Neighbors(fp, dir, at, sc)
				if err != nil {
					t.Fatal(err)
				}
				asked++
				if !slices.Equal(got, want) {
					wrong++
				}
			}
			got, err := e.Alive(fp, at, sc)
			if err != nil {
				t.Fatal(err)
			}
			want, err := ora.Alive(fp, at, sc)
			if err != nil {
				t.Fatal(err)
			}
			asked++
			if got != want {
				wrong++
			}
		}
	}
	return wrong, asked
}

func entityRecord(fp identity.Fingerprint, seq uint64, p lifecycle.Producer, secs, ttl int64) engine.Record {
	return engine.Record{
		Layer: catalog.L2, Subject: engine.EntitySubject(fp), Producer: p, EventTime: at(secs), Seq: seq,
		Kind: lifecycle.Observe, TTL: time.Duration(ttl) * time.Second,
	}
}

// inOrderStream is a stream whose event times rise strictly with the sequence numbers, in
// batches of two: every record is on time, and no batch shares an instant with the next.
func inOrderStream() [][]engine.Record {
	return [][]engine.Record{
		{observe(edge1, 1, "a", 100, 60), entityRecord(pod1, 2, "a", 101, 0)},
		{observe(edge2, 3, "a", 110, 0), del(edge1, 4, "a", 120)},
		{observe(edge1, 5, "a", 130, 20), entityRecord(node1, 6, "a", 135, 0)},
		{del(edge2, 7, "a", 140), observe(edge3, 8, "b", 150, 10)},
		{entityRecord(pod1, 9, "a", 160, 5), observe(edge2, 10, "a", 170, 0)},
	}
}

// TestTheDiagnosticIsRightOnAnInOrderStreamReadAtBatchFrontiers: with no late record, a
// commit's frontier is when its batch happened, and the commit-time reading, at the instant
// of a batch, is the true answer.
func TestTheDiagnosticIsRightOnAnInOrderStreamReadAtBatchFrontiers(t *testing.T) {
	d := doltmodel.NewCommitTime()
	ora := oracle.New()
	var frontiers []time.Time
	for _, batch := range inOrderStream() {
		write(t, d.Model, batch...)
		if err := ora.Write(batch); err != nil {
			t.Fatal(err)
		}
		head, _ := d.Head()
		frontiers = append(frontiers, doltmodel.FromNanos(head.Frontier))
		wrong, asked := wrongAnswers(t, d, ora, []identity.Fingerprint{pod1, pod2, node1}, frontiers[len(frontiers)-1:])
		if wrong != 0 || asked == 0 {
			t.Fatalf("after batch %d: %d of %d answers wrong at its frontier", len(frontiers), wrong, asked)
		}
	}
	// At every batch frontier, as of the end.
	if wrong, asked := wrongAnswers(t, d, ora, []identity.Fingerprint{pod1, pod2, node1}, frontiers); wrong != 0 {
		t.Fatalf("%d of %d answers wrong", wrong, asked)
	}
	if p := d.Paths(); p["neighbors/commit-time"] == 0 || p["alive/commit-time"] == 0 || p["neighbors/as-of-alone"] != 0 || p["neighbors/needed-log"] != 0 {
		t.Fatalf("the diagnostic read the wrong tables: %v", p)
	}
}

// lateDeleteStream writes a delete from 150 after a batch with an event time of 200, to
// the diagnostic and the reference engine, and returns how many of the answers at 170 the
// diagnostic gets wrong, and how many the design itself does. The delete is in a commit
// whose frontier is 200, so the commit-time reading of 170 finds the commit before it, and
// an edge that was already gone.
func lateDeleteStream(t *testing.T, d *doltmodel.CommitTime) (diagnostic, design int) {
	t.Helper()
	ora := oracle.New()
	for _, batch := range [][]engine.Record{
		{observe(edge1, 1, "a", 100, 0)},
		{observe(edge2, 2, "a", 200, 0)},
		{del(edge1, 3, "a", 150)}, // late: from before the batch before it
	} {
		write(t, d.Model, batch...)
		if err := ora.Write(batch); err != nil {
			t.Fatal(err)
		}
	}
	fps, instants := []identity.Fingerprint{node1, pod1, pod2}, []time.Time{at(170)}
	diagnostic, _ = wrongAnswers(t, d, ora, fps, instants)
	design, _ = wrongAnswers(t, d.Model, ora, fps, instants)
	return diagnostic, design
}

// wantTwoWrong is the check on the diagnostic: node1's reverse list and pod1's forward list
// still hold the deleted edge, and nothing else is wrong.
func wantTwoWrong(diagnostic, design int) error {
	if diagnostic != 2 || design != 0 {
		return fmt.Errorf("the diagnostic gave %d wrong answers (want 2) and the design %d (want 0)", diagnostic, design)
	}
	return nil
}

func TestTheDiagnosticIsWrongWhenARecordArrivesLate(t *testing.T) {
	if err := wantTwoWrong(lateDeleteStream(t, doltmodel.NewCommitTime())); err != nil {
		t.Fatal(err)
	}
}

// TestTheDiagnosticCountsWrongAnswersOnAStreamWithLateRecords: on a generated stream with
// heavy lateness, written to the diagnostic and to the reference engine a batch at a time,
// the commit-time reading is wrong somewhere and the real design is not.
func TestTheDiagnosticCountsWrongAnswersOnAStreamWithLateRecords(t *testing.T) {
	cfg := workload.Tiny()
	cfg.Seed = 7
	cfg.LateProbability, cfg.LateMeanDelay = 0.4, 5*time.Minute
	g, err := workload.New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	d := doltmodel.NewCommitTime()
	ora := oracle.New()
	var all []engine.Record
	for {
		batch := g.Batch(40)
		if len(batch) == 0 {
			break
		}
		write(t, d.Model, slices.Clone(batch)...)
		if err := ora.Write(slices.Clone(batch)); err != nil {
			t.Fatal(err)
		}
		all = append(all, batch...)
	}
	layers := entityLayers(all)
	var fps []identity.Fingerprint
	for _, fp := range sortedEntities(layers) {
		if slices.Contains(layers[fp], catalog.L2) {
			fps = append(fps, fp)
		}
	}
	var instants []time.Time
	lapse := lapseInstants(all)
	for i := 0; i < len(lapse); i += len(lapse)/40 + 1 {
		instants = append(instants, doltmodel.FromNanos(lapse[i]))
	}
	wrong, asked := wrongAnswers(t, d, ora, fps, instants)
	if wrong == 0 {
		t.Fatalf("0 of %d answers wrong on a stream with late records", asked)
	}
	if real, _ := wrongAnswers(t, d.Model, ora, fps, instants); real != 0 {
		t.Fatalf("the real design gave %d wrong answers", real)
	}
	t.Logf("diagnostic: %d of %d answers wrong (deterministic)", wrong, asked)
}

// TestTheDiagnosticThatUsesTheLogIsCaught: the diagnostic must read commits, not the log, or
// its count is zero and says nothing. With the log substituted, the check above fails.
func TestTheDiagnosticThatUsesTheLogIsCaught(t *testing.T) {
	d := doltmodel.NewCommitTime()
	d.Break(doltmodel.FaultDiagUsesLog)
	if err := wantTwoWrong(lateDeleteStream(t, d)); err == nil {
		t.Fatal("a diagnostic that answers from the log passed the check")
	}
	if p := d.Paths(); p["neighbors/commit-time"] != 0 {
		t.Fatalf("paths %v", p)
	}
}
