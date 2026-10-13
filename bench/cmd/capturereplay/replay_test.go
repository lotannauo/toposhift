package main

import (
	"context"
	"slices"
	"testing"
	"time"

	"github.com/lotannauo/toposhift/internal/catalog"
	"github.com/lotannauo/toposhift/internal/ingest/k8scluster"
	"github.com/lotannauo/toposhift/internal/ingest/k8sobjects"
	"github.com/lotannauo/toposhift/internal/ingest/resume"
	"github.com/lotannauo/toposhift/internal/replay/capture"
	"github.com/lotannauo/toposhift/internal/store"
)

// replayFixture replays runs written to files, and closes the stores when the test ends.
func replayFixture(t *testing.T, runs ...[][]capture.LogRecord) *replayed {
	t.Helper()
	rep, err := replayRuns(context.Background(), runFilesIn(t, runs...), replayOptions{Tmp: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := rep.close(); err != nil {
			t.Error(err)
		}
	})
	return rep
}

func TestDispatchSendsEntityEventsToTheClusterTranslator(t *testing.T) {
	created := t0.Add(-time.Hour)
	rep := replayFixture(t, [][]capture.LogRecord{{
		podState("u1", "p1", "", created, t0),
		watchRecord("ADDED", podObject("u2", "p2", "", created), t0),
	}})
	for _, p := range []*pass{rep.A, rep.B} {
		r := p.Runs[0]
		if r.Translated[srcCluster] != 1 || r.Translated[srcObjects] != 1 {
			t.Errorf("pass %s: translated %v, want one record from each translator", p.Name, r.Translated)
		}
		if n := r.Skips[srcObjects][k8sobjects.SkipEntityEvent]; n != 0 {
			t.Errorf("pass %s: the objects translator was given %d entity events", p.Name, n)
		}
		if n := r.Skips[srcCluster][k8scluster.SkipNotEntityEvent]; n != 0 {
			t.Errorf("pass %s: the cluster translator was given %d records that are not entity events", p.Name, n)
		}
		if len(r.Subjects[srcCluster][subjPod]) != 1 || len(r.Subjects[srcObjects][subjPod]) != 1 {
			t.Errorf("pass %s: each translator should have written one pod", p.Name)
		}
	}
	for _, c := range []struct {
		name string
		lr   capture.LogRecord
		want source
	}{
		{"entity.state", capture.LogRecord{EventName: "entity.state"}, srcCluster},
		{"entity.delete", capture.LogRecord{EventName: "entity.delete"}, srcCluster},
		{"watch", watchRecord("ADDED", podObject("u", "n", "", created), t0), srcObjects},
		{"pull", pullRecord(podObject("u", "n", "", created), t0), srcObjects},
		{"another event name", capture.LogRecord{EventName: "entity.other"}, srcObjects},
	} {
		if got := dispatch(c.lr); got != c.want {
			t.Errorf("%s: dispatched to %s, want %s", c.name, got, c.want)
		}
	}
}

func TestFileOrderIsKept(t *testing.T) {
	created := t0.Add(-time.Hour)
	at := func(m int) time.Time { return t0.Add(time.Duration(m) * time.Minute) }
	rec := func(uid string, m int) capture.LogRecord {
		return pullRecord(podObject(uid, uid, "", created), at(m))
	}
	// The collector exported these lines in this order; their times are not in it.
	paths := runFilesIn(t, [][]capture.LogRecord{
		{rec("a", 10), rec("b", 9)},
		{rec("c", 0)},
		{rec("d", 5), rec("e", 4)},
	})
	rf, err := scanRun(paths[0], 1)
	if err != nil {
		t.Fatal(err)
	}
	if rf.MovedBySort == 0 {
		t.Fatal("the fixture is in time order; it should not be")
	}
	order, err := replayOrder(rf.Refs)
	if err != nil {
		t.Fatal(err)
	}
	var seen []time.Time
	var lineOf []int
	err = rf.walk(order, func(b batch) error {
		for _, it := range b.Items {
			seen = append(seen, srcTime(srcObjects, it.rec))
		}
		lineOf = append(lineOf, b.Line)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	want := []time.Time{at(10), at(9), at(0), at(5), at(4)}
	if !slices.Equal(seen, want) {
		t.Errorf("records replayed at %v, want the order of the file, %v", seen, want)
	}
	if !slices.Equal(lineOf, []int{0, 1, 2}) {
		t.Errorf("batches of lines %v, want one for each line, in order", lineOf)
	}
}

func TestSortMovesCountsWhatSortRefsWouldChange(t *testing.T) {
	refs := []capture.LogRef{
		{Index: 0, Line: 1, Observed: 30},
		{Index: 1, Line: 2, Observed: 10},
		{Index: 2, Line: 3, Observed: 20},
	}
	moved, shift, err := sortMoves(refs)
	if err != nil {
		t.Fatal(err)
	}
	if moved != 3 || shift != 2 {
		t.Errorf("moved %d, furthest %d; want 3 and 2", moved, shift)
	}
	inOrder := []capture.LogRef{{Index: 0, Line: 1, Observed: 1}, {Index: 1, Line: 2, Observed: 2}}
	if moved, shift, _ := sortMoves(inOrder); moved != 0 || shift != 0 {
		t.Errorf("a capture in order moved %d, shifted %d", moved, shift)
	}
}

// A pod whose creation time lies far in the future is timed at its observed time, with no
// Through: such a record is the one a restarted coalescer must not take for a refresh.
func TestPassBRestartsTheTranslatorsAndTheCoalescer(t *testing.T) {
	min := func(m int) time.Time { return t0.Add(time.Duration(m) * time.Minute) }
	oldCreated := min(-60)
	// p1 has an implausible creation time; p2 has an ordinary one.
	p1 := func(m int) capture.LogRecord {
		return pullRecord(podObject("p1", "p1", "", min(m).Add(10*time.Minute)), min(m))
	}
	p2 := func(m int) capture.LogRecord { return pullRecord(podObject("p2", "p2", "", oldCreated), min(m)) }
	rep := replayFixture(t,
		[][]capture.LogRecord{{p1(0), p2(0)}, {p1(2), p2(2)}},
		[][]capture.LogRecord{{p1(3), p2(3)}},
	)
	if rep.A.Restarts != 0 || rep.B.Restarts != 1 {
		t.Fatalf("restarts: pass A %d, pass B %d; want 0 and 1", rep.A.Restarts, rep.B.Restarts)
	}
	// Run 2: pass A's translators and coalescer know both pods and absorb the refresh; pass
	// B's know nothing: p1 starts a run, and p2 is first seen again. Its record, timed at
	// its creation by the translator, is resumed from the store: raised to the last instant
	// the store holds for it (the refresh of minute 2, written at the restart), through the
	// time it was observed.
	if a, b := rep.A.Runs[1].Written, rep.B.Runs[1].Written; a != 0 || b != 2 {
		t.Errorf("run 2 wrote %d records in pass A and %d in pass B; want 0 and 2", a, b)
	}
	if r := rep.B.Runs[1].Resume; r.Raised != 1 || r.Unknown != 0 || r.Changed != 0 || r.New != 0 {
		t.Errorf("pass B's resume in run 2: %+v; want p2 raised", r)
	}
	if r := rep.A.Runs[1].Resume; r != (resume.Stats{}) {
		t.Errorf("pass A's resume in run 2: %+v; want nothing marked", r)
	}
	// p2's records timed at its creation: the first sighting of its first run only.
	fp, err := fingerprintOf(rep.A.resolver, catalog.K8sPod, catalog.K8sPodUID, "p2")
	if err != nil {
		t.Fatal(err)
	}
	window := func(p *pass) []store.Record {
		recs, err := p.mem.EntityWindow(context.Background(), fp, oldCreated.Add(-time.Minute), min(10), store.Current(catalog.L2))
		if err != nil {
			t.Fatal(err)
		}
		return recs
	}
	firstSightings := func(p *pass) int {
		n := 0
		for _, r := range window(p) {
			if r.EventTime.Equal(oldCreated) {
				n++
			}
		}
		return n
	}
	if a, b := firstSightings(rep.A), firstSightings(rep.B); a != 1 || b != 1 {
		t.Errorf("records of p2 timed at its creation: %d in pass A and %d in pass B; want 1 and 1", a, b)
	}
	resumed := 0
	for _, r := range window(rep.B) {
		if r.EventTime.Equal(min(2)) && r.Through.Equal(min(3)) && r.EventTimeBasis == store.BasisObserved {
			resumed++
		}
	}
	if resumed != 1 {
		t.Errorf("pass B holds %d records of p2 at minute 2 through minute 3; want 1: %+v", resumed, window(rep.B))
	}
	// The report has each pass's resume counts and what its Pebble store counted: one
	// lookup per batch.
	for _, p := range []*pass{rep.A, rep.B} {
		counts, st := resumeOf(p)
		if st.LookupIterators != int64(p.Runs[0].Batches+p.Runs[1].Batches) || st.CheckpointPolicy == "" || (p == rep.B && st.LookupKeysWalkedMax == 0) {
			t.Errorf("pass %s: store report %+v", p.Name, st)
		}
		if p == rep.B && counts["raised"] != 1 {
			t.Errorf("pass B's resume counts %v", counts)
		}
	}
}

// With a pull every few minutes, a run is bounded by its own extension interval, so a
// refresh after a gap closes the run before the new one starts. Left to extend, the run
// would be extended once instead.
func TestCoalescerBoundsRunAgeByTTL(t *testing.T) {
	min := func(m int) time.Time { return t0.Add(time.Duration(m) * time.Minute) }
	pull := func(m int) []capture.LogRecord {
		return []capture.LogRecord{pullRecord(podObject("p1", "p1", "", min(m).Add(10*time.Minute)), min(m))}
	}
	rep := replayFixture(t, [][]capture.LogRecord{pull(0), pull(7), pull(20)})
	if got := rep.A.Runs[0].Written; got != 3 {
		t.Errorf("wrote %d records for pulls at 0, 7 and 20 minutes; want 3 (the first run, the record that closes it, and the second run)", got)
	}
	if cfg := coalesceConfig(); !cfg.RunAgeFromTTL || cfg.ExtendTTLFraction != 0.5 {
		t.Errorf("coalescer config %+v", cfg)
	}
}
