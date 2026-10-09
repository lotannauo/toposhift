package pebblestore

import (
	"slices"
	"testing"
	"time"

	"github.com/lotannauo/toposhift/internal/catalog"
	"github.com/lotannauo/toposhift/internal/lifecycle"
	"github.com/lotannauo/toposhift/internal/store"
)

// tickClock is a clock that moves ten nanoseconds each time it is read, and counts
// the readings, so the time a span takes is ten times the readings inside it.
type tickClock struct {
	ns    int64
	reads int
}

func (c *tickClock) now() time.Time {
	c.reads++
	c.ns += 10
	return time.Unix(0, c.ns).UTC()
}

var phaseNames = []string{
	"write.phase_ns.validate", "write.phase_ns.state", "write.phase_ns.invalidate",
	"write.phase_ns.record_commit", "write.phase_ns.ckpt_build", "write.phase_ns.ckpt_commit",
}

// A Write that stores records samples where its time went, six numbers, once per
// Write, in spans that do not overlap: they add up to at most the Write's
// duration. Work that did not happen costs nothing: a store that writes no
// checkpoints and remembers nothing has no state, invalidation, build or commit of
// a checkpoint to time.
func TestEachWriteSamplesItsPhases(t *testing.T) {
	t.Parallel()
	for name, tc := range map[string]struct {
		ckpt      *CheckpointOptions
		wantBuilt bool // a checkpoint is built and committed by the second Write
	}{
		"checkpoints on":  {policy(stress(0)), true},
		"checkpoints off": {off(), false},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			clock := &tickClock{}
			rec := newMemRecorder()
			s := openMem(t, Options{Recorder: rec, Checkpoints: tc.ckpt, clock: clock.now})
			for i := range 3 {
				before := map[string]int{}
				for _, n := range phaseNames {
					before[n] = len(rec.samples[n])
				}
				readsBefore := clock.reads
				start := clock.now()
				if err := s.Write(bg, []store.Record{edgeRecord(uint64(i+1), "p", t0.Add(time.Duration(i)*time.Second), lifecycle.Observe, 0)}); err != nil {
					t.Fatal(err)
				}
				wall := clock.now().Sub(start).Nanoseconds()
				inside := clock.reads - readsBefore - 2 // the readings the Write made
				var sum int64
				got := map[string]int64{}
				for _, n := range phaseNames {
					if len(rec.samples[n]) != before[n]+1 {
						t.Fatalf("write %d: %s has %d samples, want one more than %d", i, n, len(rec.samples[n]), before[n])
					}
					v := rec.samples[n][before[n]]
					if v < 0 {
						t.Errorf("write %d: %s = %d", i, n, v)
					}
					got[n] = v
					sum += v
				}
				if sum > wall {
					t.Errorf("write %d: the phases add up to %d ns, more than the Write's %d: %v", i, sum, wall, got)
				}
				// Every reading the Write made is the edge of a span or inside one, and a
				// span of the clock above is ten nanoseconds for each reading in it and one
				// more: what is timed is exactly the spans, none twice. A Write of an edge
				// stores two copies; with the policy on that is a state and an invalidation
				// for each, and a build and a commit of checkpoints; always a validation
				// and a record commit.
				spans := 2
				if tc.ckpt.On {
					spans = 8
				}
				if want := int64(10 * (inside - spans)); sum != want {
					t.Errorf("write %d: the phases add up to %d ns, want %d (%d readings in %d spans): a span is timed twice or not at all: %v", i, sum, want, inside, spans, got)
				}
				for _, n := range []string{"write.phase_ns.validate", "write.phase_ns.record_commit"} {
					if got[n] == 0 {
						t.Errorf("write %d: %s was not timed", i, n)
					}
				}
				for _, n := range []string{"write.phase_ns.state", "write.phase_ns.invalidate", "write.phase_ns.ckpt_build"} {
					if (got[n] != 0) != tc.ckpt.On {
						t.Errorf("write %d: %s = %d with the policy on = %v", i, n, got[n], tc.ckpt.On)
					}
				}
				// With KMin 1 every Write makes a checkpoint due in both prefixes, and the
				// first of them builds one and commits it.
				if (got["write.phase_ns.ckpt_commit"] != 0) != tc.wantBuilt {
					t.Errorf("write %d: ckpt_commit = %d, a checkpoint written = %v", i, got["write.phase_ns.ckpt_commit"], tc.wantBuilt)
				}
			}
			if tc.wantBuilt && rec.counters["checkpoint.build_ns"] == 0 {
				t.Error("checkpoints were built and checkpoint.build_ns is 0")
			}
			if !tc.wantBuilt && rec.counters["checkpoint.build_ns"] != 0 {
				t.Errorf("no checkpoint was built and checkpoint.build_ns = %d", rec.counters["checkpoint.build_ns"])
			}
		})
	}
}

// A Write that is refused, or that stores nothing, samples nothing: the numbers
// are the cost of writes.
func TestARefusedWriteSamplesNothing(t *testing.T) {
	t.Parallel()
	rec := newMemRecorder()
	s := openMem(t, Options{Recorder: rec, clock: (&tickClock{}).now})
	if err := s.Write(bg, nil); err != nil {
		t.Fatal(err)
	}
	if err := s.Write(bg, []store.Record{edgeRecord(store.Latest, "p", t0, lifecycle.Observe, 0)}); err == nil {
		t.Fatal("a record with Seq Latest was stored")
	}
	for _, n := range phaseNames {
		if len(rec.samples[n]) != 0 {
			t.Errorf("%s has %v after writes that stored nothing", n, rec.samples[n])
		}
	}
}

// With no recorder nothing is timed, and the clock is never read: not on a Write,
// whatever it does, nor by a checkpoint's build.
func TestAStoreWithNoRecorderNeverReadsTheClock(t *testing.T) {
	t.Parallel()
	clock := &tickClock{}
	s := openMem(t, Options{Checkpoints: policy(stress(0)), clock: clock.now})
	for i := range 4 {
		if err := s.Write(bg, []store.Record{edgeRecord(uint64(i+1), "p", t0.Add(time.Duration(i)*time.Second), lifecycle.Observe, 0)}); err != nil {
			t.Fatal(err)
		}
	}
	if err := s.Instrument().CheckpointEdges(catalog.L2, podFP, store.Forward, t0.Add(time.Hour)); err != nil {
		t.Fatal(err)
	}
	if clock.reads != 0 {
		t.Errorf("a store with no recorder read the clock %d times", clock.reads)
	}
	if len(slices.DeleteFunc(dump(t, s), func(x stored) bool { return x.kind != kindCheckpoint })) == 0 {
		t.Error("the writes built no checkpoint: the test shows nothing about the build")
	}
	// And the same writes with a recorder do read it.
	timed := &tickClock{}
	s = openMem(t, Options{Checkpoints: policy(stress(0)), Recorder: newMemRecorder(), clock: timed.now})
	if err := s.Write(bg, []store.Record{edgeRecord(1, "p", t0, lifecycle.Observe, 0)}); err != nil {
		t.Fatal(err)
	}
	if timed.reads == 0 {
		t.Error("a store with a recorder never read the clock")
	}
}

// write.iterators counts the iterators a Write opens to learn state and to build
// checkpoints, and is left out when it is none; retain.max_prefix_records samples
// the most records a retention replayed for one prefix.
func TestWriteCountsItsIteratorsAndRetentionSamplesItsLongestPrefix(t *testing.T) {
	t.Parallel()
	rec := newMemRecorder()
	s := openMem(t, Options{Recorder: rec, Checkpoints: policy(CheckpointOptions{On: true, KMin: 2})})
	// One record in each of two prefixes (the edge's two copies): the writer knows
	// the database is empty, so there is no state to read, and nothing is due.
	if err := s.Write(bg, []store.Record{edgeRecord(1, "p", t0, lifecycle.Observe, 0)}); err != nil {
		t.Fatal(err)
	}
	if _, ok := rec.counters["write.iterators"]; ok {
		t.Errorf("a Write that opened no iterator counted %d", rec.counters["write.iterators"])
	}
	// The second makes both prefixes due: each builds a checkpoint with an iterator.
	if err := s.Write(bg, []store.Record{edgeRecord(2, "q", t0.Add(time.Second), lifecycle.Observe, 0)}); err != nil {
		t.Fatal(err)
	}
	if got := rec.counters["write.iterators"]; got != 2 || rec.counters["checkpoint.written"] != 2 {
		t.Errorf("write.iterators = %d with %d checkpoints written, want 2 and 2", got, rec.counters["checkpoint.written"])
	}
	// A reopened store reads each prefix it meets once, and a late record looks up
	// the checkpoints it makes untrue: all counted, whichever store opened them.
	cfg := s.kv.Config()
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	rec2 := newMemRecorder()
	s2, err := Open("db", Options{Config: cfg, Recorder: rec2, Checkpoints: policy(CheckpointOptions{On: true, KMin: 1000})})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = s2.Close() }()
	if err := s2.Write(bg, []store.Record{edgeRecord(3, "p", t0.Add(2*time.Second), lifecycle.Observe, 0)}); err != nil {
		t.Fatal(err)
	}
	if got, loads := rec2.counters["write.iterators"], rec2.counters["checkpoint.loads"]; got != loads || loads != 2 {
		t.Errorf("write.iterators = %d with %d state loads, want 2 and 2", got, loads)
	}

	// The longest prefix a retention replays.
	rec3 := newMemRecorder()
	s3 := openMem(t, Options{Recorder: rec3, Checkpoints: off()})
	var batch []store.Record
	for i := range 5 {
		batch = append(batch, edgeRecord(uint64(i+1), lifecycle.Producer("p"+string(rune('a'+i))), t0.Add(time.Duration(i)*time.Second), lifecycle.Observe, 0))
	}
	if err := s3.Write(bg, batch); err != nil {
		t.Fatal(err)
	}
	if err := s3.Retain(bg, t0.Add(time.Hour)); err != nil {
		t.Fatal(err)
	}
	if got := rec3.samples["retain.max_prefix_records"]; !slices.Equal(got, []int64{5}) {
		t.Errorf("retain.max_prefix_records = %v, want [5]", got)
	}
	// A retention that moves nothing samples nothing.
	if err := s3.Retain(bg, t0); err != nil {
		t.Fatal(err)
	}
	if got := rec3.samples["retain.max_prefix_records"]; len(got) != 1 {
		t.Errorf("a retention that moved nothing sampled: %v", got)
	}
}
