package pebblelog

import (
	"bytes"
	"fmt"
	"slices"
	"testing"
	"time"

	"github.com/cockroachdb/pebble/v2"
	"github.com/cockroachdb/pebble/v2/vfs"

	"github.com/lotannauo/toposhift/bench/spike/conformance"
	"github.com/lotannauo/toposhift/bench/spike/engine"
	"github.com/lotannauo/toposhift/bench/spike/pebblekv"
	"github.com/lotannauo/toposhift/bench/spike/workload"
	"github.com/lotannauo/toposhift/internal/catalog"
	"github.com/lotannauo/toposhift/internal/identity"
	"github.com/lotannauo/toposhift/internal/lifecycle"
)

// A scene is the pod's forward prefix with three checkpoints, c[0] < c[1] < c[2],
// written by the policy (a checkpoint when two records are in, at the instant after
// the newest) from three batches of two records. A late record between the first
// and the second then deletes the second and the third.
type replaceScene struct {
	t      *testing.T
	p      *pair
	rec    *countRecorder
	prefix []byte
	key    string
	peer   map[string]identity.Fingerprint
	c      [3]int64
}

var peerNames = []string{"x1", "x2", "x3", "x4", "x5", "x6", "x7", "x8", "x9"}

func newReplaceScene(t *testing.T, replace int, mod func(*Options)) *replaceScene {
	t.Helper()
	rec := &countRecorder{}
	opts := Options{Checkpoints: CheckpointOptions{On: true, KMin: 2, Replace: replace}, Recorder: rec}
	if mod != nil {
		mod(&opts)
	}
	s := &replaceScene{t: t, p: newPair(t, opts), rec: rec, peer: peers(peerNames...)}
	for i, base := range []int{10, 20, 30} {
		s.p.write(
			edgeTo(uint64(2*i+1), s.peer[peerNames[2*i]], "h", sec(base), lifecycle.Observe, 0),
			edgeTo(uint64(2*i+2), s.peer[peerNames[2*i+1]], "h", sec(base+1), lifecycle.Observe, 0),
		)
		s.c[i] = sec(base+1).UnixNano() + 1
	}
	prefix, ok := s.p.e.prefixOf(catalog.L2, podFP, byte(engine.Forward))
	if !ok {
		t.Fatal("the pod's type is not in the catalog")
	}
	s.prefix, s.key = prefix, string(prefix)
	if got := checkpointsOnDisk(t, s.p.e)[s.key]; !slices.Equal(got, s.c[:]) {
		t.Fatalf("the scene has checkpoints %v, want %v", got, s.c)
	}
	return s
}

// late is a record that makes the second and third checkpoints untrue: between the
// first and the second, of a peer the prefix has not met, so that its entry is new.
func (s *replaceScene) late(seq uint64, at time.Time, peer string) engine.Record {
	return edgeTo(seq, s.peer[peer], "h", at, lifecycle.Observe, 0)
}

func (s *replaceScene) count(name string) int64 { return s.rec.counts()[name] }

func (s *replaceScene) disk() []int64 { return checkpointsOnDisk(s.t, s.p.e)[s.key] }

// sameAnswers asks the engine and the oracle everything about the scene's entities.
func (s *replaceScene) sameAnswers() {
	s.t.Helper()
	fps := []identity.Fingerprint{podFP}
	for _, n := range peerNames {
		fps = append(fps, s.peer[n])
	}
	s.p.same(fps, []time.Time{sec(5), sec(12), sec(18), sec(25), sec(40)}, []uint64{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, engine.Latest})
}

// checkpointValues are the stored values of the checkpoints of a prefix by instant.
func checkpointValues(t *testing.T, e *Engine, prefix []byte) map[int64][]byte {
	t.Helper()
	lo, hi := prefixBounds(prefix)
	it, err := e.kv.NewIter(&pebble.IterOptions{LowerBound: lo, UpperBound: hi})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = it.Close() }()
	out := map[int64][]byte{}
	for ok := it.First(); ok; ok = it.Next() {
		_, ns, _, kind, err := parseKey(it.Key())
		if err != nil {
			t.Fatal(err)
		}
		if kind == kindCheckpoint {
			out[ns] = slices.Clone(it.Value())
		}
	}
	if err := it.Error(); err != nil {
		t.Fatal(err)
	}
	return out
}

// requireBuilt fails unless the stored checkpoint at each instant is exactly what
// build makes at it from the data as it is now, and says what the stamp holds.
func requireBuilt(t *testing.T, e *Engine, prefix []byte, instants []int64, w, through uint64) (valueBytes int64) {
	t.Helper()
	stored := checkpointValues(t, e, prefix)
	for _, c := range instants {
		want, err := e.build(prefix, c)
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(stored[c], want) {
			t.Fatalf("the checkpoint at %d is not what a build at %d finds in the data as it is now", c, c)
		}
		st, err := decodeStamp(stored[c])
		if err != nil {
			t.Fatal(err)
		}
		if st.W != w || st.Through != through {
			t.Fatalf("the checkpoint at %d has W %d and Through %d, want %d and %d", c, st.W, st.Through, w, through)
		}
		valueBytes += int64(len(prefix) + suffixLen + len(stored[c]))
	}
	return valueBytes
}

// The checkpoints a late record deletes come back, each at its own instant, in the
// write that deleted them, as a build over the data with the late record in it. The
// one before the late record is untouched, and the policy's spacing does not move: a
// put-back checkpoint is not the last one written.
func TestReplacePutsBackWhatALateRecordDeleted(t *testing.T) {
	t.Parallel()
	s := newReplaceScene(t, 4, func(o *Options) { o.TallyPrefixes = true })
	e := s.p.e
	old := checkpointValues(t, e, s.prefix)
	before := *e.states[s.key]
	counts := s.rec.counts()
	late := s.late(7, sec(15), "x7")
	s.p.write(late)

	if got := s.disk(); !slices.Equal(got, s.c[:]) {
		t.Fatalf("checkpoints after the late record: %v, want %v", got, s.c)
	}
	if got := e.states[s.key].ckpts; !slices.Equal(got, s.c[:]) {
		t.Fatalf("the writer remembers %v, want %v", got, s.c)
	}
	now := checkpointValues(t, e, s.prefix)
	if !bytes.Equal(now[s.c[0]], old[s.c[0]]) {
		t.Error("the checkpoint before the late record was rewritten")
	}
	for _, c := range s.c[1:] {
		if bytes.Equal(now[c], old[c]) {
			t.Errorf("the checkpoint at %d is the one the late record deleted: it does not cover the record", c)
		}
	}
	// The late record has Seq 7, the last committed: the put-back ones depend on it.
	put := requireBuilt(t, e, s.prefix, s.c[1:], 7, 7)

	delta := func(name string) int64 { return s.count(name) - counts[name] }
	for name, want := range map[string]int64{
		"checkpoint.invalidated": 2, "checkpoint.replaced": 2, "checkpoint.written": 0,
		"checkpoint.bytes_written": put, "checkpoint.errors": 0, "checkpoint.replace_capped": 0,
	} {
		if got := delta(name); got != want {
			t.Errorf("%s moved by %d, want %d", name, got, want)
		}
	}

	// The spacing is what it was plus the write's own record.
	sides, err := e.sidesOf(late)
	if err != nil {
		t.Fatal(err)
	}
	size := len(appendRecordValue(nil, sides[0].ref, pebblekv.FromRecord(late)))
	st := e.states[s.key]
	if st.since != before.since+1 || st.sinceBytes != before.sinceBytes+size || st.lastBytes != before.lastBytes {
		t.Errorf("spacing after the write: since %d, sinceBytes %d, lastBytes %d; before it %d, %d, %d, and the write adds 1 record of %d bytes",
			st.since, st.sinceBytes, st.lastBytes, before.since, before.sinceBytes, before.lastBytes, size)
	}
	if got := e.PrefixTallies()[s.key]; got != [3]int64{2, 2, 0} {
		t.Errorf("tallies of the prefix: %v, want [2 2 0]", got)
	}
	s.sameAnswers()
}

// With Replace 1 the newest one goes back, and the other is counted as not put back
// and is not there; the policy places the next as it always does.
func TestReplaceOneKeepsTheNewest(t *testing.T) {
	t.Parallel()
	s := newReplaceScene(t, 1, func(o *Options) { o.TallyPrefixes = true })
	s.p.write(s.late(7, sec(15), "x7"))
	if got, want := s.disk(), []int64{s.c[0], s.c[2]}; !slices.Equal(got, want) {
		t.Fatalf("checkpoints after the late record: %v, want %v", got, want)
	}
	requireBuilt(t, s.p.e, s.prefix, s.c[2:], 7, 7)
	if s.count("checkpoint.replaced") != 1 || s.count("checkpoint.replace_capped") != 1 || s.count("checkpoint.invalidated") != 2 {
		t.Errorf("counters: replaced %d, replace_capped %d, invalidated %d; want 1, 1, 2",
			s.count("checkpoint.replaced"), s.count("checkpoint.replace_capped"), s.count("checkpoint.invalidated"))
	}
	if got := s.p.e.PrefixTallies()[s.key]; got != [3]int64{2, 1, 1} {
		t.Errorf("tallies of the prefix: %v, want [2 1 1]", got)
	}
	s.sameAnswers()

	// Two more records make the next checkpoint due, at the usual instant, and the
	// spacing counts from them and the late record.
	s.p.write(s.late(8, sec(40), "x8"), s.late(9, sec(41), "x9"))
	if got, want := s.disk(), []int64{s.c[0], s.c[2], sec(41).UnixNano() + 1}; !slices.Equal(got, want) {
		t.Fatalf("checkpoints after two more records: %v, want %v", got, want)
	}
	if st := s.p.e.states[s.key]; st.since != 0 || st.sinceBytes != 0 {
		t.Errorf("a due checkpoint left since %d and sinceBytes %d", st.since, st.sinceBytes)
	}
	s.sameAnswers()
}

// Forty prefixes each lose a checkpoint to one write, and the write puts back the
// first thirty-two of them in key order and counts the other eight.
func TestReplaceNeverExceedsTheBatchCap(t *testing.T) {
	t.Parallel()
	rec := &countRecorder{}
	p := newPair(t, Options{Checkpoints: CheckpointOptions{On: true, KMin: 1000, Replace: 4}, Recorder: rec, TallyPrefixes: true})
	const n = 40
	fps := make([]identity.Fingerprint, n)
	var first, late []engine.Record
	for i := range n {
		fps[i] = fingerprintOf(catalog.K8sNode, byte(3*i+1))
		first = append(first, entityRecord(uint64(i+1), fps[i], "p", sec(10)))
		late = append(late, entityRecord(uint64(n+i+1), fps[i], "q", sec(15)))
	}
	p.write(first...)
	for _, fp := range fps {
		if err := p.e.CheckpointEntity(catalog.L2, fp, sec(20)); err != nil {
			t.Fatal(err)
		}
	}
	p.write(late...)

	counts := rec.counts()
	if counts["checkpoint.invalidated"] != n || counts["checkpoint.replaced"] != replaceBatchMax || counts["checkpoint.replace_capped"] != n-replaceBatchMax {
		t.Errorf("invalidated %d, replaced %d, capped %d; want %d, %d, %d",
			counts["checkpoint.invalidated"], counts["checkpoint.replaced"], counts["checkpoint.replace_capped"], n, replaceBatchMax, n-replaceBatchMax)
	}
	var keys []string
	for _, fp := range fps {
		keys = append(keys, string(entityPrefix(t, p.e, fp)))
	}
	slices.Sort(keys)
	disk, tallies := checkpointsOnDisk(t, p.e), p.e.PrefixTallies()
	for i, k := range keys {
		wantDisk, wantTally := []int64(nil), [3]int64{1, 0, 1}
		if i < replaceBatchMax {
			wantDisk, wantTally = []int64{sec(20).UnixNano()}, [3]int64{1, 1, 0}
		}
		if !slices.Equal(disk[k], wantDisk) || tallies[k] != wantTally {
			t.Errorf("prefix %d in key order: checkpoints %v and tallies %v, want %v and %v", i, disk[k], tallies[k], wantDisk, wantTally)
		}
	}
	p.same(fps, []time.Time{sec(12), sec(18), sec(25)}, []uint64{0, n, 2 * n, engine.Latest})
}

// What is chosen from the instants a write deleted: not those a checkpoint cannot be
// written at (at or below the horizon), not those the prefix already has, each once;
// newest first, at most Replace to a prefix and replaceBatchMax in all, over prefixes
// in key order; and what is left over is counted.
func TestReplacementsAreChosenNewestFirstWithinTheCaps(t *testing.T) {
	t.Parallel()
	rec := &countRecorder{}
	e := openMem(t, Options{Checkpoints: CheckpointOptions{On: true, KMin: 1000, Replace: 2}, Recorder: rec, TallyPrefixes: true})
	h := sec(100)
	if err := e.Retain(h); err != nil {
		t.Fatal(err)
	}
	hNs := h.UnixNano()
	synthetic := func(key string, ckpts ...int64) *prefixState {
		st := &prefixState{latest: -1, ckpts: ckpts}
		e.states[key] = st
		return st
	}

	t.Run("filters and order", func(t *testing.T) {
		synthetic("p", sec(150).UnixNano())
		deleted := map[string][]int64{"p": {
			sec(170).UnixNano(), hNs - 1, sec(160).UnixNano(), hNs, sec(150).UnixNano(), sec(180).UnixNano(), sec(170).UnixNano(),
		}}
		got := e.chooseReplacements([]string{"p"}, deleted)
		want := []replacement{{"p", sec(180).UnixNano()}, {"p", sec(170).UnixNano()}}
		if !slices.Equal(got, want) {
			t.Errorf("chosen %v, want %v: the instants at or below the horizon %d and the one the prefix has are dropped, the rest newest first", got, want, hNs)
		}
		if n := rec.counts()["checkpoint.replace_capped"]; n != 1 {
			t.Errorf("%d counted as not put back, want 1 (the one at %d)", n, sec(160).UnixNano())
		}
		if got := e.PrefixTallies()["p"]; got != [3]int64{0, 0, 1} {
			t.Errorf("tallies of the prefix: %v, want [0 0 1]", got)
		}
	})

	t.Run("the cap of a whole write", func(t *testing.T) {
		e.ckpt.Replace = 3
		deleted := map[string][]int64{}
		var keys []string
		for i := range 20 {
			k := fmt.Sprintf("q%02d", i)
			synthetic(k)
			keys = append(keys, k)
			deleted[k] = []int64{sec(110).UnixNano(), sec(120).UnixNano(), sec(130).UnixNano()}
		}
		before := rec.counts()["checkpoint.replace_capped"]
		got := e.chooseReplacements(keys, deleted)
		if len(got) != replaceBatchMax {
			t.Fatalf("%d chosen, want %d", len(got), replaceBatchMax)
		}
		per := map[string]int{}
		for _, r := range got {
			per[r.prefix]++
		}
		// Ten prefixes' three each are thirty; the eleventh has two chosen, and the
		// rest none.
		for i, k := range keys {
			want := map[bool]int{true: 3, false: 0}[i < 10]
			if i == 10 {
				want = 2
			}
			if per[k] != want {
				t.Errorf("prefix %s (%d in key order) has %d chosen, want %d", k, i, per[k], want)
			}
		}
		if i := slices.IndexFunc(got, func(r replacement) bool { return r.prefix == "q00" }); got[i].c != sec(130).UnixNano() {
			t.Errorf("the first chosen of a prefix is at %d, want the newest, %d", got[i].c, sec(130).UnixNano())
		}
		if n := rec.counts()["checkpoint.replace_capped"] - before; n != 60-replaceBatchMax {
			t.Errorf("%d counted as not put back, want %d", n, 60-replaceBatchMax)
		}
	})
}

// A read pinned to a token below the late record cannot use a put-back checkpoint
// and walks to the older one, as it would after the deletion, and still answers as
// the reference does.
func TestReplaceIsSkippedByATokenBelowTheLateRecord(t *testing.T) {
	t.Parallel()
	s := newReplaceScene(t, 4, nil)
	s.p.write(s.late(7, sec(15), "x7"))
	read := func(token uint64) (hits, skipped int64) {
		t.Helper()
		h0, s0 := s.count("read.checkpoint_hits"), s.count("read.checkpoint_skipped_w")
		sc := engine.Scope{Layer: catalog.L2, AsOf: token}
		got, err := s.p.e.Neighbors(podFP, engine.Forward, sec(60), sc)
		want, _ := s.p.ora.Neighbors(podFP, engine.Forward, sec(60), sc)
		if err != nil || !slices.Equal(got, want) {
			t.Fatalf("Neighbors at token %d = %v, %v; oracle says %v", token, got, err, want)
		}
		return s.count("read.checkpoint_hits") - h0, s.count("read.checkpoint_skipped_w") - s0
	}
	if hits, skipped := read(6); hits != 1 || skipped != 2 {
		t.Errorf("a token before the late record: %d checkpoints used and %d skipped, want 1 and 2 (the first, after skipping both put back)", hits, skipped)
	}
	if hits, skipped := read(engine.Latest); hits != 1 || skipped != 0 {
		t.Errorf("the newest token: %d checkpoints used and %d skipped, want 1 and 0", hits, skipped)
	}
	s.sameAnswers()
}

// A checkpoint commit that fails, whether or not it landed, leaves the writer
// remembering nothing and the database right: later reads answer as the reference
// does, and a later late record deletes and puts back what is really there.
func TestAFailedCheckpointCommitAfterAReplacementIsForgotten(t *testing.T) {
	t.Parallel()
	for name, landed := range map[string]bool{"lands": true, "does not land": false} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			f := &failures{}
			s := newReplaceScene(t, 4, f.hooks)
			e := s.p.e
			if landed {
				f.ckptAfter = true
			} else {
				f.ckptBefore = true
			}
			s.p.write(s.late(7, sec(15), "x7")) // a failed second commit is counted, not returned
			if s.count("checkpoint.errors") != 1 || s.count("checkpoint.replaced") != 0 {
				t.Errorf("errors %d, replaced %d; want 1 and 0", s.count("checkpoint.errors"), s.count("checkpoint.replaced"))
			}
			if len(e.states) != 0 || e.complete {
				t.Errorf("the writer remembers %d prefixes (complete %v) after a failed commit", len(e.states), e.complete)
			}
			want := []int64{s.c[0]}
			if landed {
				want = s.c[:]
			}
			if got := s.disk(); !slices.Equal(got, want) {
				t.Fatalf("checkpoints after the failure: %v, want %v", got, want)
			}
			if landed {
				requireBuilt(t, e, s.prefix, s.c[1:], 7, 7)
			}
			s.sameAnswers()

			// Another late record, in the prefix as the database really is.
			s.p.write(s.late(8, sec(16), "x8"))
			// Where nothing came back, the writer has found six records above the first
			// checkpoint and the policy places the next at the instant after the newest.
			if !landed {
				want = []int64{s.c[0], s.c[2]}
			}
			if got := s.disk(); !slices.Equal(got, want) {
				t.Fatalf("checkpoints after the second late record: %v, want %v", got, want)
			}
			if landed {
				requireBuilt(t, e, s.prefix, s.c[1:], 8, 8)
				if s.count("checkpoint.replaced") != 2 {
					t.Errorf("replaced %d, want the two the second record deleted", s.count("checkpoint.replaced"))
				}
			}
			s.sameAnswers()
		})
	}
}

// A due checkpoint at an instant a put-back one is at is the same checkpoint: it is
// not written twice, and the policy takes the prefix as summarized to there.
func TestReplaceAtADueInstantIsWrittenOnce(t *testing.T) {
	t.Parallel()
	s := newReplaceScene(t, 4, nil)
	before := s.rec.counts()
	// Two late records make two records since the last checkpoint, which is due at the
	// instant after the newest record, the one the third checkpoint was at.
	s.p.write(s.late(7, sec(15), "x7"), s.late(8, sec(16), "x8"))
	if got := s.disk(); !slices.Equal(got, s.c[:]) {
		t.Fatalf("checkpoints after the write: %v, want %v", got, s.c)
	}
	put := requireBuilt(t, s.p.e, s.prefix, s.c[1:], 8, 8)
	delta := func(name string) int64 { return s.count(name) - before[name] }
	if delta("checkpoint.replaced") != 2 || delta("checkpoint.written") != 0 || delta("checkpoint.bytes_written") != put {
		t.Errorf("replaced %d, written %d, bytes %d; want 2, 0 and %d: each checkpoint is written once", delta("checkpoint.replaced"), delta("checkpoint.written"), delta("checkpoint.bytes_written"), put)
	}
	if st := s.p.e.states[s.key]; st.since != 0 || st.sinceBytes != 0 {
		t.Errorf("since %d and sinceBytes %d: the prefix is summarized to the newest record", st.since, st.sinceBytes)
	}
	s.sameAnswers()
}

// With Replace zero none of this runs: nothing about the write's deletions is
// collected, no counter moves, and no put-back checkpoint is stored. With Replace
// above zero the write collects the instants it deleted, by prefix. Tallies, and
// whether they are kept, change no stored byte.
func TestWithoutReplaceNothingIsCollectedOrCounted(t *testing.T) {
	t.Parallel()
	for _, replace := range []int{0, 4} {
		t.Run(fmt.Sprintf("replace %d", replace), func(t *testing.T) {
			t.Parallel()
			var seen []map[string][]int64
			s := newReplaceScene(t, replace, func(o *Options) {
				o.TallyPrefixes = true
				o.seeDeleted = func(d map[string][]int64) { seen = append(seen, d) }
			})
			seen = nil // the scene's own writes
			s.p.write(s.late(7, sec(15), "x7"))
			if len(seen) != 1 {
				t.Fatalf("the hook was called %d times by one write", len(seen))
			}
			counts := s.rec.counts()
			if replace == 0 {
				if seen[0] != nil {
					t.Errorf("a write with Replace 0 collected %v", seen[0])
				}
				for _, name := range []string{"checkpoint.replaced", "checkpoint.replace_capped"} {
					if _, moved := counts[name]; moved {
						t.Errorf("%s was counted with Replace 0", name)
					}
				}
				if got := s.disk(); !slices.Equal(got, s.c[:1]) {
					t.Errorf("checkpoints after the late record: %v, want only %v", got, s.c[:1])
				}
				if got := s.p.e.PrefixTallies()[s.key]; got != [3]int64{2, 0, 0} {
					t.Errorf("tallies of the prefix: %v, want [2 0 0]: deleted, and none put back", got)
				}
			} else if want := map[string][]int64{s.key: {s.c[1], s.c[2]}}; fmt.Sprint(seen[0]) != fmt.Sprint(want) {
				t.Errorf("a write with Replace 4 collected %v, want %v", seen[0], want)
			}
			if counts["checkpoint.invalidated"] != 2 {
				t.Errorf("invalidated %d, want 2", counts["checkpoint.invalidated"])
			}
			s.sameAnswers()

			// The same history with the tallies off stores the same bytes.
			plain := newReplaceScene(t, replace, nil)
			plain.p.write(plain.late(7, sec(15), "x7"))
			if plain.p.e.PrefixTallies() != nil {
				t.Error("an engine that keeps no tallies has some")
			}
			a, b := rawDump(t, s.p.e), rawDump(t, plain.p.e)
			if len(a) != len(b) {
				t.Fatalf("tallies changed the number of keys: %d and %d", len(a), len(b))
			}
			for i := range a {
				if !bytes.Equal(a[i][0], b[i][0]) || !bytes.Equal(a[i][1], b[i][1]) {
					t.Fatalf("tallies changed the stored key %x", a[i][0])
				}
			}
		})
	}
}

// The option is refused where it cannot work: negative, or above zero with the
// checkpoints off.
func TestReplaceNeedsCheckpointsAndIsNotNegative(t *testing.T) {
	t.Parallel()
	for name, c := range map[string]CheckpointOptions{
		"negative": {On: true, KMin: 1, Replace: -1},
		"off":      {Replace: 1},
	} {
		if e, err := Open("db", Options{Config: pebblekv.Config{Tuning: pebblekv.TinyTuning(), FS: vfs.NewMem()}, Checkpoints: c}); err == nil {
			_ = e.Close()
			t.Errorf("%s: %+v was accepted", name, c)
		}
	}
	e, err := Open("db", Options{Config: pebblekv.Config{Tuning: pebblekv.TinyTuning(), FS: vfs.NewMem()}, Checkpoints: CheckpointOptions{On: true, KMin: 1, Replace: 1}})
	if err != nil {
		t.Fatal(err)
	}
	_ = e.Close()
}

type replaceQuietLogger struct{}

func (replaceQuietLogger) Infof(string, ...any)  {}
func (replaceQuietLogger) Errorf(string, ...any) {}
func (replaceQuietLogger) Fatalf(format string, args ...any) {
	panic(fmt.Sprintf("pebble: "+format, args...))
}

// replaceStream writes a stream, in batches of fifty, into a new in-memory engine
// and returns the digest of the data keyspace it left and the counts it made.
func replaceStream(t *testing.T, cfg workload.Config, ck CheckpointOptions, fullStateRead bool) (pebblekv.Digest, map[string]int64) {
	t.Helper()
	fs, rec := vfs.NewMem(), &countRecorder{}
	e, err := Open("db", Options{Config: pebblekv.Config{Tuning: pebblekv.TinyTuning(), FS: fs}, Checkpoints: ck, Recorder: rec, fullStateRead: fullStateRead})
	if err != nil {
		t.Fatal(err)
	}
	g, err := workload.New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	for batch := g.Batch(50); len(batch) > 0; batch = g.Batch(50) {
		if err := e.Write(batch); err != nil {
			t.Fatal(err)
		}
	}
	if err := e.Close(); err != nil {
		t.Fatal(err)
	}
	db, err := pebble.Open("db", &pebble.Options{FS: fs, ReadOnly: true, Logger: replaceQuietLogger{}})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()
	d, err := pebblekv.DigestRange(db, pebblekv.DataLo, pebblekv.DataHi)
	if err != nil {
		t.Fatal(err)
	}
	return d, rec.counts()
}

// The same stream written twice with Replace 4 leaves the same data keys and values,
// whether the writer learns a prefix by reading all of it or its tail; and the stream
// does put checkpoints back, so the bytes are not those of the same policy with
// Replace 0.
func TestReplaceIsDeterministicWhateverTheWriterReads(t *testing.T) {
	t.Parallel()
	conformance.SkipWhenTrimmed(t)
	cfg := conformance.Configs()[5] // late records, coalesced runs and outages
	ck := CheckpointOptions{On: true, KMin: 8, Alpha: 1, Replace: 4}
	a, counts := replaceStream(t, cfg, ck, false)
	b, _ := replaceStream(t, cfg, ck, false)
	whole, _ := replaceStream(t, cfg, ck, true)
	if a != b {
		t.Errorf("the same stream written twice left %+v and %+v", a, b)
	}
	if a != whole {
		t.Errorf("a writer that reads whole prefixes left %+v, one that reads tails %+v", whole, a)
	}
	if counts["checkpoint.replaced"] == 0 {
		t.Fatal("the stream put no checkpoint back: the check proves nothing")
	}
	off := ck
	off.Replace = 0
	if plain, _ := replaceStream(t, cfg, off, false); plain == a {
		t.Error("Replace 4 left the bytes of Replace 0")
	}
	t.Logf("%d keys, %d checkpoints; %d invalidated, %d put back, %d not put back",
		a.Keys, a.Checkpoints, counts["checkpoint.invalidated"], counts["checkpoint.replaced"], counts["checkpoint.replace_capped"])
}
