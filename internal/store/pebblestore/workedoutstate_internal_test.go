package pebblestore

import (
	"bytes"
	"errors"
	"fmt"
	"math"
	"slices"
	"testing"
	"time"

	"github.com/cockroachdb/pebble/v2/vfs"

	"github.com/lotannauo/toposhift/internal/catalog"
	"github.com/lotannauo/toposhift/internal/identity"
	"github.com/lotannauo/toposhift/internal/lifecycle"
	"github.com/lotannauo/toposhift/internal/store"
	"github.com/lotannauo/toposhift/internal/store/pebblekv"
)

// A retention works out the state the writer would find by reading each prefix
// afterwards, so that it does not read; and a map that holds a state for every
// prefix that has anything in it is complete, which lets the writer answer a
// prefix it lacks (a new one) without a read. These tests are about the two
// claims: what is worked out is what a whole read finds, and a complete map is
// never trusted for a prefix that holds keys.

// wholeReader is a store over the same database that reads the whole of a
// prefix to learn its state, as the writer once did, and counts and remembers
// nothing of its own.
func wholeReader(e *Store) *Store {
	return &Store{kv: e.kv, rec: nopRecorder{}, fullStateRead: true, anyCkpt: e.anyCkpt, states: map[string]*prefixState{}}
}

// checkpointsFrom are the checkpoints at or after the bound a state was read down
// to: all that state knows of.
func checkpointsFrom(ckpts []int64, below int64) []int64 {
	var out []int64
	for _, c := range ckpts {
		if c >= below {
			out = append(out, c)
		}
	}
	return out
}

// rememberedStateMismatch is requireWholeReads without a test: it returns the
// first way the store's map is not what a read of the whole database finds.
func rememberedStateMismatch(e *Store) error {
	probe := wholeReader(e)
	for k, b := range e.states {
		a, err := probe.state([]byte(k))
		if err != nil {
			return err
		}
		if a.latest != b.latest || a.since != b.since || a.sinceBytes != b.sinceBytes || a.lastBytes != b.lastBytes || !slices.Equal(checkpointsFrom(a.ckpts, b.below), b.ckpts) {
			return fmt.Errorf("the store remembers %+v of the prefix %x, where a whole read finds %+v", *b, k, *a)
		}
	}
	if e.complete {
		held, err := prefixesHoldingKeys(e)
		if err != nil {
			return err
		}
		for k := range held {
			if _, ok := e.states[k]; !ok {
				return fmt.Errorf("the map is complete and lacks the prefix %x, which holds keys", k)
			}
		}
	}
	return nil
}

// requireWholeReads fails unless every prefix the store remembers is what a read
// of the whole prefix finds now, and, if the map is complete, unless every prefix
// that holds a record or a checkpoint is in it. It is for a moment when nothing
// has changed a remembered prefix since it was learnt, such as straight after a
// retention.
func requireWholeReads(t *testing.T, e *Store) {
	t.Helper()
	if err := rememberedStateMismatch(e); err != nil {
		t.Fatal(err)
	}
}

func entityRecord(seq uint64, fp identity.Fingerprint, producer lifecycle.Producer, at time.Time) store.Record {
	return store.Record{Layer: catalog.L2, Subject: store.EntitySubject(fp), Producer: producer, EventTime: at, Seq: seq, Kind: lifecycle.Observe, Payload: []byte("payload")}
}

func entityPrefix(t *testing.T, e *Store, fp identity.Fingerprint) []byte {
	t.Helper()
	p, ok := e.prefixOf(catalog.L2, fp, dirEntity)
	if !ok {
		t.Fatal("the entity's type is not in the catalog")
	}
	return p
}

func rawKeyInPrefix(t *testing.T, e *Store, key []byte) {
	t.Helper()
	if err := e.kv.Set(key, []byte("x"), e.kv.WriteOptions()); err != nil {
		t.Fatal(err)
	}
}

// A database with no data opens with a complete map, so that no first write to a
// prefix reads it; one that holds data does not, and reads, or a late record would
// meet a map that claims a prefix with checkpoints is empty and leave them true.
func TestOnlyAnEmptyDatabaseOpensComplete(t *testing.T) {
	t.Parallel()
	for name, opts := range map[string]CheckpointOptions{"on": stress(0), "off": {}} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			cfg := pebblekv.Config{Tuning: pebblekv.TinyTuning(), FS: vfs.NewMem()}
			open := func(rec *countRecorder, ck CheckpointOptions) *Store {
				e, err := Open("db", Options{Config: cfg, Recorder: rec, Checkpoints: policy(ck)})
				if err != nil {
					t.Fatal(err)
				}
				return e
			}
			rec := &countRecorder{}
			e := open(rec, stress(0))
			if !e.complete {
				t.Fatal("a new database does not open complete")
			}
			if err := e.Close(); err != nil {
				t.Fatal(err)
			}
			if e = open(rec, stress(0)); !e.complete {
				t.Fatal("a database that was opened and closed with nothing written does not open complete")
			}
			ora := newRef(t)
			for _, r := range []store.Record{
				edgeRecord(1, "p", sec(10), lifecycle.Observe, 0), edgeRecord(2, "q", sec(20), lifecycle.Observe, 0),
			} {
				for _, x := range []store.Store{e, ora} {
					if err := x.Write(bg, cloneAll([]store.Record{r})); err != nil {
						t.Fatal(err)
					}
				}
			}
			if !e.complete || rec.counts()["checkpoint.loads"] != 0 {
				t.Fatalf("writing to new prefixes read %d of them or ended completeness (%v)", rec.counts()["checkpoint.loads"], e.complete)
			}
			c1, c2 := sec(10).UnixNano()+1, sec(20).UnixNano()+1
			if got := checkpoints(dump(t, e), byte(store.Forward)); len(got) != 2 || got[0][0] != c1 || got[1][0] != c2 {
				t.Fatalf("checkpoints before the reopening = %v, want at %d and %d", got, c1, c2)
			}
			if err := e.Close(); err != nil {
				t.Fatal(err)
			}
			rec = &countRecorder{}
			e = open(rec, opts)
			defer func() { _ = e.Close() }()
			if e.complete {
				t.Fatal("a database that holds data opens complete")
			}
			// p deletes at 15s, before the second checkpoint's instant: it no
			// longer holds. A map that took the prefix for empty would leave it.
			late := edgeRecord(3, "p", sec(15), lifecycle.Delete, 0)
			for _, x := range []store.Store{e, ora} {
				if err := x.Write(bg, cloneAll([]store.Record{late})); err != nil {
					t.Fatal(err)
				}
			}
			if rec.counts()["checkpoint.loads"] == 0 {
				t.Fatal("the writer did not read a prefix that was already in the database")
			}
			for _, c := range checkpoints(dump(t, e), byte(store.Forward)) {
				if c[0] == c2 && c[1] != 3 {
					t.Errorf("the checkpoint at %d survives with W %d, want one that depends on the late record (W 3)", c2, c[1])
				}
			}
			if !opts.On {
				for _, c := range checkpoints(dump(t, e), byte(store.Forward)) {
					if c[0] == c2 {
						t.Fatal("with checkpoints off, the checkpoint the late record made untrue was not deleted")
					}
				}
			}
			p := &pair{t: t, s: e, ref: ora}
			p.same([]identity.Fingerprint{podFP, nodeFP}, []time.Time{sec(10), sec(17), sec(25), sec(60)}, []uint64{0, 1, 2, 3, store.Latest})
		})
	}
}

// Writing without remembering anything, as a store with checkpoints off and none
// in its database does, ends the claim that the map holds every prefix.
func TestWritingWithoutRememberingEndsCompleteness(t *testing.T) {
	t.Parallel()
	e := openMem(t, Options{Checkpoints: off()})
	if !e.complete {
		t.Fatal("a new database does not open complete")
	}
	if err := e.Write(bg, []store.Record{edgeRecord(1, "p", sec(1), lifecycle.Observe, 0)}); err != nil {
		t.Fatal(err)
	}
	if e.complete || len(e.states) != 0 {
		t.Fatalf("after a write nothing remembers, complete = %v with %d prefixes", e.complete, len(e.states))
	}
}

// The state the retention works out is the state a whole read finds, for the
// prefixes it rewrites and the ones it does not, whether or not a checkpoint sits
// exactly at the horizon (the retention deletes it where it rewrites a prefix and
// leaves it where it does not), and a prefix left with nothing a read would count
// is not remembered but is known.
func TestARetentionWorksOutWhatAWholeReadFinds(t *testing.T) {
	t.Parallel()
	h := sec(10)
	hNs := h.UnixNano()
	node := func(i int) identity.Fingerprint { return fingerprintOf(catalog.K8sNode, byte(0x50+0x10*i)) }
	e1, e2, e3, e4, e5 := node(0), node(1), node(2), node(3), node(4)
	retainRec := &countRecorder{}
	p := newPair(t, Options{Recorder: retainRec, Checkpoints: policy(CheckpointOptions{On: true, KMin: 1000})})
	var seq uint64
	for _, w := range []struct {
		fp identity.Fingerprint
		at []int
	}{
		{e1, []int{1, 2, 3, 12, 14}}, // history on both sides of the horizon
		{e2, []int{11, 12}},          // nothing before it
		{e3, []int{2, 3}},            // nothing after it
		{e4, []int{5, 10, 15}},       // a record at the horizon itself
		{e5, []int{12, 13, 14}},      // no checkpoint at the horizon, nothing before it
	} {
		for _, s := range w.at {
			seq++
			p.write(entityRecord(seq, w.fp, "p", sec(s)))
		}
	}
	for _, c := range []struct {
		fp identity.Fingerprint
		at int
	}{
		{e1, 10},
		{e1, 13}, // one at the horizon, which is deleted, and one after it
		{e2, 10}, // at the horizon with nothing before it: stays
		{e3, 5},
		{e4, 10},
		{e4, 7}, // a record and a checkpoint at the horizon
		{e5, 13},
	} {
		if err := p.s.Instrument().CheckpointEntity(catalog.L2, c.fp, sec(c.at)); err != nil {
			t.Fatal(err)
		}
	}
	p.retain(h)
	if !p.s.complete {
		t.Fatal("the map is not complete after a retention that finished")
	}
	requireWholeReads(t, p.s)
	if n := retainRec.counts()["retain.state_keys"]; n < 5 {
		t.Fatalf("the retention counted %d keys read to work the state out", n)
	}
	state := func(fp identity.Fingerprint) *prefixState { return p.s.states[string(entityPrefix(t, p.s, fp))] }
	if st := state(e1); st == nil || !slices.Equal(st.ckpts, []int64{sec(13).UnixNano()}) || st.latest != sec(14).UnixNano() || st.since != 1 {
		t.Fatalf("e1: the checkpoint at the horizon is gone and the one after it is the newest: %+v", st)
	}
	if st := state(e2); st == nil || !slices.Equal(st.ckpts, []int64{hNs}) || st.since != 2 || st.below != hNs {
		t.Fatalf("e2: the checkpoint at the horizon stays, in a prefix the retention does not rewrite: %+v", st)
	}
	if st := state(e3); st != nil {
		t.Fatalf("e3: nothing a read counts is left, and the map holds %+v", st)
	}
	if st := state(e4); st == nil || len(st.ckpts) != 0 || st.since != 2 || st.below != 0 || st.latest != sec(15).UnixNano() {
		t.Fatalf("e4: the checkpoint at the horizon is gone and the record at it counts: %+v", st)
	}
	disk := checkpointsOnDisk(t, p.s)
	if got := disk[string(entityPrefix(t, p.s, e2))]; !slices.Equal(got, []int64{hNs}) {
		t.Fatalf("e2's checkpoint on disk = %v", got)
	}
	if got := disk[string(entityPrefix(t, p.s, e1))]; !slices.Equal(got, []int64{sec(13).UnixNano()}) {
		t.Fatalf("e1's checkpoints on disk = %v", got)
	}
	// Writing to each of them, new or old, needs no read, and gets the answers the
	// reference gives.
	rec := &countRecorder{}
	p.s.rec = rec
	for _, fp := range []identity.Fingerprint{e1, e2, e3, e4, e5, node(5)} {
		seq++
		p.write(entityRecord(seq, fp, "q", sec(20)))
	}
	if n := rec.counts()["checkpoint.loads"]; n != 0 {
		t.Fatalf("writing after the retention read %d prefixes", n)
	}
	p.same([]identity.Fingerprint{e1, e2, e3, e4, e5, node(5)}, []time.Time{sec(10), sec(13), sec(25)}, []uint64{seq - 6, store.Latest})
}

// A retention that leaves the horizon outside the range of instants, or that does
// not look at the checkpoints, learns nothing, and says so.
func TestARetentionThatCannotWorkTheStateOutLeavesTheMapEmptyAndIncomplete(t *testing.T) {
	// (The subtest "at the epoch" is the exception: it changes nothing.)
	t.Parallel()
	t.Run("past the end of time", func(t *testing.T) {
		t.Parallel()
		p := newPair(t, Options{Checkpoints: policy(stress(0))})
		p.write(edgeRecord(1, "p", sec(1), lifecycle.Observe, 0), edgeRecord(2, "q", store.MaxEventTime, lifecycle.Observe, 0))
		if len(p.s.states) == 0 || !p.s.complete {
			t.Fatal("the writes left nothing remembered")
		}
		// The record at the last instant has no checkpoint after it, so the hook
		// writes one: a retention only works the state out where one may exist.
		if err := p.s.Instrument().CheckpointEdges(catalog.L2, podFP, store.Forward, sec(5)); err != nil {
			t.Fatal(err)
		}
		if !p.s.anyCkpt {
			t.Fatal("the hook wrote no checkpoint")
		}
		p.retain(store.MaxEventTime.Add(time.Hour))
		if len(p.s.states) != 0 || p.s.complete {
			t.Fatalf("after a retention past the end of time, %d prefixes are remembered (complete %v)", len(p.s.states), p.s.complete)
		}
	})
	t.Run("the policy is off", func(t *testing.T) {
		t.Parallel()
		cfg := pebblekv.Config{Tuning: pebblekv.TinyTuning(), FS: vfs.NewMem()}
		e, err := Open("db", Options{Config: cfg, Checkpoints: policy(stress(0))})
		if err != nil {
			t.Fatal(err)
		}
		ora := newRef(t)
		for _, r := range []store.Record{edgeRecord(1, "p", sec(1), lifecycle.Observe, 0), edgeRecord(2, "p", sec(20), lifecycle.Observe, 0)} {
			for _, x := range []store.Store{e, ora} {
				if err := x.Write(bg, cloneAll([]store.Record{r})); err != nil {
					t.Fatal(err)
				}
			}
		}
		if err := e.Close(); err != nil {
			t.Fatal(err)
		}
		rec := &countRecorder{}
		e, err = Open("db", Options{Config: cfg, Recorder: rec, Checkpoints: off()})
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = e.Close() }()
		if err := e.Retain(bg, sec(10)); err != nil {
			t.Fatal(err)
		}
		_ = ora.Retain(bg, sec(10))
		if len(e.states) != 0 || e.complete {
			t.Fatalf("a retention with the policy off left %d prefixes remembered (complete %v)", len(e.states), e.complete)
		}
		if _, ok := rec.counts()["retain.state_keys"]; ok {
			t.Fatal("a retention that works nothing out counted keys read for it")
		}
		// The checkpoints are still in the database, and a late record must still
		// delete the ones it makes untrue.
		late := edgeRecord(3, "p", sec(15), lifecycle.Delete, 0)
		for _, x := range []store.Store{e, ora} {
			if err := x.Write(bg, cloneAll([]store.Record{late})); err != nil {
				t.Fatal(err)
			}
		}
		p := &pair{t: t, s: e, ref: ora}
		p.same([]identity.Fingerprint{podFP, nodeFP}, []time.Time{sec(10), sec(17), sec(25)}, []uint64{3, store.Latest})
	})
	// A retention that changes nothing forgets nothing.
	t.Run("at the epoch", func(t *testing.T) {
		t.Parallel()
		p := newPair(t, Options{Checkpoints: policy(stress(0))})
		p.write(edgeRecord(1, "p", store.MinEventTime.Add(time.Second), lifecycle.Observe, 0))
		n := len(p.s.states)
		if n == 0 || !p.s.complete {
			t.Fatal("the write left nothing remembered")
		}
		before := dump(t, p.s)
		p.retain(store.MinEventTime)
		if len(p.s.states) != n || !p.s.complete {
			t.Fatalf("after a retention that changed nothing, %d of %d prefixes are remembered (complete %v)", len(p.s.states), n, p.s.complete)
		}
		if !slices.Equal(dump(t, p.s), before) {
			t.Fatal("the retention changed what is stored")
		}
		requireWholeReads(t, p.s)
		p.write(edgeRecord(2, "p", store.MinEventTime.Add(2*time.Second), lifecycle.Observe, 0))
		p.same([]identity.Fingerprint{podFP, nodeFP}, []time.Time{store.MinEventTime.Add(time.Minute)}, []uint64{2, store.Latest})
	})
	t.Run("no checkpoint was ever written", func(t *testing.T) {
		t.Parallel()
		p := newPair(t, Options{Checkpoints: policy(CheckpointOptions{On: true, KMin: 1000})})
		p.write(edgeRecord(1, "p", sec(1), lifecycle.Observe, 0), edgeRecord(2, "p", sec(20), lifecycle.Observe, 0))
		p.retain(sec(10))
		if len(p.s.states) != 0 || p.s.complete {
			t.Fatalf("a retention in a database with no checkpoint left %d prefixes remembered (complete %v)", len(p.s.states), p.s.complete)
		}
		p.write(edgeRecord(3, "p", sec(30), lifecycle.Observe, 0))
		p.same([]identity.Fingerprint{podFP, nodeFP}, []time.Time{sec(10), sec(25), sec(40)}, []uint64{2, 3, store.Latest})
	})
	t.Run("the store reads whole prefixes", func(t *testing.T) {
		t.Parallel()
		p := newPair(t, Options{Checkpoints: policy(stress(0)), fullStateRead: true})
		p.write(edgeRecord(1, "p", sec(1), lifecycle.Observe, 0), edgeRecord(2, "p", sec(20), lifecycle.Observe, 0))
		if p.s.complete {
			t.Fatal("a store that reads whole prefixes opens complete")
		}
		p.retain(sec(10))
		if len(p.s.states) != 0 || p.s.complete {
			t.Fatalf("a whole-reading store left %d prefixes remembered (complete %v)", len(p.s.states), p.s.complete)
		}
	})
}

// Every place the writer drops what it remembers also ends completeness, or the
// prefixes it forgot, which hold checkpoints, would be answered as empty and a
// late record would leave those checkpoints true. A retention that stops, as one
// that fails, leaves the map empty, however late it stops (the last commit of a
// retention is the one that makes what it worked out true). After each, the
// database a store that reads the whole prefix writes is the same, and so are
// the answers.
func TestEveryFailureThatDropsTheStateEndsCompleteness(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name string
		opts func(*Options)
		arm  func(*failures)
		act  func(e *Store) error
		// skipReference says the write act makes did not land.
		skipReference bool
		retain        time.Time
	}{
		{
			name: "a record commit that fails",
			arm:  func(f *failures) { f.record = true },
			act: func(e *Store) error {
				return e.Write(bg, []store.Record{edgeRecord(4, "p", sec(40), lifecycle.Observe, 0)})
			},
			// The write did not land.
			skipReference: true,
		},
		{
			name: "a checkpoint commit that fails without landing",
			arm:  func(f *failures) { f.ckptBefore = true },
			act: func(e *Store) error {
				return e.Write(bg, []store.Record{edgeRecord(4, "p", sec(40), lifecycle.Observe, 0)})
			},
		},
		{
			name: "a checkpoint commit that fails after landing",
			arm:  func(f *failures) { f.ckptAfter = true },
			act: func(e *Store) error {
				return e.Write(bg, []store.Record{edgeRecord(4, "p", sec(40), lifecycle.Observe, 0)})
			},
		},
		{
			name: "a hook commit that fails without landing",
			arm:  func(f *failures) { f.ckptBefore = true },
			act:  func(e *Store) error { return e.Instrument().CheckpointEdges(catalog.L2, podFP, store.Forward, sec(45)) },
		},
		{
			name: "a hook commit that fails after landing",
			arm:  func(f *failures) { f.ckptAfter = true },
			act:  func(e *Store) error { return e.Instrument().CheckpointEdges(catalog.L2, podFP, store.Forward, sec(45)) },
		},
		{
			name:   "a retention that stops at its only commit",
			opts:   func(o *Options) { o.retainStopAfter = 1 },
			act:    func(e *Store) error { return e.Retain(bg, sec(15)) },
			retain: sec(15),
		},
		{
			name:   "a retention that stops at the first of its commits",
			opts:   func(o *Options) { o.retainBatchBytes, o.retainStopAfter = 1, 1 },
			act:    func(e *Store) error { return e.Retain(bg, sec(15)) },
			retain: sec(15),
		},
		{
			name:   "a retention that stops at the last of its commits",
			opts:   func(o *Options) { o.retainBatchBytes, o.retainStopAfter = 1, 2 },
			act:    func(e *Store) error { return e.Retain(bg, sec(15)) },
			retain: sec(15),
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			type side struct {
				e    *Store
				rec  *countRecorder
				f    failures
				opts Options
			}
			sides := []*side{{}, {}}
			for i, s := range sides {
				s.rec = &countRecorder{}
				s.opts = Options{Config: pebblekv.Config{Tuning: pebblekv.TinyTuning(), FS: vfs.NewMem()}, Recorder: s.rec, Checkpoints: policy(stress(0)), fullStateRead: i == 0}
				if tc.opts != nil {
					tc.opts(&s.opts)
				}
				s.f.hooks(&s.opts)
				e, err := Open("db", s.opts)
				if err != nil {
					t.Fatal(err)
				}
				s.e = e
				t.Cleanup(func() { _ = s.e.Close() })
			}
			ora := newRef(t)
			base := []store.Record{
				edgeRecord(1, "p", sec(10), lifecycle.Observe, 0), edgeRecord(2, "q", sec(20), lifecycle.Observe, 0), edgeRecord(3, "p", sec(30), lifecycle.Observe, 0),
			}
			for _, r := range base {
				for _, s := range sides {
					if err := s.e.Write(bg, cloneAll([]store.Record{r})); err != nil {
						t.Fatal(err)
					}
				}
				if err := ora.Write(bg, cloneAll([]store.Record{r})); err != nil {
					t.Fatal(err)
				}
			}
			if tail := sides[1].e; !tail.complete || len(tail.states) == 0 {
				t.Fatalf("the store starts with complete = %v and %d prefixes remembered", tail.complete, len(tail.states))
			}
			var errs []error
			for _, s := range sides {
				if tc.arm != nil {
					tc.arm(&s.f)
				}
				errs = append(errs, tc.act(s.e))
			}
			if fmt.Sprint(errs[0]) != fmt.Sprint(errs[1]) || (errs[0] != nil && !errors.Is(errs[0], errInjected)) {
				t.Fatalf("the stores returned %v and %v", errs[0], errs[1])
			}
			if tail := sides[1].e; tail.complete || len(tail.states) != 0 {
				t.Fatalf("after the failure the map is complete = %v with %d prefixes remembered", tail.complete, len(tail.states))
			}
			if tc.skipReference {
				// A store whose record commit failed stops until it is reopened.
				for _, s := range sides {
					if err := s.e.Write(bg, cloneAll([]store.Record{edgeRecord(4, "p", sec(40), lifecycle.Observe, 0)})); err == nil {
						t.Fatal("a store whose commit failed took another write")
					}
					if err := s.e.Close(); err != nil {
						t.Fatal(err)
					}
					e, err := Open("db", s.opts)
					if err != nil {
						t.Fatal(err)
					}
					s.e = e
				}
			}
			if !tc.skipReference {
				switch {
				case !tc.retain.IsZero():
					_ = ora.Retain(bg, tc.retain)
				case errs[0] != nil: // a hook that failed after landing leaves the checkpoint, which is true
				default:
					if err := ora.Write(bg, cloneAll([]store.Record{edgeRecord(4, "p", sec(40), lifecycle.Observe, 0)})); err != nil {
						t.Fatal(err)
					}
				}
			}
			// A late record, before the checkpoints that exist: every one it makes
			// untrue must go, whatever the writer last knew.
			late := edgeRecord(5, "p", sec(25), lifecycle.Delete, 0)
			if tc.skipReference {
				late.Seq = 4
			}
			for _, s := range sides {
				if err := s.e.Write(bg, cloneAll([]store.Record{late})); err != nil {
					t.Fatal(err)
				}
			}
			if err := ora.Write(bg, cloneAll([]store.Record{late})); err != nil {
				t.Fatal(err)
			}
			if a, b := rawDump(t, sides[0].e), rawDump(t, sides[1].e); !slices.EqualFunc(a, b, func(x, y [2][]byte) bool {
				return bytes.Equal(x[0], y[0]) && bytes.Equal(x[1], y[1])
			}) {
				t.Fatal("the databases differ")
			}
			for _, s := range sides {
				p := &pair{t: t, s: s.e, ref: ora}
				p.same([]identity.Fingerprint{podFP, nodeFP}, []time.Time{sec(15), sec(22), sec(27), sec(35), sec(60)}, []uint64{5, store.Latest})
			}
		})
	}
}

// The prefixes the writer forgets one at a time because reading them failed are
// forgotten with completeness, in every place it does that, and a failed write
// forgets everything. The reads fail on a key that cannot be parsed, put into the
// database for the purpose.
func TestAReadThatFailsEndsCompleteness(t *testing.T) {
	t.Parallel()
	fp := fingerprintOf(catalog.K8sNode, 0x50)
	setup := func(t *testing.T) (*Store, *countRecorder, []byte) {
		rec := &countRecorder{}
		e := openMem(t, Options{Recorder: rec, Checkpoints: policy(CheckpointOptions{On: true, KMin: 2})})
		if err := e.Write(bg, []store.Record{entityRecord(1, fp, "p", sec(10))}); err != nil {
			t.Fatal(err)
		}
		prefix := entityPrefix(t, e, fp)
		if !e.complete || e.states[string(prefix)] == nil {
			t.Fatal("the write left the prefix unremembered or the map incomplete")
		}
		return e, rec, prefix
	}
	// A key at the top of the prefix is met by a read that starts there; one at the
	// bottom, by a read that goes all the way down.
	top := func(prefix []byte) []byte { return recordKey(prefix, math.MaxInt64-5, 1)[:keyLen-1] }
	bottom := func(prefix []byte) []byte { return append(slices.Clone(prefix), 0xFF) }
	t.Run("the lookup before a checkpoint is built", func(t *testing.T) {
		t.Parallel()
		e, rec, prefix := setup(t)
		rawKeyInPrefix(t, e, top(prefix))
		st := e.states[string(prefix)]
		st.below, st.since, st.latest = math.MaxInt64, 5, sec(20).UnixNano() // what is known reaches no further down than this
		e.writeCheckpoints(map[string]struct{}{string(prefix): {}}, nil)
		if rec.counts()["checkpoint.errors"] != 1 || e.states[string(prefix)] != nil || e.complete {
			t.Fatalf("errors %d, prefix remembered %v, complete %v", rec.counts()["checkpoint.errors"], e.states[string(prefix)] != nil, e.complete)
		}
	})
	t.Run("the walk that builds a checkpoint", func(t *testing.T) {
		t.Parallel()
		e, rec, prefix := setup(t)
		rawKeyInPrefix(t, e, bottom(prefix))
		st := e.states[string(prefix)]
		st.since, st.latest = 5, sec(20).UnixNano()
		e.writeCheckpoints(map[string]struct{}{string(prefix): {}}, nil)
		if rec.counts()["checkpoint.errors"] != 1 || e.states[string(prefix)] != nil || e.complete {
			t.Fatalf("errors %d, prefix remembered %v, complete %v", rec.counts()["checkpoint.errors"], e.states[string(prefix)] != nil, e.complete)
		}
	})
	t.Run("the lookup before the hook places a checkpoint", func(t *testing.T) {
		t.Parallel()
		e, _, prefix := setup(t)
		rawKeyInPrefix(t, e, top(prefix))
		e.states[string(prefix)].below = math.MaxInt64
		if err := e.Instrument().CheckpointEntity(catalog.L2, fp, sec(20)); err == nil {
			t.Fatal("the hook did not fail")
		}
		if e.states[string(prefix)] != nil || e.complete {
			t.Fatalf("prefix remembered %v, complete %v", e.states[string(prefix)] != nil, e.complete)
		}
	})
	t.Run("the lookup before a record invalidates checkpoints", func(t *testing.T) {
		t.Parallel()
		e, _, prefix := setup(t)
		rawKeyInPrefix(t, e, top(prefix))
		e.states[string(prefix)].below = math.MaxInt64
		if err := e.Write(bg, []store.Record{entityRecord(2, fp, "p", sec(5))}); err == nil {
			t.Fatal("the write did not fail")
		}
		if len(e.states) != 0 || e.complete {
			t.Fatalf("%d prefixes remembered, complete %v", len(e.states), e.complete)
		}
	})
}

// A map that is complete is never asked about a prefix that holds keys: the one
// a store finds when it opens a database that holds data is not complete and
// reads them, and the one a retention completes has them.
func TestACompleteMapNeverAnswersAPrefixThatHoldsKeys(t *testing.T) {
	t.Parallel()
	cfg := pebblekv.Config{Tuning: pebblekv.TinyTuning(), FS: vfs.NewMem()}
	rec := &countRecorder{}
	ck := CheckpointOptions{On: true, KMin: 2}
	open := func() *Store {
		e, err := Open("db", Options{Config: cfg, Recorder: rec, Checkpoints: policy(ck)})
		if err != nil {
			t.Fatal(err)
		}
		return e
	}
	e := open()
	var seq uint64
	nodes := make([]identity.Fingerprint, 6)
	for i := range nodes {
		nodes[i] = fingerprintOf(catalog.K8sNode, byte(0x50+0x10*i))
	}
	for round := range 5 {
		for i, fp := range nodes {
			for range 1 + i%3 {
				seq++
				if err := e.Write(bg, []store.Record{entityRecord(seq, fp, "p", sec(10*round+i+1))}); err != nil {
					t.Fatal(err)
				}
			}
		}
	}
	if err := e.Close(); err != nil {
		t.Fatal(err)
	}
	answered := func(e *Store, fp identity.Fingerprint) *prefixState {
		t.Helper()
		st, err := e.state(entityPrefix(t, e, fp))
		if err != nil {
			t.Fatal(err)
		}
		return st
	}
	e = open()
	if e.complete {
		t.Fatal("a database with data opens complete")
	}
	before := rec.counts()["checkpoint.loads"]
	for _, fp := range nodes {
		if st := answered(e, fp); st.empty() {
			t.Fatal("the writer took a prefix that holds records for empty")
		}
	}
	if n := rec.counts()["checkpoint.loads"] - before; n != int64(len(nodes)) {
		t.Fatalf("the writer read %d of %d prefixes it had not met", n, len(nodes))
	}
	if answered(e, fingerprintOf(catalog.K8sNode, 0xf0)).empty() != true {
		t.Fatal("a prefix that holds nothing was not found empty")
	}
	// A retention completes the map; each prefix is then answered from memory with
	// what it holds, and a new one with nothing.
	if err := e.Retain(bg, sec(25)); err != nil {
		t.Fatal(err)
	}
	if !e.complete {
		t.Fatal("the retention did not complete the map")
	}
	requireWholeReads(t, e)
	before = rec.counts()["checkpoint.loads"]
	for _, fp := range nodes {
		if st := answered(e, fp); st.empty() {
			t.Fatal("after the retention, the writer took a prefix that holds records for empty")
		}
	}
	if answered(e, fingerprintOf(catalog.K8sNode, 0xe0)).empty() != true {
		t.Fatal("a new prefix was not found empty")
	}
	if n := rec.counts()["checkpoint.loads"] - before; n != 0 {
		t.Fatalf("the writer read %d prefixes of a complete map", n)
	}
	// And it survives the store: the next opening holds data, so reads again.
	if err := e.Close(); err != nil {
		t.Fatal(err)
	}
	e = open()
	defer func() { _ = e.Close() }()
	if e.complete {
		t.Fatal("a database with data opens complete after a retention")
	}
}

// Whole histories through the store that works the state out and the one that
// reads it: the same database and the same counts, but for the reads, and the
// store that works it out reads no prefix at all.
func TestAHistoryNeedsNoStateReadsOnceItsRetentionsHaveWorkedItOut(t *testing.T) {
	t.Parallel()
	type side struct {
		e   *Store
		rec *countRecorder
	}
	var sides []*side
	for _, whole := range []bool{true, false} {
		s := &side{rec: &countRecorder{}}
		e, err := Open("db", Options{Config: pebblekv.Config{Tuning: pebblekv.TinyTuning(), FS: vfs.NewMem()}, Recorder: s.rec, Checkpoints: policy(CheckpointOptions{On: true, KMin: 3, Alpha: 2, Lag: 2}), fullStateRead: whole, retainBatchBytes: 2000})
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = e.Close() }()
		s.e = e
		sides = append(sides, s)
	}
	ora := newRef(t)
	nodes := make([]identity.Fingerprint, 40)
	for i := range nodes {
		nodes[i] = fingerprintOf(catalog.K8sNode, byte(0x50+i))
	}
	var seq uint64
	horizon := 0
	for round := range 6 {
		for i := range 60 {
			seq++
			// New entities keep arriving, and every few records one is late.
			fp := nodes[(i*7+round*3)%(8+round*5)]
			at := horizon + 5 + round*10 + i/3
			if i%5 == 0 {
				at -= 4
			}
			r := entityRecord(seq, fp, lifecycle.Producer([]string{"p", "q"}[i%2]), sec(at))
			for _, x := range []store.Store{sides[0].e, sides[1].e, ora} {
				if err := x.Write(bg, cloneAll([]store.Record{r})); err != nil {
					t.Fatal(err)
				}
			}
		}
		horizon += 10 + round*10
		for _, x := range []store.Store{sides[0].e, sides[1].e, ora} {
			if err := x.Retain(bg, sec(horizon)); err != nil {
				t.Fatal(err)
			}
		}
		if !sides[1].e.complete || sides[0].e.complete {
			t.Fatalf("round %d: complete is %v, and %v for the whole reader", round, sides[1].e.complete, sides[0].e.complete)
		}
		requireWholeReads(t, sides[1].e)
	}
	if a, b := rawDump(t, sides[0].e), rawDump(t, sides[1].e); !slices.EqualFunc(a, b, func(x, y [2][]byte) bool {
		return bytes.Equal(x[0], y[0]) && bytes.Equal(x[1], y[1])
	}) {
		t.Fatal("the databases differ")
	}
	ca, cb := sides[0].rec.counts(), sides[1].rec.counts()
	if ca["retain.state_keys"] != 0 || cb["retain.state_keys"] == 0 {
		t.Fatalf("keys read to work the state out: %d for the whole reader, %d for the other", ca["retain.state_keys"], cb["retain.state_keys"])
	}
	delete(ca, "retain.state_keys")
	delete(cb, "retain.state_keys")
	if ca["checkpoint.loads"] == 0 || cb["checkpoint.loads"] != 0 {
		t.Fatalf("loads: %d for the whole reader and %d for the store that works the state out; want some and none", ca["checkpoint.loads"], cb["checkpoint.loads"])
	}
	for _, m := range []map[string]int64{ca, cb} {
		delete(m, "checkpoint.loads")
		delete(m, "checkpoint.load_keys")
		delete(m, "checkpoint.lookups")
		delete(m, "write.iterators")     // what they open to read the state
		delete(m, "checkpoint.build_ns") // a time
	}
	if fmt.Sprint(ca) != fmt.Sprint(cb) {
		t.Fatalf("the counts differ:\n%v\n%v", ca, cb)
	}
	if ca["checkpoint.written"] == 0 || ca["checkpoint.invalidated"] == 0 {
		t.Fatalf("the history did not write and invalidate checkpoints: %v", ca)
	}
	p := &pair{t: t, s: sides[1].e, ref: ora}
	p.same(nodes[:12], []time.Time{sec(horizon), sec(horizon + 30), sec(horizon + 200)}, []uint64{seq, store.Latest})
}

// A key at or after the horizon that cannot be read is one a retention does not
// otherwise look at, so it succeeds; the state it would have worked out from the
// prefix is not known, and neither is the map complete. The next write to the
// prefix meets the key when it reads the prefix, and fails as a store that
// reads whole prefixes does.
func TestAnUnreadableKeyAfterTheHorizonLeavesTheMapEmptyAndIncomplete(t *testing.T) {
	t.Parallel()
	fp1, fp2 := fingerprintOf(catalog.K8sNode, 0x50), fingerprintOf(catalog.K8sNode, 0x60)
	sides := []*Store{}
	rec := &countRecorder{}
	for _, whole := range []bool{true, false} {
		opts := Options{Checkpoints: policy(stress(0)), fullStateRead: whole}
		if !whole {
			opts.Recorder = rec
		}
		e := openMem(t, opts)
		for i, fp := range []identity.Fingerprint{fp1, fp2, fp1, fp2} {
			if err := e.Write(bg, []store.Record{entityRecord(uint64(i+1), fp, "p", sec(10*(i+1)))}); err != nil {
				t.Fatal(err)
			}
		}
		rawKeyInPrefix(t, e, recordKey(entityPrefix(t, e, fp1), sec(100).UnixNano(), 1)[:keyLen-1])
		sides = append(sides, e)
	}
	if tail := sides[1]; !tail.ckpt.On || !tail.anyCkpt {
		t.Fatal("the retention below would not try to work the state out")
	}
	for _, e := range sides {
		if err := e.Retain(bg, sec(15)); err != nil {
			t.Fatalf("the retention failed: %v", err)
		}
	}
	// It tried: the keys it read are counted, the unreadable one among them.
	if rec.counts()["retain.state_keys"] == 0 {
		t.Fatal("the retention did not try to work the state out, so the test shows nothing")
	}
	if tail := sides[1]; len(tail.states) != 0 || tail.complete {
		t.Fatalf("after a retention that met a key it cannot read, %d prefixes are remembered (complete %v)", len(tail.states), tail.complete)
	}
	var errs []error
	for _, e := range sides {
		errs = append(errs, e.Write(bg, []store.Record{entityRecord(5, fp1, "p", sec(50))}))
	}
	if errs[0] == nil || errs[1] == nil || errs[0].Error() != errs[1].Error() {
		t.Fatalf("a write to the prefix returned %v from the whole reader and %v from the other; want the same error", errs[0], errs[1])
	}
	for _, e := range sides {
		if err := e.Write(bg, []store.Record{entityRecord(5, fp2, "p", sec(50))}); err != nil {
			t.Fatalf("a write to another prefix failed: %v", err)
		}
	}
}
