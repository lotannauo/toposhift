package pebblestore

import (
	"context"
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
	"github.com/lotannauo/toposhift/internal/store/storetest"
)

// checkpoints are the ordinary checkpoints under one direction's prefix, as
// (instant, W).
func checkpoints(all []stored, dir byte) [][2]int64 {
	var out [][2]int64
	for _, s := range all {
		if s.dir == dir && s.kind == kindCheckpoint {
			out = append(out, [2]int64{s.ns, int64(s.w)})
		}
	}
	slices.SortFunc(out, func(a, b [2]int64) int { return int(a[0] - b[0]) })
	return out
}

func stress(lag time.Duration) CheckpointOptions {
	return CheckpointOptions{On: true, KMin: 1, Lag: lag}
}

func sec(n int) time.Time { return t0.Add(time.Duration(n) * time.Second) }

func peers(names ...string) map[string]identity.Fingerprint {
	out := map[string]identity.Fingerprint{}
	for i, n := range names {
		out[n] = fingerprintOf(catalog.K8sNode, byte(0x40+i*0x10))
	}
	return out
}

func edgeTo(seq uint64, peer identity.Fingerprint, producer lifecycle.Producer, at time.Time, kind lifecycle.Kind, ttl time.Duration) store.Record {
	r := edgeRecord(seq, producer, at, kind, ttl)
	r.Subject = store.EdgeSubject(podFP, peer, catalog.ScheduledOn)
	return r
}

// The first of the two wrong answers a checkpoint can give silently: written at
// exactly the horizon it sorts before the baseline and, built from a walk that
// starts below the baseline's key, comes out empty and hides it. No checkpoint is
// written at or below the horizon, whatever the lag makes of the policy.
func TestNoCheckpointHidesTheBaseline(t *testing.T) {
	t.Parallel()
	p := newPair(t, Options{Checkpoints: policy(stress(time.Nanosecond))})
	a, b := fingerprintOf(catalog.K8sNode, 0x50), fingerprintOf(catalog.K8sNode, 0x60)
	p.write(edgeTo(1, a, "q", sec(100), lifecycle.Observe, 0))
	p.retain(sec(200))
	// The newest record is at the horizon, so a lag of one nanosecond puts the
	// checkpoint at the horizon.
	p.write(edgeTo(2, b, "p", sec(200), lifecycle.Observe, 0))
	for _, c := range checkpoints(dump(t, p.s), byte(store.Forward)) {
		if c[0] <= sec(200).UnixNano() {
			t.Fatalf("a checkpoint at %d, at or below the horizon %d", c[0], sec(200).UnixNano())
		}
	}
	got, err := p.s.Neighbors(bg, podFP, store.Forward, sec(300), store.Current(catalog.L2))
	if err != nil || len(got) != 2 {
		t.Fatalf("Neighbors = %v, %v; want both the baseline's edge and the new one", got, err)
	}
	p.same([]identity.Fingerprint{podFP, a, b}, []time.Time{sec(200), sec(300)}, []uint64{1, 2, store.Latest})
}

// The second: a checkpoint built on an older one depends on everything the older
// one did, not only on the records it walked. With a lag of ten seconds the
// policy writes the checkpoint of the first batch at 5s+1ns and of the second at
// 20s+1ns, built on the first.
func TestACheckpointBuiltOnAnotherDependsOnIt(t *testing.T) {
	t.Parallel()
	p := newPair(t, Options{Checkpoints: policy(stress(10 * time.Second))})
	n := peers("x", "p", "q", "y")
	p.write(
		edgeTo(1, n["x"], "h", sec(15), lifecycle.Observe, 0),
		edgeTo(2, n["p"], "h", sec(0), lifecycle.Observe, 0),
		edgeTo(3, n["q"], "h", sec(3), lifecycle.Observe, 0),
		edgeTo(4, n["p"], "h", sec(5), lifecycle.Delete, 0),
	)
	p.write(edgeTo(5, n["y"], "h", sec(30), lifecycle.Observe, 0))
	got := checkpoints(dump(t, p.s), byte(store.Forward))
	want := [][2]int64{{sec(5).UnixNano() + 1, 4}, {sec(20).UnixNano() + 1, 4}}
	if !slices.Equal(got, want) {
		t.Fatalf("checkpoints (instant, W) = %v, want %v: the second depends on the first's sequence 4, not only on the record it walked", got, want)
	}
	fps := []identity.Fingerprint{podFP}
	for _, v := range n {
		fps = append(fps, v)
	}
	p.same(fps, []time.Time{sec(10), sec(25), sec(40)}, []uint64{0, 1, 2, 3, 4, 5, store.Latest})
	// The token that has seen only s1 and s2 must see {x, p}: it skips both.
	ns, err := p.s.Neighbors(bg, podFP, store.Forward, sec(25), store.Scope{Layer: catalog.L2, AsOf: 2})
	if err != nil || len(ns) != 2 {
		t.Fatalf("pinned at 2: %v, %v", ns, err)
	}
}

// A record written later with an event time before a checkpoint makes it untrue
// and deletes it, in both stored copies of an edge and for an entity; one at
// exactly its instant, or after it, does not.
func TestInvalidationBoundaries(t *testing.T) {
	t.Parallel()
	c := sec(100)
	for name, tc := range map[string]struct {
		at          time.Time
		invalidates bool
	}{
		"a nanosecond before": {c.Add(-time.Nanosecond), true},
		"exactly at":          {c, false},
		"a nanosecond after":  {c.Add(time.Nanosecond), false},
		"long before":         {sec(1), true},
		"long after":          {sec(500), false},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			p := newPair(t, Options{Checkpoints: off()})
			entity := store.Record{
				Layer: catalog.L2, Subject: store.EntitySubject(podFP), Producer: "p", EventTime: sec(10), Seq: 2,
				Kind: lifecycle.Observe, Payload: []byte("e"),
			}
			p.write(edgeRecord(1, "p", sec(10), lifecycle.Observe, 0), entity)
			for _, f := range []func() error{
				func() error { return p.s.Instrument().CheckpointEdges(catalog.L2, podFP, store.Forward, c) },
				func() error { return p.s.Instrument().CheckpointEdges(catalog.L2, nodeFP, store.Reverse, c) },
				func() error { return p.s.Instrument().CheckpointEntity(catalog.L2, podFP, c) },
			} {
				if err := f(); err != nil {
					t.Fatal(err)
				}
			}
			if got := len(slices.DeleteFunc(dump(t, p.s), func(s stored) bool { return s.kind != kindCheckpoint })); got != 3 {
				t.Fatalf("%d checkpoints before the late record, want 3", got)
			}
			late := edgeRecord(3, "q", tc.at, lifecycle.Delete, 0)
			lateEntity := entity
			lateEntity.Seq, lateEntity.EventTime, lateEntity.Kind, lateEntity.Payload = 4, tc.at, lifecycle.Delete, nil
			p.write(late, lateEntity)
			left := len(slices.DeleteFunc(dump(t, p.s), func(s stored) bool { return s.kind != kindCheckpoint }))
			if want := map[bool]int{true: 0, false: 3}[tc.invalidates]; left != want {
				t.Fatalf("%d checkpoints left after a record %s, want %d", left, name, want)
			}
			p.same([]identity.Fingerprint{podFP, nodeFP}, []time.Time{c.Add(-time.Nanosecond), c, c.Add(time.Nanosecond), sec(600)}, []uint64{0, 1, 2, 3, 4, store.Latest})
		})
	}
}

// A checkpoint is used only by a token that has seen everything it depends on,
// and only if built by the fold logic this code has: otherwise the read walks on,
// and still answers as the reference does.
func TestACheckpointIsSkippedWhenItCannotBeTrusted(t *testing.T) {
	t.Parallel()
	rec := newMemRecorder()
	p := newPair(t, Options{Checkpoints: policy(stress(0)), Recorder: rec})
	p.write(
		edgeRecord(1, "p", sec(10), lifecycle.Observe, 0),
		edgeRecord(2, "q", sec(12), lifecycle.Observe, 5*time.Second),
		edgeRecord(3, "p", sec(20), lifecycle.Delete, 0),
	)
	sc := func(tok uint64) store.Scope { return store.Scope{Layer: catalog.L2, AsOf: tok} }
	read := func(tok uint64) []store.Neighbor {
		ns, err := p.s.Neighbors(bg, podFP, store.Forward, sec(60), sc(tok))
		if err != nil {
			t.Fatal(err)
		}
		return ns
	}
	stepped := func() int64 { return rec.Counter("read.records_stepped") }
	before := stepped()
	if len(read(store.Latest)) != 0 || rec.Counter("read.checkpoint_hits") != 1 {
		t.Fatalf("a token at W did not use the checkpoint: hits %d", rec.Counter("read.checkpoint_hits"))
	}
	if got := stepped() - before; got != 0 {
		t.Fatalf("a read that used the checkpoint stepped over %d records; stopping early is the point", got)
	}
	before = stepped()
	if len(read(2)) != 1 || rec.Counter("read.checkpoint_skipped_w") != 1 {
		t.Fatalf("a token below W did not skip it, or got the wrong answer: skipped %d", rec.Counter("read.checkpoint_skipped_w"))
	}
	if got := stepped() - before; got != 3 {
		t.Fatalf("a read that skipped the checkpoint stepped over %d records, want all 3", got)
	}

	// The same checkpoint stamped by another fold version is not a base for reads.
	sides, _ := p.s.sidesOf(edgeRecord(1, "p", sec(0), lifecycle.Observe, 0))
	var key []byte
	for _, c := range checkpoints(dump(t, p.s), byte(store.Forward)) {
		key = stampKey(sides[0].prefix, c[0], kindCheckpoint)
	}
	raw, closer, err := p.s.kv.Get(key)
	if err != nil {
		t.Fatal(err)
	}
	st, err := decodeStamp(raw)
	_ = closer.Close()
	if err != nil {
		t.Fatal(err)
	}
	st.foldVersion++
	val, _ := appendStamp(nil, st)
	if err := p.s.kv.Set(key, val, nil); err != nil {
		t.Fatal(err)
	}
	if len(read(store.Latest)) != 0 || rec.Counter("read.checkpoint_skipped_version") != 1 {
		t.Fatalf("another fold version's checkpoint was used, or the answer is wrong: skipped %d", rec.Counter("read.checkpoint_skipped_version"))
	}
	p.same([]identity.Fingerprint{podFP, nodeFP}, []time.Time{sec(11), sec(15), sec(25), sec(60)}, []uint64{0, 1, 2, 3, store.Latest})

	// And it is not a base for a new checkpoint either: here its content is made
	// wrong as well as its version, so a build that took it would answer wrongly.
	// An edge to a peer nothing else has, so the garbage shows in the answer.
	bogus, err := p.s.sidesOf(edgeTo(1, fingerprintOf(catalog.K8sNode, 0x90), "bogus", sec(0), lifecycle.Observe, 0))
	if err != nil {
		t.Fatal(err)
	}
	st.entries, st.w = []entry{entryOf(bogus[0].ref, sec(1).UnixNano(), pebblekv.Value{Seq: 1, Kind: lifecycle.Observe})}, 0
	val, _ = appendStamp(nil, st)
	if err := p.s.kv.Set(key, val, nil); err != nil {
		t.Fatal(err)
	}
	p.write(edgeRecord(4, "r", sec(30), lifecycle.Observe, 0))
	p.same([]identity.Fingerprint{podFP, nodeFP}, []time.Time{sec(11), sec(25), sec(31), sec(60)}, []uint64{0, 1, 2, 3, 4, store.Latest})
}

// Retention removes the checkpoints at or below the horizon, the one exactly at it
// included, and keeps those after it, with every answer the reference's.
func TestRetentionAndCheckpoints(t *testing.T) {
	t.Parallel()
	p := newPair(t, Options{Checkpoints: policy(stress(0))})
	var seq uint64
	next := func(r store.Record) store.Record { seq++; r.Seq = seq; return r }
	for i := range 10 {
		p.write(next(edgeRecord(0, lifecycle.Producer(fmt.Sprintf("p%d", i%3)), sec(10*(i+1)), lifecycle.Observe, 35*time.Second)))
	}
	before := checkpoints(dump(t, p.s), byte(store.Forward))
	if len(before) < 5 {
		t.Fatalf("only %d checkpoints before the retention", len(before))
	}
	h := time.Unix(0, before[3][0]).UTC() // exactly at one of them
	p.retain(h)
	for _, c := range checkpoints(dump(t, p.s), byte(store.Forward)) {
		if c[0] <= h.UnixNano() {
			t.Fatalf("a checkpoint at %d survived a retention at %d", c[0], h.UnixNano())
		}
	}
	if len(checkpoints(dump(t, p.s), byte(store.Forward))) == 0 {
		t.Fatal("every checkpoint went, including those after the horizon")
	}
	last := p.s.LastSeq()
	p.write(next(edgeRecord(0, "late", h, lifecycle.Observe, time.Hour)), next(edgeRecord(0, "p0", h.Add(time.Second), lifecycle.Delete, 0)))
	p.same([]identity.Fingerprint{podFP, nodeFP}, []time.Time{h, h.Add(time.Second), sec(105), sec(300)}, []uint64{last, last + 1, p.s.LastSeq(), store.Latest})
}

// What the writer remembers is rebuilt from the database: after a reopening, with
// checkpoints on or off, a late record still deletes the checkpoint it makes
// untrue, and one that is rebuilt depends on the late record.
func TestACheckpointIsInvalidatedAfterAReopening(t *testing.T) {
	t.Parallel()
	for name, opts := range map[string]CheckpointOptions{"on": stress(0), "off": {}} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			fs := vfs.NewMem()
			cfg := pebblekv.Config{Tuning: pebblekv.TinyTuning(), FS: fs}
			e, err := Open("db", Options{Config: cfg, Checkpoints: policy(stress(0))})
			if err != nil {
				t.Fatal(err)
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
			c1, c2 := sec(10).UnixNano()+1, sec(20).UnixNano()+1
			if got := checkpoints(dump(t, e), byte(store.Forward)); len(got) != 2 || got[0][0] != c1 || got[1][0] != c2 {
				t.Fatalf("checkpoints before the reopening = %v, want at %d and %d", got, c1, c2)
			}
			if err := e.Close(); err != nil {
				t.Fatal(err)
			}
			e, err = Open("db", Options{Config: cfg, Checkpoints: policy(opts)})
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = e.Close() }()
			// p deletes at 15s, after the first checkpoint's instant and before the
			// second's: the second no longer holds, and an answer that still used it
			// would say p is alive.
			late := edgeRecord(3, "p", sec(15), lifecycle.Delete, 0)
			for _, x := range []store.Store{e, ora} {
				if err := x.Write(bg, cloneAll([]store.Record{late})); err != nil {
					t.Fatal(err)
				}
			}
			for _, c := range checkpoints(dump(t, e), byte(store.Forward)) {
				if c[0] == c1 && c[1] != 1 {
					t.Errorf("the first checkpoint was rebuilt with W %d", c[1])
				}
				if c[0] == c2 && c[1] != 3 {
					t.Errorf("a checkpoint at %d survives or was rebuilt with W %d, want one that depends on the late record (W 3)", c2, c[1])
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

// Pebble can report an error from a commit whose batch is already in. If that was
// the first checkpoint the store ever wrote, it must still know there is one, or a
// late record would leave it in place; the same for a checkpoint written by the
// hook.
func TestACheckpointCommitThatFailedAfterLandingIsStillInvalidated(t *testing.T) {
	t.Parallel()
	t.Run("a rebuild cannot hide it", func(t *testing.T) {
		t.Parallel()
		// With a spacing of one, the policy rebuilds the checkpoint at the same
		// instant after the late record and overwrites a stale one, hiding the bug.
		// A newer record in the late batch makes the policy build on the stale one
		// instead, which never saw the late record.
		fail := true
		p := newPair(t, Options{Checkpoints: policy(stress(0)), afterCheckpointApply: func() error {
			if fail {
				fail = false
				return errInjected
			}
			return nil
		}})
		a, b := fingerprintOf(catalog.K8sNode, 0x50), fingerprintOf(catalog.K8sNode, 0x60)
		p.write(edgeTo(1, a, "q", sec(2), lifecycle.Observe, 0), edgeTo(2, b, "p", sec(10), lifecycle.Observe, 0))
		p.write(edgeTo(3, a, "q", sec(5), lifecycle.Delete, 0), edgeTo(4, b, "p", sec(20), lifecycle.Observe, 0))
		p.same([]identity.Fingerprint{podFP, a, b}, []time.Time{sec(1), sec(7), sec(12), sec(25), sec(60)}, []uint64{0, 1, 2, 3, 4, store.Latest})
	})
	for name, viaHook := range map[string]bool{"the policy": false, "the hook": true} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			fail := true
			opts := Options{Checkpoints: policy(stress(0)), afterCheckpointApply: func() error {
				if fail {
					fail = false
					return errInjected
				}
				return nil
			}}
			if viaHook {
				opts.Checkpoints = off()
			}
			p := newPair(t, opts)
			a, b := fingerprintOf(catalog.K8sNode, 0x50), fingerprintOf(catalog.K8sNode, 0x60)
			p.write(edgeTo(1, a, "q", sec(2), lifecycle.Observe, 0), edgeTo(2, b, "p", sec(10), lifecycle.Observe, 0))
			if viaHook {
				if err := p.s.Instrument().CheckpointEdges(catalog.L2, podFP, store.Forward, sec(11)); !errors.Is(err, errInjected) {
					t.Fatalf("the hook returned %v, want the injected failure", err)
				}
			}
			if len(checkpoints(dump(t, p.s), byte(store.Forward))) != 1 {
				t.Fatal("the commit that failed after landing left no checkpoint")
			}
			p.write(edgeTo(3, a, "q", sec(5), lifecycle.Delete, 0)) // before the checkpoint
			got, err := p.s.Neighbors(bg, podFP, store.Forward, sec(60), store.Current(catalog.L2))
			if err != nil || len(got) != 1 {
				t.Fatalf("Neighbors = %v, %v; want only p's edge", got, err)
			}
			p.same([]identity.Fingerprint{podFP, a, b}, []time.Time{sec(1), sec(7), sec(12), sec(60)}, []uint64{0, 1, 2, 3, store.Latest})
		})
	}
}

func TestNothingIsRememberedWithoutCheckpoints(t *testing.T) {
	t.Parallel()
	e := openMem(t, Options{Checkpoints: off()})
	if err := e.Write(bg, []store.Record{edgeRecord(1, "p", sec(1), lifecycle.Observe, 0)}); err != nil {
		t.Fatal(err)
	}
	if len(e.states) != 0 {
		t.Fatalf("a store with no checkpoints, and none in its database, remembers %d prefixes", len(e.states))
	}
}

func TestNegativeCheckpointOptionsAreRefused(t *testing.T) {
	t.Parallel()
	for name, o := range map[string]CheckpointOptions{
		"lag": {On: true, Lag: -time.Second}, "KMin": {On: true, KMin: -1}, "alpha": {On: true, Alpha: -1},
		"NaN alpha": {On: true, Alpha: math.NaN()}, "infinite alpha": {On: true, Alpha: math.Inf(1)},
	} {
		cfg := pebblekv.Config{Tuning: pebblekv.TinyTuning(), FS: vfs.NewMem()}
		if _, err := Open("db", Options{Config: cfg, Checkpoints: policy(o)}); !errors.Is(err, store.ErrInvalid) {
			t.Errorf("a negative %s: Open = %v, want an error wrapping ErrInvalid", name, err)
		}
	}
}

// The meta key says a checkpoint was ever written, so a store over a database
// that has none need not read its prefixes.
func TestFirstCheckpointSetsTheFlag(t *testing.T) {
	t.Parallel()
	e := openMem(t, Options{Checkpoints: policy(stress(0))})
	flag := func() bool {
		v, err := e.kv.GetMeta(metaKey(metaCheckpoints))
		if err != nil {
			t.Fatal(err)
		}
		return v != nil
	}
	if flag() || e.anyCkpt {
		t.Fatal("a new database says it has checkpoints")
	}
	if err := e.Write(bg, []store.Record{edgeRecord(1, "p", sec(1), lifecycle.Observe, 0)}); err != nil {
		t.Fatal(err)
	}
	if !flag() || !e.anyCkpt {
		t.Fatal("the first checkpoint did not set the flag")
	}
}

func TestThePolicy(t *testing.T) {
	t.Parallel()
	st := &prefixState{latest: sec(10).UnixNano(), since: 5, sinceBytes: 500, lastBytes: 2000}
	for name, tc := range map[string]struct {
		o    CheckpointOptions
		want bool
	}{
		"off":                          {CheckpointOptions{}, false},
		"enough since the last":        {CheckpointOptions{On: true, KMin: 5}, true},
		"not enough":                   {CheckpointOptions{On: true, KMin: 6}, false},
		"a zero KMin is one":           {CheckpointOptions{On: true}, true},
		"alpha asks for more (20 > 5)": {CheckpointOptions{On: true, KMin: 1, Alpha: 1}, false},
		"alpha asks for less (2 <= 5)": {CheckpointOptions{On: true, KMin: 1, Alpha: 0.1}, true},
	} {
		if got := tc.o.due(st); got != tc.want {
			t.Errorf("%s: due = %v, want %v", name, got, tc.want)
		}
	}
	if (CheckpointOptions{On: true}).due(&prefixState{latest: 5}) {
		t.Error("an untouched prefix is due")
	}
	o := CheckpointOptions{On: true, Lag: 3 * time.Second}
	if c, ok := o.at(st); !ok || c != sec(10).UnixNano()+1-3e9 {
		t.Errorf("at = %d, %v", c, ok)
	}
	if _, ok := (CheckpointOptions{On: true}).at(&prefixState{latest: -1}); ok {
		t.Error("an empty prefix has an instant")
	}
	if _, ok := (CheckpointOptions{On: true}).at(&prefixState{latest: 1<<63 - 1}); ok {
		t.Error("an instant after the last one is possible")
	}
	if _, ok := (CheckpointOptions{On: true, Lag: time.Hour}).at(&prefixState{latest: 5}); ok {
		t.Error("an instant before 1970 is possible")
	}
}

func TestTheHookRefusesWhatCannotBePlaced(t *testing.T) {
	t.Parallel()
	e := openMem(t, Options{Checkpoints: off()})
	if err := e.Write(bg, []store.Record{edgeRecord(1, "p", sec(100), lifecycle.Observe, 0)}); err != nil {
		t.Fatal(err)
	}
	if err := e.Retain(bg, sec(50)); err != nil {
		t.Fatal(err)
	}
	for name, c := range map[string]time.Time{
		"at the horizon":   sec(50),
		"below it":         sec(40),
		"before the epoch": store.MinEventTime.Add(-time.Second),
		"after the last":   store.MaxEventTime.Add(time.Second),
		"at the epoch":     store.MinEventTime,
	} {
		if err := e.Instrument().CheckpointEdges(catalog.L2, podFP, store.Forward, c); !errors.Is(err, store.ErrInvalid) {
			t.Errorf("a checkpoint %s = %v, want ErrInvalid", name, err)
		}
	}
	if err := e.Instrument().CheckpointEdges(catalog.L2, fingerprintOf("nonesuch", 0), store.Forward, sec(60)); !errors.Is(err, store.ErrInvalid) {
		t.Errorf("a checkpoint for an unknown type = %v", err)
	}
	if err := e.Instrument().CheckpointEdges(catalog.L2, podFP, 0, sec(60)); !errors.Is(err, store.ErrInvalid) {
		t.Errorf("a checkpoint in direction 0 = %v", err)
	}
	if err := e.Instrument().CheckpointEdges(catalog.L2, podFP, store.Forward, sec(60)); err != nil {
		t.Fatal(err)
	}
	if err := e.Instrument().CheckpointEdges(catalog.L2, podFP, store.Forward, sec(60)); err != nil {
		t.Errorf("writing the same checkpoint twice = %v", err)
	}
	if got := checkpoints(dump(t, e), byte(store.Forward)); len(got) != 1 {
		t.Errorf("checkpoints = %v, want one", got)
	}
	// After the end of the range every instant is at or below the horizon.
	if err := e.Retain(bg, store.MaxEventTime.Add(time.Hour)); err != nil {
		t.Fatal(err)
	}
	if err := e.Instrument().CheckpointEdges(catalog.L2, podFP, store.Forward, store.MaxEventTime); !errors.Is(err, store.ErrInvalid) {
		t.Errorf("a checkpoint after a retention past the end of time = %v", err)
	}
}

// The checkpoints are actually written and used, and late records actually
// invalidate them, in a generated stream: otherwise the variants would prove
// nothing about them.
func TestTheWorkloadsExerciseCheckpoints(t *testing.T) {
	if storetest.Trimmed() {
		t.Skip("trimmed run: the workloads run in the full tier")
	}
	t.Parallel()
	for name, c := range map[string]CheckpointOptions{
		"stress": stress(0), "lag 1ns": stress(time.Nanosecond), "lag 2s": {On: true, KMin: 3, Alpha: 1, Lag: 2 * time.Second},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			rec := newMemRecorder()
			s := openMem(t, Options{Checkpoints: policy(c), Recorder: rec})
			cfg := storetest.Tiny()
			cfg.Seed, cfg.Duration, cfg.LateProbability, cfg.LateMax = 311, 4*time.Minute, 0.4, 20*time.Second
			if err := storetest.Check(s, storetest.Workload{Name: name, Config: cfg}, storetest.Options{RetainAt: []float64{0.5}}); err != nil {
				t.Fatal(err)
			}
			for _, n := range []string{"checkpoint.written", "read.checkpoint_hits", "checkpoint.build_records_walked"} {
				if rec.Counter(n) == 0 {
					t.Errorf("%s = 0", n)
				}
			}
			if c.Lag == 0 && rec.Counter("checkpoint.invalidated") == 0 {
				t.Error("no late record invalidated a checkpoint")
			}
		})
	}
}

// A checkpoint commit that fails without landing leaves nothing on disk, and the
// next one must still put the meta key in its batch: otherwise a database holds a
// checkpoint and no flag, and after a reopening a late record leaves it in place.
func TestAFailedFirstCheckpointCommitDoesNotLoseTheFlag(t *testing.T) {
	t.Parallel()
	fs := vfs.NewMem()
	cfg := pebblekv.Config{Tuning: pebblekv.TinyTuning(), FS: fs}
	fail := true
	e, err := Open("db", Options{Config: cfg, Checkpoints: policy(stress(0)), beforeCheckpointApply: func() error {
		if fail {
			fail = false
			return errInjected
		}
		return nil
	}})
	if err != nil {
		t.Fatal(err)
	}
	ora := newRef(t)
	a, b := fingerprintOf(catalog.K8sNode, 0x50), fingerprintOf(catalog.K8sNode, 0x60)
	for _, r := range []store.Record{
		edgeTo(1, a, "q", sec(2), lifecycle.Observe, 0), // its checkpoint commit fails
		edgeTo(2, b, "p", sec(10), lifecycle.Observe, 0),
	} {
		for _, x := range []store.Store{e, ora} {
			if err := x.Write(bg, cloneAll([]store.Record{r})); err != nil {
				t.Fatal(err)
			}
		}
	}
	if len(checkpoints(dump(t, e), byte(store.Forward))) != 1 {
		t.Fatal("the second checkpoint did not land")
	}
	if v, err := e.kv.GetMeta(metaKey(metaCheckpoints)); err != nil || v == nil {
		t.Fatalf("a checkpoint is on disk and the flag is not: %v, %v", v, err)
	}
	if err := e.Close(); err != nil {
		t.Fatal(err)
	}
	// Reopened with checkpoints on and then off, a late record before the surviving
	// checkpoint's instant must delete it either way: p's edge to b is deleted at 8s,
	// after which it is not alive at 10s or later, and a checkpoint that still held
	// it would say it is.
	for i, opts := range []CheckpointOptions{stress(0), {}} {
		e, err = Open("db", Options{Config: cfg, Checkpoints: policy(opts)})
		if err != nil {
			t.Fatal(err)
		}
		late := edgeTo(e.LastSeq()+1, map[int]identity.Fingerprint{0: a, 1: b}[i], map[int]lifecycle.Producer{0: "q", 1: "p"}[i], sec(5+3*i), lifecycle.Delete, 0)
		for _, x := range []store.Store{e, ora} {
			if err := x.Write(bg, cloneAll([]store.Record{late})); err != nil {
				t.Fatal(err)
			}
		}
		p := &pair{t: t, s: e, ref: ora}
		p.same([]identity.Fingerprint{podFP, a, b}, []time.Time{sec(1), sec(7), sec(9), sec(12), sec(60)}, []uint64{0, 1, 2, 3, 4, store.Latest})
		if err := e.Close(); err != nil {
			t.Fatal(err)
		}
	}
}

// If the Write that trimmed the list of checkpoints in memory does not finish, the
// list is out of date: the checkpoints are still on disk. The state must be dropped,
// or a later late record would leave them in place. A commit that fails stops the
// store until it is reopened, and the reopening reads the state afresh; a context
// that ends just before the commit leaves the store running, with the list trimmed.
func TestAWriteThatDoesNotFinishDoesNotLoseTheCheckpointList(t *testing.T) {
	t.Parallel()
	a, b := fingerprintOf(catalog.K8sNode, 0x50), fingerprintOf(catalog.K8sNode, 0x60)
	// A spacing too wide for the policy to rewrite anything, so a stale checkpoint
	// is not quietly replaced by a rebuilt one.
	wide := policy(CheckpointOptions{On: true, KMin: 1000})
	setup := func(t *testing.T, opts Options) (*pair, store.Record) {
		opts.Checkpoints = wide
		p := newPair(t, opts)
		p.write(edgeTo(1, a, "q", sec(2), lifecycle.Observe, 0), edgeTo(2, b, "p", sec(10), lifecycle.Observe, 0))
		if err := p.s.Instrument().CheckpointEdges(catalog.L2, podFP, store.Forward, sec(11)); err != nil {
			t.Fatal(err)
		}
		return p, edgeTo(3, a, "q", sec(5), lifecycle.Delete, 0) // before the checkpoint
	}
	t.Run("a context that ends before the commit", func(t *testing.T) {
		t.Parallel()
		p, late := setup(t, Options{})
		if got := p.s.states[string(forwardPrefix(t, p.s))]; got == nil || len(got.ckpts) != 1 {
			t.Fatalf("the writer does not know the checkpoint: %+v", got)
		}
		// The first look at the context finds it alive; the last, just before the
		// commit, finds it done.
		calls := 0
		err := p.s.Write(&endsOnTheSecondLook{calls: &calls}, cloneAll([]store.Record{late}))
		if !errors.Is(err, context.Canceled) || calls != 2 || p.s.LastSeq() != 2 {
			t.Fatalf("Write = %v after %d looks, LastSeq %d; want the context's error at the second look and nothing stored", err, calls, p.s.LastSeq())
		}
		if len(p.s.states) != 0 || p.s.complete {
			t.Fatalf("after a Write that did not finish the writer remembers %d prefixes (complete %v)", len(p.s.states), p.s.complete)
		}
		p.write(late) // the same record again: it must delete the checkpoints it makes untrue
		if got, err := p.s.Neighbors(bg, podFP, store.Forward, sec(60), store.Current(catalog.L2)); err != nil || len(got) != 1 {
			t.Fatalf("Neighbors = %v, %v; want only p's edge", got, err)
		}
		p.same([]identity.Fingerprint{podFP, a, b}, []time.Time{sec(1), sec(7), sec(12), sec(60)}, []uint64{0, 1, 2, 3, store.Latest})
	})
	t.Run("a commit that fails", func(t *testing.T) {
		t.Parallel()
		fail := false
		fs := vfs.NewMem()
		cfg := pebblekv.Config{Tuning: pebblekv.TinyTuning(), FS: fs}
		opts := Options{Config: cfg, Checkpoints: wide, beforeRecordApply: func() error {
			if fail {
				fail = false
				return errInjected
			}
			return nil
		}}
		s, err := Open("db", opts)
		if err != nil {
			t.Fatal(err)
		}
		ref := newRef(t)
		for _, r := range []store.Record{edgeTo(1, a, "q", sec(2), lifecycle.Observe, 0), edgeTo(2, b, "p", sec(10), lifecycle.Observe, 0)} {
			for _, x := range []store.Store{s, ref} {
				if err := x.Write(bg, cloneAll([]store.Record{r})); err != nil {
					t.Fatal(err)
				}
			}
		}
		if err := s.Instrument().CheckpointEdges(catalog.L2, podFP, store.Forward, sec(11)); err != nil {
			t.Fatal(err)
		}
		late := edgeTo(3, a, "q", sec(5), lifecycle.Delete, 0)
		fail = true
		if err := s.Write(bg, cloneAll([]store.Record{late})); !errors.Is(err, errInjected) {
			t.Fatalf("the failing write returned %v", err)
		}
		if s.LastSeq() != 2 {
			t.Fatalf("a refused write moved LastSeq to %d", s.LastSeq())
		}
		if len(s.states) != 0 || s.complete {
			t.Fatalf("after a failed commit the writer remembers %d prefixes (complete %v)", len(s.states), s.complete)
		}
		if err := s.Write(bg, cloneAll([]store.Record{late})); err == nil {
			t.Fatal("a store whose commit failed took another write")
		}
		if err := s.Close(); err != nil {
			t.Fatal(err)
		}
		if s, err = Open("db", Options{Config: cfg, Checkpoints: wide}); err != nil {
			t.Fatal(err)
		}
		defer func() { _ = s.Close() }()
		for _, x := range []store.Store{s, ref} {
			if err := x.Write(bg, cloneAll([]store.Record{late})); err != nil {
				t.Fatal(err)
			}
		}
		if got, err := s.Neighbors(bg, podFP, store.Forward, sec(60), store.Current(catalog.L2)); err != nil || len(got) != 1 {
			t.Fatalf("Neighbors = %v, %v; want only p's edge", got, err)
		}
		(&pair{t: t, s: s, ref: ref}).same([]identity.Fingerprint{podFP, a, b}, []time.Time{sec(1), sec(7), sec(12), sec(60)}, []uint64{0, 1, 2, 3, store.Latest})
	})
}

// endsOnTheSecondLook is a context that is alive at the first look and done at the
// second, which is the last look Write takes before its commit.
type endsOnTheSecondLook struct {
	context.Context
	calls *int
}

func (c *endsOnTheSecondLook) Err() error {
	*c.calls++
	if *c.calls >= 2 {
		return context.Canceled
	}
	return nil
}

func (c *endsOnTheSecondLook) Done() <-chan struct{}       { return nil }
func (c *endsOnTheSecondLook) Deadline() (time.Time, bool) { return time.Time{}, false }
func (c *endsOnTheSecondLook) Value(key any) any           { return nil }

// forwardPrefix is the prefix of the pod's forward edges.
func forwardPrefix(t *testing.T, s *Store) []byte {
	t.Helper()
	p, ok := s.prefixOf(catalog.L2, podFP, byte(store.Forward))
	if !ok {
		t.Fatal("no prefix")
	}
	return p
}

// A read counts the entries it decodes from a checkpoint, and from the baseline,
// including those of a checkpoint it then skips: meeting one costs its decoding.
func TestReadsCountTheEntriesTheyDecode(t *testing.T) {
	t.Parallel()
	rec := newMemRecorder()
	p := newPair(t, Options{Checkpoints: policy(stress(0)), Recorder: rec})
	p.write(
		edgeRecord(1, "p", sec(10), lifecycle.Observe, 0),
		edgeRecord(2, "q", sec(12), lifecycle.Observe, 0),
	)
	read := func(tok uint64) {
		t.Helper()
		ns, err := p.s.Neighbors(bg, podFP, store.Forward, sec(60), store.Scope{Layer: catalog.L2, AsOf: tok})
		if err != nil {
			t.Fatal(err)
		}
		if len(ns) != 1 { // two producers refer to the same edge
			t.Fatalf("token %d: %d neighbors, want 1", tok, len(ns))
		}
	}
	read(store.Latest)
	used := rec.Counter("read.checkpoint_entries_decoded")
	if rec.Counter("read.checkpoint_hits") != 1 || used < 1 {
		t.Fatalf("a read that used a checkpoint: %d hits, %d entries decoded", rec.Counter("read.checkpoint_hits"), used)
	}
	read(1) // below the checkpoint's W: skipped, but decoded
	if rec.Counter("read.checkpoint_skipped_w") != 1 || rec.Counter("read.checkpoint_entries_decoded") != 2*used {
		t.Fatalf("a skipped checkpoint: skipped %d, entries decoded %d", rec.Counter("read.checkpoint_skipped_w"), rec.Counter("read.checkpoint_entries_decoded"))
	}
	if rec.Counter("read.baseline_entries_decoded") != 0 {
		t.Fatal("entries of a baseline were counted before there was one")
	}
	if err := p.s.Retain(bg, sec(30)); err != nil {
		t.Fatal(err)
	}
	if err := p.s.Retain(bg, sec(30)); err != nil {
		t.Fatal(err)
	}
	read(store.Latest)
	if rec.Counter("read.baseline_entries_decoded") == 0 {
		t.Fatal("a read that reached the baseline decoded none of its entries")
	}
}

// The bytes of checkpoints written are the key and value bytes of the checkpoints
// that were written, by the policy and by the scripted hook alike: with none
// deleted since, they are the checkpoints the database holds.
func TestTheBytesOfCheckpointsWrittenAreCounted(t *testing.T) {
	t.Parallel()

	rec := newMemRecorder()
	p := newPair(t, Options{Recorder: rec, Checkpoints: off()})
	p.write(edgeRecord(1, "p", sec(10), lifecycle.Observe, 0))
	for _, f := range []func() error{
		func() error { return p.s.Instrument().CheckpointEdges(catalog.L2, podFP, store.Forward, sec(100)) },
		func() error { return p.s.Instrument().CheckpointEdges(catalog.L2, nodeFP, store.Reverse, sec(100)) },
	} {
		if err := f(); err != nil {
			t.Fatal(err)
		}
	}
	heldBy := func(e *Store) (n, bytes int64) {
		it, err := e.kv.NewIter(nil)
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = it.Close() }()
		for ok := it.First(); ok; ok = it.Next() {
			k := it.Key()
			if k[len(k)-1] == kindCheckpoint {
				n++
				bytes += int64(len(k) + len(it.Value()))
			}
		}
		return n, bytes
	}
	held := func() (int64, int64) { return heldBy(p.s) }
	n, bytes := held()
	if n != 2 || bytes == 0 {
		t.Fatalf("%d checkpoints holding %d bytes after two scripted ones", n, bytes)
	}
	if got := rec.Counter("checkpoint.written"); got != 2 {
		t.Fatalf("%d checkpoints counted, want 2", got)
	}
	if got := rec.Counter("checkpoint.bytes_written"); got != bytes {
		t.Errorf("%d checkpoint bytes counted, the database holds %d", got, bytes)
	}

	// And by the policy, on every write.
	rec2 := newMemRecorder()
	q := newPair(t, Options{Recorder: rec2, Checkpoints: policy(stress(0))})
	for i := range 8 {
		q.write(edgeRecord(uint64(i+1), "p", sec(10+i), lifecycle.Observe, 0))
	}
	if rec2.Counter("checkpoint.written") == 0 {
		t.Fatal("the policy wrote no checkpoint: the test shows nothing")
	}
	if rec2.Counter("checkpoint.invalidated") != 0 {
		t.Fatal("a checkpoint was deleted: the held bytes are not what was written")
	}
	if _, bytes := heldBy(q.s); rec2.Counter("checkpoint.bytes_written") != bytes {
		t.Errorf("%d checkpoint bytes counted by the policy, the database holds %d", rec2.Counter("checkpoint.bytes_written"), bytes)
	}
	if rec2.Counter("checkpoint.bytes_written") < rec2.Counter("checkpoint.written")*int64(suffixLen) {
		t.Errorf("%d checkpoints written and %d bytes: fewer than the keys alone", rec2.Counter("checkpoint.written"), rec2.Counter("checkpoint.bytes_written"))
	}
}
