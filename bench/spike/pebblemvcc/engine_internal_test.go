package pebblemvcc

import (
	"errors"
	"fmt"
	"math"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/cockroachdb/pebble/v2"
	"github.com/cockroachdb/pebble/v2/vfs"

	"github.com/lotannauo/toposhift/bench/spike/conformance"
	"github.com/lotannauo/toposhift/bench/spike/engine"
	"github.com/lotannauo/toposhift/bench/spike/oracle"
	"github.com/lotannauo/toposhift/bench/spike/pebblekv"
	"github.com/lotannauo/toposhift/bench/spike/workload"
	"github.com/lotannauo/toposhift/internal/catalog"
	"github.com/lotannauo/toposhift/internal/lifecycle"
)

func openMem(t *testing.T, opts Options) *Engine {
	t.Helper()
	if opts.Schema == 0 {
		opts.Schema = pebblekv.SchemaCRDB
	}
	opts.Tuning = pebblekv.TinyTuning()
	opts.FS = vfs.NewMem()
	e, err := Open("db", opts)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = e.Close() })
	return e
}

// stored is one version as it sits in the database.
type stored struct {
	dir      byte
	producer string
	ns       int64
	ordinal  uint32
	seq      uint64
	kind     lifecycle.Kind
}

func (s stored) String() string {
	return fmt.Sprintf("dir %d %s @%d #%d seq %d %s", s.dir, s.producer, s.ns, s.ordinal, s.seq, s.kind)
}

// dump lists every data version in key order, undecorated.
func dump(t *testing.T, e *Engine) []stored {
	t.Helper()
	lo, hi := dataBounds()
	it, err := e.kv.NewIter(&pebble.IterOptions{LowerBound: lo, UpperBound: hi})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = it.Close() }()
	var out []stored
	for ok := it.First(); ok; ok = it.Next() {
		roach, wall, logical, err := split(it.Key())
		if err != nil {
			t.Fatal(err)
		}
		v, err := pebblekv.DecodeValue(it.Value())
		if err != nil {
			t.Fatal(err)
		}
		producer := string(roach[prefixLen:])
		if roach[prefixLen-1] != dirEntity {
			producer = string(roach[edgeKeyLen:])
		}
		out = append(out, stored{roach[prefixLen-1], producer, nanosOf(wall), logical, v.Seq, v.Kind})
	}
	if err := it.Error(); err != nil {
		t.Fatal(err)
	}
	return out
}

var (
	t0 = time.Unix(1_700_000_000, 0).UTC()
	// the edge used throughout: pod scheduled on node.
	podFP, nodeFP = fingerprintOf(catalog.K8sPod, 0), fingerprintOf(catalog.K8sNode, 0x20)
)

func edgeRecord(seq uint64, producer lifecycle.Producer, at time.Time, kind lifecycle.Kind, ttl time.Duration) engine.Record {
	r := engine.Record{
		Layer: catalog.L2, Subject: engine.EdgeSubject(podFP, nodeFP, catalog.ScheduledOn), Producer: producer,
		EventTime: at, Seq: seq, Kind: kind, TTL: ttl,
	}
	if kind == lifecycle.Observe {
		r.Payload = []byte(fmt.Sprintf("payload-%d", seq))
	}
	return r
}

// Nothing is overwritten: records at one instant keep every version, numbered in
// the order they were written, in the same batch or another, and the numbering
// carries on after the engine is closed and reopened and the data is in tables.
func TestVersionsAtOneInstantAreAllKept(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	open := func() *Engine {
		e, err := Open(dir, Options{Config: pebblekv.Config{Schema: pebblekv.SchemaCRDB, Tuning: pebblekv.TinyTuning()}})
		if err != nil {
			t.Fatal(err)
		}
		return e
	}
	e := open()
	if err := e.Write([]engine.Record{
		edgeRecord(1, "kubelet", t0, lifecycle.Observe, time.Minute),
		edgeRecord(2, "kubelet", t0, lifecycle.Delete, 0), // the same instant, in the same batch
	}); err != nil {
		t.Fatal(err)
	}
	if err := e.Settle(); err != nil { // now in a table
		t.Fatal(err)
	}
	if err := e.Close(); err != nil {
		t.Fatal(err)
	}
	e = open()
	defer func() { _ = e.Close() }()
	if err := e.Write([]engine.Record{
		edgeRecord(3, "kubelet", t0, lifecycle.Observe, time.Minute), // the same instant, in a later batch
		edgeRecord(4, "other", t0, lifecycle.Observe, 0),             // another producer starts its own numbering
	}); err != nil {
		t.Fatal(err)
	}
	var want []stored
	for _, dir := range []byte{byte(engine.Forward), byte(engine.Reverse)} {
		// Newest first within a key, and keys in producer order.
		want = append(want,
			stored{dir, "kubelet", t0.UnixNano(), 3, 3, lifecycle.Observe},
			stored{dir, "kubelet", t0.UnixNano(), 2, 2, lifecycle.Delete},
			stored{dir, "kubelet", t0.UnixNano(), 1, 1, lifecycle.Observe},
			stored{dir, "other", t0.UnixNano(), 1, 4, lifecycle.Observe},
		)
	}
	// Each direction lives under its own entity's prefix, so the dump lists the
	// pod's forward keys and the node's reverse keys; the two entities sort
	// by type id, and the pod is 7 and the node 6.
	slices.SortStableFunc(want, func(a, b stored) int { return -int(a.dir) + int(b.dir) }) // reverse (node) first
	got := dump(t, e)
	if !slices.Equal(got, want) {
		t.Fatalf("stored versions:\n got %v\nwant %v", got, want)
	}
	// The window sees all of them, and a read pinned between them sees fewer.
	recs, err := e.Window(podFP, engine.Forward, t0, t0.Add(time.Nanosecond), engine.Current(catalog.L2))
	if err != nil || len(recs) != 4 {
		t.Fatalf("Window returned %d records, %v; want 4", len(recs), err)
	}
	recs, err = e.Window(podFP, engine.Forward, t0, t0.Add(time.Nanosecond), engine.Scope{Layer: catalog.L2, AsOf: 2})
	if err != nil || len(recs) != 2 {
		t.Fatalf("Window pinned at seq 2 returned %d records, %v; want 2", len(recs), err)
	}
}

// A heartbeat run is extended at its own start instant over and over, with a
// sequence number that can be anything: the ordinal, not the sequence number,
// orders the versions, so sequence numbers that differ only above bit 32 or
// that wrap in the low 32 bits cannot misorder them.
func TestSequenceNumbersBeyondTheLogicalFieldDoNotMisorder(t *testing.T) {
	t.Parallel()
	e := openMem(t, Options{})
	seqs := []uint64{1<<32 - 1, 1 << 32, 1<<32 + 1, 3 << 32, 3<<32 + 5, 1<<63 - 1}
	for _, seq := range seqs {
		r := edgeRecord(seq, "kubelet", t0, lifecycle.Observe, time.Minute)
		r.Through = t0.Add(time.Duration(seq%1000) * time.Second)
		if err := e.Write([]engine.Record{r}); err != nil {
			t.Fatal(err)
		}
	}
	var got []uint64
	for _, s := range dump(t, e) {
		if s.dir == byte(engine.Forward) {
			got = append(got, s.seq)
		}
	}
	slices.Reverse(got) // oldest first
	if !slices.Equal(got, seqs) {
		t.Fatalf("versions in write order = %v, want %v", got, seqs)
	}
	// And a read pinned at each sequence number sees exactly the versions up to it.
	for i, seq := range seqs {
		recs, err := e.Window(podFP, engine.Forward, t0, t0.Add(time.Nanosecond), engine.Scope{Layer: catalog.L2, AsOf: seq})
		if err != nil || len(recs) != i+1 {
			t.Fatalf("pinned at %d: %d records, %v; want %d", seq, len(recs), err, i+1)
		}
	}
}

func TestRetentionKeepsExactlyWhatLaterAnswersNeed(t *testing.T) {
	t.Parallel()
	h := t0.Add(10 * time.Minute)
	min := func(n int) time.Time { return t0.Add(time.Duration(n) * time.Minute) }

	for name, tc := range map[string]struct {
		before []engine.Record
		want   []int64 // event times, in minutes from t0, left under the forward prefix, newest first
	}{
		"an open observation before the horizon stays as the baseline": {
			[]engine.Record{edgeRecord(1, "p", min(1), lifecycle.Observe, 0)},
			[]int64{1},
		},
		"an observation whose deadline is after the horizon stays": {
			[]engine.Record{edgeRecord(1, "p", min(5), lifecycle.Observe, 30*time.Minute)},
			[]int64{5},
		},
		"one whose deadline has passed goes": {
			[]engine.Record{edgeRecord(1, "p", min(5), lifecycle.Observe, 2*time.Minute)},
			nil,
		},
		"a deadline exactly at the horizon has passed": {
			[]engine.Record{edgeRecord(1, "p", min(5), lifecycle.Observe, 5*time.Minute)},
			nil,
		},
		"a delete before the horizon goes with everything under it": {
			[]engine.Record{
				edgeRecord(1, "p", min(1), lifecycle.Observe, 0),
				edgeRecord(2, "p", min(3), lifecycle.Delete, 0),
			},
			nil,
		},
		"only the newest before the horizon is the baseline": {
			[]engine.Record{
				edgeRecord(1, "p", min(1), lifecycle.Observe, 0),
				edgeRecord(2, "p", min(3), lifecycle.Observe, 0),
				edgeRecord(3, "p", min(4), lifecycle.Observe, 0),
			},
			[]int64{4},
		},
		"versions at the same instant: the newest decides, the rest go": {
			[]engine.Record{
				edgeRecord(1, "p", min(4), lifecycle.Observe, 0),
				edgeRecord(2, "p", min(4), lifecycle.Delete, 0),
				edgeRecord(3, "p", min(4), lifecycle.Observe, 0),
			},
			[]int64{4},
		},
		"the newest at an instant being a delete drops the baseline": {
			[]engine.Record{
				edgeRecord(1, "p", min(4), lifecycle.Observe, 0),
				edgeRecord(2, "p", min(4), lifecycle.Delete, 0),
			},
			nil,
		},
		"everything at or after the horizon stays, the horizon instant included": {
			[]engine.Record{
				edgeRecord(1, "p", min(1), lifecycle.Observe, time.Minute),
				edgeRecord(2, "p", h, lifecycle.Delete, 0),
				edgeRecord(3, "p", min(12), lifecycle.Observe, 0),
				edgeRecord(4, "p", min(12), lifecycle.Observe, 0),
			},
			[]int64{12, 12, 10},
		},
		"a baseline is kept under a key that has newer versions": {
			[]engine.Record{
				edgeRecord(1, "p", min(2), lifecycle.Observe, 0),
				edgeRecord(2, "p", min(15), lifecycle.Delete, 0),
			},
			[]int64{15, 2},
		},
	} {
		for _, filter := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s, filter %v", name, filter), func(t *testing.T) {
				t.Parallel()
				e := openMem(t, Options{Config: pebblekv.Config{TimeFilter: filter}})
				for _, r := range tc.before {
					if err := e.Write([]engine.Record{r}); err != nil {
						t.Fatal(err)
					}
				}
				if err := e.Settle(); err != nil { // retention over tables, not only the memtable
					t.Fatal(err)
				}
				if err := e.Retain(h); err != nil {
					t.Fatal(err)
				}
				var got []int64
				for _, s := range dump(t, e) {
					if s.dir == byte(engine.Forward) {
						got = append(got, int64(time.Unix(0, s.ns).Sub(t0)/time.Minute))
					}
				}
				if !slices.Equal(got, tc.want) {
					t.Fatalf("minutes left under the forward prefix = %v, want %v", got, tc.want)
				}
				// The reverse side is retained by the same rule.
				var rev int
				for _, s := range dump(t, e) {
					if s.dir == byte(engine.Reverse) {
						rev++
					}
				}
				if rev != len(tc.want) {
					t.Errorf("%d versions left under the reverse prefix, want %d", rev, len(tc.want))
				}
			})
		}
	}
}

func TestRetentionBeyondTheRepresentableRange(t *testing.T) {
	t.Parallel()
	e := openMem(t, Options{})
	if err := e.Write([]engine.Record{
		edgeRecord(1, "open", engine.MinEventTime, lifecycle.Observe, 0),
		edgeRecord(2, "timed", engine.MinEventTime, lifecycle.Observe, time.Hour),
	}); err != nil {
		t.Fatal(err)
	}
	// After every instant a store can hold, only what never expires is alive.
	if err := e.Retain(engine.MaxEventTime.Add(time.Hour)); err != nil {
		t.Fatal(err)
	}
	got := dump(t, e)
	for _, s := range got {
		if s.producer != "open" {
			t.Errorf("a version of %q survived a retention past the end of time", s.producer)
		}
	}
	if len(got) != 2 { // forward and reverse of the open one
		t.Errorf("%d versions left, want 2: %v", len(got), got)
	}
	// Nothing can be written now, and a horizon before 1970 changes nothing.
	if err := e.Write([]engine.Record{edgeRecord(3, "p", engine.MaxEventTime, lifecycle.Observe, 0)}); !errors.Is(err, engine.ErrBeforeHorizon) {
		t.Errorf("a write after a horizon past the end of time = %v, want ErrBeforeHorizon", err)
	}
	if err := e.Retain(time.Time{}); err != nil {
		t.Error(err)
	}
}

func TestOpenRefusesAnotherFormat(t *testing.T) {
	t.Parallel()
	fs := vfs.NewMem()
	cfg := pebblekv.Config{Schema: pebblekv.SchemaCRDB, Tuning: pebblekv.TinyTuning(), FS: fs}
	e, err := Open("db", Options{Config: cfg})
	if err != nil {
		t.Fatal(err)
	}
	if err := e.kv.Set(metaKey(pebblekv.MetaFormat), []byte("somebody-else/9"), pebble.Sync); err != nil {
		t.Fatal(err)
	}
	if err := e.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := Open("db", Options{Config: cfg}); err == nil {
		t.Fatal("a database in another format opened")
	}
}

func TestRecorderSeesWritesReadsAndRetention(t *testing.T) {
	t.Parallel()
	rec := &engine.MemRecorder{}
	e := openMem(t, Options{Recorder: rec})
	for i, r := range []engine.Record{
		edgeRecord(1, "p", t0, lifecycle.Observe, 0),
		edgeRecord(2, "p", t0.Add(time.Minute), lifecycle.Observe, 0),
		edgeRecord(3, "p", t0.Add(2*time.Minute), lifecycle.Observe, 0),
	} {
		if err := e.Write([]engine.Record{r}); err != nil {
			t.Fatal(i, err)
		}
	}
	if got := rec.Counter("write.versions"); got != 6 {
		t.Errorf("write.versions = %d, want 6 (three records, two directions)", got)
	}
	// A read pinned before every record steps over each version it cannot see.
	if _, err := e.Neighbors(podFP, engine.Forward, t0.Add(time.Hour), engine.Scope{Layer: catalog.L2, AsOf: 0}); err != nil {
		t.Fatal(err)
	}
	if got := rec.Counter("read.versions_stepped"); got != 3 {
		t.Errorf("read.versions_stepped = %d, want 3", got)
	}
	if err := e.Retain(t0.Add(90 * time.Second)); err != nil {
		t.Fatal(err)
	}
	if k, b, d := rec.Counter("retain.keys_visited"), rec.Counter("retain.baselines_kept"), rec.Counter("retain.range_deletes"); k != 2 || b != 2 || d != 2 {
		t.Errorf("retain counters = visited %d, baselines %d, deletes %d; want 2, 2, 2", k, b, d)
	}
}

func TestReadsOfStrangersAreEmptyNotErrors(t *testing.T) {
	t.Parallel()
	e := openMem(t, Options{})
	stranger := testFingerprint(t, "nonesuch", 0)
	sc := engine.Current(catalog.L2)
	if ns, err := e.Neighbors(stranger, engine.Forward, t0, sc); err != nil || len(ns) != 0 {
		t.Errorf("Neighbors = %v, %v", ns, err)
	}
	if ok, err := e.Alive(stranger, t0, sc); err != nil || ok {
		t.Errorf("Alive = %v, %v", ok, err)
	}
	if rs, err := e.Window(stranger, engine.Reverse, t0, t0.Add(time.Hour), sc); err != nil || len(rs) != 0 {
		t.Errorf("Window = %v, %v", rs, err)
	}
	if _, err := e.Neighbors(podFP, 0, t0, sc); !errors.Is(err, engine.ErrInvalid) {
		t.Errorf("a direction of 0 = %v, want ErrInvalid", err)
	}
	if _, err := e.Window(podFP, 3, t0, t0.Add(time.Hour), sc); !errors.Is(err, engine.ErrInvalid) {
		t.Errorf("a direction of 3 = %v, want ErrInvalid", err)
	}
}

func TestSettleAndSize(t *testing.T) {
	t.Parallel()
	e := openMem(t, Options{})
	var _ engine.Settler = e
	if err := e.Write([]engine.Record{edgeRecord(1, "p", t0, lifecycle.Observe, 0)}); err != nil {
		t.Fatal(err)
	}
	if err := e.Settle(); err != nil {
		t.Fatal(err)
	}
	if n, err := e.Size(); err != nil || n <= 0 {
		t.Errorf("Size = %d, %v", n, err)
	}
	if e.LastSeq() != 1 {
		t.Errorf("LastSeq = %d", e.LastSeq())
	}
}

// A retention that commits in many pieces leaves every answer as it was, at every
// piece: readers run while it works, so a half-finished retention must already be
// right for the instants and tokens it promises.
func TestRetentionInManyPiecesKeepsAnswers(t *testing.T) {
	conformance.SkipWhenTrimmed(t)
	t.Parallel()
	whole := openMem(t, Options{})
	pieces := openMem(t, Options{retainBatchBytes: 1}) // every key is its own commit
	oracle := oracleFor(t)
	g, err := workload.New(workload.Tiny())
	if err != nil {
		t.Fatal(err)
	}
	// Write until the history spans a few minutes (the workload starts with a
	// burst at one instant), then retain at its middle.
	var first, last time.Time
	for range 400 {
		batch := g.Batch(40)
		if first.IsZero() {
			first = batch[0].EventTime
		}
		last = batch[len(batch)-1].EventTime
		for _, e := range []engine.Engine{whole, pieces, oracle} {
			if err := e.Write(cloneAll(batch)); err != nil {
				t.Fatal(err)
			}
		}
		if last.Sub(first) > 3*time.Minute {
			break
		}
	}
	if last.Sub(first) <= 3*time.Minute {
		t.Fatalf("the workload spans only %s", last.Sub(first))
	}
	h := first.Add(last.Sub(first) / 2)
	rec := &engine.MemRecorder{}
	pieces.rec = rec
	for _, e := range []engine.Engine{whole, pieces, oracle} {
		if err := e.Retain(h); err != nil {
			t.Fatal(err)
		}
	}
	if rec.Counter("retain.range_deletes") < 2 {
		t.Fatalf("the retention deleted %d ranges; the test needs several keys", rec.Counter("retain.range_deletes"))
	}
	if !slices.Equal(dump(t, whole), dump(t, pieces)) {
		t.Fatal("a retention in many pieces left different data from one in a single commit")
	}
	// And the answers are the oracle's, for the instants at and after the horizon.
	for _, e := range g.Entities() {
		for _, off := range []time.Duration{0, time.Minute, 5 * time.Minute, time.Hour} {
			for _, layer := range []catalog.Layer{catalog.L0, catalog.L1, catalog.L2, catalog.L3} {
				for _, dir := range []engine.Direction{engine.Forward, engine.Reverse} {
					want, err := oracle.Neighbors(e, dir, h.Add(off), engine.Current(layer))
					if err != nil {
						t.Fatal(err)
					}
					got, err := pieces.Neighbors(e, dir, h.Add(off), engine.Current(layer))
					if err != nil || !slices.Equal(got, want) {
						t.Fatalf("Neighbors(%s, %s, h+%s, %s) = %v, %v; oracle says %v", e, dir, off, layer, got, err, want)
					}
				}
			}
		}
	}
}

// Producer names are arbitrary non-empty strings, and the producer is the last
// part of a roach key: one name that is a prefix of another, or holds the zero
// byte that cockroachkvs uses as its sentinel, must still be its own key.
func TestAwkwardProducerNames(t *testing.T) {
	t.Parallel()
	names := []lifecycle.Producer{"a", "a\x00", "a\x00b", "a\x00\x00", "\xff", "\xff\xff", "ab", lifecycle.Producer(strings.Repeat("x", 300))}
	e := openMem(t, Options{})
	oracle := oracleFor(t)
	var seq uint64
	var batch []engine.Record
	for i, name := range names {
		seq++
		r := edgeRecord(seq, name, t0.Add(time.Duration(i)*time.Second), lifecycle.Observe, time.Hour)
		batch = append(batch, r)
		seq++
		batch = append(batch, edgeRecord(seq, name, t0.Add(time.Duration(i)*time.Second+time.Millisecond), lifecycle.Delete, 0))
	}
	for _, x := range []engine.Engine{e, oracle} {
		if err := x.Write(cloneAll(batch)); err != nil {
			t.Fatal(err)
		}
	}
	if err := e.Settle(); err != nil {
		t.Fatal(err)
	}
	sc := engine.Current(catalog.L2)
	for off := -time.Second; off < 10*time.Second; off += 250 * time.Millisecond {
		at := t0.Add(off)
		want, err := oracle.Neighbors(podFP, engine.Forward, at, sc)
		if err != nil {
			t.Fatal(err)
		}
		got, err := e.Neighbors(podFP, engine.Forward, at, sc)
		if err != nil || !slices.Equal(got, want) {
			t.Fatalf("Neighbors at +%s = %v, %v; oracle says %v", off, got, err, want)
		}
	}
	want, _ := oracle.Window(podFP, engine.Forward, t0.Add(-time.Hour), t0.Add(time.Hour), sc)
	got, err := e.Window(podFP, engine.Forward, t0.Add(-time.Hour), t0.Add(time.Hour), sc)
	if err != nil || len(got) != len(names)*2 || !slices.EqualFunc(got, want, func(a, b engine.Record) bool {
		return a.Producer == b.Producer && a.Seq == b.Seq && a.EventTime.Equal(b.EventTime) && a.Kind == b.Kind
	}) {
		t.Fatalf("Window returned %d records, %v; want %d matching the oracle", len(got), err, len(names)*2)
	}
}

func oracleFor(t *testing.T) *oracle.Oracle {
	t.Helper()
	return oracle.New()
}

func cloneAll(rs []engine.Record) []engine.Record {
	out := slices.Clone(rs)
	for i := range out {
		out[i].Payload = slices.Clone(out[i].Payload)
	}
	return out
}

// Readers running while a retention commits piece by piece still see only
// states the oracle was in: every piece leaves the answers it promises.
func TestConcurrentReadsDuringPiecewiseRetention(t *testing.T) {
	t.Parallel()
	e := openMem(t, Options{retainBatchBytes: 1})
	if err := conformance.CheckConcurrentReads(e); err != nil {
		t.Fatal(err)
	}
}

// The ordinal is 32 bits. At its largest the next version cannot be numbered,
// and wrapping would give it ordinal 0, which sorts as the oldest at its instant:
// the write is refused instead, and nothing of the batch is stored.
func TestOrdinalOverflowRefusesTheWrite(t *testing.T) {
	t.Parallel()
	e := openMem(t, Options{})
	r := edgeRecord(1, "p", t0, lifecycle.Observe, 0)
	roaches, err := e.roachKeys(r)
	if err != nil {
		t.Fatal(err)
	}
	value := pebblekv.FromRecord(r).Append(nil)
	for _, roach := range roaches { // as if 2^32-1 versions were already there
		if err := e.kv.Set(versionKey(roach, wallOf(t0.UnixNano()), math.MaxUint32), value, nil); err != nil {
			t.Fatal(err)
		}
	}
	before := len(dump(t, e))
	next := edgeRecord(2, "p", t0, lifecycle.Delete, 0)
	other := edgeRecord(3, "q", t0, lifecycle.Observe, 0)
	if err := e.Write([]engine.Record{next, other}); !errors.Is(err, engine.ErrInvalid) {
		t.Fatalf("Write = %v, want ErrInvalid", err)
	}
	if got := len(dump(t, e)); got != before || e.LastSeq() != 0 {
		t.Fatalf("a refused batch left %d versions (had %d) and LastSeq %d", got, before, e.LastSeq())
	}
}
