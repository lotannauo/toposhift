package pebblestore

import (
	"context"
	"errors"
	"fmt"
	"math"
	"slices"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cockroachdb/pebble/v2"
	"github.com/cockroachdb/pebble/v2/vfs"

	"github.com/lotannauo/toposhift/internal/catalog"
	"github.com/lotannauo/toposhift/internal/identity"
	"github.com/lotannauo/toposhift/internal/lifecycle"
	"github.com/lotannauo/toposhift/internal/store"
	"github.com/lotannauo/toposhift/internal/store/memstore"
	"github.com/lotannauo/toposhift/internal/store/pebblekv"
	"github.com/lotannauo/toposhift/internal/store/storetest"
)

var bg = context.Background()

// memRecorder keeps the counts it is given.
type memRecorder struct {
	counters map[string]int64
	samples  map[string][]int64
}

func newMemRecorder() *memRecorder {
	return &memRecorder{counters: map[string]int64{}, samples: map[string][]int64{}}
}

func (r *memRecorder) Count(name string, n int64)  { r.counters[name] += n }
func (r *memRecorder) Sample(name string, v int64) { r.samples[name] = append(r.samples[name], v) }

func openMem(t *testing.T, opts Options) *Store {
	t.Helper()
	opts.Tuning = pebblekv.TinyTuning()
	opts.FS = vfs.NewMem()
	s, err := Open("db", opts)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.Close() })
	return s
}

// stored is one key as it sits in the database; a baseline lists its entries as
// producer@eventNs.
type stored struct {
	dir     byte
	kind    byte
	ns      int64
	seq     uint64
	entries string
}

func (s stored) String() string {
	return fmt.Sprintf("dir %d kind %d @%d seq %d [%s]", s.dir, s.kind, s.ns, s.seq, s.entries)
}

// dump lists every data key in key order.
func dump(t *testing.T, s *Store) []stored {
	t.Helper()
	lo, hi := dataBounds()
	it, err := s.kv.NewIter(&pebble.IterOptions{LowerBound: lo, UpperBound: hi})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = it.Close() }()
	var out []stored
	for ok := it.First(); ok; ok = it.Next() {
		prefix, ns, seq, kind, err := parseKey(it.Key())
		if err != nil {
			t.Fatal(err)
		}
		st := stored{dir: prefix[prefixLen-1], kind: kind, ns: ns, seq: seq}
		switch kind {
		case kindRecord:
			ref, _, err := decodeRecordValue(st.dir, it.Value())
			if err != nil {
				t.Fatal(err)
			}
			st.entries = string(producerOf(st.dir, ref))
		default:
			sp, err := decodeStamp(it.Value())
			if err != nil {
				t.Fatal(err)
			}
			var names []string
			for _, en := range sp.entries {
				names = append(names, fmt.Sprintf("%s@%d", producerOf(st.dir, en.ref), en.eventNs))
			}
			st.entries = strings.Join(names, ",")
		}
		out = append(out, st)
	}
	if err := it.Error(); err != nil {
		t.Fatal(err)
	}
	return out
}

// forward keeps the keys under the pod's forward prefix, which is where the
// tests look.
func forward(all []stored) []stored {
	return slices.DeleteFunc(slices.Clone(all), func(s stored) bool { return s.dir != byte(store.Forward) })
}

var (
	t0            = time.Unix(1_700_000_000, 0).UTC()
	podFP, nodeFP = fingerprintOf(catalog.K8sPod, 0), fingerprintOf(catalog.K8sNode, 0x20)
)

func edgeRecord(seq uint64, producer lifecycle.Producer, at time.Time, kind lifecycle.Kind, ttl time.Duration) store.Record {
	r := store.Record{
		Layer: catalog.L2, Subject: store.EdgeSubject(podFP, nodeFP, catalog.ScheduledOn), Producer: producer,
		EventTime: at, Seq: seq, Kind: kind, TTL: ttl,
	}
	if kind == lifecycle.Observe {
		r.Payload = fmt.Appendf(nil, "payload-%d", seq)
	}
	return r
}

func cloneAll(rs []store.Record) []store.Record {
	out := slices.Clone(rs)
	for i := range out {
		out[i].Payload = slices.Clone(out[i].Payload)
	}
	return out
}

// pair is the store under test and the reference (memstore), written and retained
// alike.
type pair struct {
	t   *testing.T
	s   *Store
	ref *memstore.Store
}

func newPair(t *testing.T, opts Options) *pair {
	t.Helper()
	ref, err := memstore.Open(memstore.Options{Policy: opts.Policy})
	if err != nil {
		t.Fatal(err)
	}
	return &pair{t: t, s: openMem(t, opts), ref: ref}
}

func (p *pair) write(recs ...store.Record) {
	p.t.Helper()
	for _, x := range []store.Store{p.s, p.ref} {
		if err := x.Write(bg, cloneAll(recs)); err != nil {
			p.t.Fatal(err)
		}
	}
}

func (p *pair) retain(h time.Time) {
	p.t.Helper()
	for _, x := range []store.Store{p.s, p.ref} {
		if err := x.Retain(bg, h); err != nil {
			p.t.Fatal(err)
		}
	}
}

// same asks both stores the same questions at every instant and token, in both
// directions and for the entity, and fails on the first difference.
func (p *pair) same(entities []identity.Fingerprint, times []time.Time, tokens []uint64) {
	p.t.Helper()
	for _, fp := range entities {
		for _, at := range times {
			for _, tok := range tokens {
				sc := store.Scope{Layer: catalog.L2, AsOf: tok}
				for _, dir := range []store.Direction{store.Forward, store.Reverse} {
					want, err := p.ref.Neighbors(bg, fp, dir, at, sc)
					if err != nil {
						p.t.Fatal(err)
					}
					got, err := p.s.Neighbors(bg, fp, dir, at, sc)
					if err != nil || !slices.Equal(got, want) {
						p.t.Fatalf("Neighbors(%s, %s, %s, asOf %d) = %v, %v; the reference says %v", fp, dir, at.Sub(t0), tok, got, err, want)
					}
				}
				wantAlive, _ := p.ref.Alive(bg, fp, at, sc)
				if gotAlive, err := p.s.Alive(bg, fp, at, sc); err != nil || gotAlive != wantAlive {
					p.t.Fatalf("Alive(%s, %s, asOf %d) = %v, %v; the reference says %v", fp, at.Sub(t0), tok, gotAlive, err, wantAlive)
				}
			}
		}
	}
}

// Nothing is overwritten: records at one instant keep every version, told apart
// by their Seq alone, in the same batch or another, and they come back in the
// order the key promises, newest first and highest Seq first, after the store is
// closed and reopened and the data is in tables.
func TestRecordsAtOneInstantAreAllKept(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	open := func() *Store {
		s, err := Open(dir, Options{Config: pebblekv.Config{Tuning: pebblekv.TinyTuning()}})
		if err != nil {
			t.Fatal(err)
		}
		return s
	}
	s := open()
	if err := s.Write(bg, []store.Record{
		edgeRecord(1, "kubelet", t0, lifecycle.Observe, time.Minute),
		edgeRecord(2, "kubelet", t0, lifecycle.Delete, 0),
	}); err != nil {
		t.Fatal(err)
	}
	if err := s.kv.Settle(); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	s = open()
	defer func() { _ = s.Close() }()
	if err := s.Write(bg, []store.Record{
		edgeRecord(3, "kubelet", t0, lifecycle.Observe, time.Minute),
		edgeRecord(4, "other", t0, lifecycle.Observe, 0),
		edgeRecord(5, "kubelet", t0.Add(time.Second), lifecycle.Delete, 0),
	}); err != nil {
		t.Fatal(err)
	}
	ns := t0.UnixNano()
	want := []stored{
		{1, kindRecord, ns + 1e9, 5, "kubelet"},
		{1, kindRecord, ns, 4, "other"},
		{1, kindRecord, ns, 3, "kubelet"},
		{1, kindRecord, ns, 2, "kubelet"},
		{1, kindRecord, ns, 1, "kubelet"},
	}
	if got := forward(dump(t, s)); !slices.Equal(got, want) {
		t.Fatalf("stored under the forward prefix:\n got %v\nwant %v", got, want)
	}
	recs, err := s.Window(bg, podFP, store.Forward, t0, t0.Add(time.Nanosecond), store.Current(catalog.L2))
	if err != nil || len(recs) != 4 {
		t.Fatalf("Window returned %d records, %v; want 4", len(recs), err)
	}
	recs, err = s.Window(bg, podFP, store.Forward, t0, t0.Add(time.Nanosecond), store.Scope{Layer: catalog.L2, AsOf: 2})
	if err != nil || len(recs) != 2 {
		t.Fatalf("Window pinned at seq 2 returned %d records, %v; want 2", len(recs), err)
	}
}

// Sequence numbers are 64 bits in the key and the value; none of them is cut to
// 32, and the highest one a token can name still sorts as the newest.
func TestSequenceNumbersAreFullWidth(t *testing.T) {
	t.Parallel()
	p := newPair(t, Options{})
	seqs := []uint64{1<<32 - 1, 1 << 32, 1<<32 + 1, 3 << 32, 3<<32 + 5, 1<<63 - 1, 1 << 63, math.MaxUint64 - 1}
	for i, seq := range seqs {
		kind, ttl := lifecycle.Observe, time.Hour
		if i%3 == 2 {
			kind, ttl = lifecycle.Delete, 0
		}
		p.write(edgeRecord(seq, "kubelet", t0, kind, ttl))
	}
	var got []uint64
	for _, s := range forward(dump(t, p.s)) {
		got = append(got, s.seq)
	}
	slices.Reverse(got) // newest first in the key, so oldest first here
	if !slices.Equal(got, seqs) {
		t.Fatalf("stored seqs = %v, want %v", got, seqs)
	}
	toks := append([]uint64{0}, seqs...)
	toks = append(toks, math.MaxUint64)
	p.same([]identity.Fingerprint{podFP, nodeFP}, []time.Time{t0.Add(-time.Nanosecond), t0, t0.Add(time.Minute)}, toks)
}

// The key and the value both carry the Seq; a record whose two disagree is
// corruption, and a read says so.
func TestAKeyAndValueThatDisagreeAreRefused(t *testing.T) {
	t.Parallel()
	s := openMem(t, Options{})
	r := edgeRecord(7, "p", t0, lifecycle.Observe, 0)
	sides, err := s.sidesOf(r)
	if err != nil {
		t.Fatal(err)
	}
	bad := pebblekv.FromRecord(r)
	bad.Seq = 8
	if err := s.kv.Set(recordKey(sides[0].prefix, t0.UnixNano(), 7), appendRecordValue(nil, sides[0].ref, bad), nil); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Window(bg, podFP, store.Forward, t0, t0.Add(time.Second), store.Current(catalog.L2)); err == nil {
		t.Fatal("a record whose key and value disagree about its Seq was returned")
	}
}

// Producer names are arbitrary non-empty strings, and the producer is the last
// part of a reference: one that is a prefix of another, or holds any byte, must
// still be its own reference.
func TestAwkwardProducerNames(t *testing.T) {
	t.Parallel()
	names := []lifecycle.Producer{"a", "a\x00", "a\x00b", "a\x00\x00", "\xff", "\xff\xff", "ab", lifecycle.Producer(strings.Repeat("x", 300))}
	p := newPair(t, Options{})
	var seq uint64
	var batch []store.Record
	for i, name := range names {
		seq++
		batch = append(batch, edgeRecord(seq, name, t0.Add(time.Duration(i)*time.Second), lifecycle.Observe, time.Hour))
		seq++
		batch = append(batch, edgeRecord(seq, name, t0.Add(time.Duration(i)*time.Second+time.Millisecond), lifecycle.Delete, 0))
	}
	p.write(batch...)
	if err := p.s.kv.Settle(); err != nil {
		t.Fatal(err)
	}
	var times []time.Time
	for off := -time.Second; off < 10*time.Second; off += 250 * time.Millisecond {
		times = append(times, t0.Add(off))
	}
	p.same([]identity.Fingerprint{podFP, nodeFP}, times, []uint64{seq / 2, seq, store.Latest})
	want, _ := p.ref.Window(bg, podFP, store.Forward, t0.Add(-time.Hour), t0.Add(time.Hour), store.Current(catalog.L2))
	got, err := p.s.Window(bg, podFP, store.Forward, t0.Add(-time.Hour), t0.Add(time.Hour), store.Current(catalog.L2))
	if err != nil || len(got) != len(names)*2 || !slices.EqualFunc(got, want, func(a, b store.Record) bool {
		return a.Producer == b.Producer && a.Seq == b.Seq && a.EventTime.Equal(b.EventTime) && a.Kind == b.Kind
	}) {
		t.Fatalf("Window returned %d records, %v; want %d matching the reference", len(got), err, len(names)*2)
	}
	// Every one of them has ended, so a retention leaves nothing to rewrite.
	p.retain(t0.Add(30 * time.Second))
	if got := dump(t, p.s); len(got) != 0 {
		t.Errorf("a retention after every reference ended left %v", got)
	}
	p.same([]identity.Fingerprint{podFP, nodeFP}, []time.Time{t0.Add(30 * time.Second), t0.Add(time.Minute)}, []uint64{seq, store.Latest})
}

// Producers whose names differ only by trailing zero bytes hold separate
// references at overlapping times, so one's delete or expiry must not end
// another's, before and after a retention that carries them as baseline entries.
func TestProducersNamedAlikeAreSeparateReferences(t *testing.T) {
	t.Parallel()
	p := newPair(t, Options{})
	p.write(
		edgeRecord(1, "a", t0, lifecycle.Observe, 0),
		edgeRecord(2, "a\x00", t0.Add(500*time.Millisecond), lifecycle.Observe, 0),
		edgeRecord(3, "a\x00", t0.Add(time.Second), lifecycle.Delete, 0),                  // ends only its own
		edgeRecord(4, "a\x00\x00", t0.Add(2*time.Second), lifecycle.Observe, time.Second), // lapses at +3s
	)
	times := []time.Time{t0.Add(-time.Nanosecond), t0, t0.Add(time.Second), t0.Add(2 * time.Second), t0.Add(3 * time.Second), t0.Add(5 * time.Second), t0.Add(time.Hour)}
	toks := []uint64{0, 1, 2, 3, 4, store.Latest}
	p.same([]identity.Fingerprint{podFP, nodeFP}, times, toks)
	if got, err := p.s.Neighbors(bg, podFP, store.Forward, t0.Add(5*time.Second), store.Current(catalog.L2)); err != nil || len(got) != 1 {
		t.Fatalf("with only 'a' still holding, Neighbors = %v, %v; want the edge", got, err)
	}
	p.retain(t0.Add(3 * time.Second))
	forwardKeys := forward(dump(t, p.s))
	if len(forwardKeys) != 1 || forwardKeys[0].kind != kindBaseline || forwardKeys[0].entries != fmt.Sprintf("a@%d", t0.UnixNano()) {
		t.Fatalf("after the retention: %v; want one baseline holding only 'a' (the others ended or lapsed)", forwardKeys)
	}
	p.same([]identity.Fingerprint{podFP, nodeFP}, times[4:], []uint64{4, store.Latest})
}

func TestOpenRefusesAnotherFormat(t *testing.T) {
	t.Parallel()
	fs := vfs.NewMem()
	cfg := pebblekv.Config{Tuning: pebblekv.TinyTuning(), FS: fs}
	s, err := Open("db", Options{Config: cfg})
	if err != nil {
		t.Fatal(err)
	}
	if err := s.kv.Set(metaKey(pebblekv.MetaFormat), []byte("somebody-else/9"), pebble.Sync); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := Open("db", Options{Config: cfg}); !errors.Is(err, store.ErrInvalid) {
		t.Fatalf("a database in another format opened, or failed with %v; want an error wrapping ErrInvalid", err)
	}
}

// The spike's format tag differs from this one, so neither opens the other's
// database, whatever else the two hold.
func TestOpenRefusesTheSpikesFormat(t *testing.T) {
	t.Parallel()
	fs := vfs.NewMem()
	cfg := pebblekv.Config{Tuning: pebblekv.TinyTuning(), FS: fs}
	s, err := Open("db", Options{Config: cfg})
	if err != nil {
		t.Fatal(err)
	}
	if err := s.kv.Set(metaKey(pebblekv.MetaFormat), []byte("toposhift/bench/log;key=1;record=1;stamp=1"), pebble.Sync); err != nil {
		t.Fatal(err)
	}
	_ = s.Close()
	if _, err := Open("db", Options{Config: cfg}); !errors.Is(err, store.ErrInvalid) {
		t.Fatalf("a database in the spike's format opened, or failed with %v", err)
	}
}

func TestLayoutRefusesWhatItCannotUse(t *testing.T) {
	t.Parallel()
	for name, cfg := range map[string]pebblekv.Config{
		"the crdb1 schema": {Schema: pebblekv.SchemaCRDB, Tuning: pebblekv.TinyTuning(), FS: vfs.NewMem()},
		"a time filter":    {TimeFilter: true, Tuning: pebblekv.TinyTuning(), FS: vfs.NewMem()},
	} {
		if _, err := Open("db", Options{Config: cfg}); err == nil {
			t.Errorf("layout L opened with %s", name)
		}
	}
	if _, err := Open("db", Options{Config: pebblekv.Config{Tuning: pebblekv.TinyTuning(), FS: vfs.NewMem()}, Retention: 7}); !errors.Is(err, store.ErrInvalid) {
		t.Errorf("an unknown retention mode opened, or failed with %v", err)
	}
	if _, err := Open("db", Options{Config: pebblekv.Config{Tuning: pebblekv.TinyTuning(), FS: vfs.NewMem()}, Policy: lifecycle.Policy{Skew: -1}}); !errors.Is(err, store.ErrInvalid) {
		t.Errorf("a policy the lifecycle refuses opened, or failed with %v", err)
	}
}

func TestRecorderSeesWritesReadsAndRetention(t *testing.T) {
	t.Parallel()
	rec := newMemRecorder()
	s := openMem(t, Options{Recorder: rec})
	for i, r := range []store.Record{
		edgeRecord(1, "p", t0, lifecycle.Observe, 0),
		edgeRecord(2, "p", t0.Add(time.Minute), lifecycle.Observe, 0),
		edgeRecord(3, "p", t0.Add(2*time.Minute), lifecycle.Observe, 0),
	} {
		if err := s.Write(bg, []store.Record{r}); err != nil {
			t.Fatal(i, err)
		}
	}
	if got := rec.counters["write.records"]; got != 6 {
		t.Errorf("write.records = %d, want 6 (three records, two directions)", got)
	}
	// A read pinned before every record steps over each record it cannot see.
	if _, err := s.Neighbors(bg, podFP, store.Forward, t0.Add(time.Hour), store.Scope{Layer: catalog.L2, AsOf: 0}); err != nil {
		t.Fatal(err)
	}
	if got := rec.counters["read.records_stepped"]; got != 3 {
		t.Errorf("read.records_stepped = %d, want 3", got)
	}
	if err := s.Retain(bg, t0.Add(90*time.Second)); err != nil {
		t.Fatal(err)
	}
	for name, want := range map[string]int64{
		"retain.prefixes_replayed": 2, "retain.records_replayed": 4, "retain.baselines_written": 2, "retain.range_deletes": 2,
		"retain.prefixes_kept_for_boots": 0,
	} {
		if got := rec.counters[name]; got != want {
			t.Errorf("%s = %d, want %d", name, got, want)
		}
	}
}

func TestReadsOfStrangersAreEmptyNotErrors(t *testing.T) {
	t.Parallel()
	s := openMem(t, Options{})
	stranger := fingerprintOf("nonesuch", 0)
	sc := store.Current(catalog.L2)
	if ns, err := s.Neighbors(bg, stranger, store.Forward, t0, sc); err != nil || len(ns) != 0 {
		t.Errorf("Neighbors = %v, %v", ns, err)
	}
	if ok, err := s.Alive(bg, stranger, t0, sc); err != nil || ok {
		t.Errorf("Alive = %v, %v", ok, err)
	}
	if rs, err := s.Window(bg, stranger, store.Reverse, t0, t0.Add(time.Hour), sc); err != nil || len(rs) != 0 {
		t.Errorf("Window = %v, %v", rs, err)
	}
	if rs, err := s.EntityWindow(bg, stranger, t0, t0.Add(time.Hour), sc); err != nil || len(rs) != 0 {
		t.Errorf("EntityWindow = %v, %v", rs, err)
	}
	if _, err := s.Neighbors(bg, podFP, 0, t0, sc); !errors.Is(err, store.ErrInvalid) {
		t.Errorf("a direction of 0 = %v, want ErrInvalid", err)
	}
	if _, err := s.Window(bg, podFP, 3, t0, t0.Add(time.Hour), sc); !errors.Is(err, store.ErrInvalid) {
		t.Errorf("a direction of 3 = %v, want ErrInvalid", err)
	}
}

// Readers running while a retention commits prefix by prefix still see only
// states the reference was in: every commit leaves the answers it promises.
func TestConcurrentReadsDuringPiecewiseRetention(t *testing.T) {
	t.Parallel()
	s := openMem(t, Options{retainBatchBytes: 1})
	if err := storetest.CheckConcurrentReads(s); err != nil {
		t.Fatal(err)
	}
}

// The database says how it was made: the format, the Pebble that made it, and the
// boot key of its policy, which a reopening has to repeat.
func TestANewDatabaseRecordsHowItWasMade(t *testing.T) {
	t.Parallel()
	s := openMem(t, Options{Policy: lifecycle.Policy{BootKey: lifecycle.BootID}})
	for name, want := range map[string]string{
		pebblekv.MetaFormat: formatTag,
		metaBootKey:         string(lifecycle.BootID),
		metaPebble:          string(pebbleTag(linkedPebbleVersion(), s.kv.FormatMajorVersion())),
	} {
		got, err := s.kv.GetMeta(metaKey(name))
		if err != nil || string(got) != want {
			t.Errorf("meta %q = %q, %v; want %q", name, got, err, want)
		}
	}
	if v := linkedPebbleVersion(); !strings.HasPrefix(v, "v2.") {
		t.Errorf("the linked Pebble is %q", v)
	}
	// With no boot key, the key is absent.
	z := openMem(t, Options{})
	if got, err := z.kv.GetMeta(metaKey(metaBootKey)); err != nil || got != nil {
		t.Errorf("the zero policy wrote a boot key: %q, %v", got, err)
	}
}

func TestReopeningWithAnotherBootKeyIsRefused(t *testing.T) {
	t.Parallel()
	for name, tc := range map[string]struct{ created, reopened lifecycle.Policy }{
		"a boot key then none":    {lifecycle.Policy{BootKey: lifecycle.BootID}, lifecycle.Policy{}},
		"none then a boot key":    {lifecycle.Policy{}, lifecycle.Policy{BootKey: lifecycle.BootID}},
		"one boot key then other": {lifecycle.Policy{BootKey: lifecycle.BootID}, lifecycle.Policy{BootKey: "other.boot"}},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			cfg := pebblekv.Config{Tuning: pebblekv.TinyTuning(), FS: vfs.NewMem()}
			s, err := Open("db", Options{Config: cfg, Policy: tc.created})
			if err != nil {
				t.Fatal(err)
			}
			if err := s.Close(); err != nil {
				t.Fatal(err)
			}
			if _, err := Open("db", Options{Config: cfg, Policy: tc.reopened}); !errors.Is(err, store.ErrInvalid) {
				t.Fatalf("reopened with another boot key: %v, want an error wrapping ErrInvalid", err)
			}
			again, err := Open("db", Options{Config: cfg, Policy: tc.created})
			if err != nil {
				t.Fatalf("reopened with the same boot key: %v", err)
			}
			_ = again.Close()
		})
	}
}

// A Close that is repeated, and the values a closed store keeps.
func TestCloseIsIdempotentAndKeepsTheLastValues(t *testing.T) {
	t.Parallel()
	s := openMem(t, Options{})
	if err := s.Write(bg, []store.Record{edgeRecord(1, "p", t0, lifecycle.Observe, 0)}); err != nil {
		t.Fatal(err)
	}
	if err := s.Retain(bg, t0.Add(time.Minute)); err != nil {
		t.Fatal(err)
	}
	want := s.Horizon()
	for i := range 3 {
		if err := s.Close(); err != nil {
			t.Fatalf("Close #%d = %v", i+1, err)
		}
	}
	if s.LastSeq() != 1 || s.Horizon() != want {
		t.Errorf("a closed store reports seq %d and horizon %v; want 1 and %v", s.LastSeq(), s.Horizon(), want)
	}
	if err := s.Write(bg, nil); !errors.Is(err, store.ErrClosed) {
		t.Errorf("an empty Write after Close = %v, want ErrClosed", err)
	}
	if err := s.Retain(bg, t0.Add(time.Hour)); !errors.Is(err, store.ErrClosed) {
		t.Errorf("Retain after Close = %v, want ErrClosed", err)
	}
	if _, err := s.Alive(bg, podFP, t0, store.Current(catalog.L2)); !errors.Is(err, store.ErrClosed) {
		t.Errorf("Alive after Close = %v, want ErrClosed", err)
	}
}

// countingSyncFS counts the files synced through it.
type countingSyncFS struct {
	vfs.FS
	syncs atomic.Int64
}

func (f *countingSyncFS) wrap(file vfs.File, err error) (vfs.File, error) {
	if err != nil {
		return nil, err
	}
	return &countingSyncFile{File: file, fs: f}, nil
}

func (f *countingSyncFS) Create(name string, category vfs.DiskWriteCategory) (vfs.File, error) {
	return f.wrap(f.FS.Create(name, category))
}

func (f *countingSyncFS) ReuseForWrite(oldname, newname string, category vfs.DiskWriteCategory) (vfs.File, error) {
	return f.wrap(f.FS.ReuseForWrite(oldname, newname, category))
}

type countingSyncFile struct {
	vfs.File
	fs *countingSyncFS
}

func (f *countingSyncFile) Sync() error {
	f.fs.syncs.Add(1)
	return f.File.Sync()
}

func (f *countingSyncFile) SyncData() error {
	f.fs.syncs.Add(1)
	return f.File.SyncData()
}

// A directory that holds data keys and no format is somebody's database, not a new
// one, and is not claimed.
func TestADirectoryWithDataAndNoFormatIsNotClaimed(t *testing.T) {
	t.Parallel()
	fs := vfs.NewMem()
	cfg := pebblekv.Config{Tuning: pebblekv.TinyTuning(), FS: fs}
	kv, err := pebblekv.Open("db", pebblekv.BytewiseLayout, rawConfig(cfg))
	if err != nil {
		t.Fatal(err)
	}
	prefix, _ := keyer().prefixOf(catalog.L2, podFP, byte(store.Forward))
	if err := kv.Set(recordKey(prefix, 1, 1), []byte("x"), pebble.Sync); err != nil {
		t.Fatal(err)
	}
	if err := kv.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := Open("db", Options{Config: cfg}); !errors.Is(err, store.ErrInvalid) {
		t.Fatalf("Open = %v, want an error wrapping ErrInvalid", err)
	}
	// Meta keys alone (no data) do not make a directory somebody's: it is a new
	// database that has none of ours yet.
	fs2 := vfs.NewMem()
	cfg2 := pebblekv.Config{Tuning: pebblekv.TinyTuning(), FS: fs2}
	kv, err = pebblekv.Open("db", pebblekv.BytewiseLayout, rawConfig(cfg2))
	if err != nil {
		t.Fatal(err)
	}
	if err := kv.Set(metaKey("other"), []byte("x"), pebble.Sync); err != nil {
		t.Fatal(err)
	}
	_ = kv.Close()
	s, err := Open("db", Options{Config: cfg2})
	if err != nil {
		t.Fatalf("Open of a directory with a foreign meta key and no data = %v", err)
	}
	_ = s.Close()
}

// A database that may hold checkpoints is refused: this version reads them
// but cannot keep them up to date: it does not invalidate them when an older
// record arrives.
func TestADatabaseThatMayHoldCheckpointsIsRefused(t *testing.T) {
	t.Parallel()
	fs := vfs.NewMem()
	cfg := pebblekv.Config{Tuning: pebblekv.TinyTuning(), FS: fs}
	s, err := Open("db", Options{Config: cfg})
	if err != nil {
		t.Fatal(err)
	}
	if err := s.kv.Set(metaKey(metaCheckpoints), []byte{1}, pebble.Sync); err != nil {
		t.Fatal(err)
	}
	_ = s.Close()
	if _, err := Open("db", Options{Config: cfg}); !errors.Is(err, store.ErrInvalid) || !strings.Contains(err.Error(), "checkpoints") {
		t.Fatalf("Open = %v, want an error wrapping ErrInvalid that names checkpoints", err)
	}
}

// A database made by another Pebble, or at another format major version, is
// refused, and the error names both.
func TestADatabaseMadeByAnotherPebbleIsRefused(t *testing.T) {
	t.Parallel()
	fs := vfs.NewMem()
	cfg := pebblekv.Config{Tuning: pebblekv.TinyTuning(), FS: fs}
	s, err := Open("db", Options{Config: cfg})
	if err != nil {
		t.Fatal(err)
	}
	now := string(pebbleTag(linkedPebbleVersion(), s.kv.FormatMajorVersion()))
	for name, doctored := range map[string]string{
		"another version": "pebble=v2.0.0;fmv=24",
		"another format":  fmt.Sprintf("pebble=%s;fmv=99", linkedPebbleVersion()),
		"nothing":         "",
	} {
		if err := s.kv.Set(metaKey(metaPebble), []byte(doctored), pebble.Sync); err != nil {
			t.Fatal(err)
		}
		_ = s.Close()
		_, err := Open("db", Options{Config: cfg})
		if !errors.Is(err, store.ErrInvalid) || !strings.Contains(err.Error(), "made by "+strconv.Quote(doctored)) || !strings.Contains(err.Error(), strconv.Quote(now)) {
			t.Errorf("%s: Open = %v, want an error wrapping ErrInvalid that says it was made by %q and names %q", name, err, doctored, now)
		}
		// Put it right again for the next case.
		kv, err := pebblekv.Open("db", pebblekv.BytewiseLayout, rawConfig(cfg))
		if err != nil {
			t.Fatal(err)
		}
		if err := kv.Set(metaKey(metaPebble), []byte(now), pebble.Sync); err != nil {
			t.Fatal(err)
		}
		_ = kv.Close()
		if s, err = Open("db", Options{Config: cfg}); err != nil {
			t.Fatalf("%s: Open after putting the tag right = %v", name, err)
		}
	}
	_ = s.Close()
}

// The defaults are the product's: record and horizon commits are synced, the
// retention settles, hosts' boots are told apart.
func TestDefaultOptionsAreTheProducts(t *testing.T) {
	t.Parallel()
	o := DefaultOptions()
	if !o.Sync || !o.SettleRetention || o.Policy.BootKey != lifecycle.BootID || o.Retention != Synchronous || o.Tuning != pebblekv.BenchTuning() {
		t.Errorf("DefaultOptions() = %+v", o)
	}
}

// rawConfig is the config the store opens its database with, for a test that opens
// the database under it.
func rawConfig(c pebblekv.Config) pebblekv.Config {
	c.Schema = pebblekv.SchemaDefault
	return c
}
