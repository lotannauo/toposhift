package pebblelog

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
	"github.com/lotannauo/toposhift/internal/identity"
	"github.com/lotannauo/toposhift/internal/lifecycle"
)

func openMem(t *testing.T, opts Options) *Engine {
	t.Helper()
	opts.Tuning = pebblekv.TinyTuning()
	opts.FS = vfs.NewMem()
	e, err := Open("db", opts)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = e.Close() })
	return e
}

// stored is one key as it sits in the database; a baseline lists its entries as
// producer@eventNs.
type stored struct {
	dir     byte
	kind    byte
	ns      int64
	seq     uint64
	entries string
	w       uint64 // of a checkpoint or the baseline
}

func (s stored) String() string {
	return fmt.Sprintf("dir %d kind %d @%d seq %d [%s]", s.dir, s.kind, s.ns, s.seq, s.entries)
}

// dump lists every data key in key order.
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
		prefix, ns, seq, kind, err := parseKey(it.Key())
		if err != nil {
			t.Fatal(err)
		}
		s := stored{dir: prefix[prefixLen-1], kind: kind, ns: ns, seq: seq}
		switch kind {
		case kindRecord:
			ref, _, err := decodeRecordValue(s.dir, it.Value())
			if err != nil {
				t.Fatal(err)
			}
			s.entries = producerOf(s.dir, ref)
		default:
			st, err := decodeStamp(it.Value())
			if err != nil {
				t.Fatal(err)
			}
			s.w = st.W
			var names []string
			for _, en := range st.Entries {
				names = append(names, fmt.Sprintf("%s@%d", producerOf(s.dir, en.Ref), en.EventNs))
			}
			s.entries = strings.Join(names, ",")
		}
		out = append(out, s)
	}
	if err := it.Error(); err != nil {
		t.Fatal(err)
	}
	return out
}

func producerOf(dir byte, ref []byte) string {
	if dir == dirEntity {
		return string(ref)
	}
	return string(ref[subjectLen:])
}

// forward keeps the keys under the pod's forward prefix, which is where the
// tests look.
func forward(all []stored) []stored {
	return slices.DeleteFunc(slices.Clone(all), func(s stored) bool { return s.dir != byte(engine.Forward) })
}

var (
	t0            = time.Unix(1_700_000_000, 0).UTC()
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

func cloneAll(rs []engine.Record) []engine.Record {
	out := slices.Clone(rs)
	for i := range out {
		out[i].Payload = slices.Clone(out[i].Payload)
	}
	return out
}

// pair is the engine under test and the oracle, written and retained alike.
type pair struct {
	t   *testing.T
	e   *Engine
	ora *oracle.Oracle
}

func newPair(t *testing.T, opts Options) *pair {
	return &pair{t: t, e: openMem(t, opts), ora: oracle.New()}
}

func (p *pair) write(recs ...engine.Record) {
	p.t.Helper()
	for _, x := range []engine.Engine{p.e, p.ora} {
		if err := x.Write(cloneAll(recs)); err != nil {
			p.t.Fatal(err)
		}
	}
}

func (p *pair) retain(h time.Time) {
	p.t.Helper()
	for _, x := range []engine.Engine{p.e, p.ora} {
		if err := x.Retain(h); err != nil {
			p.t.Fatal(err)
		}
	}
}

// same asks both engines the same questions at every instant and token, in both
// directions and for the entity, and fails on the first difference.
func (p *pair) same(entities []identity.Fingerprint, times []time.Time, tokens []uint64) {
	p.t.Helper()
	for _, fp := range entities {
		for _, at := range times {
			for _, tok := range tokens {
				sc := engine.Scope{Layer: catalog.L2, AsOf: tok}
				for _, dir := range []engine.Direction{engine.Forward, engine.Reverse} {
					want, err := p.ora.Neighbors(fp, dir, at, sc)
					if err != nil {
						p.t.Fatal(err)
					}
					got, err := p.e.Neighbors(fp, dir, at, sc)
					if err != nil || !slices.Equal(got, want) {
						p.t.Fatalf("Neighbors(%s, %s, %s, asOf %d) = %v, %v; oracle says %v", fp, dir, at.Sub(t0), tok, got, err, want)
					}
				}
				wantAlive, _ := p.ora.Alive(fp, at, sc)
				if gotAlive, err := p.e.Alive(fp, at, sc); err != nil || gotAlive != wantAlive {
					p.t.Fatalf("Alive(%s, %s, asOf %d) = %v, %v; oracle says %v", fp, at.Sub(t0), tok, gotAlive, err, wantAlive)
				}
			}
		}
	}
}

// Nothing is overwritten: records at one instant keep every version, told apart
// by their Seq alone, in the same batch or another, and they come back in the
// order the key promises, newest first and highest Seq first, after the engine is
// closed and reopened and the data is in tables.
func TestRecordsAtOneInstantAreAllKept(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	open := func() *Engine {
		e, err := Open(dir, Options{Config: pebblekv.Config{Tuning: pebblekv.TinyTuning()}})
		if err != nil {
			t.Fatal(err)
		}
		return e
	}
	e := open()
	if err := e.Write([]engine.Record{
		edgeRecord(1, "kubelet", t0, lifecycle.Observe, time.Minute),
		edgeRecord(2, "kubelet", t0, lifecycle.Delete, 0),
	}); err != nil {
		t.Fatal(err)
	}
	if err := e.Settle(); err != nil {
		t.Fatal(err)
	}
	if err := e.Close(); err != nil {
		t.Fatal(err)
	}
	e = open()
	defer func() { _ = e.Close() }()
	if err := e.Write([]engine.Record{
		edgeRecord(3, "kubelet", t0, lifecycle.Observe, time.Minute),
		edgeRecord(4, "other", t0, lifecycle.Observe, 0),
		edgeRecord(5, "kubelet", t0.Add(time.Second), lifecycle.Delete, 0),
	}); err != nil {
		t.Fatal(err)
	}
	ns := t0.UnixNano()
	want := []stored{
		{1, kindRecord, ns + 1e9, 5, "kubelet", 0},
		{1, kindRecord, ns, 4, "other", 0},
		{1, kindRecord, ns, 3, "kubelet", 0},
		{1, kindRecord, ns, 2, "kubelet", 0},
		{1, kindRecord, ns, 1, "kubelet", 0},
	}
	if got := forward(dump(t, e)); !slices.Equal(got, want) {
		t.Fatalf("stored under the forward prefix:\n got %v\nwant %v", got, want)
	}
	recs, err := e.Window(podFP, engine.Forward, t0, t0.Add(time.Nanosecond), engine.Current(catalog.L2))
	if err != nil || len(recs) != 4 {
		t.Fatalf("Window returned %d records, %v; want 4", len(recs), err)
	}
	recs, err = e.Window(podFP, engine.Forward, t0, t0.Add(time.Nanosecond), engine.Scope{Layer: catalog.L2, AsOf: 2})
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
	for _, s := range forward(dump(t, p.e)) {
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
	e := openMem(t, Options{})
	r := edgeRecord(7, "p", t0, lifecycle.Observe, 0)
	sides, err := e.sidesOf(r)
	if err != nil {
		t.Fatal(err)
	}
	bad := pebblekv.FromRecord(r)
	bad.Seq = 8
	if err := e.kv.Set(recordKey(sides[0].prefix, t0.UnixNano(), 7), appendRecordValue(nil, sides[0].ref, bad), nil); err != nil {
		t.Fatal(err)
	}
	if _, err := e.Window(podFP, engine.Forward, t0, t0.Add(time.Second), engine.Current(catalog.L2)); err == nil {
		t.Fatal("a record whose key and value disagree about its Seq was returned")
	}
}

func TestRetentionKeepsExactlyWhatLaterAnswersNeed(t *testing.T) {
	t.Parallel()
	h := t0.Add(10 * time.Minute)
	min := func(n int) time.Time { return t0.Add(time.Duration(n) * time.Minute) }
	ns := func(n int) int64 { return min(n).UnixNano() }
	ent := func(prod string, at int) string { return fmt.Sprintf("%s@%d", prod, ns(at)) }

	type want struct {
		records []string // "minute/seq" of the records left, newest first
		base    string   // the baseline's entries, "" for none
	}
	for name, tc := range map[string]struct {
		before []engine.Record
		want   want
	}{
		"an open observation before the horizon becomes the baseline, with its own event time": {
			[]engine.Record{edgeRecord(1, "p", min(1), lifecycle.Observe, 0)},
			want{nil, ent("p", 1)},
		},
		"one whose deadline is after the horizon is kept": {
			[]engine.Record{edgeRecord(1, "p", min(5), lifecycle.Observe, 30*time.Minute)},
			want{nil, ent("p", 5)},
		},
		"one whose deadline has passed is not": {
			[]engine.Record{edgeRecord(1, "p", min(5), lifecycle.Observe, 2*time.Minute)},
			want{nil, ""},
		},
		"a deadline exactly at the horizon has passed": {
			[]engine.Record{edgeRecord(1, "p", min(5), lifecycle.Observe, 5*time.Minute)},
			want{nil, ""},
		},
		"a delete before the horizon ends it": {
			[]engine.Record{edgeRecord(1, "p", min(1), lifecycle.Observe, 0), edgeRecord(2, "p", min(3), lifecycle.Delete, 0)},
			want{nil, ""},
		},
		"only the newest before the horizon counts": {
			[]engine.Record{
				edgeRecord(1, "p", min(1), lifecycle.Observe, 0), edgeRecord(2, "p", min(3), lifecycle.Observe, 0),
				edgeRecord(3, "p", min(4), lifecycle.Observe, 0),
			},
			want{nil, ent("p", 4)},
		},
		"at one instant the highest Seq decides": {
			[]engine.Record{
				edgeRecord(1, "p", min(4), lifecycle.Observe, 0), edgeRecord(2, "p", min(4), lifecycle.Delete, 0),
				edgeRecord(3, "p", min(4), lifecycle.Observe, 0),
			},
			want{nil, ent("p", 4)},
		},
		"and a delete at the top ends it": {
			[]engine.Record{edgeRecord(1, "p", min(4), lifecycle.Observe, 0), edgeRecord(2, "p", min(4), lifecycle.Delete, 0)},
			want{nil, ""},
		},
		"each producer has its own entry": {
			[]engine.Record{
				edgeRecord(1, "p", min(1), lifecycle.Observe, 0), edgeRecord(2, "q", min(2), lifecycle.Observe, 0),
				edgeRecord(3, "r", min(3), lifecycle.Observe, time.Minute), edgeRecord(4, "p", min(4), lifecycle.Delete, 0),
			},
			want{nil, ent("q", 2)},
		},
		"records at or after the horizon stay, the horizon instant included": {
			[]engine.Record{
				edgeRecord(1, "p", min(1), lifecycle.Observe, time.Minute), edgeRecord(2, "p", h, lifecycle.Delete, 0),
				edgeRecord(3, "p", min(12), lifecycle.Observe, 0), edgeRecord(4, "p", min(12), lifecycle.Observe, 0),
			},
			want{[]string{"12/4", "12/3", "10/2"}, ""},
		},
		"a baseline entry stays under a key that has newer records": {
			[]engine.Record{edgeRecord(1, "p", min(2), lifecycle.Observe, 0), edgeRecord(2, "p", min(15), lifecycle.Delete, 0)},
			want{[]string{"15/2"}, ent("p", 2)},
		},
		"a run keeps its Through, and so its deadline": {
			[]engine.Record{func() engine.Record {
				r := edgeRecord(1, "p", min(1), lifecycle.Observe, 3*time.Minute)
				r.Through = min(9)
				return r
			}()},
			want{nil, ent("p", 1)}, // alive until 9+3 = 12 minutes
		},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			p := newPair(t, Options{})
			for _, r := range tc.before {
				p.write(r)
			}
			if err := p.e.Settle(); err != nil { // retention over tables, not only the memtable
				t.Fatal(err)
			}
			p.retain(h)
			checkStamps(t, p.e, p.e.LastSeq(), h)
			var records []string
			var base string
			sawBase := false
			for _, s := range forward(dump(t, p.e)) {
				if s.kind == kindBaseline {
					base, sawBase = s.entries, true
					if s.ns != h.UnixNano() {
						t.Errorf("the baseline is at %d, want the horizon %d", s.ns, h.UnixNano())
					}
					continue
				}
				records = append(records, fmt.Sprintf("%d/%d", time.Unix(0, s.ns).Sub(t0)/time.Minute, s.seq))
			}
			if !slices.Equal(records, tc.want.records) {
				t.Errorf("records left = %v, want %v", records, tc.want.records)
			}
			if base != tc.want.base || sawBase != (tc.want.base != "") {
				t.Errorf("baseline = %q (present %v), want %q", base, sawBase, tc.want.base)
			}
			// Both directions are retained by the same rule, and the answers at and
			// after the horizon are the oracle's, at every token from the retention's.
			var rev int
			for _, s := range dump(t, p.e) {
				if s.dir == byte(engine.Reverse) && s.kind == kindRecord {
					rev++
				}
			}
			if rev != len(tc.want.records) {
				t.Errorf("%d records left under the reverse prefix, want %d", rev, len(tc.want.records))
			}
			last := p.e.LastSeq()
			p.same([]identity.Fingerprint{podFP, nodeFP}, []time.Time{h, h.Add(time.Nanosecond), min(11), min(12), min(13), min(40)}, []uint64{last, last + 1, engine.Latest})
		})
	}
}

// checkStamps decodes every baseline under the pod's forward prefix and checks
// what it was stamped with: the retention's last Seq as both its built-through
// number and its W, the horizon exactly, and the fold version. Nothing in a read
// uses these yet, and a baseline is never rewritten, so a wrong stamp would sit
// silently until checkpoints build on it.
func checkStamps(t *testing.T, e *Engine, last uint64, horizon time.Time) {
	t.Helper()
	lo, hi := prefixBounds(mustPrefix(t, e))
	it, err := e.kv.NewIter(&pebble.IterOptions{LowerBound: lo, UpperBound: hi})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = it.Close() }()
	for ok := it.First(); ok; ok = it.Next() {
		_, _, _, kind, err := parseKey(it.Key())
		if err != nil {
			t.Fatal(err)
		}
		if kind != kindBaseline {
			continue
		}
		st, err := decodeStamp(it.Value())
		if err != nil {
			t.Fatal(err)
		}
		if st.Kind != kindBaseline || st.Through != last || st.W != last || !st.Horizon.Equal(horizon) || st.FoldVersion != FoldVersion {
			t.Errorf("baseline stamp = kind %d, through %d, W %d, horizon %v, fold %d; want baseline, %d, %d, %v, %d",
				st.Kind, st.Through, st.W, st.Horizon, st.FoldVersion, last, last, horizon, FoldVersion)
		}
	}
}

func mustPrefix(t *testing.T, e *Engine) []byte {
	t.Helper()
	p, ok := e.prefixOf(catalog.L2, podFP, byte(engine.Forward))
	if !ok {
		t.Fatal("no prefix")
	}
	return p
}

// A second retention builds on what the first left: the old baseline supplies an
// entry only for a key with no newer record, and one whose deadline has passed is
// dropped with it. A prefix whose only content is a baseline is rewritten too.
func TestSecondRetentionSupersedesTheFirstBaseline(t *testing.T) {
	t.Parallel()
	min := func(n int) time.Time { return t0.Add(time.Duration(n) * time.Minute) }
	p := newPair(t, Options{})
	p.write(
		edgeRecord(1, "open", min(1), lifecycle.Observe, 0),
		edgeRecord(2, "timed", min(2), lifecycle.Observe, 10*time.Minute), // until 12
		edgeRecord(3, "replaced", min(3), lifecycle.Observe, 0),
		edgeRecord(4, "doomed", min(4), lifecycle.Observe, 0),
	)
	p.retain(min(10))
	p.write(
		edgeRecord(5, "replaced", min(11), lifecycle.Observe, 30*time.Minute), // newer record for one entry
		edgeRecord(6, "doomed", min(12), lifecycle.Delete, 0),
	)
	if got := forward(dump(t, p.e)); len(got) != 3 || got[2].kind != kindBaseline {
		t.Fatalf("after the first retention: %v", got)
	}
	p.retain(min(20))
	got := forward(dump(t, p.e))
	ns := func(n int) int64 { return min(n).UnixNano() }
	want := []stored{{1, kindBaseline, ns(20), 0, fmt.Sprintf("open@%d,replaced@%d", ns(1), ns(11)), 0}}
	// "replaced" is by its newer record, and not by the baseline's entry for it.
	if len(got) != 1 || got[0].kind != kindBaseline || got[0].ns != want[0].ns {
		t.Fatalf("after the second retention: %v", got)
	}
	names := strings.Split(got[0].entries, ",")
	slices.Sort(names)
	if !slices.Equal(names, []string{fmt.Sprintf("open@%d", ns(1)), fmt.Sprintf("replaced@%d", ns(11))}) {
		t.Fatalf("baseline entries = %v: the timed entry has expired, the doomed one was deleted, and 'replaced' is the newer record's", names)
	}
	last := p.e.LastSeq()
	p.same([]identity.Fingerprint{podFP, nodeFP}, []time.Time{min(20), min(21), min(30), min(60)}, []uint64{last, engine.Latest})

	// A prefix with only a baseline, whose entries all lapse, is emptied.
	p.retain(min(100))
	for _, s := range forward(dump(t, p.e)) {
		if s.kind != kindBaseline || !strings.HasPrefix(s.entries, "open@") || strings.Contains(s.entries, "replaced") {
			t.Fatalf("after the third retention: %v", s)
		}
	}
	p.same([]identity.Fingerprint{podFP, nodeFP}, []time.Time{min(100), min(200)}, []uint64{last, engine.Latest})
}

// The probe the scripted checks do not make: a token that is at or above the
// retention's last Seq, but below a record written at the horizon itself, must
// fall through that record to the baseline.
func TestATokenBelowARecordAtTheHorizonSeesTheBaseline(t *testing.T) {
	t.Parallel()
	h := t0.Add(200 * time.Second)
	p := newPair(t, Options{})
	p.write(edgeRecord(1, "q", t0.Add(100*time.Second), lifecycle.Observe, 0))
	p.retain(h)
	p.write(edgeRecord(2, "q", h, lifecycle.Delete, 0), edgeRecord(3, "r", h, lifecycle.Observe, 0))
	p.same([]identity.Fingerprint{podFP, nodeFP}, []time.Time{h, h.Add(time.Nanosecond), h.Add(time.Hour)}, []uint64{1, 2, 3, engine.Latest})
	got, err := p.e.Neighbors(podFP, engine.Forward, h.Add(time.Hour), engine.Scope{Layer: catalog.L2, AsOf: 1})
	if err != nil || len(got) != 1 {
		t.Fatalf("pinned at the retention's seq: %v, %v; want the baseline's edge", got, err)
	}
}

func TestRetentionBeyondTheRepresentableRange(t *testing.T) {
	t.Parallel()
	p := newPair(t, Options{})
	p.write(
		edgeRecord(1, "open", engine.MinEventTime, lifecycle.Observe, 0),
		edgeRecord(2, "timed", engine.MinEventTime, lifecycle.Observe, time.Hour),
		edgeRecord(3, "last", engine.MaxEventTime, lifecycle.Observe, 0),
	)
	// After every instant a store can hold, only what never expires is alive.
	p.retain(engine.MaxEventTime.Add(time.Hour))
	for _, s := range forward(dump(t, p.e)) {
		if s.kind != kindBaseline || s.ns != math.MaxInt64 || strings.Contains(s.entries, "timed") {
			t.Errorf("after a retention past the end of time: %v", s)
		}
	}
	// The baseline is keyed at the last instant, and still stamped with the real horizon.
	checkStamps(t, p.e, 3, engine.MaxEventTime.Add(time.Hour))
	if err := p.e.Write([]engine.Record{edgeRecord(4, "p", engine.MaxEventTime, lifecycle.Observe, 0)}); !errors.Is(err, engine.ErrBeforeHorizon) {
		t.Errorf("a write after a horizon past the end of time = %v, want ErrBeforeHorizon", err)
	}
	p.same([]identity.Fingerprint{podFP, nodeFP}, []time.Time{engine.MaxEventTime, engine.MaxEventTime.Add(time.Hour)}, []uint64{3, engine.Latest})
	if err := p.e.Retain(time.Time{}); err != nil {
		t.Error(err)
	}
}

// A horizon at the epoch removes nothing, and must not wrap the range delete
// around to the whole prefix; one before it changes nothing but the horizon.
func TestRetentionAtTheEpoch(t *testing.T) {
	t.Parallel()
	p := newPair(t, Options{})
	p.write(
		edgeRecord(1, "p", engine.MinEventTime, lifecycle.Observe, 0),
		edgeRecord(2, "q", engine.MinEventTime.Add(time.Second), lifecycle.Observe, time.Hour),
	)
	before := dump(t, p.e)
	p.retain(engine.MinEventTime)
	if got := dump(t, p.e); !slices.Equal(got, before) {
		t.Fatalf("a retention at the epoch changed what is stored:\n got %v\nwant %v", got, before)
	}
	p.retain(engine.MinEventTime.Add(time.Nanosecond)) // now the epoch instant is old
	p.same([]identity.Fingerprint{podFP, nodeFP}, []time.Time{engine.MinEventTime.Add(time.Nanosecond), engine.MinEventTime.Add(time.Minute)}, []uint64{2, engine.Latest})
	if err := p.e.Write([]engine.Record{edgeRecord(3, "p", engine.MinEventTime, lifecycle.Observe, 0)}); !errors.Is(err, engine.ErrBeforeHorizon) {
		t.Errorf("a write at the epoch after retaining past it = %v, want ErrBeforeHorizon", err)
	}
}

// Retention committed in many pieces leaves the same data as one commit, and
// every answer is the oracle's, at the end.
func TestRetentionInManyPiecesKeepsAnswers(t *testing.T) {
	conformance.SkipWhenTrimmed(t)
	t.Parallel()
	whole := openMem(t, Options{})
	pieces := openMem(t, Options{retainBatchBytes: 1}) // every prefix is its own commit
	ora := oracle.New()
	g, err := workload.New(workload.Tiny())
	if err != nil {
		t.Fatal(err)
	}
	var first, last time.Time
	for range 400 {
		batch := g.Batch(40)
		if first.IsZero() {
			first = batch[0].EventTime
		}
		last = batch[len(batch)-1].EventTime
		for _, e := range []engine.Engine{whole, pieces, ora} {
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
	for _, e := range []engine.Engine{whole, pieces, ora} {
		if err := e.Retain(h); err != nil {
			t.Fatal(err)
		}
	}
	if rec.Counter("retain.range_deletes") < 2 {
		t.Fatalf("the retention deleted %d ranges; the test needs several prefixes", rec.Counter("retain.range_deletes"))
	}
	if !slices.Equal(dump(t, whole), dump(t, pieces)) {
		t.Fatal("a retention in many pieces left different data from one in a single commit")
	}
	compareToOracle(t, pieces, ora, g.Entities(), h, pieces.LastSeq(), engine.Latest)
}

func compareToOracle(t *testing.T, e engine.Engine, ora *oracle.Oracle, entities []identity.Fingerprint, h time.Time, tokens ...uint64) {
	t.Helper()
	if len(tokens) == 0 {
		tokens = []uint64{engine.Latest}
	}
	for _, fp := range entities {
		for _, off := range []time.Duration{0, time.Minute, 5 * time.Minute, time.Hour} {
			for _, layer := range []catalog.Layer{catalog.L0, catalog.L1, catalog.L2, catalog.L3} {
				for _, tok := range tokens {
					sc := engine.Scope{Layer: layer, AsOf: tok}
					for _, dir := range []engine.Direction{engine.Forward, engine.Reverse} {
						want, err := ora.Neighbors(fp, dir, h.Add(off), sc)
						if err != nil {
							t.Fatal(err)
						}
						got, err := e.Neighbors(fp, dir, h.Add(off), sc)
						if err != nil || !slices.Equal(got, want) {
							t.Fatalf("Neighbors(%s, %s, h+%s, %s, asOf %d) = %v, %v; oracle says %v", fp, dir, off, layer, tok, got, err, want)
						}
					}
					wantAlive, err := ora.Alive(fp, h.Add(off), sc)
					if err != nil {
						t.Fatal(err)
					}
					if gotAlive, err := e.Alive(fp, h.Add(off), sc); err != nil || gotAlive != wantAlive {
						t.Fatalf("Alive(%s, h+%s, %s, asOf %d) = %v, %v; oracle says %v", fp, off, layer, tok, gotAlive, err, wantAlive)
					}
				}
			}
		}
	}
}

// If a retention stops after k commits, the horizon is already committed and the
// prefixes not yet rewritten keep their old records; every answer at and after
// the horizon is still the oracle's, also after a reopening, and a later
// retention finishes the work.
func TestARetentionThatStopsHalfwayLeavesCorrectAnswers(t *testing.T) {
	conformance.SkipWhenTrimmed(t)
	t.Parallel()
	for name, tc := range map[string]struct {
		stopAfter   int
		checkpoints CheckpointOptions
	}{
		"after 1 commit, no checkpoints":    {1, CheckpointOptions{}},
		"after 2 commits, no checkpoints":   {2, CheckpointOptions{}},
		"after 4 commits, with checkpoints": {4, CheckpointOptions{On: true, KMin: 2, Lag: 5 * time.Second}},
	} {
		stopAfter := tc.stopAfter
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			fs := vfs.NewMem()
			cfg := pebblekv.Config{Tuning: pebblekv.TinyTuning(), FS: fs}
			e, err := Open("db", Options{Config: cfg, Checkpoints: tc.checkpoints, retainBatchBytes: 1, retainStopAfter: stopAfter})
			if err != nil {
				t.Fatal(err)
			}
			ora := oracle.New()
			g, err := workload.New(workload.Tiny())
			if err != nil {
				t.Fatal(err)
			}
			var first, last time.Time
			for range 400 {
				batch := g.Batch(40)
				if first.IsZero() {
					first = batch[0].EventTime
				}
				last = batch[len(batch)-1].EventTime
				for _, x := range []engine.Engine{e, ora} {
					if err := x.Write(cloneAll(batch)); err != nil {
						t.Fatal(err)
					}
				}
				if last.Sub(first) > 90*time.Second {
					break
				}
			}
			h := first.Add(last.Sub(first) / 2)
			before := dump(t, e)
			if tc.checkpoints.On && len(slices.DeleteFunc(slices.Clone(before), func(s stored) bool { return s.kind != kindCheckpoint })) == 0 {
				t.Fatal("no checkpoint was written before the retention")
			}
			if err := e.Retain(h); !errors.Is(err, errInjected) {
				t.Fatalf("Retain = %v, want the injected failure", err)
			}
			_ = ora.Retain(h)
			if got := dump(t, e); slices.Equal(got, before) {
				t.Fatal("the failed retention changed nothing")
			}
			compareToOracle(t, e, ora, g.Entities(), h, e.LastSeq(), engine.Latest)
			if err := e.Close(); err != nil {
				t.Fatal(err)
			}
			e, err = Open("db", Options{Config: cfg, Checkpoints: tc.checkpoints})
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = e.Close() }()
			compareToOracle(t, e, ora, g.Entities(), h, e.LastSeq(), engine.Latest)
			// The horizon was committed before any of the work, so it survived the
			// stop and a record before it is refused. (Were it committed last, such a
			// record would be accepted into a prefix already rewritten, below its
			// baseline, where no read at or after the horizon would ever see it.)
			late := edgeRecord(e.LastSeq()+1, "late", h.Add(-time.Nanosecond), lifecycle.Delete, 0)
			if err := e.Write([]engine.Record{late}); !errors.Is(err, engine.ErrBeforeHorizon) {
				t.Fatalf("after the stop, a write before the horizon = %v, want ErrBeforeHorizon", err)
			}
			// The same horizon is a no-op; a later one reclaims the rest.
			if err := e.Retain(h); err != nil {
				t.Fatal(err)
			}
			h2 := h.Add(time.Minute)
			if err := e.Retain(h2); err != nil {
				t.Fatal(err)
			}
			_ = ora.Retain(h2)
			compareToOracle(t, e, ora, g.Entities(), h2, e.LastSeq(), engine.Latest)
		})
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
	var batch []engine.Record
	for i, name := range names {
		seq++
		batch = append(batch, edgeRecord(seq, name, t0.Add(time.Duration(i)*time.Second), lifecycle.Observe, time.Hour))
		seq++
		batch = append(batch, edgeRecord(seq, name, t0.Add(time.Duration(i)*time.Second+time.Millisecond), lifecycle.Delete, 0))
	}
	p.write(batch...)
	if err := p.e.Settle(); err != nil {
		t.Fatal(err)
	}
	var times []time.Time
	for off := -time.Second; off < 10*time.Second; off += 250 * time.Millisecond {
		times = append(times, t0.Add(off))
	}
	p.same([]identity.Fingerprint{podFP, nodeFP}, times, []uint64{seq / 2, seq, engine.Latest})
	want, _ := p.ora.Window(podFP, engine.Forward, t0.Add(-time.Hour), t0.Add(time.Hour), engine.Current(catalog.L2))
	got, err := p.e.Window(podFP, engine.Forward, t0.Add(-time.Hour), t0.Add(time.Hour), engine.Current(catalog.L2))
	if err != nil || len(got) != len(names)*2 || !slices.EqualFunc(got, want, func(a, b engine.Record) bool {
		return a.Producer == b.Producer && a.Seq == b.Seq && a.EventTime.Equal(b.EventTime) && a.Kind == b.Kind
	}) {
		t.Fatalf("Window returned %d records, %v; want %d matching the oracle", len(got), err, len(names)*2)
	}
	// Every one of them has ended, so a retention leaves nothing to rewrite.
	p.retain(t0.Add(30 * time.Second))
	if got := dump(t, p.e); len(got) != 0 {
		t.Errorf("a retention after every reference ended left %v", got)
	}
	p.same([]identity.Fingerprint{podFP, nodeFP}, []time.Time{t0.Add(30 * time.Second), t0.Add(time.Minute)}, []uint64{seq, engine.Latest})
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
	toks := []uint64{0, 1, 2, 3, 4, engine.Latest}
	p.same([]identity.Fingerprint{podFP, nodeFP}, times, toks)
	if got, err := p.e.Neighbors(podFP, engine.Forward, t0.Add(5*time.Second), engine.Current(catalog.L2)); err != nil || len(got) != 1 {
		t.Fatalf("with only 'a' still holding, Neighbors = %v, %v; want the edge", got, err)
	}
	p.retain(t0.Add(3 * time.Second))
	forwardKeys := forward(dump(t, p.e))
	if len(forwardKeys) != 1 || forwardKeys[0].kind != kindBaseline || forwardKeys[0].entries != fmt.Sprintf("a@%d", t0.UnixNano()) {
		t.Fatalf("after the retention: %v; want one baseline holding only 'a' (the others ended or lapsed)", forwardKeys)
	}
	p.same([]identity.Fingerprint{podFP, nodeFP}, times[4:], []uint64{4, engine.Latest})
}

func TestOpenRefusesAnotherFormat(t *testing.T) {
	t.Parallel()
	fs := vfs.NewMem()
	cfg := pebblekv.Config{Tuning: pebblekv.TinyTuning(), FS: fs}
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
	if got := rec.Counter("write.records"); got != 6 {
		t.Errorf("write.records = %d, want 6 (three records, two directions)", got)
	}
	// A read pinned before every record steps over each record it cannot see.
	if _, err := e.Neighbors(podFP, engine.Forward, t0.Add(time.Hour), engine.Scope{Layer: catalog.L2, AsOf: 0}); err != nil {
		t.Fatal(err)
	}
	if got := rec.Counter("read.records_stepped"); got != 3 {
		t.Errorf("read.records_stepped = %d, want 3", got)
	}
	if err := e.Retain(t0.Add(90 * time.Second)); err != nil {
		t.Fatal(err)
	}
	for name, want := range map[string]int64{
		"retain.prefixes_replayed": 2, "retain.records_replayed": 4, "retain.baselines_written": 2, "retain.range_deletes": 2,
	} {
		if got := rec.Counter(name); got != want {
			t.Errorf("%s = %d, want %d", name, got, want)
		}
	}
}

func TestReadsOfStrangersAreEmptyNotErrors(t *testing.T) {
	t.Parallel()
	e := openMem(t, Options{})
	stranger := fingerprintOf("nonesuch", 0)
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

// Readers running while a retention commits prefix by prefix still see only
// states the oracle was in: every commit leaves the answers it promises.
func TestConcurrentReadsDuringPiecewiseRetention(t *testing.T) {
	t.Parallel()
	e := openMem(t, Options{retainBatchBytes: 1})
	if err := conformance.CheckConcurrentReads(e); err != nil {
		t.Fatal(err)
	}
}
