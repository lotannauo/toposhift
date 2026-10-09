package pebblestore

import (
	"context"
	"errors"
	"fmt"
	"math"
	"slices"
	"strings"
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

func TestRetentionKeepsExactlyWhatLaterAnswersNeed(t *testing.T) {
	t.Parallel()
	h := t0.Add(10 * time.Minute)
	at := func(n int) time.Time { return t0.Add(time.Duration(n) * time.Minute) }
	ns := func(n int) int64 { return at(n).UnixNano() }
	ent := func(prod string, minute int) string { return fmt.Sprintf("%s@%d", prod, ns(minute)) }

	type want struct {
		records []string // "minute/seq" of the records left, newest first
		base    string   // the baseline's entries, "" for none
	}
	for name, tc := range map[string]struct {
		before []store.Record
		want   want
	}{
		"an open observation before the horizon becomes the baseline, with its own event time": {
			[]store.Record{edgeRecord(1, "p", at(1), lifecycle.Observe, 0)},
			want{nil, ent("p", 1)},
		},
		"one whose deadline is after the horizon is kept": {
			[]store.Record{edgeRecord(1, "p", at(5), lifecycle.Observe, 30*time.Minute)},
			want{nil, ent("p", 5)},
		},
		"one whose deadline has passed is not": {
			[]store.Record{edgeRecord(1, "p", at(5), lifecycle.Observe, 2*time.Minute)},
			want{nil, ""},
		},
		"a deadline exactly at the horizon has passed": {
			[]store.Record{edgeRecord(1, "p", at(5), lifecycle.Observe, 5*time.Minute)},
			want{nil, ""},
		},
		"a delete before the horizon ends it": {
			[]store.Record{edgeRecord(1, "p", at(1), lifecycle.Observe, 0), edgeRecord(2, "p", at(3), lifecycle.Delete, 0)},
			want{nil, ""},
		},
		"only the newest before the horizon counts": {
			[]store.Record{
				edgeRecord(1, "p", at(1), lifecycle.Observe, 0), edgeRecord(2, "p", at(3), lifecycle.Observe, 0),
				edgeRecord(3, "p", at(4), lifecycle.Observe, 0),
			},
			want{nil, ent("p", 4)},
		},
		"at one instant the highest Seq decides": {
			[]store.Record{
				edgeRecord(1, "p", at(4), lifecycle.Observe, 0), edgeRecord(2, "p", at(4), lifecycle.Delete, 0),
				edgeRecord(3, "p", at(4), lifecycle.Observe, 0),
			},
			want{nil, ent("p", 4)},
		},
		"and a delete at the top ends it": {
			[]store.Record{edgeRecord(1, "p", at(4), lifecycle.Observe, 0), edgeRecord(2, "p", at(4), lifecycle.Delete, 0)},
			want{nil, ""},
		},
		"each producer has its own entry": {
			[]store.Record{
				edgeRecord(1, "p", at(1), lifecycle.Observe, 0), edgeRecord(2, "q", at(2), lifecycle.Observe, 0),
				edgeRecord(3, "r", at(3), lifecycle.Observe, time.Minute), edgeRecord(4, "p", at(4), lifecycle.Delete, 0),
			},
			want{nil, ent("q", 2)},
		},
		"records at or after the horizon stay, the horizon instant included": {
			[]store.Record{
				edgeRecord(1, "p", at(1), lifecycle.Observe, time.Minute), edgeRecord(2, "p", h, lifecycle.Delete, 0),
				edgeRecord(3, "p", at(12), lifecycle.Observe, 0), edgeRecord(4, "p", at(12), lifecycle.Observe, 0),
			},
			want{[]string{"12/4", "12/3", "10/2"}, ""},
		},
		"a baseline entry stays under a key that has newer records": {
			[]store.Record{edgeRecord(1, "p", at(2), lifecycle.Observe, 0), edgeRecord(2, "p", at(15), lifecycle.Delete, 0)},
			want{[]string{"15/2"}, ent("p", 2)},
		},
		"a run keeps its Through, and so its deadline": {
			[]store.Record{func() store.Record {
				r := edgeRecord(1, "p", at(1), lifecycle.Observe, 3*time.Minute)
				r.Through = at(9)
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
			if err := p.s.kv.Settle(); err != nil { // retention over tables, not only the memtable
				t.Fatal(err)
			}
			p.retain(h)
			checkStamps(t, p.s, p.s.LastSeq(), h)
			var records []string
			var base string
			sawBase := false
			for _, s := range forward(dump(t, p.s)) {
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
			// after the horizon are the reference's, at every token from the
			// retention's.
			var rev int
			for _, s := range dump(t, p.s) {
				if s.dir == byte(store.Reverse) && s.kind == kindRecord {
					rev++
				}
			}
			if rev != len(tc.want.records) {
				t.Errorf("%d records left under the reverse prefix, want %d", rev, len(tc.want.records))
			}
			last := p.s.LastSeq()
			p.same([]identity.Fingerprint{podFP, nodeFP}, []time.Time{h, h.Add(time.Nanosecond), at(11), at(12), at(13), at(40)}, []uint64{last, last + 1, store.Latest})
		})
	}
}

// checkStamps decodes every baseline under the pod's forward prefix and checks
// what it was stamped with: the retention's last Seq as both its built-through
// number and its W, the horizon exactly, and the fold version. Nothing in a read
// uses these yet, and a baseline is never rewritten, so a wrong stamp would sit
// silently until checkpoints build on it.
func checkStamps(t *testing.T, s *Store, last uint64, horizon time.Time) {
	t.Helper()
	prefix, ok := s.prefixOf(catalog.L2, podFP, byte(store.Forward))
	if !ok {
		t.Fatal("no prefix")
	}
	lo, hi := prefixBounds(prefix)
	it, err := s.kv.NewIter(&pebble.IterOptions{LowerBound: lo, UpperBound: hi})
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
		if st.kind != kindBaseline || st.through != last || st.w != last || !st.horizon.Equal(horizon) || st.foldVersion != foldVersion {
			t.Errorf("baseline stamp = kind %d, through %d, W %d, horizon %v, fold %d; want baseline, %d, %d, %v, %d",
				st.kind, st.through, st.w, st.horizon, st.foldVersion, last, last, horizon, foldVersion)
		}
	}
}

// A second retention builds on what the first left: the old baseline supplies an
// entry only for a key with no newer record, and one whose deadline has passed is
// dropped with it. A prefix whose only content is a baseline is rewritten too.
func TestSecondRetentionSupersedesTheFirstBaseline(t *testing.T) {
	t.Parallel()
	at := func(n int) time.Time { return t0.Add(time.Duration(n) * time.Minute) }
	p := newPair(t, Options{})
	p.write(
		edgeRecord(1, "open", at(1), lifecycle.Observe, 0),
		edgeRecord(2, "timed", at(2), lifecycle.Observe, 10*time.Minute), // until 12
		edgeRecord(3, "replaced", at(3), lifecycle.Observe, 0),
		edgeRecord(4, "doomed", at(4), lifecycle.Observe, 0),
	)
	p.retain(at(10))
	p.write(
		edgeRecord(5, "replaced", at(11), lifecycle.Observe, 30*time.Minute), // newer record for one entry
		edgeRecord(6, "doomed", at(12), lifecycle.Delete, 0),
	)
	if got := forward(dump(t, p.s)); len(got) != 3 || got[2].kind != kindBaseline {
		t.Fatalf("after the first retention: %v", got)
	}
	p.retain(at(20))
	got := forward(dump(t, p.s))
	ns := func(n int) int64 { return at(n).UnixNano() }
	// "replaced" is by its newer record, and not by the baseline's entry for it.
	if len(got) != 1 || got[0].kind != kindBaseline || got[0].ns != ns(20) {
		t.Fatalf("after the second retention: %v", got)
	}
	names := strings.Split(got[0].entries, ",")
	slices.Sort(names)
	if !slices.Equal(names, []string{fmt.Sprintf("open@%d", ns(1)), fmt.Sprintf("replaced@%d", ns(11))}) {
		t.Fatalf("baseline entries = %v: the timed entry has expired, the doomed one was deleted, and 'replaced' is the newer record's", names)
	}
	last := p.s.LastSeq()
	p.same([]identity.Fingerprint{podFP, nodeFP}, []time.Time{at(20), at(21), at(30), at(60)}, []uint64{last, store.Latest})

	// A prefix with only a baseline, whose entries all lapse, is emptied.
	p.retain(at(100))
	for _, s := range forward(dump(t, p.s)) {
		if s.kind != kindBaseline || !strings.HasPrefix(s.entries, "open@") || strings.Contains(s.entries, "replaced") {
			t.Fatalf("after the third retention: %v", s)
		}
	}
	p.same([]identity.Fingerprint{podFP, nodeFP}, []time.Time{at(100), at(200)}, []uint64{last, store.Latest})
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
	p.same([]identity.Fingerprint{podFP, nodeFP}, []time.Time{h, h.Add(time.Nanosecond), h.Add(time.Hour)}, []uint64{1, 2, 3, store.Latest})
	got, err := p.s.Neighbors(bg, podFP, store.Forward, h.Add(time.Hour), store.Scope{Layer: catalog.L2, AsOf: 1})
	if err != nil || len(got) != 1 {
		t.Fatalf("pinned at the retention's seq: %v, %v; want the baseline's edge", got, err)
	}
}

func TestRetentionBeyondTheRepresentableRange(t *testing.T) {
	t.Parallel()
	p := newPair(t, Options{})
	p.write(
		edgeRecord(1, "open", store.MinEventTime, lifecycle.Observe, 0),
		edgeRecord(2, "timed", store.MinEventTime, lifecycle.Observe, time.Hour),
		edgeRecord(3, "last", store.MaxEventTime, lifecycle.Observe, 0),
	)
	// After every instant a store can hold, only what never expires is alive.
	p.retain(store.MaxEventTime.Add(time.Hour))
	for _, s := range forward(dump(t, p.s)) {
		if s.kind != kindBaseline || s.ns != math.MaxInt64 || strings.Contains(s.entries, "timed") {
			t.Errorf("after a retention past the end of time: %v", s)
		}
	}
	// The baseline is keyed at the last instant, and still stamped with the real horizon.
	checkStamps(t, p.s, 3, store.MaxEventTime.Add(time.Hour))
	if err := p.s.Write(bg, []store.Record{edgeRecord(4, "p", store.MaxEventTime, lifecycle.Observe, 0)}); !errors.Is(err, store.ErrBeforeHorizon) {
		t.Errorf("a write after a horizon past the end of time = %v, want ErrBeforeHorizon", err)
	}
	p.same([]identity.Fingerprint{podFP, nodeFP}, []time.Time{store.MaxEventTime.Add(time.Hour), store.MaxEventTime.Add(2 * time.Hour)}, []uint64{3, store.Latest})
	if err := p.s.Retain(bg, time.Time{}); err != nil {
		t.Error(err)
	}
}

// A horizon at the epoch removes nothing, and must not wrap the range delete
// around to the whole prefix; one before it changes nothing but the horizon.
func TestRetentionAtTheEpoch(t *testing.T) {
	t.Parallel()
	p := newPair(t, Options{})
	p.write(
		edgeRecord(1, "p", store.MinEventTime, lifecycle.Observe, 0),
		edgeRecord(2, "q", store.MinEventTime.Add(time.Second), lifecycle.Observe, time.Hour),
	)
	before := dump(t, p.s)
	p.retain(store.MinEventTime)
	if got := dump(t, p.s); !slices.Equal(got, before) {
		t.Fatalf("a retention at the epoch changed what is stored:\n got %v\nwant %v", got, before)
	}
	p.retain(store.MinEventTime.Add(time.Nanosecond)) // now the epoch instant is old
	p.same([]identity.Fingerprint{podFP, nodeFP}, []time.Time{store.MinEventTime.Add(time.Nanosecond), store.MinEventTime.Add(time.Minute)}, []uint64{2, store.Latest})
	if err := p.s.Write(bg, []store.Record{edgeRecord(3, "p", store.MinEventTime, lifecycle.Observe, 0)}); !errors.Is(err, store.ErrBeforeHorizon) {
		t.Errorf("a write at the epoch after retaining past it = %v, want ErrBeforeHorizon", err)
	}
}

// A Retain that does not move the horizon changes nothing: not the keys, not the
// horizon, not LastSeq.
func TestARetentionThatMovesNothingChangesNothing(t *testing.T) {
	t.Parallel()
	p := newPair(t, Options{})
	p.write(edgeRecord(1, "p", t0, lifecycle.Observe, 0), edgeRecord(2, "q", t0.Add(time.Minute), lifecycle.Observe, time.Hour))
	h := t0.Add(30 * time.Second)
	p.retain(h)
	want := store.Horizon{Time: h, Seq: 2}
	if got := p.s.Horizon(); got != want {
		t.Fatalf("Horizon = %v, want %v", got, want)
	}
	p.write(edgeRecord(3, "r", t0.Add(2*time.Minute), lifecycle.Observe, 0))
	before := dump(t, p.s)
	for _, again := range []time.Time{h, h.Add(-time.Second), {}} {
		if err := p.s.Retain(bg, again); err != nil {
			t.Fatal(err)
		}
	}
	if got := p.s.Horizon(); got != want {
		t.Errorf("Horizon after retentions that move nothing = %v, want %v (its Seq must not follow LastSeq)", got, want)
	}
	if p.s.LastSeq() != 3 {
		t.Errorf("LastSeq = %d, want 3", p.s.LastSeq())
	}
	if after := dump(t, p.s); !slices.Equal(after, before) {
		t.Errorf("a retention that moved nothing changed what is stored:\n got %v\nwant %v", after, before)
	}
}

// A horizon is stored per layer, with the Seq it was applied at, and comes back
// from a reopening.
func TestTheHorizonIsStoredForEveryLayerAndComesBack(t *testing.T) {
	t.Parallel()
	cfg := pebblekv.Config{Tuning: pebblekv.TinyTuning(), FS: vfs.NewMem()}
	s, err := Open("db", Options{Config: cfg})
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Write(bg, []store.Record{edgeRecord(1, "p", t0, lifecycle.Observe, 0), edgeRecord(2, "p", t0.Add(time.Hour), lifecycle.Observe, 0)}); err != nil {
		t.Fatal(err)
	}
	h := t0.Add(time.Minute)
	if err := s.Retain(bg, h); err != nil {
		t.Fatal(err)
	}
	for l := catalog.L0; l <= catalog.L3; l++ {
		raw, err := s.kv.GetMeta(metaKey(pebblekv.HorizonMetaName(l)))
		if err != nil {
			t.Fatal(err)
		}
		want, err := pebblekv.EncodeLayerHorizon(h, 2)
		if err != nil || string(raw) != string(want) {
			t.Errorf("horizon of %s = %x, %v; want %x", l, raw, err, want)
		}
		if got := s.horizonOf(l); got != (store.Horizon{Time: h, Seq: 2}) {
			t.Errorf("horizonOf(%s) = %v", l, got)
		}
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	s, err = Open("db", Options{Config: cfg})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = s.Close() }()
	if got := s.Horizon(); !got.Time.Equal(h) || got.Seq != 2 {
		t.Errorf("Horizon after reopening = %v, want {%v, 2}", got, h)
	}
	if _, err := s.Neighbors(bg, podFP, store.Forward, h.Add(-time.Nanosecond), store.Current(catalog.L2)); !errors.Is(err, store.ErrBeforeHorizon) {
		t.Errorf("a read before the reopened horizon = %v, want ErrBeforeHorizon", err)
	}
}

// Retention committed in many pieces leaves the same data as one commit, and
// every answer is the reference's, at the end.
func TestRetentionInManyPiecesKeepsAnswers(t *testing.T) {
	if testing.Short() {
		t.Skip("trimmed run: this check runs in the full tier")
	}
	t.Parallel()
	whole := openMem(t, Options{})
	rec := newMemRecorder()
	pieces := openMem(t, Options{retainBatchBytes: 1, Recorder: rec}) // every prefix is its own commit
	ref, err := memstore.Open(memstore.Options{})
	if err != nil {
		t.Fatal(err)
	}
	g, err := storetest.NewGenerator(storetest.Tiny())
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
		for _, s := range []store.Store{whole, pieces, ref} {
			if err := s.Write(bg, cloneAll(batch)); err != nil {
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
	for _, s := range []store.Store{whole, pieces, ref} {
		if err := s.Retain(bg, h); err != nil {
			t.Fatal(err)
		}
	}
	if rec.counters["retain.range_deletes"] < 2 {
		t.Fatalf("the retention deleted %d ranges; the test needs several prefixes", rec.counters["retain.range_deletes"])
	}
	if !slices.Equal(dump(t, whole), dump(t, pieces)) {
		t.Fatal("a retention in many pieces left different data from one in a single commit")
	}
	compareToReference(t, pieces, ref, g.Entities(), h, pieces.LastSeq(), store.Latest)
}

func compareToReference(t *testing.T, s, ref store.Store, entities []identity.Fingerprint, h time.Time, tokens ...uint64) {
	t.Helper()
	if len(tokens) == 0 {
		tokens = []uint64{store.Latest}
	}
	for _, fp := range entities {
		for _, off := range []time.Duration{0, time.Minute, 5 * time.Minute, time.Hour} {
			for _, layer := range []catalog.Layer{catalog.L0, catalog.L1, catalog.L2, catalog.L3} {
				for _, tok := range tokens {
					sc := store.Scope{Layer: layer, AsOf: tok}
					for _, dir := range []store.Direction{store.Forward, store.Reverse} {
						want, err := ref.Neighbors(bg, fp, dir, h.Add(off), sc)
						if err != nil {
							t.Fatal(err)
						}
						got, err := s.Neighbors(bg, fp, dir, h.Add(off), sc)
						if err != nil || !slices.Equal(got, want) {
							t.Fatalf("Neighbors(%s, %s, h+%s, %s, asOf %d) = %v, %v; the reference says %v", fp, dir, off, layer, tok, got, err, want)
						}
					}
					wantAlive, err := ref.Alive(bg, fp, h.Add(off), sc)
					if err != nil {
						t.Fatal(err)
					}
					if gotAlive, err := s.Alive(bg, fp, h.Add(off), sc); err != nil || gotAlive != wantAlive {
						t.Fatalf("Alive(%s, h+%s, %s, asOf %d) = %v, %v; the reference says %v", fp, off, layer, tok, gotAlive, err, wantAlive)
					}
				}
			}
		}
	}
}

// If a retention stops after k commits, the horizon is already committed and the
// prefixes not yet rewritten keep their old records; every answer at and after
// the horizon is still the reference's, also after a reopening, and a later
// retention finishes the work.
func TestARetentionThatStopsHalfwayLeavesCorrectAnswers(t *testing.T) {
	if testing.Short() {
		t.Skip("trimmed run: this check runs in the full tier")
	}
	t.Parallel()
	for _, stopAfter := range []int{1, 2, 4} {
		t.Run(fmt.Sprintf("after %d commits", stopAfter), func(t *testing.T) {
			t.Parallel()
			fs := vfs.NewMem()
			cfg := pebblekv.Config{Tuning: pebblekv.TinyTuning(), FS: fs}
			s, err := Open("db", Options{Config: cfg, retainBatchBytes: 1, retainStopAfter: stopAfter})
			if err != nil {
				t.Fatal(err)
			}
			ref, err := memstore.Open(memstore.Options{})
			if err != nil {
				t.Fatal(err)
			}
			g, err := storetest.NewGenerator(storetest.Tiny())
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
				for _, x := range []store.Store{s, ref} {
					if err := x.Write(bg, cloneAll(batch)); err != nil {
						t.Fatal(err)
					}
				}
				if last.Sub(first) > 90*time.Second {
					break
				}
			}
			h := first.Add(last.Sub(first) / 2)
			before := dump(t, s)
			if err := s.Retain(bg, h); !errors.Is(err, errInjected) {
				t.Fatalf("Retain = %v, want the injected failure", err)
			}
			_ = ref.Retain(bg, h)
			if got := dump(t, s); slices.Equal(got, before) {
				t.Fatal("the failed retention changed nothing")
			}
			if got := s.Horizon(); !got.Time.Equal(h) {
				t.Fatalf("after the failed retention the horizon is %v, want it published whole at %v", got, h)
			}
			compareToReference(t, s, ref, g.Entities(), h, s.LastSeq(), store.Latest)
			if err := s.Close(); err != nil {
				t.Fatal(err)
			}
			s, err = Open("db", Options{Config: cfg})
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = s.Close() }()
			compareToReference(t, s, ref, g.Entities(), h, s.LastSeq(), store.Latest)
			// The horizon was committed before any of the work, so it survived the
			// stop and a record before it is refused. (Were it committed last, such a
			// record would be accepted into a prefix already rewritten, below its
			// baseline, where no read at or after the horizon would ever see it.)
			late := edgeRecord(s.LastSeq()+1, "late", h.Add(-time.Nanosecond), lifecycle.Delete, 0)
			if err := s.Write(bg, []store.Record{late}); !errors.Is(err, store.ErrBeforeHorizon) {
				t.Fatalf("after the stop, a write before the horizon = %v, want ErrBeforeHorizon", err)
			}
			// The same horizon is a no-op; a later one reclaims the rest.
			if err := s.Retain(bg, h); err != nil {
				t.Fatal(err)
			}
			h2 := h.Add(time.Minute)
			if err := s.Retain(bg, h2); err != nil {
				t.Fatal(err)
			}
			_ = ref.Retain(bg, h2)
			compareToReference(t, s, ref, g.Entities(), h2, s.LastSeq(), store.Latest)
		})
	}
}

// The horizon of a retention is on disk before the first of its rewriting
// commits: a retention that stops after its first commit finds the horizon in the
// database a new store opens.
func TestTheHorizonIsCommittedBeforeAnyRewriting(t *testing.T) {
	t.Parallel()
	fs := vfs.NewMem()
	cfg := pebblekv.Config{Tuning: pebblekv.TinyTuning(), FS: fs}
	s, err := Open("db", Options{Config: cfg, retainBatchBytes: 1, retainStopAfter: 1})
	if err != nil {
		t.Fatal(err)
	}
	var recs []store.Record
	for i := range 6 {
		fp := fingerprintOf(catalog.K8sPod, byte(0x40+i))
		r := edgeRecord(uint64(i+1), "p", t0.Add(time.Duration(i)*time.Second), lifecycle.Observe, 0)
		r.Subject = store.EdgeSubject(fp, nodeFP, catalog.ScheduledOn)
		recs = append(recs, r)
	}
	if err := s.Write(bg, recs); err != nil {
		t.Fatal(err)
	}
	h := t0.Add(time.Hour)
	if err := s.Retain(bg, h); !errors.Is(err, errInjected) {
		t.Fatalf("Retain = %v, want the injected failure", err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	s, err = Open("db", Options{Config: cfg})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = s.Close() }()
	if got := s.Horizon(); !got.Time.Equal(h) || got.Seq != 6 {
		t.Errorf("Horizon after a retention that stopped = %v, want {%v, 6}", got, h)
	}
}

// Under Config.Sync the horizon commit is synced and the commits that rewrite
// history are not; without it nothing is.
func TestOnlyTheHorizonAndTheRecordsAreSynced(t *testing.T) {
	t.Parallel()
	for _, sync := range []bool{true, false} {
		t.Run(fmt.Sprintf("sync %v", sync), func(t *testing.T) {
			t.Parallel()
			fsys := &countingSyncFS{FS: vfs.NewMem()}
			cfg := pebblekv.Config{Tuning: pebblekv.TinyTuning(), FS: fsys, Sync: sync}
			s, err := Open("db", Options{Config: cfg, retainBatchBytes: 1})
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = s.Close() }()
			var recs []store.Record
			for i := range 6 {
				fp := fingerprintOf(catalog.K8sPod, byte(0x40+i))
				r := edgeRecord(uint64(i+1), "p", t0.Add(time.Duration(i)*time.Second), lifecycle.Observe, 0)
				r.Subject = store.EdgeSubject(fp, nodeFP, catalog.ScheduledOn)
				recs = append(recs, r)
			}
			syncsAfter := func(what func()) int64 {
				before := fsys.syncs.Load()
				what()
				return fsys.syncs.Load() - before
			}
			writes := syncsAfter(func() {
				for _, r := range recs {
					if err := s.Write(bg, []store.Record{r}); err != nil {
						t.Fatal(err)
					}
				}
			})
			// The horizon is one synced commit; the six rewriting commits (every
			// prefix is its own) would add six more if they were synced.
			retain := syncsAfter(func() {
				if err := s.Retain(bg, t0.Add(time.Hour)); err != nil {
					t.Fatal(err)
				}
			})
			if sync && (writes < int64(len(recs)) || retain != 1) {
				t.Errorf("with Sync: %d syncs for %d writes and %d for a retention; want at least one per write and exactly one for the retention", writes, len(recs), retain)
			}
			if !sync && (writes != 0 || retain != 0) {
				t.Errorf("without Sync: %d syncs for the writes and %d for the retention; want none", writes, retain)
			}
		})
	}
}

// The retention can be stopped by its context between commits, once the horizon
// is published: it then returns the context's error, and what it left is correct.
func TestARetentionWhoseContextEndsLeavesCorrectAnswers(t *testing.T) {
	t.Parallel()
	p := newPair(t, Options{retainBatchBytes: 1})
	for i := range 6 {
		fp := fingerprintOf(catalog.K8sPod, byte(0x40+i))
		r := edgeRecord(uint64(i+1), "p", t0.Add(time.Duration(i)*time.Second), lifecycle.Observe, 0)
		r.Subject = store.EdgeSubject(fp, nodeFP, catalog.ScheduledOn)
		p.write(r)
	}
	ctx, cancel := context.WithCancel(bg)
	cancel()
	if err := p.s.Retain(ctx, t0.Add(time.Hour)); !errors.Is(err, context.Canceled) {
		t.Fatalf("Retain with a done context = %v", err)
	}
	if !p.s.Horizon().IsZero() {
		t.Errorf("a Retain that was refused before it began moved the horizon to %v", p.s.Horizon())
	}
	p.retain(t0.Add(time.Hour))
	p.same([]identity.Fingerprint{nodeFP}, []time.Time{t0.Add(time.Hour), t0.Add(2 * time.Hour)}, []uint64{6, store.Latest})
}

// After every commit of the rewriting, the horizon is published and stored, a read
// before it is refused, and a read at it is answered: the discarding never runs
// ahead of the refusal.
func TestTheHorizonIsPublishedBeforeTheFirstRewritingCommit(t *testing.T) {
	t.Parallel()
	var s *Store
	h := t0.Add(time.Hour)
	calls := 0
	hook := func() {
		calls++
		if got := s.Horizon(); !got.Time.Equal(h) || got.Seq != 6 {
			t.Errorf("after commit %d the published horizon is %v, want {%v, 6}", calls, got, h)
		}
		raw, err := s.kv.GetMeta(metaKey(pebblekv.HorizonMetaName(catalog.L2)))
		if want, _ := pebblekv.EncodeLayerHorizon(h, 6); err != nil || string(raw) != string(want) {
			t.Errorf("after commit %d the stored horizon is %x, %v; want %x", calls, raw, err, want)
		}
		if _, err := s.Neighbors(bg, nodeFP, store.Reverse, h.Add(-time.Nanosecond), store.Current(catalog.L2)); !errors.Is(err, store.ErrBeforeHorizon) {
			t.Errorf("after commit %d a read before the horizon = %v, want ErrBeforeHorizon", calls, err)
		}
		if _, err := s.Neighbors(bg, nodeFP, store.Reverse, h, store.Current(catalog.L2)); err != nil {
			t.Errorf("after commit %d a read at the horizon = %v", calls, err)
		}
	}
	s = openMem(t, Options{retainBatchBytes: 1, afterRetainCommit: hook})
	for i := range 6 {
		fp := fingerprintOf(catalog.K8sPod, byte(0x40+i))
		r := edgeRecord(uint64(i+1), "p", t0.Add(time.Duration(i)*time.Second), lifecycle.Observe, 0)
		r.Subject = store.EdgeSubject(fp, nodeFP, catalog.ScheduledOn)
		if err := s.Write(bg, []store.Record{r}); err != nil {
			t.Fatal(err)
		}
	}
	if err := s.Retain(bg, h); err != nil {
		t.Fatal(err)
	}
	if calls == 0 {
		t.Fatal("the retention committed nothing, so nothing was checked")
	}
}

// An error from the commit of a horizon stops the store from writing and retaining
// until it is reopened, as an error from a record commit does, whether or not the
// commit landed; reads continue, and the horizon is published only if the database
// shows it.
func TestAFailedHorizonCommitStopsTheStore(t *testing.T) {
	t.Parallel()
	h := t0.Add(time.Minute)
	for name, tc := range map[string]struct {
		before, after func() error
		published     bool
	}{
		"the commit did not land": {before: func() error { return errCommit }},
		"the commit landed":       {after: func() error { return errCommit }, published: true},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			s := openMem(t, Options{beforeHorizonApply: tc.before, afterHorizonApply: tc.after})
			if err := s.Write(bg, []store.Record{edgeRecord(1, "p", t0, lifecycle.Observe, 0), edgeRecord(2, "p", t0.Add(time.Hour), lifecycle.Observe, 0)}); err != nil {
				t.Fatal(err)
			}
			before := dump(t, s)
			if err := s.Retain(bg, h); !errors.Is(err, errCommit) {
				t.Fatalf("Retain = %v, want an error wrapping the commit's", err)
			}
			// Nothing was rewritten: the record before the horizon is still there,
			// and no baseline was written.
			if after := dump(t, s); !slices.Equal(after, before) {
				t.Errorf("the failed Retain changed the data keys:\n got %v\nwant %v", after, before)
			}
			if got := !s.Horizon().IsZero(); got != tc.published {
				t.Errorf("horizon published = %v (%v), want %v", got, s.Horizon(), tc.published)
			}
			s.beforeHorizonApply, s.afterHorizonApply = nil, nil
			if err := s.Retain(bg, h.Add(time.Hour)); err == nil {
				t.Error("a Retain after a failed horizon commit succeeded")
			}
			if err := s.Write(bg, []store.Record{edgeRecord(3, "p", t0.Add(2*time.Hour), lifecycle.Observe, 0)}); err == nil {
				t.Error("a Write after a failed horizon commit succeeded")
			}
			if got, err := s.Neighbors(bg, podFP, store.Forward, t0.Add(90*time.Minute), store.Current(catalog.L2)); err != nil || len(got) != 1 {
				t.Errorf("a read after a failed horizon commit = %v, %v; want the edge", got, err)
			}
		})
	}
}
