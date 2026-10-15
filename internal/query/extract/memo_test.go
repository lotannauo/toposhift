package extract_test

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"testing"

	"pgregory.net/rapid"

	"github.com/lotannauo/toposhift/internal/query/coresidency"
	"github.com/lotannauo/toposhift/internal/query/extract"
	"github.com/lotannauo/toposhift/internal/query/interval"
	"github.com/lotannauo/toposhift/internal/store"
	"github.com/lotannauo/toposhift/internal/store/memstore"
)

// withoutStoreReads is r with StoreReads zeroed, the one field the memo adds.
func withoutStoreReads(r extract.Result) extract.Result {
	r.StoreReads = coresidency.Reads{}
	return r
}

// sameErr reports whether two extractions failed alike: both succeeded, or
// both failed with the same sentinel or context error. Text is not compared.
func sameErr(a, b error) bool {
	if (a == nil) != (b == nil) {
		return false
	}
	for _, target := range []error{store.ErrInvalid, store.ErrBeforeHorizon, interval.ErrRetained, context.Canceled, context.DeadlineExceeded} {
		if errors.Is(a, target) != errors.Is(b, target) {
			return false
		}
	}
	return true
}

// atMost reports whether each count of a is at most b's, and a has no Calls.
func atMost(a, b coresidency.Reads) bool {
	return a.Calls == 0 && a.Windows <= b.Windows && a.Alive <= b.Alive && a.EntityWindows <= b.EntityWindows
}

// atLeast reports whether each count of a is at least b's.
func atLeast(a, b coresidency.Reads) bool {
	return a.Windows >= b.Windows && a.Alive >= b.Alive && a.EntityWindows >= b.EntityWindows
}

// memoCaps are the sizes a differential also runs with, below any real read.
var memoCaps = []int{1, 2, 5, 20}

// checkMemo is the memo's contract for one extraction of st:
//   - the answer, Reads and every Find's Reads equal those of an extractor with
//     no memo, over a recording store that sees every request;
//   - every Find of the answer equals a Find run alone at the same token;
//   - the instants and the token of each method are constant, and the reads that
//     reach the store, with a memo that holds everything, are exactly the
//     distinct reads of the recording;
//   - StoreReads is at most Reads, and Calls is 0;
//   - a second Extract on the same extractor reads the same;
//   - with a small memo the answer is unchanged and StoreReads is between the
//     exact count and Reads.
func checkMemo(t tb, st store.Store, seed extract.Seed, w interval.Window, o extract.Options) extract.Result {
	t.Helper()
	recNo, recMemo := &recorder{Store: st}, &recorder{Store: st}
	no, errNo := extract.NewExtractorNoMemo(recNo).Extract(ctx, seed, w, o)
	ex := extract.NewExtractor(recMemo)
	got, errGot := ex.Extract(ctx, seed, w, o)
	if !sameErr(errNo, errGot) {
		t.Fatalf("seed %+v: without the memo %v, with it %v", seed, errNo, errGot)
	}
	if errGot != nil {
		if (got.StoreReads != coresidency.Reads{}) {
			t.Fatalf("a failed extraction has StoreReads %+v", got.StoreReads)
		}
		return got
	}
	// The recording below the extractor without a memo is every request.
	want := recNo.counts()
	if reads := no.Reads; reads.Windows != want.Windows || reads.Alive != want.Alive || reads.EntityWindows != want.EntityWindows {
		t.Fatalf("seed %+v: Reads %+v, but %+v reached a store with no memo", seed, reads, want)
	}
	if no.StoreReads != want {
		t.Fatalf("seed %+v: with no memo StoreReads %+v, want every request %+v", seed, no.StoreReads, want)
	}
	if recNo.writes != 0 || recMemo.writes != 0 || recNo.closes != 0 || recMemo.closes != 0 {
		t.Fatalf("a write or a close reached the store")
	}
	// The instants and the token are constant, and the reads that reach the
	// store are exactly the distinct reads of the recording.
	if d := constant(recNo.reads); d != "" {
		t.Fatalf("seed %+v: %s", seed, d)
	}
	f := formulaF(recNo.reads)
	if got.StoreReads != f {
		t.Fatalf("seed %+v: StoreReads %+v, the distinct reads of the recording are %+v", seed, got.StoreReads, f)
	}
	if recMemo.counts() != got.StoreReads {
		t.Fatalf("seed %+v: StoreReads %+v, but %+v reached the store", seed, got.StoreReads, recMemo.counts())
	}
	// Nothing else differs, Reads and the Finds' Reads included.
	if !reflect.DeepEqual(withoutStoreReads(got), withoutStoreReads(no)) {
		t.Fatalf("seed %+v: the answer with the memo differs from the answer without: %s", seed, sameResult(got, no))
	}
	// The reads that reached the store are a part of the requests.
	if !atMost(got.StoreReads, got.Reads) {
		t.Fatalf("seed %+v: StoreReads %+v above Reads %+v", seed, got.StoreReads, got.Reads)
	}
	// Each Find is what a Find alone gives at the same token.
	pol := o.Policy
	if pol == (extract.Policy{}) {
		pol = extract.DefaultPolicy()
	}
	finder := coresidency.NewFinder(st)
	for _, fr := range got.PodResults {
		alone, err := finder.Find(ctx, fr.Seed, interval.Window{From: got.From, To: got.To, AsOf: got.Token},
			coresidency.Options{NodeOnly: o.NodeOnly, Policy: pol.CoResidency})
		if err != nil {
			t.Fatalf("seed %+v: Find %s alone: %v", seed, fr.Seed, err)
		}
		if !reflect.DeepEqual(alone, fr) {
			t.Fatalf("seed %+v: Find %s differs from the same Find alone: %s", seed, fr.Seed,
				diffLines("entries", describeEntries(fr.CoResidents), describeEntries(alone.CoResidents)))
		}
	}
	// The memo is one extraction's.
	again, err := ex.Extract(ctx, seed, w, o)
	if err != nil || !reflect.DeepEqual(again, got) {
		t.Fatalf("seed %+v: a second Extract on the extractor gave %v, StoreReads %+v then %+v", seed, err, got.StoreReads, again.StoreReads)
	}
	if recMemo.counts().Windows != 2*got.StoreReads.Windows {
		t.Fatalf("seed %+v: the second Extract read %d Windows, want %d", seed, recMemo.counts().Windows-got.StoreReads.Windows, got.StoreReads.Windows)
	}
	// With a small memo the answers are the same and more reads reach the store.
	for _, c := range memoCaps {
		small, err := extract.NewExtractorCap(st, c).Extract(ctx, seed, w, o)
		if err != nil {
			t.Fatalf("seed %+v, memo size %d: %v", seed, c, err)
		}
		if !reflect.DeepEqual(withoutStoreReads(small), withoutStoreReads(no)) {
			t.Fatalf("seed %+v, memo size %d: the answer differs: %s", seed, c, sameResult(small, no))
		}
		if !atLeast(small.StoreReads, f) || !atMost(small.StoreReads, small.Reads) {
			t.Fatalf("seed %+v, memo size %d: StoreReads %+v, want between %+v and Reads %+v", seed, c, small.StoreReads, f, small.Reads)
		}
	}
	return got
}

// expectStoreReads is expectReads for the reads that reached the store: it
// compares with equality, and Calls is 0.
func expectStoreReads(t *testing.T, got coresidency.Reads, windows, alive, ew int) {
	t.Helper()
	if want := (coresidency.Reads{Windows: windows, Alive: alive, EntityWindows: ew}); got != want {
		t.Errorf("store reads %+v, want %+v", got, want)
	}
}

// TestMemoDifferential is checkMemo over the drawn worlds, every seed, at
// every drawn token.
func TestMemoDifferential(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		c := drawCase(t)
		for _, s := range c.seeds() {
			checkMemo(t, c.st, s.seed, c.w, s.o)
		}
	})
}

// TestMemoCancelledContext: a context cancelled before the call
// fails the same with the memo, and a context cancelled by a read mid-run fails
// the extraction, whatever the memo holds.
func TestMemoCancelledContext(t *testing.T) {
	st, _ := scenarioQA(t, memstore.Options{})
	seed := extract.Anchored{Service: S, Host: H, At: hm(10, 15)}
	for _, mk := range []struct {
		name string
		ex   func(store.Store) *extract.Extractor
	}{{"memo", extract.NewExtractor}, {"no memo", extract.NewExtractorNoMemo}} {
		t.Run(mk.name+" before", func(t *testing.T) {
			c, cancel := context.WithCancel(ctx)
			cancel()
			if _, err := mk.ex(st).Extract(c, seed, w30, extract.Options{}); !errors.Is(err, context.Canceled) {
				t.Errorf("got %v, want context.Canceled", err)
			}
		})
		// Cancel at each Window the store sees: the run fails, never partly.
		for at := 1; at <= 11; at++ {
			t.Run(fmt.Sprintf("%s during Window %d", mk.name, at), func(t *testing.T) {
				c, cancel := context.WithCancel(ctx)
				defer cancel()
				sp := &spy{Store: st, onWindow: func(n int) {
					if n == at {
						cancel()
					}
				}}
				res, err := mk.ex(sp).Extract(c, seed, w30, extract.Options{})
				if !errors.Is(err, context.Canceled) || res.Token != 0 || res.Members != nil {
					t.Errorf("got %v and %+v, want context.Canceled and a zero result", err, res)
				}
			})
		}
	}
}

// TestMemoRetentionMidRun: a Retain that lands while the extraction runs, with a
// horizon after the window's start, fails it with and without the memo, in the
// same way, even where every later read would be a hit: a kept answer is never
// served past a horizon, and the store refuses the read.
func TestMemoRetentionMidRun(t *testing.T) {
	seed := extract.Anchored{Service: S, Host: H, At: hm(10, 15)}
	for _, mk := range []struct {
		name string
		ex   func(store.Store) *extract.Extractor
	}{{"memo", extract.NewExtractor}, {"no memo", extract.NewExtractorNoMemo}} {
		for _, at := range []int{2, 6, 10} {
			t.Run(fmt.Sprintf("%s retain before Window %d", mk.name, at), func(t *testing.T) {
				inner, _ := scenarioQA(t, memstore.Options{})
				sp := &spy{Store: inner, onWindow: func(n int) {
					if n == at {
						if err := inner.Retain(ctx, hm(10, 5)); err != nil {
							t.Error(err)
						}
					}
				}}
				res, err := mk.ex(sp).Extract(ctx, seed, w30, extract.Options{})
				if !errors.Is(err, store.ErrBeforeHorizon) || res.Token != 0 {
					t.Errorf("got %v and token %d, want ErrBeforeHorizon and a zero result", err, res.Token)
				}
			})
		}
	}
}

// TestMemoRetainedLayer: after a retention at the window's start the extraction
// answers, with and without the memo, as it does on a store that kept
// everything; the memo keeps nothing under a horizon, so every request reaches
// the store.
func TestMemoRetainedLayer(t *testing.T) {
	seed := extract.Anchored{Service: S, Host: H, At: hm(10, 15)}
	twin, _ := scenarioQA(t, memstore.Options{})
	want, err := extract.NewExtractor(twin).Extract(ctx, seed, w30, extract.Options{})
	if err != nil {
		t.Fatal(err)
	}
	st, _ := scenarioQA(t, memstore.Options{})
	if err := st.Retain(ctx, hm(10, 0)); err != nil {
		t.Fatal(err)
	}
	for _, ex := range []*extract.Extractor{extract.NewExtractor(st), extract.NewExtractorNoMemo(st)} {
		got, err := ex.Extract(ctx, seed, w30, extract.Options{})
		if err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(withoutStoreReads(got), withoutStoreReads(want)) {
			t.Errorf("after the retention the answer differs from the twin's: %s", sameResult(got, want))
		}
		if got.StoreReads.Windows != got.Reads.Windows || got.StoreReads.EntityWindows != got.Reads.EntityWindows || got.StoreReads.Alive != got.Reads.Alive {
			t.Errorf("StoreReads %+v under a horizon, want every request %+v", got.StoreReads, got.Reads)
		}
	}
}
