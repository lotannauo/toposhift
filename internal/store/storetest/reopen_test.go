package storetest_test

import (
	"slices"
	"testing"

	"github.com/lotannauo/toposhift/internal/lifecycle"
	"github.com/lotannauo/toposhift/internal/store"
	"github.com/lotannauo/toposhift/internal/store/memstore"
	"github.com/lotannauo/toposhift/internal/store/storetest"
)

var zeroPolicy = lifecycle.Policy{}

func TestCheckReopenCatchesAStoreThatForgets(t *testing.T) {
	t.Parallel()

	// Persisting in a journal is correct, so the check must pass it.
	honest := &journals{}
	if err := storetest.CheckReopen(storetest.Factory{Open: honest.open, Durable: true}); err != nil {
		t.Fatalf("a store that keeps everything fails the reopen check: %v", err)
	}

	moveRetentionsAfterWrites := func(entries []journalEntry) []journalEntry {
		var writes, retains []journalEntry
		for _, e := range entries {
			if e.retain.IsZero() {
				writes = append(writes, e)
			} else {
				retains = append(retains, e)
			}
		}
		return append(writes, retains...)
	}
	for name, damage := range map[string]*reopenDamage{
		// The records come back, but the horizon is not remembered, so a record
		// before it is accepted again.
		"forgets the retention horizon": {entries: func(entries []journalEntry) []journalEntry {
			return slices.DeleteFunc(entries, func(e journalEntry) bool { return !e.retain.IsZero() })
		}},
		// Whatever it is handed with a Seq that is not above the last is taken.
		"accepts a sequence that is not above the last": {after: func(b *broken, _ uint64) {
			b.swallow = func(err error) bool { return isInvalidNotBeforeHorizon(err) }
		}},
		"forgets the last sequence":    {after: func(b *broken, _ uint64) { b.lastSeq = 0 }},
		"reports a sequence one ahead": {after: func(b *broken, _ uint64) { b.lastSeq++ }},
		"loses the newest record": {after: func(b *broken, _ uint64) {
			kept := b.written[:len(b.written)-1]
			if err := b.replace(cloneRecs(kept)); err != nil {
				panic(err)
			}
			b.written = kept
		}},
		// The retentions are replayed after all the writes, so the horizon's Seq
		// becomes the last LastSeq instead of the one it had.
		"forgets the horizon's Seq": {entries: moveRetentionsAfterWrites},
		// The records of the clone are not replayed, so a quarantined host is not
		// quarantined any more.
		"loses a quarantine": {
			entries: func(entries []journalEntry) []journalEntry {
				var out []journalEntry
				for _, e := range entries {
					e.batch = slices.DeleteFunc(cloneRecs(e.batch), func(r store.Record) bool { return r.Producer == "clone" })
					if len(e.batch) > 0 || !e.retain.IsZero() {
						out = append(out, e)
					}
				}
				return out
			},
			after: func(b *broken, lastSeq uint64) { b.lastSeq = lastSeq },
		},
	} {
		j := &journals{damage: damage}
		if err := storetest.CheckReopen(storetest.Factory{Open: j.open, Durable: true}); err == nil {
			t.Errorf("the reopen check did not notice a store that %s", name)
		}
	}
}

// A store that is not durable is not asked to come back.
func TestTheReopenCheckIsForADurableStore(t *testing.T) {
	t.Parallel()
	// A store with no files comes back empty, which the check must report.
	f := storetest.Factory{Open: func(_ string, p lifecycle.Policy) (store.Store, error) {
		return memstore.Open(memstore.Options{Policy: p})
	}}
	if err := storetest.CheckReopen(f); err == nil {
		t.Error("a store that forgets everything passed the reopen check")
	}
}

func TestReopenOptionsAreValidated(t *testing.T) {
	t.Parallel()
	w := storetest.Workloads()[0]
	open := func(t *testing.T) store.Store {
		t.Helper()
		m, err := memstore.Open(memstore.Options{})
		if err != nil {
			t.Fatal(err)
		}
		return m
	}
	// Reopening needs somewhere to reopen to, and valid fractions.
	if err := storetest.Check(open(t), w, storetest.Options{ReopenAt: []float64{0.5}}); err == nil {
		t.Error("ReopenAt without Reopen was accepted")
	}
	for _, bad := range [][]float64{{0}, {1}, {0.6, 0.4}} {
		opts := storetest.Options{ReopenAt: bad, Reopen: func(s store.Store) (store.Store, error) { return s, nil }}
		if err := storetest.Check(open(t), w, opts); err == nil {
			t.Errorf("ReopenAt %v was accepted", bad)
		}
	}
	// A Reopen that leaves the old handle open is reported.
	opts := storetest.Options{ReopenAt: []float64{0.5}, Reopen: func(s store.Store) (store.Store, error) { return s, nil }}
	if err := storetest.Check(open(t), w, opts); err == nil {
		t.Error("a Reopen that returned the store it was given, still open, was accepted")
	}
}
