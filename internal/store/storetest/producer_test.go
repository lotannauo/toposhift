package storetest_test

import (
	"context"
	"strings"
	"testing"

	"github.com/lotannauo/toposhift/internal/lifecycle"
	"github.com/lotannauo/toposhift/internal/store"
	"github.com/lotannauo/toposhift/internal/store/memstore"
	"github.com/lotannauo/toposhift/internal/store/storetest"
)

// lenient is a store that lets through what it should refuse: it hands the reference
// each record after fix has changed it, so a record with a producer the reference
// refuses reaches it with one it accepts. With before set, it also refuses a batch
// holding an invalid producer ahead of the reference's own checks, which puts that
// refusal before the retention horizon's; with partial set, it writes the batch one
// record at a time and reports the first refusal, which leaves the valid prefix.
type lenient struct {
	store.Store
	fix     func(store.Record) store.Record
	before  bool
	partial bool
}

func (l lenient) Write(ctx context.Context, batch []store.Record) error {
	out := make([]store.Record, len(batch))
	for i, r := range batch {
		if l.before {
			if err := store.WritableProducer(r.Producer); err != nil {
				return err
			}
		}
		if l.fix != nil {
			r = l.fix(r)
		}
		out[i] = r
	}
	if !l.partial {
		return l.Store.Write(ctx, out)
	}
	for _, r := range out {
		if err := l.Store.Write(ctx, []store.Record{r}); err != nil {
			return err
		}
	}
	return nil
}

func openReference(t *testing.T) store.Store {
	t.Helper()
	s, err := memstore.Open(memstore.Options{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.Close() })
	return s
}

func TestCheckProducerNamesAcceptsTheReference(t *testing.T) {
	t.Parallel()
	if err := storetest.CheckProducerNames(openReference(t)); err != nil {
		t.Fatal(err)
	}
}

// Each store below breaks one rule of the producer names, and the check must say so,
// for that rule.
func TestCheckProducerNamesCatchesAStoreThatBreaksARule(t *testing.T) {
	t.Parallel()
	fixIf := func(bad func(store.Record) bool) func(store.Record) store.Record {
		return func(r store.Record) store.Record {
			if bad(r) {
				r.Producer = "fixed"
			}
			return r
		}
	}
	refusable := func(r store.Record) bool { return store.WritableProducer(r.Producer) != nil }
	for name, c := range map[string]struct {
		l    lenient
		want string // a part of the check's error
	}{
		"writes a producer over the limit": {
			lenient{fix: fixIf(func(r store.Record) bool { return len(r.Producer) > store.MaxProducerLen })}, "one byte over the limit",
		},
		"writes a producer with a space": {
			lenient{fix: fixIf(func(r store.Record) bool { return strings.Contains(string(r.Producer), " ") })}, "a space",
		},
		"writes a producer with a rune outside ASCII": {
			lenient{fix: fixIf(func(r store.Record) bool {
				return strings.ContainsFunc(string(r.Producer), func(c rune) bool { return c > 0x7f })
			})}, "outside ASCII",
		},
		"writes the empty producer": {
			lenient{fix: fixIf(func(r store.Record) bool { return r.Producer == "" })}, "the empty name",
		},
		"does not check a delete": {
			lenient{fix: fixIf(func(r store.Record) bool { return r.Kind == lifecycle.Delete && refusable(r) })}, "a delete",
		},
		"does not check an edge": {
			lenient{fix: fixIf(func(r store.Record) bool { return r.Subject.Kind == store.SubjectEdge && refusable(r) })}, "an edge",
		},
		"stores the valid records of a refused batch":      {lenient{partial: true}, "refused, but"},
		"refuses an invalid producer ahead of the horizon": {lenient{before: true}, "then one with a producer over the limit"},
	} {
		c.l.Store = openReference(t)
		err := storetest.CheckProducerNames(c.l)
		if err == nil {
			t.Errorf("a store that %s passed CheckProducerNames", name)
		} else if !strings.Contains(err.Error(), c.want) {
			t.Errorf("a store that %s: err = %q, want it to contain %q", name, err, c.want)
		}
	}
}
