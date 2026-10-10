package k8scluster_test

import (
	"context"
	"reflect"
	"testing"
	"time"

	"github.com/lotannauo/toposhift/internal/catalog"
	"github.com/lotannauo/toposhift/internal/coalesce"
	"github.com/lotannauo/toposhift/internal/identity"
	"github.com/lotannauo/toposhift/internal/ingest/k8scluster"
	"github.com/lotannauo/toposhift/internal/lifecycle"
	"github.com/lotannauo/toposhift/internal/store"
	"github.com/lotannauo/toposhift/internal/store/memstore"
)

var fixtureRuns = []string{"run1.jsonl", "run2.jsonl"}

// storeFrom sequences the records of the batches in order, passes each through
// the coalescer, and writes the result of each batch to a memory store.
func storeFrom(t *testing.T, batches [][]store.Record) *memstore.Store {
	t.Helper()
	s, err := memstore.Open(memstore.Options{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.Close() })
	co, err := coalesce.New(coalesce.DefaultConfig())
	if err != nil {
		t.Fatal(err)
	}
	seq := uint64(0)
	for _, batch := range batches {
		var out []store.Record
		for _, r := range batch {
			out = co.Add(r, out)
		}
		for i := range out {
			seq++
			out[i].Seq = seq
		}
		if err := s.Write(context.Background(), out); err != nil {
			t.Fatal(err)
		}
	}
	return s
}

// Properties of the translation of the fixtures, with the producer's clock
// trusted and with it distrusted.
func TestProperties(t *testing.T) {
	for _, untrusted := range []bool{false, true} {
		mode := "trusted"
		var opts []func(*k8scluster.Config)
		if untrusted {
			mode = "untrusted"
			opts = append(opts, distrust)
		}
		for _, name := range fixtureRuns {
			t.Run(mode+"/"+name, func(t *testing.T) {
				in := loadFixture(t, name)
				res := translateAll(t, newTranslator(t, opts...), in, untrusted)

				// Every record passes the store's own validation.
				seq := uint64(0)
				for i, r := range res {
					for _, rec := range r.Records {
						seq++
						rec.Seq = seq
						if err := rec.Validate(); err != nil {
							t.Errorf("%s: %v", in[i].Name, err)
						}
					}
				}
				if seq == 0 {
					t.Fatalf("%s gave no records", name)
				}

				// Translating twice gives the same results.
				again := translateAll(t, newTranslator(t, opts...), in, untrusted)
				if !reflect.DeepEqual(res, again) {
					t.Errorf("two translators disagree")
				}

				// No Observe lapses before its instant plus its TTL: what it asserts
				// reaches the instant of the record, so that the TTL is counted from
				// there.
				for i, r := range res {
					instant := instantOf(in[i].Rec)
					if untrusted {
						instant = receivedFor(in[i].Rec)
					}
					for _, rec := range r.Records {
						if rec.Kind != lifecycle.Observe {
							continue
						}
						last := rec.EventTime
						if rec.Through.After(last) {
							last = rec.Through
						}
						if lapse := last.Add(rec.TTL); lapse.Before(instant.Add(rec.TTL)) {
							t.Errorf("%s: Observe of %v lapses at %s, before its instant %s plus its TTL", in[i].Name, rec.Subject, lapse, instant)
						}
					}
				}

				// Through the coalescer into a store, each node that is deleted is
				// alive from its first record through its delete, and not after.
				batches := make([][]store.Record, len(res))
				for i, r := range res {
					batches[i] = r.Records
				}
				s := storeFrom(t, batches)
				first := map[identity.Fingerprint]time.Time{}
				deleted := map[identity.Fingerprint]time.Time{}
				for _, rec := range records(res) {
					if rec.Subject.Kind != store.SubjectEntity || rec.Subject.A.Type() != catalog.K8sNode {
						continue
					}
					if _, ok := first[rec.Subject.A]; !ok && rec.Kind == lifecycle.Observe {
						first[rec.Subject.A] = rec.EventTime
					}
					if rec.Kind == lifecycle.Delete {
						deleted[rec.Subject.A] = rec.EventTime
					}
				}
				if len(deleted) == 0 {
					t.Fatalf("%s deletes no node", name)
				}
				ctx := context.Background()
				alive := func(f identity.Fingerprint, at time.Time) bool {
					ok, err := s.Alive(ctx, f, at, store.Current(catalog.L2))
					if err != nil {
						t.Fatal(err)
					}
					return ok
				}
				for f, end := range deleted {
					born, ok := first[f]
					if !ok {
						continue // deleted without having been seen: nothing to be alive
					}
					if alive(f, born.Add(-time.Nanosecond)) {
						t.Errorf("node %v alive before its first record", f)
					}
					checked := 0
					for at := born; at.Before(end); at = at.Add(30 * time.Second) {
						checked++
						if !alive(f, at) {
							t.Errorf("node %v not alive at %s, between its first record (%s) and its delete (%s)", f, at.Format(time.RFC3339Nano), born.Format(time.RFC3339Nano), end.Format(time.RFC3339Nano))
						}
					}
					if !alive(f, end.Add(-time.Nanosecond)) {
						t.Errorf("node %v not alive just before its delete", f)
					}
					if alive(f, end) || alive(f, end.Add(time.Hour)) {
						t.Errorf("node %v alive at or after its delete", f)
					}
					if checked < 2 {
						t.Errorf("node %v: only %d samples between %s and %s", f, checked, born, end)
					}
				}
			})
		}
	}
}
