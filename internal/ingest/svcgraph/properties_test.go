package svcgraph_test

import (
	"go/ast"
	"go/parser"
	"go/token"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/lotannauo/toposhift/internal/catalog"
	"github.com/lotannauo/toposhift/internal/ingest/svcgraph"
	"github.com/lotannauo/toposhift/internal/lifecycle"
	"github.com/lotannauo/toposhift/internal/store"
)

// Every record of every scripted run is a record a store accepts, an Observe of this
// producer with its one TTL, with no Through and no payload, in the layer the catalog
// records for its subject: depends_on in L3, not L2, and a service in L3.
func TestEveryRecordIsWhatTheCatalogAndTheStoreExpect(t *testing.T) {
	t.Parallel()
	cat := catalog.Default()
	rel, ok := cat.Relation(catalog.DependsOn)
	if !ok || rel.Layer() != catalog.L3 {
		t.Fatalf("the catalog's depends_on is in %v", rel.Layer())
	}
	total, edges, services := 0, 0, 0
	for seed := uint64(1); seed <= 60; seed++ {
		sc := genScript(seed)
		tr := newTranslator(t, maxSeries(sc.maxSeries))
		for _, s := range sc.steps {
			if s.renew {
				tr = newTranslator(t, maxSeries(sc.maxSeries))
			}
			res := tr.Translate(resource(s.points...), s.received)
			if s.commit {
				tr.Commit()
			} else {
				tr.Rollback()
			}
			for _, r := range res.Records {
				total++
				if err := r.Validate(); err != nil {
					t.Fatalf("seed %d: %v", seed, err)
				}
				if err := store.WritableProducer(r.Producer); err != nil {
					t.Fatalf("seed %d: %v", seed, err)
				}
				switch {
				case r.Kind != lifecycle.Observe:
					t.Fatalf("seed %d: a %v record", seed, r.Kind)
				case !r.Through.IsZero() || len(r.Payload) != 0 || r.Seq != 0 || r.Boot != "":
					t.Fatalf("seed %d: through %v payload %q seq %d boot %q", seed, r.Through, r.Payload, r.Seq, r.Boot)
				case r.TTL != expiry || r.Producer != testProducer || r.EventTimeBasis != store.BasisObserved:
					t.Fatalf("seed %d: ttl %s producer %s basis %s", seed, r.TTL, r.Producer, r.EventTimeBasis)
				}
				if r.Subject.Kind == store.SubjectEdge {
					edges++
					rl, ok := cat.Relation(r.Subject.Relation)
					if !ok || r.Subject.Relation != catalog.DependsOn || r.Layer != rl.Layer() || r.Layer != catalog.L3 {
						t.Fatalf("seed %d: an edge %s in %s", seed, r.Subject.Relation, r.Layer)
					}
					if !rl.Allows(r.Subject.A.Type(), r.Subject.B.Type()) {
						t.Fatalf("seed %d: the catalog does not allow %s -> %s", seed, r.Subject.A.Type(), r.Subject.B.Type())
					}
				} else {
					services++
					e, ok := cat.Entity(r.Subject.A.Type())
					if !ok || r.Subject.A.Type() != catalog.Service || r.Layer != e.Layer() || r.Layer != catalog.L3 {
						t.Fatalf("seed %d: an entity %s in %s", seed, r.Subject.A.Type(), r.Layer)
					}
				}
			}
		}
	}
	if edges < 500 || services < 500 {
		t.Errorf("only %d edges and %d services were written: the check meant to be large", edges, services)
	}
	t.Logf("%d records checked", total)
}

// A translator is a function of what it was given: two made alike and fed alike give the
// same results, record for record.
func TestTwoTranslatorsGiveTheSameResults(t *testing.T) {
	t.Parallel()
	for seed := uint64(1); seed <= 30; seed++ {
		sc := genScript(seed)
		a, b := newTranslator(t, maxSeries(sc.maxSeries)), newTranslator(t, maxSeries(sc.maxSeries))
		for i, s := range sc.steps {
			if s.renew {
				a, b = newTranslator(t, maxSeries(sc.maxSeries)), newTranslator(t, maxSeries(sc.maxSeries))
			}
			ra, rb := a.Translate(resource(s.points...), s.received), b.Translate(resource(s.points...), s.received)
			if !reflect.DeepEqual(ra, rb) {
				t.Fatalf("seed %d step %d: two translators gave different results", seed, i)
			}
			if s.commit {
				a.Commit()
				b.Commit()
			} else {
				a.Rollback()
				b.Rollback()
			}
		}
	}
}

// The translator never reads a clock: no non-test file of the package names a function
// of package time that reads or waits on one.
func TestTheTranslatorNeverReadsAClock(t *testing.T) {
	t.Parallel()
	banned := map[string]bool{
		"Now": true, "Since": true, "Until": true, "Sleep": true, "After": true, "Tick": true,
		"NewTimer": true, "NewTicker": true, "AfterFunc": true,
	}
	files, err := filepath.Glob("*.go")
	if err != nil || len(files) == 0 {
		t.Fatalf("glob: %v, %d files", err, len(files))
	}
	checked := 0
	for _, name := range files {
		if strings.HasSuffix(name, "_test.go") {
			continue
		}
		checked++
		f, err := parser.ParseFile(token.NewFileSet(), name, nil, 0)
		if err != nil {
			t.Fatal(err)
		}
		ast.Inspect(f, func(n ast.Node) bool {
			sel, ok := n.(*ast.SelectorExpr)
			if !ok {
				return true
			}
			if id, ok := sel.X.(*ast.Ident); ok && id.Name == "time" && banned[sel.Sel.Name] {
				t.Errorf("%s uses time.%s", name, sel.Sel.Name)
			}
			return true
		})
	}
	if checked < 3 {
		t.Errorf("only %d source files were checked", checked)
	}
}

func TestConfigIsChecked(t *testing.T) {
	t.Parallel()
	good := svcgraph.Config{Producer: testProducer, Resolver: resolver(), Expiry: time.Hour}
	if _, err := svcgraph.New(good); err != nil {
		t.Fatal(err)
	}
	for name, mut := range map[string]func(*svcgraph.Config){
		"no producer":      func(c *svcgraph.Config) { c.Producer = "" },
		"no resolver":      func(c *svcgraph.Config) { c.Resolver = nil },
		"no expiry":        func(c *svcgraph.Config) { c.Expiry = 0 },
		"negative expiry":  func(c *svcgraph.Config) { c.Expiry = -time.Hour },
		"huge expiry":      func(c *svcgraph.Config) { c.Expiry = 400 * 24 * time.Hour },
		"negative flush":   func(c *svcgraph.Config) { c.Flush = -time.Second },
		"huge cache loop":  func(c *svcgraph.Config) { c.CacheLoop = 400 * 24 * time.Hour },
		"negative series":  func(c *svcgraph.Config) { c.MaxSeries = -1 },
		"huge flush":       func(c *svcgraph.Config) { c.Flush = 400 * 24 * time.Hour },
		"negative cleanup": func(c *svcgraph.Config) { c.CacheLoop = -1 },
	} {
		c := good
		mut(&c)
		if _, err := svcgraph.New(c); err == nil {
			t.Errorf("%s: no error", name)
		}
	}
	tr, err := svcgraph.New(good)
	if err != nil || tr.TTL() != time.Hour {
		t.Errorf("ttl %v, %v", tr.TTL(), err)
	}
}

// A point of another producer's metric must not be able to make state: only the
// counter's scope and name do.
func TestOnlyTheCounterMakesState(t *testing.T) {
	t.Parallel()
	tr := newTranslator(t)
	p := counter(edge("a", "b"), connStart, t0, 1)
	p.Metric = "other"
	send(tr, rcv(t0), p)
	if tr.Series() != 0 {
		t.Error("another metric made state")
	}
}

// The producer of a cluster whose name has the longest length the store allows is a name
// the store writes.
func TestTheLongestProducerIsWritable(t *testing.T) {
	t.Parallel()
	p := lifecycle.Producer(svcgraph.Source + "/" + strings.Repeat("c", store.MaxClusterNameLen))
	if err := store.WritableProducer(p); err != nil {
		t.Error(err)
	}
	if svcgraph.Source != "svcgraph" {
		t.Errorf("the source is %q: it is never renamed", svcgraph.Source)
	}
}
