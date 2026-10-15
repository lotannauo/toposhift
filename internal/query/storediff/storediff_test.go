package storediff_test

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/cockroachdb/pebble/v2/vfs"

	"github.com/lotannauo/toposhift/internal/catalog"
	"github.com/lotannauo/toposhift/internal/identity"
	"github.com/lotannauo/toposhift/internal/lifecycle"
	"github.com/lotannauo/toposhift/internal/query/blastradius"
	"github.com/lotannauo/toposhift/internal/query/coresidency"
	"github.com/lotannauo/toposhift/internal/query/extract"
	"github.com/lotannauo/toposhift/internal/query/graphdiff"
	"github.com/lotannauo/toposhift/internal/query/interval"
	"github.com/lotannauo/toposhift/internal/query/subgraph"
	"github.com/lotannauo/toposhift/internal/store"
	"github.com/lotannauo/toposhift/internal/store/memstore"
	"github.com/lotannauo/toposhift/internal/store/pebblekv"
	"github.com/lotannauo/toposhift/internal/store/pebblestore"
	"github.com/lotannauo/toposhift/internal/store/storetest"
)

var ctx = context.Background()

// world is one generated stream, the policy entity existence is folded with, and
// the fraction of its span a retention is placed at (0 for none).
type world struct {
	name   string
	cfg    storetest.Config
	policy lifecycle.Policy
	retain float64
	// boundary, if set, replaces the generated stream with the boundaries world,
	// written one record per batch.
	boundary bool
}

// worlds are the generated cluster streams of storetest, which the query
// packages' own tests read: heartbeats, runs, a second producer on placements,
// late records, reboots and clones; and the boundaries world. Each is read
// without a retention and with one. The seeds are fixed.
func worlds() []world {
	var base []world
	for _, seed := range []uint64{1, 2} {
		c := storetest.Tiny()
		c.Seed = seed
		base = append(base, world{name: fmt.Sprintf("tiny seed %d", seed), cfg: c})
	}
	c := storetest.Tiny()
	c.Seed, c.Runs, c.ConfirmProbability, c.LateProbability = 3, true, 0.5, 0.3
	base = append(base, world{name: "runs, confirmed, late", cfg: c})
	c = storetest.Tiny()
	c.Seed, c.HeartbeatInterval = 4, 0
	base = append(base, world{name: "watch mode", cfg: c})
	c = storetest.Tiny()
	c.Seed, c.Runs, c.RebootProbability, c.CloneProbability = 5, true, 0.2, 0.5
	base = append(base, world{name: "reboots and clones", cfg: c, policy: storetest.QuarantinePolicy()})
	base = append(base, world{name: "boundaries", boundary: true})
	var out []world
	for _, w := range base {
		out = append(out, w)
		w.name += " retained"
		w.retain = 0.45
		if w.boundary {
			w.retain = 0.15 // at F, the boundary
		}
		out = append(out, w)
	}
	return out
}

// boundaryStart is where the boundaries world begins, and boundaryAt its
// boundary F: three minutes later, where the first window starts.
var (
	boundaryStart = time.Date(2026, 1, 1, 10, 0, 0, 0, time.UTC)
	boundaryAt    = boundaryStart.Add(3 * time.Minute)
)

// boundaries is the world the generated streams rarely reach: references whose
// deadline is exactly F, one continued at F by another producer and one not,
// and a record a nanosecond before F, after which a store that places a
// checkpoint after every record without a lag places one at exactly F. Its span
// is 20 minutes, so a retention at 0.15 of it is at F.
func boundaries() ([]store.Record, []identity.Fingerprint) {
	fp := func(typ catalog.EntityType, n byte) identity.Fingerprint {
		var h [identity.FingerprintBytes]byte
		h[0], h[1] = 0xb0, n
		f, err := identity.FingerprintFromHash(typ, h)
		if err != nil {
			panic(err)
		}
		return f
	}
	n, p1, p2, p3 := fp(catalog.K8sNode, 1), fp(catalog.K8sPod, 1), fp(catalog.K8sPod, 2), fp(catalog.K8sPod, 3)
	ents := []identity.Fingerprint{n, p1, p2, p3}
	var recs []store.Record
	add := func(sub store.Subject, p lifecycle.Producer, at time.Time, ttl time.Duration) {
		l := catalog.L2
		if sub.Kind == store.SubjectEntity {
			e, _ := catalog.Default().Entity(sub.A.Type())
			l = e.Layer()
		}
		recs = append(recs, store.Record{
			Layer: l, Subject: sub, Producer: p, EventTime: at, Seq: uint64(len(recs) + 1), Kind: lifecycle.Observe, TTL: ttl,
			EventTimeBasis: store.BasisObserved, Payload: []byte(fmt.Sprintf("%s %d", p, len(recs)+1)),
		})
	}
	for _, e := range ents {
		add(store.EntitySubject(e), "k8sobjects", boundaryStart, 0)
	}
	edge := func(p identity.Fingerprint) store.Subject { return store.EdgeSubject(p, n, catalog.ScheduledOn) }
	add(edge(p1), "kubelet", boundaryStart, 3*time.Minute) // deadline F, continued at F
	add(edge(p3), "kubelet", boundaryStart, 3*time.Minute) // deadline F, not continued
	add(edge(p2), "k8sobjects", boundaryAt.Add(-time.Nanosecond), 0)
	add(edge(p1), "k8sobjects", boundaryAt, 0)
	add(edge(p3), "k8sobjects", boundaryAt.Add(5*time.Minute), 0)
	return recs, ents
}

// variant is a way of opening pebblestore.
type variant struct {
	name       string
	checkpoint pebblestore.CheckpointOptions
	retention  pebblestore.RetentionMode
}

// variants keep the checkpoint gaps small, so the seed walks stop at
// checkpoints, and retain both ways: the Background pass may not have reached a
// prefix when it is read.
func variants() []variant {
	return []variant{
		{"a checkpoint per record, synchronous", pebblestore.CheckpointOptions{On: true, KMin: 1}, pebblestore.Synchronous},
		{"k8 with a lag, background", pebblestore.CheckpointOptions{On: true, KMin: 8, Alpha: 1, Lag: time.Minute}, pebblestore.Background},
		{"the default, synchronous", pebblestore.DefaultCheckpoints(), pebblestore.Synchronous},
	}
}

func openPebble(t testing.TB, v variant, p lifecycle.Policy, rec pebblestore.Recorder) *pebblestore.Store {
	t.Helper()
	o := pebblestore.DefaultOptions()
	o.Tuning = pebblekv.TinyTuning()
	o.Sync, o.SettleRetention = false, false
	o.FS = vfs.NewMem()
	o.Policy, o.Retention, o.Recorder = p, v.retention, rec
	ck := v.checkpoint
	o.Checkpoints = &ck
	s, err := pebblestore.Open("db", o)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.Close() })
	return s
}

// filled is a world written to a store, and retained if the world says so.
type filled struct {
	start, end time.Time
	h          time.Time // the horizon, zero for none
	entities   []identity.Fingerprint
}

func fill(t testing.TB, w world, stores ...store.Store) filled {
	t.Helper()
	var f filled
	var recs []store.Record
	batch := 64
	if w.boundary {
		recs, f.entities = boundaries()
		f.start, f.end, batch = boundaryStart, boundaryStart.Add(20*time.Minute), 1
	} else {
		g, err := storetest.NewGenerator(w.cfg)
		if err != nil {
			t.Fatal(err)
		}
		recs, f.entities, f.start, f.end = g.All(), g.Entities(), g.Start(), g.End()
	}
	for _, st := range stores {
		for i := 0; i < len(recs); i += batch {
			if err := st.Write(ctx, recs[i:min(i+batch, len(recs))]); err != nil {
				t.Fatal(err)
			}
		}
	}
	if w.retain > 0 {
		f.h = f.start.Add(time.Duration(w.retain * float64(f.end.Sub(f.start)))).Truncate(time.Second)
		for _, st := range stores {
			if err := st.Retain(ctx, f.h); err != nil {
				t.Fatal(err)
			}
		}
	}
	return f
}

// of lists a world's entities of one type, at most n.
func (f filled) of(typ catalog.EntityType, n int) []identity.Fingerprint {
	var out []identity.Fingerprint
	for _, e := range f.entities {
		if e.Type() == typ && len(out) < n {
			out = append(out, e)
		}
	}
	return out
}

// windows are the windows asked: from the horizon (or near the start) and a
// nanosecond after it, one in the middle, and one that runs past the end.
func (f filled) windows() []interval.Window {
	first := f.start.Add(3 * time.Minute)
	if !f.h.IsZero() {
		first = f.h
	}
	mid := f.start.Add(f.end.Sub(f.start) * 2 / 3)
	return []interval.Window{
		{From: first, To: first.Add(7 * time.Minute), AsOf: store.Latest},
		{From: first.Add(time.Nanosecond), To: first.Add(4 * time.Minute), AsOf: store.Latest},
		{From: mid, To: f.end.Add(10 * time.Minute), AsOf: store.Latest},
	}
}

// errClass names what kind of error err is, so two stores' errors can be
// compared: a store's message differs, its meaning may not.
func errClass(err error) string {
	switch {
	case err == nil:
		return "none"
	case errors.Is(err, interval.ErrRetained):
		return "retained"
	case errors.Is(err, store.ErrBeforeHorizon):
		return "before the horizon"
	case errors.Is(err, store.ErrInvalid):
		return "invalid"
	}
	return "other: " + err.Error()
}

// coverage counts what the comparisons met, so the test can require that they
// met answers, refusals and the store's checkpoints, not only empty agreement.
type coverage struct {
	mu       sync.Mutex
	answered int
	pieces   int // elements with a piece in package interval's answers
	byClass  map[string]int
}

func (c *coverage) elements(els ...[]interval.Element) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, list := range els {
		for _, el := range list {
			if len(el.Pieces) > 0 {
				c.pieces++
			}
		}
	}
}

func (c *coverage) note(class string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.byClass == nil {
		c.byClass = map[string]int{}
	}
	c.byClass[class]++
	if class == "none" {
		c.answered++
	}
}

// same requires two stores' answers to a query to be equal, and their errors to
// be of one class.
func same[T any](t *testing.T, cov *coverage, what string, got T, gerr error, want T, werr error) {
	t.Helper()
	if gc, wc := errClass(gerr), errClass(werr); gc != wc {
		t.Fatalf("%s: pebblestore %s (%v), memstore %s (%v)", what, gc, gerr, wc, werr)
	}
	if werr == nil && !reflect.DeepEqual(got, want) {
		t.Fatalf("%s: pebblestore\n%+v\nmemstore\n%+v", what, got, want)
	}
	cov.note(errClass(werr))
}

// TestQueriesAgreeAcrossStores puts every query to memstore and pebblestore over
// the same worlds and requires the same answers, reads and errors.
func TestQueriesAgreeAcrossStores(t *testing.T) {
	ws, vs := worlds(), variants()
	if testing.Short() {
		ws, vs = []world{ws[1], ws[len(ws)-1]}, vs[:2]
	}
	cov := &coverage{}
	rec := newRecorder()
	t.Cleanup(func() {
		if t.Failed() {
			return
		}
		t.Logf("answers %d, %d elements with pieces, by class %v; seed walks: %v", cov.answered, cov.pieces, cov.byClass, rec.summary())
		if cov.pieces == 0 || cov.byClass["retained"] == 0 || cov.byClass["before the horizon"] != 0 {
			t.Errorf("the comparisons met too little, or a refusal they should not: %+v", cov.byClass)
		}
		for _, name := range []string{"read.checkpoint_hits", "read.baseline_entries_decoded", "read.seed_payload_steps"} {
			if rec.counter(name) == 0 {
				t.Errorf("no seed walk counted %s", name)
			}
		}
	})
	for _, w := range ws {
		for _, v := range vs {
			t.Run(w.name+"/"+v.name, func(t *testing.T) {
				t.Parallel()
				ref, err := memstore.Open(memstore.Options{Policy: w.policy})
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() { _ = ref.Close() })
				peb := openPebble(t, v, w.policy, rec)
				f := fill(t, w, ref, peb)
				for _, win := range f.windows() {
					compareAll(t, cov, ref, peb, f, win)
				}
				if v.retention == pebblestore.Background {
					// And once the pass has rewritten every prefix.
					if err := peb.Instrument().WaitRetained(ctx); err != nil {
						t.Fatal(err)
					}
					compareAll(t, cov, ref, peb, f, f.windows()[0])
				}
			})
		}
	}
}

func compareAll(t *testing.T, cov *coverage, ref, peb store.Store, f filled, w interval.Window) {
	t.Helper()
	label := func(what string) string {
		return fmt.Sprintf("%s over [%s, %s)", what, w.From.Sub(f.start), w.To.Sub(f.start))
	}
	pods, nodes, hosts := f.of(catalog.K8sPod, 4), f.of(catalog.K8sNode, 3), f.of(catalog.Host, 3)
	services := f.of(catalog.Service, 2)

	// Package interval.
	for _, o := range []interval.Options{{}, {Raw: true}, {Describe: true}} {
		gt, g, gerr := interval.NewReader(peb, o).Entities(ctx, f.entities, w)
		wt, wn, werr := interval.NewReader(ref, o).Entities(ctx, f.entities, w)
		same(t, cov, label(fmt.Sprintf("Entities %+v", o)), struct {
			T uint64
			E []interval.Element
		}{gt, g}, gerr, struct {
			T uint64
			E []interval.Element
		}{wt, wn}, werr)
		cov.elements(wn)
		for _, d := range []store.Direction{store.Forward, store.Reverse} {
			gt, g, gerr := interval.NewReader(peb, o).EdgesBatch(ctx, f.entities, d, w)
			wt, wn, werr := interval.NewReader(ref, o).EdgesBatch(ctx, f.entities, d, w)
			same(t, cov, label(fmt.Sprintf("EdgesBatch %s %+v", d, o)), struct {
				T uint64
				E [][]interval.Element
			}{gt, g}, gerr, struct {
				T uint64
				E [][]interval.Element
			}{wt, wn}, werr)
			cov.elements(wn...)
		}
	}

	// Co-residency, and its subgraph.
	for _, p := range pods {
		for _, o := range []coresidency.Options{{}, {NodeOnly: true}} {
			g, gerr := coresidency.NewFinder(peb).Find(ctx, p, w, o)
			wn, werr := coresidency.NewFinder(ref).Find(ctx, p, w, o)
			same(t, cov, label("Find "+p.String()), g, gerr, wn, werr)
			if werr == nil {
				gs, gerr := subgraph.FromCoResidency(ctx, peb, g, subgraph.ReadPolicy{})
				ws, werr := subgraph.FromCoResidency(ctx, ref, wn, subgraph.ReadPolicy{})
				same(t, cov, label("FromCoResidency "+p.String()), gs, gerr, ws, werr)
			}
		}
	}

	// Blast radius and Around, and the subgraph of an impact.
	for _, seed := range append(slices.Clone(hosts), nodes...) {
		g, gerr := blastradius.NewWalker(peb).Impact(ctx, seed, w, blastradius.Options{})
		wn, werr := blastradius.NewWalker(ref).Impact(ctx, seed, w, blastradius.Options{})
		same(t, cov, label("Impact "+seed.String()), g, gerr, wn, werr)
		if werr == nil {
			gs, gerr := subgraph.FromBlastRadius(ctx, peb, g, subgraph.ReadPolicy{})
			ws, werr := subgraph.FromBlastRadius(ctx, ref, wn, subgraph.ReadPolicy{})
			same(t, cov, label("FromBlastRadius "+seed.String()), gs, gerr, ws, werr)
		}
	}
	for _, p := range pods[:min(2, len(pods))] {
		for _, o := range []blastradius.Options{{}, {ExpandFabric: true}} {
			g, gerr := blastradius.NewWalker(peb).Around(ctx, p, w, o)
			wn, werr := blastradius.NewWalker(ref).Around(ctx, p, w, o)
			same(t, cov, label("Around "+p.String()), g, gerr, wn, werr)
		}
	}

	// Extraction, both seeds, and its subgraph.
	var seeds []extract.Seed
	for _, s := range services {
		seeds = append(seeds, extract.Exploratory{Service: s})
		for _, h := range hosts[:min(1, len(hosts))] {
			seeds = append(seeds, extract.Anchored{Service: s, Host: h, At: w.From.Add(time.Minute)})
		}
	}
	for _, seed := range seeds {
		g, gerr := extract.NewExtractor(peb).Extract(ctx, seed, w, extract.Options{})
		wn, werr := extract.NewExtractor(ref).Extract(ctx, seed, w, extract.Options{})
		same(t, cov, label(fmt.Sprintf("Extract %+v", seed)), g, gerr, wn, werr)
		if werr == nil {
			gs, gerr := subgraph.FromExtraction(ctx, peb, g, subgraph.ReadPolicy{})
			ws, werr := subgraph.FromExtraction(ctx, ref, wn, subgraph.ReadPolicy{})
			same(t, cov, label(fmt.Sprintf("FromExtraction %+v", seed)), gs, gerr, ws, werr)
		}
	}

	// Graph diff and the scope subgraph.
	for _, scope := range [][]identity.Fingerprint{nodes[:min(1, len(nodes))], append(append(slices.Clone(nodes), pods...), hosts...)} {
		t2 := w.From.Add(5 * time.Minute)
		g, gerr := graphdiff.NewDiffer(peb).Diff(ctx, scope, w.From, t2, w.AsOf, graphdiff.Options{})
		wn, werr := graphdiff.NewDiffer(ref).Diff(ctx, scope, w.From, t2, w.AsOf, graphdiff.Options{})
		same(t, cov, label(fmt.Sprintf("Diff of %d", len(scope))), g, gerr, wn, werr)
		gs, gerr := subgraph.ReadScope(ctx, peb, scope, w, subgraph.ScopePolicy{})
		ws, werr := subgraph.ReadScope(ctx, ref, scope, w, subgraph.ScopePolicy{})
		same(t, cov, label(fmt.Sprintf("ReadScope of %d", len(scope))), gs, gerr, ws, werr)
	}
}

// recorder keeps a store's counters and samples, for the goroutines of every
// store that shares it.
type recorder struct {
	mu       sync.Mutex
	counters map[string]int64
	samples  map[string][]int64
}

func newRecorder() *recorder {
	return &recorder{counters: map[string]int64{}, samples: map[string][]int64{}}
}

func (r *recorder) Count(name string, n int64) {
	r.mu.Lock()
	r.counters[name] += n
	r.mu.Unlock()
}

func (r *recorder) Sample(name string, v int64) {
	r.mu.Lock()
	r.samples[name] = append(r.samples[name], v)
	r.mu.Unlock()
}

func (r *recorder) counter(name string) int64 {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.counters[name]
}

func (r *recorder) sampled(name string) []int64 {
	r.mu.Lock()
	defer r.mu.Unlock()
	return slices.Clone(r.samples[name])
}

func (r *recorder) summary() string {
	walks := r.sampled("read.seed_records_stepped")
	return fmt.Sprintf("%d walks, %d checkpoint hits, %d baseline entries, %d payload seeks", len(walks),
		r.counter("read.checkpoint_hits"), r.counter("read.baseline_entries_decoded"), r.counter("read.seed_payload_steps"))
}
