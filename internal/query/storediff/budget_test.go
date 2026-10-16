package storediff_test

import (
	"context"
	"encoding/json"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/lotannauo/toposhift/internal/catalog"
	"github.com/lotannauo/toposhift/internal/coalesce"
	"github.com/lotannauo/toposhift/internal/identity"
	"github.com/lotannauo/toposhift/internal/ingest/sequencer"
	"github.com/lotannauo/toposhift/internal/lifecycle"
	"github.com/lotannauo/toposhift/internal/query/blastradius"
	"github.com/lotannauo/toposhift/internal/query/coresidency"
	"github.com/lotannauo/toposhift/internal/query/interval"
	"github.com/lotannauo/toposhift/internal/store"
	"github.com/lotannauo/toposhift/internal/store/pebblestore"
)

func nth(typ catalog.EntityType, i int) identity.Fingerprint {
	var h [identity.FingerprintBytes]byte
	h[0], h[1], h[2] = 0x5e, byte(i>>8), byte(i)
	fp, err := identity.FingerprintFromHash(typ, h)
	if err != nil {
		panic(err)
	}
	return fp
}

// referenceCell writes the reference cell to st: one node with 110 live pods
// placed on it by the k8sobjects translator, refreshed by a pull every 5 minutes
// with a 15 minute TTL, written through ingest's coalescer
// (sequencer.CoalesceConfig) with a flush every coalesce.FlushPeriod, as serve
// writes them. Under that configuration a run is never extended, and every second
// pull, every 10 minutes, starts a new run, an Observe at its own instant with no
// Through; the other pulls are absorbed, and no flush writes anything, because a
// run is replaced at its next start before it comes within coalesce.FlushLead of
// its deadline. The cell fails if the coalescer writes anything else. Each record
// carries the translator's edge payload, and the pods' cadences are spread over
// the interval. It covers span from t0 and returns the node, the pods and the
// instant of the last record.
func referenceCell(t *testing.T, st store.Store, t0 time.Time, span time.Duration) (identity.Fingerprint, []identity.Fingerprint, time.Time) {
	t.Helper()
	const pods = 110
	pull, ttl := 5*time.Minute, 15*time.Minute
	every := 2 * pull
	node := nth(catalog.K8sNode, 1)
	payload, err := json.Marshal(map[string]any{
		"node_name":  "ip-10-0-12-34.eu-west-1.compute.internal",
		"resolution": map[string]any{"basis": "node-name-index", "node_interval_start": t0.Format(time.RFC3339Nano)},
	})
	if err != nil {
		t.Fatal(err)
	}
	var ps []identity.Fingerprint
	var refreshes []store.Record
	var pulls, want int
	for i := range pods {
		p := nth(catalog.K8sPod, i)
		ps = append(ps, p)
		n := 0
		for at := t0.Add(time.Duration(i) * every / pods); at.Before(t0.Add(span)); at = at.Add(pull) {
			refreshes = append(refreshes, store.Record{
				Layer: catalog.L2, Subject: store.EdgeSubject(p, node, catalog.ScheduledOn), Producer: "k8sobjects/default",
				EventTime: at, Kind: lifecycle.Observe, TTL: ttl, EventTimeBasis: store.BasisObjectField, Payload: payload,
			})
			n++
		}
		pulls += n
		want += (n + 1) / 2
	}
	slices.SortStableFunc(refreshes, func(a, b store.Record) int { return a.EventTime.Compare(b.EventTime) })
	// The sequencer's loop: a flush at every tick up to a refresh, then the refresh.
	// FlushAll is not called, because serve keeps running.
	co, err := coalesce.New(sequencer.CoalesceConfig())
	if err != nil {
		t.Fatal(err)
	}
	var all []store.Record
	tick := t0
	for _, r := range refreshes {
		for !tick.After(r.EventTime) {
			all = co.Flush(tick, all)
			tick = tick.Add(coalesce.FlushPeriod)
		}
		all = co.Add(r, all)
	}
	for tick.Before(t0.Add(span)) {
		all = co.Flush(tick, all)
		tick = tick.Add(coalesce.FlushPeriod)
	}
	through := 0
	for _, r := range all {
		if !r.Through.IsZero() {
			through++
		}
	}
	if through != 0 || len(all) != want || slices.ContainsFunc(all, func(r store.Record) bool { return r.Kind != lifecycle.Observe }) {
		t.Fatalf("ingest's coalescer (sequencer.CoalesceConfig) wrote %d records, %d with a Through, from %d pulls; SW1's precondition writes one Observe per two pulls at a 5-minute pull and a 15-minute TTL, with none: the cell no longer stands for serve (see pebblestore's seed-walk budget)", len(all), through, pulls)
	}
	for i := range all {
		all[i].Seq = uint64(i + 1)
	}
	for i := 0; i < len(all); i += 110 {
		if err := st.Write(ctx, all[i:min(i+110, len(all))]); err != nil {
			t.Fatal(err)
		}
	}
	return node, ps, all[len(all)-1].EventTime
}

// At the reference cell, with the default checkpoint policy, every seed walk of
// the node's reverse prefix at the latest token stays within SW1, whatever
// instant the window starts at; and the reads go through package interval's
// call, as a query makes them. SW1 is pebblestore's and is read from it. It
// holds at a token at or above the W of the checkpoint the walk uses, in a prefix
// the retention pass has rewritten, and in a store written under ingest's
// coalescer configuration, which the cell is.
func TestTheSeedWalkKeepsItsBudgetAtTheReferenceCell(t *testing.T) {
	t.Parallel()
	rec := newRecorder()
	st := openPebble(t, variant{checkpoint: pebblestore.DefaultCheckpoints()}, lifecycle.Policy{}, rec)
	budgetRecords, budgetStamps := pebblestore.SeedWalkBudget()
	sw1Records, sw1Stamps := int64(budgetRecords), int64(budgetStamps) // the recorder samples int64
	t0 := time.Date(2026, 3, 1, 0, 0, 0, 0, time.UTC)
	span := 4 * time.Hour
	if testing.Short() {
		span = 2 * time.Hour
	}
	node, _, last := referenceCell(t, st, t0, span)
	before := len(rec.sampled("read.seed_records_stepped"))
	r := interval.NewReader(st, interval.Options{Raw: true})
	for from := t0.Add(time.Hour); from.Before(last); from = from.Add(time.Minute + 7*time.Second) {
		res, err := r.Edges(ctx, node, store.Reverse, interval.Window{From: from, To: from.Add(30 * time.Minute), AsOf: store.Latest})
		if err != nil {
			t.Fatal(err)
		}
		if len(res.Elements) != 110 {
			t.Fatalf("from %s: %d placements, want the 110 live pods", from.Sub(t0), len(res.Elements))
		}
	}
	steps := rec.sampled("read.seed_records_stepped")[before:]
	stamps := rec.sampled("read.seed_stamps_decoded")[before:]
	if len(steps) == 0 {
		t.Fatal("no seed walk")
	}
	t.Logf("%d walks: at most %d records and %d stamps; SW1 is %d and %d", len(steps), slices.Max(steps), slices.Max(stamps), sw1Records, sw1Stamps)
	if slices.Max(steps) > sw1Records || slices.Max(stamps) > sw1Stamps {
		t.Errorf("a seed walk stepped %d records and decoded %d stamps, over SW1 (%d, %d)", slices.Max(steps), slices.Max(stamps), sw1Records, sw1Stamps)
	}
	if rec.counter("read.checkpoint_hits") == 0 {
		t.Error("no walk stopped at a checkpoint")
	}
}

// sinceCalls counts the WindowSince calls that reach a store.
type sinceCalls struct {
	store.Store
	mu sync.Mutex
	n  int
}

func (c *sinceCalls) WindowSince(ctx2 context.Context, fps []identity.Fingerprint, d store.Direction, from, to time.Time, sc store.Scope, o store.SinceOptions) ([]store.Since, error) {
	c.mu.Lock()
	c.n++
	c.mu.Unlock()
	return c.Store.WindowSince(ctx2, fps, d, from, to, sc, o)
}

// Each WindowSince a query makes is one iterator on pebblestore, however many
// entities it reads.
func TestEachWindowSinceIsOneIterator(t *testing.T) {
	t.Parallel()
	rec := newRecorder()
	st := openPebble(t, variant{checkpoint: pebblestore.DefaultCheckpoints()}, lifecycle.Policy{}, rec)
	t0 := time.Date(2026, 3, 1, 0, 0, 0, 0, time.UTC)
	node, pods, _ := referenceCell(t, st, t0, time.Hour)
	calls := &sinceCalls{Store: st}
	w := interval.Window{From: t0.Add(40 * time.Minute), To: t0.Add(55 * time.Minute), AsOf: store.Latest}
	if _, err := coresidency.NewFinder(calls).Find(ctx, pods[0], w, coresidency.Options{}); err != nil {
		t.Fatal(err)
	}
	if _, err := blastradius.NewWalker(calls).Impact(ctx, node, w, blastradius.Options{}); err != nil {
		t.Fatal(err)
	}
	if _, _, err := interval.NewReader(calls, interval.Options{}).EdgesBatch(ctx, append(pods[:20:20], node), store.Forward, w); err != nil {
		t.Fatal(err)
	}
	if got := rec.counter("read.since.reads"); got != int64(calls.n) || calls.n < 3 {
		t.Errorf("%d WindowSince calls opened %d iterators", calls.n, got)
	}
}
