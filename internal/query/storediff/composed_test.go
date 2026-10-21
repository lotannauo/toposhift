package storediff_test

import (
	"context"
	"encoding/json"
	"fmt"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/lotannauo/toposhift/internal/catalog"
	"github.com/lotannauo/toposhift/internal/coalesce"
	"github.com/lotannauo/toposhift/internal/identity"
	"github.com/lotannauo/toposhift/internal/ingest/sequencer"
	"github.com/lotannauo/toposhift/internal/lifecycle"
	"github.com/lotannauo/toposhift/internal/mcp"
	"github.com/lotannauo/toposhift/internal/store"
	"github.com/lotannauo/toposhift/internal/store/pebblestore"
)

// serveCellProducers are the producers serve has: the Kubernetes objects' and the
// node agent's, under serve's rank.
const (
	serveObjects = lifecycle.Producer("k8sobjects/default")
	serveAgent   = lifecycle.Producer("nodeagent/default")
)

func serveRank() map[lifecycle.Producer]int {
	return map[lifecycle.Producer]int{serveObjects: 2, "k8scluster/default": 1}
}

// serveCellData is what serveCell wrote: the entities, and the records of each
// entity prefix, which the entity walks are held to.
type serveCellData struct {
	node, host, hot identity.Fingerprint
	pods            []identity.Fingerprint
	last            time.Time
	hotRecords      int
	// entityTimes are the event times of the records of each entity, ascending.
	entityTimes map[identity.Fingerprint][]time.Time
}

// serveCell writes to st the cell shaped as serve stores it: a node with 110 live
// pods, a host the node runs on, every one refreshed by a pull every 5 minutes
// with a 15-minute TTL, their cadences spread over 10 minutes, written through
// ingest's coalescer (sequencer.CoalesceConfig) with a flush at every
// coalesce.FlushPeriod, as serve writes them; the producers and ranks serve has,
// and its policy, a boot key of lifecycle.BootID. The first pod is crash-looping:
// its description alternates every 15 seconds, so its entity prefix holds a record
// for each change. The store must be opened with the policy the cell returns the
// rank of (serveRank) and the default checkpoints, as serve's is.
func serveCell(t *testing.T, st store.Store, t0 time.Time, span time.Duration) serveCellData {
	t.Helper()
	const pods = 110
	pull, ttl, flip := 5*time.Minute, 15*time.Minute, 15*time.Second
	every := 2 * pull
	cat := catalog.Default()
	layerOfEntity := func(typ catalog.EntityType) catalog.Layer {
		e, ok := cat.Entity(typ)
		if !ok {
			t.Fatalf("no entity type %s", typ)
		}
		return e.Layer()
	}
	layerOfRelation := func(rel catalog.RelationType) catalog.Layer {
		r, ok := cat.Relation(rel)
		if !ok {
			t.Fatalf("no relation %s", rel)
		}
		return r.Layer()
	}
	d := serveCellData{
		node: nth(catalog.K8sNode, 1), host: nth(catalog.Host, 1), hot: nth(catalog.K8sPod, 0),
		entityTimes: map[identity.Fingerprint][]time.Time{},
	}
	placement, err := json.Marshal(map[string]any{
		"node_name":  "ip-10-0-12-34.eu-west-1.compute.internal",
		"resolution": map[string]any{"basis": "node-name-index", "node_interval_start": t0.Format(time.RFC3339Nano)},
	})
	if err != nil {
		t.Fatal(err)
	}
	end := t0.Add(span)
	var refreshes []store.Record
	var pulls, want int
	// add writes the pulls of one subject from first on, and counts the records the
	// coalescer must write for them: one for every second pull.
	add := func(sub store.Subject, layer catalog.Layer, p lifecycle.Producer, boot string, first time.Time, payload []byte) {
		n := 0
		for at := first; at.Before(end); at = at.Add(pull) {
			refreshes = append(refreshes, store.Record{
				Layer: layer, Subject: sub, Producer: p, EventTime: at, Kind: lifecycle.Observe, TTL: ttl,
				EventTimeBasis: store.BasisObjectField, Boot: boot, Payload: payload,
			})
			n++
		}
		pulls += n
		want += (n + 1) / 2
	}
	podLayer, nodeLayer, hostLayer := layerOfEntity(catalog.K8sPod), layerOfEntity(catalog.K8sNode), layerOfEntity(catalog.Host)
	scheduled, runsOn := layerOfRelation(catalog.ScheduledOn), layerOfRelation(catalog.RunsOn)
	for i := range pods {
		p := nth(catalog.K8sPod, i)
		d.pods = append(d.pods, p)
		first := t0.Add(time.Duration(i) * every / pods)
		add(store.EdgeSubject(p, d.node, catalog.ScheduledOn), scheduled, serveObjects, "", first, placement)
		if i == 0 {
			// The crash-looping pod: a different description every 15 seconds, each
			// of which the coalescer writes.
			for k, at := 0, first; at.Before(end); k, at = k+1, at.Add(flip) {
				phase := "Running"
				if k%2 == 1 {
					phase = "Pending"
				}
				refreshes = append(refreshes, store.Record{
					Layer: podLayer, Subject: store.EntitySubject(p), Producer: serveObjects, EventTime: at, Kind: lifecycle.Observe,
					TTL: ttl, EventTimeBasis: store.BasisObjectField,
					Payload: []byte(`{"name":"pod-0","namespace":"shop","phase":"` + phase + `"}`),
				})
				d.hotRecords++
			}
			continue
		}
		add(store.EntitySubject(p), podLayer, serveObjects, "", first, []byte(fmt.Sprintf(`{"name":"pod-%d","namespace":"shop","phase":"Running"}`, i)))
	}
	add(store.EntitySubject(d.node), nodeLayer, serveObjects, "", t0, []byte(`{"name":"worker-1"}`))
	add(store.EntitySubject(d.host), hostLayer, serveAgent, "boot-1", t0.Add(20*time.Second), []byte(`{"host.arch":"arm64"}`))
	add(store.EdgeSubject(d.node, d.host, catalog.RunsOn), runsOn, serveAgent, "", t0.Add(40*time.Second), []byte(`{}`))
	want += d.hotRecords

	slices.SortStableFunc(refreshes, func(a, b store.Record) int { return a.EventTime.Compare(b.EventTime) })
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
	for tick.Before(end) {
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
		t.Fatalf("ingest's coalescer wrote %d records, %d with a Through, from %d pulls and %d changes of the crash-looping pod; want %d, none with a Through, as SW1's precondition",
			len(all), through, pulls, d.hotRecords, want)
	}
	for i := range all {
		all[i].Seq = uint64(i + 1)
		if sub := all[i].Subject; sub.Kind == store.SubjectEntity {
			d.entityTimes[sub.A] = append(d.entityTimes[sub.A], all[i].EventTime)
		}
	}
	for i := 0; i < len(all); i += 110 {
		if err := st.Write(ctx, all[i:min(i+110, len(all))]); err != nil {
			t.Fatal(err)
		}
	}
	d.last = all[len(all)-1].EventTime
	d.hotRecords = len(d.entityTimes[d.hot])
	if d.hotRecords <= 300 {
		t.Fatalf("the cell lost its hot pod: %d records in its entity prefix, want more than 300", d.hotRecords)
	}
	return d
}

const (
	stepsSample  = "read.seed_records_stepped"
	stampsSample = "read.seed_stamps_decoded"
)

// walkStats are the seed walks of one tool's calls in one run, by kind: an edge
// walk is a seed walk of an edge prefix, an entity walk one of an entity's own.
type walkStats struct {
	calls                      int
	since                      [2]int // WindowSince calls by kind, edge then entity
	walks                      [2]int
	maxRecords, maxStamps      [2]int64
	entityLong                 int // entity walks of an entity whose history to the window is longer than SW1
	payloadSeeks, recordsSteps int64
}

const (
	edgeKind   = 0
	entityKind = 1
)

// walkSplit wraps the pebblestore a source reads. In each WindowSince it takes the
// seed walks the call added to the recorder and files them by the direction of the
// call (edge walks for a direction, entity walks for none), zipped with the
// fingerprints, which pebblestore samples once each, in order. A walk whose
// attribution cannot be shown fails the check. The calls are serialized so that
// the samples a call adds are its own.
type walkSplit struct {
	store.Store
	rec  *recorder
	cell serveCellData
	mu   sync.Mutex
	// label is the tool being asked.
	label string
	stats map[string]*walkStats
	// sinceCalls is every WindowSince the store saw.
	sinceCalls int
	// problems are the walks that broke an assertion, as sentences.
	problems []string
	// entityMax is the longest entity walk and the entity it was of.
	entityMax int64
}

func (w *walkSplit) WindowSince(c context.Context, fps []identity.Fingerprint, d store.Direction, from, to time.Time, sc store.Scope, o store.SinceOptions) ([]store.Since, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	nSteps, nStamps := len(w.rec.sampled(stepsSample)), len(w.rec.sampled(stampsSample))
	out, err := w.Store.WindowSince(c, fps, d, from, to, sc, o)
	w.sinceCalls++
	if err != nil {
		return out, err
	}
	steps, stamps := w.rec.sampled(stepsSample)[nSteps:], w.rec.sampled(stampsSample)[nStamps:]
	kind := edgeKind
	if d == 0 {
		kind = entityKind
	}
	s := w.stats[w.label]
	s.since[kind]++
	if len(steps) != len(fps) || len(stamps) != len(fps) {
		w.problem(fmt.Sprintf("%s: a WindowSince of %d fingerprints (direction %s) sampled %d seed walks and %d stamp counts: the walks cannot be attributed", w.label, len(fps), d, len(steps), len(stamps)))
		return out, nil
	}
	budgetRecords, budgetStamps := pebblestore.SeedWalkBudget()
	for i, fp := range fps {
		s.walks[kind]++
		s.maxRecords[kind], s.maxStamps[kind] = max(s.maxRecords[kind], steps[i]), max(s.maxStamps[kind], stamps[i])
		switch kind {
		case edgeKind:
			// Assertion 2: SW1.
			if steps[i] > int64(budgetRecords) || stamps[i] > int64(budgetStamps) {
				w.problem(fmt.Sprintf("%s: an edge walk of %s (direction %s) from %s stepped %d records and decoded %d stamps, over SW1 (%d, %d)",
					w.label, fp, d, from.Format(time.RFC3339), steps[i], stamps[i], budgetRecords, budgetStamps))
			}
		case entityKind:
			w.entityMax = max(w.entityMax, steps[i])
			// Assertion 4: the walk is the entity's own history, nothing more.
			n, _ := slices.BinarySearchFunc(w.cell.entityTimes[fp], from, func(at, x time.Time) int {
				if at.After(x) {
					return 1
				}
				return -1
			})
			if steps[i] > int64(n)+1 {
				w.problem(fmt.Sprintf("%s: an entity walk of %s from %s stepped %d records, its history to then is %d", w.label, fp, from.Format(time.RFC3339), steps[i], n))
			}
			// Assertion 2 for the entity: a checkpoint stands in the prefix of every
			// entity but a host, under a boot key as without one, so its walk is held
			// to SW1 too. A walk of an entity with more history than that is the
			// positive control.
			if n > budgetRecords {
				s.entityLong++
			}
			if steps[i] > int64(budgetRecords) || stamps[i] > int64(budgetStamps) {
				w.problem(fmt.Sprintf("%s: an entity walk of %s from %s stepped %d records and decoded %d stamps, over SW1 (%d, %d); its history to then is %d",
					w.label, fp, from.Format(time.RFC3339), steps[i], stamps[i], budgetRecords, budgetStamps, n))
			}
		}
	}
	return out, nil
}

// problem keeps the first few walks that broke an assertion.
func (w *walkSplit) problem(msg string) {
	if len(w.problems) < 10 {
		w.problems = append(w.problems, msg)
	}
}

// composedRun asks the tools the composed questions of the cell, window by
// window, of a source over st, and holds the seed walks to the budget.
func composedRun(t *testing.T, name string, st *pebblestore.Store, rec *recorder, d serveCellData, t0 time.Time) {
	t.Helper()
	spy := &walkSplit{Store: st, rec: rec, cell: d, stats: map[string]*walkStats{}}
	src, err := mcp.NewSource(mcp.Config{Store: spy, Rank: serveRank(), RangeReads: true})
	if err != nil {
		t.Fatal(err)
	}
	ids := func(fps ...identity.Fingerprint) []string {
		out := make([]string, len(fps))
		for i, fp := range fps {
			out[i] = fp.String()
		}
		return out
	}
	scope := ids(append([]identity.Fingerprint{d.node}, d.pods...)...)
	rfc := func(x time.Time) string { return x.UTC().Format(time.RFC3339) }
	sinceBefore := rec.counter("read.since.reads")
	var calls int
	var failures []string
	for from := t0.Add(time.Hour); !from.Add(30 * time.Minute).After(d.last); from = from.Add(7*time.Minute + 7*time.Second) {
		to := from.Add(30 * time.Minute)
		ask := func(tool string, f func() error) {
			spy.mu.Lock()
			spy.label = tool
			if spy.stats[tool] == nil {
				spy.stats[tool] = &walkStats{}
			}
			s := spy.stats[tool]
			payload0, steps0 := rec.counter("read.seed_payload_steps"), rec.counter("read.records_stepped")
			spy.mu.Unlock()
			err := f()
			spy.mu.Lock()
			s.calls++
			calls++
			s.payloadSeeks += rec.counter("read.seed_payload_steps") - payload0
			s.recordsSteps += rec.counter("read.records_stepped") - steps0
			spy.mu.Unlock()
			if err != nil && len(failures) < 10 {
				failures = append(failures, fmt.Sprintf("%s from %s: %v", tool, rfc(from), err))
			}
		}
		for _, e := range []identity.Fingerprint{d.node, d.host} {
			ask("blast_radius", func() error {
				_, err := src.BlastRadius(ctx, mcp.BlastRadiusInput{Entity: e.String(), From: rfc(from), To: rfc(to)})
				return err
			})
		}
		for _, p := range []identity.Fingerprint{d.pods[1], d.hot} {
			ask("co_residents", func() error {
				_, err := src.CoResidents(ctx, mcp.CoResidentsInput{Pod: p.String(), From: rfc(from), To: rfc(to)})
				return err
			})
		}
		ask("what_changed", func() error {
			_, err := src.WhatChanged(ctx, mcp.WhatChangedInput{Scope: scope, From: rfc(from), To: rfc(to)})
			return err
		})
		for _, e := range []identity.Fingerprint{d.hot, d.node} {
			ask("entity_history", func() error {
				_, err := src.EntityHistory(ctx, mcp.EntityHistoryInput{Entity: e.String(), From: rfc(from), To: rfc(to)})
				return err
			})
		}
		ask("neighbors_at", func() error {
			_, err := src.NeighborsAt(ctx, mcp.NeighborsAtInput{Entity: d.node.String(), At: rfc(from)})
			return err
		})
	}
	if calls == 0 {
		t.Fatalf("%s: no call was asked", name)
	}
	// Assertion 1: every call answers.
	for _, f := range failures {
		t.Errorf("%s: a call failed: %s", name, f)
	}
	// Assertions 2 and 4 were checked walk by walk.
	for _, p := range spy.problems {
		t.Errorf("%s: %s", name, p)
	}
	// Assertion 3: one iterator for each WindowSince.
	if got := rec.counter("read.since.reads") - sinceBefore; got != int64(spy.sinceCalls) {
		t.Errorf("%s: %d WindowSince calls opened %d iterators", name, spy.sinceCalls, got)
	}
	// The report, one line for each tool.
	var entityLong int
	for _, tool := range []string{"blast_radius", "co_residents", "what_changed", "entity_history", "neighbors_at"} {
		s := spy.stats[tool]
		if s == nil {
			t.Errorf("%s: %s was never asked", name, tool)
			continue
		}
		entityLong += s.entityLong
		t.Logf("composed %-14s run=%-10s calls=%d since(edge,entity)=%d,%d walks(edge,entity)=%d,%d max_edge(records=%d,stamps=%d) max_entity(records=%d,stamps=%d) entity_walks_of_long_histories=%d seed_payload_steps=%d records_stepped=%d",
			tool, name, s.calls, s.since[edgeKind], s.since[entityKind], s.walks[edgeKind], s.walks[entityKind],
			s.maxRecords[edgeKind], s.maxStamps[edgeKind], s.maxRecords[entityKind], s.maxStamps[entityKind], s.entityLong, s.payloadSeeks, s.recordsSteps)
	}
	// Assertion 5: the positive control. Without an entity walk of an entity with
	// more history than the budget, the entity walks held to it prove nothing.
	if entityLong == 0 {
		t.Errorf("%s: no entity walk was of an entity with more history than the budget: the hot pod's longest walk (%d) does not show the entity walks held to it", name, spy.entityMax)
	}
}

// The composed queries, asked through the MCP tools at a cell shaped as serve
// stores it, keep SW1 on every edge seed walk: at most 3 x G records and one
// stamp, read from pebblestore. Each WindowSince is one iterator. The entity seed
// walks, which have checkpoints under a boot key for every entity but a host, are
// counted apart and held to the same budget, and to the entity's own history, and
// shown to be of an entity whose history is longer than the budget at the hot
// pod. Unretained, and retained at the cell's 30th minute. The cell spans 4 hours,
// 2 under the race detector and with -short; the two runs are independent and go
// side by side.
func TestComposedQueriesKeepTheSeedWalkBudget(t *testing.T) {
	t.Parallel()
	t0 := time.Date(2026, 3, 1, 0, 0, 0, 0, time.UTC)
	span := 4 * time.Hour
	if testing.Short() || raceEnabled {
		span = 2 * time.Hour
	}
	for _, retain := range []bool{false, true} {
		name := "unretained"
		if retain {
			name = "retained"
		}
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			rec := newRecorder()
			policy := lifecycle.Policy{BootKey: lifecycle.BootID, Rank: serveRank()}
			st := openPebble(t, variant{checkpoint: pebblestore.DefaultCheckpoints(), retention: pebblestore.Synchronous}, policy, rec)
			d := serveCell(t, st, t0, span)
			if retain {
				if err := st.Retain(ctx, t0.Add(30*time.Minute)); err != nil {
					t.Fatal(err)
				}
			}
			composedRun(t, name, st, rec, d, t0)
		})
	}
}
