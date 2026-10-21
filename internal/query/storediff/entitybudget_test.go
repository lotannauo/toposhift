package storediff_test

import (
	"encoding/json"
	"reflect"
	"slices"
	"testing"
	"time"

	"github.com/lotannauo/toposhift/internal/catalog"
	"github.com/lotannauo/toposhift/internal/coalesce"
	"github.com/lotannauo/toposhift/internal/identity"
	"github.com/lotannauo/toposhift/internal/ingest/sequencer"
	"github.com/lotannauo/toposhift/internal/lifecycle"
	"github.com/lotannauo/toposhift/internal/query/interval"
	"github.com/lotannauo/toposhift/internal/store"
	"github.com/lotannauo/toposhift/internal/store/pebblestore"
)

// longLivedPods writes pods that live for span from t0 to st, the way serve writes
// them: each is observed by a pull every 5 minutes with a 15 minute TTL, through
// ingest's coalescer (sequencer.CoalesceConfig) with a flush every
// coalesce.FlushPeriod, so that a run starts every second pull, every 10 minutes,
// and a pod has 144 records a day. What the pods report is the entity's own
// existence (the pod's own prefix, not an edge). It returns the pods, how many
// records each has, and the instant of the last record.
func longLivedPods(t *testing.T, st store.Store, t0 time.Time, span time.Duration, n int) (pods []identity.Fingerprint, perPod int, last time.Time) {
	t.Helper()
	pull, ttl := 5*time.Minute, 15*time.Minute
	every := 2 * pull
	payload, err := json.Marshal(map[string]any{"name": "web-7d9f8", "namespace": "default", "phase": "Running"})
	if err != nil {
		t.Fatal(err)
	}
	var refreshes []store.Record
	for i := range n {
		p := nth(catalog.K8sPod, 1000+i)
		pods = append(pods, p)
		for at := t0.Add(time.Duration(i) * every / time.Duration(n)); at.Before(t0.Add(span)); at = at.Add(pull) {
			refreshes = append(refreshes, store.Record{
				Layer: catalog.L2, Subject: store.EntitySubject(p), Producer: "k8sobjects/default",
				EventTime: at, Kind: lifecycle.Observe, TTL: ttl, EventTimeBasis: store.BasisObjectField, Payload: payload,
			})
		}
	}
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
	for tick.Before(t0.Add(span)) {
		all = co.Flush(tick, all)
		tick = tick.Add(coalesce.FlushPeriod)
	}
	if slices.ContainsFunc(all, func(r store.Record) bool { return !r.Through.IsZero() || r.Kind != lifecycle.Observe }) {
		t.Fatal("ingest's coalescer (sequencer.CoalesceConfig) wrote a run extension or a delete for a pod that is simply refreshed: the history no longer stands for serve (see pebblestore's seed-walk budget)")
	}
	for i := range all {
		all[i].Seq = uint64(i + 1)
	}
	for i := 0; i < len(all); i += 64 {
		if err := st.Write(ctx, all[i:min(i+64, len(all))]); err != nil {
			t.Fatal(err)
		}
	}
	return pods, len(all) / n, all[len(all)-1].EventTime
}

// A pod that has lived for 30 days under serve's policy (a boot key, the default
// checkpoints, a coalescer that starts a run every 10 minutes) has about 4,300
// records in its own prefix. Its seed walk, which a query makes for a window of the
// entity, steps over at most SW1's budget of them, because a checkpoint stands in
// the prefix of every entity but a host, under a boot key as without one. Without
// checkpoints (the control, which is what a store with a boot key did to every
// entity's prefix before) it steps over every record before the window. The
// answers of the two stores are the same.
func TestAPodsSeedWalkKeepsItsBudgetUnderABootKey(t *testing.T) {
	t.Parallel()
	span, pods := 30*24*time.Hour, 4
	if raceEnabled || testing.Short() {
		span, pods = 7*24*time.Hour, 2
	}
	t0 := time.Date(2026, 3, 1, 0, 0, 0, 0, time.UTC)
	serve := lifecycle.Policy{BootKey: lifecycle.BootID}
	rec, ctl := newRecorder(), newRecorder()
	with := openPebble(t, variant{checkpoint: pebblestore.DefaultCheckpoints()}, serve, rec)
	without := openPebble(t, variant{checkpoint: pebblestore.CheckpointOptions{}}, serve, ctl)
	fps, perPod, last := longLivedPods(t, with, t0, span, pods)
	longLivedPods(t, without, t0, span, pods)
	days := int(span / (24 * time.Hour))
	if want := 144 * days; perPod < want-2 || perPod > want+2 {
		t.Fatalf("a pod has %d records, want %d (144 a day for %d days)", perPod, want, days)
	}
	budgetRecords, _ := pebblestore.SeedWalkBudget()
	read := func(st store.Store, r *recorder) (steps []int64, answers [][]interval.Element) {
		before := len(r.sampled("read.seed_records_stepped"))
		rd := interval.NewReader(st, interval.Options{Raw: true})
		for from := t0.Add(time.Hour); from.Before(last); from = from.Add(7*time.Hour + 13*time.Minute) {
			_, els, err := rd.Entities(ctx, fps, interval.Window{From: from, To: from.Add(30 * time.Minute), AsOf: store.Latest})
			if err != nil {
				t.Fatal(err)
			}
			if len(els) != len(fps) {
				t.Fatalf("from %s: %d pods in the window, want %d", from.Sub(t0), len(els), len(fps))
			}
			answers = append(answers, els)
		}
		return r.sampled("read.seed_records_stepped")[before:], answers
	}
	got, gotAnswers := read(with, rec)
	want, wantAnswers := read(without, ctl)
	if len(got) == 0 || len(got) != len(want) {
		t.Fatalf("%d seed walks with checkpoints and %d without", len(got), len(want))
	}
	if !reflect.DeepEqual(gotAnswers, wantAnswers) {
		t.Fatal("the store with checkpoints and the one without answer a window of the pods differently")
	}
	// A count, local and deterministic: a function of the history above.
	t.Logf("%d records a pod, %d seed walks: at most %d records stepped with checkpoints (SW1 is %d), at most %d without", perPod, len(got), slices.Max(got), budgetRecords, slices.Max(want))
	if int(slices.Max(got)) > budgetRecords {
		t.Errorf("a pod's seed walk stepped %d records, over SW1's %d", slices.Max(got), budgetRecords)
	}
	if rec.counter("read.checkpoint_hits") == 0 {
		t.Error("no walk stopped at a checkpoint")
	}
	if floor := int64(perPod) * 9 / 10; slices.Max(want) < floor {
		t.Errorf("without checkpoints the longest seed walk stepped %d records, under %d (nine tenths of a pod's %d): the control shows nothing", slices.Max(want), floor, perPod)
	}
}
