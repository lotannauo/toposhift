package mcp

import (
	"slices"
	"testing"

	"github.com/lotannauo/toposhift/internal/catalog"
)

func entriesOf(es []ChangeEntry, kind string) map[string]ChangeEntry {
	out := map[string]ChangeEntry{}
	for _, e := range es {
		switch {
		case e.Entity != nil && kind == "entity":
			out[e.Entity.ID] = e
		case e.Edge != nil && kind == "relation":
			out[e.Edge.From.ID] = e
		}
	}
	return out
}

// The buckets of a diff are exclusive, and an element that came and went is in
// the transient bucket; every disappearance says how it came about.
func TestWhatChangedBuckets(t *testing.T) {
	w := newWorld(t, rankObjs)
	s := w.source(t, rankObjs, true)
	out, err := s.WhatChanged(ctx, WhatChangedInput{
		Scope: []string{w.n1.String(), w.p1.String(), w.p2.String(), w.p3.String(), w.p4.String(), w.p6.String(), w.p3.String()},
		From:  w10, To: w1030,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(out.Scope) != 6 {
		t.Errorf("scope %d entities, want 6 once each", len(out.Scope))
	}
	if out.Window.To != "2026-01-01T10:30:00.000000001Z" || !contains(out.Notes, "The window read is [from, to+1ns): both instants are inside it.") {
		t.Errorf("window %+v, notes %q", out.Window, out.Notes)
	}

	if _, ok := entriesOf(out.Created, "entity")[w.p3.String()]; !ok {
		t.Errorf("P3 is not created: %+v", out.Created)
	}
	created := entriesOf(out.Created, "relation")
	if e, ok := created[w.p3.String()]; !ok || e.Edge.Relation != "scheduled_on" {
		t.Errorf("P3's edge is not created")
	}
	deleted := entriesOf(out.Deleted, "entity")
	if e := deleted[w.p2.String()]; e.Spans[0].EndReason == nil || *e.Spans[0].EndReason != "producer" || e.DeletedAt != "2026-01-01T10:05:00Z" {
		t.Errorf("P2 deleted: %+v", e)
	}
	if e := deleted[w.p6.String()]; e.Spans[0].EndReason == nil || *e.Spans[0].EndReason != "liveness_expiry" {
		t.Errorf("P6 deleted: %+v", e)
	}
	// P2's edge outlives it in the store, and ends by cascade.
	if e := entriesOf(out.Deleted, "relation")[w.p2.String()]; e.Spans[0].EndReason == nil || *e.Spans[0].EndReason != "cascade" {
		t.Errorf("P2's edge: %+v", e)
	}
	changed := entriesOf(out.Changed, "entity")
	if e := changed[w.p1.String()]; e.Change == nil || e.Change.Kind != "keys" || !slices.Equal(e.Change.StateKeys, []string{"phase"}) {
		t.Errorf("P1 changed: %+v", e.Change)
	}
	// P4's edge left at 10:10 and came back at 10:20.
	flap := entriesOf(out.Transient, "relation")[w.p4.String()]
	if flap.Class != "flapping" || len(flap.Spans) != 2 || flap.Rule != "diff.transient" {
		t.Errorf("P4's edge: %+v", flap)
	}
	// Exclusive: no element is in two net buckets.
	seen := map[string]string{}
	for bucket, es := range map[string][]ChangeEntry{"created": out.Created, "deleted": out.Deleted, "changed": out.Changed} {
		for _, e := range es {
			key := e.Kind
			if e.Entity != nil {
				key += e.Entity.ID
			} else {
				key += e.Edge.From.ID + e.Edge.To.ID + e.Edge.Relation
			}
			if other, dup := seen[key]; dup {
				t.Errorf("%s is in %s and %s", key, other, bucket)
			}
			seen[key] = bucket
		}
	}

	// The events are in time order, each disappearance with its reason.
	var last string
	for _, ev := range out.Events {
		if ev.At < last {
			t.Errorf("events out of order at %s", ev.At)
		}
		last = ev.At
		if ev.Event == "disappeared" && ev.EndReason == nil {
			t.Errorf("a disappearance with no reason: %+v", ev)
		}
		if ev.Event == "appeared" && ev.EndReason != nil {
			t.Errorf("an appearance with a reason: %+v", ev)
		}
	}
	if out.Counts.Events != len(out.Events) || len(out.Events) == 0 {
		t.Errorf("events %d, counted %d", len(out.Events), out.Counts.Events)
	}
}

// A change of winner, and a description that is not a JSON object, are reported
// as what they are.
func TestWhatChangedKinds(t *testing.T) {
	w := newWorld(t, rankObjs)
	opaque := fpOf(t, catalog.K8sPod, catalog.K8sPodUID, "pod-uid-opaque")
	dual := fpOf(t, catalog.K8sPod, catalog.K8sPodUID, "pod-uid-dual")
	w.feed.write(
		observe(ent(dual), objs, hm(9, 0), 0, `{"name":"dual"}`),
		observe(ent(dual), clus, hm(9, 0), 0, `{"k8s.pod.name":"dual-cluster"}`),
		deleted(ent(dual), objs, hm(10, 10)),
		observe(ent(opaque), objs, hm(9, 0), 0, `{"name":"o"}`),
		observe(ent(opaque), objs, hm(10, 10), 0, `["not an object"]`),
	)
	s := w.source(t, rankObjs, true)
	out, err := s.WhatChanged(ctx, WhatChangedInput{Scope: []string{dual.String(), opaque.String()}, From: w10, To: w1030})
	if err != nil {
		t.Fatal(err)
	}
	changed := entriesOf(out.Changed, "entity")
	if e := changed[dual.String()]; e.Change == nil || e.Change.Kind != "source_changed" || e.Change.FromProducer != string(objs) || e.Change.ToProducer != string(clus) {
		t.Errorf("a pod that lost its winner: %+v", e.Change)
	}
	if e := changed[opaque.String()]; e.Change == nil || e.Change.Kind != "opaque" {
		t.Errorf("opaque: %+v", e.Change)
	}
}
