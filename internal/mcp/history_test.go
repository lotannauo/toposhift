package mcp

import (
	"testing"
)

// The records of an entity are facts with both time axes, and its existence is
// derived; a deleted pod's history says who deleted it.
func TestEntityHistory(t *testing.T) {
	w := newWorld(t, rankObjs)
	s := w.source(t, rankObjs, true)

	out, err := s.EntityHistory(ctx, EntityHistoryInput{Entity: w.p2.String(), From: "2026-01-01T09:00:00Z", To: w1030})
	if err != nil {
		t.Fatal(err)
	}
	if !out.Derived || out.Existence == nil || out.Existence.Basis != "derived" || out.Existence.Rule != "existence" || out.Existence.Subject != "asserted" {
		t.Fatalf("existence %+v", out.Existence)
	}
	sp := out.Existence.Spans
	if len(sp) != 1 || sp[0].End != "2026-01-01T10:05:00Z" || reason(sp[0].EndReason) != "producer" || sp[0].EndSentence != "deleted by its producer" ||
		len(sp[0].AssertedBy) != 1 || sp[0].AssertedBy[0] != string(objs) {
		t.Errorf("spans %+v", sp)
	}
	if out.Existence.DescriptionAtStart == nil || out.Existence.DescriptionAtEnd != nil {
		t.Errorf("descriptions at the window's ends: %+v %+v", out.Existence.DescriptionAtStart, out.Existence.DescriptionAtEnd)
	}
	if len(out.Records) != 2 {
		t.Fatalf("%d records", len(out.Records))
	}
	obs, del := out.Records[0], out.Records[1]
	if obs.Basis != "fact" || obs.Subject != "asserted" || obs.Kind != "observe" || obs.EventTime != "2026-01-01T09:00:00Z" || obs.Seq != "18" ||
		obs.Producer != string(objs) || obs.TTL != "0s" || obs.Layer != "L2" || obs.Payload["name"] != "web-1" {
		t.Errorf("observe: %+v", obs)
	}
	if del.Kind != "delete" || del.EventTime != "2026-01-01T10:05:00Z" || del.Payload != nil || del.Seq <= obs.Seq && len(del.Seq) <= len(obs.Seq) {
		t.Errorf("delete: %+v", del)
	}

	// A window before the first record has none, and no existence.
	early, err := s.EntityHistory(ctx, EntityHistoryInput{Entity: w.p2.String(), From: "2026-01-01T07:00:00Z", To: "2026-01-01T08:00:00Z"})
	if err != nil || len(early.Records) != 0 || len(early.Existence.Spans) != 0 || early.Existence.Class != "none" {
		t.Errorf("early: %v %+v", err, early)
	}

	// A record with a TTL and one that stands for a run carry them.
	lim, err := s.EntityHistory(ctx, EntityHistoryInput{Entity: w.p1.String(), From: "2026-01-01T09:00:00Z", To: w1030, Limit: 2})
	if err != nil {
		t.Fatal(err)
	}
	if len(lim.Records) != 2 || lim.Counts.Records != 3 || lim.Counts.Shown != 2 || !contains(lim.Notes, "Showing 2 of 3 records, oldest first.") {
		t.Errorf("limit 2: %d records of %d; notes %q", len(lim.Records), lim.Counts.Records, lim.Notes)
	}
}
