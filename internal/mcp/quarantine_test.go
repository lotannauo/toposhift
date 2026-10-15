package mcp

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/lotannauo/toposhift/internal/identity"
	"github.com/lotannauo/toposhift/internal/store"
)

// nilCollisionStore reports every quarantine without detail, as the contract
// allows: a QuarantineError whose Collision is nil.
type nilCollisionStore struct{ store.Store }

func (n nilCollisionStore) Alive(ctx context.Context, fp identity.Fingerprint, t time.Time, sc store.Scope) (bool, error) {
	alive, err := n.Store.Alive(ctx, fp, t, sc)
	var qe *store.QuarantineError
	if errors.As(err, &qe) {
		return false, &store.QuarantineError{Entity: qe.Entity, Layer: qe.Layer}
	}
	return alive, err
}

// quarantineObjects finds the quarantine objects of an answer: those with an
// entity and a collision.
func quarantineObjects(ans map[string]any) []map[string]any {
	var out []map[string]any
	walk(ans, "$", func(_ string, obj map[string]any) {
		_, hasEntity := obj["entity"]
		_, hasCollision := obj["collision"]
		if hasEntity && hasCollision {
			out = append(out, obj)
		}
	})
	return out
}

// A quarantine is never an error and never dropped: in each tool, with the
// collision the store reports and with none.
func TestQuarantineInEachTool(t *testing.T) {
	w := newWorld(t, rankObjs)
	h2 := w.h2
	for _, withDetail := range []bool{true, false} {
		name := "with a collision"
		var st store.Store = w.st
		if !withDetail {
			name = "with a nil collision"
			st = nilCollisionStore{w.st}
		}
		t.Run(name, func(t *testing.T) {
			s, err := NewSource(Config{Store: st, Rank: rankObjs, RangeReads: true})
			if err != nil {
				t.Fatal(err)
			}
			asks := map[string]func() (any, error){
				"blast_radius": func() (any, error) {
					return s.BlastRadius(ctx, BlastRadiusInput{Entity: h2.String(), From: w10, To: w1030})
				},
				"what_changed": func() (any, error) {
					return s.WhatChanged(ctx, WhatChangedInput{Scope: []string{h2.String(), w.n3.String(), w.p5.String()}, From: w10, To: w1030})
				},
				"co_residents": func() (any, error) {
					return s.CoResidents(ctx, CoResidentsInput{Pod: w.p5.String(), From: w10, To: w1030})
				},
				"neighbors_at": func() (any, error) {
					return s.NeighborsAt(ctx, NeighborsAtInput{Entity: h2.String(), At: "2026-01-01T10:08:00Z"})
				},
				"entity_history": func() (any, error) {
					return s.EntityHistory(ctx, EntityHistoryInput{Entity: h2.String(), From: "2026-01-01T09:00:00Z", To: w1030})
				},
				"resolve_entity": func() (any, error) {
					return s.ResolveEntity(ctx, ResolveEntityInput{Type: "host", Attributes: rawAttrs(`{"host.id":"host-two"}`), At: w10})
				},
			}
			for tool, ask := range asks {
				v, err := ask()
				if err != nil {
					t.Errorf("%s: a quarantine failed the call: %v", tool, err)
					continue
				}
				b, _ := json.Marshal(v)
				qs := quarantineObjects(decode(t, b))
				if len(qs) == 0 {
					t.Errorf("%s: the quarantine was dropped: %s", tool, b)
				}
				for _, q := range qs {
					id := q["entity"].(map[string]any)["id"]
					if id != h2.String() {
						t.Errorf("%s: the quarantine is of %v", tool, id)
					}
					col, has := q["collision"]
					if !has {
						t.Errorf("%s: no collision key", tool)
						continue
					}
					if !withDetail {
						if col != nil {
							t.Errorf("%s: collision %v, want null", tool, col)
						}
						continue
					}
					c, _ := col.(map[string]any)
					if c["stale_boot"] != "b1" || c["newer_boot"] != "b2" ||
						c["observed_at"] != "2026-01-01T09:10:00Z" || c["newer_first_seen"] != "2026-01-01T09:05:00Z" {
						t.Errorf("%s: collision %v", tool, c)
					}
				}
			}
		})
	}
}

// The shapes of a quarantine in each tool: the seed's edges are listed and not
// followed, a quarantined host is not lifted through, an entity's history has no
// spans, and resolve says so.
func TestQuarantineShapes(t *testing.T) {
	w := newWorld(t, rankObjs)
	s := w.source(t, rankObjs, true)

	blast, err := s.BlastRadius(ctx, BlastRadiusInput{Entity: w.h2.String(), From: w10, To: w1030})
	if err != nil {
		t.Fatal(err)
	}
	if len(blast.Members) != 0 || len(blast.Quarantined) != 1 || blast.Quarantined[0].Kind != "edge" ||
		blast.Quarantined[0].Edge.Relation != "runs_on" || blast.Quarantined[0].Quarantined.Entity.FP != w.h2 {
		t.Errorf("a quarantined seed: %d members, quarantined %+v", len(blast.Members), blast.Quarantined)
	}

	co, err := s.CoResidents(ctx, CoResidentsInput{Pod: w.p5.String(), From: w10, To: w1030})
	if err != nil {
		t.Fatal(err)
	}
	if len(co.Unattributed) != 1 || co.Unattributed[0].Reason != "quarantined_host" ||
		co.Unattributed[0].Rule != "unattributed.quarantined_host" || len(co.Quarantined) != 1 {
		t.Errorf("a node on a quarantined host: unattributed %+v, quarantined %+v", co.Unattributed, co.Quarantined)
	}

	hist, err := s.EntityHistory(ctx, EntityHistoryInput{Entity: w.h2.String(), From: "2026-01-01T09:00:00Z", To: w1030})
	if err != nil {
		t.Fatal(err)
	}
	if hist.Quarantined == nil || len(hist.Existence.Spans) != 0 || hist.Existence.Class != "none" || len(hist.Records) != 3 {
		t.Errorf("history of a quarantined host: %+v, %d spans, %d records", hist.Quarantined, len(hist.Existence.Spans), len(hist.Records))
	}
	for _, r := range hist.Records {
		if r.Basis != "fact" {
			t.Errorf("record basis %q", r.Basis)
		}
	}

	res, err := s.ResolveEntity(ctx, ResolveEntityInput{Type: "host", Attributes: rawAttrs(`{"host.id":"host-two"}`), At: w10})
	if err != nil {
		t.Fatal(err)
	}
	if res.Existence.Status != "quarantined" || res.Existence.Quarantined == nil || res.Entity.FP != w.h2 {
		t.Errorf("resolve: %+v", res.Existence)
	}

	nb, err := s.NeighborsAt(ctx, NeighborsAtInput{Entity: w.n3.String(), At: "2026-01-01T10:08:00Z", Direction: "forward"})
	if err != nil {
		t.Fatal(err)
	}
	if len(nb.Edges) != 1 || nb.Edges[0].Quarantined == nil || nb.Edges[0].Quarantined.Entity.FP != w.h2 || len(nb.Quarantined) != 1 {
		t.Errorf("neighbors of a node on a quarantined host: %+v", nb)
	}
}
