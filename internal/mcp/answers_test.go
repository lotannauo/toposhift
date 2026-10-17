package mcp

import (
	"encoding/json"
	"errors"
	"sort"
	"testing"
)

// answer is one tool's answer to one question, as the client receives it.
type answer struct {
	name string
	tool string
	json []byte
	// echo says the answer repeats attributes the request gave.
	echo bool
}

func marshal(t testing.TB, name, tool string, v any, err error) answer {
	t.Helper()
	if err != nil {
		t.Fatalf("%s: %v", name, err)
	}
	b, err := json.Marshal(v)
	if err != nil {
		t.Fatal(err)
	}
	return answer{name: name, tool: tool, json: b}
}

// question is one question of the fixture: the tool it asks, whether its answer
// echoes the request's attributes, and how to ask a source.
type question struct {
	name string
	tool string
	echo bool
	ask  func(*Source) (any, error)
}

// questions are the questions allAnswers asks, of the fixture w, in the order it
// asks them.
func questions(w *world) []question {
	ids := func(fps ...interface{ String() string }) []string {
		var out []string
		for _, f := range fps {
			out = append(out, f.String())
		}
		return out
	}
	var out []question
	add := func(name, tool string, ask func(*Source) (any, error)) {
		out = append(out, question{name: name, tool: tool, ask: ask})
	}
	add("blast host", "blast_radius", func(s *Source) (any, error) {
		return s.BlastRadius(ctx, BlastRadiusInput{Entity: w.h1.String(), From: w10, To: w1030})
	})
	add("blast quarantined host", "blast_radius", func(s *Source) (any, error) {
		return s.BlastRadius(ctx, BlastRadiusInput{Entity: w.h2.String(), From: w10, To: w1030})
	})
	add("blast node at an instant, depth 1", "blast_radius", func(s *Source) (any, error) {
		return s.BlastRadius(ctx, BlastRadiusInput{Entity: w.n1.String(), At: "2026-01-01T10:08:00Z", Depth: 1})
	})
	add("diff", "what_changed", func(s *Source) (any, error) {
		return s.WhatChanged(ctx, WhatChangedInput{
			Scope: ids(w.n1, w.p1, w.p2, w.p3, w.p4, w.p6, w.c1, w.h1), From: w10, To: w1030,
		})
	})
	add("diff quarantined", "what_changed", func(s *Source) (any, error) {
		return s.WhatChanged(ctx, WhatChangedInput{Scope: ids(w.h2, w.n3, w.p5), From: w10, To: w1030})
	})
	add("co-residents", "co_residents", func(s *Source) (any, error) {
		return s.CoResidents(ctx, CoResidentsInput{Pod: w.p1.String(), From: w10, To: w1030})
	})
	add("co-residents on a quarantined host", "co_residents", func(s *Source) (any, error) {
		return s.CoResidents(ctx, CoResidentsInput{Pod: w.p5.String(), From: w10, To: w1030})
	})
	add("co-residents, node only", "co_residents", func(s *Source) (any, error) {
		return s.CoResidents(ctx, CoResidentsInput{Pod: w.p1.String(), From: w10, To: w1030, NodeOnly: true})
	})
	for _, e := range []struct {
		name string
		fp   interface{ String() string }
	}{{"node", w.n1}, {"quarantined host", w.h2}, {"host", w.h1}, {"pod", w.p1}} {
		add("neighbors of a "+e.name, "neighbors_at", func(s *Source) (any, error) {
			return s.NeighborsAt(ctx, NeighborsAtInput{Entity: e.fp.String(), At: "2026-01-01T10:08:00Z"})
		})
	}
	for _, e := range []struct {
		name string
		fp   interface{ String() string }
	}{{"pod", w.p1}, {"node", w.n1}, {"host", w.h1}, {"quarantined host", w.h2}, {"container", w.c1}, {"zone", w.z1}, {"deleted pod", w.p2}} {
		add("history of a "+e.name, "entity_history", func(s *Source) (any, error) {
			return s.EntityHistory(ctx, EntityHistoryInput{Entity: e.fp.String(), From: "2026-01-01T09:00:00Z", To: w1030})
		})
	}
	add("find pods by name at an instant", "find_entities", func(s *Source) (any, error) {
		return s.FindEntities(ctx, FindEntitiesInput{Type: "k8s.pod", Name: "web", At: "2026-01-01T10:08:00Z"})
	})
	add("find every node at an instant", "find_entities", func(s *Source) (any, error) {
		return s.FindEntities(ctx, FindEntitiesInput{Type: "k8s.node", At: w10})
	})
	add("find containers by namespace in the window", "find_entities", func(s *Source) (any, error) {
		return s.FindEntities(ctx, FindEntitiesInput{Type: "container", Namespace: "shop-cluster", From: w10, To: w1030})
	})
	add("resolve a host", "resolve_entity", func(s *Source) (any, error) {
		return s.ResolveEntity(ctx, ResolveEntityInput{
			Type: "host", Attributes: map[string]json.RawMessage{"host.id": json.RawMessage(`"` + hostSecret + `"`)}, At: w10,
		})
	})
	out[len(out)-1].echo = true
	add("schema", "describe_schema", func(s *Source) (any, error) {
		return s.DescribeSchema(ctx, DescribeSchemaInput{})
	})
	return out
}

// allAnswers asks every tool the questions of the fixture, the quarantined host
// included, and returns the answers in name order.
func allAnswers(t testing.TB, w *world, s *Source) []answer {
	t.Helper()
	var out []answer
	for _, q := range questions(w) {
		v, err := q.ask(s)
		a := marshal(t, q.name, q.tool, v, err)
		a.echo = q.echo
		out = append(out, a)
	}
	sort.SliceStable(out, func(i, j int) bool { return out[i].name < out[j].name })
	return out
}

// decode reads an answer into generic JSON.
func decode(t testing.TB, b []byte) map[string]any {
	t.Helper()
	var m map[string]any
	if err := json.Unmarshal(b, &m); err != nil {
		t.Fatal(err)
	}
	return m
}

func contains(xs []string, x string) bool {
	for _, y := range xs {
		if y == x {
			return true
		}
	}
	return false
}

func asError(err error, target **Error) bool { return errors.As(err, target) }

func rawAttrs(js string) map[string]json.RawMessage {
	var m map[string]json.RawMessage
	if err := json.Unmarshal([]byte(js), &m); err != nil {
		panic(err)
	}
	return m
}
