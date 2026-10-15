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

// allAnswers asks every tool the questions of the fixture, the quarantined host
// included, and returns the answers in name order.
func allAnswers(t testing.TB, w *world, s *Source) []answer {
	t.Helper()
	ids := func(fps ...interface{ String() string }) []string {
		var out []string
		for _, f := range fps {
			out = append(out, f.String())
		}
		return out
	}
	var out []answer
	add := func(name, tool string, v any, err error) { out = append(out, marshal(t, name, tool, v, err)) }
	{
		v, err := s.BlastRadius(ctx, BlastRadiusInput{Entity: w.h1.String(), From: w10, To: w1030})
		add("blast host", "blast_radius", v, err)
	}
	{
		v, err := s.BlastRadius(ctx, BlastRadiusInput{Entity: w.h2.String(), From: w10, To: w1030})
		add("blast quarantined host", "blast_radius", v, err)
	}
	{
		v, err := s.BlastRadius(ctx, BlastRadiusInput{Entity: w.n1.String(), At: "2026-01-01T10:08:00Z", Depth: 1})
		add("blast node at an instant, depth 1", "blast_radius", v, err)
	}
	{
		v, err := s.WhatChanged(ctx, WhatChangedInput{
			Scope: ids(w.n1, w.p1, w.p2, w.p3, w.p4, w.p6, w.c1, w.h1), From: w10, To: w1030,
		})
		add("diff", "what_changed", v, err)
	}
	{
		v, err := s.WhatChanged(ctx, WhatChangedInput{Scope: ids(w.h2, w.n3, w.p5), From: w10, To: w1030})
		add("diff quarantined", "what_changed", v, err)
	}
	{
		v, err := s.CoResidents(ctx, CoResidentsInput{Pod: w.p1.String(), From: w10, To: w1030})
		add("co-residents", "co_residents", v, err)
	}
	{
		v, err := s.CoResidents(ctx, CoResidentsInput{Pod: w.p5.String(), From: w10, To: w1030})
		add("co-residents on a quarantined host", "co_residents", v, err)
	}
	{
		v, err := s.CoResidents(ctx, CoResidentsInput{Pod: w.p1.String(), From: w10, To: w1030, NodeOnly: true})
		add("co-residents, node only", "co_residents", v, err)
	}
	for _, e := range []struct {
		name string
		fp   interface{ String() string }
	}{{"node", w.n1}, {"quarantined host", w.h2}, {"host", w.h1}, {"pod", w.p1}} {
		v, err := s.NeighborsAt(ctx, NeighborsAtInput{Entity: e.fp.String(), At: "2026-01-01T10:08:00Z"})
		add("neighbors of a "+e.name, "neighbors_at", v, err)
	}
	for _, e := range []struct {
		name string
		fp   interface{ String() string }
	}{{"pod", w.p1}, {"node", w.n1}, {"host", w.h1}, {"quarantined host", w.h2}, {"container", w.c1}, {"zone", w.z1}, {"deleted pod", w.p2}} {
		v, err := s.EntityHistory(ctx, EntityHistoryInput{Entity: e.fp.String(), From: "2026-01-01T09:00:00Z", To: w1030})
		add("history of a "+e.name, "entity_history", v, err)
	}
	{
		v, err := s.ResolveEntity(ctx, ResolveEntityInput{
			Type: "host", Attributes: map[string]json.RawMessage{"host.id": json.RawMessage(`"` + hostSecret + `"`)}, At: w10,
		})
		a := marshal(t, "resolve a host", "resolve_entity", v, err)
		a.echo = true
		out = append(out, a)
	}
	{
		v, err := s.DescribeSchema(ctx, DescribeSchemaInput{})
		add("schema", "describe_schema", v, err)
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
