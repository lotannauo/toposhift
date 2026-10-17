package mcp

import (
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

var update = flag.Bool("update", false, "rewrite the golden answers in testdata")

var ctx = context.Background()

const (
	w10   = "2026-01-01T10:00:00Z"
	w1030 = "2026-01-01T10:30:00Z"
)

func TestToolsTable(t *testing.T) {
	tools := Tools()
	want := []string{"blast_radius", "what_changed", "co_residents", "service_context", "neighbors_at", "entity_history", "find_entities", "resolve_entity", "describe_schema"}
	if len(tools) != len(want) {
		t.Fatalf("%d tools, want %d", len(tools), len(want))
	}
	for i, tl := range tools {
		if tl.Name != want[i] {
			t.Errorf("tool %d is %q, want %q", i, tl.Name, want[i])
		}
		if tl.Description == "" || !strings.HasSuffix(tl.Description, ".") {
			t.Errorf("%s: the description is not a sentence: %q", tl.Name, tl.Description)
		}
		low := strings.ToLower(tl.Description)
		for _, banned := range []string{"assistant", "model", "claude", "llm"} {
			if strings.Contains(low, banned) {
				t.Errorf("%s: the description says %q; it says \"client\"", tl.Name, banned)
			}
		}
	}
	isRange := map[string]bool{"blast_radius": true, "what_changed": true, "co_residents": true, "service_context": true, "find_entities": true}
	for _, tl := range tools {
		if tl.Range != isRange[tl.Name] {
			t.Errorf("%s: Range is %t", tl.Name, tl.Range)
		}
	}
}

// goldenAnswers are the answers every tool gives on the world fixture. They are
// regenerated with -update only when the shape of an answer changes on purpose.
func goldenAnswers(t *testing.T, w *world, s *Source) map[string]any {
	t.Helper()
	must := func(v any, err error) any {
		t.Helper()
		if err != nil {
			t.Fatal(err)
		}
		return v
	}
	return map[string]any{
		"blast_radius": must(s.BlastRadius(ctx, BlastRadiusInput{Entity: w.h1.String(), From: w10, To: w1030})),
		"what_changed": must(s.WhatChanged(ctx, WhatChangedInput{
			Scope: []string{w.n1.String(), w.p1.String(), w.p2.String(), w.p3.String(), w.p4.String(), w.p6.String()},
			From:  w10, To: w1030,
		})),
		"co_residents":   must(s.CoResidents(ctx, CoResidentsInput{Pod: w.p1.String(), From: w10, To: w1030})),
		"neighbors_at":   must(s.NeighborsAt(ctx, NeighborsAtInput{Entity: w.n1.String(), At: "2026-01-01T10:08:00Z"})),
		"entity_history": must(s.EntityHistory(ctx, EntityHistoryInput{Entity: w.p1.String(), From: "2026-01-01T09:00:00Z", To: w1030})),
		"find_entities":  must(s.FindEntities(ctx, FindEntitiesInput{Type: "k8s.pod", Name: "WEB", From: w10, To: w1030})),
		"resolve_entity": must(s.ResolveEntity(ctx, ResolveEntityInput{
			Type: "host", Attributes: map[string]json.RawMessage{"host.id": json.RawMessage(`"` + hostSecret + `"`)}, At: w10,
		})),
		"describe_schema": must(s.DescribeSchema(ctx, DescribeSchemaInput{})),
	}
}

func TestGoldenAnswers(t *testing.T) {
	w := newWorld(t, rankObjs)
	s := w.source(t, rankObjs, true)
	for name, v := range goldenAnswers(t, w, s) {
		t.Run(name, func(t *testing.T) {
			got, err := json.MarshalIndent(v, "", "  ")
			if err != nil {
				t.Fatal(err)
			}
			got = append(got, '\n')
			path := filepath.Join("testdata", name+".golden.json")
			if *update {
				if err := os.MkdirAll("testdata", 0o755); err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(path, got, 0o644); err != nil {
					t.Fatal(err)
				}
				return
			}
			want, err := os.ReadFile(path)
			if err != nil {
				t.Fatalf("%v (run with -update to write it)", err)
			}
			if !bytes.Equal(got, want) {
				t.Errorf("%s differs from %s; run with -update if the shape changed on purpose.\ngot:\n%s", name, path, got)
			}
		})
	}
}
