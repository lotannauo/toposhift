package catalog_test

import (
	"bytes"
	"encoding/json"
	"flag"
	"os"
	"testing"

	"github.com/lotannauo/toposhift/internal/catalog"
)

var update = flag.Bool("update", false, "rewrite testdata/ids_v1.json from the shipped catalog")

const idsFile = "testdata/ids_v1.json"

type idsDoc struct {
	Entities  map[catalog.EntityType]catalog.EntityID     `json:"entities"`
	Relations map[catalog.RelationType]catalog.RelationID `json:"relations"`
	Layers    map[catalog.Layer]uint8                     `json:"layers"`
}

// The numeric ids are in every stored key. A change to the shipped catalog
// that renumbers anything fails here instead of silently orphaning stored
// data. Adding a type or relation with the next unused number is the one
// change that keeps every existing id, and still needs the file regenerated:
//
//	go test ./internal/catalog -run TestIDsAreFrozen -update
func TestIDsAreFrozen(t *testing.T) {
	c := catalog.Default()
	doc := idsDoc{
		Entities:  map[catalog.EntityType]catalog.EntityID{},
		Relations: map[catalog.RelationType]catalog.RelationID{},
		Layers:    map[catalog.Layer]uint8{},
	}
	// The ids are read through the lookups as well as ID(), so what is
	// frozen is the number a store would resolve, not only the number the
	// declaration carries.
	for e := range c.Entities() {
		got, ok := c.EntityByID(e.ID())
		if !ok || got.Type() != e.Type() {
			t.Fatalf("EntityByID(%d) = %q, %v; want %q", e.ID(), got.Type(), ok, e.Type())
		}
		doc.Entities[got.Type()] = got.ID()
	}
	for r := range c.Relations() {
		got, ok := c.RelationByID(r.ID())
		if !ok || got.Type() != r.Type() {
			t.Fatalf("RelationByID(%d) = %q, %v; want %q", r.ID(), got.Type(), ok, r.Type())
		}
		doc.Relations[got.Type()] = got.ID()
	}
	for _, l := range []catalog.Layer{catalog.L0, catalog.L1, catalog.L2, catalog.L3} {
		doc.Layers[l] = uint8(l)
	}
	got, err := json.MarshalIndent(doc, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	got = append(got, '\n')
	if *update {
		if err := os.WriteFile(idsFile, got, 0o644); err != nil {
			t.Fatal(err)
		}
		return
	}
	want, err := os.ReadFile(idsFile)
	if err != nil {
		t.Fatalf("%v (create it with -update)", err)
	}
	if !bytes.Equal(got, want) {
		t.Fatalf("the numeric ids of the catalog no longer match %s.\n"+
			"Every stored key carries them, so renumbering orphans stored data. If the change only\n"+
			"appends a type or relation, regenerate the file with -update; otherwise stop.\n got: %s\nwant: %s", idsFile, got, want)
	}
}

// TestIDsMatchDeclarationOrderToday records a fact about the shipped catalog,
// not a rule: its ids happen to be 1, 2, 3... in declaration order. Code that
// still numbers by position, such as the storage benchmarks, agrees with the
// catalog only while this holds. A type appended with the next number keeps it
// true; the rule that matters is the frozen file above.
func TestIDsMatchDeclarationOrderToday(t *testing.T) {
	t.Parallel()

	c := catalog.Default()
	want := catalog.EntityID(1)
	for e := range c.Entities() {
		if e.ID() != want {
			t.Errorf("entity %s has id %d, want %d", e.Type(), e.ID(), want)
		}
		want++
	}
	wantRel := catalog.RelationID(1)
	for r := range c.Relations() {
		if r.ID() != wantRel {
			t.Errorf("relation %s has id %d, want %d", r.Type(), r.ID(), wantRel)
		}
		wantRel++
	}
}

func TestDefaultLookupByIDRoundTrips(t *testing.T) {
	t.Parallel()

	c := catalog.Default()
	for e := range c.Entities() {
		got, ok := c.EntityByID(e.ID())
		if !ok || got.Type() != e.Type() {
			t.Errorf("EntityByID(%d) = %q, %v; want %q", e.ID(), got.Type(), ok, e.Type())
		}
	}
	for r := range c.Relations() {
		got, ok := c.RelationByID(r.ID())
		if !ok || got.Type() != r.Type() {
			t.Errorf("RelationByID(%d) = %q, %v; want %q", r.ID(), got.Type(), ok, r.Type())
		}
	}
	for _, id := range []catalog.EntityID{0, 12, 0xFFFF} {
		if got, ok := c.EntityByID(id); ok {
			t.Errorf("EntityByID(%d) = %q, want none", id, got.Type())
		}
	}
	for _, id := range []catalog.RelationID{0, 8, 0xFFFF} {
		if got, ok := c.RelationByID(id); ok {
			t.Errorf("RelationByID(%d) = %q, want none", id, got.Type())
		}
	}
}
