package catalog_test

import (
	"bytes"
	"encoding/json"
	"flag"
	"os"
	"testing"

	"github.com/lotannauo/toposhift/internal/catalog"
)

var updateLayers = flag.Bool("update-layers", false, "rewrite testdata/relation_layers_v1.json from the shipped catalog")

const relationLayersFile = "testdata/relation_layers_v1.json"

// The layer of a stored relation is the first byte of every stored key of its
// edges, so a change to it orphans stored edges. It is frozen here as the
// stored number (L0 is 1, L1 is 2, L2 is 3, L3 is 4), not as a name. A change
// to a layer needs a decision recorded in writing and a migration of stored
// edges; regenerating the file is the last step of that, never the first:
//
//	go test ./internal/catalog -run TestRelationLayersAreFrozen -update-layers
func TestRelationLayersAreFrozen(t *testing.T) {
	doc := map[catalog.RelationType]uint8{}
	for r := range catalog.Default().Relations() {
		if r.Derived() {
			if r.Layer() != 0 {
				t.Errorf("%s: derived relation has layer %s", r.Type(), r.Layer())
			}
			continue
		}
		doc[r.Type()] = uint8(r.Layer())
	}
	got, err := json.MarshalIndent(doc, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	got = append(got, '\n')
	if *updateLayers {
		if err := os.WriteFile(relationLayersFile, got, 0o644); err != nil {
			t.Fatal(err)
		}
		return
	}
	want, err := os.ReadFile(relationLayersFile)
	if err != nil {
		t.Fatalf("%v (create it with -update-layers)", err)
	}
	if !bytes.Equal(got, want) {
		t.Fatalf("the layers of the catalog's relations no longer match %s.\n"+
			"The layer is the first byte of every stored key of a relation's edges, so changing it\n"+
			"orphans stored edges. Stop unless the change comes with a migration.\n got: %s\nwant: %s", relationLayersFile, got, want)
	}
}

// TestRelationLayerRule checks the documented rule: an edge is stored in the
// layer of its "from" entity type, for every endpoint of the relation, except
// for the relations listed in overrides. Adding an exception means adding it
// here, where review sees it.
func TestRelationLayerRule(t *testing.T) {
	t.Parallel()

	overrides := map[catalog.RelationType]catalog.Layer{
		// Both sources are L2 entities, but binding a workload to its host is
		// host placement.
		catalog.RunsOn: catalog.L1,
	}
	checkRelationLayerRule(t, catalog.Default(), overrides)
}

func checkRelationLayerRule(t *testing.T, c *catalog.Catalog, overrides map[catalog.RelationType]catalog.Layer) {
	t.Helper()

	for r := range c.Relations() {
		if r.Derived() {
			continue
		}
		override, overridden := overrides[r.Type()]
		differs := false
		for ep := range r.Endpoints() {
			from, ok := c.Entity(ep.From)
			if !ok {
				t.Errorf("%s: endpoint source %s is not registered", r.Type(), ep.From)
				continue
			}
			if from.Layer() != r.Layer() {
				differs = true
			}
			want := from.Layer()
			if overridden {
				want = override
			}
			if r.Layer() != want {
				t.Errorf("%s from %s: layer %s, want %s", r.Type(), ep.From, r.Layer(), want)
			}
		}
		if overridden && !differs {
			t.Errorf("%s: the override to %s does not differ from the rule; remove it", r.Type(), override)
		}
	}
	for typ := range overrides {
		if _, ok := c.Relation(typ); !ok {
			t.Errorf("override for %s, which is not registered", typ)
		}
	}
}
