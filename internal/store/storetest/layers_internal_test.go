package storetest

import (
	"testing"

	"github.com/lotannauo/toposhift/internal/catalog"
)

// The generator keeps its own switch from a relation to its layer, because the
// store digests it feeds are frozen. The switch must name the layer the
// catalog records for every stored relation, so that an edge it draws lands
// where a reader of the catalog looks for it.
func TestGeneratorLayersAgreeWithTheCatalog(t *testing.T) {
	t.Parallel()

	stored := 0
	for r := range catalog.Default().Relations() {
		if r.Derived() {
			continue
		}
		stored++
		if got := layerOf(r.Type()); got != r.Layer() {
			t.Errorf("layerOf(%s) = %s, catalog says %s", r.Type(), got, r.Layer())
		}
	}
	entities := 0
	for e := range catalog.Default().Entities() {
		entities++
		if got := entityLayer(e.Type()); got != e.Layer() {
			t.Errorf("entityLayer(%s) = %s, catalog says %s", e.Type(), got, e.Layer())
		}
	}
	if stored == 0 || entities == 0 {
		t.Fatalf("checked %d relations and %d entity types", stored, entities)
	}
}
