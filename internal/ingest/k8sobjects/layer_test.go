package k8sobjects_test

import (
	"testing"

	"github.com/lotannauo/toposhift/internal/catalog"
	"github.com/lotannauo/toposhift/internal/store"
)

// Every record the golden fixtures produce is stored in the layer the catalog
// records: an edge in its relation's layer, an entity in its type's layer.
func TestRecordLayersAgreeWithTheCatalog(t *testing.T) {
	t.Parallel()

	c := catalog.Default()
	edges, entities := 0, 0
	for _, name := range []string{"run1", "run2"} {
		t.Run(name, func(t *testing.T) {
			in := loadCapture(t, name+".jsonl")
			for i, r := range translateAll(t, newTranslator(t, 0), in) {
				for _, rec := range r.Records {
					switch rec.Subject.Kind {
					case store.SubjectEdge:
						edges++
						rel, ok := c.Relation(rec.Subject.Relation)
						if !ok {
							t.Fatalf("%s: relation %q is not in the catalog", in[i].Name, rec.Subject.Relation)
						}
						if rec.Layer != rel.Layer() {
							t.Errorf("%s: %s edge in layer %s, catalog says %s", in[i].Name, rel.Type(), rec.Layer, rel.Layer())
						}
					case store.SubjectEntity:
						entities++
						e, ok := c.Entity(rec.Subject.A.Type())
						if !ok {
							t.Fatalf("%s: entity type %q is not in the catalog", in[i].Name, rec.Subject.A.Type())
						}
						if rec.Layer != e.Layer() {
							t.Errorf("%s: %s entity in layer %s, catalog says %s", in[i].Name, e.Type(), rec.Layer, e.Layer())
						}
					}
				}
			}
		})
	}
	// Both kinds must appear, or the check above proved nothing.
	if edges == 0 || entities == 0 {
		t.Fatalf("the fixtures gave %d edge and %d entity records", edges, entities)
	}
}
