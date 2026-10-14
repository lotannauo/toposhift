package k8sobjects

import (
	"testing"

	"github.com/lotannauo/toposhift/internal/catalog"
	"github.com/lotannauo/toposhift/internal/identity"
	"github.com/lotannauo/toposhift/internal/store"
)

// subjectFP is a fingerprint of the given type. Only the type matters to the
// layer of a record, so the hash is a fixed byte.
func subjectFP(t *testing.T, typ catalog.EntityType) identity.Fingerprint {
	t.Helper()
	var h [identity.FingerprintBytes]byte
	h[0] = 1
	fp, err := identity.FingerprintFromHash(typ, h)
	if err != nil {
		t.Fatal(err)
	}
	return fp
}

// An edge is stored in the layer the catalog records for its relation, for
// every endpoint pair the relation allows, whatever the layer of the source
// entity.
func TestRecordLayerOfAnEdgeIsTheCatalogsRelationLayer(t *testing.T) {
	t.Parallel()

	stored := 0
	for r := range catalog.Default().Relations() {
		if r.Derived() {
			continue
		}
		stored++
		endpoints := 0
		for ep := range r.Endpoints() {
			endpoints++
			subj := store.EdgeSubject(subjectFP(t, ep.From), subjectFP(t, ep.To), r.Type())
			if got := recordLayer(subj); got != r.Layer() {
				t.Errorf("%s %s -> %s: layer %s, want %s", r.Type(), ep.From, ep.To, got, r.Layer())
			}
		}
		if endpoints == 0 {
			t.Errorf("%s: stored relation with no endpoint, nothing was checked", r.Type())
		}
	}
	if stored == 0 {
		t.Fatal("no stored relation was checked")
	}
}

// An entity is stored in the layer of its type, whatever relations exist.
func TestRecordLayerOfAnEntityMatchesItsType(t *testing.T) {
	t.Parallel()

	n := 0
	for e := range catalog.Default().Entities() {
		n++
		if got := recordLayer(store.EntitySubject(subjectFP(t, e.Type()))); got != e.Layer() {
			t.Errorf("%s: layer %s, want %s", e.Type(), got, e.Layer())
		}
	}
	if n == 0 {
		t.Fatal("no entity type was checked")
	}
}

// A translator emits only registered stored relations, so anything else is a
// programming error and stops.
func TestRecordLayerRefusesAnUnstoredRelation(t *testing.T) {
	t.Parallel()

	for _, rel := range []catalog.RelationType{"no_such_relation", catalog.SameAs} {
		func() {
			defer func() {
				if recover() == nil {
					t.Errorf("recordLayer of relation %s did not panic", rel)
				}
			}()
			recordLayer(store.EdgeSubject(subjectFP(t, catalog.K8sPod), subjectFP(t, catalog.K8sNode), rel))
		}()
	}
}
