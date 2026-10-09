package pebblekv

import (
	"encoding/binary"
	"encoding/json"
	"errors"
	"os"
	"slices"
	"testing"

	"github.com/lotannauo/toposhift/internal/catalog"
	"github.com/lotannauo/toposhift/internal/identity"
	"github.com/lotannauo/toposhift/internal/store"
)

// idsDoc is the shape of the catalog's frozen ids file.
type idsDoc struct {
	Entities  map[catalog.EntityType]catalog.EntityID     `json:"entities"`
	Relations map[catalog.RelationType]catalog.RelationID `json:"relations"`
	Layers    map[catalog.Layer]uint8                     `json:"layers"`
}

// The numeric ids are in every stored key. The catalog freezes them in
// testdata/ids_v1.json (its own test fails when they change); this checks that
// the bytes this package puts in keys are those same numbers, so the key encoding
// cannot drift from the file by numbering another way.
func TestKeyIDsAreTheFrozenCatalogIDs(t *testing.T) {
	t.Parallel()
	raw, err := os.ReadFile("../../catalog/testdata/ids_v1.json")
	if err != nil {
		t.Fatal(err)
	}
	var doc idsDoc
	if err := json.Unmarshal(raw, &doc); err != nil {
		t.Fatal(err)
	}
	if len(doc.Entities) == 0 || len(doc.Relations) == 0 || len(doc.Layers) == 0 {
		t.Fatalf("the frozen ids file is empty: %+v", doc)
	}
	ids := Default

	for typ, want := range doc.Entities {
		id, ok := ids.EntityID(typ)
		if !ok || id != want {
			t.Errorf("entity %s has id %d (found %v), the file says %d", typ, id, ok, want)
		}
		fp, err := identity.FingerprintFromHash(typ, [identity.FingerprintBytes]byte{9})
		if err != nil {
			t.Fatal(err)
		}
		key, err := ids.AppendFingerprint(nil, fp)
		if err != nil {
			t.Fatal(err)
		}
		if got := binary.BigEndian.Uint16(key); catalog.EntityID(got) != want {
			t.Errorf("the key of an entity %s starts with id %d, the file says %d", typ, got, want)
		}
	}
	for rel, want := range doc.Relations {
		key, err := ids.AppendRelation(nil, rel)
		if err != nil {
			t.Fatal(err)
		}
		if len(key) != 2 || catalog.RelationID(binary.BigEndian.Uint16(key)) != want {
			t.Errorf("the key of relation %s is %x, the file says %d", rel, key, want)
		}
	}
	for layer, want := range doc.Layers {
		if got := LayerByte(layer); got != want {
			t.Errorf("layer %s has byte %d, the file says %d", layer, got, want)
		}
	}

	// Nothing the file does not know, and in the file's order of ids.
	if got, want := len(ids.EntityTypes()), len(doc.Entities); got != want {
		t.Errorf("%d entity types are numbered, the file has %d", got, want)
	}
	if got, want := len(ids.Relations()), len(doc.Relations); got != want {
		t.Errorf("%d relations are numbered, the file has %d", got, want)
	}
}

// Ids come from the catalog's stable numbers and not from where a type is
// declared: a catalog that declares its types in another order, with the same
// ids, numbers them the same way, and the lists come in id order.
func TestIDsFollowTheCatalogNotDeclarationOrder(t *testing.T) {
	t.Parallel()
	key := func(name catalog.AttributeKey) []catalog.Key {
		return []catalog.Key{{Name: name, Kind: catalog.KindString}}
	}
	entities := []catalog.EntitySpec{
		{ID: 1, Type: "alpha", Layer: catalog.L0, Keys: key("alpha.id")},
		{ID: 2, Type: "beta", Layer: catalog.L1, Keys: key("beta.id")},
		{ID: 7, Type: "gamma", Layer: catalog.L2, Keys: key("gamma.id")},
	}
	relations := []catalog.RelationSpec{
		{ID: 1, Type: "near", Storage: catalog.StorageFrom, Propagation: catalog.PropagateDown, Endpoints: []catalog.Endpoint{{From: "alpha", To: "beta"}}},
		{ID: 5, Type: "far", Storage: catalog.StorageFrom, Propagation: catalog.PropagateDown, Endpoints: []catalog.Endpoint{{From: "beta", To: "gamma"}}},
	}
	build := func(es []catalog.EntitySpec, rs []catalog.RelationSpec) *IDs {
		c, err := catalog.New(es, rs)
		if err != nil {
			t.Fatal(err)
		}
		return NewIDs(c)
	}
	inOrder := build(entities, relations)
	swapped := build(
		[]catalog.EntitySpec{entities[2], entities[0], entities[1]},
		[]catalog.RelationSpec{relations[1], relations[0]},
	)
	for name, ids := range map[string]*IDs{"declared in id order": inOrder, "declared out of order": swapped} {
		for typ, want := range map[catalog.EntityType]catalog.EntityID{"alpha": 1, "beta": 2, "gamma": 7} {
			if id, ok := ids.EntityID(typ); !ok || id != want {
				t.Errorf("%s: entity %s has id %d (found %v), want %d", name, typ, id, ok, want)
			}
			if back, ok := ids.EntityType(want); !ok || back != typ {
				t.Errorf("%s: entity id %d is %q, want %q", name, want, back, typ)
			}
		}
		for rel, want := range map[catalog.RelationType]catalog.RelationID{"near": 1, "far": 5} {
			if id, ok := ids.RelationID(rel); !ok || id != want {
				t.Errorf("%s: relation %s has id %d (found %v), want %d", name, rel, id, ok, want)
			}
			if back, ok := ids.Relation(want); !ok || back != rel {
				t.Errorf("%s: relation id %d is %q, want %q", name, want, back, rel)
			}
		}
		if got := ids.EntityTypes(); !slices.Equal(got, []catalog.EntityType{"alpha", "beta", "gamma"}) {
			t.Errorf("%s: entity types in id order = %v", name, got)
		}
		if got := ids.Relations(); !slices.Equal(got, []catalog.RelationType{"near", "far"}) {
			t.Errorf("%s: relations in id order = %v", name, got)
		}
		// Ids that are not assigned stay unassigned, though there are three types.
		if typ, ok := ids.EntityType(3); ok {
			t.Errorf("%s: entity id 3 is %q, want none", name, typ)
		}
	}
}

func TestIDsRoundTrip(t *testing.T) {
	t.Parallel()
	ids := Default
	for _, typ := range ids.EntityTypes() {
		id, ok := ids.EntityID(typ)
		back, ok2 := ids.EntityType(id)
		if !ok || !ok2 || back != typ {
			t.Errorf("entity %s: id %d, back %q", typ, id, back)
		}
	}
	for _, rel := range ids.Relations() {
		id, ok := ids.RelationID(rel)
		back, ok2 := ids.Relation(id)
		if !ok || !ok2 || back != rel {
			t.Errorf("relation %s: id %d, back %q", rel, id, back)
		}
	}
	for _, id := range []catalog.EntityID{0, catalog.EntityID(len(ids.EntityTypes()) + 1), 0xFFFF} {
		if typ, ok := ids.EntityType(id); ok {
			t.Errorf("entity id %d is %q, want none", id, typ)
		}
	}
	for _, id := range []catalog.RelationID{0, catalog.RelationID(len(ids.Relations()) + 1), 0xFFFF} {
		if rel, ok := ids.Relation(id); ok {
			t.Errorf("relation id %d is %q, want none", id, rel)
		}
	}
	if _, ok := ids.EntityID("nonesuch"); ok {
		t.Error("an unknown entity type has an id")
	}
	if _, ok := ids.RelationID("nonesuch"); ok {
		t.Error("an unknown relation has an id")
	}
}

func TestFingerprintKeyRoundTrips(t *testing.T) {
	t.Parallel()
	ids := Default
	res := identity.NewResolver(catalog.Default())
	id, err := res.Resolve(catalog.K8sPod, []identity.Attr{{Key: catalog.K8sPodUID, Value: "pod-1"}})
	if err != nil {
		t.Fatal(err)
	}
	fp := id.Fingerprint()
	key, err := ids.AppendFingerprint([]byte{0xAA}, fp)
	if err != nil {
		t.Fatal(err)
	}
	if len(key) != 1+FingerprintKeyLen || key[0] != 0xAA {
		t.Fatalf("appended %x, want a one byte prefix kept and %d bytes added", key, FingerprintKeyLen)
	}
	typeID, _ := ids.EntityID(catalog.K8sPod)
	if got := catalog.EntityID(uint16(key[1])<<8 | uint16(key[2])); got != typeID {
		t.Errorf("type id bytes = %d, want %d", got, typeID)
	}
	back, err := ids.Fingerprint(key[1:])
	if err != nil || back != fp {
		t.Fatalf("Fingerprint(%x) = %v, %v; want %s", key[1:], back, err, fp)
	}

	// Bytes that are not a fingerprint are errors, not a wrong fingerprint.
	if _, err := ids.Fingerprint(key[1 : len(key)-1]); err == nil {
		t.Error("a short key was read as a fingerprint")
	}
	bad := append([]byte(nil), key[1:]...)
	bad[0], bad[1] = 0xFF, 0xFF
	if _, err := ids.Fingerprint(bad); err == nil {
		t.Error("an unknown type id was read as a fingerprint")
	}

	// A type the catalog does not know cannot be stored, and says so as an
	// invalid record.
	stranger, err := identity.FingerprintFromHash("nonesuch", [identity.FingerprintBytes]byte{1})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := ids.AppendFingerprint(nil, stranger); !errors.Is(err, store.ErrInvalid) {
		t.Errorf("AppendFingerprint(unknown type) = %v, want ErrInvalid", err)
	}
	if _, err := ids.AppendRelation(nil, "nonesuch"); !errors.Is(err, store.ErrInvalid) {
		t.Errorf("AppendRelation(unknown) = %v, want ErrInvalid", err)
	}
}

func TestLayerByte(t *testing.T) {
	t.Parallel()
	for _, l := range []catalog.Layer{catalog.L0, catalog.L1, catalog.L2, catalog.L3} {
		back, ok := LayerFromByte(LayerByte(l))
		if !ok || back != l {
			t.Errorf("layer %s round-trips to %s, %v", l, back, ok)
		}
		if LayerByte(l) == 0 {
			t.Errorf("layer %s uses byte 0, which meta keys own", l)
		}
	}
	for _, b := range []byte{0, 5, 0xFF} {
		if l, ok := LayerFromByte(b); ok {
			t.Errorf("byte %d is layer %s, want none", b, l)
		}
	}
}
