package pebblekv_test

import (
	"bytes"
	"encoding/json"
	"errors"
	"flag"
	"os"
	"testing"

	"github.com/lotannauo/toposhift/bench/spike/engine"
	"github.com/lotannauo/toposhift/bench/spike/pebblekv"
	"github.com/lotannauo/toposhift/internal/catalog"
	"github.com/lotannauo/toposhift/internal/identity"
)

var update = flag.Bool("update", false, "rewrite testdata/ids_v1.json from the shipped catalog")

const idsFile = "testdata/ids_v1.json"

type idsDoc struct {
	Entities  map[catalog.EntityType]uint16   `json:"entities"`
	Relations map[catalog.RelationType]uint16 `json:"relations"`
	Layers    map[catalog.Layer]byte          `json:"layers"`
}

// The numeric ids are in every stored key. This is the standing reminder of the
// plan's prerequisite that the catalog carry stable ids: until it does, a
// catalog change that renumbers anything fails here instead of silently
// orphaning stored data. Adding a type or relation at the end is the one change
// that keeps every existing id, and still needs the file regenerated:
//
//	go test ./spike/pebblekv -run TestIDsAreFrozen -update
func TestIDsAreFrozen(t *testing.T) {
	doc := idsDoc{
		Entities:  map[catalog.EntityType]uint16{},
		Relations: map[catalog.RelationType]uint16{},
		Layers:    map[catalog.Layer]byte{},
	}
	for i, e := range pebblekv.Default.EntityTypes() {
		doc.Entities[e] = uint16(i + 1)
	}
	for i, r := range pebblekv.Default.Relations() {
		doc.Relations[r] = uint16(i + 1)
	}
	for _, l := range []catalog.Layer{catalog.L0, catalog.L1, catalog.L2, catalog.L3} {
		doc.Layers[l] = pebblekv.LayerByte(l)
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

func TestIDsRoundTrip(t *testing.T) {
	t.Parallel()
	ids := pebblekv.Default
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
	for _, id := range []uint16{0, uint16(len(ids.EntityTypes()) + 1), 0xFFFF} {
		if typ, ok := ids.EntityType(id); ok {
			t.Errorf("entity id %d is %q, want none", id, typ)
		}
		if rel, ok := ids.Relation(id + 100); ok {
			t.Errorf("relation id %d is %q, want none", id+100, rel)
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
	ids := pebblekv.Default
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
	if len(key) != 1+pebblekv.FingerprintKeyLen || key[0] != 0xAA {
		t.Fatalf("appended %x, want a one byte prefix kept and %d bytes added", key, pebblekv.FingerprintKeyLen)
	}
	typeID, _ := ids.EntityID(catalog.K8sPod)
	if got := uint16(key[1])<<8 | uint16(key[2]); got != typeID {
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
	if _, err := ids.AppendFingerprint(nil, stranger); !errors.Is(err, engine.ErrInvalid) {
		t.Errorf("AppendFingerprint(unknown type) = %v, want ErrInvalid", err)
	}
	if _, err := ids.AppendRelation(nil, "nonesuch"); !errors.Is(err, engine.ErrInvalid) {
		t.Errorf("AppendRelation(unknown) = %v, want ErrInvalid", err)
	}
}

func TestLayerByte(t *testing.T) {
	t.Parallel()
	for _, l := range []catalog.Layer{catalog.L0, catalog.L1, catalog.L2, catalog.L3} {
		back, ok := pebblekv.LayerFromByte(pebblekv.LayerByte(l))
		if !ok || back != l {
			t.Errorf("layer %s round-trips to %s, %v", l, back, ok)
		}
		if pebblekv.LayerByte(l) == 0 {
			t.Errorf("layer %s uses byte 0, which meta keys own", l)
		}
	}
	for _, b := range []byte{0, 5, 0xFF} {
		if l, ok := pebblekv.LayerFromByte(b); ok {
			t.Errorf("byte %d is layer %s, want none", b, l)
		}
	}
}
