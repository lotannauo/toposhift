// Package pebblekv is what the Pebble-backed storage candidates share: the
// numeric forms of entity types, relations and layers that keys are built from,
// the value codec, the way a database is opened, and the liveness rule a layout
// applies to the versions it reads.
//
// The plumbing itself (opening and tuning, the layouts, the value codec, the
// ids, the meta keys, the settle and the iterator recording) is the root
// module's internal/store/pebblekv, so that the benchmarks measure the code the
// product store runs on. What is here is what only a measurement needs: the cockroachkvs
// layout, the waits that make a size repeatable, the snapshot and description of
// a database, the canonical rewrite and the digest, and the conversion of the
// benchmarks' records to the root's values and ids.
package pebblekv

import (
	"fmt"

	"github.com/lotannauo/toposhift/bench/spike/engine"
	"github.com/lotannauo/toposhift/internal/catalog"
	"github.com/lotannauo/toposhift/internal/identity"
	rootkv "github.com/lotannauo/toposhift/internal/store/pebblekv"
)

// FingerprintKeyLen is the bytes a fingerprint takes in a key: a 16-bit entity
// type id and the digest.
const FingerprintKeyLen = rootkv.FingerprintKeyLen

// IDs maps entity types and relations to the small integers keys carry. It is
// the root module's [rootkv.IDs] with the ids as plain integers, which is what
// the layouts' keys hold, and with the benchmarks' [engine.ErrInvalid] for a type
// the catalog does not know.
type IDs struct {
	root *rootkv.IDs
}

// NewIDs numbers the types and relations of a catalog by their stable ids.
func NewIDs(c *catalog.Catalog) *IDs { return &IDs{root: rootkv.NewIDs(c)} }

// Default numbers the shipped catalog.
var Default = NewIDs(catalog.Default())

// EntityID returns the id of an entity type.
func (ids *IDs) EntityID(t catalog.EntityType) (uint16, bool) {
	id, ok := ids.root.EntityID(t)
	return uint16(id), ok
}

// EntityType returns the entity type with the given id.
func (ids *IDs) EntityType(id uint16) (catalog.EntityType, bool) {
	return ids.root.EntityType(catalog.EntityID(id))
}

// RelationID returns the id of a relation.
func (ids *IDs) RelationID(r catalog.RelationType) (uint16, bool) {
	id, ok := ids.root.RelationID(r)
	return uint16(id), ok
}

// Relation returns the relation with the given id.
func (ids *IDs) Relation(id uint16) (catalog.RelationType, bool) {
	return ids.root.Relation(catalog.RelationID(id))
}

// EntityTypes lists the entity types in id order, the lowest id first.
func (ids *IDs) EntityTypes() []catalog.EntityType { return ids.root.EntityTypes() }

// Relations lists the relations in id order, the lowest id first.
func (ids *IDs) Relations() []catalog.RelationType { return ids.root.Relations() }

// AppendFingerprint appends the key form of fp: the entity type's id, big
// endian, then the digest. It wraps [engine.ErrInvalid] for a type the catalog
// does not know, which no engine can store.
func (ids *IDs) AppendFingerprint(dst []byte, fp identity.Fingerprint) ([]byte, error) {
	if _, ok := ids.root.EntityID(fp.Type()); !ok {
		return dst, fmt.Errorf("entity type %q is not in the catalog: %w", fp.Type(), engine.ErrInvalid)
	}
	return ids.root.AppendFingerprint(dst, fp)
}

// Fingerprint is the inverse of [IDs.AppendFingerprint], reading the first
// [FingerprintKeyLen] bytes of b.
func (ids *IDs) Fingerprint(b []byte) (identity.Fingerprint, error) { return ids.root.Fingerprint(b) }

// AppendRelation appends the key form of a relation: its id, big endian. It wraps
// [engine.ErrInvalid] for a relation the catalog does not know.
func (ids *IDs) AppendRelation(dst []byte, r catalog.RelationType) ([]byte, error) {
	if _, ok := ids.root.RelationID(r); !ok {
		return dst, fmt.Errorf("relation %q is not in the catalog: %w", r, engine.ErrInvalid)
	}
	return ids.root.AppendRelation(dst, r)
}

// LayerByte is the key form of a layer: its number, 1 to 4. Zero is left for
// keys that are not data (see the layouts' meta keys).
func LayerByte(l catalog.Layer) byte { return rootkv.LayerByte(l) }

// LayerFromByte is the inverse of [LayerByte].
func LayerFromByte(b byte) (catalog.Layer, bool) { return rootkv.LayerFromByte(b) }
