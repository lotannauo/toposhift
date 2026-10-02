// Package pebblekv is what the Pebble-backed storage candidates share: the
// numeric forms of entity types, relations and layers that keys are built from,
// the value codec, the way a database is opened, and the liveness rule a layout
// applies to the versions it reads.
//
// It is the spike's, not the product's. The product's store needs the same
// things, but its numeric ids must come from a catalog that carries stable ids
// (a prerequisite the plan records); here they are derived from the shipped
// catalog's declaration order and pinned by a golden test, so a catalog change
// that would renumber stored keys fails loudly instead of silently orphaning
// them.
package pebblekv

import (
	"encoding/binary"
	"fmt"

	"github.com/lotannauo/toposhift/bench/spike/engine"
	"github.com/lotannauo/toposhift/internal/catalog"
	"github.com/lotannauo/toposhift/internal/identity"
)

// FingerprintKeyLen is the bytes a fingerprint takes in a key: a 16-bit entity
// type id and the digest.
const FingerprintKeyLen = 2 + identity.FingerprintBytes

// IDs maps entity types and relations to the small integers keys carry.
type IDs struct {
	entityID     map[catalog.EntityType]uint16
	entityName   []catalog.EntityType // entityName[id-1]
	relationID   map[catalog.RelationType]uint16
	relationName []catalog.RelationType // relationName[id-1]
}

// NewIDs numbers the catalog's entity types and relations from 1 in declaration
// order, which the catalog promises is stable across runs. Appending a type to
// the catalog gives it the next id and renumbers nothing; reordering or removing
// one renumbers, and the golden test is how that is noticed.
func NewIDs(c *catalog.Catalog) *IDs {
	ids := &IDs{
		entityID:   make(map[catalog.EntityType]uint16),
		relationID: make(map[catalog.RelationType]uint16),
	}
	for e := range c.Entities() {
		ids.entityName = append(ids.entityName, e.Type())
		ids.entityID[e.Type()] = uint16(len(ids.entityName))
	}
	for r := range c.Relations() {
		ids.relationName = append(ids.relationName, r.Type())
		ids.relationID[r.Type()] = uint16(len(ids.relationName))
	}
	return ids
}

// Default numbers the shipped catalog.
var Default = NewIDs(catalog.Default())

// EntityID returns the id of an entity type.
func (ids *IDs) EntityID(t catalog.EntityType) (uint16, bool) {
	id, ok := ids.entityID[t]
	return id, ok
}

// EntityType returns the entity type with the given id.
func (ids *IDs) EntityType(id uint16) (catalog.EntityType, bool) {
	if id == 0 || int(id) > len(ids.entityName) {
		return "", false
	}
	return ids.entityName[id-1], true
}

// RelationID returns the id of a relation.
func (ids *IDs) RelationID(r catalog.RelationType) (uint16, bool) {
	id, ok := ids.relationID[r]
	return id, ok
}

// Relation returns the relation with the given id.
func (ids *IDs) Relation(id uint16) (catalog.RelationType, bool) {
	if id == 0 || int(id) > len(ids.relationName) {
		return "", false
	}
	return ids.relationName[id-1], true
}

// EntityTypes lists the entity types in id order, id 1 first.
func (ids *IDs) EntityTypes() []catalog.EntityType {
	return append([]catalog.EntityType(nil), ids.entityName...)
}

// Relations lists the relations in id order, id 1 first.
func (ids *IDs) Relations() []catalog.RelationType {
	return append([]catalog.RelationType(nil), ids.relationName...)
}

// AppendFingerprint appends the key form of fp: the entity type's id, big
// endian, then the digest. It wraps [engine.ErrInvalid] for a type the catalog
// does not know, which no engine can store.
func (ids *IDs) AppendFingerprint(dst []byte, fp identity.Fingerprint) ([]byte, error) {
	id, ok := ids.entityID[fp.Type()]
	if !ok {
		return dst, fmt.Errorf("entity type %q is not in the catalog: %w", fp.Type(), engine.ErrInvalid)
	}
	dst = binary.BigEndian.AppendUint16(dst, id)
	h := fp.Hash()
	return append(dst, h[:]...), nil
}

// Fingerprint is the inverse of [IDs.AppendFingerprint], reading the first
// [FingerprintKeyLen] bytes of b.
func (ids *IDs) Fingerprint(b []byte) (identity.Fingerprint, error) {
	if len(b) < FingerprintKeyLen {
		return identity.Fingerprint{}, fmt.Errorf("fingerprint key is %d bytes, want %d", len(b), FingerprintKeyLen)
	}
	typ, ok := ids.EntityType(binary.BigEndian.Uint16(b))
	if !ok {
		return identity.Fingerprint{}, fmt.Errorf("entity type id %d is not in the catalog", binary.BigEndian.Uint16(b))
	}
	return identity.FingerprintFromHash(typ, [identity.FingerprintBytes]byte(b[2:FingerprintKeyLen]))
}

// AppendRelation appends the key form of a relation: its id, big endian.
func (ids *IDs) AppendRelation(dst []byte, r catalog.RelationType) ([]byte, error) {
	id, ok := ids.relationID[r]
	if !ok {
		return dst, fmt.Errorf("relation %q is not in the catalog: %w", r, engine.ErrInvalid)
	}
	return binary.BigEndian.AppendUint16(dst, id), nil
}

// LayerByte is the key form of a layer: its number, 1 to 4. Zero is left for
// keys that are not data (see the layouts' meta keys).
func LayerByte(l catalog.Layer) byte { return byte(l) }

// LayerFromByte is the inverse of [LayerByte].
func LayerFromByte(b byte) (catalog.Layer, bool) {
	l := catalog.Layer(b)
	return l, l >= catalog.L0 && l <= catalog.L3
}
