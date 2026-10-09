package pebblekv

import (
	"encoding/binary"
	"fmt"
	"slices"

	"github.com/lotannauo/toposhift/internal/catalog"
	"github.com/lotannauo/toposhift/internal/identity"
	"github.com/lotannauo/toposhift/internal/store"
)

// FingerprintKeyLen is the bytes a fingerprint takes in a key: a 16-bit entity
// type id and the digest.
const FingerprintKeyLen = 2 + identity.FingerprintBytes

// IDs maps entity types and relations to the small integers keys carry. The
// numbers are the catalog's own stable ids ([catalog.Entity.ID],
// [catalog.Relation.ID]), which are append-only and independent of declaration
// order, so reordering the catalog's declarations renumbers nothing.
type IDs struct {
	cat *catalog.Catalog
	// entityTypes and relations list the catalog's types in id order.
	entityTypes []catalog.EntityType
	relations   []catalog.RelationType
}

// NewIDs numbers the types and relations of a catalog by their stable ids.
func NewIDs(c *catalog.Catalog) *IDs {
	ids := &IDs{cat: c}
	var entities []catalog.Entity
	for e := range c.Entities() {
		entities = append(entities, e)
	}
	slices.SortFunc(entities, func(a, b catalog.Entity) int { return int(a.ID()) - int(b.ID()) })
	for _, e := range entities {
		ids.entityTypes = append(ids.entityTypes, e.Type())
	}
	var relations []catalog.Relation
	for r := range c.Relations() {
		relations = append(relations, r)
	}
	slices.SortFunc(relations, func(a, b catalog.Relation) int { return int(a.ID()) - int(b.ID()) })
	for _, r := range relations {
		ids.relations = append(ids.relations, r.Type())
	}
	return ids
}

// Default numbers the shipped catalog.
var Default = NewIDs(catalog.Default())

// EntityID returns the id of an entity type.
func (ids *IDs) EntityID(t catalog.EntityType) (catalog.EntityID, bool) {
	e, ok := ids.cat.Entity(t)
	if !ok {
		return 0, false
	}
	return e.ID(), true
}

// EntityType returns the entity type with the given id.
func (ids *IDs) EntityType(id catalog.EntityID) (catalog.EntityType, bool) {
	e, ok := ids.cat.EntityByID(id)
	if !ok {
		return "", false
	}
	return e.Type(), true
}

// RelationID returns the id of a relation.
func (ids *IDs) RelationID(r catalog.RelationType) (catalog.RelationID, bool) {
	rel, ok := ids.cat.Relation(r)
	if !ok {
		return 0, false
	}
	return rel.ID(), true
}

// Relation returns the relation with the given id.
func (ids *IDs) Relation(id catalog.RelationID) (catalog.RelationType, bool) {
	rel, ok := ids.cat.RelationByID(id)
	if !ok {
		return "", false
	}
	return rel.Type(), true
}

// EntityTypes lists the entity types in id order, the lowest id first.
func (ids *IDs) EntityTypes() []catalog.EntityType { return slices.Clone(ids.entityTypes) }

// Relations lists the relations in id order, the lowest id first. A derived
// relation is listed: it has an id, though no record of it is ever stored.
func (ids *IDs) Relations() []catalog.RelationType { return slices.Clone(ids.relations) }

// AppendFingerprint appends the key form of fp: the entity type's id, big
// endian, then the digest. It wraps [store.ErrInvalid] for a type the catalog
// does not know, which no store can hold.
func (ids *IDs) AppendFingerprint(dst []byte, fp identity.Fingerprint) ([]byte, error) {
	id, ok := ids.EntityID(fp.Type())
	if !ok {
		return dst, fmt.Errorf("entity type %q is not in the catalog: %w", fp.Type(), store.ErrInvalid)
	}
	dst = binary.BigEndian.AppendUint16(dst, uint16(id))
	h := fp.Hash()
	return append(dst, h[:]...), nil
}

// Fingerprint is the inverse of [IDs.AppendFingerprint], reading the first
// [FingerprintKeyLen] bytes of b.
func (ids *IDs) Fingerprint(b []byte) (identity.Fingerprint, error) {
	if len(b) < FingerprintKeyLen {
		return identity.Fingerprint{}, fmt.Errorf("fingerprint key is %d bytes, want %d", len(b), FingerprintKeyLen)
	}
	id := catalog.EntityID(binary.BigEndian.Uint16(b))
	typ, ok := ids.EntityType(id)
	if !ok {
		return identity.Fingerprint{}, fmt.Errorf("entity type id %d is not in the catalog", id)
	}
	return identity.FingerprintFromHash(typ, [identity.FingerprintBytes]byte(b[2:FingerprintKeyLen]))
}

// AppendRelation appends the key form of a relation: its id, big endian. It wraps
// [store.ErrInvalid] for a relation the catalog does not know. Whether a relation
// may be stored at all (a derived one may not) is the store's rule, not the
// encoding's.
func (ids *IDs) AppendRelation(dst []byte, r catalog.RelationType) ([]byte, error) {
	id, ok := ids.RelationID(r)
	if !ok {
		return dst, fmt.Errorf("relation %q is not in the catalog: %w", r, store.ErrInvalid)
	}
	return binary.BigEndian.AppendUint16(dst, uint16(id)), nil
}

// LayerByte is the key form of a layer: its number, 1 to 4. Zero is left for
// keys that are not data (see the layouts' meta keys).
func LayerByte(l catalog.Layer) byte { return byte(l) }

// LayerFromByte is the inverse of [LayerByte].
func LayerFromByte(b byte) (catalog.Layer, bool) {
	l := catalog.Layer(b)
	return l, l >= catalog.L0 && l <= catalog.L3
}
