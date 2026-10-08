// Package catalog is the registry of entity and relation types.
//
// It is the single source of truth for which entity types exist, which
// attributes identify each one, and how each relation is directed. The
// identity layer reads it to know which keys to canonicalize and hash, ingest
// reads it to reject edges between the wrong types, and the query layer reads
// it for failure-propagation rules.
//
// A [Catalog] can only be built by [New], which checks every invariant and
// reports all violations at once, so a Catalog that exists is valid. It is
// immutable and safe for concurrent use. [Default] returns the shipped
// catalog.
//
// Entity types and identifying attributes that OpenTelemetry defines keep
// OpenTelemetry's names verbatim, so identities match other producers. Types
// OpenTelemetry lacks use a "topo." prefix.
package catalog

import (
	"iter"
	"slices"
	"strconv"
)

// Layer is the churn layer an entity lives in. The zero value is invalid.
type Layer uint8

const (
	// L0 is the physical fabric: racks, switches, ports. Churn: weeks.
	L0 Layer = iota + 1
	// L1 is host placement: hosts and zones. Churn: hours.
	L1
	// L2 is workload placement: nodes, pods, containers, processes. Churn:
	// seconds.
	L2
	// L3 is logical dependency between services.
	L3
)

func (l Layer) String() string {
	switch l {
	case L0:
		return "L0"
	case L1:
		return "L1"
	case L2:
		return "L2"
	case L3:
		return "L3"
	}
	return "Layer(" + strconv.Itoa(int(l)) + ")"
}

// Propagation says how failure travels along a relation. The zero value is
// invalid.
//
// For both [PropagateDown] and [PropagateUp] the failure of the "to" side
// degrades the "from" side. The two differ only in vocabulary: down is a
// substrate failing under its dependents (a host dying takes its pods with
// it), up is a callee failing under its callers.
type Propagation uint8

const (
	// PropagateDown means a substrate failure kills what runs on it.
	PropagateDown Propagation = iota + 1
	// PropagateUp means a callee failure hits its callers.
	PropagateUp
	// PropagateNone means the relation is structural or an aggregation, not
	// a failure path.
	PropagateNone
)

func (p Propagation) String() string {
	switch p {
	case PropagateDown:
		return "down"
	case PropagateUp:
		return "up"
	case PropagateNone:
		return "none"
	}
	return "Propagation(" + strconv.Itoa(int(p)) + ")"
}

// Storage says where a relation comes from. The zero value is invalid.
type Storage uint8

const (
	// StorageFrom means a producer asserts the relation and it is stored as a
	// fact on the "from" entity, the shorter-lived side, as OpenTelemetry
	// entity events recommend.
	StorageFrom Storage = iota + 1
	// StorageDerived means the relation is computed at read time from facts.
	// It is never written to the fact log.
	StorageDerived
)

func (s Storage) String() string {
	switch s {
	case StorageFrom:
		return "from"
	case StorageDerived:
		return "derived"
	}
	return "Storage(" + strconv.Itoa(int(s)) + ")"
}

// Kind is the value type of an identifying attribute. The identity layer
// converts every incoming value to its key's kind before hashing, so producers
// that disagree on integer versus string for one attribute still agree on one
// entity. The zero value is invalid.
type Kind uint8

const (
	// KindString is a UTF-8 string.
	KindString Kind = iota + 1
	// KindInt is a 64-bit signed integer.
	KindInt
	// KindTime is an instant in time.
	KindTime
)

func (k Kind) String() string {
	switch k {
	case KindString:
		return "string"
	case KindInt:
		return "int"
	case KindTime:
		return "time"
	}
	return "Kind(" + strconv.Itoa(int(k)) + ")"
}

// Key is one identifying attribute of an entity type.
type Key struct {
	// Name is the attribute name used in the identity hash.
	Name AttributeKey
	// Kind is the attribute's value type. An attribute name has one kind in
	// every entity type that uses it.
	Kind Kind
	// Optional marks a key whose absence is valid; an empty value normalizes
	// to absent. Every required key rejects empty values. OpenTelemetry
	// treats an empty service.namespace as an unspecified one, which is why
	// it is the only optional key in the shipped catalog.
	Optional bool
}

// Endpoint is one (from, to) pair of entity types that a relation connects.
type Endpoint struct {
	From, To EntityType
}

// EntityID is an entity type's stable number: it is part of every stored key
// of the type's entities. Zero is invalid. Ids are append-only: a new type
// takes a new number, and a number is never changed or reused, because stored
// keys outlive any one version of the catalog. An id is not derived from the
// type's position in the catalog, so reordering declarations renumbers
// nothing.
type EntityID uint16

// RelationID is a relation's stable number, with the same rules as [EntityID]
// and in a space of its own: entity id 1 and relation id 1 may coexist. A
// derived relation has an id too, though it is never stored, so that adding a
// stored relation after it renumbers nothing.
type RelationID uint16

// EntitySpec describes an entity type to [New]. Key order is the declaration
// order reported by [Entity.Keys]; the identity layer sorts keys itself, so
// order carries no meaning.
type EntitySpec struct {
	ID    EntityID
	Type  EntityType
	Layer Layer
	Keys  []Key
}

// RelationSpec describes a relation type to [New]. A relation with no
// endpoints is unconstrained: it may connect any two entity types.
type RelationSpec struct {
	ID          RelationID
	Type        RelationType
	Endpoints   []Endpoint
	Propagation Propagation
	Storage     Storage
}

// Entity is a registered entity type. It is a read-only view.
type Entity struct {
	id    EntityID
	typ   EntityType
	layer Layer
	keys  []Key
}

// ID returns the entity type's stable number.
func (e Entity) ID() EntityID { return e.id }

// Type returns the entity type name.
func (e Entity) Type() EntityType { return e.typ }

// Layer returns the churn layer the type lives in.
func (e Entity) Layer() Layer { return e.layer }

// Keys yields the identifying keys in declaration order.
func (e Entity) Keys() iter.Seq[Key] { return slices.Values(e.keys) }

// Key returns the identifying key with the given name.
func (e Entity) Key(name AttributeKey) (Key, bool) {
	for _, k := range e.keys {
		if k.Name == name {
			return k, true
		}
	}
	return Key{}, false
}

// Relation is a registered relation type. It is a read-only view.
type Relation struct {
	id          RelationID
	typ         RelationType
	endpoints   []Endpoint
	propagation Propagation
	storage     Storage
}

// ID returns the relation's stable number.
func (r Relation) ID() RelationID { return r.id }

// Type returns the relation type name.
func (r Relation) Type() RelationType { return r.typ }

// Endpoints yields the (from, to) pairs the relation connects, in
// declaration order. It yields nothing for an unconstrained relation.
func (r Relation) Endpoints() iter.Seq[Endpoint] { return slices.Values(r.endpoints) }

// Propagation returns how failure travels along the relation.
func (r Relation) Propagation() Propagation { return r.propagation }

// Storage returns where the relation comes from.
func (r Relation) Storage() Storage { return r.storage }

// Derived reports whether the relation is computed at read time and never
// stored as a fact.
func (r Relation) Derived() bool { return r.storage == StorageDerived }

// Allows reports whether an edge of this relation may connect an entity of
// type from to an entity of type to. An unconstrained relation allows any
// pair.
func (r Relation) Allows(from, to EntityType) bool {
	if len(r.endpoints) == 0 {
		return true
	}
	return slices.Contains(r.endpoints, Endpoint{From: from, To: to})
}

// Catalog is an immutable, validated registry of entity and relation types.
type Catalog struct {
	entities  []Entity
	relations []Relation
	entityIdx map[EntityType]int
	relIdx    map[RelationType]int

	// entityByID and relByID index the same slices by stable number.
	entityByID map[EntityID]int
	relByID    map[RelationID]int
}

// Entity returns the registered entity type with the given name.
func (c *Catalog) Entity(t EntityType) (Entity, bool) {
	i, ok := c.entityIdx[t]
	if !ok {
		return Entity{}, false
	}
	return c.entities[i], true
}

// Relation returns the registered relation type with the given name.
func (c *Catalog) Relation(t RelationType) (Relation, bool) {
	i, ok := c.relIdx[t]
	if !ok {
		return Relation{}, false
	}
	return c.relations[i], true
}

// EntityByID returns the registered entity type with the given stable number.
// Zero and unassigned numbers are not found.
func (c *Catalog) EntityByID(id EntityID) (Entity, bool) {
	i, ok := c.entityByID[id]
	if !ok {
		return Entity{}, false
	}
	return c.entities[i], true
}

// RelationByID returns the registered relation with the given stable number.
// Zero and unassigned numbers are not found.
func (c *Catalog) RelationByID(id RelationID) (Relation, bool) {
	i, ok := c.relByID[id]
	if !ok {
		return Relation{}, false
	}
	return c.relations[i], true
}

// Entities yields every entity type in declaration order, which is stable
// across runs.
func (c *Catalog) Entities() iter.Seq[Entity] { return slices.Values(c.entities) }

// Relations yields every relation type in declaration order, which is stable
// across runs.
func (c *Catalog) Relations() iter.Seq[Relation] { return slices.Values(c.relations) }
