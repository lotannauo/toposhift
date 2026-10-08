package catalog

import (
	"errors"
	"fmt"
	"regexp"
	"slices"
)

// Sentinel errors wrapped by the errors [New] returns, so callers can tell
// the kind of violation with [errors.Is].
var (
	// ErrInvalid marks a malformed or missing value.
	ErrInvalid = errors.New("invalid")
	// ErrDuplicate marks a name or id used twice in the same scope.
	ErrDuplicate = errors.New("duplicate")
	// ErrUnknown marks a reference to an entity type that is not registered.
	ErrUnknown = errors.New("unknown entity type")
)

// namePattern is the syntax of every type, relation and attribute name; see
// [EntityType.Valid].
var namePattern = regexp.MustCompile(`^[a-z][a-z0-9_]*(\.[a-z0-9_]+)*$`)

func validName(s string) bool { return len(s) <= MaxNameLen && namePattern.MatchString(s) }

// New builds a Catalog from the given specs. It copies its inputs, so later
// changes to the specs do not affect the Catalog.
//
// New checks every invariant and returns all violations joined, not just the
// first:
//   - every name matches the name syntax and is at most MaxNameLen bytes;
//   - entity names are unique, and so are relation names;
//   - every entity id and relation id is non-zero, entity ids are unique among
//     entities, and relation ids are unique among relations (the two spaces
//     are separate, so entity id 1 and relation id 1 may coexist);
//   - layers, propagations, storages and key kinds are valid (non-zero and
//     known);
//   - an attribute name has the same kind in every entity type that uses it;
//   - an entity has at least one key, no duplicate key names, and at least
//     one required key (an entity whose keys are all optional could have an
//     empty identity, and every instance would then fingerprint the same);
//   - a stored relation has at least one endpoint;
//   - every endpoint names a registered entity type, and no endpoint repeats.
func New(entities []EntitySpec, relations []RelationSpec) (*Catalog, error) {
	var errs []error
	c := &Catalog{
		entities:  make([]Entity, 0, len(entities)),
		relations: make([]Relation, 0, len(relations)),
		entityIdx: make(map[EntityType]int, len(entities)),
		relIdx:    make(map[RelationType]int, len(relations)),

		entityByID: make(map[EntityID]int, len(entities)),
		relByID:    make(map[RelationID]int, len(relations)),
	}

	kinds := make(map[AttributeKey]keyKind)
	for _, s := range entities {
		errs = append(errs, checkEntity(s)...)
		errs = append(errs, checkKindConsistency(s, kinds)...)
		_, idDup := c.entityByID[s.ID]
		if idDup && s.ID != 0 {
			errs = append(errs, fmt.Errorf("entity %q: id %d: %w", s.Type, s.ID, ErrDuplicate))
		}
		if _, dup := c.entityIdx[s.Type]; dup {
			errs = append(errs, fmt.Errorf("entity %q: %w", s.Type, ErrDuplicate))
			continue
		}
		if s.ID != 0 && !idDup {
			c.entityByID[s.ID] = len(c.entities)
		}
		c.entityIdx[s.Type] = len(c.entities)
		c.entities = append(c.entities, Entity{id: s.ID, typ: s.Type, layer: s.Layer, keys: slices.Clone(s.Keys)})
	}

	for _, s := range relations {
		errs = append(errs, checkRelation(s, c.entityIdx)...)
		_, idDup := c.relByID[s.ID]
		if idDup && s.ID != 0 {
			errs = append(errs, fmt.Errorf("relation %q: id %d: %w", s.Type, s.ID, ErrDuplicate))
		}
		if _, dup := c.relIdx[s.Type]; dup {
			errs = append(errs, fmt.Errorf("relation %q: %w", s.Type, ErrDuplicate))
			continue
		}
		if s.ID != 0 && !idDup {
			c.relByID[s.ID] = len(c.relations)
		}
		c.relIdx[s.Type] = len(c.relations)
		c.relations = append(c.relations, Relation{
			id:          s.ID,
			typ:         s.Type,
			endpoints:   slices.Clone(s.Endpoints),
			propagation: s.Propagation,
			storage:     s.Storage,
		})
	}

	if err := errors.Join(errs...); err != nil {
		return nil, fmt.Errorf("catalog: %w", err)
	}
	return c, nil
}

func checkEntity(s EntitySpec) []error {
	var errs []error
	fail := func(err error, format string, args ...any) {
		errs = append(errs, fmt.Errorf("entity %q: %s: %w", s.Type, fmt.Sprintf(format, args...), err))
	}

	if !s.Type.Valid() {
		fail(ErrInvalid, "name does not match %s", namePattern)
	}
	if s.ID == 0 {
		fail(ErrInvalid, "id 0")
	}
	if s.Layer < L0 || s.Layer > L3 {
		fail(ErrInvalid, "layer %s", s.Layer)
	}
	if len(s.Keys) == 0 {
		fail(ErrInvalid, "no identifying keys")
	}

	seen := make(map[AttributeKey]bool, len(s.Keys))
	required := 0
	for _, k := range s.Keys {
		if !k.Name.Valid() {
			fail(ErrInvalid, "key %q: name does not match %s", k.Name, namePattern)
		}
		if k.Kind < KindString || k.Kind > KindTime {
			fail(ErrInvalid, "key %q: kind %s", k.Name, k.Kind)
		}
		if seen[k.Name] {
			fail(ErrDuplicate, "key %q", k.Name)
		}
		seen[k.Name] = true
		if !k.Optional {
			required++
		}
	}
	if len(s.Keys) > 0 && required == 0 {
		fail(ErrInvalid, "all keys are optional, so the identity could be empty")
	}
	return errs
}

func checkRelation(s RelationSpec, entities map[EntityType]int) []error {
	var errs []error
	fail := func(err error, format string, args ...any) {
		errs = append(errs, fmt.Errorf("relation %q: %s: %w", s.Type, fmt.Sprintf(format, args...), err))
	}

	if !s.Type.Valid() {
		fail(ErrInvalid, "name does not match %s", namePattern)
	}
	if s.ID == 0 {
		fail(ErrInvalid, "id 0")
	}
	if s.Propagation < PropagateDown || s.Propagation > PropagateNone {
		fail(ErrInvalid, "propagation %s", s.Propagation)
	}
	if s.Storage < StorageFrom || s.Storage > StorageDerived {
		fail(ErrInvalid, "storage %s", s.Storage)
	}
	if s.Storage == StorageFrom && len(s.Endpoints) == 0 {
		fail(ErrInvalid, "a stored relation needs at least one endpoint")
	}

	seen := make(map[Endpoint]bool, len(s.Endpoints))
	for _, ep := range s.Endpoints {
		for _, t := range []EntityType{ep.From, ep.To} {
			if _, ok := entities[t]; !ok {
				fail(ErrUnknown, "endpoint %s -> %s references %q", ep.From, ep.To, t)
			}
		}
		if seen[ep] {
			fail(ErrDuplicate, "endpoint %s -> %s", ep.From, ep.To)
		}
		seen[ep] = true
	}
	return errs
}

// keyKind remembers the kind an attribute name was first declared with, and by
// which entity type, so a later conflicting declaration can name both.
type keyKind struct {
	kind   Kind
	entity EntityType
}

func checkKindConsistency(s EntitySpec, first map[AttributeKey]keyKind) []error {
	var errs []error
	for _, k := range s.Keys {
		prev, ok := first[k.Name]
		switch {
		case !ok:
			first[k.Name] = keyKind{kind: k.Kind, entity: s.Type}
		case prev.kind != k.Kind:
			errs = append(errs, fmt.Errorf(
				"entity %q: key %q: kind %s conflicts with kind %s declared by entity %q: %w",
				s.Type, k.Name, k.Kind, prev.kind, prev.entity, ErrInvalid))
		}
	}
	return errs
}
