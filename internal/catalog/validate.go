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
	// ErrDuplicate marks a name used twice in the same scope.
	ErrDuplicate = errors.New("duplicate")
	// ErrUnknown marks a reference to an entity type that is not registered.
	ErrUnknown = errors.New("unknown entity type")
)

// namePattern is the syntax of every type, relation and attribute name:
// lowercase dotted segments. It excludes ":" and whitespace, because a
// fingerprint is the entity type, a colon, then a hash, and names also end up
// in storage keys and on the wire.
var namePattern = regexp.MustCompile(`^[a-z][a-z0-9_]*(\.[a-z0-9_]+)*$`)

// New builds a Catalog from the given specs. It copies its inputs, so later
// changes to the specs do not affect the Catalog.
//
// New checks every invariant and returns all violations joined, not just the
// first:
//   - every name matches the name syntax;
//   - entity names are unique, and so are relation names;
//   - layers, propagations and storages are valid (non-zero and known);
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
	}

	for _, s := range entities {
		errs = append(errs, checkEntity(s)...)
		if _, dup := c.entityIdx[s.Type]; dup {
			errs = append(errs, fmt.Errorf("entity %q: %w", s.Type, ErrDuplicate))
			continue
		}
		c.entityIdx[s.Type] = len(c.entities)
		c.entities = append(c.entities, Entity{typ: s.Type, layer: s.Layer, keys: slices.Clone(s.Keys)})
	}

	for _, s := range relations {
		errs = append(errs, checkRelation(s, c.entityIdx)...)
		if _, dup := c.relIdx[s.Type]; dup {
			errs = append(errs, fmt.Errorf("relation %q: %w", s.Type, ErrDuplicate))
			continue
		}
		c.relIdx[s.Type] = len(c.relations)
		c.relations = append(c.relations, Relation{
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

	if !namePattern.MatchString(string(s.Type)) {
		fail(ErrInvalid, "name does not match %s", namePattern)
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
		if !namePattern.MatchString(string(k.Name)) {
			fail(ErrInvalid, "key %q: name does not match %s", k.Name, namePattern)
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

	if !namePattern.MatchString(string(s.Type)) {
		fail(ErrInvalid, "name does not match %s", namePattern)
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
