package identity

import (
	"cmp"
	"errors"
	"slices"
	"strings"
	"time"

	"github.com/lotannauo/toposhift/internal/catalog"
)

// Identity is an entity's full identity: its fingerprint and the canonical
// bytes the fingerprint was hashed from. It is immutable and comparable. The
// zero value is invalid.
type Identity struct {
	fp    Fingerprint
	canon string
}

// Type returns the entity type.
func (id Identity) Type() catalog.EntityType { return id.fp.typ }

// Fingerprint returns the handle consumers hold.
func (id Identity) Fingerprint() Fingerprint { return id.fp }

// IsZero reports whether id is the zero, invalid identity.
func (id Identity) IsZero() bool { return id == Identity{} }

// Canonical returns the canonical bytes, the form to persist and compare. See
// the package documentation for the encoding.
func (id Identity) Canonical() string { return id.canon }

// String returns the fingerprint.
func (id Identity) String() string { return id.fp.String() }

// Attrs returns the identifying attributes in name order, with typed values:
// string, int64, or time.Time in UTC. An optional attribute that is absent is
// not returned.
func (id Identity) Attrs() []Attr {
	_, entries, err := decode(id.canon)
	if err != nil {
		return nil // only the zero Identity: a non-zero one holds valid bytes
	}
	attrs := make([]Attr, len(entries))
	for i, e := range entries {
		attrs[i] = Attr{Key: e.name, Value: e.v.native()}
	}
	return attrs
}

func (v value) native() any {
	switch v.kind {
	case catalog.KindString:
		return v.s
	case catalog.KindInt:
		return v.i
	default:
		return time.Unix(v.i, int64(v.ns)).UTC()
	}
}

// Option configures a [Resolver].
type Option func(*Resolver)

// WithLenient makes the resolver ignore attributes that are not identifying
// keys of the entity type, instead of rejecting them. Use it when passing a
// producer's whole attribute set. Every other rule still applies.
func WithLenient() Option { return func(r *Resolver) { r.lenient = true } }

func withHash(h hashFunc) Option { return func(r *Resolver) { r.hash = h } }

// Resolver turns attributes into identities for the entity types of one
// catalog. It is immutable and safe for concurrent use.
type Resolver struct {
	plans   map[catalog.EntityType]*plan
	lenient bool
	hash    hashFunc // nil means SHA-256; set only by tests
}

// sumOf hashes a preimage, given both as bytes and as the string made from
// them. The default path hashes buf directly: calling an unknown function
// with buf would force the caller's stack buffer onto the heap.
func (r *Resolver) sumOf(buf []byte, canon string) [FingerprintBytes]byte {
	if r.hash != nil {
		return r.hash([]byte(canon))
	}
	return sha256Prefix(buf)
}

// plan is what the resolver needs about an entity type, computed once: its
// keys in the bytewise name order the encoding requires.
type plan struct {
	keys []planKey
}

type planKey struct {
	name     catalog.AttributeKey
	kind     catalog.Kind
	optional bool
}

func (p *plan) index(name catalog.AttributeKey) int {
	for i := range p.keys {
		if p.keys[i].name == name {
			return i
		}
	}
	return -1
}

// NewResolver builds a resolver for the entity types in c.
func NewResolver(c *catalog.Catalog, opts ...Option) *Resolver {
	r := &Resolver{plans: make(map[catalog.EntityType]*plan)}
	for e := range c.Entities() {
		p := &plan{}
		for k := range e.Keys() {
			p.keys = append(p.keys, planKey{name: k.Name, kind: k.Kind, optional: k.Optional})
		}
		slices.SortFunc(p.keys, func(a, b planKey) int {
			return cmp.Compare(string(a.name), string(b.name))
		})
		r.plans[e.Type()] = p
	}
	for _, o := range opts {
		o(r)
	}
	return r
}

// maxStackKeys is how many keys Resolve handles without allocating. Every
// shipped entity type has three or fewer.
const maxStackKeys = 8

// slot is the state of one planned key while resolving.
type slot struct {
	seen    bool // the key appeared in the input
	present bool // it has a value (an optional key given as "" has none)
	v       value
}

// Resolve converts attrs to the keys of entity type t and returns the
// resulting identity. It reports every violation it finds, joined; each
// unwraps to an [*AttrError] and one of this package's sentinel errors.
func (r *Resolver) Resolve(t catalog.EntityType, attrs []Attr) (Identity, error) {
	p, ok := r.plans[t]
	if !ok {
		return Identity{}, &AttrError{Type: t, Err: ErrUnknownType}
	}

	var stack [maxStackKeys]slot
	slots := stack[:0]
	if len(p.keys) > len(stack) {
		slots = make([]slot, 0, len(p.keys))
	}
	slots = slots[:len(p.keys)]

	var errs []error
	report := func(key catalog.AttributeKey, e *AttrError) {
		e.Type, e.Key = t, key
		errs = append(errs, e)
	}

	for _, a := range attrs {
		i := p.index(a.Key)
		if i < 0 {
			if !r.lenient {
				report(a.Key, fail(ErrUnregistered, ""))
			}
			continue
		}
		if slots[i].seen {
			report(a.Key, fail(ErrDuplicateKey, ""))
			continue
		}
		slots[i].seen = true

		if s, isString := a.Value.(string); isString {
			if s == "" && p.keys[i].optional {
				continue // absent
			}
			if strings.TrimSpace(s) == "" {
				report(a.Key, fail(ErrEmpty, ""))
				continue
			}
		}
		v, err := convert(p.keys[i].kind, a.Value)
		if err != nil {
			report(a.Key, err)
			continue
		}
		slots[i].present, slots[i].v = true, v
	}

	present := 0
	for i, k := range p.keys {
		switch {
		case slots[i].present:
			present++
		case !slots[i].seen && !k.optional:
			report(k.name, fail(ErrMissing, ""))
		}
	}
	if len(errs) > 0 {
		return Identity{}, errors.Join(errs...)
	}

	var scratch [256]byte
	buf := appendHeader(scratch[:0], t, present)
	for i, k := range p.keys {
		if slots[i].present {
			buf = appendEntry(buf, k.name, slots[i].v)
		}
	}
	canon := string(buf)
	return Identity{
		fp:    Fingerprint{typ: t, sum: r.sumOf(buf, canon)},
		canon: canon,
	}, nil
}

// Parse reads back canonical bytes that [Identity.Canonical] produced,
// typically from the store. It accepts only the exact canonical encoding and
// only identities valid under the resolver's catalog: a known entity type,
// only its identifying keys, each with its declared kind, and every required
// key present. The fingerprint is recomputed, never trusted.
func (r *Resolver) Parse(canonical string) (Identity, error) {
	t, entries, err := decode(canonical)
	if err != nil {
		return Identity{}, err
	}
	p, ok := r.plans[t]
	if !ok {
		return Identity{}, &AttrError{Type: t, Err: ErrUnknownType}
	}

	var errs []error
	report := func(key catalog.AttributeKey, err error, detail string) {
		errs = append(errs, &AttrError{Type: t, Key: key, Err: err, Detail: detail})
	}
	seen := make([]bool, len(p.keys))
	for _, e := range entries {
		i := p.index(e.name)
		switch {
		case i < 0:
			report(e.name, ErrUnregistered, "")
		case p.keys[i].kind != e.v.kind:
			report(e.name, ErrType, "stored as "+e.v.kind.String()+", declared "+p.keys[i].kind.String())
		default:
			seen[i] = true
		}
	}
	for i, k := range p.keys {
		if !seen[i] && !k.optional {
			report(k.name, ErrMissing, "")
		}
	}
	if len(errs) > 0 {
		return Identity{}, errors.Join(errs...)
	}
	return Identity{
		fp:    Fingerprint{typ: t, sum: r.sumOf([]byte(canonical), canonical)},
		canon: canonical,
	}, nil
}
