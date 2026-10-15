package mcp

import (
	"bytes"
	"cmp"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/lotannauo/toposhift/internal/catalog"
	"github.com/lotannauo/toposhift/internal/identity"
	"github.com/lotannauo/toposhift/internal/query/interval"
	"github.com/lotannauo/toposhift/internal/store"
)

// ResolveEntityInput is the input of the resolve_entity tool.
type ResolveEntityInput struct {
	// Type is the entity type, such as "host" or "k8s.pod".
	Type string `json:"type"`
	// Attributes are exactly the identifying attributes of the type, by name: a
	// string as a JSON string, an integer as a JSON integer, a time as an RFC 3339
	// string. Nothing is searched for, trimmed or case-folded.
	Attributes map[string]json.RawMessage `json:"attributes"`
	// At, if given, asks whether the entity exists at that instant.
	At string `json:"at,omitempty"`
	// AsOf is a token from an earlier answer, to read the same world again.
	AsOf string `json:"asof,omitempty"`
}

// ResolveEntityOutput is the answer of the resolve_entity tool. An id is a hash of
// the exact identifying attributes: it says nothing about whether the entity
// exists unless the answer has an existence.
type ResolveEntityOutput struct {
	// Token is set only when the store was read.
	Token   string `json:"token,omitempty"`
	Derived bool   `json:"derived"`
	Policy  Policy `json:"policy"`
	Entity  Ref    `json:"entity"`
	// Attributes echoes the attributes the request gave, and no others.
	Attributes map[string]any `json:"attributes"`
	Existence  *ExistenceAt   `json:"existence,omitempty"`
	Notes      []string       `json:"notes"`
}

// ExistenceAt is whether an entity exists at an instant.
type ExistenceAt struct {
	At string `json:"at"`
	// Status is "alive", "not_alive" or "quarantined".
	Status      string      `json:"status"`
	Sentence    string      `json:"sentence"`
	Subject     string      `json:"subject"`
	Basis       string      `json:"basis"`
	Rule        string      `json:"rule"`
	Quarantined *Quarantine `json:"quarantined,omitempty"`
}

// ResolveEntity computes the id of an entity from its identifying attributes.
// It resolves exactly: a missing, extra, empty or mistyped attribute is refused
// and named, and it never searches.
func (s *Source) ResolveEntity(ctx context.Context, in ResolveEntityInput) (ResolveEntityOutput, error) {
	needsStore := in.At != ""
	if !needsStore {
		// No store is read: neither a slot nor a deadline is needed.
		out, err := s.resolve(ctx, in)
		if err != nil {
			return ResolveEntityOutput{}, s.fail(err)
		}
		return out, nil
	}
	return run(s, ctx, false, func(ctx context.Context) (ResolveEntityOutput, error) { return s.resolve(ctx, in) })
}

func (s *Source) resolve(ctx context.Context, in ResolveEntityInput) (ResolveEntityOutput, error) {
	var zero ResolveEntityOutput
	ent, ok := s.cat.Entity(catalog.EntityType(in.Type))
	if !ok {
		return zero, invalid("The argument \"type\" names the entity type %s, which is not in the catalog. The types are %s.", quote(in.Type), s.typeList())
	}
	attrs, echo, err := identifyingAttrs(ent, in.Attributes)
	if err != nil {
		return zero, err
	}
	id, err := s.resolver.Resolve(ent.Type(), attrs)
	if err != nil {
		return zero, attrError(ent.Type(), err)
	}
	fp := id.Fingerprint()

	out := ResolveEntityOutput{
		Policy: Policy{Interval: interval.PolicyVersion, Rank: s.rankDigest, Names: NameTableVersion},
		Entity: ref(fp), Attributes: echo, Notes: []string{},
	}
	if in.At == "" {
		if in.AsOf != "" {
			return zero, invalid("The argument \"asof\" only applies with \"at\".")
		}
		out.Notes = append(out.Notes, "Nothing was read from the store: an id is a hash of the attributes, and says nothing about whether the entity exists. Pass \"at\" to check.")
		return out, nil
	}

	at, err := parseTime("at", in.At)
	if err != nil {
		return zero, err
	}
	token, ahead, err := s.pin(in.AsOf)
	if err != nil {
		return zero, err
	}
	out.Notes = notesFor(ahead)
	alive, err := s.st.Alive(ctx, fp, at, store.Scope{Layer: ent.Layer(), AsOf: token})
	ex := &ExistenceAt{At: fmtTime(at), Subject: SubjectAsserted, Basis: BasisDerived, Rule: RuleExistence}
	var qe *store.QuarantineError
	switch {
	case errors.As(err, &qe):
		q := quarantineOf(qe)
		ex.Status, ex.Quarantined = "quarantined", &q
		ex.Sentence = fmt.Sprintf("quarantined at token %d: two boots of it were live at once, so its existence is not folded", token)
	case err != nil:
		return zero, inLayer(ent.Layer(), err)
	case alive:
		ex.Status, ex.Sentence = "alive", fmt.Sprintf("alive at %s at token %d", ex.At, token)
	default:
		ex.Status, ex.Sentence = "not_alive", fmt.Sprintf("not alive at %s at token %d", ex.At, token)
	}
	out.Existence, out.Token, out.Derived = ex, tokenString(token), true

	if err := s.finish(ctx, &out, &out.Notes, token, at, at.Add(time.Nanosecond)); err != nil {
		return zero, err
	}
	return out, nil
}

// identifyingAttrs converts the attributes of a request to the Go values the
// resolver takes, by the catalog kind of each key: a string only from a JSON
// string, an integer only from an exact JSON integer, a time from an RFC 3339
// string. A key the type does not have is passed on, so that the resolver
// refuses it by name.
func identifyingAttrs(ent catalog.Entity, in map[string]json.RawMessage) ([]identity.Attr, map[string]any, error) {
	keys := make([]string, 0, len(in))
	for k := range in {
		keys = append(keys, k)
	}
	slices.Sort(keys)
	echo := make(map[string]any, len(in))
	var attrs []identity.Attr
	for _, name := range keys {
		raw := bytes.TrimSpace(in[name])
		var v any
		dec := json.NewDecoder(bytes.NewReader(raw))
		dec.UseNumber()
		if err := dec.Decode(&v); err != nil {
			return nil, nil, invalid("The attribute %q is not valid JSON.", name)
		}
		echo[name] = v
		key, ok := ent.Key(catalog.AttributeKey(name))
		if !ok {
			attrs = append(attrs, identity.Attr{Key: catalog.AttributeKey(name), Value: string(raw)})
			continue
		}
		val, err := convertAttr(name, key.Kind, raw, v)
		if err != nil {
			return nil, nil, err
		}
		attrs = append(attrs, identity.Attr{Key: key.Name, Value: val})
	}
	return attrs, echo, nil
}

func convertAttr(name string, kind catalog.Kind, raw []byte, v any) (any, error) {
	switch kind {
	case catalog.KindString, catalog.KindTime:
		s, ok := v.(string)
		if !ok {
			return nil, invalid("The attribute %q must be a JSON string; got %s.", name, quote(string(raw)))
		}
		return s, nil
	case catalog.KindInt:
		n, ok := v.(json.Number)
		text := string(raw)
		if !ok || !canonicalInteger(text) {
			return nil, invalid("The attribute %q must be a JSON integer without a fraction or exponent, such as 4242; got %s.", name, quote(text))
		}
		i, err := strconv.ParseInt(n.String(), 10, 64)
		if err != nil {
			return nil, invalid("The attribute %q does not fit in 64 bits; got %s.", name, quote(text))
		}
		return i, nil
	}
	return nil, invalid("The attribute %q has a kind this server cannot take.", name)
}

// canonicalInteger reports whether s is "0", or an optional minus sign then
// digits with no leading zero.
func canonicalInteger(s string) bool {
	digits := strings.TrimPrefix(s, "-")
	if digits == "" || s == "-0" {
		return false
	}
	if digits[0] == '0' {
		return s == "0"
	}
	for i := range len(digits) {
		if digits[i] < '0' || digits[i] > '9' {
			return false
		}
	}
	return true
}

// attrError turns the errors of the resolver into one sentence that names every
// attribute at fault, in name order.
func attrError(t catalog.EntityType, err error) error {
	var list []*identity.AttrError
	var walk func(error)
	walk = func(e error) {
		if j, ok := e.(interface{ Unwrap() []error }); ok {
			for _, x := range j.Unwrap() {
				walk(x)
			}
			return
		}
		var ae *identity.AttrError
		if errors.As(e, &ae) {
			list = append(list, ae)
		}
	}
	walk(err)
	if len(list) == 0 {
		return err
	}
	slices.SortFunc(list, func(a, b *identity.AttrError) int { return cmp.Compare(a.Key, b.Key) })
	parts := make([]string, len(list))
	for i, ae := range list {
		detail := ae.Err.Error()
		if ae.Detail != "" {
			detail += " (" + ae.Detail + ")"
		}
		if ae.Key == "" {
			parts[i] = detail
		} else {
			parts[i] = fmt.Sprintf("%q: %s", string(ae.Key), detail)
		}
	}
	return &Error{
		Message: fmt.Sprintf("The attributes do not identify a %s: %s.", t, strings.Join(parts, "; ")),
		Cause:   err,
	}
}
