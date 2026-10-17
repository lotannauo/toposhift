package mcp

import (
	"context"
	"errors"
	"reflect"
	"regexp"
	"slices"
	"time"
	"unicode"

	"github.com/lotannauo/toposhift/internal/catalog"
	"github.com/lotannauo/toposhift/internal/identity"
	"github.com/lotannauo/toposhift/internal/query/interval"
	"github.com/lotannauo/toposhift/internal/store"
)

// A name is derived from a description, never stored as a fact. It is read from
// the description of the producer that wins under the store's ranks at an end of
// the answer's window, from a top-level JSON string under one of these keys,
// version NameTableVersion. The first key present wins; a type that is not here
// has no name. A host is unnamed on purpose: the node agent drops host.name and
// every address from the host's description, and the server never prints an
// identity attribute it was not given in the request, except a service's, whose
// description is its identity, its name and namespace, nor an endpoint's id,
// which reveals its address, unless the request named it.
//
// A name is never taken from an identity attribute, except a service's, whose
// description is written from its identity attributes: the translators write a
// service's name and namespace as its description, and the table reads them like
// any other. It is never taken from a producer that lost under the ranks, or from
// the description of a peer.
//
// What a name or a namespace may print: the value is sender-chosen text for a
// service (UsableID accepts any non-blank UTF-8 up to 4096 bytes), so every
// name and namespace of every named type passes the print rule before it is
// printed (see [printable]): a value over 253 bytes, or with a control character,
// a format character (Unicode Cf: zero-width, bidi controls, tag characters) or an endpoint id's text, is
// refused. A value the rule refuses is withheld: null, with a fixed note that
// holds none of it, and a search by name or namespace does not match it. The same
// rule applies to the keys of a description payload that hold sender text
// (payload.go).
var nameTable = map[catalog.EntityType]struct{ name, namespace []string }{
	catalog.K8sPod:    {name: []string{"name", "k8s.pod.name"}, namespace: []string{"namespace", "k8s.namespace.name"}},
	catalog.K8sNode:   {name: []string{"name", "k8s.node.name"}},
	catalog.Container: {name: []string{"name", "k8s.container.name"}, namespace: []string{"k8s.namespace.name"}},
	catalog.Service:   {name: []string{"name"}, namespace: []string{"namespace"}},
}

const (
	namesNeedRange = "Names need a range read, which this store does not offer."
	notPresent     = "not present at either end of the window"
)

// maxPrintedText is the longest name or namespace that is printed, in bytes: 253
// is the longest Kubernetes object name, so no Kubernetes name meets the bound.
// It bounds what is printed, never what is stored.
const maxPrintedText = 253

// endpointText matches the text of a topo.endpoint id, with the hex digits in
// either case. The last check of an answer (withhold.go) refuses only lower case,
// so this is the stricter of the two: a value that holds it is withheld, and the
// answer that would have carried it is no longer refused whole.
var endpointText = regexp.MustCompile(regexp.QuoteMeta(string(catalog.NetworkEndpoint)) + ":[0-9a-fA-F]{32}")

// The per-value notes of a name or a namespace the print rule withheld. They are
// fixed sentences: they hold no text of the value, which may be anything a sender
// chose.
const (
	nameWithheld   = "name withheld: over 253 bytes, or it holds a control character, a format character (Unicode Cf: zero-width, bidi controls, tag characters) or an endpoint id's text"
	nsWithheld     = "namespace withheld: over 253 bytes, or it holds a control character, a format character (Unicode Cf: zero-width, bidi controls, tag characters) or an endpoint id's text"
	nameNsWithheld = "name and namespace withheld: over 253 bytes, or they hold a control character, a format character (Unicode Cf: zero-width, bidi controls, tag characters) or an endpoint id's text"
)

// printable is the print rule: a name, a namespace or another text a sender chose
// is printed only if it is at most 253 bytes, holds no control character, holds
// no format character and holds no endpoint id's text. A control character is any
// of Unicode category Cc (a newline and a tab among them) and the line and
// paragraph separators U+2028 and U+2029 (categories Zl and Zp), which a reader
// takes for a line break. A format character is any of Unicode category Cf: the
// zero-width characters, the bidirectional controls (U+202A to U+202E and U+2066
// to U+2069), the tag characters (U+E0000 to U+E007F) and the soft hyphen, which
// change how a reader sees the text or carry text a reader does not see. The
// length is in bytes, not runes. The rule does not claim to stop text that fits in 253 characters and
// reads as an instruction to a model: a client must treat every answer as data.
func printable(v string) bool {
	if len(v) > maxPrintedText {
		return false
	}
	for _, r := range v {
		if unicode.Is(unicode.Cc, r) || unicode.Is(unicode.Cf, r) || unicode.Is(unicode.Zl, r) || unicode.Is(unicode.Zp, r) {
			return false
		}
	}
	return !endpointText.MatchString(v)
}

// printRule is the print rule a call applies to a name and a namespace. Only the
// test hooks can change it.
func (s *Source) printRule() func(string) bool {
	if s.hooks != nil && s.hooks.noPrintRule {
		return func(string) bool { return true }
	}
	return printable
}

// held says which of a name and a namespace the print rule withheld.
type held struct{ name, namespace bool }

// note is the per-value note for what was withheld, or "" if nothing was.
func (h held) note() string {
	switch {
	case h.name && h.namespace:
		return nameNsWithheld
	case h.name:
		return nameWithheld
	case h.namespace:
		return nsWithheld
	}
	return ""
}

// nameFields reads the name and the namespace of an entity of type typ out of a
// description's payload, and applies the print rule ok ([printable] in production):
// a value the rule refuses is nil, and held says so. A value that is not there is
// nil and not held. The first key present wins, and the rule is applied to its
// value, never to the next key's.
func nameFields(typ catalog.EntityType, payload []byte, ok func(string) bool) (name, namespace *string, h held) {
	keys, named := nameTable[typ]
	if !named {
		return nil, nil, held{}
	}
	obj, isObj := payloadObject(payload)
	if !isObj {
		return nil, nil, held{}
	}
	first := func(ks []string) (p *string, withheld bool) {
		for _, k := range ks {
			if s, isString := obj[k].(string); isString && s != "" {
				if !ok(s) {
					return nil, true
				}
				return &s, false
			}
		}
		return nil, false
	}
	name, h.name = first(keys.name)
	namespace, h.namespace = first(keys.namespace)
	return name, namespace, h
}

var refType = reflect.TypeFor[Ref]()

// eachRef calls f on every [Ref] reachable from v through pointers, structs,
// slices and arrays. v must be addressable, so that f can fill the ref in.
func eachRef(v reflect.Value, f func(*Ref)) {
	switch v.Kind() {
	case reflect.Pointer, reflect.Interface:
		if !v.IsNil() {
			eachRef(v.Elem(), f)
		}
	case reflect.Struct:
		if v.Type() == refType {
			f(v.Addr().Interface().(*Ref))
			return
		}
		for i := range v.NumField() {
			if v.Type().Field(i).IsExported() {
				eachRef(v.Field(i), f)
			}
		}
	case reflect.Slice, reflect.Array:
		for i := range v.Len() {
			eachRef(v.Index(i), f)
		}
	}
}

// fillNames reads the names of every entity an answer lists, in one extra call of
// the interval package over the answer's window, at the answer's token and under
// the store's ranks, and sets them in place. out must be a pointer to the answer.
//
// The name comes from the description at the window's last instant, else at its
// first, so an entity deleted inside the window keeps its name. An error other
// than a cancellation never fails the answer: the names stay null, and the
// returned sentence says why. On a store without range reads no name is read.
func (s *Source) fillNames(ctx context.Context, out any, token uint64, from, to time.Time) (note string, err error) {
	var refs []*Ref
	eachRef(reflect.ValueOf(out), func(r *Ref) { refs = append(refs, r) })
	if len(refs) == 0 {
		return "", nil
	}
	if !s.rangeReads {
		return namesNeedRange, nil
	}
	fps := make([]identity.Fingerprint, 0, len(refs))
	seen := map[identity.Fingerprint]bool{}
	for _, r := range refs {
		if !seen[r.FP] {
			seen[r.FP] = true
			fps = append(fps, r.FP)
		}
	}
	slices.SortFunc(fps, store.CompareFingerprints)

	rd := interval.NewReader(s.st, interval.Options{Describe: true, Rank: s.rank})
	_, els, err := rd.Entities(ctx, fps, interval.Window{From: from, To: to, AsOf: token})
	if err != nil {
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			return "", err
		}
		// The note is a client's text, and no request named an endpoint for it.
		return "Names are unavailable: " + s.safeSentence(err, nil, " reading names"), nil
	}
	printOK := s.printRule()
	byFP := make(map[identity.Fingerprint]interval.Element, len(fps))
	for i, fp := range fps {
		byFP[fp] = els[i]
	}
	for _, r := range refs {
		el := byFP[r.FP]
		d := el.AtLast
		if d == nil {
			d = el.AtFrom
		}
		if d == nil {
			if el.Quarantine == nil {
				r.NameNote = notPresent
			}
			continue
		}
		var h held
		if r.Name, r.Namespace, h = nameFields(r.FP.Type(), d.Payload, printOK); r.Name != nil {
			r.NameFrom = string(d.Producer)
		}
		r.NameNote = h.note()
	}
	return "", nil
}
