package mcp

import (
	"context"
	"errors"
	"reflect"
	"slices"
	"time"

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
// identity attribute it was not given in the request, nor an endpoint's id, which
// reveals its address, unless the request named it.
//
// A name is never taken from an identity attribute, from a producer that lost
// under the ranks, or from the description of a peer.
var nameTable = map[catalog.EntityType]struct{ name, namespace []string }{
	catalog.K8sPod:    {name: []string{"name", "k8s.pod.name"}, namespace: []string{"namespace", "k8s.namespace.name"}},
	catalog.K8sNode:   {name: []string{"name", "k8s.node.name"}},
	catalog.Container: {name: []string{"name", "k8s.container.name"}, namespace: []string{"k8s.namespace.name"}},
}

const (
	namesNeedRange = "Names need a range read, which this store does not offer yet."
	notPresent     = "not present at either end of the window"
)

// nameFields reads the name and the namespace of an entity of type typ out of a
// description's payload.
func nameFields(typ catalog.EntityType, payload []byte) (name, namespace *string) {
	keys, ok := nameTable[typ]
	if !ok {
		return nil, nil
	}
	obj, ok := payloadObject(payload)
	if !ok {
		return nil, nil
	}
	first := func(ks []string) *string {
		for _, k := range ks {
			if s, ok := obj[k].(string); ok && s != "" {
				return &s
			}
		}
		return nil
	}
	return first(keys.name), first(keys.namespace)
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
		if r.Name, r.Namespace = nameFields(r.FP.Type(), d.Payload); r.Name != nil {
			r.NameFrom = string(d.Producer)
		}
	}
	return "", nil
}
