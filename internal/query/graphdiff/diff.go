package graphdiff

import (
	"bytes"
	"cmp"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"reflect"
	"slices"
	"sync"
	"time"
	"unicode/utf8"

	"github.com/lotannauo/toposhift/internal/catalog"
	"github.com/lotannauo/toposhift/internal/identity"
	"github.com/lotannauo/toposhift/internal/lifecycle"
	"github.com/lotannauo/toposhift/internal/query/interval"
	"github.com/lotannauo/toposhift/internal/store"
)

// Differ diffs scopes of a store, which it only reads.
type Differ struct{ st store.Store }

// NewDiffer returns a differ over st.
func NewDiffer(st store.Store) *Differ { return &Differ{st: st} }

// counting is the store a diff reads through: it counts the reads the budget
// is stated in, and never writes.
type counting struct {
	store.Store
	mu    sync.Mutex
	reads Reads
}

func (c *counting) count(f func(*Reads)) {
	c.mu.Lock()
	f(&c.reads)
	c.mu.Unlock()
}

func (c *counting) Window(ctx context.Context, fp identity.Fingerprint, d store.Direction, from, to time.Time, s store.Scope) ([]store.Record, error) {
	c.count(func(r *Reads) { r.Windows++ })
	return c.Store.Window(ctx, fp, d, from, to, s)
}

func (c *counting) EntityWindow(ctx context.Context, fp identity.Fingerprint, from, to time.Time, s store.Scope) ([]store.Record, error) {
	c.count(func(r *Reads) { r.EntityWindows++ })
	return c.Store.EntityWindow(ctx, fp, from, to, s)
}

// WindowSince counts one Window per fingerprint of an edge read, and one
// EntityWindow per fingerprint of an existence read (direction 0): the reads it
// stands for.
func (c *counting) WindowSince(ctx context.Context, fps []identity.Fingerprint, d store.Direction, from, to time.Time, s store.Scope, o store.SinceOptions) ([]store.Since, error) {
	c.count(func(r *Reads) {
		if d == 0 {
			r.EntityWindows += len(fps)
		} else {
			r.Windows += len(fps)
		}
	})
	return c.Store.WindowSince(ctx, fps, d, from, to, s, o)
}

func (c *counting) Alive(ctx context.Context, fp identity.Fingerprint, t time.Time, s store.Scope) (bool, error) {
	c.count(func(r *Reads) { r.Alive++ })
	return c.Store.Alive(ctx, fp, t, s)
}

// Diff diffs scope between the instants t1 and t2 at the token asOf
// (store.Latest, or a snapshot token): the scope's entities, and every edge
// with an endpoint in it, read as the window [t1, t2+1ns). Arguments are
// checked before any store call. After that, any error fails the whole diff
// with a zero Result: there is no partial answer. A t1 before a layer's
// retention horizon gives an error wrapping store.ErrBeforeHorizon, and a
// description asserted before the horizon one wrapping interval.ErrRetained.
func (d *Differ) Diff(ctx context.Context, scope []identity.Fingerprint, t1, t2 time.Time, asOf uint64, o Options) (Result, error) {
	fail := func(err error) (Result, error) { return Result{}, fmt.Errorf("graphdiff: Diff: %w", err) }
	fps, pol, err := check(scope, t1, t2, o.Policy)
	if err != nil {
		return fail(err)
	}
	if err := ctx.Err(); err != nil {
		return fail(err)
	}
	cnt := &counting{Store: d.st}
	w := interval.Window{From: t1, To: t2.Add(time.Nanosecond), AsOf: asOf}
	rd, err := readScope(ctx, cnt, fps, w, o.Rank)
	if err != nil {
		return fail(err)
	}
	res := Result{
		Token: rd.token, T1: t1.UTC(), T2: t2.UTC(), Scope: fps,
		PolicyVersion: PolicyVersion, IntervalPolicy: interval.PolicyVersion,
	}
	if err := res.bucket(rd, w.From, w.To, pol); err != nil {
		return fail(err)
	}
	res.Reads = cnt.reads
	res.Reads.Calls = rd.calls
	return res, nil
}

// check checks a diff's arguments, and returns the scope once each and
// ordered, and the policy in force.
func check(scope []identity.Fingerprint, t1, t2 time.Time, p Policy) ([]identity.Fingerprint, Policy, error) {
	if !t1.Before(t2) {
		return nil, Policy{}, fmt.Errorf("t1 %s is not before t2 %s: %w", formatTime(t1), formatTime(t2), store.ErrInvalid)
	}
	if len(scope) == 0 {
		return nil, Policy{}, fmt.Errorf("the scope is empty: %w", store.ErrInvalid)
	}
	cat := catalog.Default()
	for i, fp := range scope {
		if fp.IsZero() {
			return nil, Policy{}, fmt.Errorf("scope entity %d is the zero fingerprint: %w", i, store.ErrInvalid)
		}
		if _, ok := cat.Entity(fp.Type()); !ok {
			return nil, Policy{}, fmt.Errorf("scope entity %d has the unknown entity type %q: %w", i, fp.Type(), store.ErrInvalid)
		}
	}
	if p == (Policy{}) {
		p = DefaultPolicy()
	}
	if err := p.validate(); err != nil {
		return nil, Policy{}, err
	}
	fps := slices.Clone(scope)
	slices.SortFunc(fps, store.CompareFingerprints)
	fps = slices.Compact(fps)
	if len(fps) > p.MaxScope {
		return nil, Policy{}, fmt.Errorf("the scope has %d distinct entities, over the cap of %d: %w", len(fps), p.MaxScope, store.ErrInvalid)
	}
	return fps, p, nil
}

// read is what a diff has read of its scope: the token, the scope's entities
// in scope order, the edges once each, and the interval calls made.
type read struct {
	token    uint64
	entities []interval.Element
	edges    []interval.Element
	calls    int
}

// readScope makes the diff's interval calls, all at the token of the first:
// the scope's entities, described under rank, then the edges forward and
// reverse, each skipped when no scope entity has an edge layer in its
// direction.
func readScope(ctx context.Context, st store.Store, fps []identity.Fingerprint, w interval.Window, rank map[lifecycle.Producer]int) (read, error) {
	var rd read
	rd.calls++
	tok, ents, err := interval.NewReader(st, interval.Options{Describe: true, Rank: rank}).Entities(ctx, fps, w)
	if err != nil {
		return read{}, err
	}
	rd.token, rd.entities, w.AsOf = tok, ents, tok
	cat := catalog.Default()
	plain := interval.NewReader(st, interval.Options{})
	seen := map[store.Subject]bool{}
	for _, dir := range []store.Direction{store.Forward, store.Reverse} {
		if !slices.ContainsFunc(fps, func(fp identity.Fingerprint) bool { return len(interval.EdgeLayers(cat, fp.Type(), dir)) > 0 }) {
			continue
		}
		rd.calls++
		_, out, err := plain.EdgesBatch(ctx, fps, dir, w)
		if err != nil {
			return read{}, err
		}
		// An edge between two scope entities is read from both ends, at one
		// token and with one cascade, so the two reads are one element.
		for _, els := range out {
			for _, el := range els {
				if !seen[el.Subject] {
					seen[el.Subject] = true
					rd.edges = append(rd.edges, el)
				}
			}
		}
	}
	return rd, nil
}

// bucket sorts the elements read into the buckets, then counts, orders and
// caps them.
func (res *Result) bucket(rd read, from, to time.Time, pol Policy) error {
	var created, deleted, changed, transient []Entry
	for _, el := range slices.Concat(rd.entities, rd.edges) {
		if el.Quarantine != nil || el.Uncascaded != nil {
			res.Quarantined = append(res.Quarantined, el)
			continue
		}
		if len(el.Pieces) == 0 {
			continue
		}
		first, last := el.Pieces[0], el.Pieces[len(el.Pieces)-1]
		atFrom, atTo := first.Start.Equal(from), last.End.Equal(to)
		switch {
		case !atFrom && atTo:
			created = append(created, Entry{Element: el})
		case atFrom && !atTo:
			deleted = append(deleted, Entry{Element: el, DeleteSource: last.EndSource, DeletedAt: last.End})
		case atFrom && atTo && el.Subject.Kind == store.SubjectEntity:
			ch, err := describeChange(el)
			if err != nil {
				return err
			}
			if ch != nil {
				changed = append(changed, Entry{Element: el, Change: ch})
			}
		}
		if el.Class == interval.ClassTransient || el.Class == interval.ClassFlapping {
			// A copy: an element in a net bucket too shares nothing with it.
			transient = append(transient, Entry{Element: cloneElement(el)})
		}
	}
	sortElements(res.Quarantined)
	res.Counts.Quarantined = countElements(res.Quarantined)
	if n := len(res.Quarantined); n > pol.MaxListed {
		res.Quarantined, res.Truncated.Quarantined = res.Quarantined[:pol.MaxListed:pol.MaxListed], n-pol.MaxListed
	}
	res.Created, res.Counts.Created, res.Truncated.Created = list(created, pol.MaxListed)
	res.Deleted, res.Counts.Deleted, res.Truncated.Deleted = list(deleted, pol.MaxListed)
	res.Changed, res.Counts.Changed, res.Truncated.Changed = list(changed, pol.MaxListed)
	res.Transient, res.Counts.Transient, res.Truncated.Transient = list(transient, pol.MaxListed)
	return nil
}

// cloneElement copies an element's pieces and descriptions.
func cloneElement(el interval.Element) interval.Element {
	el.Pieces = slices.Clone(el.Pieces)
	for i := range el.Pieces {
		el.Pieces[i].Producers = slices.Clone(el.Pieces[i].Producers)
	}
	for _, d := range []**interval.Description{&el.AtFrom, &el.AtLast} {
		if *d != nil {
			*d = &interval.Description{Producer: (*d).Producer, Payload: bytes.Clone((*d).Payload)}
		}
	}
	return el
}

// list orders a bucket's entries, counts them, and keeps the first limit.
func list(es []Entry, limit int) ([]Entry, Count, int) {
	slices.SortFunc(es, func(a, b Entry) int { return compareSubjects(a.Element.Subject, b.Element.Subject) })
	var c Count
	for _, e := range es {
		add(&c, e.Element.Subject)
	}
	if len(es) <= limit {
		return es, c, 0
	}
	return es[:limit:limit], c, len(es) - limit
}

func countElements(els []interval.Element) Count {
	var c Count
	for _, el := range els {
		add(&c, el.Subject)
	}
	return c
}

func add(c *Count, s store.Subject) {
	if s.Kind == store.SubjectEntity {
		c.Entities++
	} else {
		c.Relations++
	}
}

func sortElements(els []interval.Element) {
	slices.SortFunc(els, func(a, b interval.Element) int { return compareSubjects(a.Subject, b.Subject) })
}

// compareSubjects is the order of every list: entities before edges, then the
// entity or the edge's source, then its target, then the relation.
func compareSubjects(a, b store.Subject) int {
	ea, eb := a.Kind == store.SubjectEntity, b.Kind == store.SubjectEntity
	switch {
	case ea && !eb:
		return -1
	case !ea && eb:
		return 1
	}
	if c := store.CompareFingerprints(a.A, b.A); c != 0 {
		return c
	}
	if c := store.CompareFingerprints(a.B, b.B); c != 0 {
		return c
	}
	return cmp.Compare(a.Relation, b.Relation)
}

// describeChange compares an entity's descriptions at the two instants, at
// both of which it exists, and returns nil when they do not differ.
func describeChange(el interval.Element) (*DescriptionChange, error) {
	from, last := el.AtFrom, el.AtLast
	if from == nil || last == nil {
		// The interval package describes an entity at every end a piece
		// contains; a missing description would be a guess either way.
		return nil, fmt.Errorf("entity %s exists at both instants but has no description at one of them", el.Subject.A)
	}
	ch := &DescriptionChange{FromProducer: from.Producer, ToProducer: last.Producer}
	switch {
	case from.Producer != last.Producer:
		ch.Kind = ChangeSourceChanged
		return ch, nil
	case bytes.Equal(from.Payload, last.Payload):
		return nil, nil
	}
	a, okA := jsonObject(from.Payload)
	b, okB := jsonObject(last.Payload)
	if !okA || !okB {
		ch.Kind = ChangeOpaque
		return ch, nil
	}
	for k, va := range a {
		if vb, ok := b[k]; !ok || !reflect.DeepEqual(va, vb) {
			ch.addKey(k)
		}
	}
	for k := range b {
		if _, ok := a[k]; !ok {
			ch.addKey(k)
		}
	}
	if len(ch.StateKeys) == 0 && len(ch.AttributeKeys) == 0 {
		// The bytes differ, the values do not: a reordering or a respacing.
		return nil, nil
	}
	ch.Kind = ChangeKeys
	slices.Sort(ch.StateKeys)
	slices.Sort(ch.AttributeKeys)
	return ch, nil
}

func (ch *DescriptionChange) addKey(k string) {
	if stateKeys[k] {
		ch.StateKeys = append(ch.StateKeys, k)
	} else {
		ch.AttributeKeys = append(ch.AttributeKeys, k)
	}
}

// jsonObject decodes a payload that is one JSON object, numbers kept as their
// text. An empty payload is the empty object. Anything else, invalid UTF-8
// (which decoding would replace rather than refuse), null and trailing data
// included, is not an object.
func jsonObject(p []byte) (map[string]any, bool) {
	if len(p) == 0 {
		return map[string]any{}, true
	}
	if !utf8.Valid(p) {
		return nil, false
	}
	if t := bytes.TrimLeft(p, " \t\r\n"); len(t) == 0 || t[0] != '{' {
		return nil, false
	}
	dec := json.NewDecoder(bytes.NewReader(p))
	dec.UseNumber()
	var m map[string]any
	if err := dec.Decode(&m); err != nil {
		return nil, false
	}
	if _, err := dec.Token(); err != io.EOF {
		return nil, false
	}
	return m, true
}

// Events is the change log of a diff: every piece boundary strictly inside the
// window [T1, T2+1ns) of the elements in the result's bucket lists, an
// appearance at each piece start after T1 and a disappearance, with its end
// source, at each piece end before T2+1ns. Every element with such a boundary
// is in Created, Deleted or Transient, so the log is complete unless a list
// was truncated, and then it lacks the boundaries of the elements that no list
// holds; an element left out of one list but kept in another keeps its
// events. Quarantined
// elements, whose pieces are not cascaded existence, have no events. Events
// are ordered by time, then entity before edge, then subject as the lists
// are, then appearance before disappearance. Description changes are not
// events.
func Events(res Result) []Event {
	from, to := res.T1, res.T2.Add(time.Nanosecond)
	seen := map[store.Subject]bool{}
	var out []Event
	for _, es := range [][]Entry{res.Created, res.Deleted, res.Changed, res.Transient} {
		for _, e := range es {
			s := e.Element.Subject
			if seen[s] {
				continue
			}
			seen[s] = true
			for _, p := range e.Element.Pieces {
				if p.Start.After(from) {
					out = append(out, Event{At: p.Start, Subject: s, Appear: true})
				}
				if p.End.Before(to) {
					out = append(out, Event{At: p.End, Subject: s, Source: p.EndSource})
				}
			}
		}
	}
	slices.SortFunc(out, func(a, b Event) int {
		if c := a.At.Compare(b.At); c != 0 {
			return c
		}
		if c := compareSubjects(a.Subject, b.Subject); c != 0 {
			return c
		}
		switch {
		case a.Appear && !b.Appear:
			return -1
		case !a.Appear && b.Appear:
			return 1
		}
		return 0
	})
	return out
}
