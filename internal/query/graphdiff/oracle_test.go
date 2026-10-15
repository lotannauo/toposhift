package graphdiff_test

import (
	"bytes"
	"cmp"
	"encoding/json"
	"errors"
	"reflect"
	"slices"
	"time"
	"unicode/utf8"

	"github.com/lotannauo/toposhift/internal/catalog"
	"github.com/lotannauo/toposhift/internal/identity"
	"github.com/lotannauo/toposhift/internal/lifecycle"
	"github.com/lotannauo/toposhift/internal/query/graphdiff"
	"github.com/lotannauo/toposhift/internal/query/interval"
	"github.com/lotannauo/toposhift/internal/store"
)

// The oracle answers a diff straight from the definitions, by probing liveness
// at every instant where it can change. It shares no code with lifecycle.Fold,
// with package interval's non-test code, or with this package. Its liveness
// core (deadline, latest, liveAt, critical, endCause, pieces, classOf) is
// copied from package interval's test oracle.

func deadline(r store.Record) (time.Time, bool) {
	if r.Kind != lifecycle.Observe || r.TTL == 0 {
		return time.Time{}, false
	}
	last := r.EventTime
	if r.Through.After(last) {
		last = r.Through
	}
	return last.Add(r.TTL), true
}

// latest is each producer's assertion in force at x: of those with
// EventTime <= x, the one at the latest instant, and of those, the highest Seq.
func latest(recs []store.Record, x time.Time) map[lifecycle.Producer]store.Record {
	out := map[lifecycle.Producer]store.Record{}
	for _, r := range recs {
		if r.EventTime.After(x) {
			continue
		}
		cur, ok := out[r.Producer]
		if !ok || r.EventTime.After(cur.EventTime) || (r.EventTime.Equal(cur.EventTime) && r.Seq > cur.Seq) {
			out[r.Producer] = r
		}
	}
	return out
}

// liveAt says whether the subject exists at x, and which producers' references
// are live there, sorted.
func liveAt(recs []store.Record, x time.Time) (bool, []lifecycle.Producer) {
	var ps []lifecycle.Producer
	for p, r := range latest(recs, x) {
		if r.Kind != lifecycle.Observe {
			continue
		}
		if d, ok := deadline(r); ok && !x.Before(d) {
			continue
		}
		ps = append(ps, p)
	}
	slices.Sort(ps)
	return len(ps) > 0, ps
}

func critical(lists [][]store.Record, from, to time.Time) []time.Time {
	out := []time.Time{from}
	in := func(x time.Time) bool { return x.After(from) && x.Before(to) }
	for _, recs := range lists {
		for _, r := range recs {
			if in(r.EventTime) {
				out = append(out, r.EventTime)
			}
			if d, ok := deadline(r); ok && in(d) {
				out = append(out, d)
			}
		}
	}
	slices.SortFunc(out, func(a, b time.Time) int { return a.Compare(b) })
	return slices.CompactFunc(out, func(a, b time.Time) bool { return a.Equal(b) })
}

func endCause(recs []store.Record, x time.Time, live []lifecycle.Producer) lifecycle.EndSource {
	in := latest(recs, x)
	src := lifecycle.EndLivenessExpiry
	for _, p := range live {
		if r, ok := in[p]; ok && r.Kind == lifecycle.Delete && r.EventTime.Equal(x) {
			src = lifecycle.EndProducer
		}
	}
	return src
}

// pieces probes own (and, for the cascade, the endpoints in ends) at every
// critical instant of all of them.
func pieces(own []store.Record, ends [][]store.Record, from, to time.Time) []interval.Piece {
	alive := func(x time.Time) bool {
		if ok, _ := liveAt(own, x); !ok {
			return false
		}
		for _, e := range ends {
			if ok, _ := liveAt(e, x); !ok {
				return false
			}
		}
		return true
	}
	crit := critical(append([][]store.Record{own}, ends...), from, to)
	var out []interval.Piece
	var cur *interval.Piece
	var last time.Time
	for _, x := range crit {
		if alive(x) {
			if cur == nil {
				cur = &interval.Piece{Span: interval.Span{Start: x}}
				cur.Before = x.Equal(from) && alive(from.Add(-time.Nanosecond))
			}
			_, ps := liveAt(own, x)
			for _, p := range ps {
				if !slices.Contains(cur.Producers, p) {
					cur.Producers = append(cur.Producers, p)
				}
			}
			last = x
			continue
		}
		if cur == nil {
			continue
		}
		cur.End = x
		if ok, _ := liveAt(own, x); ok {
			cur.EndSource = lifecycle.EndCascade
		} else {
			_, ps := liveAt(own, last)
			cur.EndSource = endCause(own, x, ps)
		}
		slices.Sort(cur.Producers)
		out = append(out, *cur)
		cur = nil
	}
	if cur != nil {
		cur.End = to
		slices.Sort(cur.Producers)
		out = append(out, *cur)
	}
	return out
}

func classOf(ps []interval.Piece, from, to time.Time) (interval.Class, int) {
	n := 0
	for _, p := range ps {
		if p.Start.After(from) && p.Start.Before(to) {
			n++
		}
		if p.End.After(from) && p.End.Before(to) {
			n++
		}
	}
	k := len(ps)
	if k == 0 {
		return interval.ClassNone, n
	}
	if k >= 2 {
		return interval.ClassFlapping, n
	}
	a, b := ps[0].Start.Equal(from), ps[k-1].End.Equal(to)
	switch {
	case a && b:
		return interval.ClassStable, n
	case b:
		return interval.ClassAppeared, n
	case a:
		return interval.ClassDisappeared, n
	}
	return interval.ClassTransient, n
}

// oracleStateKeys is the state-key table of policy version 1, carried here as
// the oracle's own list.
var oracleStateKeys = []string{
	"phase", "reason", "conditions", "containers", "deletion_timestamp", "host_ip", "pod_ips", "node_info", "node_name",
	"k8s.pod.phase", "k8s.pod.status_reason", "k8s.node.condition_ready", "k8s.node.condition_memory_pressure",
	"k8s.node.condition_disk_pressure", "k8s.node.condition_pid_pressure", "k8s.node.condition_network_unavailable",
	"container.status", "container.status.reason", "k8s.node.name", "container.image.tag",
}

// oracle holds the records a token sees, by subject, and the entities the store
// quarantines at that token (quarantine is the store's to decide).
type oracle struct {
	bySub       map[store.Subject][]store.Record
	quarantined map[identity.Fingerprint]*store.QuarantineError
}

func newOracle(recs []store.Record, token uint64, quarantined map[identity.Fingerprint]*store.QuarantineError) *oracle {
	o := &oracle{bySub: map[store.Subject][]store.Record{}, quarantined: quarantined}
	if o.quarantined == nil {
		o.quarantined = map[identity.Fingerprint]*store.QuarantineError{}
	}
	for _, r := range recs {
		if r.Seq <= token {
			o.bySub[r.Subject] = append(o.bySub[r.Subject], r)
		}
	}
	return o
}

// storeQuarantines asks the store which of fps it quarantines at token.
func storeQuarantines(t tb, st store.Store, fps []identity.Fingerprint, token uint64) map[identity.Fingerprint]*store.QuarantineError {
	t.Helper()
	out := map[identity.Fingerprint]*store.QuarantineError{}
	for _, fp := range fps {
		_, err := st.Alive(ctx, fp, store.MinEventTime, store.Scope{Layer: layerOf(ent(fp)), AsOf: token})
		var qe *store.QuarantineError
		switch {
		case errors.As(err, &qe):
			out[fp] = qe
		case err != nil:
			t.Fatal(err)
		}
	}
	return out
}

// beats is the authority rule of descriptions: a listed producer beats an
// unlisted one, a higher rank a lower one, and the smaller name every tie.
func beats(a, b lifecycle.Producer, rank map[lifecycle.Producer]int) bool {
	ra, la := rank[a]
	rb, lb := rank[b]
	if la != lb {
		return la
	}
	if la && ra != rb {
		return ra > rb
	}
	return a < b
}

// describe is the description at x: the winner among the producers live there,
// and the payload of its latest assertion at or before x.
func describe(recs []store.Record, x time.Time, rank map[lifecycle.Producer]int) *interval.Description {
	ok, live := liveAt(recs, x)
	if !ok {
		return nil
	}
	best := live[0]
	for _, p := range live[1:] {
		if beats(p, best, rank) {
			best = p
		}
	}
	d := &interval.Description{Producer: best}
	if p := latest(recs, x)[best].Payload; len(p) > 0 {
		d.Payload = bytes.Clone(p)
	}
	return d
}

func asObject(p []byte) (map[string]any, bool) {
	if len(p) == 0 {
		return map[string]any{}, true
	}
	if !utf8.Valid(p) || !json.Valid(p) {
		return nil, false
	}
	var v any
	d := json.NewDecoder(bytes.NewReader(p))
	d.UseNumber()
	if err := d.Decode(&v); err != nil {
		return nil, false
	}
	m, ok := v.(map[string]any)
	return m, ok
}

// change is the description change between a and b, nil for none.
func change(a, b *interval.Description) *graphdiff.DescriptionChange {
	c := &graphdiff.DescriptionChange{FromProducer: a.Producer, ToProducer: b.Producer}
	if a.Producer != b.Producer {
		c.Kind = graphdiff.ChangeSourceChanged
		return c
	}
	if string(a.Payload) == string(b.Payload) {
		return nil
	}
	ma, okA := asObject(a.Payload)
	mb, okB := asObject(b.Payload)
	if !okA || !okB {
		c.Kind = graphdiff.ChangeOpaque
		return c
	}
	keys := map[string]bool{}
	for k := range ma {
		keys[k] = true
	}
	for k := range mb {
		keys[k] = true
	}
	for k := range keys {
		va, inA := ma[k]
		vb, inB := mb[k]
		if inA && inB && reflect.DeepEqual(va, vb) {
			continue
		}
		if slices.Contains(oracleStateKeys, k) {
			c.StateKeys = append(c.StateKeys, k)
		} else {
			c.AttributeKeys = append(c.AttributeKeys, k)
		}
	}
	if len(c.StateKeys)+len(c.AttributeKeys) == 0 {
		return nil
	}
	c.Kind = graphdiff.ChangeKeys
	slices.Sort(c.StateKeys)
	slices.Sort(c.AttributeKeys)
	return c
}

// oel is one element of the oracle's diff, and its presence at t1 and t2,
// probed directly rather than read from its pieces.
type oel struct {
	el     interval.Element
	p1, p2 bool
}

func scopeOf(scope []identity.Fingerprint) []identity.Fingerprint {
	fps := slices.Clone(scope)
	slices.SortFunc(fps, store.CompareFingerprints)
	return slices.Compact(fps)
}

// elements is every element of the diff of scope over [t1, t2+1ns): the scope
// entities, then the edges with an endpoint in it and a piece in the window.
func (o *oracle) elements(scope []identity.Fingerprint, t1, t2 time.Time, rank map[lifecycle.Producer]int) []oel {
	from, to := t1, t2.Add(time.Nanosecond)
	in := map[identity.Fingerprint]bool{}
	var out []oel
	for _, fp := range scopeOf(scope) {
		in[fp] = true
		sub := ent(fp)
		if q := o.quarantined[fp]; q != nil {
			out = append(out, oel{el: interval.Element{Subject: sub, Layer: layerOf(sub), Quarantine: q}})
			continue
		}
		own := o.bySub[sub]
		ps := pieces(own, nil, from, to)
		e := oel{el: interval.Element{Subject: sub, Layer: layerOf(sub), Pieces: ps}}
		e.el.Class, e.el.Transitions = classOf(ps, from, to)
		e.p1, _ = liveAt(own, t1)
		e.p2, _ = liveAt(own, t2)
		if e.p1 {
			e.el.AtFrom = describe(own, t1, rank)
		}
		if e.p2 {
			e.el.AtLast = describe(own, t2, rank)
		}
		out = append(out, e)
	}
	for sub, own := range o.bySub {
		if sub.Kind != store.SubjectEdge || (!in[sub.A] && !in[sub.B]) {
			continue
		}
		ea, eb := o.bySub[ent(sub.A)], o.bySub[ent(sub.B)]
		e := oel{el: interval.Element{Subject: sub, Layer: layerOf(sub)}}
		if q := cmp.Or(o.quarantined[sub.A], o.quarantined[sub.B]); q != nil {
			e.el.Uncascaded = q
			e.el.Pieces = pieces(own, nil, from, to)
		} else {
			e.el.Pieces = pieces(own, [][]store.Record{ea, eb}, from, to)
			alive := func(x time.Time) bool {
				a, _ := liveAt(own, x)
				b, _ := liveAt(ea, x)
				c, _ := liveAt(eb, x)
				return a && b && c
			}
			e.p1, e.p2 = alive(t1), alive(t2)
		}
		if len(e.el.Pieces) == 0 {
			continue
		}
		e.el.Class, e.el.Transitions = classOf(e.el.Pieces, from, to)
		out = append(out, e)
	}
	return out
}

func oracleOrder(a, b store.Subject) int {
	if a.Kind != b.Kind {
		if a.Kind == store.SubjectEntity {
			return -1
		}
		return 1
	}
	return cmp.Or(store.CompareFingerprints(a.A, b.A), store.CompareFingerprints(a.B, b.B), cmp.Compare(a.Relation, b.Relation))
}

// diff is what Diff must answer, but for Token and Reads.
func (o *oracle) diff(scope []identity.Fingerprint, t1, t2 time.Time, rank map[lifecycle.Producer]int, maxListed int) graphdiff.Result {
	res := graphdiff.Result{
		T1: t1.UTC(), T2: t2.UTC(), Scope: scopeOf(scope),
		PolicyVersion: 1, IntervalPolicy: interval.PolicyVersion,
	}
	var created, deleted, changed, transient []graphdiff.Entry
	for _, e := range o.elements(scope, t1, t2, rank) {
		el := e.el
		if el.Quarantine != nil || el.Uncascaded != nil {
			res.Quarantined = append(res.Quarantined, el)
			continue
		}
		switch {
		case !e.p1 && e.p2:
			created = append(created, graphdiff.Entry{Element: el})
		case e.p1 && !e.p2:
			lastPiece := el.Pieces[len(el.Pieces)-1]
			deleted = append(deleted, graphdiff.Entry{Element: el, DeleteSource: lastPiece.EndSource, DeletedAt: lastPiece.End})
		case e.p1 && e.p2 && el.Subject.Kind == store.SubjectEntity:
			if c := change(el.AtFrom, el.AtLast); c != nil {
				changed = append(changed, graphdiff.Entry{Element: el, Change: c})
			}
		}
		if el.Class == interval.ClassTransient || el.Class == interval.ClassFlapping {
			transient = append(transient, graphdiff.Entry{Element: el})
		}
	}
	slices.SortFunc(res.Quarantined, func(a, b interval.Element) int { return oracleOrder(a.Subject, b.Subject) })
	for _, el := range res.Quarantined {
		tally(&res.Counts.Quarantined, el.Subject)
	}
	if len(res.Quarantined) > maxListed {
		res.Truncated.Quarantined = len(res.Quarantined) - maxListed
		res.Quarantined = res.Quarantined[:maxListed]
	}
	capped := func(es []graphdiff.Entry, c *graphdiff.Count, dropped *int) []graphdiff.Entry {
		slices.SortFunc(es, func(a, b graphdiff.Entry) int { return oracleOrder(a.Element.Subject, b.Element.Subject) })
		for _, e := range es {
			tally(c, e.Element.Subject)
		}
		if len(es) > maxListed {
			*dropped = len(es) - maxListed
			es = es[:maxListed]
		}
		return es
	}
	res.Created = capped(created, &res.Counts.Created, &res.Truncated.Created)
	res.Deleted = capped(deleted, &res.Counts.Deleted, &res.Truncated.Deleted)
	res.Changed = capped(changed, &res.Counts.Changed, &res.Truncated.Changed)
	res.Transient = capped(transient, &res.Counts.Transient, &res.Truncated.Transient)
	return res
}

func tally(c *graphdiff.Count, s store.Subject) {
	if s.Kind == store.SubjectEntity {
		c.Entities++
	} else {
		c.Relations++
	}
}

// events is the change log from the definition: every piece boundary strictly
// inside the window, of every element that is not quarantined.
func (o *oracle) events(scope []identity.Fingerprint, t1, t2 time.Time) []graphdiff.Event {
	from, to := t1, t2.Add(time.Nanosecond)
	var out []graphdiff.Event
	for _, e := range o.elements(scope, t1, t2, nil) {
		if e.el.Quarantine != nil || e.el.Uncascaded != nil {
			continue
		}
		for _, p := range e.el.Pieces {
			if p.Start.After(from) {
				out = append(out, graphdiff.Event{At: p.Start, Subject: e.el.Subject, Appear: true})
			}
			if p.End.Before(to) {
				out = append(out, graphdiff.Event{At: p.End, Subject: e.el.Subject, Source: p.EndSource})
			}
		}
	}
	slices.SortFunc(out, func(a, b graphdiff.Event) int {
		appearFirst := func(e graphdiff.Event) int {
			if e.Appear {
				return 0
			}
			return 1
		}
		return cmp.Or(a.At.Compare(b.At), oracleOrder(a.Subject, b.Subject), cmp.Compare(appearFirst(a), appearFirst(b)))
	})
	return out
}

// edgeLayers is, from the catalog's relations, the layers a type's edges are
// stored in when read in dir.
func edgeLayers(typ catalog.EntityType, dir store.Direction) []catalog.Layer {
	var out []catalog.Layer
	for rel := range catalog.Default().Relations() {
		if rel.Derived() {
			continue
		}
		for ep := range rel.Endpoints() {
			if (dir == store.Forward && ep.From == typ) || (dir == store.Reverse && ep.To == typ) {
				if !slices.Contains(out, rel.Layer()) {
					out = append(out, rel.Layer())
				}
			}
		}
	}
	return out
}

// reads is the read formula of the budget's shape: one call for the entities,
// one per direction with an edge layer; one Window per scope entity and edge
// layer; one existence read per scope entity, plus, per edge call, one per
// distinct endpoint of an edge with an interval of its own in the window; and
// an EntityWindow for each existence read of an entity not quarantined.
func (o *oracle) reads(scope []identity.Fingerprint, t1, t2 time.Time) graphdiff.Reads {
	from, to := t1, t2.Add(time.Nanosecond)
	fps := scopeOf(scope)
	r := graphdiff.Reads{Calls: 1}
	existence := func(set map[identity.Fingerprint]bool) {
		for fp := range set {
			r.Alive++
			if o.quarantined[fp] == nil {
				r.EntityWindows++
			}
		}
	}
	in := map[identity.Fingerprint]bool{}
	for _, fp := range fps {
		in[fp] = true
	}
	existence(in)
	for _, dir := range []store.Direction{store.Forward, store.Reverse} {
		n := 0
		for _, fp := range fps {
			n += len(edgeLayers(fp.Type(), dir))
		}
		if n == 0 {
			continue
		}
		r.Calls++
		r.Windows += n
		ends := map[identity.Fingerprint]bool{}
		for sub, own := range o.bySub {
			if sub.Kind != store.SubjectEdge {
				continue
			}
			if (dir == store.Forward && !in[sub.A]) || (dir == store.Reverse && !in[sub.B]) {
				continue
			}
			if len(pieces(own, nil, from, to)) > 0 {
				ends[sub.A], ends[sub.B] = true, true
			}
		}
		existence(ends)
	}
	return r
}

// checkOracle runs one diff and checks it against the oracle: every bucket,
// entry, count, quarantine and event (G1), and the read formula (G10).
func checkOracle(t tb, st store.Store, recs []store.Record, entities, scope []identity.Fingerprint, t1, t2 time.Time, asOf uint64, o graphdiff.Options) graphdiff.Result {
	t.Helper()
	res, err := graphdiff.NewDiffer(st).Diff(ctx, scope, t1, t2, asOf, o)
	if err != nil {
		t.Fatalf("Diff(%v, %s, %s, %d): %v", scope, ts(t1), ts(t2), asOf, err)
	}
	want := clampToken(asOf, st.LastSeq())
	if res.Token != want {
		t.Fatalf("token %d, want %d", res.Token, want)
	}
	orc := newOracle(recs, res.Token, storeQuarantines(t, st, entities, res.Token))
	maxListed := o.Policy.MaxListed
	if o.Policy == (graphdiff.Policy{}) {
		maxListed = graphdiff.DefaultPolicy().MaxListed
	}
	if d := sameResult(res, orc.diff(scope, t1, t2, o.Rank, maxListed)); d != "" {
		t.Fatalf("Diff(%v, %s, %s, token %d) disagrees with the oracle: %s", scope, ts(t1), ts(t2), res.Token, d)
	}
	if res.Truncated == (graphdiff.Truncation{}) {
		if d := diffLines("events", eventStrings(graphdiff.Events(res)), eventStrings(orc.events(scope, t1, t2))); d != "" {
			t.Fatalf("Events(Diff(%v, %s, %s)): %s", scope, ts(t1), ts(t2), d)
		}
	}
	if r := orc.reads(scope, t1, t2); res.Reads != r {
		t.Fatalf("Diff(%v, %s, %s): reads %+v, want %+v", scope, ts(t1), ts(t2), res.Reads, r)
	}
	return res
}

func clampToken(tok, last uint64) uint64 {
	if tok == store.Latest || tok > last {
		return last
	}
	return tok
}
