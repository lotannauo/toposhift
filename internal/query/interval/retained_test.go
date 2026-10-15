package interval_test

import (
	"errors"
	"fmt"
	"slices"
	"testing"
	"time"

	"github.com/lotannauo/toposhift/internal/catalog"
	"github.com/lotannauo/toposhift/internal/identity"
	"github.com/lotannauo/toposhift/internal/lifecycle"
	"github.com/lotannauo/toposhift/internal/query/interval"
	"github.com/lotannauo/toposhift/internal/store"
	"github.com/lotannauo/toposhift/internal/store/memstore"
)

// A retained store and its twin, which holds the same records and was never
// retained, answer every read of a window that starts at or after the horizon
// alike, on the generated streams, with retentions at fixed fractions of each:
// entities, and edges both ways, raw and with the cascade, one at a time and in
// a batch.
//
// The one difference allowed is the Before of a piece of a window that starts
// at the horizon itself: the state just before the horizon is the state at it,
// so a reference whose deadline is exactly the horizon is no seed, and an
// interval it joined across From starts at From. It is allowed only where the
// twin has such a reference, on the element or on an endpoint.
//
// With descriptions, an answer is the twin's, or refused with ErrRetained
// exactly when one of the twin's descriptions was asserted, by its winning
// producer, before the horizon.
func TestRetainedStoreAnswersAsItsTwin(t *testing.T) {
	for _, s := range streams() {
		for _, frac := range []float64{0.3, 0.55, 0.8} {
			t.Run(fmt.Sprintf("%s retained at %.2f", s.name, frac), func(t *testing.T) {
				t.Parallel()
				twin, recs, entities, start, end := openStream(t, s)
				h := start.Add(time.Duration(frac * float64(end.Sub(start)))).Truncate(time.Second)
				st := openStore(t, memstore.Options{Policy: s.policy})
				for i := 0; i < len(recs); i += 64 {
					if err := st.Write(ctx, recs[i:min(i+64, len(recs))]); err != nil {
						t.Fatal(err)
					}
				}
				if err := st.Retain(ctx, h); err != nil {
					t.Fatal(err)
				}
				hz := st.Horizon()
				var starts []time.Time
				starts = append(starts, h, h.Add(time.Nanosecond), h.Add(time.Minute))
				for _, r := range recs {
					if last := r.EventTime.Add(r.TTL); r.TTL > 0 && !last.Before(h) && len(starts) < 8 {
						starts = append(starts, last) // a deadline: a reference ends exactly at From
					}
				}
				var allowed, refused, described int
				for _, from := range starts {
					w := interval.Window{From: from, To: from.Add(7 * time.Minute), AsOf: store.Latest}
					c := twinCheck{t: t, st: st, twin: twin, w: w, h: hz}
					allowed += c.entities(entities)
					for _, raw := range []bool{false, true} {
						for _, d := range dirs {
							allowed += c.edges(entities, d, raw)
						}
					}
					r, d := c.descriptions(entities)
					refused += r
					described += d
				}
				t.Logf("horizon %s: %d Before bits allowed to differ, %d description calls refused, %d answered", h.Sub(start), allowed, refused, described)
				if described == 0 {
					t.Error("no call with descriptions was answered")
				}
			})
		}
	}
}

// twinCheck compares the reads of a retained store and its twin over one window.
type twinCheck struct {
	t        *testing.T
	st, twin store.Store
	w        interval.Window
	h        store.Horizon
}

// endsAtTheHorizon says whether the twin has a reference of the subject, or of an
// edge's endpoints, whose seed at the horizon has its deadline exactly there.
func (c twinCheck) endsAtTheHorizon(sub store.Subject) bool {
	c.t.Helper()
	subjects := []store.Subject{sub}
	if sub.Kind == store.SubjectEdge {
		subjects = append(subjects, store.EntitySubject(sub.A), store.EntitySubject(sub.B))
	}
	for _, s := range subjects {
		fp, dir := s.A, store.Forward
		if s.Kind == store.SubjectEntity {
			dir = 0
		}
		for _, l := range layersOf(s) {
			ans, err := c.twin.WindowSince(ctx, []identity.Fingerprint{fp}, dir, c.h.Time, c.h.Time.Add(time.Nanosecond), store.Current(l), store.SinceOptions{})
			if err != nil {
				c.t.Fatal(err)
			}
			for _, sd := range ans[0].Seeds {
				last, _ := store.LastOf(sd.Record.EventTime, sd.Record.Through)
				if sd.Record.Subject == s && sd.Record.TTL > 0 && last.Add(sd.Record.TTL).Equal(c.h.Time) {
					return true
				}
			}
		}
	}
	return false
}

// same compares two answers element by element, and returns how many Before bits
// differed where that is allowed.
func (c twinCheck) same(what string, got, want []interval.Element) int {
	c.t.Helper()
	if df := diff(got, want); df == "" {
		return 0
	}
	allowed := 0
	if len(got) == len(want) && c.w.From.Equal(c.h.Time) {
		ok := true
		for i := range got {
			g, w := got[i], want[i]
			if diff([]interval.Element{g}, []interval.Element{w}) == "" {
				continue
			}
			g2, w2 := clearBefore(g), clearBefore(w)
			if diff([]interval.Element{g2}, []interval.Element{w2}) != "" || !c.endsAtTheHorizon(w.Subject) ||
				!slices.EqualFunc(g.Pieces, w.Pieces, func(a, b interval.Piece) bool { return a.Before == b.Before || (!a.Before && b.Before) }) {
				ok = false
				break
			}
			allowed++
		}
		if ok {
			return allowed
		}
	}
	c.t.Fatalf("%s over [%s, %s) with the horizon at %s:\n%s", what, ts(c.w.From), ts(c.w.To), ts(c.h.Time), diff(got, want))
	return 0
}

func clearBefore(el interval.Element) interval.Element {
	el.Pieces = slices.Clone(el.Pieces)
	for i := range el.Pieces {
		el.Pieces[i].Before = false
	}
	return el
}

func (c twinCheck) entities(fps []identity.Fingerprint) int {
	c.t.Helper()
	_, got, err := interval.NewReader(c.st, interval.Options{}).Entities(ctx, fps, c.w)
	if err != nil {
		c.t.Fatalf("Entities: %v", err)
	}
	_, want, err := interval.NewReader(c.twin, interval.Options{}).Entities(ctx, fps, c.w)
	if err != nil {
		c.t.Fatal(err)
	}
	return c.same("Entities", got, want)
}

func (c twinCheck) edges(fps []identity.Fingerprint, d store.Direction, raw bool) int {
	c.t.Helper()
	_, got, err := interval.NewReader(c.st, interval.Options{Raw: raw}).EdgesBatch(ctx, fps, d, c.w)
	if err != nil {
		c.t.Fatalf("EdgesBatch: %v", err)
	}
	_, want, err := interval.NewReader(c.twin, interval.Options{Raw: raw}).EdgesBatch(ctx, fps, d, c.w)
	if err != nil {
		c.t.Fatal(err)
	}
	n := 0
	for i := range got {
		n += c.same(fmt.Sprintf("EdgesBatch %s raw %t, entity %s", d, raw, fps[i]), got[i], want[i])
	}
	return n
}

// descriptions reads every entity with descriptions, and each entity's reverse
// edges, from both stores, and requires the twin's answer, or ErrRetained exactly
// when a description the twin gives was asserted before the horizon by its
// winner. It returns how many calls were refused and how many answered.
func (c twinCheck) descriptions(fps []identity.Fingerprint) (refused, answered int) {
	c.t.Helper()
	judge := func(what string, got, want []interval.Element, err, werr error) {
		c.t.Helper()
		if werr != nil {
			c.t.Fatal(werr)
		}
		before := false
		for _, el := range want {
			for _, d := range []struct {
				x time.Time
				d *interval.Description
			}{{c.w.From, el.AtFrom}, {c.w.To.Add(-time.Nanosecond), el.AtLast}} {
				if d.d != nil && c.assertedBefore(el.Subject, d.d.Producer, d.x) {
					before = true
				}
			}
		}
		switch {
		case before:
			if !errors.Is(err, interval.ErrRetained) || !errors.Is(err, errors.ErrUnsupported) {
				c.t.Fatalf("%s: %v; want ErrRetained, since a description was asserted before the horizon", what, err)
			}
			refused++
		case err != nil:
			c.t.Fatalf("%s: %v; want the twin's answer", what, err)
		default:
			if df := describedDiff(got, want); df != "" && !c.w.From.Equal(c.h.Time) {
				c.t.Fatalf("%s:\n%s", what, df)
			}
			answered++
		}
	}
	_, got, err := describing(c.st, nil).Entities(ctx, fps, c.w)
	_, want, werr := describing(c.twin, nil).Entities(ctx, fps, c.w)
	judge("Entities with descriptions", got, want, err, werr)
	for _, fp := range fps {
		g, err := describing(c.st, nil).Edges(ctx, fp, store.Reverse, c.w)
		w, werr := describing(c.twin, nil).Edges(ctx, fp, store.Reverse, c.w)
		judge("Edges with descriptions of "+fp.String(), g.Elements, w.Elements, err, werr)
	}
	return refused, answered
}

// assertedBefore says whether the latest assertion by p of sub at or before x,
// which gives the description there, has an event time before the horizon.
func (c twinCheck) assertedBefore(sub store.Subject, p lifecycle.Producer, x time.Time) bool {
	c.t.Helper()
	var latest time.Time
	for _, l := range layersOf(sub) {
		var recs []store.Record
		var err error
		if sub.Kind == store.SubjectEntity {
			recs, err = c.twin.EntityWindow(ctx, sub.A, store.MinEventTime, x.Add(time.Nanosecond), store.Current(l))
		} else {
			recs, err = c.twin.Window(ctx, sub.A, store.Forward, store.MinEventTime, x.Add(time.Nanosecond), store.Current(l))
		}
		if err != nil {
			c.t.Fatal(err)
		}
		for _, r := range recs {
			if r.Subject == sub && r.Producer == p && r.EventTime.After(latest) {
				latest = r.EventTime
			}
		}
	}
	return latest.Before(c.h.Time)
}

// layersOf is the layer a subject is stored in: its entity type's, or its
// relation's.
func layersOf(sub store.Subject) []catalog.Layer {
	if sub.Kind == store.SubjectEntity {
		e, _ := catalog.Default().Entity(sub.A.Type())
		return []catalog.Layer{e.Layer()}
	}
	rel, _ := catalog.Default().Relation(sub.Relation)
	return []catalog.Layer{rel.Layer()}
}

// The one answer a window from the horizon gives differently: a reference whose
// deadline is the horizon, joined at the horizon by another producer's record.
// The twin has one interval across From, Before set; after the retention the
// state just before the horizon is the state at it, where the first reference has
// lapsed, so the interval starts at From. Its span, class and end are the same.
func TestAtTheHorizonADeadlineThereIsNoSeed(t *testing.T) {
	p := mkfp(t, catalog.K8sPod, 1)
	h := hm(10, 0)
	recs := []store.Record{observe(ent(p), "a", hm(9, 57), 3*time.Minute), observe(ent(p), "b", h, 0)}
	twin, st := openStore(t, memstore.Options{}), openStore(t, memstore.Options{})
	for _, s := range []store.Store{twin, st} {
		newFeed(t, s).write(recs...)
	}
	if err := st.Retain(ctx, h); err != nil {
		t.Fatal(err)
	}
	w := win(h, hm(10, 30))
	want := mustEntity(t, interval.NewReader(twin, interval.Options{}), p, w)
	got := mustEntity(t, interval.NewReader(st, interval.Options{}), p, w)
	if len(want.Elements[0].Pieces) != 1 || !want.Elements[0].Pieces[0].Before {
		t.Fatalf("the twin: %v", describeAll(want.Elements))
	}
	if len(got.Elements[0].Pieces) != 1 || got.Elements[0].Pieces[0].Before {
		t.Fatalf("after the retention: %v; want the piece to start at From", describeAll(got.Elements))
	}
	if df := diff([]interval.Element{clearBefore(got.Elements[0])}, []interval.Element{clearBefore(want.Elements[0])}); df != "" {
		t.Errorf("beyond Before:\n%s", df)
	}
	c := twinCheck{t: t, st: st, twin: twin, w: w, h: st.Horizon()}
	if !c.endsAtTheHorizon(ent(p)) {
		t.Error("the twin check does not see the reference that ends at the horizon")
	}
	if c.same("Entity", got.Elements, want.Elements) != 1 {
		t.Error("the twin check does not allow the one Before that differs")
	}
	// A nanosecond later the two agree.
	w = win(h.Add(time.Nanosecond), hm(10, 30))
	if df := diff(mustEntity(t, interval.NewReader(st, interval.Options{}), p, w).Elements, mustEntity(t, interval.NewReader(twin, interval.Options{}), p, w).Elements); df != "" {
		t.Errorf("a nanosecond after the horizon:\n%s", df)
	}
}
