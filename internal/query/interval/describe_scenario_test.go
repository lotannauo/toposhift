package interval_test

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/lotannauo/toposhift/internal/catalog"
	"github.com/lotannauo/toposhift/internal/identity"
	"github.com/lotannauo/toposhift/internal/lifecycle"
	"github.com/lotannauo/toposhift/internal/query/interval"
	"github.com/lotannauo/toposhift/internal/store"
	"github.com/lotannauo/toposhift/internal/store/memstore"
	"github.com/lotannauo/toposhift/internal/store/storetest"
)

// Scenarios for descriptions and Entities, with exact answers. Times are on
// 2026-01-01 UTC, written hh:mm.

const k8sCluster lifecycle.Producer = "k8scluster/default"

func desc(p lifecycle.Producer, payload string) *interval.Description {
	d := &interval.Description{Producer: p}
	if payload != "" {
		d.Payload = []byte(payload)
	}
	return d
}

// with is r with the payload given.
func with(r store.Record, payload string) store.Record {
	r.Payload = []byte(payload)
	return r
}

func described(el interval.Element, from, last *interval.Description) interval.Element {
	el.AtFrom, el.AtLast = from, last
	return el
}

func checkDescribed(t *testing.T, what string, got, want []interval.Element) {
	t.Helper()
	if d := describedDiff(got, want); d != "" {
		t.Errorf("%s:\n%s", what, d)
	}
}

func describing(st store.Store, rank map[lifecycle.Producer]int) *interval.Reader {
	return interval.NewReader(st, interval.Options{Describe: true, Rank: rank})
}

// A pod described by two producers: the object watcher's reference lapses at
// 10:05, the cluster receiver's never does. The description goes by rank, not
// by which producer spoke last, so with no rank the receiver (the smaller
// name) wins at both ends; ranked above it, the watcher wins at From and the
// receiver at the last instant, a change of source with no change of
// existence.
func TestDescribeBySourceRankNotRecency(t *testing.T) {
	p := mkfp(t, catalog.K8sPod, 1)
	const objects, cluster = `{"name":"p","phase":"Running"}`, `{"k8s.pod.name":"p","k8s.pod.phase":"Running"}`
	st := openStore(t, memstore.Options{})
	newFeed(t, st).write(
		with(run(ent(p), k8s, hm(9, 0), 10*time.Minute, hm(9, 55)), objects),
		with(observe(ent(p), k8sCluster, hm(9, 0), 0), cluster),
	)
	w := win(hm(10, 0), hm(10, 30))
	el := elem(ent(p), interval.ClassStable, 0, pc(hm(10, 0), hm(10, 30), true, lifecycle.EndUnknown, k8sCluster, k8s))
	for _, tc := range []struct {
		name     string
		rank     map[lifecycle.Producer]int
		from, to *interval.Description
	}{
		{"ranked", map[lifecycle.Producer]int{k8s: 2, k8sCluster: 1}, desc(k8s, objects), desc(k8sCluster, cluster)},
		{"no rank", nil, desc(k8sCluster, cluster), desc(k8sCluster, cluster)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			res := mustEntity(t, describing(st, tc.rank), p, w)
			checkDescribed(t, "Entity(P)", res.Elements, []interval.Element{described(el, tc.from, tc.to)})
			plain := mustEntity(t, interval.NewReader(st, interval.Options{Rank: tc.rank}), p, w)
			checkDescribed(t, "Entity(P) with Describe off", plain.Elements, []interval.Element{el})
		})
	}
}

// The description at the window's ends: at From, and at To-1ns, never at To.
// A record at To or later is not folded, so only a deadline can make To and
// To-1ns differ: the first two cases tell them apart.
func TestDescribeAtTheWindowsEnds(t *testing.T) {
	w := win(hm(10, 0), hm(10, 30))
	last := hm(10, 30).Add(-time.Nanosecond)
	for i, tc := range []struct {
		name string
		recs func(store.Subject) []store.Record
		w    interval.Window
		want func(store.Subject) interval.Element
	}{
		{"a deadline exactly at To", func(s store.Subject) []store.Record {
			return []store.Record{with(run(s, k8s, hm(9, 0), 30*time.Minute, hm(10, 0)), "a")}
		}, w, func(s store.Subject) interval.Element {
			return described(elem(s, interval.ClassStable, 0, pc(hm(10, 0), hm(10, 30), true, lifecycle.EndUnknown, k8s)),
				desc(k8s, "a"), desc(k8s, "a"))
		}},
		{"the winner's deadline exactly at To", func(s store.Subject) []store.Record {
			return []store.Record{
				with(run(s, "p1", hm(9, 0), 30*time.Minute, hm(10, 0)), "a"),
				with(observe(s, "p2", hm(9, 0), 0), "c"),
			}
		}, w, func(s store.Subject) interval.Element {
			return described(elem(s, interval.ClassStable, 0, pc(hm(10, 0), hm(10, 30), true, lifecycle.EndUnknown, "p1", "p2")),
				desc("p1", "a"), desc("p1", "a"))
		}},
		{"deleted exactly at To", func(s store.Subject) []store.Record {
			return []store.Record{with(observe(s, k8s, hm(9, 0), 0), "a"), deleted(s, k8s, hm(10, 30))}
		}, w, func(s store.Subject) interval.Element {
			return described(elem(s, interval.ClassStable, 0, pc(hm(10, 0), hm(10, 30), true, lifecycle.EndUnknown, k8s)),
				desc(k8s, "a"), desc(k8s, "a"))
		}},
		{"a new payload exactly at To", func(s store.Subject) []store.Record {
			return []store.Record{with(observe(s, k8s, hm(9, 0), 0), "a"), with(observe(s, k8s, hm(10, 30), 0), "b")}
		}, w, func(s store.Subject) interval.Element {
			return described(elem(s, interval.ClassStable, 0, pc(hm(10, 0), hm(10, 30), true, lifecycle.EndUnknown, k8s)),
				desc(k8s, "a"), desc(k8s, "a"))
		}},
		{"a new payload one nanosecond before To", func(s store.Subject) []store.Record {
			return []store.Record{with(observe(s, k8s, hm(9, 0), 0), "a"), with(observe(s, k8s, last, 0), "b")}
		}, w, func(s store.Subject) interval.Element {
			return described(elem(s, interval.ClassStable, 0, pc(hm(10, 0), hm(10, 30), true, lifecycle.EndUnknown, k8s)),
				desc(k8s, "a"), desc(k8s, "b"))
		}},
		{"a new payload exactly at From", func(s store.Subject) []store.Record {
			return []store.Record{with(observe(s, k8s, hm(9, 0), 0), "a"), with(observe(s, k8s, hm(10, 0), 0), "b")}
		}, w, func(s store.Subject) interval.Element {
			return described(elem(s, interval.ClassStable, 0, pc(hm(10, 0), hm(10, 30), true, lifecycle.EndUnknown, k8s)),
				desc(k8s, "b"), desc(k8s, "b"))
		}},
		{"two payloads at one instant: the later sequence number", func(s store.Subject) []store.Record {
			return []store.Record{with(observe(s, k8s, hm(9, 0), 0), "a"), with(observe(s, k8s, hm(9, 0), 0), "b")}
		}, w, func(s store.Subject) interval.Element {
			return described(elem(s, interval.ClassStable, 0, pc(hm(10, 0), hm(10, 30), true, lifecycle.EndUnknown, k8s)),
				desc(k8s, "b"), desc(k8s, "b"))
		}},
		{"appears inside the window", func(s store.Subject) []store.Record {
			return []store.Record{with(observe(s, k8s, hm(10, 10), 0), "b")}
		}, w, func(s store.Subject) interval.Element {
			return described(elem(s, interval.ClassAppeared, 1, pc(hm(10, 10), hm(10, 30), false, lifecycle.EndUnknown, k8s)),
				nil, desc(k8s, "b"))
		}},
		{"ends inside the window", func(s store.Subject) []store.Record {
			return []store.Record{with(observe(s, k8s, hm(9, 0), 0), "a"), deleted(s, k8s, hm(10, 10))}
		}, w, func(s store.Subject) interval.Element {
			return described(elem(s, interval.ClassDisappeared, 1, pc(hm(10, 0), hm(10, 10), true, lifecycle.EndProducer, k8s)),
				desc(k8s, "a"), nil)
		}},
		{"flapping, described at both ends", func(s store.Subject) []store.Record {
			return []store.Record{
				with(observe(s, k8s, hm(9, 0), 0), "a"), deleted(s, k8s, hm(10, 5)),
				with(observe(s, k8s, hm(10, 20), 0), "b"),
			}
		}, w, func(s store.Subject) interval.Element {
			return described(elem(s, interval.ClassFlapping, 2,
				pc(hm(10, 0), hm(10, 5), true, lifecycle.EndProducer, k8s),
				pc(hm(10, 20), hm(10, 30), false, lifecycle.EndUnknown, k8s)),
				desc(k8s, "a"), desc(k8s, "b"))
		}},
		{"transient: neither end", func(s store.Subject) []store.Record {
			return []store.Record{with(observe(s, k8s, hm(10, 5), 0), "a"), deleted(s, k8s, hm(10, 10))}
		}, w, func(s store.Subject) interval.Element {
			return elem(s, interval.ClassTransient, 2, pc(hm(10, 5), hm(10, 10), false, lifecycle.EndProducer, k8s))
		}},
		{"an empty payload", func(s store.Subject) []store.Record {
			return []store.Record{with(observe(s, k8s, hm(9, 0), 0), "")}
		}, w, func(s store.Subject) interval.Element {
			return described(elem(s, interval.ClassStable, 0, pc(hm(10, 0), hm(10, 30), true, lifecycle.EndUnknown, k8s)),
				desc(k8s, ""), desc(k8s, ""))
		}},
		{"an instant", func(s store.Subject) []store.Record {
			return []store.Record{with(observe(s, k8s, hm(9, 0), 0), "a"), with(observe(s, k8s, hm(10, 0).Add(time.Nanosecond), 0), "b")}
		}, win(hm(10, 0), hm(10, 0).Add(time.Nanosecond)), func(s store.Subject) interval.Element {
			return described(elem(s, interval.ClassStable, 0, pc(hm(10, 0), hm(10, 0).Add(time.Nanosecond), true, lifecycle.EndUnknown, k8s)),
				desc(k8s, "a"), desc(k8s, "a"))
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			st := openStore(t, memstore.Options{})
			pod, node := mkfp(t, catalog.K8sPod, byte(i+1)), mkfp(t, catalog.K8sNode, 1)
			e := edge(pod, node, catalog.ScheduledOn)
			newFeed(t, st).write(append(tc.recs(ent(pod)), tc.recs(e)...)...)
			res := mustEntity(t, describing(st, nil), pod, tc.w)
			checkDescribed(t, "Entity", res.Elements, []interval.Element{tc.want(ent(pod))})
			res = mustEdges(t, interval.NewReader(st, interval.Options{Raw: true, Describe: true}), pod, store.Forward, tc.w)
			checkDescribed(t, "raw Edges", res.Elements, []interval.Element{tc.want(e)})
			if len(res.Elements) != 1 {
				return
			}
			if el := res.Elements[0]; el.AtFrom != nil && el.AtLast != nil && len(el.AtFrom.Payload) > 0 && len(el.AtLast.Payload) > 0 {
				el.AtFrom.Payload[0] = 'z'
				if el.AtLast.Payload[0] == 'z' {
					t.Error("AtFrom and AtLast share their payload")
				}
			}
		})
	}
}

// The rank rule: listed beats unlisted, whatever the rank; a higher rank beats
// a lower one; the smaller name breaks a tie. The reader copies its Rank.
func TestDescribeRankRule(t *testing.T) {
	p := mkfp(t, catalog.K8sPod, 1)
	st := openStore(t, memstore.Options{})
	newFeed(t, st).write(
		with(observe(ent(p), "p1", hm(9, 0), 0), "1"),
		with(observe(ent(p), "p2", hm(9, 0), 0), "2"),
		with(observe(ent(p), "p3", hm(9, 0), 0), "3"),
	)
	w := win(hm(10, 0), hm(10, 30))
	for _, tc := range []struct {
		rank map[lifecycle.Producer]int
		want lifecycle.Producer
	}{
		{nil, "p1"},
		{map[lifecycle.Producer]int{}, "p1"},
		{map[lifecycle.Producer]int{"p3": 0}, "p3"},
		{map[lifecycle.Producer]int{"p3": -1}, "p3"},
		{map[lifecycle.Producer]int{"p2": 1, "p3": 1}, "p2"},
		{map[lifecycle.Producer]int{"p1": 1, "p3": 5}, "p3"},
		{map[lifecycle.Producer]int{"p1": -1, "p2": -2, "p3": -3}, "p1"},
		{map[lifecycle.Producer]int{"p1": 0, "p2": 0, "p3": 0}, "p1"},
	} {
		name := fmt.Sprint(tc.rank)
		r := describing(st, tc.rank)
		if tc.rank != nil {
			// A change to the caller's map after NewReader changes no answer.
			clear(tc.rank)
			tc.rank["p2"] = 1000
		}
		res := mustEntity(t, r, p, w)
		if len(res.Elements) != 1 {
			t.Fatalf("Entity(P) = %v", describeAll(res.Elements))
		}
		el := res.Elements[0]
		want := desc(tc.want, strings.TrimPrefix(string(tc.want), "p"))
		if g, w := fmtDescription(el.AtFrom), fmtDescription(want); g != w {
			t.Errorf("rank %s: AtFrom %s, want %s", name, g, w)
		}
		if g, w := fmtDescription(el.AtLast), fmtDescription(want); g != w {
			t.Errorf("rank %s: AtLast %s, want %s", name, g, w)
		}
	}
}

// An edge read with the cascade is described where its cut piece is: the pod
// is deleted at 10:10, so the edge has no description at the last instant,
// although its own producer still describes it; read raw, it has one.
func TestDescribeEdgesWhereTheirCutPiecesAre(t *testing.T) {
	p, n := mkfp(t, catalog.K8sPod, 1), mkfp(t, catalog.K8sNode, 1)
	e := edge(p, n, catalog.ScheduledOn)
	st := openStore(t, memstore.Options{})
	newFeed(t, st).write(
		observe(ent(n), k8s, hm(9, 0), 0),
		observe(ent(p), k8s, hm(9, 0), 0), deleted(ent(p), k8s, hm(10, 10)),
		with(observe(e, k8s, hm(9, 0), 0), "e1"), with(observe(e, k8s, hm(10, 5), 0), "e2"),
	)
	w := win(hm(10, 0), hm(10, 30))
	cascade := described(elem(e, interval.ClassDisappeared, 1, pc(hm(10, 0), hm(10, 10), true, lifecycle.EndCascade, k8s)),
		desc(k8s, "e1"), nil)
	raw := described(elem(e, interval.ClassStable, 0, pc(hm(10, 0), hm(10, 30), true, lifecycle.EndUnknown, k8s)),
		desc(k8s, "e1"), desc(k8s, "e2"))
	r := describing(st, nil)
	checkDescribed(t, "Edges(P, Forward)", mustEdges(t, r, p, store.Forward, w).Elements, []interval.Element{cascade})
	checkDescribed(t, "Edges(N, Reverse)", mustEdges(t, r, n, store.Reverse, w).Elements, []interval.Element{cascade})
	rawR := interval.NewReader(st, interval.Options{Raw: true, Describe: true})
	checkDescribed(t, "raw Edges(P, Forward)", mustEdges(t, rawR, p, store.Forward, w).Elements, []interval.Element{raw})

	_, out, err := rawR.EdgesBatch(ctx, []identity.Fingerprint{n, n}, store.Reverse, w)
	if err != nil || len(out) != 2 {
		t.Fatalf("EdgesBatch = %d answers, %v", len(out), err)
	}
	out[0][0].AtFrom.Payload[0] = 'z'
	out[0][0].AtLast.Producer = "changed"
	checkDescribed(t, "EdgesBatch[1] after a change to EdgesBatch[0]", out[1], []interval.Element{raw})
}

// A quarantined entity has no description; an edge to it is uncascaded and
// described on its own pieces.
func TestDescribeQuarantine(t *testing.T) {
	inner := openStore(t, memstore.Options{Policy: storetest.QuarantinePolicy()})
	h, n := mkfp(t, catalog.Host, 1), mkfp(t, catalog.K8sNode, 1)
	e := edge(n, h, catalog.RunsOn)
	host := func(boot string, at time.Time) store.Record {
		r := observe(ent(h), "node-collector", at, 15*time.Minute)
		r.Boot = boot
		return r
	}
	newFeed(t, inner).write(
		host("b1", hm(9, 0)), host("b2", hm(9, 5)), host("b1", hm(9, 10)),
		observe(ent(n), k8s, hm(9, 0), 0),
		observe(e, "node-collector", hm(9, 0), 0),
	)
	r := describing(inner, nil)
	w := win(hm(10, 0), hm(10, 30))
	tok, els, err := r.Entities(ctx, []identity.Fingerprint{h, n}, w)
	if err != nil || tok != inner.LastSeq() || len(els) != 2 {
		t.Fatalf("Entities(H, N) = %d, %v, %v", tok, describeAll(els), err)
	}
	if q := els[0]; q.Quarantine == nil || q.AtFrom != nil || q.AtLast != nil || q.Pieces != nil {
		t.Errorf("Entities[H] = %s", describeWithDescriptions(q))
	}
	checkDescribed(t, "Entities[N]", els[1:], []interval.Element{described(
		elem(ent(n), interval.ClassStable, 0, pc(hm(10, 0), hm(10, 30), true, lifecycle.EndUnknown, k8s)),
		desc(k8s, "x"), desc(k8s, "x"))})
	want := described(elem(e, interval.ClassStable, 0, pc(hm(10, 0), hm(10, 30), true, lifecycle.EndUnknown, "node-collector")),
		desc("node-collector", "x"), desc("node-collector", "x"))
	want.Uncascaded = els[0].Quarantine
	checkDescribed(t, "Edges(N, Forward)", mustEdges(t, r, n, store.Forward, w).Elements, []interval.Element{want})
}

// hooked runs a hook after the n-th EntityWindow returns; a WindowSince of
// entities counts as one EntityWindow per fingerprint, and the hook runs after
// it returns for each of them.
type hooked struct {
	*spy
	n     int
	after func(n int)
}

func (h *hooked) EntityWindow(ctx context.Context, fp identity.Fingerprint, from, to time.Time, sc store.Scope) ([]store.Record, error) {
	recs, err := h.spy.EntityWindow(ctx, fp, from, to, sc)
	h.n++
	if h.after != nil {
		h.after(h.n)
	}
	return recs, err
}

func (h *hooked) WindowSince(ctx context.Context, fps []identity.Fingerprint, d store.Direction, from, to time.Time, sc store.Scope, o store.SinceOptions) ([]store.Since, error) {
	out, err := h.spy.WindowSince(ctx, fps, d, from, to, sc, o)
	if d != 0 {
		return out, err
	}
	for range fps {
		h.n++
		if h.after != nil {
			h.after(h.n)
		}
	}
	return out, err
}

func TestEntities(t *testing.T) {
	inner := openStore(t, memstore.Options{})
	sc := newScenarioA(t, newFeed(t, inner))
	w := win(hm(10, 0), hm(10, 30))
	stable := func(fp identity.Fingerprint) interval.Element {
		return elem(ent(fp), interval.ClassStable, 0, pc(hm(10, 0), hm(10, 30), true, lifecycle.EndUnknown, k8s))
	}

	t.Run("one read per distinct entity, at one token", func(t *testing.T) {
		st := &spy{Store: inner}
		tok, out, err := describing(st, nil).Entities(ctx, []identity.Fingerprint{sc.n, sc.s, sc.n, sc.a}, w)
		if err != nil || tok != 8 {
			t.Fatalf("Entities = %d, %v", tok, err)
		}
		x := desc(k8s, "x")
		checkDescribed(t, "Entities", out, []interval.Element{
			described(stable(sc.n), x, x), described(stable(sc.s), x, x),
			described(stable(sc.n), x, x), described(stable(sc.a), x, x),
		})
		// One Alive per distinct entity, then one read of all three in their layer.
		wantLog := []string{
			"Alive k8s.node L2", "Alive k8s.pod L2", "Alive k8s.pod L2",
			"EntityWindow k8s.node L2", "EntityWindow k8s.pod L2", "EntityWindow k8s.pod L2",
		}
		if !slices.Equal(st.log, wantLog) {
			t.Errorf("reads %q, want %q", st.log, wantLog)
		}
		out[0].Pieces[0].Producers[0] = "changed"
		out[0].AtFrom.Payload[0] = 'z'
		out[0].AtLast.Producer = "changed"
		checkDescribed(t, "Entities[2] after a change to Entities[0]", out[2:3], []interval.Element{described(stable(sc.n), x, x)})
		if _, writes := st.counts(); writes != 0 {
			t.Errorf("the reader wrote %d times", writes)
		}
	})

	t.Run("the token is pinned once", func(t *testing.T) {
		before := inner.LastSeq()
		late := newFeed(t, inner)
		st := &hooked{spy: &spy{Store: inner}}
		st.after = func(n int) {
			if n == 1 {
				late.write(deleted(ent(sc.a), k8s, hm(10, 1)))
			}
		}
		tok, out, err := interval.NewReader(st, interval.Options{}).Entities(ctx, []identity.Fingerprint{sc.s, sc.a}, w)
		if err != nil || tok != before || inner.LastSeq() != before+1 {
			t.Fatalf("Entities = %d, %v; last %d, want the token %d from before the write", tok, err, inner.LastSeq(), before)
		}
		check(t, "Entities", out, []interval.Element{stable(sc.s), stable(sc.a)})
	})

	t.Run("no fingerprints", func(t *testing.T) {
		st := &spy{Store: inner}
		tok, out, err := interval.NewReader(st, interval.Options{}).Entities(ctx, nil, w)
		if err != nil || out == nil || len(out) != 0 || tok != inner.LastSeq() {
			t.Errorf("Entities(nil) = %d, %v, %v", tok, out, err)
		}
		if reads, _ := st.counts(); reads != 0 {
			t.Errorf("%d reads: %q", reads, st.log)
		}
	})

	t.Run("arguments", func(t *testing.T) {
		st := &spy{Store: inner}
		r := interval.NewReader(st, interval.Options{Describe: true})
		unknown, err := identity.FingerprintFromHash("x.unknown", [identity.FingerprintBytes]byte{1})
		if err != nil {
			t.Fatal(err)
		}
		for _, tc := range []struct {
			name string
			fps  []identity.Fingerprint
			w    interval.Window
			says string
		}{
			{"an empty window", []identity.Fingerprint{sc.s}, win(hm(10, 0), hm(10, 0)), "window"},
			{"a reversed window", []identity.Fingerprint{sc.s}, win(hm(10, 1), hm(10, 0)), "window"},
			{"the window first", []identity.Fingerprint{{}}, win(hm(10, 1), hm(10, 0)), "window"},
			{"a zero fingerprint", []identity.Fingerprint{sc.s, {}}, w, "fingerprint 1"},
			{"an unknown type", []identity.Fingerprint{unknown}, w, "unknown entity type"},
		} {
			tok, out, err := r.Entities(ctx, tc.fps, tc.w)
			if !errors.Is(err, store.ErrInvalid) || !strings.Contains(err.Error(), tc.says) || out != nil || tok != 0 {
				t.Errorf("%s: %d, %v, %v; want ErrInvalid naming %q", tc.name, tok, out, err, tc.says)
			}
		}
		if reads, _ := st.counts(); reads != 0 {
			t.Errorf("%d reads for invalid arguments: %q", reads, st.log)
		}
	})

	t.Run("cancelled and closed", func(t *testing.T) {
		st := openStore(t, memstore.Options{})
		newScenarioA(t, newFeed(t, st))
		r := interval.NewReader(st, interval.Options{})
		cancelled, cancel := context.WithCancel(ctx)
		cancel()
		if _, out, err := r.Entities(cancelled, []identity.Fingerprint{sc.n}, w); !errors.Is(err, context.Canceled) || out != nil {
			t.Errorf("cancelled: %v, %v", out, err)
		}
		if err := st.Close(); err != nil {
			t.Fatal(err)
		}
		if _, out, err := r.Entities(ctx, []identity.Fingerprint{sc.n}, w); !errors.Is(err, store.ErrClosed) || out != nil {
			t.Errorf("closed: %v, %v", out, err)
		}
	})
}

// After a retention, an Entities call whose window starts at the horizon gives
// the existence a store that kept everything gives. Its descriptions are refused
// with ErrRetained while the winning producer's latest assertion is before the
// horizon, whose payload is not kept, and given once one at or after it is in the
// window. A window that starts before the horizon is refused by the store.
func TestEntitiesRetention(t *testing.T) {
	p, h := mkfp(t, catalog.K8sPod, 1), mkfp(t, catalog.Host, 1)
	w := win(hm(10, 0), hm(10, 30))
	recs := []store.Record{observe(ent(p), k8s, hm(9, 0), 0), observe(ent(h), k8s, hm(9, 0), 0)}
	for _, tc := range []struct {
		name  string
		keep  [4]bool
		layer string
		fp    identity.Fingerprint // the entity in the retained layer
	}{
		{"L1 retained", [4]bool{true, false, true, true}, "L1", h},
		{"L2 retained", [4]bool{true, true, false, true}, "L2", p},
	} {
		t.Run(tc.name, func(t *testing.T) {
			inner, twin := openStore(t, memstore.Options{Keep: tc.keep}), openStore(t, memstore.Options{})
			for _, st := range []store.Store{inner, twin} {
				newFeed(t, st).write(recs...)
			}
			if err := inner.Retain(ctx, hm(10, 0)); err != nil {
				t.Fatal(err)
			}
			fps := []identity.Fingerprint{p, h}
			_, got, err := interval.NewReader(inner, interval.Options{}).Entities(ctx, fps, w)
			_, want, _ := interval.NewReader(twin, interval.Options{}).Entities(ctx, fps, w)
			if err != nil || diff(got, want) != "" {
				t.Errorf("Entities after the retention = %v, %v; want %v", describeAll(got), err, describeAll(want))
			}
			_, out, err := interval.NewReader(inner, interval.Options{Describe: true}).Entities(ctx, fps, w)
			if !errors.Is(err, interval.ErrRetained) || !errors.Is(err, errors.ErrUnsupported) || !strings.Contains(err.Error(), tc.fp.String()) || out != nil {
				t.Errorf("Entities with descriptions = %v, %v; want ErrRetained naming %s", out, err, tc.fp)
			}
			_, _, err = interval.NewReader(inner, interval.Options{}).Entities(ctx, fps, win(hm(9, 59), hm(10, 30)))
			if !errors.Is(err, store.ErrBeforeHorizon) || errors.Is(err, interval.ErrRetained) || !strings.Contains(err.Error(), tc.layer) {
				t.Errorf("a window before the horizon: %v; want ErrBeforeHorizon naming %s", err, tc.layer)
			}
			// A re-assertion at the horizon is in the window: the descriptions are known.
			for _, st := range []store.Store{inner, twin} {
				newFeed(t, st).write(observe(ent(tc.fp), k8s, hm(10, 0), 0))
			}
			_, got, err = interval.NewReader(inner, interval.Options{Describe: true}).Entities(ctx, []identity.Fingerprint{tc.fp}, w)
			_, want, _ = interval.NewReader(twin, interval.Options{Describe: true}).Entities(ctx, []identity.Fingerprint{tc.fp}, w)
			if err != nil {
				t.Fatalf("Entities with a description at the horizon: %v", err)
			}
			checkDescribed(t, "Entities with a description at the horizon", got, want)
		})
	}
}
