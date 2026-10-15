package graphdiff_test

import (
	"errors"
	"fmt"
	"slices"
	"testing"
	"time"

	"pgregory.net/rapid"

	"github.com/lotannauo/toposhift/internal/identity"
	"github.com/lotannauo/toposhift/internal/lifecycle"
	"github.com/lotannauo/toposhift/internal/query/graphdiff"
	"github.com/lotannauo/toposhift/internal/query/interval"
	"github.com/lotannauo/toposhift/internal/store"
	"github.com/lotannauo/toposhift/internal/store/memstore"
)

// G1 and G10: Diff equals the oracle exactly, its reads follow the formula of
// the budget's shape, and it never writes.
func TestPropertyOracle(t *testing.T) {
	seen := map[string]int{}
	t.Cleanup(func() { t.Logf("what the drawn diffs held: %v", seen) })
	rapid.Check(t, func(t *rapid.T) {
		c := drawCase(t)
		sp := &spy{Store: c.st}
		res := checkOracle(t, sp, c.recs, c.g.all, c.scope, c.t1, c.t2, c.asOf, c.options())
		if sp.writes != 0 {
			t.Fatalf("%d writes or retentions through the differ's store", sp.writes)
		}
		tallyCoverage(seen, res)
	})
}

// tallyCoverage counts what a result holds, so a run can say what it covered.
func tallyCoverage(seen map[string]int, res graphdiff.Result) {
	for name, c := range map[string]graphdiff.Count{
		"created": res.Counts.Created, "deleted": res.Counts.Deleted, "changed": res.Counts.Changed,
		"transient": res.Counts.Transient, "quarantined": res.Counts.Quarantined,
	} {
		seen[name+" entities"] += c.Entities
		seen[name+" relations"] += c.Relations
	}
	for _, e := range res.Deleted {
		seen["deleted by "+e.DeleteSource.String()]++
	}
	for _, e := range res.Changed {
		seen["change "+e.Change.Kind.String()]++
		if len(e.Change.StateKeys) > 0 {
			seen["change with state keys"]++
		}
		if len(e.Change.AttributeKeys) > 0 {
			seen["change with attribute keys"]++
		}
	}
}

// presentAt reads an element's presence at the window's first instant (end
// false) or last (end true) from its pieces.
func presentAt(el interval.Element, t1, t2 time.Time, end bool) bool {
	if len(el.Pieces) == 0 {
		return false
	}
	if end {
		return el.Pieces[len(el.Pieces)-1].End.Equal(t2.Add(time.Nanosecond))
	}
	return el.Pieces[0].Start.Equal(t1)
}

// pointPresence is what the store's point reads say is present at x, with the
// cascade: each scope entity Alive, and each edge listed by Neighbors of a
// scope entity with both endpoints Alive. Quarantined entities and the edges
// touching them are left out.
func pointPresence(t tb, st store.Store, scope []identity.Fingerprint, x time.Time, token uint64) map[store.Subject]bool {
	t.Helper()
	alive := func(fp identity.Fingerprint) (bool, bool) {
		ok, err := st.Alive(ctx, fp, x, store.Scope{Layer: layerOf(ent(fp)), AsOf: token})
		var qe *store.QuarantineError
		if errors.As(err, &qe) {
			return false, true
		}
		if err != nil {
			t.Fatal(err)
		}
		return ok, false
	}
	out := map[store.Subject]bool{}
	for _, fp := range scopeOf(scope) {
		if ok, q := alive(fp); ok && !q {
			out[ent(fp)] = true
		}
		for _, dir := range []store.Direction{store.Forward, store.Reverse} {
			for _, l := range edgeLayers(fp.Type(), dir) {
				ns, err := st.Neighbors(ctx, fp, dir, x, store.Scope{Layer: l, AsOf: token})
				if err != nil {
					t.Fatal(err)
				}
				for _, n := range ns {
					sub := store.EdgeSubject(fp, n.Peer, n.Relation)
					if dir == store.Reverse {
						sub = store.EdgeSubject(n.Peer, fp, n.Relation)
					}
					a, qa := alive(sub.A)
					b, qb := alive(sub.B)
					if a && b && !qa && !qb {
						out[sub] = true
					}
				}
			}
		}
	}
	return out
}

// checkPointReads is G2: every element a diff reads is present at t1, and at
// t2, exactly when the store's point reads say so, every edge the point reads
// show present is read, and the net buckets are what those presences make
// them.
func checkPointReads(t tb, st store.Store, scope []identity.Fingerprint, t1, t2 time.Time, asOf uint64, rank map[lifecycle.Producer]int) {
	t.Helper()
	tok, ents, edges, err := graphdiff.ReadScope(ctx, st, scope, t1, t2, asOf, rank)
	if err != nil {
		t.Fatal(err)
	}
	at1, at2 := pointPresence(t, st, scope, t1, tok), pointPresence(t, st, scope, t2, tok)
	for _, el := range slices.Concat(ents, edges) {
		if el.Quarantine != nil || el.Uncascaded != nil {
			continue
		}
		s := el.Subject
		if p := presentAt(el, t1, t2, false); p != at1[s] {
			t.Fatalf("%s: present at t1 %s is %t, the point reads say %t", subjectString(s), ts(t1), p, at1[s])
		}
		if p := presentAt(el, t1, t2, true); p != at2[s] {
			t.Fatalf("%s: present at t2 %s is %t, the point reads say %t", subjectString(s), ts(t2), p, at2[s])
		}
		delete(at1, s)
		delete(at2, s)
	}
	for _, m := range []map[store.Subject]bool{at1, at2} {
		for s := range m {
			t.Fatalf("%s is present by the point reads but was not read", subjectString(s))
		}
	}
	res := diffOf(t, st, scope, t1, t2, tok, graphdiff.Options{Rank: rank, Policy: wide()})
	p1, p2 := pointPresence(t, st, scope, t1, tok), pointPresence(t, st, scope, t2, tok)
	var created, deleted []string
	for _, s := range sortedSubjects(p1, p2) {
		switch {
		case !p1[s] && p2[s]:
			created = append(created, subjectString(s))
		case p1[s] && !p2[s]:
			deleted = append(deleted, subjectString(s))
		}
	}
	if d := diffLines("created by the point reads", entrySubjects(res.Created), created); d != "" {
		t.Fatal(d)
	}
	if d := diffLines("deleted by the point reads", entrySubjects(res.Deleted), deleted); d != "" {
		t.Fatal(d)
	}
}

func sortedSubjects(ms ...map[store.Subject]bool) []store.Subject {
	var out []store.Subject
	for _, m := range ms {
		for s := range m {
			if !slices.Contains(out, s) {
				out = append(out, s)
			}
		}
	}
	slices.SortFunc(out, oracleOrder)
	return out
}

func entrySubjects(es []graphdiff.Entry) []string {
	out := []string{}
	for _, e := range es {
		out = append(out, subjectString(e.Element.Subject))
	}
	return out
}

func TestPropertyPointReads(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		c := drawCase(t)
		checkPointReads(t, c.st, c.scope, c.t1, c.t2, c.asOf, c.rank)
	})
}

// presences maps each element read that is not quarantined to its presence at
// the window's first and last instants.
func presences(t tb, st store.Store, scope []identity.Fingerprint, t1, t2 time.Time, tok uint64) map[store.Subject][2]bool {
	t.Helper()
	_, ents, edges, err := graphdiff.ReadScope(ctx, st, scope, t1, t2, tok, nil)
	if err != nil {
		t.Fatal(err)
	}
	out := map[store.Subject][2]bool{}
	for _, el := range slices.Concat(ents, edges) {
		if el.Quarantine == nil && el.Uncascaded == nil {
			out[el.Subject] = [2]bool{presentAt(el, t1, t2, false), presentAt(el, t1, t2, true)}
		}
	}
	return out
}

// net is an element's net bucket in a result: "created", "deleted" or "" for
// neither.
func net(res graphdiff.Result) map[store.Subject]string {
	out := map[store.Subject]string{}
	for _, e := range res.Created {
		out[e.Element.Subject] = "created"
	}
	for _, e := range res.Deleted {
		out[e.Element.Subject] = "deleted"
	}
	return out
}

// G3: chaining. For t1 < m < t2, an element's presence at m is the same at the
// end of Diff(t1, m) and at the start of Diff(m, t2), its presence at t1 and t2
// in Diff(t1, t2) is that of the two halves, and so the net buckets compose.
func TestPropertyChaining(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		c := drawCase(t)
		var ms []time.Time
		for _, r := range c.recs {
			for _, x := range []time.Time{r.EventTime, r.EventTime.Add(-1), r.EventTime.Add(1)} {
				if x.After(c.t1) && x.Before(c.t2) {
					ms = append(ms, x)
				}
			}
			if d, ok := deadline(r); ok && d.After(c.t1) && d.Before(c.t2) {
				ms = append(ms, d)
			}
		}
		if mid := c.t1.Add(c.t2.Sub(c.t1) / 2); mid.After(c.t1) && mid.Before(c.t2) {
			ms = append(ms, mid)
		}
		if len(ms) == 0 {
			t.Skip("no instant strictly between t1 and t2")
		}
		m := rapid.SampledFrom(ms).Draw(t, "m")
		tok := clampToken(c.asOf, c.st.LastSeq())
		whole := presences(t, c.st, c.scope, c.t1, c.t2, tok)
		first := presences(t, c.st, c.scope, c.t1, m, tok)
		second := presences(t, c.st, c.scope, m, c.t2, tok)
		var subs []store.Subject
		for _, mp := range []map[store.Subject][2]bool{whole, first, second} {
			for s := range mp {
				if !slices.Contains(subs, s) {
					subs = append(subs, s)
				}
			}
		}
		for _, s := range subs {
			if first[s][1] != second[s][0] {
				t.Fatalf("%s at m %s: present %t at the end of the first half, %t at the start of the second", subjectString(s), ts(m), first[s][1], second[s][0])
			}
			if whole[s][0] != first[s][0] || whole[s][1] != second[s][1] {
				t.Fatalf("%s: present %v over [t1, t2], %t at t1 in the first half, %t at t2 in the second", subjectString(s), whole[s], first[s][0], second[s][1])
			}
		}
		o := c.options()
		nw := net(diffOf(t, c.st, c.scope, c.t1, c.t2, tok, o))
		n1 := net(diffOf(t, c.st, c.scope, c.t1, m, tok, o))
		n2 := net(diffOf(t, c.st, c.scope, m, c.t2, tok, o))
		for _, s := range subs {
			want := ""
			switch a, b := n1[s], n2[s]; {
			case a == b && a != "":
				t.Fatalf("%s is %s in both halves", subjectString(s), a)
			case a == "" || b == "":
				want = a + b
			}
			if nw[s] != want {
				t.Fatalf("%s: %q over the whole, %q then %q over the halves", subjectString(s), nw[s], n1[s], n2[s])
			}
		}
	})
}

// existenceString is an entry without its descriptions or description change.
func existenceString(e graphdiff.Entry) string {
	e.Element.AtFrom, e.Element.AtLast, e.Change = nil, nil, nil
	return entryString(e)
}

func existenceStrings(es []graphdiff.Entry) []string {
	out := make([]string, len(es))
	for i, e := range es {
		out[i] = existenceString(e)
	}
	return out
}

// G4: the ranks change only the changed bucket and the descriptions: created,
// deleted, transient and quarantined, their existence, and every count but
// Changed are equal under any two ranks.
func TestPropertyRankOnlyChangesChanged(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		c := drawCase(t)
		tok := clampToken(c.asOf, c.st.LastSeq())
		a := diffOf(t, c.st, c.scope, c.t1, c.t2, tok, c.options())
		o := c.options()
		o.Rank = drawRank(t, "other rank")
		b := diffOf(t, c.st, c.scope, c.t1, c.t2, tok, o)
		for _, d := range []string{
			diffLines("created", existenceStrings(a.Created), existenceStrings(b.Created)),
			diffLines("deleted", existenceStrings(a.Deleted), existenceStrings(b.Deleted)),
			diffLines("transient", existenceStrings(a.Transient), existenceStrings(b.Transient)),
			diffLines("quarantined", elementStrings(a.Quarantined), elementStrings(b.Quarantined)),
			diffLines("events", eventStrings(graphdiff.Events(a)), eventStrings(graphdiff.Events(b))),
		} {
			if d != "" {
				t.Fatal(d)
			}
		}
		a.Counts.Changed, b.Counts.Changed = graphdiff.Count{}, graphdiff.Count{}
		a.Truncated.Changed, b.Truncated.Changed = 0, 0
		if a.Counts != b.Counts || a.Truncated != b.Truncated || a.Reads != b.Reads {
			t.Fatalf("counts %+v %+v reads %+v, under other ranks %+v %+v %+v", a.Counts, a.Truncated, a.Reads, b.Counts, b.Truncated, b.Reads)
		}
	})
}

// G5: the answer at token S equals the answer at the latest token of a fresh
// store fed only the records with Seq <= S, and every read uses the result's
// token, even when records are written while the diff reads.
func TestPropertyTokenPrefix(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		c := drawCase(t)
		late := make([]store.Record, uniform(t, "late records", 0, 4))
		for i := range late {
			late[i] = drawRecord(t, c.g)
			late[i].Seq = c.st.LastSeq() + uint64(i) + 1
		}
		sp := &spy{Store: c.st, onWindow: func(n int) {
			if n == 1 && len(late) > 0 {
				if err := c.st.Write(ctx, late); err != nil {
					t.Fatal(err)
				}
			}
		}}
		res := diffOf(t, sp, c.scope, c.t1, c.t2, c.asOf, c.options())
		for _, tok := range sp.tokens {
			if tok != res.Token {
				t.Fatalf("a read at token %d, the result's is %d", tok, res.Token)
			}
		}
		var prefix []store.Record
		for _, r := range c.recs {
			if r.Seq <= res.Token {
				prefix = append(prefix, r)
			}
		}
		fresh := freshStore(t, memstore.Options{}, prefix)
		defer fresh.Close()
		want := diffOf(t, fresh, c.scope, c.t1, c.t2, store.Latest, c.options())
		if d := sameResult(res, want); d != "" || res.Token != want.Token || res.Reads != want.Reads {
			t.Fatalf("token %d reads %+v, the prefix store's %d %+v: %s", res.Token, res.Reads, want.Token, want.Reads, d)
		}
	})
}

// G6: with no two records of one producer and subject at one instant, the
// write order changes nothing.
func TestPropertyArrivalOrder(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		g := newGrid()
		type key struct {
			s store.Subject
			p lifecycle.Producer
			t int64
		}
		seen := map[key]bool{}
		var recs []store.Record
		for _, r := range drawRecords(t, g, 70) {
			k := key{r.Subject, r.Producer, r.EventTime.UnixNano()}
			if !seen[k] {
				seen[k] = true
				recs = append(recs, r)
			}
		}
		a, b := freshStore(t, memstore.Options{}, nil), freshStore(t, memstore.Options{}, nil)
		defer a.Close()
		defer b.Close()
		writeShuffled(t, a, recs)
		writeShuffled(t, b, recs)
		t1, t2 := drawInstants(t)
		o := graphdiff.Options{Rank: drawRank(t, "rank"), Policy: wide()}
		scope := drawScope(t, g)
		ra := diffOf(t, a, scope, t1, t2, store.Latest, o)
		rb := diffOf(t, b, scope, t1, t2, store.Latest, o)
		if d := sameResult(ra, rb); d != "" || ra.Reads != rb.Reads {
			t.Fatalf("two write orders: reads %+v, %+v: %s", ra.Reads, rb.Reads, d)
		}
	})
}

// G7: records with an event time after t2 change nothing.
func TestPropertyAfterT2ChangesNothing(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		c := drawCase(t)
		var kept []store.Record
		for _, r := range c.recs {
			if !r.EventTime.After(c.t2) {
				r.Seq = uint64(len(kept) + 1)
				kept = append(kept, r)
			}
		}
		fresh := freshStore(t, memstore.Options{}, kept)
		defer fresh.Close()
		got := diffOf(t, c.st, c.scope, c.t1, c.t2, store.Latest, c.options())
		want := diffOf(t, fresh, c.scope, c.t1, c.t2, store.Latest, c.options())
		if d := sameResult(got, want); d != "" || got.Reads != want.Reads {
			t.Fatalf("with the records after t2: reads %+v, without %+v: %s", got.Reads, want.Reads, d)
		}
	})
}

// G8: every entry of Diff(S) is in Diff(S ∪ S') in the same bucket, the same.
func TestPropertyScopeMonotonic(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		c := drawCase(t)
		small := diffOf(t, c.st, c.scope, c.t1, c.t2, c.asOf, c.options())
		big := diffOf(t, c.st, slices.Concat(c.scope, drawScope(t, c.g)), c.t1, c.t2, small.Token, c.options())
		for _, b := range []struct {
			name     string
			sub, sup []string
		}{
			{"created", entryStrings(small.Created), entryStrings(big.Created)},
			{"deleted", entryStrings(small.Deleted), entryStrings(big.Deleted)},
			{"changed", entryStrings(small.Changed), entryStrings(big.Changed)},
			{"transient", entryStrings(small.Transient), entryStrings(big.Transient)},
			{"quarantined", elementStrings(small.Quarantined), elementStrings(big.Quarantined)},
		} {
			for _, e := range b.sub {
				if !slices.Contains(b.sup, e) {
					t.Fatalf("%s: %s is not in the larger scope's diff:\n  %v", b.name, e, b.sup)
				}
			}
		}
	})
}

// G9: per bucket, the counts are the entries listed plus those truncated, by
// subject kind; a capped list is the first MaxListed entries of the uncapped
// one; and the counts do not depend on the cap.
func TestPropertyCountsAndTruncation(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		c := drawCase(t)
		tok := clampToken(c.asOf, c.st.LastSeq())
		full := diffOf(t, c.st, c.scope, c.t1, c.t2, tok, c.options())
		o := c.options()
		o.Policy.MaxListed = uniform(t, "max listed", 1, 3)
		capped := diffOf(t, c.st, c.scope, c.t1, c.t2, tok, o)
		if capped.Counts != full.Counts {
			t.Fatalf("counts %+v with a cap of %d, %+v without", capped.Counts, o.Policy.MaxListed, full.Counts)
		}
		for _, b := range []struct {
			name        string
			list, whole []graphdiff.Entry
			count       graphdiff.Count
			dropped     int
		}{
			{"created", capped.Created, full.Created, capped.Counts.Created, capped.Truncated.Created},
			{"deleted", capped.Deleted, full.Deleted, capped.Counts.Deleted, capped.Truncated.Deleted},
			{"changed", capped.Changed, full.Changed, capped.Counts.Changed, capped.Truncated.Changed},
			{"transient", capped.Transient, full.Transient, capped.Counts.Transient, capped.Truncated.Transient},
		} {
			var whole graphdiff.Count
			for _, e := range b.whole {
				tally(&whole, e.Element.Subject)
			}
			n := min(len(b.whole), o.Policy.MaxListed)
			if whole != b.count || b.count.Entities+b.count.Relations != len(b.list)+b.dropped ||
				fmt.Sprint(entryStrings(b.list)) != fmt.Sprint(entryStrings(b.whole[:n])) {
				t.Fatalf("%s: count %+v, %d listed, %d dropped; uncapped %+v, %d listed", b.name, b.count, len(b.list), b.dropped, whole, len(b.whole))
			}
		}
		n := min(len(full.Quarantined), o.Policy.MaxListed)
		if q := capped.Counts.Quarantined; q.Entities+q.Relations != len(capped.Quarantined)+capped.Truncated.Quarantined ||
			fmt.Sprint(elementStrings(capped.Quarantined)) != fmt.Sprint(elementStrings(full.Quarantined[:n])) {
			t.Fatalf("quarantined: count %+v, %d listed, %d dropped; uncapped %d listed", q, len(capped.Quarantined), capped.Truncated.Quarantined, len(full.Quarantined))
		}
	})
}
