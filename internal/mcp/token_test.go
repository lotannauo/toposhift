package mcp

import (
	"bytes"
	"encoding/json"
	"strconv"
	"strings"
	"testing"

	"github.com/lotannauo/toposhift/internal/catalog"
	"github.com/lotannauo/toposhift/internal/store"
)

// lateWrites changes the world the way a writer does while a call is running: a
// pod scheduled in the window, a rename, an edge that ends.
func (w *world) lateWrites() {
	w.feed.write(
		observe(ent(w.p7), objs, hm(10, 21), 0, `{"name":"renamed-web-6","namespace":"shop","phase":"Running"}`),
		observe(ent(w.n1), objs, hm(10, 21), 0, `{"name":"renamed-worker-1"}`),
		observe(ent(w.n2), objs, hm(10, 21), 0, `{"name":"renamed-worker-2"}`),
		deleted(edge(w.n1, w.h1, catalog.RunsOn), agent, hm(10, 20)),
		deleted(edge(w.p1, w.n1, catalog.ScheduledOn), objs, hm(10, 22)),
		observe(edge(w.p7, w.n1, catalog.ScheduledOn), objs, hm(10, 15), 0, `{}`),
	)
}

// call is one question to a tool, for the tests that ask the same question again.
type call struct {
	tool string
	ask  func(s *Source, asof string) (any, error)
}

func calls(w *world) []call {
	return []call{
		{"blast_radius", func(s *Source, asof string) (any, error) {
			return s.BlastRadius(ctx, BlastRadiusInput{Entity: w.h1.String(), From: w10, To: w1030, AsOf: asof})
		}},
		{"what_changed", func(s *Source, asof string) (any, error) {
			return s.WhatChanged(ctx, WhatChangedInput{
				Scope: []string{w.n1.String(), w.n2.String(), w.p1.String(), w.p7.String()}, From: w10, To: w1030, AsOf: asof,
			})
		}},
		{"co_residents", func(s *Source, asof string) (any, error) {
			return s.CoResidents(ctx, CoResidentsInput{Pod: w.p1.String(), From: w10, To: w1030, AsOf: asof})
		}},
		{"neighbors_at", func(s *Source, asof string) (any, error) {
			return s.NeighborsAt(ctx, NeighborsAtInput{Entity: w.n1.String(), At: "2026-01-01T10:25:00Z", AsOf: asof})
		}},
		{"entity_history", func(s *Source, asof string) (any, error) {
			return s.EntityHistory(ctx, EntityHistoryInput{Entity: w.n1.String(), From: "2026-01-01T09:00:00Z", To: w1030, AsOf: asof})
		}},
		{"find_entities", func(s *Source, asof string) (any, error) {
			return s.FindEntities(ctx, FindEntitiesInput{Type: "k8s.node", Name: "worker", At: "2026-01-01T10:25:00Z", AsOf: asof})
		}},
		{"resolve_entity", func(s *Source, asof string) (any, error) {
			return s.ResolveEntity(ctx, ResolveEntityInput{
				Type: "host", Attributes: map[string]json.RawMessage{"host.id": json.RawMessage(`"` + hostSecret + `"`)}, At: w10, AsOf: asof,
			})
		}},
	}
}

// asked runs a call and returns its answer as JSON.
func asked(t *testing.T, c call, s *Source, asof string) []byte {
	t.Helper()
	v, err := c.ask(s, asof)
	if err != nil {
		t.Fatal(err)
	}
	b, err := json.Marshal(v)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

// Every read of a call has one snapshot token, the one the answer names, even
// when a writer commits while the call runs. A spy writes through the raw store
// right after the call's first read.
func TestOneTokenPerAnswer(t *testing.T) {
	for _, tc := range calls(newWorld(t, rankObjs)) {
		t.Run(tc.tool, func(t *testing.T) {
			w := newWorld(t, rankObjs)
			var c call
			for _, x := range calls(w) {
				if x.tool == tc.tool {
					c = x
				}
			}
			token := w.st.LastSeq()
			before := asked(t, c, w.source(t, rankObjs, true), "")
			var head struct{ Token string }
			if err := json.Unmarshal(before, &head); err != nil || head.Token != strconv.FormatUint(token, 10) {
				t.Fatalf("token %q (%v), want LastSeq %d", head.Token, err, token)
			}

			spy := &spyStore{Store: w.st}
			spy.afterFirst = w.lateWrites
			s, err := NewSource(Config{Store: spy, Rank: rankObjs, RangeReads: true})
			if err != nil {
				t.Fatal(err)
			}
			during := asked(t, c, s, "")
			if w.st.LastSeq() == token {
				t.Fatal("the late writes did not happen")
			}
			if spy.reads == 0 {
				t.Fatal("the call read nothing")
			}
			for i, got := range spy.asOf {
				if got != token {
					t.Fatalf("read %d of %d used the token %d, want %d", i+1, len(spy.asOf), got, token)
				}
			}
			if !bytes.Equal(before, during) {
				t.Errorf("the answer changed with a write during the call:\nbefore: %s\nduring: %s", before, during)
			}

			// Asking again with the same token gives byte-identical output.
			again := asked(t, c, s, strconv.FormatUint(token, 10))
			if !bytes.Equal(before, again) {
				t.Errorf("asof=%d gave another answer:\nfirst: %s\nagain: %s", token, before, again)
			}
			// And without it, the world has moved.
			if c.tool != "resolve_entity" {
				now := asked(t, c, s, "")
				if bytes.Equal(before, now) {
					t.Error("the late writes changed nothing in the answer; the test cannot tell a pinned token from a moving one")
				}
			}
		})
	}
}

// A token above LastSeq is clamped to it, and the answer says so. A token that
// is not a decimal number is refused.
func TestTokenAhead(t *testing.T) {
	w := newWorld(t, rankObjs)
	s := w.source(t, rankObjs, true)
	last := w.st.LastSeq()
	for _, ahead := range []string{strconv.FormatUint(last+100, 10), strconv.FormatUint(store.Latest, 10)} {
		for _, c := range calls(w) {
			b := asked(t, c, s, ahead)
			var out struct {
				Token string
				Notes []string
			}
			if err := json.Unmarshal(b, &out); err != nil {
				t.Fatal(err)
			}
			want := "Token " + ahead + " is ahead of the store; answered at " + strconv.FormatUint(last, 10) + "."
			if out.Token != strconv.FormatUint(last, 10) || !contains(out.Notes, want) {
				t.Errorf("%s asof=%s: token %q, notes %q", c.tool, ahead, out.Token, out.Notes)
			}
		}
	}
	for _, bad := range []string{"abc", "-1", "01", "1e3", " 5", "5 ", "+5", "1.5", "18446744073709551616"} {
		_, err := s.BlastRadius(ctx, BlastRadiusInput{Entity: w.h1.String(), From: w10, To: w1030, AsOf: bad})
		var te *Error
		if !asError(err, &te) || !strings.Contains(te.Message, `"asof"`) {
			t.Errorf("asof=%q: %v", bad, err)
		}
	}
}

// The token in the answer is honoured: an earlier token reads an earlier world.
func TestEarlierToken(t *testing.T) {
	w := newWorld(t, rankObjs)
	s := w.source(t, rankObjs, true)
	out, err := s.BlastRadius(ctx, BlastRadiusInput{Entity: w.h1.String(), From: w10, To: w1030, AsOf: "0"})
	if err != nil {
		t.Fatal(err)
	}
	if out.Token != "0" || len(out.Members) != 0 {
		t.Errorf("token %s, %d members at token 0", out.Token, len(out.Members))
	}
}
