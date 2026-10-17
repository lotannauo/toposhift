package demo

import (
	"bytes"
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/cockroachdb/pebble/v2/vfs"

	"github.com/lotannauo/toposhift/internal/mcp"
	"github.com/lotannauo/toposhift/internal/store"
	"github.com/lotannauo/toposhift/internal/store/pebblekv"
	"github.com/lotannauo/toposhift/internal/store/pebblestore"
)

// openTwin opens an empty pebblestore in memory, with the tiny tuning, under the
// policy of the build and with its default checkpoints. Retention is not run, and
// nothing is written to disk. It is returned as a store, so that the test asserts
// what it is.
func openTwin(tb testing.TB) store.Store {
	tb.Helper()
	o := pebblestore.DefaultOptions()
	o.Tuning = pebblekv.TinyTuning()
	o.Sync, o.SettleRetention = false, false
	o.FS = vfs.NewMem()
	o.Policy = testPolicy()
	st, err := pebblestore.Open("twin", o)
	if err != nil {
		tb.Fatal(err)
	}
	tb.Cleanup(func() { _ = st.Close() })
	return st
}

// source is the tools over the store, with the ranks the build was made under.
func source(tb testing.TB, st store.Store) *mcp.Source {
	tb.Helper()
	src, err := mcp.NewSource(mcp.Config{Store: st, Rank: testPolicy().Rank, RangeReads: mcp.RangeReadsFor(st)})
	if err != nil {
		tb.Fatal(err)
	}
	tb.Cleanup(func() { _ = src.Drain(context.Background()) })
	return src
}

// The records a build commits, copied as they are committed into a pebblestore in
// memory, answer the questions of the end-to-end test with the same bytes as the
// memstore the build wrote them to: the same records in the same order give the
// same answers, whichever engine holds them, and the services' as well as the
// cluster's. The pebblestore here is not retained; retained twins are the store
// package's.
func TestAPebblestoreHoldingTheSameRecordsAnswersTheSame(t *testing.T) {
	twin := openTwin(t)
	b := build(t, DefaultSeed, withTee(Options{}, twin))
	if b.LastSeq == 0 || twin.LastSeq() != b.LastSeq {
		t.Fatalf("the build's store is at %d and the twin at %d", b.LastSeq, twin.LastSeq())
	}
	// The twin is a pebblestore and the build's store is a memstore, so the
	// comparison is between two engines.
	if _, ok := twin.(*pebblestore.Store); !ok {
		t.Fatalf("the twin is a %T, want a pebblestore", twin)
	}
	var built store.Store = b.Store
	if built == twin {
		t.Fatal("the build's store and the twin are one store")
	}

	w := NewWorld(DefaultSeed)
	s := w.Story()
	var ip string
	for _, p := range w.Pods {
		if p.UID == s.Checkout.UID {
			ip = p.IP
		}
	}
	if ip == "" {
		t.Fatal("the checkout pod has no address")
	}
	i1 := s.NotReady.Add(-15 * time.Minute)
	w1a, w1b := s.NotReady.Add(-5*time.Minute), s.NotReady.Add(30*time.Minute)
	at, rfc := i1.Format(time.RFC3339), func(t time.Time) string { return t.Format(time.RFC3339) }
	var scope []string
	for i, v := range s.Evicted {
		scope = append(scope, v.ID, s.Replacements[i].ID)
	}
	attrs := map[string]json.RawMessage{
		"topo.endpoint.realm":   json.RawMessage(`"k8s/` + ClusterName + `"`),
		"topo.endpoint.address": json.RawMessage(`"` + ip + `"`),
	}

	type ask func(src *mcp.Source) (any, error)
	var endpointID string
	questions := []struct {
		name string
		ask  ask
	}{
		{"find", func(src *mcp.Source) (any, error) {
			return src.FindEntities(t.Context(), mcp.FindEntitiesInput{Type: "k8s.pod", Name: "checkout", At: at})
		}},
		{"neighbors of the node", func(src *mcp.Source) (any, error) {
			return src.NeighborsAt(t.Context(), mcp.NeighborsAtInput{Entity: s.Failed.ID, At: at})
		}},
		{"blast radius", func(src *mcp.Source) (any, error) {
			return src.BlastRadius(t.Context(), mcp.BlastRadiusInput{Entity: s.Failed.ID, From: rfc(w1a), To: rfc(w1b)})
		}},
		{"co-residents", func(src *mcp.Source) (any, error) {
			return src.CoResidents(t.Context(), mcp.CoResidentsInput{Pod: s.Checkout.ID, From: rfc(s.Anchor), To: rfc(w1a)})
		}},
		{"what changed", func(src *mcp.Source) (any, error) {
			return src.WhatChanged(t.Context(), mcp.WhatChangedInput{Scope: scope, From: rfc(s.Evict.Add(-10 * time.Minute)), To: rfc(s.Evict.Add(10 * time.Minute))})
		}},
		{"neighbors of the pod", func(src *mcp.Source) (any, error) {
			return src.NeighborsAt(t.Context(), mcp.NeighborsAtInput{Entity: s.Checkout.ID, At: at})
		}},
		{"the address", func(src *mcp.Source) (any, error) {
			out, err := src.ResolveEntity(t.Context(), mcp.ResolveEntityInput{Type: "topo.endpoint", Attributes: attrs, At: at})
			endpointID = out.Entity.ID
			return out, err
		}},
		{"neighbors of the address", func(src *mcp.Source) (any, error) {
			return src.NeighborsAt(t.Context(), mcp.NeighborsAtInput{Entity: endpointID, At: at})
		}},
		// The questions about the services: a name, the blast radius of a service while
		// it is called by one caller and of the service that is the last of the chain,
		// the change of checkout while it moves from one callee to the other, and its
		// callers, callees and the services that shared a place with them.
		{"find services", func(src *mcp.Source) (any, error) {
			return src.FindEntities(t.Context(), mcp.FindEntitiesInput{Type: "service", Name: "pay", At: rfc(s.Anchor.Add(time.Hour))})
		}},
		{"blast radius of ledger", func(src *mcp.Source) (any, error) {
			return src.BlastRadius(t.Context(), mcp.BlastRadiusInput{Entity: s.Service("payments", "ledger").ID, From: rfc(s.FaultFrom), To: rfc(s.FaultTo)})
		}},
		{"blast radius of legacy-pay", func(src *mcp.Source) (any, error) {
			return src.BlastRadius(t.Context(), mcp.BlastRadiusInput{Entity: s.Service("payments", "legacy-pay").ID, From: rfc(s.Anchor.Add(time.Hour)), To: rfc(s.Anchor.Add(time.Hour + 30*time.Minute))})
		}},
		{"what changed for checkout", func(src *mcp.Source) (any, error) {
			return src.WhatChanged(t.Context(), mcp.WhatChangedInput{Scope: []string{s.Service("shop", "checkout").ID}, From: rfc(s.Migrate.Add(-5 * time.Minute)), To: rfc(s.Migrate.Add(35 * time.Minute))})
		}},
		{"service context of checkout", func(src *mcp.Source) (any, error) {
			return src.ServiceContext(t.Context(), mcp.ServiceContextInput{Service: s.Service("shop", "checkout").ID, From: rfc(s.Migrate.Add(-5 * time.Minute)), To: rfc(s.Migrate.Add(35 * time.Minute)), Limit: 500})
		}},
	}
	mem, peb := source(t, b.Store), source(t, twin)
	for _, q := range questions {
		want, err := q.ask(mem)
		if err != nil {
			t.Fatalf("%s on the memstore: %v", q.name, err)
		}
		got, err := q.ask(peb)
		if err != nil {
			t.Fatalf("%s on the pebblestore: %v", q.name, err)
		}
		a, _ := json.Marshal(want)
		c, _ := json.Marshal(got)
		if !bytes.Equal(a, c) {
			t.Errorf("%s: the pebblestore answers\n%s\nthe memstore\n%s", q.name, c, a)
		}
		// An answer that is empty on both would agree without meaning anything.
		if len(a) < 400 {
			t.Errorf("%s: the answer is only %d bytes: %s", q.name, len(a), a)
		}
	}
	if endpointID == "" {
		t.Error("the address did not resolve")
	}
}
