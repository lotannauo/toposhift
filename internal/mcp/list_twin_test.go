package mcp

import (
	"fmt"
	"math/rand/v2"
	"slices"
	"testing"
	"time"

	"github.com/lotannauo/toposhift/internal/catalog"
	"github.com/lotannauo/toposhift/internal/identity"
	"github.com/lotannauo/toposhift/internal/query/interval"
	"github.com/lotannauo/toposhift/internal/store"
	"github.com/lotannauo/toposhift/internal/store/memstore"
)

// listWorld is a seeded world of entities of several types, with observations,
// deletes, TTLs of none and of 15 minutes, and ids that are the target of an edge
// only.
type listWorld struct {
	records []store.Record
	// own are the ids with a record of their own existence, by type.
	own map[catalog.EntityType][]identity.Fingerprint
	// last is the latest event time of a record.
	last time.Time
}

func newListWorld(tb testing.TB) *listWorld {
	tb.Helper()
	rng := rand.New(rand.NewPCG(1, 1))
	w := &listWorld{own: map[catalog.EntityType][]identity.Fingerprint{}}
	mk := func(typ catalog.EntityType, i int) identity.Fingerprint {
		switch typ {
		case catalog.K8sPod:
			return fpOf(tb, typ, catalog.K8sPodUID, fmt.Sprintf("pod-%d", i))
		case catalog.K8sNode:
			return fpOf(tb, typ, catalog.K8sNodeUID, fmt.Sprintf("node-%d", i))
		case catalog.Container:
			return fpOf(tb, typ, catalog.ContainerID, fmt.Sprintf("container-%d", i))
		case catalog.Service:
			return fpOf(tb, typ, catalog.ServiceName, fmt.Sprintf("service-%d", i))
		case catalog.Host:
			return fpOf(tb, typ, catalog.HostID, fmt.Sprintf("host-%d", i))
		}
		return fpOf(tb, typ, catalog.EndpointRealm, "global", catalog.EndpointAddress, fmt.Sprintf("10.0.%d.%d", i/200, i%200+1))
	}
	counts := []struct {
		typ catalog.EntityType
		n   int
	}{{catalog.K8sPod, 150}, {catalog.K8sNode, 30}, {catalog.Container, 100}, {catalog.Service, 60}, {catalog.Host, 20}, {catalog.NetworkEndpoint, 40}}
	instant := func() time.Time { return hm(9, 0).Add(time.Duration(rng.IntN(50*60)) * time.Second) }
	for _, c := range counts {
		for i := range c.n {
			fp := mk(c.typ, i)
			w.own[c.typ] = append(w.own[c.typ], fp)
			ttl := time.Duration(0)
			if rng.IntN(2) == 0 {
				ttl = 15 * time.Minute
			}
			at := instant()
			obs := observe(ent(fp), objs, at, ttl, fmt.Sprintf(`{"name":"%s-%d"}`, c.typ, i))
			if c.typ == catalog.Host {
				obs.Boot = fmt.Sprintf("boot-%d", i)
			}
			w.records = append(w.records, obs)
			switch rng.IntN(4) {
			case 0: // deleted afterwards
				d := deleted(ent(fp), objs, at.Add(time.Duration(1+rng.IntN(600))*time.Second))
				w.records = append(w.records, d)
			case 1: // seen again later
				again := observe(ent(fp), objs, at.Add(time.Duration(1+rng.IntN(600))*time.Second), ttl, string(obs.Payload))
				again.Boot = obs.Boot
				w.records = append(w.records, again)
			}
		}
	}
	// Ids that are the target of an edge only: pods of containers, nodes of pods,
	// hosts of nodes.
	for i := range 20 {
		pod := fpOf(tb, catalog.K8sPod, catalog.K8sPodUID, fmt.Sprintf("edge-only-pod-%d", i))
		w.records = append(w.records, observe(edge(w.own[catalog.Container][i], pod, catalog.PartOf), objs, instant(), 0, `{}`))
	}
	for i := range 5 {
		node := fpOf(tb, catalog.K8sNode, catalog.K8sNodeUID, fmt.Sprintf("edge-only-node-%d", i))
		w.records = append(w.records, observe(edge(w.own[catalog.K8sPod][i], node, catalog.ScheduledOn), objs, instant(), 0, `{}`))
	}
	for i := range 5 {
		host := fpOf(tb, catalog.Host, catalog.HostID, fmt.Sprintf("edge-only-host-%d", i))
		w.records = append(w.records, observe(edge(w.own[catalog.K8sNode][i], host, catalog.RunsOn), agent, instant(), 0, `{}`))
	}
	rng.Shuffle(len(w.records), func(i, j int) { w.records[i], w.records[j] = w.records[j], w.records[i] })
	for _, r := range w.records {
		if r.EventTime.After(w.last) {
			w.last = r.EventTime
		}
	}
	return w
}

// pagesOf lists the ids of type typ, in pages of n, on a store that has the listing read.
func pagesOf(t *testing.T, l Lister, typ catalog.EntityType, n int) []identity.Fingerprint {
	t.Helper()
	var all []identity.Fingerprint
	var after identity.Fingerprint
	for i := 0; ; i++ {
		if i > 2000 {
			t.Fatalf("ListEntities(%s) in pages of %d does not end", typ, n)
		}
		page, err := l.ListEntities(ctx, typ, after, n)
		if err != nil {
			t.Fatalf("ListEntities(%s, %v, %d): %v", typ, after, n, err)
		}
		if len(page) > n {
			t.Fatalf("a page of %d for n = %d", len(page), n)
		}
		if len(page) == 0 {
			return all
		}
		all = append(all, page...)
		after = page[len(page)-1]
	}
}

func sortedFPs(fps []identity.Fingerprint) []identity.Fingerprint {
	out := slices.Clone(fps)
	slices.SortFunc(out, store.CompareFingerprints)
	return out
}

// subset reports whether every id of a is in b, both ascending.
func subset(a, b []identity.Fingerprint) (missing identity.Fingerprint, ok bool) {
	for _, x := range a {
		if _, found := slices.BinarySearchFunc(b, x, store.CompareFingerprints); !found {
			return x, false
		}
	}
	return identity.Fingerprint{}, true
}

// The two stores that list, holding the same records, give the same ids in any
// paging: unretained, they are exactly the entities with a record of their own;
// retained, the pebblestore lists a subset of the memstore's, and both list every
// entity that exists after the horizon.
func TestListersAgree(t *testing.T) {
	w := newListWorld(t)
	types := []catalog.EntityType{}
	for e := range catalog.Default().Entities() {
		types = append(types, e.Type())
	}

	for _, v := range variants() {
		t.Run(v.name, func(t *testing.T) {
			mem := openMem(t, memstore.Options{Policy: servePolicy(nil)})
			peb := openPeb(t, servePolicy(nil), v.ck, v.mode)
			for _, st := range []store.Store{mem, peb} {
				newFeed(t, st).write(w.records...)
			}
			lm, lp := Lister(mem), Lister(peb)

			for _, typ := range types {
				want := sortedFPs(w.own[typ])
				for _, n := range []int{1, 7, 64, 1000} {
					gm, gp := pagesOf(t, lm, typ, n), pagesOf(t, lp, typ, n)
					if !slices.Equal(gm, want) || !slices.Equal(gp, want) {
						t.Errorf("%s in pages of %d: memstore lists %d ids, pebblestore %d, want %d", typ, n, len(gm), len(gp), len(want))
					}
				}
			}
			if len(w.own[catalog.K8sPod]) != 150 {
				t.Fatalf("%d pods", len(w.own[catalog.K8sPod]))
			}

			// Retained at a mid instant.
			h := hm(9, 30)
			for _, st := range []store.Store{mem, peb} {
				if err := st.Retain(ctx, h); err != nil {
					t.Fatal(err)
				}
			}
			settle(t, peb)
			rd := interval.NewReader(mem, interval.Options{})
			window := interval.Window{From: h, To: w.last.Add(time.Hour), AsOf: mem.LastSeq()}
			dropped := 0
			for _, typ := range types {
				for _, n := range []int{1, 7, 64, 1000} {
					gm, gp := pagesOf(t, lm, typ, n), pagesOf(t, lp, typ, n)
					if !slices.Equal(gm, sortedFPs(w.own[typ])) {
						t.Errorf("%s: memstore after the retention lists %d ids, want its %d (it discards nothing)", typ, len(gm), len(w.own[typ]))
					}
					if x, ok := subset(gp, gm); !ok {
						t.Errorf("%s: pebblestore lists %v, which memstore does not", typ, x)
					}
					if n == 1000 {
						dropped += len(gm) - len(gp)
					}
					var exist []identity.Fingerprint
					if len(gm) > 0 {
						_, els, err := rd.Entities(ctx, gm, window)
						if err != nil {
							t.Fatal(err)
						}
						for i, el := range els {
							if len(el.Pieces) > 0 || el.Quarantine != nil {
								exist = append(exist, gm[i])
							}
						}
					}
					if x, ok := subset(exist, gm); !ok {
						t.Errorf("%s: memstore lacks %v, which exists after the horizon", typ, x)
					}
					if x, ok := subset(exist, gp); !ok {
						t.Errorf("%s: pebblestore lacks %v, which exists after the horizon", typ, x)
					}
				}
			}
			if dropped == 0 {
				t.Error("the retention dropped nothing from the pebblestore's list: the fixture cannot tell the two apart")
			}
		})
	}
}
