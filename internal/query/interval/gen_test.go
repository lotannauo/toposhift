package interval_test

import (
	"context"
	"slices"
	"time"

	"pgregory.net/rapid"

	"github.com/lotannauo/toposhift/internal/catalog"
	"github.com/lotannauo/toposhift/internal/identity"
	"github.com/lotannauo/toposhift/internal/lifecycle"
	"github.com/lotannauo/toposhift/internal/store"
	"github.com/lotannauo/toposhift/internal/store/memstore"
)

// The grid generator: two pods and two nodes (L2) and one host (L1),
// scheduled_on edges stored in L2 and runs_on edges stored in L1, three
// producers, and records on a one-second grid over about 40 s, about one in
// five of them one nanosecond off it.

var gridBase = time.Date(2026, 1, 1, 10, 0, 0, 0, time.UTC)

var gridProducers = []lifecycle.Producer{"p1", "p2", "p3"}

type grid struct {
	entities []identity.Fingerprint
	subjects []store.Subject
}

func gridFP(typ catalog.EntityType, n byte) identity.Fingerprint {
	var h [identity.FingerprintBytes]byte
	h[0] = n
	fp, err := identity.FingerprintFromHash(typ, h)
	if err != nil {
		panic(err)
	}
	return fp
}

func newGrid() grid {
	pods := []identity.Fingerprint{gridFP(catalog.K8sPod, 1), gridFP(catalog.K8sPod, 2)}
	nodes := []identity.Fingerprint{gridFP(catalog.K8sNode, 1), gridFP(catalog.K8sNode, 2)}
	host := gridFP(catalog.Host, 1)
	g := grid{entities: append(append(slices.Clone(pods), nodes...), host)}
	for _, e := range g.entities {
		g.subjects = append(g.subjects, store.EntitySubject(e))
	}
	for _, p := range pods {
		for _, n := range nodes {
			g.subjects = append(g.subjects, store.EdgeSubject(p, n, catalog.ScheduledOn))
		}
	}
	for _, n := range nodes {
		g.subjects = append(g.subjects, store.EdgeSubject(n, host, catalog.RunsOn))
	}
	return g
}

// uniform draws an integer in [lo, hi] with every value about as likely:
// rapid.IntRange favors small values, which would crowd the records at the
// start of the grid.
func uniform(t *rapid.T, label string, lo, hi int) int {
	vs := make([]int, 0, hi-lo+1)
	for v := lo; v <= hi; v++ {
		vs = append(vs, v)
	}
	return rapid.SampledFrom(vs).Draw(t, label)
}

// drawInstant is a whole second in [lo, hi] past gridBase, shifted by -1, 0 or
// +1 ns when uniformShift is set, or about one time in five otherwise.
func drawInstant(t *rapid.T, label string, lo, hi int, uniformShift bool) time.Time {
	at := gridBase.Add(time.Duration(uniform(t, label, lo, hi)) * time.Second)
	var s int
	if uniformShift {
		s = rapid.IntRange(-1, 1).Draw(t, label+" shift")
	} else {
		switch rapid.IntRange(0, 9).Draw(t, label+" shift") {
		case 0:
			s = -1
		case 1:
			s = 1
		}
	}
	return at.Add(time.Duration(s))
}

// drawRecord is one record with no Seq, in its catalog layer.
func drawRecord(t *rapid.T, subs []store.Subject) store.Record {
	sub := rapid.SampledFrom(subs).Draw(t, "subject")
	p := rapid.SampledFrom(gridProducers).Draw(t, "producer")
	at := drawInstant(t, "at", 0, 40, false)
	if uniform(t, "delete", 0, 3) == 0 {
		return deleted(sub, p, at)
	}
	ttl := time.Duration(uniform(t, "ttl", 1, 8)) * time.Second
	if rapid.Bool().Draw(t, "watch") {
		ttl = 0
	}
	r := observe(sub, p, at, ttl)
	if rapid.IntRange(0, 3).Draw(t, "run") == 0 {
		r.Through = at.Truncate(time.Second).Add(time.Duration(uniform(t, "through", 1, 6)) * time.Second)
	}
	return r
}

func drawRecords(t *rapid.T, g grid, max int) []store.Record {
	n := rapid.IntRange(0, max).Draw(t, "records")
	if rapid.Bool().Draw(t, "dense") {
		n = max - n/4
	}
	out := make([]store.Record, n)
	for i := range out {
		out[i] = drawRecord(t, g.subjects)
	}
	return out
}

// writeShuffled writes the records in a random order and random batches,
// numbering them in write order after the store's LastSeq, and returns them with
// their Seq.
func writeShuffled(t *rapid.T, st store.Store, recs []store.Record) []store.Record {
	recs = rapid.Permutation(recs).Draw(t, "order")
	return writeInOrder(t, st, recs, func(left int) int { return rapid.IntRange(1, left).Draw(t, "batch") })
}

func writeInOrder(t interface{ Fatal(...any) }, st store.Store, recs []store.Record, batch func(left int) int) []store.Record {
	recs = slices.Clone(recs)
	seq := st.LastSeq()
	for i := range recs {
		seq++
		recs[i].Seq = seq
	}
	for i := 0; i < len(recs); {
		k := batch(len(recs) - i)
		if err := st.Write(context.Background(), recs[i:i+k]); err != nil {
			t.Fatal(err)
		}
		i += k
	}
	return recs
}

// freshStore is a memstore holding exactly recs, with their own Seq.
func freshStore(t interface{ Fatal(...any) }, opts memstore.Options, recs []store.Record) *memstore.Store {
	st, err := memstore.Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	for _, r := range recs {
		if err := st.Write(context.Background(), []store.Record{r}); err != nil {
			t.Fatal(err)
		}
	}
	return st
}

// drawWindow is [From, To) on the grid, each end shifted by -1, 0 or +1 ns, of
// a width drawn from a spread of lengths.
func drawWindow(t *rapid.T) (time.Time, time.Time) {
	a := uniform(t, "from", -3, 42)
	b := min(44, a+rapid.SampledFrom([]int{1, 2, 5, 10, 20, 30, 47}).Draw(t, "width"))
	from := gridBase.Add(time.Duration(a)*time.Second + time.Duration(rapid.IntRange(-1, 1).Draw(t, "from shift")))
	to := gridBase.Add(time.Duration(b)*time.Second + time.Duration(rapid.IntRange(-1, 1).Draw(t, "to shift")))
	return from, to
}

// drawToken is a token in {0, ..., last + 1} or Latest.
func drawToken(t *rapid.T, last uint64) uint64 {
	k := uniform(t, "token", 0, int(last)+2)
	if k == int(last)+2 {
		return store.Latest
	}
	return uint64(k)
}

func clampToken(tok, last uint64) uint64 {
	if tok == store.Latest || tok > last {
		return last
	}
	return tok
}
