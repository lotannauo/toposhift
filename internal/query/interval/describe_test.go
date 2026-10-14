package interval_test

import (
	"fmt"
	"slices"
	"strings"
	"testing"
	"time"

	"pgregory.net/rapid"

	"github.com/lotannauo/toposhift/internal/identity"
	"github.com/lotannauo/toposhift/internal/lifecycle"
	"github.com/lotannauo/toposhift/internal/query/interval"
	"github.com/lotannauo/toposhift/internal/store"
	"github.com/lotannauo/toposhift/internal/store/memstore"
)

// The description oracle. The winner at x is chosen by the rank rule written
// out here from its definition, not by calling lifecycle's; the live
// producers and each producer's assertion in force come from the range oracle
// (liveAt and latest). It shares no code with lifecycle or with the package.

// outranksDef reports whether producer a wins over b under rank: a listed
// producer beats an unlisted one, a higher rank beats a lower one, and the
// smaller name breaks every remaining tie.
func outranksDef(rank map[lifecycle.Producer]int, a, b lifecycle.Producer) bool {
	ra, listedA := rank[a]
	rb, listedB := rank[b]
	if listedA != listedB {
		return listedA
	}
	if listedA && ra != rb {
		return ra > rb
	}
	return a < b
}

// describedAt is the description of a subject's records at x: the payload of
// the winning live producer's assertion in force at x, or nil when no
// producer's reference is live there.
func describedAt(recs []store.Record, x time.Time, rank map[lifecycle.Producer]int) *interval.Description {
	ok, live := liveAt(recs, x)
	if !ok {
		return nil
	}
	win := live[0]
	for _, p := range live[1:] {
		if outranksDef(rank, p, win) {
			win = p
		}
	}
	d := &interval.Description{Producer: win}
	if r := latest(recs, x)[win]; len(r.Payload) > 0 {
		d.Payload = slices.Clone(r.Payload)
	}
	return d
}

func pieceContains(ps []interval.Piece, x time.Time) bool {
	for _, p := range ps {
		if !x.Before(p.Start) && x.Before(p.End) {
			return true
		}
	}
	return false
}

// withDescriptions is el with the descriptions it must carry: at from and at
// to-1ns, each when one of its pieces contains the instant, from the
// subject's own records; none for a quarantined entity.
func (o *oracle) withDescriptions(el interval.Element, from, to time.Time, rank map[lifecycle.Producer]int) interval.Element {
	if el.Quarantine != nil {
		return el
	}
	own := o.bySub[el.Subject]
	if pieceContains(el.Pieces, from) {
		el.AtFrom = describedAt(own, from, rank)
	}
	if last := to.Add(-time.Nanosecond); pieceContains(el.Pieces, last) {
		el.AtLast = describedAt(own, last, rank)
	}
	return el
}

func fmtDescription(d *interval.Description) string {
	if d == nil {
		return "nil"
	}
	s := fmt.Sprintf("%s %q", d.Producer, d.Payload)
	if d.Payload != nil && len(d.Payload) == 0 {
		s += " (empty, not nil)"
	}
	return s
}

// describeWithDescriptions is describe with the two descriptions.
func describeWithDescriptions(el interval.Element) string {
	return fmt.Sprintf("%s from=%s last=%s", describe(el), fmtDescription(el.AtFrom), fmtDescription(el.AtLast))
}

// describedDiff is diff, comparing the descriptions too.
func describedDiff(got, want []interval.Element) string {
	g, w := make([]string, len(got)), make([]string, len(want))
	for i, el := range got {
		g[i] = describeWithDescriptions(el)
	}
	for i, el := range want {
		w[i] = describeWithDescriptions(el)
	}
	if slices.Equal(g, w) {
		return ""
	}
	return fmt.Sprintf("got:\n  %s\nwant:\n  %s", strings.Join(g, "\n  "), strings.Join(w, "\n  "))
}

// readAll reads, with opts and raw and with the cascade, every entity alone and
// all of them with Entities, and the edges of every entity in both directions
// alone and all of them with EdgesBatch, at one window. It requires every
// answer to name the same token.
func readAll(t tb, st store.Store, entities []identity.Fingerprint, w interval.Window, opts interval.Options) (uint64, map[string][]interval.Element) {
	t.Helper()
	out := map[string][]interval.Element{}
	var token uint64
	seen := false
	note := func(tok uint64) {
		if seen && tok != token {
			t.Fatalf("answers at tokens %d and %d", token, tok)
		}
		token, seen = tok, true
	}
	for _, raw := range []bool{false, true} {
		o := opts
		o.Raw = raw
		r := interval.NewReader(st, o)
		if !raw {
			for _, e := range entities {
				res, err := r.Entity(ctx, e, w)
				if err != nil {
					t.Fatal(err)
				}
				note(res.Token)
				out["entity "+e.String()] = res.Elements
			}
			tok, els, err := r.Entities(ctx, entities, w)
			if err != nil {
				t.Fatal(err)
			}
			note(tok)
			out["entities"] = els
		}
		for _, d := range dirs {
			for _, e := range entities {
				res, err := r.Edges(ctx, e, d, w)
				if err != nil {
					t.Fatal(err)
				}
				note(res.Token)
				out[fmt.Sprintf("edges %s %s raw=%t", e, d, raw)] = res.Elements
			}
			tok, batch, err := r.EdgesBatch(ctx, entities, d, w)
			if err != nil {
				t.Fatal(err)
			}
			note(tok)
			for i, els := range batch {
				out[fmt.Sprintf("batch %d %s raw=%t", i, d, raw)] = els
			}
		}
	}
	return token, out
}

// oracleAll is what readAll must answer with Describe on, from the oracle.
func oracleAll(o *oracle, entities []identity.Fingerprint, from, to time.Time, rank map[lifecycle.Producer]int) map[string][]interval.Element {
	want := map[string][]interval.Element{}
	var all []interval.Element
	for _, e := range entities {
		el := o.withDescriptions(o.entity(e, from, to), from, to, rank)
		want["entity "+e.String()] = []interval.Element{el}
		all = append(all, el)
	}
	want["entities"] = all
	for _, raw := range []bool{false, true} {
		for _, d := range dirs {
			for i, e := range entities {
				var els []interval.Element
				for _, el := range o.edges(e, d, from, to, raw) {
					els = append(els, o.withDescriptions(el, from, to, rank))
				}
				want[fmt.Sprintf("edges %s %s raw=%t", e, d, raw)] = els
				want[fmt.Sprintf("batch %d %s raw=%t", i, d, raw)] = els
			}
		}
	}
	return want
}

// checkDescriptions is P-D2: with Describe on, every answer, its
// descriptions included, equals the oracle's.
func checkDescriptions(t tb, st store.Store, recs []store.Record, entities []identity.Fingerprint, w interval.Window, rank map[lifecycle.Producer]int) {
	t.Helper()
	tok := clampToken(w.AsOf, st.LastSeq())
	o := newOracle(recs, tok)
	quarantines(t, st, o, entities, tok)
	got, ans := readAll(t, st, entities, w, interval.Options{Describe: true, Rank: rank})
	if got != tok {
		t.Fatalf("token %d, want %d", got, tok)
	}
	want := oracleAll(o, entities, w.From.UTC(), w.To.UTC(), rank)
	for k, wl := range want {
		if d := describedDiff(ans[k], wl); d != "" {
			t.Fatalf("rank %v: %s:\n%s", rank, k, d)
		}
	}
	if len(ans) != len(want) {
		t.Fatalf("%d answers, want %d", len(ans), len(want))
	}
}

// checkDescribeChangesNoExistence is P-D1: with Describe on, under rank,
// every answer's pieces, classes, transitions and quarantine marks equal
// those with Describe off, the reads are the same reads in the same order, and
// with Describe off no element carries a description.
func checkDescribeChangesNoExistence(t tb, st store.Store, entities []identity.Fingerprint, w interval.Window, rank map[lifecycle.Producer]int) {
	t.Helper()
	off, on := &spy{Store: st}, &spy{Store: st}
	_, plain := readAll(t, off, entities, w, interval.Options{Rank: rank})
	_, described := readAll(t, on, entities, w, interval.Options{Describe: true, Rank: rank})
	sameAnswers(t, "with Describe on and off", described, plain)
	if !slices.Equal(on.log, off.log) {
		t.Fatalf("reads with Describe on %q, off %q", on.log, off.log)
	}
	for k, els := range plain {
		for _, el := range els {
			if el.AtFrom != nil || el.AtLast != nil {
				t.Fatalf("%s: %s carries a description with Describe off", k, describeWithDescriptions(el))
			}
		}
	}
}

// checkEntities is P-D3: Entities equals one Entity per fingerprint, at one
// token, and makes exactly one existence read (one Alive and, unless the
// entity is quarantined, one EntityWindow) per distinct fingerprint, and no
// other read.
func checkEntities(t tb, st store.Store, fps []identity.Fingerprint, w interval.Window, opts interval.Options) {
	t.Helper()
	sp := &spy{Store: st}
	tok, out, err := interval.NewReader(sp, opts).Entities(ctx, fps, w)
	if err != nil {
		t.Fatal(err)
	}
	if want := clampToken(w.AsOf, st.LastSeq()); tok != want {
		t.Fatalf("Entities token %d, want %d", tok, want)
	}
	if len(out) != len(fps) {
		t.Fatalf("Entities answered %d of %d fingerprints", len(out), len(fps))
	}
	r := interval.NewReader(st, opts)
	distinct := map[identity.Fingerprint]bool{}
	quarantined := 0
	for i, fp := range fps {
		w1 := w
		w1.AsOf = tok
		res, err := r.Entity(ctx, fp, w1)
		if err != nil {
			t.Fatal(err)
		}
		if d := describedDiff(out[i:i+1], res.Elements); d != "" {
			t.Fatalf("Entities[%d] against Entity(%s):\n%s", i, fp, d)
		}
		if !distinct[fp] {
			distinct[fp] = true
			if out[i].Quarantine != nil {
				quarantined++
			}
		}
	}
	var alive, windows int
	for _, l := range sp.log {
		switch {
		case strings.HasPrefix(l, "Alive "):
			alive++
		case strings.HasPrefix(l, "EntityWindow "):
			windows++
		}
	}
	if alive != len(distinct) || windows != len(distinct)-quarantined || len(sp.log) != alive+windows {
		t.Fatalf("Entities of %d fingerprints, %d distinct and %d of them quarantined, read %q", len(fps), len(distinct), quarantined, sp.log)
	}
}

// descPayloads are the payloads the described grid draws from: empty, three
// JSON objects that differ in one key or in every key, and two that are not
// JSON. The package treats every payload as opaque bytes.
var descPayloads = [][]byte{
	nil,
	[]byte(`{"name":"a","phase":"Pending"}`),
	[]byte(`{"name":"a","phase":"Running"}`),
	[]byte(`{"name":"b","phase":"Running"}`),
	[]byte("x"),
	[]byte("y"),
}

// drawRank is nil, empty, or a rank of some of the grid's producers, with
// ties and negative ranks.
func drawRank(t *rapid.T) map[lifecycle.Producer]int {
	switch rapid.IntRange(0, 3).Draw(t, "rank kind") {
	case 0:
		return nil
	case 1:
		return map[lifecycle.Producer]int{}
	}
	rank := map[lifecycle.Producer]int{}
	for _, p := range gridProducers {
		if rapid.Bool().Draw(t, "listed "+string(p)) {
			rank[p] = rapid.IntRange(-1, 1).Draw(t, "rank "+string(p))
		}
	}
	return rank
}

// drawDescribedCase is drawCase with each Observe's payload drawn from
// descPayloads, and a rank.
func drawDescribedCase(t *rapid.T) (gridCase, map[lifecycle.Producer]int) {
	g := newGrid()
	recs := drawRecords(t, g, 60)
	for i := range recs {
		if recs[i].Kind == lifecycle.Observe {
			recs[i].Payload = rapid.SampledFrom(descPayloads).Draw(t, "payload")
		}
	}
	st := freshStore(t, memstore.Options{}, nil)
	recs = writeShuffled(t, st, recs)
	from, to := drawWindow(t)
	w := interval.Window{From: from, To: to, AsOf: drawToken(t, st.LastSeq())}
	return gridCase{g: g, st: st, recs: recs, w: w}, drawRank(t)
}

func TestPD1DescribeChangesNoExistence(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		c, rank := drawDescribedCase(t)
		checkDescribeChangesNoExistence(t, c.st, c.g.entities, c.w, rank)
	})
}

func TestPD2DescriptionsEqualTheOracle(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		c, rank := drawDescribedCase(t)
		checkDescriptions(t, c.st, c.recs, c.g.entities, c.w, rank)
	})
}

func TestPD3EntitiesIsEntityPerFingerprint(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		c, rank := drawDescribedCase(t)
		fps := rapid.SliceOfN(rapid.SampledFrom(c.g.entities), 0, 8).Draw(t, "fps")
		checkEntities(t, c.st, fps, c.w, interval.Options{Describe: rapid.Bool().Draw(t, "describe"), Rank: rank})
	})
}

// TestStreamsDescriptions runs P-D1, P-D2 and P-D3 on the generated cluster
// streams, the quarantining one included, at their fixed windows, with no
// rank and with a rank that reverses the order of the producers' names.
func TestStreamsDescriptions(t *testing.T) {
	for _, s := range streams() {
		t.Run(s.name, func(t *testing.T) {
			t.Parallel()
			st, recs, entities, start, end := openStream(t, s)
			var producers []lifecycle.Producer
			for _, r := range recs {
				if !slices.Contains(producers, r.Producer) {
					producers = append(producers, r.Producer)
				}
			}
			slices.Sort(producers)
			reversed := map[lifecycle.Producer]int{}
			for i, p := range producers {
				reversed[p] = i
			}
			fps := append(slices.Clone(entities), entities...)
			for _, fw := range windows(start, end) {
				w := interval.Window{From: fw[0], To: fw[1], AsOf: store.Latest}
				for _, rank := range []map[lifecycle.Producer]int{nil, reversed} {
					checkDescriptions(t, st, recs, entities, w, rank)
					checkDescribeChangesNoExistence(t, st, entities, w, rank)
					checkEntities(t, st, fps, w, interval.Options{Describe: true, Rank: rank})
				}
			}
		})
	}
}
