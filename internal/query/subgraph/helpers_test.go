package subgraph_test

import (
	"context"
	"slices"
	"sort"
	"testing"
	"time"

	"github.com/lotannauo/toposhift/internal/catalog"
	"github.com/lotannauo/toposhift/internal/identity"
	"github.com/lotannauo/toposhift/internal/lifecycle"
	"github.com/lotannauo/toposhift/internal/query/subgraph"
)

var ctx = context.Background()

// base is the window's From in the builder tests.
var base = time.Date(2026, 1, 1, 10, 0, 0, 0, time.UTC)

// tb is what the checks need of a *testing.T or a *rapid.T.
type tb interface {
	Helper()
	Fatal(args ...any)
	Fatalf(format string, args ...any)
}

// mkfp is an entity of type typ whose hash starts with the two bytes of n, so
// tests fix the order of subjects and can name thousands of them.
func mkfp(typ catalog.EntityType, n int) identity.Fingerprint {
	var h [identity.FingerprintBytes]byte
	h[0], h[1] = byte(n>>8), byte(n)
	fp, err := identity.FingerprintFromHash(typ, h)
	if err != nil {
		panic(err)
	}
	return fp
}

func pod(n int) identity.Fingerprint  { return mkfp(catalog.K8sPod, n) }
func node(n int) identity.Fingerprint { return mkfp(catalog.K8sNode, n) }
func host(n int) identity.Fingerprint { return mkfp(catalog.Host, n) }

func ent(fp identity.Fingerprint) subgraph.Key { return subgraph.EntityKey(fp) }

func sched(p, n identity.Fingerprint) subgraph.Key {
	return subgraph.EdgeKey(p, n, catalog.ScheduledOn)
}

func runsOn(n, h identity.Fingerprint) subgraph.Key { return subgraph.EdgeKey(n, h, catalog.RunsOn) }

func derived(rule, detail string) subgraph.Key {
	return subgraph.Key{Kind: subgraph.KindDerived, Rule: rule, Detail: detail}
}

// at is the instant sec seconds after base.
func at(sec int) time.Time { return base.Add(time.Duration(sec) * time.Second) }

// span is one piece in seconds after base; an end of -1 is the window's To.
type span struct {
	start, end int
	reason     subgraph.EndReason
	before     bool
	producers  []lifecycle.Producer
}

// sp is a piece ending for a reason a producer authors; an end of -1 reaches To.
func sp(start, end int) span { return span{start: start, end: end, reason: subgraph.EndReasonProducer} }

// mk builds the source of a key from pieces over a window of width seconds,
// with the class and the transitions computed here, apart from the builder.
func mk(width int, key subgraph.Key, spans ...span) subgraph.Source {
	s := subgraph.Source{Key: key}
	for _, p := range spans {
		end, reason := p.end, p.reason
		if end < 0 || end == width {
			end, reason = width, subgraph.EndReasonUnknown
		}
		s.Pieces = append(s.Pieces, subgraph.SourcePiece{
			Start: at(p.start), End: at(end), Before: p.before, EndReason: reason, Producers: p.producers,
		})
	}
	s.Class, s.Transitions = classOf(s.Pieces, at(0), at(width))
	return s
}

// classOf is the class table and the transition count, written out again from
// the specification.
func classOf(ps []subgraph.SourcePiece, from, to time.Time) (subgraph.Class, int) {
	n := 0
	for _, p := range ps {
		if p.Start.After(from) {
			n++
		}
		if p.End.Before(to) {
			n++
		}
	}
	if len(ps) == 0 {
		return subgraph.ClassNone, 0
	}
	if len(ps) > 1 {
		return subgraph.ClassFlapping, n
	}
	a, b := ps[0].Start.Equal(from), ps[0].End.Equal(to)
	switch {
	case a && b:
		return subgraph.ClassStable, n
	case b:
		return subgraph.ClassAppeared, n
	case a:
		return subgraph.ClassDisappeared, n
	}
	return subgraph.ClassTransient, n
}

func input(width int, srcs ...subgraph.Source) subgraph.Input {
	return subgraph.Input{Token: 7, From: at(0), To: at(width), Sources: srcs}
}

func build(t tb, in subgraph.Input) subgraph.Subgraph {
	t.Helper()
	sg, err := subgraph.Build(in)
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	return sg
}

// plain reports whether a source is in the keyframes and deltas: it has no
// quarantine mark and a piece.
func plain(s subgraph.Source) bool {
	return s.Quarantine == nil && s.Uncascaded == nil && len(s.Pieces) > 0
}

// truth is the ids of the elements present at the relative instant rel, read
// from the input's own pieces, the specification's definition of S(t).
func truth(t tb, in subgraph.Input, ids map[subgraph.Key]uint32, rel uint64) []uint32 {
	x := in.From.Add(time.Duration(rel))
	var out []uint32
	for _, s := range in.Sources {
		if !plain(s) {
			continue
		}
		for _, p := range s.Pieces {
			if !x.Before(p.Start) && x.Before(p.End) {
				id, ok := ids[s.Key]
				if !ok {
					t.Fatalf("%s has a piece and no element", s.Key)
				}
				out = append(out, id)
				break
			}
		}
	}
	slices.Sort(out)
	return out
}

// forward is the set at rel, from the last keyframe at or before it and the
// deltas after that keyframe up to rel. It checks that every delta's before
// state is the state it finds.
func forward(t tb, sg subgraph.Subgraph, rel uint64) []uint32 {
	t.Helper()
	k := -1
	for i, kf := range sg.Keyframes {
		if kf.At <= rel {
			k = i
		}
	}
	if k < 0 {
		t.Fatalf("no keyframe at or before %d", rel)
	}
	set := slices.Clone(sg.Keyframes[k].Elements)
	i := firstAfter(sg.Deltas, sg.Keyframes[k].At)
	for ; i < len(sg.Deltas) && sg.Deltas[i].At <= rel; i++ {
		set = apply(t, set, sg.Deltas[i], sg.Deltas[i].Before, sg.Deltas[i].After)
	}
	return set
}

// backward is the set at rel, from the first keyframe after it and the deltas
// between, inverted. ok is false when no keyframe follows rel.
func backward(t tb, sg subgraph.Subgraph, rel uint64) ([]uint32, bool) {
	t.Helper()
	k := -1
	for i := len(sg.Keyframes) - 1; i >= 0; i-- {
		if sg.Keyframes[i].At > rel {
			k = i
		}
	}
	if k < 0 {
		return nil, false
	}
	set := slices.Clone(sg.Keyframes[k].Elements)
	for i := firstAfter(sg.Deltas, sg.Keyframes[k].At) - 1; i >= 0 && sg.Deltas[i].At > rel; i-- {
		set = apply(t, set, sg.Deltas[i], sg.Deltas[i].After, sg.Deltas[i].Before)
	}
	return set, true
}

// firstAfter is the index of the first delta strictly after the time.
func firstAfter(ds []subgraph.Delta, at uint64) int {
	return sort.Search(len(ds), func(i int) bool { return ds[i].At > at })
}

// apply moves one delta's element from the state it must be in to the state it
// goes to, and fails if the element is not in the state the delta says.
func apply(t tb, set []uint32, d subgraph.Delta, from, to subgraph.State) []uint32 {
	i, found := slices.BinarySearch(set, d.Element)
	if found != from.Present {
		t.Fatalf("the delta %+v finds element %d present=%t, want %t", d, d.Element, found, from.Present)
	}
	switch {
	case to.Present && !found:
		return slices.Insert(set, i, d.Element)
	case !to.Present && found:
		return slices.Delete(set, i, i+1)
	}
	return set
}

// truthStates is the state of every element at rel, from the input's own
// pieces: present if a piece contains the instant, and otherwise absent for the
// reason of the last piece that ended at or before it, or not seen in the window
// if none had.
func truthStates(in subgraph.Input, sg subgraph.Subgraph, rel uint64) []subgraph.State {
	x := in.From.Add(time.Duration(rel))
	byKey := map[subgraph.Key]subgraph.Source{}
	for _, s := range in.Sources {
		byKey[s.Key] = s
	}
	out := make([]subgraph.State, len(sg.Elements))
	for i, el := range sg.Elements {
		out[i] = subgraph.State{Reason: subgraph.EndReasonNotSeenInWindow}
		for _, p := range byKey[el.Key].Pieces {
			switch {
			case !x.Before(p.Start) && x.Before(p.End):
				out[i] = subgraph.State{Present: true}
			case !p.End.After(x) && !out[i].Present:
				out[i] = subgraph.State{Reason: p.EndReason}
			}
		}
	}
	return out
}

// keyframeStates is the states at a keyframe: its elements are present, and the
// reason another is absent comes from the interval lists, the last piece that
// ended at or before the keyframe.
func keyframeStates(sg subgraph.Subgraph, kf subgraph.Keyframe) []subgraph.State {
	out := make([]subgraph.State, len(sg.Elements))
	for i, el := range sg.Elements {
		out[i] = subgraph.State{Reason: subgraph.EndReasonNotSeenInWindow}
		for _, p := range el.Pieces {
			if p.End <= kf.At {
				out[i] = subgraph.State{Reason: p.EndReason}
			}
		}
	}
	for _, id := range kf.Elements {
		out[id] = subgraph.State{Present: true}
	}
	return out
}

// forwardStates is every element's state at rel, from the last keyframe at or
// before it and the deltas after: each delta must find the state it says it
// leaves, reason included.
func forwardStates(t tb, sg subgraph.Subgraph, rel uint64) []subgraph.State {
	t.Helper()
	k := 0
	for i, kf := range sg.Keyframes {
		if kf.At <= rel {
			k = i
		}
	}
	st := keyframeStates(sg, sg.Keyframes[k])
	for i := firstAfter(sg.Deltas, sg.Keyframes[k].At); i < len(sg.Deltas) && sg.Deltas[i].At <= rel; i++ {
		d := sg.Deltas[i]
		if st[d.Element] != d.Before {
			t.Fatalf("the delta %+v finds the state %+v", d, st[d.Element])
		}
		st[d.Element] = d.After
	}
	return st
}

// backwardStates is every element's state at rel, from the first keyframe after
// it and the deltas between, inverted: a delta's before-state is the state it
// restores, so a disappearance by expiry is undone to what preceded it.
func backwardStates(t tb, sg subgraph.Subgraph, rel uint64) ([]subgraph.State, bool) {
	t.Helper()
	k := -1
	for i := len(sg.Keyframes) - 1; i >= 0; i-- {
		if sg.Keyframes[i].At > rel {
			k = i
		}
	}
	if k < 0 {
		return nil, false
	}
	st := keyframeStates(sg, sg.Keyframes[k])
	for i := firstAfter(sg.Deltas, sg.Keyframes[k].At) - 1; i >= 0 && sg.Deltas[i].At > rel; i-- {
		d := sg.Deltas[i]
		if st[d.Element] != d.After {
			t.Fatalf("the delta %+v, inverted, finds the state %+v", d, st[d.Element])
		}
		st[d.Element] = d.Before
	}
	return st, true
}

// instants are the relative instants to check: From, To-1ns, and every delta's
// time and its neighbours at 1ns.
func instants(sg subgraph.Subgraph) []uint64 {
	width := uint64(sg.To.Sub(sg.From))
	set := map[uint64]bool{0: true, width - 1: true}
	for _, d := range sg.Deltas {
		for _, x := range []uint64{d.At - 1, d.At, d.At + 1} {
			if x < width {
				set[x] = true
			}
		}
	}
	out := make([]uint64, 0, len(set))
	for x := range set {
		out = append(out, x)
	}
	slices.Sort(out)
	return out
}

// checkOracle checks the subgraph against the input at every instant: forward
// from the keyframe before it, backward from the keyframe after it, and every
// keyframe directly.
func checkOracle(t tb, in subgraph.Input, sg subgraph.Subgraph) {
	t.Helper()
	ids := map[subgraph.Key]uint32{}
	for i, el := range sg.Elements {
		ids[el.Key] = uint32(i)
	}
	for _, x := range instants(sg) {
		want := truth(t, in, ids, x)
		if got := forward(t, sg, x); !slices.Equal(got, want) {
			t.Fatalf("forward at +%dns: %v, want %v", x, got, want)
		}
		if got, ok := backward(t, sg, x); ok && !slices.Equal(got, want) {
			t.Fatalf("backward at +%dns: %v, want %v", x, got, want)
		}
		wantStates := truthStates(in, sg, x)
		if got := forwardStates(t, sg, x); !slices.Equal(got, wantStates) {
			t.Fatalf("forward states at +%dns: %v, want %v", x, got, wantStates)
		}
		if got, ok := backwardStates(t, sg, x); ok && !slices.Equal(got, wantStates) {
			t.Fatalf("backward states at +%dns: %v, want %v", x, got, wantStates)
		}
	}
	for i, kf := range sg.Keyframes {
		if want := truth(t, in, ids, kf.At); !slices.Equal(kf.Elements, want) {
			t.Fatalf("keyframe %d at +%dns holds %v, the state is %v", i, kf.At, kf.Elements, want)
		}
	}
}

// checkBounds checks the structure and the two bounds: the first keyframe is at
// From and later ones are at delta instants only, E is the sum of the
// transitions of the elements in keyframes and deltas, the entries are at most
// N0 + 2E, the pieces at most N0 + E, and a seek costs less than max(1, |S|)
// deltas wherever no keyframe is.
func checkBounds(t tb, in subgraph.Input, sg subgraph.Subgraph) (entries, e, n0 int) {
	t.Helper()
	if len(sg.Keyframes) == 0 || sg.Keyframes[0].At != 0 {
		t.Fatalf("the first keyframe is not at From: %v", sg.Keyframes)
	}
	deltaTimes := map[uint64]bool{}
	for i, d := range sg.Deltas {
		deltaTimes[d.At] = true
		if d.At == 0 || d.At >= uint64(sg.To.Sub(sg.From)) {
			t.Fatalf("delta %d at +%dns is not strictly inside the window", i, d.At)
		}
		if i > 0 && d.At < sg.Deltas[i-1].At {
			t.Fatalf("deltas %d and %d decrease in time", i-1, i)
		}
	}
	for i, kf := range sg.Keyframes[1:] {
		if !deltaTimes[kf.At] || kf.At <= sg.Keyframes[i].At {
			t.Fatalf("keyframe %d at +%dns is not at a new delta instant", i+1, kf.At)
		}
	}
	n0, e, sum, pieces := len(sg.Keyframes[0].Elements), len(sg.Deltas), 0, 0
	for _, s := range in.Sources {
		if plain(s) {
			sum += s.Transitions
		}
	}
	elSum := 0
	for _, el := range sg.Elements {
		elSum += el.Transitions
		pieces += len(el.Pieces)
	}
	if e != sum || e != elSum {
		t.Fatalf("E = %d deltas, the transitions of the sources sum to %d and of the elements to %d", e, sum, elSum)
	}
	entries = sg.Entries()
	if entries > n0+2*e {
		t.Fatalf("%d entries, over N0 + 2E = %d + 2*%d", entries, n0, e)
	}
	if pieces > n0+e {
		t.Fatalf("%d pieces, over N0 + E = %d + %d", pieces, n0, e)
	}
	// seek: the deltas from the last keyframe at or before an instant, up to it.
	size, cost, k := n0, 0, 0
	for i := 0; i < len(sg.Deltas); {
		at := sg.Deltas[i].At
		for ; i < len(sg.Deltas) && sg.Deltas[i].At == at; i++ {
			cost++
			if sg.Deltas[i].Appear() {
				size++
			} else {
				size--
			}
		}
		if k+1 < len(sg.Keyframes) && sg.Keyframes[k+1].At == at {
			k++
			if len(sg.Keyframes[k].Elements) != size {
				t.Fatalf("keyframe at +%dns holds %d elements, the state has %d", at, len(sg.Keyframes[k].Elements), size)
			}
			cost = 0
			continue
		}
		if cost >= max(1, size) {
			t.Fatalf("a seek to +%dns costs %d deltas with no keyframe, at a size of %d", at, cost, size)
		}
	}
	return entries, e, n0
}

func check(t tb, in subgraph.Input, sg subgraph.Subgraph) (entries, e, n0 int) {
	t.Helper()
	checkOracle(t, in, sg)
	return checkBounds(t, in, sg)
}

func sortedKeys(sg subgraph.Subgraph) []string {
	var out []string
	for _, el := range sg.Elements {
		out = append(out, el.Key.String())
	}
	sort.Strings(out)
	return out
}

func expectEqual[T comparable](t *testing.T, what string, got, want []T) {
	t.Helper()
	if !slices.Equal(got, want) {
		t.Errorf("%s:\ngot:  %v\nwant: %v", what, got, want)
	}
}
