package bytesgate_test

import (
	"bytes"
	"cmp"
	"errors"
	"os"
	"slices"
	"testing"
	"time"

	"github.com/lotannauo/toposhift/internal/query/subgraph"
	"github.com/lotannauo/toposhift/internal/store"
	"github.com/lotannauo/toposhift/internal/wire"
	"github.com/lotannauo/toposhift/internal/wire/bytesgate"
)

// The reference scale is the one the gate is registered at: a change to it is a
// change to the fixtures, and is made on purpose.
func TestReferenceScale(t *testing.T) {
	want := bytesgate.Scale{Seed: 1, Pods: 110, Hosts: 40, ReplaceEvery: 30 * time.Second, SmallScope: 2000, LargeScope: 10000}
	if got := bytesgate.Reference(); got != want {
		t.Errorf("the reference scale is %+v, not %+v", got, want)
	}
	names := ""
	for _, f := range bytesgate.Fixtures(want) {
		names += f.Name
	}
	if names != "abcde" {
		t.Errorf("the fixtures are %q", names)
	}
}

func TestFixturesAreDeterministic(t *testing.T) {
	first, second := build(t, small()), build(t, small())
	for name, d := range first {
		a, err := wire.Encode(d)
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		b, err := wire.Encode(second[name])
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		if !bytes.Equal(a, b) {
			t.Errorf("fixture %s: two builds, two documents", name)
		}
	}
	other := small()
	other.Seed++
	for name, d := range build(t, other) {
		a, _ := wire.Encode(d)
		b, _ := wire.Encode(first[name])
		if bytes.Equal(a, b) {
			t.Errorf("fixture %s: the seed changed nothing", name)
		}
	}
}

// Every fixture is a document the codec reads back, built from the adapters'
// own output: a subgraph with a history, not only a keyframe.
func TestFixturesAreDocumentsOfTheFormat(t *testing.T) {
	for name, d := range build(t, small()) {
		b, err := wire.Encode(d)
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		back, err := wire.Decode(b)
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		if again, err := wire.Encode(back); err != nil || !bytes.Equal(again, b) {
			t.Errorf("%s: Encode(Decode(b)) is not b: %v", name, err)
		}
		sg := d.Subgraph
		if sg.Basis != subgraph.BasisDerived || !sg.To.Equal(sg.From.Add(time.Hour)) {
			t.Errorf("%s: basis %s, window %s", name, sg.Basis, sg.To.Sub(sg.From))
		}
		if len(sg.Elements) == 0 || len(sg.Deltas) == 0 || len(sg.Keyframes) == 0 {
			t.Errorf("%s: %d elements, %d deltas, %d keyframes", name, len(sg.Elements), len(sg.Deltas), len(sg.Keyframes))
		}
		if d.Query.AsOf != store.Latest || !slices.IsSortedFunc(d.Query.Options, func(a, b wire.Option) int { return cmp.Compare(a.Name, b.Name) }) {
			t.Errorf("%s: query %+v", name, d.Query)
		}
	}
}

// What each fixture is for: a co-residency search, an anchored and an
// exploratory extraction, and two scopes whose unions are the sizes asked for.
func TestFixturesAreWhatTheyAreFor(t *testing.T) {
	s := small()
	docs := build(t, s)
	kinds := map[string]wire.QueryKind{
		"a": wire.QueryCoResidents, "b": wire.QueryExtraction, "c": wire.QueryExtraction,
		"d": wire.QueryScope, "e": wire.QueryScope,
	}
	for name, kind := range kinds {
		if docs[name].Query.Kind != kind {
			t.Errorf("fixture %s answers a %s query, not %s", name, docs[name].Query.Kind, kind)
		}
	}
	for name, want := range map[string]int{"d": s.SmallScope, "e": s.LargeScope} {
		if got := len(docs[name].Subgraph.Elements); got != want {
			t.Errorf("fixture %s: a union of %d elements, not %d", name, got, want)
		}
	}
	a := docs["a"].Subgraph
	derived := 0
	for _, el := range a.Elements {
		if el.Key.Kind == subgraph.KindDerived {
			derived++
		}
	}
	if derived < s.Pods-1 {
		t.Errorf("fixture a: %d co-residents of a node of %d pods", derived, s.Pods)
	}
	if got := docs["b"].Subgraph.Sections.Anchor; got == subgraph.AnchorNone {
		t.Error("fixture b is not anchored")
	}
	if got := docs["c"].Subgraph.Sections.Anchor; got != subgraph.AnchorNone {
		t.Errorf("fixture c is anchored: %s", got)
	}
	if len(docs["b"].Subgraph.Sections.Pods) == 0 || len(docs["c"].Subgraph.Sections.Pods) <= len(docs["b"].Subgraph.Sections.Pods) {
		t.Errorf("pod sections: %d anchored, %d exploratory", len(docs["b"].Subgraph.Sections.Pods), len(docs["c"].Subgraph.Sections.Pods))
	}
}

func TestScaleIsValidated(t *testing.T) {
	with := func(edit func(*bytesgate.Scale)) bytesgate.Scale {
		s := small()
		edit(&s)
		return s
	}
	for _, s := range []bytesgate.Scale{
		{},
		with(func(s *bytesgate.Scale) { s.Pods = 6 }),
		with(func(s *bytesgate.Scale) { s.ReplaceEvery = time.Hour }),
		with(func(s *bytesgate.Scale) { s.Hosts = 0 }),
	} {
		for _, f := range bytesgate.Fixtures(s) {
			if _, err := f.Build(ctx); !errors.Is(err, store.ErrInvalid) {
				t.Errorf("fixture %s at %+v: %v", f.Name, s, err)
			}
		}
	}
}

// TestReferenceFixturesBuild builds the five fixtures at the reference scale,
// without measuring them: it says whether the adapters take the full cells, and
// logs their shape. It is skipped unless TOPOSHIFT_BYTES_GATE is set, like
// TestReport, because the cells are large.
func TestReferenceFixturesBuild(t *testing.T) {
	if os.Getenv("TOPOSHIFT_BYTES_GATE") == "" {
		t.Skip("set TOPOSHIFT_BYTES_GATE=1 to build the reference fixtures")
	}
	s := bytesgate.Reference()
	for _, f := range bytesgate.Fixtures(s) {
		start := time.Now()
		d, err := f.Build(ctx)
		if err != nil {
			t.Fatalf("fixture %s: %v", f.Name, err)
		}
		sg := d.Subgraph
		t.Logf("fixture %s (%s): %d entities, %d elements, %d keyframes, %d deltas, built in %s",
			f.Name, f.Title, len(sg.Entities), len(sg.Elements), len(sg.Keyframes), len(sg.Deltas), time.Since(start).Round(time.Millisecond))
		switch f.Name {
		case "d":
			if len(sg.Elements) != s.SmallScope {
				t.Errorf("fixture d: a union of %d elements, not %d", len(sg.Elements), s.SmallScope)
			}
		case "e":
			if len(sg.Elements) != s.LargeScope {
				t.Errorf("fixture e: a union of %d elements, not %d", len(sg.Elements), s.LargeScope)
			}
		}
	}
}
