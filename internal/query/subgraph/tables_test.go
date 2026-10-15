package subgraph_test

import (
	"errors"
	"strings"
	"testing"

	"github.com/lotannauo/toposhift/internal/lifecycle"
	"github.com/lotannauo/toposhift/internal/query/interval"
	"github.com/lotannauo/toposhift/internal/query/subgraph"
	"github.com/lotannauo/toposhift/internal/store"
)

// The tables are frozen: each value and each name is written out here, so a
// change to either fails this test before it changes a stored result. A value
// is appended under a new policy version; none moves.
func TestFrozenTables(t *testing.T) {
	type row struct {
		value uint8
		name  string
	}
	got := func(rows ...row) []row { return rows }
	for name, c := range map[string]struct{ got, want []row }{
		"basis": {
			got(row{uint8(subgraph.BasisFact), subgraph.BasisFact.String()}, row{uint8(subgraph.BasisDerived), subgraph.BasisDerived.String()}),
			[]row{{1, "fact"}, {2, "derived"}},
		},
		"kind": {
			got(row{uint8(subgraph.KindEntity), subgraph.KindEntity.String()}, row{uint8(subgraph.KindEdge), subgraph.KindEdge.String()},
				row{uint8(subgraph.KindDerived), subgraph.KindDerived.String()}),
			[]row{{1, "entity"}, {2, "edge"}, {3, "derived"}},
		},
		"subject": {
			got(row{uint8(subgraph.SubjectAsserted), subgraph.SubjectAsserted.String()}, row{uint8(subgraph.SubjectDerived), subgraph.SubjectDerived.String()}),
			[]row{{1, "asserted"}, {2, "derived"}},
		},
		"end reason": {
			got(
				row{uint8(subgraph.EndReasonUnknown), subgraph.EndReasonUnknown.String()},
				row{uint8(subgraph.EndReasonProducer), subgraph.EndReasonProducer.String()},
				row{uint8(subgraph.EndReasonLivenessExpiry), subgraph.EndReasonLivenessExpiry.String()},
				row{uint8(subgraph.EndReasonCascade), subgraph.EndReasonCascade.String()},
				row{uint8(subgraph.EndReasonRetention), subgraph.EndReasonRetention.String()},
				row{uint8(subgraph.EndReasonOperator), subgraph.EndReasonOperator.String()},
				row{uint8(subgraph.EndReasonUnattributed), subgraph.EndReasonUnattributed.String()},
				row{uint8(subgraph.EndReasonReachEnded), subgraph.EndReasonReachEnded.String()},
				row{uint8(subgraph.EndReasonNotSeenInWindow), subgraph.EndReasonNotSeenInWindow.String()},
			),
			[]row{
				{0, "unknown"},
				{1, "producer"},
				{2, "liveness_expiry"},
				{3, "cascade"},
				{4, "retention"},
				{5, "operator"},
				{6, "unattributed"},
				{7, "reach_ended"},
				{8, "not_seen_in_window"},
			},
		},
		"class": {
			got(
				row{uint8(subgraph.ClassNone), subgraph.ClassNone.String()},
				row{uint8(subgraph.ClassStable), subgraph.ClassStable.String()},
				row{uint8(subgraph.ClassAppeared), subgraph.ClassAppeared.String()},
				row{uint8(subgraph.ClassDisappeared), subgraph.ClassDisappeared.String()},
				row{uint8(subgraph.ClassTransient), subgraph.ClassTransient.String()},
				row{uint8(subgraph.ClassFlapping), subgraph.ClassFlapping.String()},
			),
			[]row{{0, "none"}, {1, "stable"}, {2, "appeared"}, {3, "disappeared"}, {4, "transient"}, {5, "flapping"}},
		},
	} {
		if len(c.got) != len(c.want) {
			t.Errorf("%s: %d values, want %d", name, len(c.got), len(c.want))
			continue
		}
		for i := range c.want {
			if c.got[i] != c.want[i] {
				t.Errorf("%s: row %d is %v, want %v", name, i, c.got[i], c.want[i])
			}
		}
	}
	// A value outside a table names itself and is not valid.
	if subgraph.EndReason(9).Valid() || subgraph.EndReason(9).String() != "EndReason(9)" || !subgraph.EndReasonNotSeenInWindow.Valid() {
		t.Error("the end reason table has a different extent")
	}
	for _, s := range []string{subgraph.Basis(0).String(), subgraph.Kind(0).String(), subgraph.Subject(0).String(), subgraph.Class(6).String()} {
		if !strings.Contains(s, "(") {
			t.Errorf("a value outside a table is named %q", s)
		}
	}
}

// Every lifecycle end source maps by name to its wire value; a value its table
// does not have is refused, never carried across.
func TestEndReasonOf(t *testing.T) {
	want := map[lifecycle.EndSource]subgraph.EndReason{
		lifecycle.EndUnknown:        subgraph.EndReasonUnknown,
		lifecycle.EndProducer:       subgraph.EndReasonProducer,
		lifecycle.EndLivenessExpiry: subgraph.EndReasonLivenessExpiry,
		lifecycle.EndCascade:        subgraph.EndReasonCascade,
		lifecycle.EndRetention:      subgraph.EndReasonRetention,
		lifecycle.EndOperator:       subgraph.EndReasonOperator,
	}
	// If lifecycle adds a source, this fails until the wire decides its value.
	n := 0
	for s := lifecycle.EndSource(0); !strings.HasPrefix(s.String(), "EndSource("); s++ {
		n++
		got, err := subgraph.EndReasonOf(s)
		if err != nil || got != want[s] || got.String() != s.String() {
			t.Errorf("EndReasonOf(%s) = %s, %v, want %s", s, got, err, want[s])
		}
	}
	if n != len(want) {
		t.Errorf("lifecycle has %d end sources, the wire table maps %d", n, len(want))
	}
	for _, s := range []lifecycle.EndSource{lifecycle.EndSource(n), 7, 255} {
		if got, err := subgraph.EndReasonOf(s); err == nil || !errors.Is(err, store.ErrInvalid) {
			t.Errorf("EndReasonOf(%s) = %s, %v, want a refusal", s, got, err)
		}
	}
}

// Every interval class maps by name to its wire value, and one outside the
// table is refused.
func TestClassOf(t *testing.T) {
	want := map[interval.Class]subgraph.Class{
		interval.ClassNone:        subgraph.ClassNone,
		interval.ClassStable:      subgraph.ClassStable,
		interval.ClassAppeared:    subgraph.ClassAppeared,
		interval.ClassDisappeared: subgraph.ClassDisappeared,
		interval.ClassTransient:   subgraph.ClassTransient,
		interval.ClassFlapping:    subgraph.ClassFlapping,
	}
	n := 0
	for c := interval.Class(0); !strings.HasPrefix(c.String(), "Class("); c++ {
		n++
		got, err := subgraph.ClassOf(c)
		if err != nil || got != want[c] || got.String() != c.String() {
			t.Errorf("ClassOf(%s) = %s, %v, want %s", c, got, err, want[c])
		}
	}
	if n != len(want) {
		t.Errorf("interval has %d classes, the wire table maps %d", n, len(want))
	}
	for _, c := range []interval.Class{interval.Class(n), 9, 255} {
		if got, err := subgraph.ClassOf(c); err == nil || !errors.Is(err, store.ErrInvalid) {
			t.Errorf("ClassOf(%s) = %s, %v, want a refusal", c, got, err)
		}
	}
}
