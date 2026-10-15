package subgraph_test

import (
	"errors"
	"strings"
	"testing"

	"github.com/lotannauo/toposhift/internal/catalog"
	"github.com/lotannauo/toposhift/internal/mcp"
	"github.com/lotannauo/toposhift/internal/query/coresidency"
	"github.com/lotannauo/toposhift/internal/query/extract"
	"github.com/lotannauo/toposhift/internal/query/subgraph"
	"github.com/lotannauo/toposhift/internal/store"
)

// The derived-kind table is frozen: each value and rule is written out here.
func TestDerivedKindTable(t *testing.T) {
	want := []struct {
		kind subgraph.DerivedKind
		rule string
	}{
		{1, "co_residency.shared_node"},
		{2, "co_residency.shared_host"},
		{3, "pair.shared_pod"},
		{4, "pair.shared_node"},
		{5, "pair.shared_host"},
		{6, "blast_radius.reach"},
		{7, "same_as"},
	}
	kinds := subgraph.DerivedKinds()
	if len(kinds) != len(want) {
		t.Fatalf("%d kinds, want %d", len(kinds), len(want))
	}
	for i, w := range want {
		if kinds[i] != w.kind || kinds[i].Rule() != w.rule || kinds[i].String() != w.rule || !kinds[i].Valid() {
			t.Errorf("row %d: %d %q", i, kinds[i], kinds[i].Rule())
		}
		if k, ok := subgraph.DerivedKindOf(w.rule); !ok || k != w.kind {
			t.Errorf("DerivedKindOf(%q) = %d, %t", w.rule, k, ok)
		}
	}
	for _, bad := range []subgraph.DerivedKind{0, 8, 255} {
		if bad.Valid() || !strings.HasPrefix(bad.Rule(), "DerivedKind(") {
			t.Errorf("%d is in the table", bad)
		}
	}
	for _, r := range []string{"", "existence", "co_residency", "pair.shared_galaxy", "unattributed.no_host", "diff.appeared", "class"} {
		if _, ok := subgraph.DerivedKindOf(r); ok {
			t.Errorf("DerivedKindOf(%q) found a kind", r)
		}
	}
	// The rules are those of the MCP vocabulary, where it has them, and the
	// MCP-only rules are not elements.
	for name, pair := range map[string][2]string{
		"reach": {subgraph.RuleReach, mcp.RuleReach}, "shared node": {subgraph.RuleCoSharedNode, mcp.RuleSharedNode},
		"shared host": {subgraph.RuleCoSharedHost, mcp.RuleSharedHost}, "existence": {subgraph.RuleExistence, mcp.RuleExistence},
	} {
		if pair[0] != pair[1] {
			t.Errorf("%s: the wire's rule %q, the MCP rule %q", name, pair[0], pair[1])
		}
	}
	for _, r := range []string{mcp.RuleUnattributed, mcp.RuleDiff, "class"} {
		if _, ok := subgraph.DerivedKindOf(strings.TrimSuffix(r, ".")); ok {
			t.Errorf("the MCP-only rule %q is a derived kind", r)
		}
	}
	// The one derived relation of the catalog has a row, and no other relation
	// is derived.
	for r := range catalog.Default().Relations() {
		_, ok := subgraph.DerivedKindOf(string(r.Type()))
		if (r.Storage() == catalog.StorageDerived) != ok {
			t.Errorf("relation %s: storage %s, in the table %t", r.Type(), r.Storage(), ok)
		}
	}
}

// Every table of the adapters is frozen, and a Go constant of another package
// reaches it by name only: coresidency's SharedNode is 1, extract's is 2.
func TestAdapterTablesAreFrozen(t *testing.T) {
	// The tables, the shape of a derived key and its validation are the policy.
	if subgraph.PolicyVersion != 2 {
		t.Errorf("policy version %d: a change to the tables or the labels raises it", subgraph.PolicyVersion)
	}
	type row struct {
		value uint8
		name  string
	}
	for name, c := range map[string]struct{ got, want []row }{
		"mechanism": {
			[]row{
				{uint8(subgraph.MechanismSharedPod), subgraph.MechanismSharedPod.String()},
				{uint8(subgraph.MechanismSharedNode), subgraph.MechanismSharedNode.String()},
				{uint8(subgraph.MechanismSharedHost), subgraph.MechanismSharedHost.String()},
			},
			[]row{{1, "shared_pod"}, {2, "shared_node"}, {3, "shared_host"}},
		},
		"suppress reason": {
			[]row{{uint8(subgraph.SuppressedBelowMinOverlap), subgraph.SuppressedBelowMinOverlap.String()}, {uint8(subgraph.SuppressedUbiquitous), subgraph.SuppressedUbiquitous.String()}},
			[]row{{1, "below_min_overlap"}, {2, "ubiquitous"}},
		},
		"unattributed reason": {
			[]row{
				{uint8(subgraph.UnattributedNoHost), subgraph.UnattributedNoHost.String()},
				{uint8(subgraph.UnattributedSeveralHosts), subgraph.UnattributedSeveralHosts.String()},
				{uint8(subgraph.UnattributedQuarantinedHost), subgraph.UnattributedQuarantinedHost.String()},
				{uint8(subgraph.UnattributedSeveralNodes), subgraph.UnattributedSeveralNodes.String()},
				{uint8(subgraph.UnattributedSeveralPods), subgraph.UnattributedSeveralPods.String()},
				{uint8(subgraph.UnattributedSeveralServices), subgraph.UnattributedSeveralServices.String()},
				{uint8(subgraph.UnattributedSeveralClaims), subgraph.UnattributedSeveralClaims.String()},
			},
			[]row{{1, "no_host"}, {2, "several_hosts"}, {3, "quarantined_host"}, {4, "several_nodes"}, {5, "several_pods"}, {6, "several_services"}, {7, "several_claims"}},
		},
		"anchor": {
			[]row{
				{uint8(subgraph.AnchorNone), subgraph.AnchorNone.String()},
				{uint8(subgraph.AnchorOnHost), subgraph.AnchorOnHost.String()},
				{uint8(subgraph.AnchorNotOnHost), subgraph.AnchorNotOnHost.String()},
				{uint8(subgraph.AnchorHostUnattributed), subgraph.AnchorHostUnattributed.String()},
			},
			[]row{{0, "none"}, {1, "on_host"}, {2, "not_on_host"}, {3, "host_unattributed"}},
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
	for _, s := range []string{subgraph.Mechanism(9).String(), subgraph.SuppressReason(9).String(), subgraph.UnattributedReason(9).String(), subgraph.AnchorState(9).String()} {
		if !strings.Contains(s, "(") {
			t.Errorf("a value outside a table is named %q", s)
		}
	}
	if subgraph.Mechanism(0).Valid() || subgraph.Mechanism(4).Valid() || subgraph.SuppressReason(0).Valid() || subgraph.UnattributedReason(0).Valid() ||
		subgraph.UnattributedReason(8).Valid() || subgraph.AnchorState(4).Valid() {
		t.Error("a value outside a table is valid")
	}
}

// The two Mechanism types map by name; their Go values differ, so a cast would
// be a bug; and a value outside either table, or the zero, is refused.
func TestMechanismMapping(t *testing.T) {
	if uint8(coresidency.SharedNode) == uint8(extract.SharedNode) {
		t.Fatal("the premise fails: the two packages number SharedNode alike")
	}
	if uint8(coresidency.SharedNode) == uint8(subgraph.MechanismSharedNode) {
		t.Error("a cast of coresidency's SharedNode would be right; the wire value must not depend on it")
	}
	n := 0
	for m := coresidency.SharedNode; !strings.HasPrefix(m.String(), "Mechanism("); m++ {
		n++
		got, err := subgraph.MechanismOfCoResidency(m)
		if err != nil || got.String() != m.String() {
			t.Errorf("MechanismOfCoResidency(%s) = %s, %v", m, got, err)
		}
	}
	if n != 2 {
		t.Errorf("coresidency has %d mechanisms, the wire maps 2", n)
	}
	n = 0
	for m := extract.SharedPod; !strings.HasPrefix(m.String(), "Mechanism("); m++ {
		n++
		got, err := subgraph.MechanismOfExtract(m)
		if err != nil || got.String() != m.String() {
			t.Errorf("MechanismOfExtract(%s) = %s, %v", m, got, err)
		}
	}
	if n != 3 {
		t.Errorf("extract has %d mechanisms, the wire maps 3", n)
	}
	for _, m := range []coresidency.Mechanism{coresidency.MechanismNone, 3, 9, 255} {
		if got, err := subgraph.MechanismOfCoResidency(m); err == nil || !errors.Is(err, store.ErrInvalid) {
			t.Errorf("MechanismOfCoResidency(%s) = %s, %v, want a refusal", m, got, err)
		}
	}
	for _, m := range []extract.Mechanism{extract.MechanismNone, 4, 9, 255} {
		if got, err := subgraph.MechanismOfExtract(m); err == nil || !errors.Is(err, store.ErrInvalid) {
			t.Errorf("MechanismOfExtract(%s) = %s, %v, want a refusal", m, got, err)
		}
	}
}

func TestReasonMappings(t *testing.T) {
	for r := coresidency.BelowMinOverlap; !strings.HasPrefix(r.String(), "Reason("); r++ {
		got, err := subgraph.SuppressReasonOf(r)
		if err != nil || got.String() != r.String() {
			t.Errorf("SuppressReasonOf(%s) = %s, %v", r, got, err)
		}
	}
	for _, r := range []coresidency.Reason{coresidency.ReasonNone, 3, 255} {
		if got, err := subgraph.SuppressReasonOf(r); err == nil || !errors.Is(err, store.ErrInvalid) {
			t.Errorf("SuppressReasonOf(%s) = %s, %v, want a refusal", r, got, err)
		}
	}
	n := 0
	for r := coresidency.NoHost; !strings.HasPrefix(r.String(), "UnattributedReason("); r++ {
		n++
		got, err := subgraph.UnattributedReasonOfCoResidency(r)
		if err != nil || got.String() != r.String() {
			t.Errorf("UnattributedReasonOfCoResidency(%s) = %s, %v", r, got, err)
		}
	}
	if n != 4 {
		t.Errorf("coresidency has %d unattributed reasons, the wire maps 4", n)
	}
	for _, r := range []coresidency.UnattributedReason{0, 5, 255} {
		if got, err := subgraph.UnattributedReasonOfCoResidency(r); err == nil || !errors.Is(err, store.ErrInvalid) {
			t.Errorf("UnattributedReasonOfCoResidency(%s) = %s, %v, want a refusal", r, got, err)
		}
	}
	n = 0
	for r := extract.SeveralPods; !strings.HasPrefix(r.String(), "UnattributedReason("); r++ {
		n++
		got, err := subgraph.UnattributedReasonOfExtract(r)
		if err != nil || got.String() != r.String() {
			t.Errorf("UnattributedReasonOfExtract(%s) = %s, %v", r, got, err)
		}
	}
	if n != 2 {
		t.Errorf("extract has %d mapping reasons, the wire maps 2", n)
	}
	for _, r := range []extract.UnattributedReason{0, 3, 255} {
		if got, err := subgraph.UnattributedReasonOfExtract(r); err == nil || !errors.Is(err, store.ErrInvalid) {
			t.Errorf("UnattributedReasonOfExtract(%s) = %s, %v, want a refusal", r, got, err)
		}
	}
	n = 0
	for a := extract.AnchorNone; !strings.HasPrefix(a.String(), "AnchorState("); a++ {
		n++
		got, err := subgraph.AnchorStateOf(a)
		if err != nil || got.String() != a.String() {
			t.Errorf("AnchorStateOf(%s) = %s, %v", a, got, err)
		}
	}
	if n != 4 {
		t.Errorf("extract has %d anchor states, the wire maps 4", n)
	}
	for _, a := range []extract.AnchorState{4, 255} {
		if got, err := subgraph.AnchorStateOf(a); err == nil || !errors.Is(err, store.ErrInvalid) {
			t.Errorf("AnchorStateOf(%s) = %s, %v, want a refusal", a, got, err)
		}
	}
}
