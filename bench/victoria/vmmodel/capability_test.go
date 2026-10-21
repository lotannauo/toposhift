package vmmodel_test

import (
	"strings"
	"testing"

	"github.com/lotannauo/toposhift/bench/victoria/vmmodel"
)

// registeredCapabilities is the capability table as text, frozen. It is registered before
// any data exists: a change to it is a change to a registration, not a refactor.
const registeredCapabilities = `| Point placement, forward and reverse, at an instant | Yes: at sample resolution |
| Existence at an instant; dead objects | Yes |
| Existence ends one TTL after a producer goes silent, per producer | Yes: by query construction: each query names each producer's window. A single store-wide lookback could not; the query writer must know every producer's interval |
| Why a span ended (deleted vs expired vs retention) | No |
| As-known-at (snapshot token) | No |
| Late corrections that change a past answer | No: a late sample is accepted, but the earlier answer is not versioned |
| Every stored record in a window (Window) | No: The created and deleted sets between two instants: yes, reported apart |
| Co-residency over a window by exact interval overlap | Partly: a join on ` + "`node`" + ` with subqueries at step resolution; overlaps exact only to one sampling interval |
| Multi-hop blast radius with every path checked by interval | No: not in the query language; only a client issuing k queries |
| A description change without a new identity | No: a changed label is a new series |
`

func TestTheCapabilityTableIsTheRegisteredOne(t *testing.T) {
	if got := vmmodel.CapabilityTable(); got != registeredCapabilities {
		t.Errorf("the table is\n%s\nwant\n%s", got, registeredCapabilities)
	}
	if got := vmmodel.CapabilityDigest(); len(got) != 64 {
		t.Errorf("digest %q", got)
	}
}

func TestCapabilityRows(t *testing.T) {
	ids := map[string]bool{}
	var yes, no, partly int
	for _, c := range vmmodel.Capabilities() {
		if c.ID == "" || c.Question == "" || ids[c.ID] {
			t.Errorf("row %+v has no ID, no question, or an ID used before", c)
		}
		ids[c.ID] = true
		switch c.Support {
		case vmmodel.Yes:
			yes++
		case vmmodel.No:
			no++
		case vmmodel.Partly:
			partly++
		default:
			t.Errorf("row %s has support %v", c.ID, c.Support)
		}
	}
	// Three the store answers, one it answers in part, and the rest it cannot.
	if yes != 3 || partly != 1 || no != 6 {
		t.Errorf("%d yes, %d partly, %d no", yes, partly, no)
	}
	// Every capability refusal the rewriter can make names a row that says No.
	for _, id := range []string{vmmodel.CapWindow, vmmodel.CapAsKnownAt} {
		c, ok := vmmodel.CapabilityByID(id)
		if !ok || c.Support != vmmodel.No {
			t.Errorf("refusal row %q: %+v, %v", id, c, ok)
		}
	}
	if _, ok := vmmodel.CapabilityByID("nope"); ok {
		t.Error("a row that is not one")
	}
	if !strings.Contains(vmmodel.Yes.String()+vmmodel.No.String()+vmmodel.Partly.String(), "YesNoPartly") || vmmodel.Support(9).String() != "Support(9)" {
		t.Error("Support.String")
	}
	// The returned slice is a copy.
	cs := vmmodel.Capabilities()
	cs[0].Question = "changed"
	if vmmodel.Capabilities()[0].Question == "changed" {
		t.Error("Capabilities shares its storage")
	}
}
