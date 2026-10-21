package vmmodel

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"strings"
)

// Support is whether the metrics store can answer a kind of question.
type Support uint8

const (
	// Yes: the store answers it.
	Yes Support = iota + 1
	// No: the store cannot answer it.
	No
	// Partly: it answers with a limit, which the row says.
	Partly
)

func (s Support) String() string {
	switch s {
	case Yes:
		return "Yes"
	case No:
		return "No"
	case Partly:
		return "Partly"
	}
	return fmt.Sprintf("Support(%d)", uint8(s))
}

// The identifiers of the rows a refused question is recorded under.
const (
	CapWindow    = "window"
	CapAsKnownAt = "as-known-at"
)

// Capability is one row of the capability table.
type Capability struct {
	// ID names the row for a refusal to point at; it is not part of the registered text.
	ID       string
	Question string
	Support  Support
	// Note says how, or what limits it.
	Note string
}

// capabilities is the table, registered before any data. A question the store cannot
// express is a row here. It is never a loss, never timed, and never in a ratio.
var capabilities = []Capability{
	{"point-placement", "Point placement, forward and reverse, at an instant", Yes, "at sample resolution"},
	{"existence", "Existence at an instant; dead objects", Yes, ""},
	{
		"expiry-per-producer", "Existence ends one TTL after a producer goes silent, per producer", Yes,
		"by query construction: each query names each producer's window. A single store-wide lookback could not; the query writer must know every producer's interval",
	},
	{"why-ended", "Why a span ended (deleted vs expired vs retention)", No, ""},
	{CapAsKnownAt, "As-known-at (snapshot token)", No, ""},
	{
		"late-corrections", "Late corrections that change a past answer", No,
		"a late sample is accepted, but the earlier answer is not versioned",
	},
	{
		CapWindow, "Every stored record in a window (Window)", No,
		"The created and deleted sets between two instants: yes, reported apart",
	},
	{
		"co-residency", "Co-residency over a window by exact interval overlap", Partly,
		"a join on `node` with subqueries at step resolution; overlaps exact only to one sampling interval",
	},
	{
		"blast-radius", "Multi-hop blast radius with every path checked by interval", No,
		"not in the query language; only a client issuing k queries",
	},
	{"description-change", "A description change without a new identity", No, "a changed label is a new series"},
}

// Capabilities returns the table in its registered order.
func Capabilities() []Capability {
	out := make([]Capability, len(capabilities))
	copy(out, capabilities)
	return out
}

// CapabilityByID returns the row with the identifier.
func CapabilityByID(id string) (Capability, bool) {
	for _, c := range capabilities {
		if c.ID == id {
			return c, true
		}
	}
	return Capability{}, false
}

// CapabilityTable is the table as text, one row a line, the form a registration records.
func CapabilityTable() string {
	var b strings.Builder
	for _, c := range capabilities {
		fmt.Fprintf(&b, "| %s | %s", c.Question, c.Support)
		if c.Note != "" {
			fmt.Fprintf(&b, ": %s", c.Note)
		}
		b.WriteString(" |\n")
	}
	return b.String()
}

// CapabilityDigest is the SHA-256 of [CapabilityTable], as hex.
func CapabilityDigest() string {
	sum := sha256.Sum256([]byte(CapabilityTable()))
	return hex.EncodeToString(sum[:])
}
