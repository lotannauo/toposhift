package mcp

import (
	"slices"
	"testing"
	"time"

	"github.com/lotannauo/toposhift/internal/lifecycle"
	"github.com/lotannauo/toposhift/internal/query/interval"
	"github.com/lotannauo/toposhift/internal/query/subgraph"
)

// MCP prints an end as a string and the wire as a subgraph.EndReason. Every
// lifecycle end names the same on both surfaces, so an answer and an encoded
// subgraph never disagree about why something ended.
func TestEndReasonsMatchTheWire(t *testing.T) {
	t.Run("sources", func(t *testing.T) {
		for s := lifecycle.EndProducer; s <= lifecycle.EndOperator; s++ {
			r, err := subgraph.EndReasonOf(s)
			if err != nil {
				t.Fatalf("%s: %v", s, err)
			}
			got, sentence := sourceReason(s)
			if got == nil || *got != r.String() {
				t.Errorf("%s: MCP prints %v, the wire names %q", s, got, r)
			}
			if sentence == "" {
				t.Errorf("%s: no sentence", s)
			}
		}
	})

	t.Run("derived values", func(t *testing.T) {
		if endReasonReachEnded != subgraph.EndReasonReachEnded.String() {
			t.Errorf("reach_ended: MCP prints %q, the wire names %q", endReasonReachEnded, subgraph.EndReasonReachEnded)
		}
		if endReasonNotKnown != subgraph.EndReasonUnattributed.String() {
			t.Errorf("unattributed: MCP prints %q, the wire names %q", endReasonNotKnown, subgraph.EndReasonUnattributed)
		}
	})

	// A piece that reaches To has no end reason in MCP (null). That is the wire's
	// EndReasonUnknown, "unknown": the piece has not ended yet.
	t.Run("the open end", func(t *testing.T) {
		to := time.Date(2026, 10, 1, 10, 30, 0, 0, time.UTC)
		p := interval.Piece{Span: interval.Span{Start: to.Add(-time.Minute), End: to}, EndSource: lifecycle.EndUnknown}
		reason, sentence := pieceEnd(p, to)
		if reason != nil {
			t.Errorf("a piece that reaches To has the end reason %q, want null", *reason)
		}
		if sentence != stillPresent {
			t.Errorf("a piece that reaches To has the sentence %q", sentence)
		}
		if subgraph.EndReasonUnknown.String() != "unknown" {
			t.Errorf("the wire names the open end %q", subgraph.EndReasonUnknown)
		}
	})
}

// describe_schema lists every end_reason an answer can carry, with the sentence
// the answer prints, and only names the wire has.
func TestVocabularyListsEveryEndReason(t *testing.T) {
	meaning := map[string]string{}
	for _, w := range vocabulary.EndReasons {
		meaning[w.Term] = w.Meaning
	}

	t.Run("printed", func(t *testing.T) {
		sources := []lifecycle.EndSource{
			lifecycle.EndProducer, lifecycle.EndLivenessExpiry, lifecycle.EndCascade,
			lifecycle.EndRetention, lifecycle.EndOperator,
			lifecycle.EndUnknown, lifecycle.EndSource(250), // the default: a source with no name
		}
		for _, s := range sources {
			reason, sentence := sourceReason(s)
			if reason == nil {
				t.Errorf("%s: no end reason", s)
				continue
			}
			m, ok := meaning[*reason]
			if !ok {
				t.Errorf("%s: %q is not a term of the vocabulary", s, *reason)
				continue
			}
			if m != sentence {
				t.Errorf("%s: the vocabulary says %q, an answer says %q", *reason, m, sentence)
			}
		}
		if _, ok := meaning[endReasonReachEnded]; !ok {
			t.Errorf("%s is not a term of the vocabulary", endReasonReachEnded)
		}
	})

	t.Run("wire names", func(t *testing.T) {
		var names []string
		for v := 0; v <= int(subgraph.EndReasonNotSeenInWindow); v++ {
			names = append(names, subgraph.EndReason(v).String())
		}
		for _, w := range vocabulary.EndReasons {
			if w.Term == "null" {
				continue
			}
			if !slices.Contains(names, w.Term) {
				t.Errorf("the term %q is not a name of the wire's end table %v", w.Term, names)
			}
		}
	})
}

// The rules of a pair are the wire's: a pair of services prints exactly the
// strings the wire's derived kinds are named by, and no other.
func TestPairRulesMatchTheWire(t *testing.T) {
	for _, c := range []struct{ mcp, wire string }{
		{RulePairSharedPod, subgraph.RulePairSharedPod},
		{RulePairSharedNode, subgraph.RulePairSharedNode},
		{RulePairSharedHost, subgraph.RulePairSharedHost},
	} {
		if c.mcp != c.wire {
			t.Errorf("MCP prints %q, the wire names %q", c.mcp, c.wire)
		}
	}
	for _, r := range []string{RulePairSharedPod, RulePairSharedNode, RulePairSharedHost, RuleUnmapped} {
		if !slices.ContainsFunc(vocabulary.Rules, func(w Word) bool { return w.Term == r }) {
			t.Errorf("the rule %q is not in the vocabulary", r)
		}
	}
}
