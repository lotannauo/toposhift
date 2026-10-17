package mcp

import (
	"fmt"
	"slices"
	"strings"
	"testing"

	"github.com/lotannauo/toposhift/internal/lifecycle"
)

// elementLists are the lists of each tool whose entries are elements: each must
// carry basis and subject, and rule when derived.
var elementLists = map[string][]string{
	"blast_radius":    {"members", "unattributed", "quarantined"},
	"what_changed":    {"created", "deleted", "changed", "transient", "events", "quarantined"},
	"co_residents":    {"placement", "co_residents", "suppressed", "unattributed", "quarantined"},
	"service_context": {"callers", "callees", "pairs", "suppressed", "unattributed", "unmapped", "quarantined"},
	"neighbors_at":    {"edges", "quarantined"},
	"entity_history":  {"records"},
	"find_entities":   {"matches"},
}

var endVocabulary = []string{"producer", "liveness_expiry", "cascade", "unattributed", "reach_ended"}

// walk calls f on every JSON object in v, with its path.
func walk(v any, path string, f func(path string, obj map[string]any)) {
	switch x := v.(type) {
	case map[string]any:
		f(path, x)
		for k, c := range x {
			walk(c, path+"."+k, f)
		}
	case []any:
		for i, c := range x {
			walk(c, fmt.Sprintf("%s[%d]", path, i), f)
		}
	}
}

func TestLabels(t *testing.T) {
	w := newWorld(t, rankObjs)
	s := w.source(t, rankObjs, true)
	sw := newSvcWorld(t, newSvcMem(t))
	for _, a := range append(allAnswers(t, w, s), svcAnswers(t, sw, sw.source(t))...) {
		t.Run(a.name, func(t *testing.T) {
			ans := decode(t, a.json)

			// Every element carries basis and subject, and every derived element a rule.
			for _, list := range elementLists[a.tool] {
				items, ok := ans[list].([]any)
				if !ok {
					t.Errorf("%s is not a list", list)
					continue
				}
				for i, it := range items {
					obj := it.(map[string]any)
					if _, ok := obj["basis"].(string); !ok {
						t.Errorf("%s[%d] has no basis", list, i)
					}
					if _, ok := obj["subject"].(string); !ok {
						t.Errorf("%s[%d] has no subject", list, i)
					}
				}
			}
			walk(ans, "$", func(path string, obj map[string]any) {
				basis, has := obj["basis"].(string)
				if !has {
					return
				}
				if basis != "fact" && basis != "derived" {
					t.Errorf("%s: basis %q", path, basis)
				}
				if sub, _ := obj["subject"].(string); sub != "asserted" && sub != "derived" {
					t.Errorf("%s: subject %q", path, obj["subject"])
				}
				if basis == "derived" {
					if rule, _ := obj["rule"].(string); rule == "" {
						t.Errorf("%s: a derived element has no rule", path)
					}
				}
				if basis == "fact" {
					if _, has := obj["rule"]; has {
						t.Errorf("%s: a fact has a rule", path)
					}
				}
			})

			// Every span has end_reason and its sentence. The reason is null exactly
			// when the span reaches the window's end, and otherwise a word of the table.
			windowTo := ""
			if win, ok := ans["window"].(map[string]any); ok {
				windowTo, _ = win["to"].(string)
			}
			walk(ans, "$", func(path string, obj map[string]any) {
				_, hasStart := obj["start"]
				end, hasEnd := obj["end"].(string)
				if !hasStart || !hasEnd {
					return
				}
				reason, has := obj["end_reason"]
				if !has {
					t.Errorf("%s: a span has no end_reason", path)
					return
				}
				if s, _ := obj["end_sentence"].(string); s == "" {
					t.Errorf("%s: a span has no sentence", path)
				}
				if windowTo != "" && (reason == nil) != (end == windowTo) {
					t.Errorf("%s: end %s, window end %s, end_reason %v", path, end, windowTo, reason)
				}
				if reason != nil && !slices.Contains(endVocabulary, reason.(string)) {
					t.Errorf("%s: end_reason %q is not in the vocabulary", path, reason)
				}
			})
		})
	}
}

// A blast-radius reach end is reach_ended, and never carries a producer or an
// expiry sentence: it has no source of its own, the legs say how they ended.
func TestBlastReachEndIsReachEnded(t *testing.T) {
	w := newWorld(t, rankObjs)
	s := w.source(t, rankObjs, true)
	out, err := s.BlastRadius(ctx, BlastRadiusInput{Entity: w.h1.String(), From: w10, To: w1030})
	if err != nil {
		t.Fatal(err)
	}
	ended, legEnded := 0, map[string]bool{}
	for _, m := range out.Members {
		for _, sp := range m.Reach {
			if sp.EndReason == nil {
				continue
			}
			ended++
			if *sp.EndReason != "reach_ended" || strings.Contains(sp.EndSentence, "producer") || strings.Contains(sp.EndSentence, "TTL") {
				t.Errorf("%s: reach end %q %q", m.Entity.ID, *sp.EndReason, sp.EndSentence)
			}
		}
		for _, v := range m.Via {
			for _, sp := range v.Leg {
				if sp.EndReason != nil {
					legEnded[*sp.EndReason] = true
				}
			}
		}
	}
	if ended == 0 {
		t.Fatal("the fixture has no reach that ends inside the window")
	}
	for _, want := range []string{"producer", "liveness_expiry", "cascade"} {
		if !legEnded[want] {
			t.Errorf("no leg ends with %q: %v", want, legEnded)
		}
	}
}

// The rank digest is part of the world an answer was read in: it is on every
// answer that carries names or descriptions, and it changes when the ranks do.
func TestRankDigestInPolicy(t *testing.T) {
	w := newWorld(t, rankObjs)
	a := w.source(t, rankObjs, true)
	b := w.source(t, rankClus, true)
	c := w.source(t, map[lifecycle.Producer]int{agent: 1, clus: 2, objs: 3}, true)

	for _, ans := range allAnswers(t, w, a) {
		pol, ok := decode(t, ans.json)["policy"].(map[string]any)
		if !ok {
			t.Fatalf("%s: no policy", ans.name)
		}
		if ans.tool == "describe_schema" {
			if _, has := pol["rank"]; has {
				t.Errorf("%s: the schema reads no store and has no rank", ans.name)
			}
			continue
		}
		if rank, _ := pol["rank"].(string); rank == "" {
			t.Errorf("%s: no rank in the policy", ans.name)
		}
		if pol["names"] != float64(NameTableVersion) {
			t.Errorf("%s: names %v", ans.name, pol["names"])
		}
	}

	digest := func(s *Source) string {
		out, err := s.BlastRadius(ctx, BlastRadiusInput{Entity: w.h1.String(), From: w10, To: w1030})
		if err != nil {
			t.Fatal(err)
		}
		return out.Policy.Rank
	}
	if digest(a) == digest(b) {
		t.Error("other ranks give the same digest")
	}
	if digest(a) != digest(c) {
		t.Error("the same ranks, written in another order, give another digest")
	}
}

// Every rule an answer carries is in the vocabulary that describe_schema gives.
func TestRulesAreInTheVocabulary(t *testing.T) {
	w := newWorld(t, rankObjs)
	s := w.source(t, rankObjs, true)
	var terms []string
	for _, r := range vocabulary.Rules {
		terms = append(terms, r.Term)
	}
	used := map[string]bool{}
	sw := newSvcWorld(t, newSvcMem(t))
	for _, a := range append(allAnswers(t, w, s), svcAnswers(t, sw, sw.source(t))...) {
		walk(decode(t, a.json), "$", func(path string, obj map[string]any) {
			rule, ok := obj["rule"].(string)
			if !ok {
				return
			}
			used[rule] = true
			known := slices.Contains(terms, rule)
			for _, prefix := range []string{"unattributed.", "diff."} {
				if strings.HasPrefix(rule, prefix) && slices.Contains(terms, prefix+"<"+map[string]string{"unattributed.": "reason", "diff.": "bucket"}[prefix]+">") {
					known = true
				}
			}
			if !known {
				t.Errorf("%s: the rule %q is not in the vocabulary", path, rule)
			}
		})
	}
	for _, want := range []string{
		"existence", "blast_radius.reach", "co_residency.shared_node", "co_residency.shared_host", "unattributed.no_host", "unattributed.quarantined_host", "diff.created", "diff.deleted", "diff.changed", "diff.transient", "diff.event",
		"pair.shared_pod", "pair.shared_node", "pair.shared_host", "unmapped", "unattributed.several_pods",
	} {
		if !used[want] && want != "unattributed.no_host" {
			t.Errorf("no answer of the fixture uses the rule %q", want)
		}
	}
}
