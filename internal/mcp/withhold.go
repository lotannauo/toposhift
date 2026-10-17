package mcp

import (
	"encoding/json"
	"fmt"
	"regexp"
	"slices"
	"strings"

	"github.com/lotannauo/toposhift/internal/catalog"
	"github.com/lotannauo/toposhift/internal/identity"
	"github.com/lotannauo/toposhift/internal/query/graphdiff"
	"github.com/lotannauo/toposhift/internal/query/interval"
	"github.com/lotannauo/toposhift/internal/store"
)

// An endpoint's id is the first 16 bytes of SHA-256 over its realm and its
// address. The realm is known and a private IPv4 realm holds at most 2^24
// addresses, so hashing every candidate inverts an id in seconds: an endpoint's id
// is as revealing as its address, which the payload table already withholds.
//
// The rule (version [WithholdVersion]): an answer, an error sentence or a log line
// prints a topo.endpoint id, or an element that names one, only when the request
// named that endpoint, by its id or by the realm and address it was computed from.
// What would print any other is withheld and counted, never dropped silently and
// never blanked. A last check refuses an answer that still carries one.

// endpointID matches the text of a topo.endpoint id.
var endpointID = regexp.MustCompile(regexp.QuoteMeta(string(catalog.NetworkEndpoint)) + ":[0-9a-f]{32}")

// withheldEndpoint is what an endpoint id the request did not name becomes in a
// sentence.
var withheldEndpoint = string(catalog.NetworkEndpoint) + ":withheld"

// request is what one call's request named: the endpoints its answer may print.
type request struct {
	// named holds the ids as text, "topo.endpoint:<32 hex>".
	named map[string]bool
	// noFilter is set only by the test hooks: the filters keep everything.
	noFilter bool
}

func (s *Source) newRequest() *request {
	return &request{named: map[string]bool{}, noFilter: s.hooks != nil && s.hooks.noFilter}
}

// nameText records every endpoint id in the given argument strings, as the
// client wrote them and before they are parsed.
func (r *request) nameText(args ...string) {
	for _, a := range args {
		for _, m := range endpointID.FindAllString(a, -1) {
			r.named[m] = true
		}
	}
}

// name records fp if it is an endpoint.
func (r *request) name(fp identity.Fingerprint) {
	if fp.Type() == catalog.NetworkEndpoint {
		r.named[fp.String()] = true
	}
}

// hides says fp is an endpoint the request did not name.
func (r *request) hides(fp identity.Fingerprint) bool {
	return !r.noFilter && fp.Type() == catalog.NetworkEndpoint && !r.named[fp.String()]
}

// hidesSubject says an entity subject, or either end of an edge, is an endpoint
// the request did not name.
func (r *request) hidesSubject(s store.Subject) bool {
	if r.hides(s.A) {
		return true
	}
	return s.Kind == store.SubjectEdge && r.hides(s.B)
}

// unnamedEndpoints lists, sorted and once each, the endpoint ids in b that named
// does not hold.
func unnamedEndpoints(b []byte, named map[string]bool) []string {
	var out []string
	for _, m := range endpointID.FindAll(b, -1) {
		if id := string(m); !named[id] {
			out = append(out, id)
		}
	}
	slices.Sort(out)
	return slices.Compact(out)
}

// redactEndpoints replaces each endpoint id in s that named does not hold.
func redactEndpoints(s string, named map[string]bool) string {
	return endpointID.ReplaceAllStringFunc(s, func(m string) string {
		if named[m] {
			return m
		}
		return withheldEndpoint
	})
}

// redactLog is a message made safe for a log, which can be shipped off the
// machine to a party no request authorized: it prints no endpoint id at all, named
// or not, and no raw hex key.
func redactLog(msg string) string {
	return redactHexRuns(redactEndpoints(msg, nil))
}

// hexRun is a run of 32 or more hex digits, in either case.
var hexRun = regexp.MustCompile(`[0-9a-fA-F]{32,}`)

// redactHexRuns is the second rule of redaction, applied after the first: it
// replaces every maximal run of 32 or more hex digits in s, in either case, with
// "withheld". The one exception is a run of exactly 32 lowercase digits that
// immediately follows <type>:, where <type> is a catalog entity type other than
// topo.endpoint and is not itself preceded by a type-name character: that is the
// id of another type, which an operator needs to see.
//
// The reason: pebblestore's parseKey prints a whole key in hex, and the read paths
// return that error. A key holds an entity's 16-byte hash behind a 2-byte numeric
// type id, not the type name, so redactEndpoints cannot see it. The threshold of 32
// sits above every decimal number an error prints (at most 20 digits), and below
// every whole key a read path prints (at least 40 digits). A truncated or corrupt
// key can print fewer than 32 digits and is not caught; that needs corruption of
// the data on disk, which is why the rule does not try. A named endpoint's id is
// withheld by this rule too, from an internal error's sentence. The rule is not
// applied to the sentence of a refusal, which echoes the client's text.
func redactHexRuns(s string) string {
	var sb strings.Builder
	last := 0
	for _, loc := range hexRun.FindAllStringIndex(s, -1) {
		if isEntityID(s, loc[0], loc[1]) {
			continue
		}
		sb.WriteString(s[last:loc[0]])
		sb.WriteString("withheld")
		last = loc[1]
	}
	sb.WriteString(s[last:])
	return sb.String()
}

// isEntityID says s[start:end] is the hex of an id of a type other than
// topo.endpoint, written <type>:<32 lowercase digits>.
func isEntityID(s string, start, end int) bool {
	if end-start != 32 || strings.ToLower(s[start:end]) != s[start:end] || start == 0 || s[start-1] != ':' {
		return false
	}
	before := s[:start-1]
	for e := range catalog.Default().Entities() {
		t := string(e.Type())
		if e.Type() == catalog.NetworkEndpoint || !strings.HasSuffix(before, t) {
			continue
		}
		if i := len(before) - len(t) - 1; i >= 0 && isTypeChar(before[i]) {
			continue
		}
		return true
	}
	return false
}

func isTypeChar(c byte) bool {
	return c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9' || c == '.' || c == '_' || c == '-'
}

// egressHooks are for tests. They are nil in production, and NewSource never sets
// them.
type egressHooks struct {
	// noFilter keeps the per-tool filters from withholding anything.
	noFilter bool
	// noPrintRule keeps the print rule from withholding any name or namespace the
	// name path reads, so that the backstops behind it can be reached.
	noPrintRule bool
	// noBackstop makes egress accept every answer.
	noBackstop bool
	// inject is called on the answer, a pointer to it, just before egress.
	inject func(out any)
}

// egressError is the failure of the last check. Its text holds no id.
type egressError struct {
	n   int
	typ string
}

func (e *egressError) Error() string {
	return fmt.Sprintf("an answer of type %s would have printed %d endpoint ids the request did not name", e.typ, e.n)
}

// egressSentence is the sentence for an answer the last check refused.
const egressSentence = "Internal error: the answer would have printed an endpoint id the request did not name, so it was withheld."

// egress refuses an answer that would print an endpoint id the request did not
// name. out is a pointer to the answer. It scans the prefixed form of the id only:
// a scan for bare hex would refuse the container ids an allowed payload key
// prints.
func (s *Source) egress(out any, req *request) error {
	if h := s.hooks; h != nil {
		if h.inject != nil {
			h.inject(out)
		}
		if h.noBackstop {
			return nil
		}
	}
	b, err := json.Marshal(out)
	if err != nil {
		return err
	}
	if ids := unnamedEndpoints(b, req.named); len(ids) > 0 {
		return &egressError{n: len(ids), typ: strings.TrimPrefix(fmt.Sprintf("%T", out), "*")}
	}
	return nil
}

// NeighborWithheld counts what neighbors_at left out because it names an endpoint
// the request did not name.
type NeighborWithheld struct {
	// Edges counts the edges that would have been listed, to such an endpoint.
	Edges int `json:"edges"`
	// Quarantined counts the quarantined entities that are such an endpoint.
	Quarantined int `json:"quarantined"`
}

// ChangeWithheld counts what what_changed left out of each list because it names
// an endpoint the scope does not name.
type ChangeWithheld struct {
	Created     Pair `json:"created"`
	Deleted     Pair `json:"deleted"`
	Changed     Pair `json:"changed"`
	Transient   Pair `json:"transient"`
	Quarantined Pair `json:"quarantined"`
	Events      int  `json:"events"`
}

// withholdDiff drops from a diff, and from its events, every element whose
// subject names an endpoint the request did not name, and counts each drop in its
// own bucket. The counts and truncation of the result stay as the diff gave them.
func withholdDiff(res graphdiff.Result, events []graphdiff.Event, req *request) (graphdiff.Result, []graphdiff.Event, ChangeWithheld) {
	var w ChangeWithheld
	count := func(p *Pair, s store.Subject) {
		if s.Kind == store.SubjectEntity {
			p.Entities++
		} else {
			p.Relations++
		}
	}
	entries := func(es []graphdiff.Entry, p *Pair) []graphdiff.Entry {
		out := make([]graphdiff.Entry, 0, len(es))
		for _, e := range es {
			if req.hidesSubject(e.Element.Subject) {
				count(p, e.Element.Subject)
				continue
			}
			out = append(out, e)
		}
		return out
	}
	res.Created = entries(res.Created, &w.Created)
	res.Deleted = entries(res.Deleted, &w.Deleted)
	res.Changed = entries(res.Changed, &w.Changed)
	res.Transient = entries(res.Transient, &w.Transient)

	quarantined := make([]interval.Element, 0, len(res.Quarantined))
	for _, el := range res.Quarantined {
		if req.hidesSubject(el.Subject) {
			count(&w.Quarantined, el.Subject)
			continue
		}
		quarantined = append(quarantined, el)
	}
	res.Quarantined = quarantined

	kept := make([]graphdiff.Event, 0, len(events))
	for _, ev := range events {
		if req.hidesSubject(ev.Subject) {
			w.Events++
			continue
		}
		kept = append(kept, ev)
	}
	return res, kept, w
}

func (p Pair) total() int { return p.Entities + p.Relations }

// neighborWithheldNote says what neighbors_at withheld, or "" if nothing.
func neighborWithheldNote(w NeighborWithheld) string {
	if w.Edges+w.Quarantined == 0 {
		return ""
	}
	quarantined := ""
	if w.Quarantined > 0 {
		quarantined = fmt.Sprintf(", and %d quarantined endpoints", w.Quarantined)
	}
	return fmt.Sprintf("%d edges to endpoints the request did not name were withheld%s: an endpoint's id reveals its address. Name the endpoint, by its id or through resolve_entity, to see it.", w.Edges, quarantined)
}

// changeWithheldNote says what what_changed withheld, or "" if nothing.
func changeWithheldNote(w ChangeWithheld) string {
	var parts []string
	for _, p := range []struct {
		n    int
		what string
	}{
		{w.Created.total(), "created"},
		{w.Deleted.total(), "deleted"},
		{w.Changed.total(), "changed"},
		{w.Transient.total(), "transient"},
		{w.Events, "events"},
		{w.Quarantined.total(), "quarantined"},
	} {
		if p.n > 0 {
			parts = append(parts, fmt.Sprintf("%d %s", p.n, p.what))
		}
	}
	if len(parts) == 0 {
		return ""
	}
	return "Withheld because each names an endpoint the scope does not (an endpoint's id reveals its address): " + strings.Join(parts, ", ") + "."
}

// changeCapNotes says, for each list the diff's own cap cut and from which some
// listed entries were withheld, that the entries it did not list are counted only.
func changeCapNotes(res graphdiff.Result, w ChangeWithheld) []string {
	var out []string
	for _, l := range []struct {
		what     string
		listed   int
		cut      int
		withheld int
	}{
		{"created", len(res.Created), res.Truncated.Created, w.Created.total()},
		{"deleted", len(res.Deleted), res.Truncated.Deleted, w.Deleted.total()},
		{"changed", len(res.Changed), res.Truncated.Changed, w.Changed.total()},
		{"transient", len(res.Transient), res.Truncated.Transient, w.Transient.total()},
		{"quarantined", len(res.Quarantined), res.Truncated.Quarantined, w.Quarantined.total()},
	} {
		if l.cut > 0 && l.withheld > 0 {
			out = append(out, fmt.Sprintf("Of the %d %s entries the diff listed under its cap, %d were withheld; the %d it did not list are counted only.", l.listed, l.what, l.withheld, l.cut))
		}
	}
	return out
}
