package mcp

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/lotannauo/toposhift/internal/catalog"
	"github.com/lotannauo/toposhift/internal/identity"
	"github.com/lotannauo/toposhift/internal/query/interval"
	"github.com/lotannauo/toposhift/internal/store"
)

// Lister is the listing read find_entities scans: the ids of one type, in id
// order, outside [store.Store], whose reads all take a fingerprint. memstore and
// pebblestore have it; a store that wraps one does not, unless it says so.
type Lister interface {
	ListEntities(ctx context.Context, typ catalog.EntityType, after identity.Fingerprint, n int) ([]identity.Fingerprint, error)
}

const (
	// findScanCap is the most ids a call checks: what_changed's scope cap, which has
	// the same cost per id.
	findScanCap = maxScope
	// findBatch is how many ids one read of the interval package takes.
	findBatch = 32
	// findDefaultLimit and findMaxLimit bound how many matches a call lists.
	findDefaultLimit = 50
	findMaxLimit     = 200
	// findMaxText is the most bytes of a name or a namespace to look for.
	findMaxText = 256
	// findMaxSpans is how many spans of an entity's existence a match lists.
	findMaxSpans = 8
)

// findTypes are the entity types find_entities lists: those the name table names.
var findTypes = []catalog.EntityType{catalog.K8sPod, catalog.K8sNode, catalog.Container, catalog.Service}

// FindEntitiesInput is the input of the find_entities tool.
type FindEntitiesInput struct {
	Type      string `json:"type" jsonschema:"The entity type: k8s.pod, k8s.node, container or service. Hosts have no name, and endpoints are never listed."`
	Name      string `json:"name,omitempty" jsonschema:"Text the name must contain, compared in lower case. Omit it to list every entity of the type. A name that is not printed (over 253 bytes, or with a control character, a format character (Unicode Cf: zero-width, bidi controls, tag characters) or an endpoint id's text) never matches."`
	Namespace string `json:"namespace,omitempty" jsonschema:"The namespace, exactly. An entity whose name has no namespace never matches: a node has none, and a container has one only from the cluster receiver's description. A namespace that is not printed never matches."`
	At        string `json:"at,omitempty" jsonschema:"An instant, RFC 3339 with a zone: the entities that existed then. Give at, or from and to."`
	From      string `json:"from,omitempty" jsonschema:"The start of a window [from, to): the entities that existed at any instant of it. The name is read at the window's last instant, else its first."`
	To        string `json:"to,omitempty" jsonschema:"The end of the window, excluded."`
	AsOf      string `json:"asof,omitempty" jsonschema:"A token from an earlier answer, to read the same world again. Pass it with after."`
	After     string `json:"after,omitempty" jsonschema:"The next_after id of an earlier answer, to continue its scan."`
	Limit     int    `json:"limit,omitempty" jsonschema:"How many matches to list, 1 to 200; the default is 50. The scan stops when it has this many."`
}

// FindEntitiesOutput is the answer of the find_entities tool. Every match is
// derived: its existence and its name are read at the token under the store's ranks.
type FindEntitiesOutput struct {
	Token     string        `json:"token"`
	Derived   bool          `json:"derived"`
	Policy    Policy        `json:"policy"`
	Type      string        `json:"type"`
	Window    Window        `json:"window"`
	Matches   []FoundEntity `json:"matches"`
	Complete  bool          `json:"complete"`
	NextAfter *string       `json:"next_after"`
	Counts    FindCounts    `json:"counts"`
	Notes     []string      `json:"notes"`
}

// FoundEntity is an entity that existed in the window and matched.
type FoundEntity struct {
	Entity      Ref    `json:"entity"`
	Subject     string `json:"subject"`
	Basis       string `json:"basis"`
	Rule        string `json:"rule"`
	Class       string `json:"class"`
	Transitions int    `json:"transitions"`
	Spans       []Span `json:"spans"`
	SpansTotal  int    `json:"spans_total"`
}

// FindCounts are the exact sizes of a scan: the ids it checked and what became of
// them. Checked is exact, and the other counts are of the ids checked.
type FindCounts struct {
	Checked     int `json:"checked"`
	Matched     int `json:"matched"`
	Undescribed int `json:"undescribed"`
	// NotPrinted counts the entities whose name or namespace, which the request
	// asked to match, is not printed (see names.go), so that it could not be
	// matched. It is not the withheld count of the answers that withhold endpoint
	// edges: that one counts edges, this one counts entities.
	NotPrinted  int `json:"not_printed"`
	Retained    int `json:"retained"`
	Quarantined int `json:"quarantined"`
}

// FindEntities lists the ids of the entities of one type that existed in a window
// and whose name matches. It reads the ids from the store's listing read, which
// has no token, and decides existence and name at the answer's token, so a call
// checks at most findScanCap ids and says how to continue.
func (s *Source) FindEntities(ctx context.Context, in FindEntitiesInput) (FindEntitiesOutput, error) {
	req := s.newRequest()
	return run(s, ctx, true, req, func(ctx context.Context) (FindEntitiesOutput, error) { return s.find(ctx, in) })
}

// findArgs is a call's arguments, parsed.
type findArgs struct {
	typ       catalog.EntityType
	layer     catalog.Layer
	name, ns  string
	lowerName string
	filtered  bool
	from, to  time.Time
	limit     int
	after     identity.Fingerprint
}

// parseFind checks every argument, in this order, before anything is read.
func (s *Source) parseFind(in FindEntitiesInput) (findArgs, error) {
	var a findArgs
	typ, err := s.findType(in.Type)
	if err != nil {
		return a, err
	}
	a.typ = typ
	ent, _ := s.cat.Entity(typ)
	a.layer = ent.Layer()
	for _, t := range []struct{ arg, v string }{{"name", in.Name}, {"namespace", in.Namespace}} {
		if t.v != "" && (len(t.v) > findMaxText || !utf8.ValidString(t.v) || strings.TrimSpace(t.v) == "") {
			return a, invalid("The argument %q must be at most %d bytes of UTF-8 text with a character that is not a space.", t.arg, findMaxText)
		}
	}
	if in.Namespace != "" && typ == catalog.K8sNode {
		return a, invalid("Nodes have no namespace; leave \"namespace\" out.")
	}
	a.name, a.ns, a.lowerName = in.Name, in.Namespace, strings.ToLower(in.Name)
	a.filtered = a.name != "" || a.ns != ""
	if a.from, a.to, err = windowArgs(in.At, in.From, in.To); err != nil {
		return a, err
	}
	switch {
	case in.Limit == 0:
		a.limit = findDefaultLimit
	case in.Limit < 1 || in.Limit > findMaxLimit:
		return a, invalid("The argument \"limit\" must be between 1 and %d; got %d.", findMaxLimit, in.Limit)
	default:
		a.limit = in.Limit
	}
	if in.After != "" {
		fp, err := s.parseEntity("after", in.After)
		if err != nil {
			return a, err
		}
		if fp.Type() != typ {
			return a, invalid("The argument \"after\" must be an id of type %s, from the next_after of an earlier answer.", typ)
		}
		a.after = fp
	}
	return a, nil
}

// findType checks the entity type of a call.
func (s *Source) findType(v string) (catalog.EntityType, error) {
	typ := catalog.EntityType(v)
	if _, ok := s.cat.Entity(typ); !ok {
		return "", invalid("The argument \"type\" names the entity type %s, which is not in the catalog. The types are %s.", quote(v), s.typeList())
	}
	switch {
	case slices.Contains(findTypes, typ):
		return typ, nil
	case typ == catalog.NetworkEndpoint:
		return "", invalid("find_entities never lists endpoints: an endpoint's id reveals its address. Name one by its realm and address through resolve_entity.")
	case typ == catalog.Host:
		return "", invalid("Hosts are unnamed on purpose, so they cannot be found by name. A node's host is the target of its runs_on edge: ask neighbors_at of the node.")
	}
	return "", invalid("The type %s has no name, so it cannot be found by one. find_entities lists k8s.pod, k8s.node, container and service.", typ)
}

// The fate of one id a scan checked.
const (
	fateNone uint8 = iota // not a match
	fateMatch
	fateUndescribed
	fateNotPrinted
	fateRetained
	fateQuarantined
)

// checkedID is what a scan found of one id.
type checkedID struct {
	fate  uint8
	found FoundEntity
}

func (s *Source) find(ctx context.Context, in FindEntitiesInput) (FindEntitiesOutput, error) {
	var zero FindEntitiesOutput
	a, err := s.parseFind(in)
	if err != nil {
		return zero, err
	}
	if s.lister == nil {
		return zero, ErrNoListing
	}
	token, ahead, err := s.pin(in.AsOf)
	if err != nil {
		return zero, err
	}

	ids, err := s.lister.ListEntities(ctx, a.typ, a.after, findScanCap+1)
	if err != nil {
		return zero, err
	}
	more := len(ids) > findScanCap
	ids = head(ids, findScanCap)

	checked, stoppedAtLimit, err := s.scanFind(ctx, a, ids, token)
	if err != nil {
		return zero, err
	}
	n := len(checked)

	out := FindEntitiesOutput{
		Token: tokenString(token), Derived: true,
		Policy: Policy{Interval: interval.PolicyVersion, Rank: s.rankDigest, Names: NameTableVersion, Withhold: WithholdVersion, Find: FindVersion},
		Type:   string(a.typ), Window: windowOf(a.from, a.to), Matches: []FoundEntity{}, Notes: notesFor(ahead),
	}

	// The matches in id order, and where each was found.
	matches, pos, counts := foldChecked(checked, n)
	out.Matches = matches
	cut := false
	tl := listTail(&out.Matches)
	if _, err := fitAnswer(&out, maxAnswerBytes, namedTail{"matches", tl}); err != nil {
		return zero, err
	}
	alone := false
	if len(out.Matches) == 0 && len(matches) > 0 {
		// One match is always kept, so that the next page starts after it. If it does
		// not fit alone, its name and namespace are withheld below.
		out.Matches = matches[:1:1]
		alone = true
	}
	if kept := len(out.Matches); kept < len(matches) {
		// The cursor is the id before the first match that was dropped, so that the next
		// page starts with it.
		cut = true
		n = pos[kept]
		matches, _, counts = foldChecked(checked, n)
		out.Matches = matches
	}

	complete := !cut && !more && n == len(ids)
	out.Complete = complete
	if !complete {
		cursor := ids[n-1].String()
		out.NextAfter = &cursor
	}
	counts.Checked = n
	out.Counts = counts

	slices.SortFunc(out.Matches, compareFound)
	out.Counts.Matched = len(out.Matches)
	if alone {
		if err := withholdOversized(&out); err != nil {
			return zero, err
		}
	}

	out.Notes = append(out.Notes, s.findNotes(a, out, in.After != "", cut, stoppedAtLimit, token)...)
	return out, nil
}

// The per-match notes of a name or a namespace withheld because the match alone
// would not fit the answer cap.
const (
	nameTooLarge   = "name larger than the answer cap, withheld"
	nsTooLarge     = "namespace larger than the answer cap, withheld"
	nameNsTooLarge = "name and namespace larger than the answer cap, withheld"
)

// withholdOversized is the backstop behind the print rule, which bounds every name
// and namespace of the four types to 253 bytes, so that no text of theirs reaches
// the answer cap. It keeps the cap hard if some other text ever does: when the only
// match of an answer does not fit, the match stays, so that the cursor moves past
// it, with its id, type and existence, but its name or namespace, the larger first,
// is left out, with a note, until the answer fits. It never changes a match that
// fits.
func withholdOversized(out *FindEntitiesOutput) error {
	fits := func() (bool, error) {
		b, err := json.Marshal(out)
		return len(b) <= maxAnswerBytes-budgetSlack, err
	}
	if ok, err := fits(); ok || err != nil {
		return err
	}
	r := &out.Matches[0].Entity
	size := func(p *string) int {
		if p == nil {
			return 0
		}
		return len(*p)
	}
	steps := []string{"name", "namespace"}
	if size(r.Namespace) > size(r.Name) {
		steps = []string{"namespace", "name"}
	}
	var name, ns bool
	for _, step := range steps {
		switch {
		case step == "name" && r.Name != nil:
			r.Name, name = nil, true
		case step == "namespace" && r.Namespace != nil:
			r.Namespace, ns = nil, true
		default:
			continue
		}
		switch {
		case name && ns:
			r.NameNote = nameNsTooLarge
		case name:
			r.NameNote = nameTooLarge
		default:
			r.NameNote = nsTooLarge
		}
		if ok, err := fits(); ok || err != nil {
			return err
		}
	}
	return nil
}

// scanFind reads the existence and the name of ids, in order, in batches, at the
// token, and stops at the match that fills the limit. It returns what it found of
// each id it checked, and whether it stopped at the limit.
func (s *Source) scanFind(ctx context.Context, a findArgs, ids []identity.Fingerprint, token uint64) ([]checkedID, bool, error) {
	rd := interval.NewReader(s.st, interval.Options{Describe: true, Rank: s.rank})
	w := interval.Window{From: a.from, To: a.to, AsOf: token}
	checked := make([]checkedID, 0, len(ids))
	matched := 0
	for lo := 0; lo < len(ids); lo += findBatch {
		batch := ids[lo:min(lo+findBatch, len(ids))]
		els, retained, err := s.readBatch(ctx, rd, a.layer, batch, w)
		if err != nil {
			return nil, false, err
		}
		for i, fp := range batch {
			c := s.judge(a, fp, els[i], retained[i])
			checked = append(checked, c)
			if c.fate == fateMatch {
				matched++
			}
			if matched == a.limit {
				return checked, true, nil
			}
		}
	}
	return checked, false, nil
}

// readBatch reads a batch of ids in one call. If a description the call needs was
// recorded before the retention horizon, the whole call is refused, so the ids are
// read one by one, and each that is refused alone is marked retained. Any other
// error fails the call.
func (s *Source) readBatch(ctx context.Context, rd *interval.Reader, layer catalog.Layer, batch []identity.Fingerprint, w interval.Window) ([]interval.Element, []bool, error) {
	retained := make([]bool, len(batch))
	_, els, err := rd.Entities(ctx, batch, w)
	if err == nil {
		return els, retained, nil
	}
	if !errors.Is(err, interval.ErrRetained) {
		return nil, nil, inLayer(layer, err)
	}
	els = make([]interval.Element, len(batch))
	for i, fp := range batch {
		res, err := rd.Entity(ctx, fp, w)
		switch {
		case errors.Is(err, interval.ErrRetained):
			retained[i] = true
		case err != nil:
			return nil, nil, inLayer(layer, err)
		default:
			els[i] = res.Elements[0]
		}
	}
	return els, retained, nil
}

// judge decides what became of one id: whether it existed in the window, and
// whether its name matches.
func (s *Source) judge(a findArgs, fp identity.Fingerprint, el interval.Element, retained bool) checkedID {
	switch {
	case retained:
		return checkedID{fate: fateRetained}
	case el.Quarantine != nil:
		return checkedID{fate: fateQuarantined}
	case len(el.Pieces) == 0:
		return checkedID{fate: fateNone}
	}
	r := ref(fp)
	d := el.AtLast
	if d == nil {
		d = el.AtFrom
	}
	if d == nil {
		if a.filtered {
			return checkedID{fate: fateUndescribed}
		}
		r.NameNote = notPresent
	} else {
		var h held
		r.Name, r.Namespace, h = nameFields(fp.Type(), d.Payload, s.printRule())
		if r.Name != nil {
			r.NameFrom = string(d.Producer)
		}
		r.NameNote = h.note()
		// A value that is not printed is never matched, and is counted, as an
		// undescribed one is: a search cannot tell what the value says.
		notPrinted := false
		if a.name != "" {
			switch {
			case h.name:
				notPrinted = true
			case r.Name == nil || !strings.Contains(strings.ToLower(*r.Name), a.lowerName):
				return checkedID{fate: fateNone}
			}
		}
		if a.ns != "" {
			switch {
			case h.namespace:
				notPrinted = true
			case r.Namespace == nil || *r.Namespace != a.ns:
				return checkedID{fate: fateNone}
			}
		}
		if notPrinted {
			return checkedID{fate: fateNotPrinted}
		}
	}
	spans := pieceSpans(el.Pieces, a.to)
	return checkedID{fate: fateMatch, found: FoundEntity{
		Entity: r, Subject: SubjectAsserted, Basis: BasisDerived, Rule: RuleExistence,
		Class: el.Class.String(), Transitions: el.Transitions,
		Spans: head(spans, findMaxSpans), SpansTotal: len(spans),
	}}
}

// foldChecked collects what the first n ids found: the matches in id order, the
// position of each, and the counts other than Checked and Matched.
func foldChecked(checked []checkedID, n int) (matches []FoundEntity, pos []int, c FindCounts) {
	matches = []FoundEntity{}
	for i, x := range checked[:n] {
		switch x.fate {
		case fateMatch:
			matches = append(matches, x.found)
			pos = append(pos, i)
		case fateUndescribed:
			c.Undescribed++
		case fateNotPrinted:
			c.NotPrinted++
		case fateRetained:
			c.Retained++
		case fateQuarantined:
			c.Quarantined++
		}
	}
	return matches, pos, c
}

// compareFound orders matches by name (by bytes, none last), then namespace (none
// first), then id.
func compareFound(a, b FoundEntity) int {
	if c := compareNames(a.Entity.Name, b.Entity.Name, true); c != 0 {
		return c
	}
	if c := compareNames(a.Entity.Namespace, b.Entity.Namespace, false); c != 0 {
		return c
	}
	return store.CompareFingerprints(a.Entity.FP, b.Entity.FP)
}

// compareNames orders two optional strings by bytes; a missing one is last if
// nilLast, else first.
func compareNames(a, b *string, nilLast bool) int {
	switch {
	case a == nil && b == nil:
		return 0
	case a == nil:
		return cmpNil(nilLast)
	case b == nil:
		return -cmpNil(nilLast)
	}
	return bytes.Compare([]byte(*a), []byte(*b))
}

func cmpNil(nilLast bool) int {
	if nilLast {
		return 1
	}
	return -1
}

// matchWord is "match" for one and "matches" for any other number.
func matchWord(n int) string {
	if n == 1 {
		return "match"
	}
	return "matches"
}

// findNotes writes the notes after the sentence about an ahead token, in order.
func (s *Source) findNotes(a findArgs, out FindEntitiesOutput, hasAfter, cut, stoppedAtLimit bool, token uint64) []string {
	var notes []string
	asof := tokenString(token)
	cursor := ""
	if out.NextAfter != nil {
		cursor = *out.NextAfter
	}
	n := out.Counts.Checked
	switch {
	case cut:
		notes = append(notes, fmt.Sprintf("The answer was cut at %d KiB after %d %s, through %s. Pass after=%s and asof=%s to continue.", maxAnswerBytes>>10, len(out.Matches), matchWord(len(out.Matches)), cursor, cursor, asof))
	case out.Complete && hasAfter:
		notes = append(notes, fmt.Sprintf("Checked every %s id the store holds after the one given (%d).", a.typ, n))
	case out.Complete:
		notes = append(notes, fmt.Sprintf("Checked every %s id the store holds (%d).", a.typ, n))
	case stoppedAtLimit:
		notes = append(notes, fmt.Sprintf("Stopped at %d matches after checking %d %s ids, through %s. Pass after=%s and asof=%s to continue.", a.limit, n, a.typ, cursor, cursor, asof))
	default:
		notes = append(notes, fmt.Sprintf("Checked %d %s ids, in id order, through %s, and stopped at the scan cap of %d; more remain. Pass after=%s and asof=%s to continue.", n, a.typ, cursor, findScanCap, cursor, asof))
	}
	if out.Counts.Matched > 0 {
		notes = append(notes, "Matches are listed by name; the scan goes in id order.")
	}
	if k := out.Counts.Undescribed; k > 0 {
		notes = append(notes, fmt.Sprintf("%d %s entities existed in the window but have no description at either end of it, so their names are not known; a window that ends while they exist names them.", k, a.typ))
	}
	if k := out.Counts.NotPrinted; k > 0 {
		notes = append(notes, fmt.Sprintf("%d %s entities have a name or namespace that is not printed (over 253 bytes, or it holds a control character, a format character (Unicode Cf: zero-width, bidi controls, tag characters) or an endpoint id's text), so they could not be matched.", k, a.typ))
	}
	if k := out.Counts.Retained; k > 0 {
		notes = append(notes, fmt.Sprintf("%d had a description recorded before the retention horizon, which the store no longer keeps, so they could not be matched.", k))
	}
	if k := out.Counts.Quarantined; k > 0 {
		notes = append(notes, fmt.Sprintf("%d are quarantined and are not listed; entity_history shows why.", k))
	}
	return append(notes, passAsOf(token))
}
