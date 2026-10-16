package mcp

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"slices"
	"time"

	"github.com/lotannauo/toposhift/internal/catalog"
	"github.com/lotannauo/toposhift/internal/identity"
	"github.com/lotannauo/toposhift/internal/query/interval"
	"github.com/lotannauo/toposhift/internal/store"
)

// NeighborsAtInput is the input of the neighbors_at tool.
type NeighborsAtInput struct {
	// Entity is the id of the entity.
	Entity string `json:"entity"`
	// At is the instant, RFC 3339 with a zone.
	At string `json:"at"`
	// Direction is "forward" (the edges the entity is the source of), "reverse"
	// (the edges it is the target of) or "both"; the default is "both".
	Direction string `json:"direction,omitempty"`
	// AsOf is a token from an earlier answer, to read the same world again.
	AsOf string `json:"asof,omitempty"`
}

// NeighborsAtOutput is the answer of the neighbors_at tool: the edges of an
// entity that exist at one instant, each only while both its ends exist.
type NeighborsAtOutput struct {
	Token       string         `json:"token"`
	Derived     bool           `json:"derived"`
	Policy      Policy         `json:"policy"`
	Entity      Ref            `json:"entity"`
	At          string         `json:"at"`
	Direction   string         `json:"direction"`
	Edges       []NeighborEdge `json:"edges"`
	Quarantined []Quarantined  `json:"quarantined"`
	Counts      NeighborCounts `json:"counts"`
	Notes       []string       `json:"notes"`
}

// NeighborEdge is one edge at the instant, seen from the entity.
type NeighborEdge struct {
	// Direction is "forward" when the entity is the source of the edge and
	// "reverse" when it is the target.
	Direction string `json:"direction"`
	Relation  string `json:"relation"`
	Layer     string `json:"layer"`
	Peer      Ref    `json:"peer"`
	Subject   string `json:"subject"`
	Basis     string `json:"basis"`
	Rule      string `json:"rule"`
	// Quarantined is the quarantine of an end of the edge, the source's if both
	// are. The edge is then listed as its producers asserted it, without being cut
	// by the existence of its ends.
	Quarantined *Quarantine `json:"quarantined,omitempty"`
	Note        string      `json:"note,omitempty"`
}

// NeighborCounts are the exact sizes of an answer.
//
// Edges is the number listed: it leaves out the edges withheld because they reach
// an endpoint the request did not name, which Withheld counts. The edges listed
// and the edges withheld together are the edges of the interval package over the
// instant.
type NeighborCounts struct {
	Edges    int              `json:"edges"`
	Shown    int              `json:"edges_shown"`
	Withheld NeighborWithheld `json:"withheld"`
}

// NeighborsAt lists the edges of an entity at one instant. It reads points, not
// ranges, so it runs on any store. Its answer equals the edges of the interval
// package over the window [at, at+1ns) with the cascade on.
func (s *Source) NeighborsAt(ctx context.Context, in NeighborsAtInput) (NeighborsAtOutput, error) {
	req := s.newRequest()
	req.nameText(in.Entity)
	return run(s, ctx, false, req, func(ctx context.Context) (NeighborsAtOutput, error) {
		var zero NeighborsAtOutput
		fp, err := s.parseEntity("entity", in.Entity)
		if err != nil {
			return zero, err
		}
		at, err := parseTime("at", in.At)
		if err != nil {
			return zero, err
		}
		dirName := in.Direction
		if dirName == "" {
			dirName = "both"
		}
		var dirs []store.Direction
		switch dirName {
		case "forward":
			dirs = []store.Direction{store.Forward}
		case "reverse":
			dirs = []store.Direction{store.Reverse}
		case "both":
			dirs = []store.Direction{store.Forward, store.Reverse}
		default:
			return zero, invalid("The argument \"direction\" must be \"forward\", \"reverse\" or \"both\"; got %s.", quote(dirName))
		}
		token, ahead, err := s.pin(in.AsOf)
		if err != nil {
			return zero, err
		}

		found, err := s.edgesAt(ctx, fp, dirs, at, token)
		if err != nil {
			return zero, err
		}
		state, err := s.existenceAt(ctx, fp, found, at, token)
		if err != nil {
			return zero, err
		}

		out := NeighborsAtOutput{
			Token: tokenString(token), Derived: true,
			Policy: Policy{
				Interval: interval.PolicyVersion, Rank: s.rankDigest, Names: NameTableVersion, Withhold: WithholdVersion,
			},
			Entity: ref(fp), At: fmtTime(at), Direction: dirName,
			Edges: []NeighborEdge{}, Quarantined: []Quarantined{}, Notes: notesFor(ahead),
		}
		var withheld NeighborWithheld
		for _, f := range found {
			src, dst := f.ends(fp)
			// The rules of the interval package: a quarantined source lists the edge
			// uncut, even when the other end is not alive; otherwise a quarantined
			// target does; otherwise the edge is listed only while both ends are alive.
			var q *store.QuarantineError
			switch {
			case state[src].quarantine != nil:
				q = state[src].quarantine
			case state[dst].quarantine != nil:
				q = state[dst].quarantine
			case !state[src].alive || !state[dst].alive:
				continue
			}
			// Counted after the cut above, so that the edges listed and the edges
			// withheld together are the edges of the interval package.
			if req.hides(f.peer) {
				withheld.Edges++
				continue
			}
			e := NeighborEdge{
				Direction: f.dir.String(), Relation: string(f.rel), Layer: f.layer.String(), Peer: ref(f.peer),
				Subject: subjectOfRelation(f.rel), Basis: BasisDerived, Rule: RuleExistence,
			}
			if q != nil {
				qq := quarantineOf(q)
				e.Quarantined = &qq
				e.Note = "An end of the edge is quarantined, so the edge is shown as its producers asserted it, without cutting it by the existence of its ends."
			}
			out.Edges = append(out.Edges, e)
		}
		for _, x := range slices.SortedFunc(mapKeys(state), store.CompareFingerprints) {
			if q := state[x].quarantine; q != nil {
				if req.hides(x) {
					withheld.Quarantined++
					continue
				}
				e := Quarantined{
					Kind: "entity", Subject: SubjectAsserted, Basis: BasisDerived, Rule: RuleExistence,
					Quarantined: quarantineOf(q),
					Note:        "The entity is quarantined: two boots of it were live at once, so its existence is not folded.",
				}
				r := ref(x)
				e.Entity = &r
				out.Quarantined = append(out.Quarantined, e)
			}
		}
		out.Counts = NeighborCounts{Edges: len(out.Edges), Shown: len(out.Edges), Withheld: withheld}
		if note := neighborWithheldNote(withheld); note != "" {
			out.Notes = append(out.Notes, note)
		}

		err = s.finish(ctx, &out, &out.Notes, token, at, at.Add(time.Nanosecond),
			namedTail{"edges", listTail(&out.Edges)})
		if err != nil {
			return zero, err
		}
		out.Counts.Shown = len(out.Edges)
		return out, nil
	})
}

func mapKeys[V any](m map[identity.Fingerprint]V) func(func(identity.Fingerprint) bool) {
	return func(yield func(identity.Fingerprint) bool) {
		for k := range m {
			if !yield(k) {
				return
			}
		}
	}
}

// foundEdge is a stored edge alive at the instant, with the entity read from.
type foundEdge struct {
	dir   store.Direction
	layer catalog.Layer
	peer  identity.Fingerprint
	rel   catalog.RelationType
}

// ends are the source and the target of the edge, given the entity it was read from.
func (f foundEdge) ends(self identity.Fingerprint) (src, dst identity.Fingerprint) {
	if f.dir == store.Forward {
		return self, f.peer
	}
	return f.peer, self
}

// edgesAt reads the edges alive at the instant, per direction and per layer of
// interval.EdgeLayers, all at the one token. A stored edge that disagrees with
// the catalog is an error, as the interval package refuses it.
func (s *Source) edgesAt(ctx context.Context, fp identity.Fingerprint, dirs []store.Direction, at time.Time, token uint64) ([]foundEdge, error) {
	var out []foundEdge
	for _, dir := range dirs {
		var forDir []foundEdge
		for _, layer := range interval.EdgeLayers(s.cat, fp.Type(), dir) {
			ns, err := s.st.Neighbors(ctx, fp, dir, at, store.Scope{Layer: layer, AsOf: token})
			if err != nil {
				return nil, inLayer(layer, err)
			}
			for _, n := range ns {
				f := foundEdge{dir: dir, layer: layer, peer: n.Peer, rel: n.Relation}
				src, dst := f.ends(fp)
				if err := s.checkEdge(src, dst, n.Relation, layer); err != nil {
					return nil, err
				}
				forDir = append(forDir, f)
			}
		}
		slices.SortFunc(forDir, func(a, b foundEdge) int {
			if c := store.CompareFingerprints(a.peer, b.peer); c != 0 {
				return c
			}
			return cmp.Compare(a.rel, b.rel)
		})
		out = append(out, forDir...)
	}
	return out, nil
}

// checkEdge refuses a stored edge that disagrees with the catalog: a writer has
// drifted from it, and dropping the edge would hide that.
func (s *Source) checkEdge(src, dst identity.Fingerprint, relType catalog.RelationType, layer catalog.Layer) error {
	why := ""
	rel, ok := s.cat.Relation(relType)
	switch {
	case !ok:
		why = fmt.Sprintf("the relation %q is not registered", relType)
	case rel.Derived():
		why = fmt.Sprintf("the relation %q is derived and never stored", relType)
	case rel.Layer() != layer:
		why = fmt.Sprintf("the relation %q is stored in layer %s", relType, rel.Layer())
	case !rel.Allows(src.Type(), dst.Type()):
		why = fmt.Sprintf("the relation %q does not connect %s to %s", relType, src.Type(), dst.Type())
	default:
		return nil
	}
	return &Error{
		Message: fmt.Sprintf("A stored edge disagrees with the catalog: %s %s %s was read in layer %s, but %s.", src.Type(), relType, dst.Type(), layer, why),
		Cause:   store.ErrInvalid,
	}
}

// existence is whether an entity exists at the instant, or its quarantine.
type existence struct {
	alive      bool
	quarantine *store.QuarantineError
}

// existenceAt reads whether the entity and each distinct peer exist at the
// instant, each in its own entity layer, at the one token.
func (s *Source) existenceAt(ctx context.Context, fp identity.Fingerprint, found []foundEdge, at time.Time, token uint64) (map[identity.Fingerprint]existence, error) {
	fps := []identity.Fingerprint{fp}
	for _, f := range found {
		fps = append(fps, f.peer)
	}
	slices.SortFunc(fps, store.CompareFingerprints)
	fps = slices.Compact(fps)

	out := make(map[identity.Fingerprint]existence, len(fps))
	for _, x := range fps {
		ent, ok := s.cat.Entity(x.Type())
		if !ok {
			return nil, &Error{
				Message: fmt.Sprintf("A stored edge reaches %s, whose type is not in the catalog.", x),
				Cause:   store.ErrInvalid,
			}
		}
		alive, err := s.st.Alive(ctx, x, at, store.Scope{Layer: ent.Layer(), AsOf: token})
		var qe *store.QuarantineError
		switch {
		case errors.As(err, &qe):
			out[x] = existence{quarantine: qe}
		case err != nil:
			return nil, inLayer(ent.Layer(), err)
		default:
			out[x] = existence{alive: alive}
		}
	}
	return out, nil
}
