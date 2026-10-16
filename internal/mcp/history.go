package mcp

import (
	"context"

	"github.com/lotannauo/toposhift/internal/query/interval"
	"github.com/lotannauo/toposhift/internal/store"
)

// EntityHistoryInput is the input of the entity_history tool.
type EntityHistoryInput struct {
	// Entity is the id of the entity.
	Entity string `json:"entity"`
	// From and To are the window [from, to).
	From string `json:"from"`
	To   string `json:"to"`
	// AsOf is a token from an earlier answer, to read the same world again.
	AsOf string `json:"asof,omitempty"`
	// Limit is how many records to show, 1 to 500; the default is 50.
	Limit int `json:"limit,omitempty"`
}

// EntityHistoryOutput is the answer of the entity_history tool: when an entity
// existed in a window, which is derived, and the stored records that say so,
// which are facts.
type EntityHistoryOutput struct {
	Token   string `json:"token"`
	Derived bool   `json:"derived"`
	Policy  Policy `json:"policy"`
	Entity  Ref    `json:"entity"`
	Window  Window `json:"window"`
	// Existence is null on a store without range reads.
	Existence *Existence `json:"existence"`
	// Quarantined is set when the entity is quarantined, and then its existence
	// has no spans.
	Quarantined *Quarantine     `json:"quarantined"`
	Records     []HistoryRecord `json:"records"`
	Counts      HistoryCounts   `json:"counts"`
	Notes       []string        `json:"notes"`
}

// Existence is an entity's existence in the window, folded from its records at
// the token. It is derived.
type Existence struct {
	Subject     string `json:"subject"`
	Basis       string `json:"basis"`
	Rule        string `json:"rule"`
	Class       string `json:"class"`
	Transitions int    `json:"transitions"`
	Spans       []Span `json:"spans"`
	// DescriptionAtStart and DescriptionAtEnd are the merged descriptions, by
	// rank, at the window's first and last instants, where the entity exists.
	DescriptionAtStart *Described `json:"description_at_start"`
	DescriptionAtEnd   *Described `json:"description_at_end"`
}

// HistoryRecord is one stored record of the entity, as stored, and a fact. Both
// time axes are on it: event_time is when the statement was true, and seq is the
// ingest sequence number, which is the token that first sees it. Its payload is
// printed through the payload table.
type HistoryRecord struct {
	Basis          string `json:"basis"`
	Subject        string `json:"subject"`
	Layer          string `json:"layer"`
	EventTime      string `json:"event_time"`
	Seq            string `json:"seq"`
	Kind           string `json:"kind"`
	Producer       string `json:"producer"`
	TTL            string `json:"ttl"`
	Through        string `json:"through,omitempty"`
	EventTimeBasis string `json:"event_time_basis"`
	// Payload holds the allowed top-level keys; withheld_keys names the others.
	Payload      map[string]any `json:"payload,omitempty"`
	WithheldKeys []string       `json:"withheld_keys,omitempty"`
	PayloadNote  string         `json:"payload_note,omitempty"`
}

// HistoryCounts are the exact sizes of an answer.
type HistoryCounts struct {
	Records int `json:"records"`
	Shown   int `json:"records_shown"`
	Spans   int `json:"spans"`
}

// EntityHistory shows when an entity existed over a window, and the records that
// say so.
func (s *Source) EntityHistory(ctx context.Context, in EntityHistoryInput) (EntityHistoryOutput, error) {
	req := s.newRequest()
	req.nameText(in.Entity)
	return run(s, ctx, false, req, func(ctx context.Context) (EntityHistoryOutput, error) {
		var zero EntityHistoryOutput
		fp, err := s.parseEntity("entity", in.Entity)
		if err != nil {
			return zero, err
		}
		from, to, err := rangeArgs(in.From, in.To)
		if err != nil {
			return zero, err
		}
		limit, err := parseLimit(in.Limit)
		if err != nil {
			return zero, err
		}
		token, ahead, err := s.pin(in.AsOf)
		if err != nil {
			return zero, err
		}
		ent, _ := s.cat.Entity(fp.Type())

		out := EntityHistoryOutput{
			Token: tokenString(token), Derived: s.rangeReads,
			Policy: Policy{
				Interval: interval.PolicyVersion, Rank: s.rankDigest, Names: NameTableVersion, Payload: PayloadTableVersion, Withhold: WithholdVersion,
			},
			Entity: ref(fp), Window: windowOf(from, to), Records: []HistoryRecord{}, Notes: notesFor(ahead),
		}

		if s.rangeReads {
			rd := interval.NewReader(s.st, interval.Options{Describe: true, Rank: s.rank})
			res, err := rd.Entity(ctx, fp, interval.Window{From: from, To: to, AsOf: token})
			if err != nil {
				return zero, err
			}
			el := res.Elements[0]
			out.Existence = &Existence{
				Subject: SubjectAsserted, Basis: BasisDerived, Rule: RuleExistence,
				Class: el.Class.String(), Transitions: el.Transitions, Spans: pieceSpans(el.Pieces, to),
			}
			if d := el.AtFrom; d != nil {
				out.Existence.DescriptionAtStart = describe(fp.Type(), string(d.Producer), d.Payload)
			}
			if d := el.AtLast; d != nil {
				out.Existence.DescriptionAtEnd = describe(fp.Type(), string(d.Producer), d.Payload)
			}
			if el.Quarantine != nil {
				q := quarantineOf(el.Quarantine)
				out.Quarantined = &q
			}
			out.Counts.Spans = len(el.Pieces)
		} else {
			out.Notes = append(out.Notes, "Existence intervals need a range read, which this store does not offer; the stored records are shown.")
		}

		recs, err := s.st.EntityWindow(ctx, fp, from, to, store.Scope{Layer: ent.Layer(), AsOf: token})
		if err != nil {
			return zero, inLayer(ent.Layer(), err)
		}
		for _, r := range head(recs, limit) {
			out.Records = append(out.Records, historyRecord(r))
		}
		out.Counts.Records, out.Counts.Shown = len(recs), len(out.Records)
		if len(out.Records) < len(recs) {
			out.Notes = append(out.Notes, shown(len(out.Records), len(recs), "records", "oldest first"))
		}

		err = s.finish(ctx, &out, &out.Notes, token, from, to,
			namedTail{"records", listTail(&out.Records)})
		if err != nil {
			return zero, err
		}
		out.Counts.Shown = len(out.Records)
		return out, nil
	})
}

// historyRecord prints a stored record. The boot id of a host observation is not
// printed: boot ids are shown only in a quarantine.
func historyRecord(r store.Record) HistoryRecord {
	out := HistoryRecord{
		Basis: BasisFact, Subject: SubjectAsserted, Layer: r.Layer.String(),
		EventTime: fmtTime(r.EventTime), Seq: tokenString(r.Seq), Kind: r.Kind.String(),
		Producer: string(r.Producer), TTL: r.TTL.String(), EventTimeBasis: r.EventTimeBasis.String(),
	}
	if !r.Through.IsZero() {
		out.Through = fmtTime(r.Through)
	}
	if len(r.Payload) > 0 {
		out.Payload, out.WithheldKeys, out.PayloadNote = filterPayload(r.Subject.A.Type(), r.Payload)
	}
	return out
}
