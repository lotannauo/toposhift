package doltmodel

import (
	"fmt"

	"github.com/lotannauo/toposhift/bench/spike/engine"
)

// logRows are the rows of the edge log, or the one row of the entity log, that a record
// of the subject is stored as.
func (st *subject) logRows(p *producer, c rec) (edges []LogRow, ent *EntLogRow) {
	through, hasThrough := c.through, c.hasThrough
	if !hasThrough {
		through = 0
	}
	kind := EncodeKind(c.kind)
	payload := string(c.payload)
	if st.id.Kind == engine.SubjectEntity {
		return nil, &EntLogRow{
			Fp: string(EncodeFingerprint(st.id.A)), Producer: string(p.name), Layer: int(st.layer),
			EventNs: c.event, Seq: EncodeSeq(c.seq), Kind: kind, TTLNs: int64(c.ttl),
			HasThrough: hasThrough, ThroughNs: through, Payload: payload,
		}
	}
	a, b := string(EncodeFingerprint(st.id.A)), string(EncodeFingerprint(st.id.B))
	row := LogRow{
		Layer: int(st.layer), Rel: string(st.id.Relation), Producer: string(p.name),
		EventNs: c.event, Seq: EncodeSeq(c.seq), Kind: kind, TTLNs: int64(c.ttl),
		HasThrough: hasThrough, ThroughNs: through, Payload: payload,
	}
	fwd, rev := row, row
	fwd.Dir, fwd.A, fwd.Peer = int(engine.Forward), a, b
	rev.Dir, rev.A, rev.Peer = int(engine.Reverse), b, a
	return []LogRow{fwd, rev}, nil
}

func (st *subject) stateRows(p *producer, s stateRow) (edges []StateRow, ent *EntStateRow) {
	kind := EncodeKind(s.kind)
	if st.id.Kind == engine.SubjectEntity {
		return nil, &EntStateRow{
			Fp: string(EncodeFingerprint(st.id.A)), Producer: string(p.name), Layer: int(st.layer),
			EventNs: s.event, Seq: EncodeSeq(s.seq), Kind: kind, Open: s.open, DeadlineNs: s.deadline,
		}
	}
	a, b := string(EncodeFingerprint(st.id.A)), string(EncodeFingerprint(st.id.B))
	row := StateRow{
		Layer: int(st.layer), Rel: string(st.id.Relation), Producer: string(p.name),
		EventNs: s.event, Seq: EncodeSeq(s.seq), Kind: kind, Open: s.open, DeadlineNs: s.deadline,
	}
	fwd, rev := row, row
	fwd.Dir, fwd.A, fwd.Peer = int(engine.Forward), a, b
	rev.Dir, rev.A, rev.Peer = int(engine.Reverse), b, a
	return []StateRow{fwd, rev}, nil
}

// Tables returns the tables as they are now, in the order of their primary keys: the
// logs as retention left them and the state tables at the head. The model keeps the
// state as it goes; [DeriveState] derives it from the logs alone.
func (m *Model) Tables() Tables {
	m.mu.RLock()
	defer m.mu.RUnlock()
	t := Tables{Meta: Meta{LastSeq: EncodeSeq(m.lastSeq), HorizonNs: m.horizon, PastEnd: m.pastEnd, SchemaVersion: SchemaVersion}}
	for _, st := range m.subjects {
		for _, p := range st.prods {
			for _, c := range p.recs {
				edges, ent := st.logRows(p, c)
				t.Log = append(t.Log, edges...)
				if ent != nil {
					t.EntLog = append(t.EntLog, *ent)
				}
			}
			if s, ok := p.head(); ok {
				edges, ent := st.stateRows(p, s)
				t.State = append(t.State, edges...)
				if ent != nil {
					t.EntState = append(t.EntState, *ent)
				}
			}
		}
	}
	sortLog(t.Log)
	sortEntLog(t.EntLog)
	sortState(t.State)
	sortEntState(t.EntState)
	return t
}

// StateAt returns the state tables as the commit with the ordinal left them: what an
// AS OF read of that commit sees. It is an error in the arm with no commits, or for an
// ordinal that is no commit's.
func (m *Model) StateAt(ordinal int) ([]StateRow, []EntStateRow, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if m.arm != ArmCommit {
		return nil, nil, fmt.Errorf("doltmodel: the %s arm has no commits", m.arm)
	}
	if ordinal < 1 || ordinal > len(m.commits) {
		return nil, nil, fmt.Errorf("doltmodel: no commit %d among %d", ordinal, len(m.commits))
	}
	var state []StateRow
	var ents []EntStateRow
	for _, st := range m.subjects {
		for _, p := range st.prods {
			if s, ok := p.stateAt(ordinal); ok {
				edges, ent := st.stateRows(p, s)
				state = append(state, edges...)
				if ent != nil {
					ents = append(ents, *ent)
				}
			}
		}
	}
	sortState(state)
	sortEntState(ents)
	return state, ents, nil
}
