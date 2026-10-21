package doltengine_test

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/lotannauo/toposhift/bench/dolt/doltengine"
	"github.com/lotannauo/toposhift/bench/dolt/doltmodel"
	"github.com/lotannauo/toposhift/bench/spike/engine"
	"github.com/lotannauo/toposhift/internal/catalog"
	"github.com/lotannauo/toposhift/internal/identity"
	"github.com/lotannauo/toposhift/internal/lifecycle"
)

// The engine is held to the model statement for statement: a pair applies every write and
// every retention to a [doltmodel.Model] and to an engine over a live server, and asks both
// every read of a set of probes, comparing the answers, the counters of the reads and the
// writes, the rows of every table and of the state as each commit left it, and the commits.
// The model is the pre-registered rule; the engine is held to it, not the other way round.

func fp(typ catalog.EntityType, b byte) identity.Fingerprint {
	var h [identity.FingerprintBytes]byte
	h[0] = b
	f, err := identity.FingerprintFromHash(typ, h)
	if err != nil {
		panic(err)
	}
	return f
}

var (
	pod1, pod2, node1 = fp("k8s.pod", 1), fp("k8s.pod", 2), fp("k8s.node", 3)

	edge1 = engine.EdgeSubject(pod1, node1, catalog.ScheduledOn)
	edge2 = engine.EdgeSubject(pod2, node1, catalog.ScheduledOn)
	edge3 = engine.EdgeSubject(pod1, node1, "part_of")
)

// at is an instant a number of seconds after the epoch.
func at(s int64) time.Time { return time.Unix(s, 0).UTC() }

func observe(s engine.Subject, seq uint64, p lifecycle.Producer, secs, ttl int64) engine.Record {
	return engine.Record{
		Layer: catalog.L2, Subject: s, Producer: p, EventTime: at(secs), Seq: seq,
		Kind: lifecycle.Observe, TTL: time.Duration(ttl) * time.Second, Payload: []byte{byte(seq)},
	}
}

func del(s engine.Subject, seq uint64, p lifecycle.Producer, secs int64) engine.Record {
	return engine.Record{Layer: catalog.L2, Subject: s, Producer: p, EventTime: at(secs), Seq: seq, Kind: lifecycle.Delete}
}

// existence is an Observe of an entity's existence, in the layer of its type.
func existence(f identity.Fingerprint, seq uint64, p lifecycle.Producer, secs, ttl int64) engine.Record {
	e, _ := catalog.Default().Entity(f.Type())
	return engine.Record{
		Layer: e.Layer(), Subject: engine.EntitySubject(f), Producer: p, EventTime: at(secs), Seq: seq,
		Kind: lifecycle.Observe, TTL: time.Duration(ttl) * time.Second, Payload: []byte{byte(seq)},
	}
}

// disagreement is the error of a write that the model and the engine did not treat alike. It
// wraps neither, so that a check for a refusal is not satisfied by it.
type disagreement struct{ model, engine error }

func (d *disagreement) Error() string {
	return fmt.Sprintf("write: the model said %v, the engine %v", d.model, d.engine)
}

// pair is a model and an engine that are given the same writes and reads.
type pair struct {
	m *doltmodel.Model
	e *doltengine.Engine
	// dsn is the server, for reading its own commit log.
	dsn string

	records []engine.Record
	// horizon is the latest retention horizon, and floor the token at which it was made: a
	// read before the horizon, or as of a token below the floor, is not specified.
	horizon time.Time
	floor   uint64
}

func newPair(t testing.TB, e *doltengine.Engine, arm doltmodel.Arm) *pair {
	return &pair{
		m:   doltmodel.New(doltmodel.WithArm(arm), doltmodel.WithPayloadCapacity(doltmodel.RegisteredPayloadCapacity)),
		e:   e,
		dsn: serverDSN(t),
	}
}

func clone(rs []engine.Record) []engine.Record {
	out := slices.Clone(rs)
	for i := range out {
		out[i].Payload = slices.Clone(out[i].Payload)
	}
	return out
}

// write gives both the batch and requires them to agree: both accept it, or both refuse it
// for the same reason.
func (p *pair) write(rs ...engine.Record) error {
	merr, eerr := p.m.Write(clone(rs)), p.e.Write(clone(rs))
	disagree := (merr == nil) != (eerr == nil)
	for _, class := range []error{engine.ErrInvalid, engine.ErrBeforeHorizon} {
		disagree = disagree || errors.Is(merr, class) != errors.Is(eerr, class)
	}
	if disagree {
		return &disagreement{model: merr, engine: eerr}
	}
	if merr == nil {
		p.records = append(p.records, rs...)
	}
	return merr
}

func (p *pair) retain(h time.Time) error {
	if err := p.m.Retain(h); err != nil {
		return err
	}
	if err := p.e.Retain(h); err != nil {
		return fmt.Errorf("retain: %w", err)
	}
	if h.After(p.horizon) {
		p.horizon, p.floor = h, p.m.LastSeq()
	}
	return nil
}

// probes are the reads a check asks.
type probes struct {
	fps      []identity.Fingerprint
	layers   []catalog.Layer
	instants []time.Time
	tokens   []uint64
}

// probesFor are the reads that tell the rules apart: the instants of every record, a
// nanosecond either side, the ends of each record's life (through, deadline), and the
// extremes; every token at which something was committed and the tokens inside batches.
func (p *pair) probesFor() probes {
	pr := probes{
		fps:    []identity.Fingerprint{pod1, pod2, node1},
		layers: []catalog.Layer{catalog.L2, catalog.L1, catalog.L3},
	}
	seen := map[time.Time]bool{}
	add := func(t time.Time) {
		for _, d := range []time.Duration{-1, 0, 1} {
			x := t.Add(d)
			if !seen[x] && !x.Before(p.horizon) {
				seen[x] = true
				pr.instants = append(pr.instants, x)
			}
		}
	}
	for _, r := range p.records {
		add(r.EventTime)
		if !r.Through.IsZero() {
			add(r.Through)
		}
		if r.TTL > 0 {
			last := r.EventTime
			if r.Through.After(last) {
				last = r.Through
			}
			add(last.Add(r.TTL))
		}
	}
	add(p.horizon)
	for _, t := range []time.Time{at(0), engine.MaxEventTime, engine.MaxEventTime.Add(time.Hour)} {
		if !seen[t] && !t.Before(p.horizon) {
			seen[t] = true
			pr.instants = append(pr.instants, t)
		}
	}
	slices.SortFunc(pr.instants, func(a, b time.Time) int { return a.Compare(b) })

	tokens := map[uint64]bool{engine.Latest: true}
	for _, c := range p.m.Commits() {
		for _, tok := range []uint64{c.Token, c.Token - 1} {
			if tok >= p.floor && c.Token > 0 {
				tokens[tok] = true
			}
		}
	}
	for _, r := range p.records {
		if r.Seq >= p.floor {
			tokens[r.Seq] = true
		}
	}
	pr.tokens = slices.Sorted(maps.Keys(tokens))
	return pr
}

// readsAgree asks the model and the engine every probe and compares the answers.
func (p *pair) readsAgree(pr probes) error {
	for _, token := range pr.tokens {
		for _, layer := range pr.layers {
			sc := engine.Scope{Layer: layer, AsOf: token}
			for _, t := range pr.instants {
				for _, f := range pr.fps {
					wa, err := p.m.Alive(f, t, sc)
					if err != nil {
						return err
					}
					ga, err := p.e.Alive(f, t, sc)
					if err != nil {
						return fmt.Errorf("alive %s at %s as of %d in %s: %w", f, t.Format(time.RFC3339Nano), token, layer, err)
					}
					if wa != ga {
						return fmt.Errorf("alive %s at %s as of %d in %s: the engine %v, the model %v", f, t.Format(time.RFC3339Nano), token, layer, ga, wa)
					}
					for _, dir := range []engine.Direction{engine.Forward, engine.Reverse} {
						want, err := p.m.Neighbors(f, dir, t, sc)
						if err != nil {
							return err
						}
						got, err := p.e.Neighbors(f, dir, t, sc)
						if err != nil {
							return fmt.Errorf("neighbors %s %s at %s as of %d in %s: %w", f, dir, t.Format(time.RFC3339Nano), token, layer, err)
						}
						if !slices.Equal(got, want) {
							return fmt.Errorf("neighbors %s %s at %s as of %d in %s: the engine %v, the model %v", f, dir, t.Format(time.RFC3339Nano), token, layer, got, want)
						}
					}
				}
				// A batch with a repeat and every entity: one snapshot, and an empty list for an
				// entity with no edges.
				batch := []identity.Fingerprint{node1, pod1, pod2, node1, fp("k8s.pod", 99)}
				for _, dir := range []engine.Direction{engine.Forward, engine.Reverse} {
					want, err := p.m.NeighborsBatch(batch, dir, t, sc)
					if err != nil {
						return err
					}
					got, err := p.e.NeighborsBatch(batch, dir, t, sc)
					if err != nil {
						return fmt.Errorf("batch %s at %s as of %d in %s: %w", dir, t.Format(time.RFC3339Nano), token, layer, err)
					}
					if len(got) != len(want) {
						return fmt.Errorf("batch %s at %s as of %d: %d lists, the model %d", dir, t.Format(time.RFC3339Nano), token, len(got), len(want))
					}
					for i := range want {
						if !slices.Equal(got[i], want[i]) {
							return fmt.Errorf("batch %s list %d at %s as of %d in %s: the engine %v, the model %v", dir, i, t.Format(time.RFC3339Nano), token, layer, got[i], want[i])
						}
					}
				}
			}
			// Windows: the whole range, and each gap between two instants.
			var spans [][2]time.Time
			spans = append(spans, [2]time.Time{at(0), engine.MaxEventTime.Add(time.Hour)})
			for i := 0; i+1 < len(pr.instants) && i < 12; i++ {
				spans = append(spans, [2]time.Time{pr.instants[i], pr.instants[i+1]})
			}
			for _, sp := range spans {
				if sp[0].Before(p.horizon) {
					continue
				}
				for _, f := range pr.fps {
					for _, dir := range []engine.Direction{engine.Forward, engine.Reverse} {
						want, err := p.m.Window(f, dir, sp[0], sp[1], sc)
						if err != nil {
							return err
						}
						got, err := p.e.Window(f, dir, sp[0], sp[1], sc)
						if err != nil {
							return fmt.Errorf("window: %w", err)
						}
						if d := diffRecords(got, want); d != "" {
							return fmt.Errorf("window %s %s [%s, %s) as of %d in %s: %s", f, dir, sp[0].Format(time.RFC3339Nano), sp[1].Format(time.RFC3339Nano), token, layer, d)
						}
					}
				}
			}
		}
	}
	return nil
}

func diffRecords(got, want []engine.Record) string {
	if len(got) != len(want) {
		return fmt.Sprintf("the engine has %d records, the model %d", len(got), len(want))
	}
	for i := range want {
		g, w := got[i], want[i]
		if g.Layer != w.Layer || g.Subject != w.Subject || g.Producer != w.Producer || !g.EventTime.Equal(w.EventTime) || g.Seq != w.Seq ||
			g.Kind != w.Kind || g.TTL != w.TTL || !g.Through.Equal(w.Through) || string(g.Payload) != string(w.Payload) {
			return fmt.Sprintf("record %d: the engine %+v, the model %+v", i, g, w)
		}
	}
	return ""
}

// storesAgree compares what is stored and what was counted: the rows of every table, the
// state as of each commit, the commits, the writer's counters, and the paths of the reads.
func (p *pair) storesAgree(ctx context.Context) error {
	got, err := p.e.Tables(ctx)
	if err != nil {
		return err
	}
	if d := got.FirstDifference(p.m.Tables()); d != "" {
		return fmt.Errorf("the tables differ from the model's: %s", d)
	}
	if got.Digest() != p.m.Tables().Digest() {
		return errors.New("the tables' digests differ")
	}
	mc, ec := p.m.Commits(), p.e.Commits()
	if len(mc) != len(ec) {
		return fmt.Errorf("the engine made %d commits, the model %d", len(ec), len(mc))
	}
	for i := range mc {
		m, e := mc[i], ec[i]
		if m.Ordinal != e.Ordinal || m.Kind != e.Kind || m.Token != e.Token || m.Message != e.Message || !m.Date.Equal(e.Date) {
			return fmt.Errorf("commit %d: the engine %+v, the model %+v", i+1, e, m)
		}
		if p.m.Arm() == doltmodel.ArmCommit {
			ws, wents, err := p.m.StateAt(m.Ordinal)
			if err != nil {
				return err
			}
			gs, gents, err := p.e.StateAt(ctx, m.Ordinal)
			if err != nil {
				return err
			}
			if !slices.Equal(gs, ws) || !slices.Equal(gents, wents) {
				return fmt.Errorf("the state as of commit %d differs from the model's", m.Ordinal)
			}
		}
	}
	if err := p.commitLogAgrees(ctx); err != nil {
		return err
	}
	if m, e := p.m.Counts(), p.e.Counts(); m != e {
		return fmt.Errorf("writer counts: the engine %+v, the model %+v", e, m)
	}
	if m, e := p.m.Paths(), p.e.Paths(); !maps.Equal(m, e) {
		return fmt.Errorf("the reads were settled differently:\n the engine %v\n the model  %v", e, m)
	}
	if p.m.LastSeq() != p.e.LastSeq() {
		return fmt.Errorf("LastSeq: the engine %d, the model %d", p.e.LastSeq(), p.m.LastSeq())
	}
	return nil
}

// commitLogAgrees compares the engine's commits with the server's own log of them: each has the
// engine's hash, message and synthetic date, and the synthetic author, and nothing else is in
// the log but the commit the server made when it created the database.
func (p *pair) commitLogAgrees(ctx context.Context) error {
	log, err := connect(p.dsn, p.e.Database())
	if err != nil {
		return err
	}
	defer func() { _ = log.Close() }()
	rows, err := log.QueryContext(ctx, "SELECT commit_hash, committer, email, date, message FROM dolt_log ORDER BY date ASC, commit_order ASC")
	if err != nil {
		return err
	}
	defer func() { _ = rows.Close() }()
	type entry struct{ hash, who, email, date, msg string }
	var got []entry
	for rows.Next() {
		var e entry
		if err := rows.Scan(&e.hash, &e.who, &e.email, &e.date, &e.msg); err != nil {
			return err
		}
		got = append(got, e)
	}
	if err := rows.Err(); err != nil {
		return err
	}
	var ours []entry
	for _, e := range got {
		if e.msg != doltengine.InitMessage {
			ours = append(ours, e)
		}
	}
	commits := p.e.Commits()
	if len(ours) != len(commits) {
		return fmt.Errorf("the server's log has %d commits besides its own, the engine made %d: %v", len(ours), len(commits), got)
	}
	for i, c := range commits {
		e := ours[i]
		if e.hash != c.Hash || e.msg != c.Message || e.who != doltengine.AuthorName || e.email != doltengine.AuthorEmail || !strings.HasPrefix(e.date, c.Date.Format("2006-01-02 15:04:05")) {
			return fmt.Errorf("commit %d: the server's log has %+v, the engine %+v", c.Ordinal, e, c)
		}
	}
	return nil
}

// agree is both: every read of the probes, then what is stored.
func (p *pair) agree(ctx context.Context) error {
	if err := p.readsAgree(p.probesFor()); err != nil {
		return err
	}
	return p.storesAgree(ctx)
}
