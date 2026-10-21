package xtengine

import (
	"cmp"
	"context"
	"fmt"
	"slices"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/lotannauo/toposhift/bench/spike/engine"
	"github.com/lotannauo/toposhift/bench/xtdb/xtmodel"
	"github.com/lotannauo/toposhift/internal/catalog"
	"github.com/lotannauo/toposhift/internal/identity"
	"github.com/lotannauo/toposhift/internal/lifecycle"
)

// load reads back what an engine left under the prefix: from the meta documents, the
// token, the ordinal of the last transaction, the system time each token names and the
// retention horizon; from the log tables, the layer of each subject and what the
// planner remembers.
func (e *Engine) load(ctx context.Context) error {
	type metaDoc struct {
		id, last, sysUs, horizon, past int64
		kind                           string
	}
	var docs []metaDoc
	var q sqlText
	q.s("SELECT _id, kind, last_seq, sys_us, horizon_ns, past_end FROM " + e.names.of(xtmodel.Meta) + " FOR VALID_TIME ALL")
	err := scanRows(ctx, e.w, q.stmt(), func(rows pgx.Rows) error {
		var d metaDoc
		if err := rows.Scan(&d.id, &d.kind, &d.last, &d.sysUs, &d.horizon, &d.past); err != nil {
			return err
		}
		docs = append(docs, d)
		return nil
	})
	if err != nil {
		return fmt.Errorf("xtengine: reading the meta documents: %w", err)
	}
	// The documents of one engine are the transactions it made, ordinals 1 to the last, each
	// at the base plus its ordinal, in an order in which the sequence number only rises at a
	// batch and a retention leaves it as it was.
	slices.SortFunc(docs, func(a, b metaDoc) int { return cmp.Compare(a.id, b.id) })
	var base int64
	var prev uint64
	for i, d := range docs {
		n := uint64(i) + 1
		last := seqFromDB(d.last)
		switch {
		case d.id != int64(n):
			return fmt.Errorf("xtengine: the meta documents have a gap: the %dth is ordinal %d: the tables are not an engine's", n, d.id)
		case i == 0:
			base = d.sysUs - d.id
		case d.sysUs-d.id != base:
			return fmt.Errorf("xtengine: transaction %d is at system time base %d µs, the others at %d", n, d.sysUs-d.id, base)
		}
		switch d.kind {
		case metaBatch:
			if last <= prev {
				return fmt.Errorf("xtengine: batch %d ends at sequence number %d, not above %d", n, last, prev)
			}
		case metaRetain:
			if last != prev {
				return fmt.Errorf("xtengine: retention %d is at sequence number %d, the last batch ended at %d", n, last, prev)
			}
		default:
			return fmt.Errorf("xtengine: a meta document %d of kind %q", d.id, d.kind)
		}
		prev = last
		e.ordinal = n
		e.lastSeq = last
		if last > 0 {
			e.tokens[last] = n
		}
		e.horizon = max(e.horizon, d.horizon)
		e.pastEnd = e.pastEnd || d.past != 0
	}
	if err := e.setBase(base, len(docs) > 0); err != nil {
		return err
	}

	rb := xtmodel.NewRebuilder()
	for _, edges := range []bool{true, false} {
		if err := e.loadLog(ctx, rb, edges); err != nil {
			return err
		}
	}
	e.pl = rb.Planner(history{e})
	return nil
}

// loadLog reads every record of the forward edge log, or of the entity log, into the
// rebuilder and the layers.
func (e *Engine) loadLog(ctx context.Context, rb *xtmodel.Rebuilder, edges bool) error {
	var q sqlText
	if edges {
		q.s("SELECT layer, fp, peer, rel, producer, seq, event_ns, kind, ttl_ns, through_ns FROM " + e.names.of(xtmodel.LogF) + " FOR VALID_TIME ALL")
	} else {
		q.s("SELECT layer, fp, producer, seq, event_ns, kind, ttl_ns, through_ns FROM " + e.names.of(xtmodel.LogE) + " FOR VALID_TIME ALL")
	}
	err := scanRows(ctx, e.w, q.stmt(), func(rows pgx.Rows) error {
		var layer int64
		var fp, peer, rel string
		var a lifecycle.Assertion
		var err error
		if edges {
			lead := []any{&layer, &fp, &peer, &rel}
			a, err = scanAssertion(rows, lead...)
		} else {
			a, err = scanAssertion(rows, &layer, &fp)
		}
		if err != nil {
			return err
		}
		from, err := identity.ParseFingerprint(fp)
		if err != nil {
			return err
		}
		s := engine.EntitySubject(from)
		if edges {
			to, err := identity.ParseFingerprint(peer)
			if err != nil {
				return err
			}
			s = engine.EdgeSubject(from, to, catalog.RelationType(rel))
		}
		rb.Add(engine.Record{
			Layer: catalog.Layer(layer), Subject: s, Producer: a.Producer, EventTime: a.EventTime,
			Seq: a.Seq, Kind: a.Kind, TTL: a.TTL, Through: a.Through,
		})
		e.layers[s] = catalog.Layer(layer)
		return nil
	})
	if err != nil {
		return fmt.Errorf("xtengine: reading the log: %w", err)
	}
	return nil
}

// setBase fixes the system time of the zeroth batch: the one the meta documents
// record, which a configured base must equal, or the configured base (the default if
// it is not set) of a node that has none. stored is the base in microseconds.
func (e *Engine) setBase(stored int64, haveStored bool) error {
	want := e.cfg.Base
	if want.IsZero() {
		if haveStored {
			e.base = xtmodel.Instant(stored * nanosPerMicro)
		} else {
			e.base = xtmodel.BaseSystemTime
		}
		return nil
	}
	if want.Nanosecond()%int(nanosPerMicro) != 0 || want.Before(engine.MinEventTime) || want.After(engine.MaxEventTime) {
		return fmt.Errorf("xtengine: the base %s is not a whole microsecond in the range of event times: %w", want.Format(time.RFC3339Nano), engine.ErrInvalid)
	}
	if haveStored && stored != want.UnixMicro() {
		return fmt.Errorf("xtengine: the node was written with a base of %s, not %s: %w",
			time.UnixMicro(stored).UTC().Format(time.RFC3339Nano), want.UTC().Format(time.RFC3339Nano), engine.ErrInvalid)
	}
	e.base = xtmodel.Instant(want.UnixNano())
	return nil
}
