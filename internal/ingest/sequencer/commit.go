package sequencer

import (
	"context"
	"errors"
	"fmt"
	"strconv"

	"github.com/lotannauo/toposhift/internal/catalog"
	"github.com/lotannauo/toposhift/internal/coalesce"
	"github.com/lotannauo/toposhift/internal/ingest/otlphttp"
	"github.com/lotannauo/toposhift/internal/store"
)

// commit takes claimed requests through one synced commit and answers each of
// them: it reads the horizons, translates and checks each request in arrival
// order, coalesces what passed, writes it once, and decides the outcome from the
// error. horizonRetry allows the one rerun a refusal for the horizon gets, and
// split the one-by-one writing that a refusal for any other validation error
// gets.
func (s *Sequencer) commit(ctx context.Context, reqs []*request, horizonRetry, split bool) {
	now := s.now()
	s.readHorizons()
	ws := s.translateAll(reqs, now)
	for _, n := range s.det.Notes(now) {
		s.log.Warn("sequencer: detector note", "message", n.Message)
	}
	var batch []store.Record
	for _, w := range ws {
		batch = w.coalesce(s.co, batch)
	}
	last, err := s.write(ctx, batch)
	switch {
	case err == nil:
		s.observeClocks(batch, ws)
		s.answer(ws, nil)
	case errors.Is(err, store.ErrBeforeHorizon):
		s.observeClocks(nil, ws)
		// A retention moved a horizon after it was read. Horizons only move
		// forward, so one rerun against the horizons now published converges; the
		// records now before them are rejected into each request's partial
		// success.
		s.reset("the store refused the group: a horizon moved before the write", err)
		if horizonRetry {
			s.commit(ctx, reqs, false, split)
			return
		}
		s.answer(ws, busy(err))
	case errors.Is(err, store.ErrInvalid):
		s.observeClocks(nil, ws)
		// A case the checks missed. It is deterministic, so the group is not
		// written again whole: each request is written on its own, and the one the
		// store refuses is refused.
		s.reset("the store refused the group as invalid", err)
		if split && len(reqs) > 1 {
			for _, r := range reqs {
				s.commit(ctx, []*request{r}, true, false)
			}
			return
		}
		s.answer(ws, &otlphttp.RefusedError{Message: "the store refused the request's records: " + err.Error()})
	default:
		// Whether the batch was stored is told by the last sequence number.
		if len(batch) > 0 && s.cfg.Store.LastSeq() >= last {
			s.log.Warn("sequencer: the write returned an error but its batch was stored", "error", err, "last_seq", last)
			s.observeClocks(batch, ws)
			s.answer(ws, nil)
			return
		}
		s.observeClocks(nil, ws)
		s.reset("the write failed and its batch was not stored", err)
		s.answer(ws, writeFailed(err))
	}
}

// write gives the batch its sequence numbers, contiguous above the store's last
// one, and writes it once, synced, with the sequencer's own context. It returns
// the last sequence number of the batch. An empty batch is written too, so that a
// closed store is noticed.
func (s *Sequencer) write(ctx context.Context, batch []store.Record) (uint64, error) {
	seq := s.cfg.Store.LastSeq()
	for i := range batch {
		seq++
		batch[i].Seq = seq
	}
	if s.hooks.beforeWrite != nil {
		s.hooks.beforeWrite(batch)
	}
	return seq, s.cfg.Store.Write(ctx, batch)
}

// writeDerived writes the records of a flush, which no request waits on. A
// refusal drops them; a failure that did not store them drops them too. Either
// way the coalescer is made anew, because it took them as written.
func (s *Sequencer) writeDerived(ctx context.Context, out []store.Record, what string) {
	if len(out) == 0 {
		return
	}
	last, err := s.write(ctx, out)
	switch {
	case err == nil:
		s.observeClocks(out, nil)
		return
	case errors.Is(err, store.ErrInvalid):
	case s.cfg.Store.LastSeq() >= last:
		s.log.Warn("sequencer: the write returned an error but its batch was stored", "what", what, "error", err, "last_seq", last)
		s.observeClocks(out, nil)
		return
	}
	s.log.Error("sequencer: dropped the records of "+what, "records", len(out), "error", err)
	s.resetCoalescer()
}

// tick is the flush tick: the absorbed refreshes whose stored copy is about to
// lapse are written, as a group of their own, and the coalescer, the detector and
// the cluster names are pruned.
func (s *Sequencer) tick(ctx context.Context) {
	now := s.now()
	s.readHorizons()
	s.writeDerived(ctx, s.co.Flush(now, nil), "a flush")
	s.co.Prune(now)
	s.det.Prune(now)
	idle := idleMultiple * max(s.cfg.PullInterval, s.cfg.CollectionInterval)
	for name, c := range s.clusters {
		if now.Sub(c.lastUsed) > idle {
			delete(s.clusters, name)
			s.log.Info("sequencer: freed the slot of an idle cluster name", "cluster", name, "idle_since", c.lastUsed)
		}
	}
	if s.hooks.afterTick != nil {
		s.hooks.afterTick()
	}
}

// readHorizons reads every layer's horizon and tells the coalescer, which keeps
// only the moves forward. A retention can move a horizon at any time, and a
// coalescer made anew knows none.
func (s *Sequencer) readHorizons() {
	for l := catalog.L0; l <= catalog.L3; l++ {
		h := s.cfg.Store.LayerHorizon(l).Time
		s.horizons[l-catalog.L0] = h
		s.co.SetHorizon(l, h)
	}
}

// reset makes the translators of every cluster name and the coalescer anew, as a
// restart would; the detector is kept. See "Restart effects" in the package
// documentation.
func (s *Sequencer) reset(why string, err error) {
	s.log.Warn("sequencer: the translators and the coalescer are made anew", "why", why, "error", err)
	for name := range s.clusters {
		s.renewCluster(name)
	}
	s.resetCoalescer()
}

func (s *Sequencer) resetCoalescer() {
	co, err := coalesce.New(s.coCfg)
	if err != nil {
		// The configuration is a constant that New accepted.
		panic("sequencer: " + err.Error())
	}
	s.co = co
}

// busy is the answer to a group whose write failed in a way that a retry may not
// meet: 503 with Retry-After: 1.
func busy(err error) error {
	return fmt.Errorf("%w: the store did not take the write: %w", otlphttp.ErrBusy, err)
}

// writeFailed is the answer to a group the store did not store. A closed store is
// 503 with a longer Retry-After and says so. Any other error is 503 with
// Retry-After: 1: a store that keeps failing every write (a latched failure)
// stays so until serve is restarted, and the log says so.
func writeFailed(err error) error {
	if errors.Is(err, store.ErrClosed) {
		return &otlphttp.UnavailableError{Message: "the store is closed: restart toposhift serve"}
	}
	return busy(err)
}

// answer answers each request of an attempt: a refusal is a 400 whatever the
// write did, and the others get err, or 200 with their partial success.
func (s *Sequencer) answer(ws []*work, err error) {
	for _, w := range ws {
		w.logNotes(s.log)
		var o outcome
		switch {
		case w.refused != "":
			s.log.Warn("sequencer: refused a request", "message", w.refused)
			o.err = &otlphttp.RefusedError{Message: w.refused}
		case err != nil:
			o.err = err
		default:
			o.partial = w.partial()
		}
		w.r.done <- o
	}
}

// partial is the request's partial success.
func (w *work) partial() otlphttp.Partial {
	if w.rejected == 0 {
		return otlphttp.Partial{}
	}
	noun := "log records"
	if w.rejected == 1 {
		noun = "log record"
	}
	return otlphttp.Partial{
		Rejected: w.rejected,
		Message:  strconv.FormatInt(w.rejected, 10) + " " + noun + " rejected, which a retry would not change; the first: " + w.firstReject,
	}
}
