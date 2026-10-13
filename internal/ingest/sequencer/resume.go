package sequencer

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/lotannauo/toposhift/internal/ingest/resume"
	"github.com/lotannauo/toposhift/internal/store"
)

// openLookup opens the group's lookup, or returns nil when resume is off. An
// opener's error that aborts (see [resume.IsAbort]) is returned, and fails the
// group. Any other error gives a lookup whose every answer is that error, so that
// each marked record falls back to the time it was observed, counted, and the
// group goes on.
func (s *Sequencer) openLookup() (resume.Lookup, error) {
	if s.cfg.Lookup == nil {
		return nil, nil
	}
	lk, err := s.cfg.Lookup()
	switch {
	case err != nil && resume.IsAbort(err):
		return nil, fmt.Errorf("sequencer: opening the resume lookup: %w", err)
	case err != nil:
		return failedLookup{fmt.Errorf("sequencer: opening the resume lookup: %w", err)}, nil
	case lk == nil:
		return failedLookup{errors.New("sequencer: the resume lookup's opener returned none")}, nil
	}
	return lk, nil
}

// failedLookup is the lookup of a group whose opener failed.
type failedLookup struct{ err error }

func (f failedLookup) Last(context.Context, store.LookupRef) (store.LastAssertion, error) {
	return store.LastAssertion{}, f.err
}

func (failedLookup) Close() error { return nil }

// resumed adds the resume counts of a group that was answered without error, and
// logs its warnings, each kind at most once per resumeLogEvery: a walk that
// reached its bound, a description that changed during a silence longer than the
// TTL, and, at error level, a store that could not be read. A refused request's
// counts are not added: nothing of it was written.
func (s *Sequencer) resumed(ws []*work, now time.Time) {
	var g resume.Stats
	for _, w := range ws {
		if w.refused == "" {
			g.Add(w.resumed)
		}
	}
	s.resumeMu.Lock()
	s.resumeStats.Add(g)
	s.resumeMu.Unlock()
	if g.UnknownExhausted > 0 && s.resumeLogDue("exhausted", now) {
		s.log.Warn("sequencer: resume: a lookup reached its walk bound; the records take the time they were observed",
			"count", g.UnknownExhausted, "example", showRef(g.ExhaustedExample))
	}
	if g.ChangedGap > 0 && s.resumeLogDue("changed_gap", now) {
		s.log.Warn("sequencer: resume: a description changed during a silence longer than its TTL; the producer's reference is absent from its stored lapse to the time it was observed",
			"count", g.ChangedGap, "example", showRef(g.GapExample))
	}
	if g.UnknownError > 0 && s.resumeLogDue("error", now) {
		s.log.Error("sequencer: resume: the store could not be read; the records take the time they were observed",
			"count", g.UnknownError, "error", g.FirstError, "example", showRef(g.ErrorExample))
	}
}

// resumeLogDue says whether a resume warning of the kind may be logged now, and
// notes it if so.
func (s *Sequencer) resumeLogDue(kind string, now time.Time) bool {
	if last, ok := s.resumeWarned[kind]; ok && now.Sub(last) < resumeLogEvery {
		return false
	}
	s.resumeWarned[kind] = now
	return true
}

// showRef names a reference for a log line.
func showRef(r store.LookupRef) string {
	sub := r.Subject.A.String()
	if r.Subject.Kind == store.SubjectEdge {
		sub = fmt.Sprintf("%s -%s-> %s", r.Subject.A, r.Subject.Relation, r.Subject.B)
	}
	return fmt.Sprintf("%s %s in %s", r.Producer, sub, r.Layer)
}
