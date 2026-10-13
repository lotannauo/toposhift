package sequencer_test

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/lotannauo/toposhift/internal/ingest/otlphttp"
	"github.com/lotannauo/toposhift/internal/ingest/resume"
	"github.com/lotannauo/toposhift/internal/ingest/sequencer"
	"github.com/lotannauo/toposhift/internal/store"
	"github.com/lotannauo/toposhift/internal/store/memstore"
)

// faultyLookups opens memstore lookups whose answers fail with err while it is
// set, and counts the lookups made.
type faultyLookups struct {
	m       *memstore.Store
	mu      sync.Mutex
	err     error // the answer of every lookup while set
	openErr error // the opener's error while set
	calls   int
}

func (f *faultyLookups) set(err, openErr error) {
	f.mu.Lock()
	f.err, f.openErr = err, openErr
	f.mu.Unlock()
}

func (f *faultyLookups) open() (resume.Lookup, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.openErr != nil {
		return nil, f.openErr
	}
	lk, err := f.m.NewLookup()
	if err != nil {
		return nil, err
	}
	return &faultyLookup{f: f, lk: lk}, nil
}

type faultyLookup struct {
	f  *faultyLookups
	lk *memstore.Lookup
}

func (l *faultyLookup) Last(ctx context.Context, ref store.LookupRef) (store.LastAssertion, error) {
	l.f.mu.Lock()
	l.f.calls++
	err := l.f.err
	l.f.mu.Unlock()
	if err != nil {
		return store.LastAssertion{}, err
	}
	return l.lk.Last(ctx, ref)
}

func (l *faultyLookup) Close() error { return l.lk.Close() }

func (f *faultyLookups) count() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.calls
}

// errorHarness is a sequencer over a memstore whose resume lookups the test can
// make fail, with the log kept.
func errorHarness(t *testing.T) (*harness, *fakeStore, *faultyLookups, *logCapture) {
	t.Helper()
	m := newMem(t)
	t.Cleanup(func() { _ = m.Close() })
	fs := &fakeStore{inner: m}
	fl := &faultyLookups{m: m}
	logs := newLogCapture(t.Output())
	h := newHarness(t, fs, withConfig(func(c *sequencer.Config) {
		c.Lookup = fl.open
		c.Logger = slog.New(logs)
	}))
	h.start()
	return h, fs, fl, logs
}

// The world of these tests: one node and one pod, listed by the objects receiver.
func errorWorld() *rsWorld {
	w, _ := onePod()
	return w
}

// A lookup that fails because the store or the lookup is closed, or the context
// is done, aborts the group: every request is answered 503 with Retry-After (never
// a 500 or a refusal), nothing is written, nothing is counted, and the translators
// are made anew, so that the same records are first sightings again when the
// collector sends them again. The opener's own closed store aborts too.
func TestResumeAnAbortFailsTheGroup(t *testing.T) {
	for _, c := range []struct {
		name          string
		lookup, open  error
		wantAvailable bool // the answer is the closed store's (Retry-After: 30)
	}{
		{"a closed store", fmt.Errorf("memstore: Lookup.Last: %w", store.ErrClosed), nil, true},
		{"a cancelled context", fmt.Errorf("memstore: Lookup.Last: %w", context.Canceled), nil, false},
		{"a closed store at the opening", nil, fmt.Errorf("memstore: NewLookup: %w", store.ErrClosed), true},
	} {
		t.Run(c.name, func(t *testing.T) {
			h, fs, fl, _ := errorHarness(t)
			w := errorWorld()
			h.sendOK(w.pull(t0))
			before, _ := h.seq.ResumeStats()
			writes := fs.writeCount()

			fl.set(c.lookup, c.open)
			h.clock.set(at(5 * time.Minute))
			w.pods = append(w.pods, &rsPod{uid: "pod-new", name: "new", node: "n", created: at(4 * time.Minute), scheduled: at(4 * time.Minute), phase: "Running"})
			_, err := h.send(w.pull(at(5 * time.Minute)))
			var unavailable *otlphttp.UnavailableError
			var refused *otlphttp.RefusedError
			switch {
			case err == nil || errors.As(err, &refused):
				t.Fatalf("answered %v, want 503", err)
			case c.wantAvailable && !errors.As(err, &unavailable):
				t.Errorf("answered %v, want the closed store's 503", err)
			case !c.wantAvailable && !errors.Is(err, otlphttp.ErrBusy):
				t.Errorf("answered %v, want 503 with Retry-After: 1", err)
			}
			if n := fs.writeCount(); n != writes {
				t.Errorf("%d writes after the abort, want none", n-writes)
			}
			if after, _ := h.seq.ResumeStats(); after != before {
				t.Errorf("counted %+v after the abort, want %+v", after, before)
			}

			// The collector sends it again: the translators were made anew, so the
			// pod seen before is a first sighting again, resumed from the store.
			fl.set(nil, nil)
			calls := fl.count()
			h.sendOK(w.pull(at(5 * time.Minute)))
			if fl.count() == calls {
				t.Error("no lookup after the abort: the translators were not made anew")
			}
			if after, _ := h.seq.ResumeStats(); after.Raised == 0 {
				t.Errorf("%+v: the pod seen before is not raised", after)
			}
		})
	}
}

// Any other read error falls back for its records alone: the group is written,
// each marked record at the time it was observed with no Through, counted as
// unknown with the reason error, the first error kept, and logged at error level
// at most once a minute. An opener that fails so does the same.
func TestResumeAReadErrorFallsBack(t *testing.T) {
	for _, atOpen := range []bool{false, true} {
		t.Run(fmt.Sprint("at the opening ", atOpen), func(t *testing.T) {
			h, fs, fl, logs := errorHarness(t)
			w := errorWorld()
			readErr := errors.New("a key could not be decoded")
			if atOpen {
				fl.set(nil, readErr)
			} else {
				fl.set(readErr, nil)
			}
			h.sendOK(w.pull(t0))
			h.clock.set(at(30 * time.Second))
			w.pods[0].uid = "pod-other"
			h.sendOK(w.pull(at(30 * time.Second)))
			st, on := h.seq.ResumeStats()
			// The first request marks the node, the pod, its container, part_of and
			// edge; the second the other pod's four.
			if !on || st.UnknownError != 9 || st.Unknown != 9 || st.FirstError == nil || !errors.Is(st.FirstError, readErr) {
				t.Errorf("%+v, want 9 unknown for a read error, with the error kept", st)
			}
			for _, r := range fs.records() {
				if !r.Through.IsZero() || !r.EventTime.Equal(r.EventTime.Truncate(30*time.Second)) {
					t.Errorf("a record kept its backdated time: %s", show(r))
				}
			}
			if n := count(logs.take(), slog.LevelError, "the store could not be read"); n != 1 {
				t.Errorf("%d error logs in half a minute, want 1", n)
			}
			h.clock.set(at(90 * time.Second))
			w.pods[0].uid = "pod-third"
			h.sendOK(w.pull(at(90 * time.Second)))
			if n := count(logs.take(), slog.LevelError, "the store could not be read"); n != 1 {
				t.Errorf("%d error logs a minute later, want 1", n)
			}
		})
	}
}

// With no lookup the step is off, and says so.
func TestResumeOffWithoutALookup(t *testing.T) {
	st := &fakeStore{inner: newMem(t)}
	h := newHarness(t, st).start()
	h.sendOK(errorWorld().pull(t0))
	if s, on := h.seq.ResumeStats(); on || s != (resume.Stats{}) {
		t.Errorf("%+v %v, want off and nothing counted", s, on)
	}
	backdated := 0
	for _, r := range st.records() {
		if r.EventTime.Before(t0) && r.Through.Equal(t0) {
			backdated++
		}
	}
	if backdated != 5 {
		t.Errorf("%d records written as the translator backdated them, want 5", backdated)
	}
}
