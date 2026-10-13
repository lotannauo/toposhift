package sequencer_test

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/lotannauo/toposhift/internal/ingest/otlphttp"
	"github.com/lotannauo/toposhift/internal/ingest/resume"
	"github.com/lotannauo/toposhift/internal/replay/capture"
	"github.com/lotannauo/toposhift/internal/store"
)

// The reset scenarios run on a stream of 2 nodes and 10 pods, each with a
// container, reported by both receivers every five minutes for eight hours, so
// that every prefix the reads name holds checkpoints by then. The reset happens at
// eight hours; the stream goes on for an hour after.
const (
	rsReset = 8 * time.Hour
	rsEnd   = rsReset + time.Hour
)

// rsInstants are the ten instants the reads are compared at: eight spread over the
// history before the reset, where a deleted checkpoint would cost steps, and two
// after the stream has resumed.
func rsInstants() []time.Time {
	var out []time.Time
	for i := range 8 {
		out = append(out, at(time.Hour+time.Duration(i)*50*time.Minute))
	}
	return append(out, at(rsReset+45*time.Minute), at(rsEnd))
}

// resetScenario is one way the translators lose their state.
type resetScenario struct {
	name string
	// configured runs with the cluster name configured.
	configured bool
	// skip leaves out a pull and report, in the scenario and its control alike.
	skip func(time.Time) bool
	// dropped is a pull of the objects receiver the scenario refuses, which the
	// control leaves out.
	dropped time.Time
	// reset does what the scenario does at the reset, after the pull at it.
	reset func(t *testing.T, r *rs, w *rsWorld)
	// panicAt makes the translation of the first record of the pull at that
	// instant panic, once.
	panicAt time.Time
	// horizonRefusal refuses the write of the pull after the reset once, for the
	// horizon.
	horizonRefusal bool
	// controlCollection is the control's collection interval: a 30-minute
	// silence keeps the slot when it is 15 minutes.
	controlCollection time.Duration
	// keepsSlot says the scenario keeps the translators: no lookup is expected.
	keepsSlot bool
}

func silence(tm time.Time) bool {
	return tm.After(at(rsReset)) && tm.Before(at(rsReset+30*time.Minute))
}

// sendRefused sends a request that must be refused (400).
func (r *rs) sendRefused(d capture.LogsData) {
	r.t.Helper()
	_, err := r.h.seq.AcceptPartial(context.Background(), d)
	var refused *otlphttp.RefusedError
	if !errors.As(err, &refused) {
		r.t.Fatalf("at %s: %v, want a refusal", r.clock.now().Sub(t0), err)
	}
}

func resetScenarios() []resetScenario {
	refusedAt := at(rsReset + 5*time.Minute)
	return []resetScenario{
		{name: "1 a process restart", reset: func(t *testing.T, r *rs, w *rsWorld) {
			r.clock.set(r.clock.now().Add(2 * time.Minute))
			r.restart()
		}},
		{name: "2 a 30-minute silence, unconfigured name", skip: silence, controlCollection: 15 * time.Minute},
		{name: "3 a 30-minute silence, configured name", configured: true, skip: silence, keepsSlot: true},
		{name: "4 a write refused as invalid", dropped: refusedAt},
		{name: "5 a write refused for the horizon", horizonRefusal: true},
		{name: "6 a translation panic", dropped: refusedAt, panicAt: refusedAt},
	}
}

// runResetScenario runs the stream with the scenario's reset, or as its control;
// off runs it with no lookup.
func runResetScenario(t *testing.T, sc resetScenario, lag time.Duration, control, off bool) (*rs, *rsWorld, resume.Stats) {
	t.Helper()
	o := rsOptions{lag: lag, configured: sc.configured, off: off}
	if control {
		o.collection = sc.controlCollection
	}
	var panicked atomic.Bool
	var r *rs
	if !control && !sc.panicAt.IsZero() {
		o.beforeTranslate = func(_ string, _ capture.LogRecord) {
			if r.clock.now().Equal(sc.panicAt) && panicked.CompareAndSwap(false, true) {
				panic("a translation panic injected by the test")
			}
		}
	}
	r = newRS(t, o)
	w := newWorld(2, 10)
	if sc.configured {
		w.cluster = "c1"
	}
	skip := func(tm time.Time) bool { return sc.skip != nil && sc.skip(tm) }
	r.pullAt(w)
	r.tick()
	r.run(w, at(rsReset), skip)
	before := r.stats()
	walked := len(r.keysWalked())
	r.arm()
	switch {
	case control:
	case sc.reset != nil:
		sc.reset(t, r, w)
	case sc.horizonRefusal:
		var once atomic.Bool
		r.fs.inject = func(_ int, b []store.Record) (bool, error) {
			if r.clock.now().Equal(at(rsReset+5*time.Minute)) && once.CompareAndSwap(false, true) {
				return false, fmt.Errorf("injected: %w", store.ErrBeforeHorizon)
			}
			return true, nil
		}
	}
	for tm := r.clock.now().Add(30 * time.Second); !tm.After(at(rsEnd)); tm = tm.Add(30 * time.Second) {
		r.clock.set(tm)
		if tm.Sub(t0)%(5*time.Minute) == 0 && !skip(tm) {
			switch {
			case !sc.dropped.IsZero() && tm.Equal(sc.dropped) && control:
				// The control leaves out the pull the scenario's store never took.
				if !w.noCluster {
					r.send(w.report(tm))
				}
			case !sc.dropped.IsZero() && tm.Equal(sc.dropped) && sc.panicAt.IsZero():
				var once atomic.Bool
				r.fs.inject = func(int, []store.Record) (bool, error) {
					if once.CompareAndSwap(false, true) {
						return false, fmt.Errorf("injected: %w", store.ErrInvalid)
					}
					return true, nil
				}
				r.sendRefused(w.pull(tm))
				r.fs.inject = nil
				r.send(w.report(tm))
			case !sc.dropped.IsZero() && tm.Equal(sc.dropped):
				r.sendRefused(w.pull(tm))
				r.send(w.report(tm))
			default:
				r.pullAt(w)
			}
		}
		r.tick()
	}
	if sc.keepsSlot && !control {
		if n := len(r.keysWalked()) - walked; n != 0 {
			t.Errorf("%d lookups after the silence, want none: the configured name's translators are kept", n)
		}
	}
	delta := since(r.stats(), before)
	return r, w, delta
}

// The reset scenarios, at both lags: the post-reset groups delete at most one
// checkpoint per prefix each (none expected), no resumed record is unknown or
// changed, no lookup walks more than 8 keys, and the reads at ten instants answer
// as a run of the same stream without the reset does, in as many steps. Without
// the step, a restart deletes checkpoints of the history before it.
func TestResumeResetScenarios(t *testing.T) {
	if testing.Short() {
		t.Skip("eight hours of stream per run")
	}
	controls := map[string][]probe{}
	for _, lag := range lags {
		for _, sc := range resetScenarios() {
			t.Run(fmt.Sprintf("%s lag %s", sc.name, lag), func(t *testing.T) {
				key := fmt.Sprint(lag, sc.configured, sc.dropped, sc.controlCollection, sc.skip != nil)
				want, ok := controls[key]
				if !ok {
					c, w, _ := runResetScenario(t, sc, lag, true, false)
					want = c.reads(w, rsInstants())
					controls[key] = want
				}
				r, w, st := runResetScenario(t, sc, lag, false, false)
				most, total := r.maxPerPrefix()
				walked := r.keysWalked()
				maxWalked := int64(0)
				for _, k := range walked {
					maxWalked = max(maxWalked, k)
				}
				got := r.reads(w, rsInstants())
				diffs, notes := compareReads(got, want)
				t.Logf("invalidated after the reset: %d in all, at most %d per prefix per write; resume %v; lookups %d, keys walked at most %d; reads that differ %d, steps that differ within the extra records %d",
					total, most, counters(st), len(walked), maxWalked, len(diffs), len(notes))
				for _, n := range notes {
					t.Log(n)
				}
				if most > 1 {
					t.Errorf("a write deleted %d checkpoints of one prefix, more than 1", most)
				}
				if st.Unknown != 0 || st.Changed != 0 {
					t.Errorf("resume %+v: want no unknown and no changed", counters(st))
				}
				if maxWalked > 8 {
					t.Errorf("a lookup walked %d keys, more than 8", maxWalked)
				}
				if !sc.keepsSlot && st.Raised == 0 {
					t.Errorf("nothing was raised: the scenario resumed nothing")
				}
				for _, d := range diffs {
					t.Error(d)
				}
			})
		}
	}
}

// The power of the scenarios: with no lookup (the step off), the same restart
// deletes checkpoints of the history before it, and the reads before it step
// more.
func TestResumeResetScenariosWithoutTheStep(t *testing.T) {
	if testing.Short() {
		t.Skip("eight hours of stream per run")
	}
	for _, lag := range lags {
		t.Run(fmt.Sprint("lag ", lag), func(t *testing.T) {
			sc := resetScenarios()[0]
			c, w, _ := runResetScenario(t, sc, lag, true, true)
			want := c.reads(w, rsInstants())
			r, w, _ := runResetScenario(t, sc, lag, false, true)
			most, total := r.maxPerPrefix()
			diffs, _ := compareReads(r.reads(w, rsInstants()), want)
			t.Logf("without the step: invalidated %d in all, at most %d per prefix per write; reads that differ %d", total, most, len(diffs))
			for _, d := range diffs {
				t.Log(d)
			}
			if total == 0 || len(diffs) == 0 {
				t.Errorf("a restart without the step deleted %d checkpoints and changed %d reads: the scenario has no power", total, len(diffs))
			}
		})
	}
}

func counters(s resume.Stats) map[string]int64 {
	m := s.Counters()
	for k, v := range m {
		if v == 0 {
			delete(m, k)
		}
	}
	return m
}
