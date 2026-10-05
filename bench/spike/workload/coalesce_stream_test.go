package workload_test

import (
	"testing"
	"time"

	"github.com/lotannauo/toposhift/bench/spike/engine"
	"github.com/lotannauo/toposhift/bench/spike/workload"
	"github.com/lotannauo/toposhift/internal/lifecycle"
)

// existences folds the records of each subject and returns where it exists.
func existences(t *testing.T, rs []engine.Record) map[engine.Subject][]lifecycle.Interval {
	t.Helper()
	by := map[engine.Subject][]lifecycle.Assertion{}
	for _, r := range rs {
		by[r.Subject] = append(by[r.Subject], r.Assertion())
	}
	out := make(map[engine.Subject][]lifecycle.Interval, len(by))
	for s, as := range by {
		tl, err := lifecycle.Fold(as, lifecycle.Policy{})
		if err != nil {
			t.Fatal(err)
		}
		out[s] = tl.Existence()
	}
	return out
}

func isHorizon(horizons []time.Duration, at time.Duration) bool {
	for _, h := range horizons {
		if h == at {
			return true
		}
	}
	return false
}

var forever = time.Date(9999, 1, 1, 0, 0, 0, 0, time.UTC)

func endOf(i lifecycle.Interval) time.Time {
	if i.Open() {
		return forever
	}
	return i.End
}

// TestExtensionNeverOpensAHoleInAnInterval is the stream-level form of the
// coalescer's contract, for every way of bounding the extension interval and with
// retentions that make runs start again: against the uncoalesced stream, every
// interval of existence is one interval that starts when the raw one does and
// ends no later than it. Ending early is the bounded extension's price; a gap
// inside an interval, or an interval that is not there, is a death and a rebirth
// that nothing in the producer's refreshes said.
//
// The outages are shorter than the TTL, so a refresh arrives late enough to make
// the run that continues after a retention start after the stored deadline of the
// one before it.
func TestExtensionNeverOpensAHoleInAnInterval(t *testing.T) {
	t.Parallel()

	base := workload.Tiny()
	base.Duration = 3 * time.Hour
	base.FreshIdentities = true
	base.HeartbeatInterval, base.HeartbeatTTLFactor = time.Minute, 4
	base.PodHeartbeatInterval = 100 * time.Second
	base.OutageProbability, base.OutageLength = 0.04, 130*time.Second
	base.ConfirmProbability, base.ConfirmTTL = 0.5, 3*time.Minute
	base.LateProbability = 0
	horizons := []time.Duration{30 * time.Minute, 70 * time.Minute, 110 * time.Minute, 150 * time.Minute}

	g, err := workload.New(base)
	if err != nil {
		t.Fatal(err)
	}
	reference := existences(t, g.All())

	for name, set := range map[string]func(*workload.Config){
		"every refresh":           func(c *workload.Config) {},
		"half the TTL":            func(c *workload.Config) { c.ExtendTTLFraction = 0.5 },
		"three quarters":          func(c *workload.Config) { c.ExtendTTLFraction = 0.75 },
		"nine tenths":             func(c *workload.Config) { c.ExtendTTLFraction = 0.9 },
		"the whole TTL":           func(c *workload.Config) { c.ExtendTTLFraction = 1 },
		"longer than a TTL":       func(c *workload.Config) { c.ExtendEvery = 7 * time.Minute },
		"rollover, every refresh": func(c *workload.Config) { c.RunMaxAge = 15 * time.Minute },
		"rollover, half the TTL":  func(c *workload.Config) { c.ExtendTTLFraction, c.RunMaxAge = 0.5, 20*time.Minute },
		"rollover, nine tenths":   func(c *workload.Config) { c.ExtendTTLFraction, c.RunMaxAge = 0.9, 25*time.Minute },
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			cfg := base
			cfg.CoalesceRuns = true
			set(&cfg)
			g, err := workload.New(cfg)
			if err != nil {
				t.Fatal(err)
			}
			var got []engine.Record
			var horizon time.Time
			next := 0
			for {
				r, ok := g.Next()
				if !ok {
					break
				}
				if r.EventTime.Before(horizon) {
					t.Fatalf("offered a record at %s after the horizon %s", r.EventTime.Sub(cfg.Start), horizon.Sub(cfg.Start))
				}
				got = append(got, r)
				for next < len(horizons) && !r.EventTime.Before(cfg.Start.Add(horizons[next])) {
					horizon = cfg.Start.Add(horizons[next])
					g.SetHorizon(horizon)
					next++
				}
			}
			intervals := existences(t, got)

			bad := 0
			for subject, want := range reference {
				have := intervals[subject]
				i := 0
				for _, w := range want {
					var inside []lifecycle.Interval
					for ; i < len(have) && have[i].Start.Before(endOf(w)); i++ {
						inside = append(inside, have[i])
					}
					// A run whose stored deadline lapsed before a retention is begun
					// again at the horizon, and what lies before the horizon is gone
					// once the store has retained to it: a gap that ends at a
					// horizon is not one a read can see.
					visible := inside[:min(1, len(inside))]
					for _, in := range inside[min(1, len(inside)):] {
						if at := in.Start.Sub(cfg.Start); !isHorizon(horizons, at) {
							visible = append(visible, in)
						}
					}
					switch {
					case len(inside) == 0 || len(visible) != 1:
						bad++
						if bad <= 3 {
							t.Errorf("%v: the raw stream has one interval [%s, %s) and the coalesced stream %d inside it: %v",
								subject.Relation, w.Start.Sub(cfg.Start), endOf(w).Sub(cfg.Start), len(visible), inside)
						}
					case !inside[0].Start.Equal(w.Start) || endOf(inside[len(inside)-1]).After(endOf(w)):
						bad++
						if bad <= 3 {
							t.Errorf("%v: raw [%s, %s), coalesced %v: it must start together and end no later",
								subject.Relation, w.Start.Sub(cfg.Start), endOf(w).Sub(cfg.Start), inside)
						}
					}
				}
				if i < len(have) {
					bad++
					if bad <= 3 {
						t.Errorf("%v: %d intervals of existence in the coalesced stream that are not in the raw one", subject.Relation, len(have)-i)
					}
				}
			}
			if bad > 3 {
				t.Errorf("%d subjects differ in all", bad)
			}
		})
	}
}
