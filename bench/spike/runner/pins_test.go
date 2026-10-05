package runner_test

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/lotannauo/toposhift/bench/spike/conformance"
	"github.com/lotannauo/toposhift/bench/spike/engine"
	"github.com/lotannauo/toposhift/bench/spike/runner"
	"github.com/lotannauo/toposhift/bench/spike/workload"
	"github.com/lotannauo/toposhift/internal/catalog"
	"github.com/lotannauo/toposhift/internal/identity"
	"github.com/lotannauo/toposhift/internal/lifecycle"
)

func TestPinsAreTheHubPrefixesOfTheStreamAndTheSameEveryTime(t *testing.T) {
	t.Parallel()
	conformance.SkipWhenTrimmed(t)

	spec := tinySpec()
	a, err := runner.MakePins(context.Background(), spec)
	if err != nil {
		t.Fatal(err)
	}
	b, err := runner.MakePins(context.Background(), spec)
	if err != nil {
		t.Fatal(err)
	}
	if a.Digest == "" || a.Digest != b.Digest {
		t.Errorf("pins made twice from one spec have digests %q and %q", a.Digest, b.Digest)
	}
	if err := a.Verify(); err != nil {
		t.Errorf("fresh pins do not verify: %v", err)
	}
	classes := 0
	for _, pc := range a.Classes {
		classes++
		if pc.Class.Owner == catalog.K8sPod || pc.Class.Owner == catalog.ServiceInstance || pc.Class.Owner == catalog.Container {
			t.Errorf("the class %s is not a hub: its entities are replaced", pc.Class)
		}
		if len(pc.Records) == 0 || len(pc.Records) > spec.Hot || len(pc.Median) > spec.Median {
			t.Errorf("%s: %d by records, %d by extensions, %d median (hot %d, median %d)", pc.Class, len(pc.Records), len(pc.Extensions), len(pc.Median), spec.Hot, spec.Median)
		}
	}
	if classes < 5 {
		t.Errorf("%d classes pinned", classes)
	}

	// A different seed is another scenario: its pins are chosen from another
	// stream (the hubs are the same entities in every seed, so they are told apart
	// by where they came from).
	other := spec
	other.Workload.Seed++
	c, err := runner.MakePins(context.Background(), other)
	if err != nil {
		t.Fatal(err)
	}
	if c.Digest == a.Digest {
		t.Error("another seed gave the same pins")
	}

	// Pins that were edited do not verify, and a spec that holds them is invalid.
	edited := *a
	edited.Classes = append([]runner.PinnedClass(nil), a.Classes...)
	edited.Classes[0].Records = edited.Classes[0].Records[1:]
	if edited.Verify() == nil {
		t.Error("edited pins verify")
	}
	pinned := spec
	pinned.Pins = &edited
	if pinned.Validate() == nil {
		t.Error("a spec with edited pins is valid")
	}
	pinned.Pins = a
	if err := pinned.Validate(); err != nil {
		t.Errorf("a spec with fresh pins: %v", err)
	}

	// The pins of one scenario do not fit a spec of another, but fit a window of it.
	for name, edit := range map[string]func(s *runner.Spec){
		"another seed":       func(s *runner.Spec) { s.Workload.Seed++ },
		"another run age":    func(s *runner.Spec) { s.Workload.CoalesceRuns, s.Workload.RunMaxAge = true, time.Hour },
		"another batch size": func(s *runner.Spec) { s.BatchSize++ },
	} {
		wrong := spec
		edit(&wrong)
		wrong.Pins = a
		if err := wrong.Validate(); err == nil || !strings.Contains(err.Error(), "another scenario") {
			t.Errorf("%s: pins of another scenario were accepted (%v)", name, err)
		}
	}
	window := runner.WindowSpec(spec, 1)
	window.Pins = a
	if err := window.Validate(); err != nil {
		t.Errorf("the pins of a scenario do not fit a window of it: %v", err)
	}
}

// A plan made from pins asks about the pinned prefixes and nothing else, and its
// digest and its stream are not the full stream's.
func TestAPlanFromPinsAsksAboutThePinsOnly(t *testing.T) {
	t.Parallel()
	conformance.SkipWhenTrimmed(t)

	spec := tinySpec()
	pins, err := runner.MakePins(context.Background(), spec)
	if err != nil {
		t.Fatal(err)
	}
	pinned := spec
	pinned.Pins = pins
	plan := mustPlan(t, pinned)
	full := mustPlan(t, spec)
	set := map[string]bool{}
	for _, pc := range pins.Classes {
		for _, list := range [][]identity.Fingerprint{pc.Records, pc.Extensions, pc.Median} {
			for _, fp := range list {
				set[fp.String()] = true
			}
		}
	}
	for _, q := range plan.Queries {
		for _, fp := range q.Fps {
			if !set[fp.String()] {
				t.Errorf("%s asks about %s, which is not pinned", q.Name(), fp)
			}
		}
		if q.Op == runner.OpNeighbors && q.Group[:3] == "pod" {
			t.Errorf("%s: a pod's own prefix is asked", q.Name())
		}
	}
	if len(plan.Queries) == 0 || plan.Stream.Digest == full.Stream.Digest || plan.Stream.Records >= full.Stream.Records {
		t.Errorf("the plan has %d queries, %d records of %d, and the full stream's digest: %v", len(plan.Queries), plan.Stream.Records, full.Stream.Records, plan.Stream.Digest == full.Stream.Digest)
	}
	d1, _ := plan.Digest()
	d2, _ := full.Digest()
	if d1 == d2 {
		t.Error("a plan with pins has the digest of one without")
	}
}

// A window of R days is a stream of 2R + 1.5 days with a retention each day from R + 1
// that keeps R, the last twelve hours before the end.
func TestWindowSpecShapes(t *testing.T) {
	t.Parallel()

	base := runner.DefaultSpec(workload.CI())
	for _, r := range []int{1, 2, 7, 14, 30} {
		s := runner.WindowSpec(base, r)
		day := 24 * time.Hour
		days := time.Duration(r) * day
		if s.Workload.Duration != 2*days+36*time.Hour {
			t.Errorf("R=%d: %s long, want %s", r, s.Workload.Duration, 2*days+36*time.Hour)
		}
		if len(s.Retentions) != r+1 {
			t.Fatalf("R=%d: %d retentions, want %d", r, len(s.Retentions), r+1)
		}
		for k, ret := range s.Retentions {
			if want := days + day + time.Duration(k)*day; ret.At != want || ret.Keep != days {
				t.Errorf("R=%d retention %d: at %s keep %s, want at %s keep %s", r, k, ret.At, ret.Keep, want, days)
			}
			if k > 0 && ret.Horizon(s.Workload.Start).Sub(s.Retentions[k-1].Horizon(s.Workload.Start)) != day {
				t.Errorf("R=%d retention %d: the horizon does not move by a day", r, k)
			}
		}
		if last := s.Retentions[len(s.Retentions)-1]; s.Workload.Duration-last.At != 12*time.Hour {
			t.Errorf("R=%d: the stream ends %s after the last retention, want 12h", r, s.Workload.Duration-last.At)
		}
		if err := s.Validate(); err != nil {
			t.Errorf("R=%d: %v", r, err)
		}
		// It changes the workload's length and retentions and nothing else.
		if s.BatchSize != base.BatchSize || s.CacheBytes != base.CacheBytes || s.Workload.Seed != base.Workload.Seed {
			t.Errorf("R=%d: the window changed more than the length and the retentions", r)
		}
	}
	if len(base.Retentions) != 1 { // the base is untouched
		t.Errorf("the base spec was changed: %v", base.Retentions)
	}
}

// startSink remembers when the run each node's existence is in at the end began.
type startSink struct{ starts map[string]time.Time }

func (s *startSink) Write(b []engine.Record) error {
	for _, r := range b {
		if r.Kind == lifecycle.Observe && r.TTL > 0 && r.Through.IsZero() && r.Subject.Kind == engine.SubjectEntity && r.Subject.A.Type() == catalog.K8sNode {
			s.starts[r.Subject.A.String()] = r.EventTime
		}
	}
	return nil
}
func (s *startSink) Retain(time.Time) error { return nil }

// The schedule of a window is what makes its runs the same age in every window: at the
// end the oldest run alive is R + 0.5 days old, whatever R is, because every run that
// began at the start restarts at the first retention that passes it, and the last
// retention (twelve hours before the end) does not pass the ones that began at the
// retention before.
func TestAWindowEndsWithItsOldestRunsHalfADayOlderThanItsRetention(t *testing.T) {
	t.Parallel()
	conformance.SkipWhenTrimmed(t)

	for _, r := range []int{1, 2, 3} {
		w := workload.Tiny()
		w.EventsPerSecond = 0.05
		w.CoalesceRuns, w.ExtendTTLFraction, w.OutageProbability = true, 0.5, 0
		spec := runner.WindowSpec(runner.DefaultSpec(w), r)
		spec.BatchSize = 20 // the retention comes within minutes of its instant
		sink := &startSink{starts: map[string]time.Time{}}
		info, err := runner.Drive(context.Background(), spec, sink)
		if err != nil {
			t.Fatal(err)
		}
		// Every node runs from the start of the stream, so every node's run at the end is
		// that old, not only the oldest: the reads of a window ask about the nodes.
		want := time.Duration(r)*24*time.Hour + 12*time.Hour
		if len(sink.starts) == 0 {
			t.Fatalf("R=%d: no node run", r)
		}
		for node, st := range sink.starts {
			if age := info.End.Sub(st); age < want-5*time.Minute || age > want {
				t.Errorf("R=%d: the run of node %s alive at the end is %s old, want %s", r, node, age, want)
			}
		}
	}
}
