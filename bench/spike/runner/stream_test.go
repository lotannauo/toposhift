package runner_test

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/lotannauo/toposhift/bench/spike/conformance"
	"github.com/lotannauo/toposhift/bench/spike/engine"
	"github.com/lotannauo/toposhift/bench/spike/runner"
	"github.com/lotannauo/toposhift/bench/spike/workload"
)

// tinySpec is twenty minutes of a handful of entities with a retention in the
// middle: small enough to build every candidate in a test.
func tinySpec() runner.Spec {
	s := runner.DefaultSpec(workload.Tiny())
	s.BatchSize = 64
	s.Retentions = []runner.Retention{{At: 12 * time.Minute, Keep: 6 * time.Minute}}
	s.MinNonEmpty = 0 // twenty minutes has no day to look back over
	return s
}

// recorder keeps what a stream was written as.
type recorder struct {
	batches  [][]engine.Record
	retained []time.Time
	// after is how many batches had been written when each retention came.
	after []int
	fail  error
}

func (r *recorder) Write(b []engine.Record) error {
	if r.fail != nil {
		return r.fail
	}
	r.batches = append(r.batches, append([]engine.Record(nil), b...))
	return nil
}

func (r *recorder) Retain(h time.Time) error {
	r.retained = append(r.retained, h)
	r.after = append(r.after, len(r.batches))
	return nil
}

func TestDriveShapesTheStream(t *testing.T) {
	t.Parallel()

	spec := tinySpec()
	var rec recorder
	info, err := runner.Drive(context.Background(), spec, &rec)
	if err != nil {
		t.Fatal(err)
	}

	var n, lastSeq uint64
	horizon := time.Time{}
	retained := 0
	for i, b := range rec.batches {
		if len(b) > spec.BatchSize || (len(b) < spec.BatchSize && i != len(rec.batches)-1 && retained == 0) {
			// Dropping records before the horizon can leave a short batch in the middle
			// once a retention has happened; before it every batch is full.
			t.Errorf("batch %d has %d records, want %d", i, len(b), spec.BatchSize)
		}
		for retained < len(rec.after) && rec.after[retained] == i {
			horizon = rec.retained[retained]
			retained++
		}
		for _, r := range b {
			if r.Seq <= lastSeq {
				t.Fatalf("seq %d after %d", r.Seq, lastSeq)
			}
			if r.EventTime.Before(horizon) {
				t.Fatalf("seq %d at %s was offered after the horizon %s", r.Seq, r.EventTime, horizon)
			}
			lastSeq = r.Seq
			n++
		}
	}
	var payload uint64
	for _, b := range rec.batches {
		for _, r := range b {
			payload += uint64(len(r.Payload))
		}
	}
	if payload != info.PayloadBytes || payload == 0 {
		t.Errorf("payload bytes %d, the stream says %d", payload, info.PayloadBytes)
	}
	if n != info.Records || lastSeq != info.LastSeq {
		t.Errorf("wrote %d records ending at %d, the stream says %d and %d", n, lastSeq, info.Records, info.LastSeq)
	}
	if want := spec.Workload.Start.Add(6 * time.Minute); len(rec.retained) != 1 || !rec.retained[0].Equal(want) || !info.Horizon.Equal(want) {
		t.Errorf("retentions %v, final horizon %s; want one at %s", rec.retained, info.Horizon, want)
	}
	if len(info.Retentions) != 1 || info.Retentions[0].LastSeq != info.TokenFloor || info.TokenFloor == 0 || info.TokenFloor >= info.LastSeq {
		t.Errorf("retention bookkeeping: %+v, token floor %d", info.Retentions, info.TokenFloor)
	}
	if info.OldToken < info.TokenFloor || info.OldToken > info.LastSeq {
		t.Errorf("old token %d is outside [%d, %d]", info.OldToken, info.TokenFloor, info.LastSeq)
	}

	// Everything the generator makes is either written or dropped.
	g, err := workload.New(spec.Workload)
	if err != nil {
		t.Fatal(err)
	}
	if total := uint64(len(g.All())); total != info.Records+info.Dropped {
		t.Errorf("the workload makes %d records, %d were written and %d dropped", total, info.Records, info.Dropped)
	}
}

func TestStreamDigestSeesEverythingItIsMeantTo(t *testing.T) {
	t.Parallel()

	digest := func(mod func(*runner.Spec)) string {
		t.Helper()
		s := tinySpec()
		mod(&s)
		info, err := runner.Drive(context.Background(), s, &recorder{})
		if err != nil {
			t.Fatal(err)
		}
		return info.Digest
	}
	base := digest(func(*runner.Spec) {})
	if again := digest(func(*runner.Spec) {}); again != base {
		t.Fatalf("the same spec gave %s, then %s", base, again)
	}
	for name, mod := range map[string]func(*runner.Spec){
		"seed":                   func(s *runner.Spec) { s.Workload.Seed++ },
		"retention":              func(s *runner.Spec) { s.Retentions[0].Keep = 3 * time.Minute },
		"no horizon":             func(s *runner.Spec) { s.Retentions = nil },
		"payload":                func(s *runner.Spec) { s.Workload.PayloadMax++ },
		"a nanosecond more kept": func(s *runner.Spec) { s.Retentions[0].Keep += time.Nanosecond },
	} {
		if got := digest(mod); got == base {
			t.Errorf("changing the %s did not change the digest", name)
		}
	}
	// The batch size is not part of what is written, only of how: the records are the same
	// and so is the digest, unless a retention lands between different records.
	if got := digest(func(s *runner.Spec) { s.Retentions, s.BatchSize = nil, 1000 }); got != digest(func(s *runner.Spec) { s.Retentions, s.BatchSize = nil, 7 }) {
		t.Errorf("batch size changed the digest of a stream with no retention")
	}
}

func TestDriveStopsAtTheFirstThingWrong(t *testing.T) {
	t.Parallel()

	boom := errors.New("boom")
	if _, err := runner.Drive(context.Background(), tinySpec(), &recorder{fail: boom}); !errors.Is(err, boom) || !strings.Contains(err.Error(), "seq") {
		t.Errorf("a failing sink: %v", err)
	}

	// A retention that the stream never reaches is a spec that cannot be run.
	late := tinySpec()
	late.Retentions = []runner.Retention{{At: late.Workload.Duration - time.Nanosecond, Keep: time.Minute}} // no record is stamped that late
	if _, err := runner.Drive(context.Background(), late, &recorder{}); err == nil || !strings.Contains(err.Error(), "ended before retention") {
		t.Errorf("a retention the stream never reaches: %v", err)
	}
}

func TestSpecValidate(t *testing.T) {
	t.Parallel()

	good := tinySpec()
	if err := good.Validate(); err != nil {
		t.Fatal(err)
	}
	for name, mod := range map[string]func(*runner.Spec){
		"version":              func(s *runner.Spec) { s.Version++ },
		"batch":                func(s *runner.Spec) { s.BatchSize = 0 },
		"hot":                  func(s *runner.Spec) { s.Hot = 0 },
		"median":               func(s *runner.Spec) { s.Median = 0 },
		"no cache":             func(s *runner.Spec) { s.CacheBytes = 0 },
		"two caches":           func(s *runner.Spec) { s.CacheFraction = 0.25 },
		"negative cache":       func(s *runner.Spec) { s.CacheBytes = -1 },
		"non-empty high":       func(s *runner.Spec) { s.MinNonEmpty = 1.5 },
		"workload":             func(s *runner.Spec) { s.Workload.Hosts = 0 },
		"keep zero":            func(s *runner.Spec) { s.Retentions[0].Keep = 0 },
		"retention at the end": func(s *runner.Spec) { s.Retentions[0].At = s.Workload.Duration },
		"retention past end":   func(s *runner.Spec) { s.Retentions[0].At = time.Hour },
		"keeps more than was":  func(s *runner.Spec) { s.Retentions[0].Keep = 13 * time.Minute },
		"out of order": func(s *runner.Spec) {
			s.Retentions = []runner.Retention{{At: 10 * time.Minute, Keep: time.Minute}, {At: 5 * time.Minute, Keep: time.Minute}}
		},
		"same instant": func(s *runner.Spec) {
			s.Retentions = []runner.Retention{{At: 10 * time.Minute, Keep: time.Minute}, {At: 10 * time.Minute, Keep: 5 * time.Minute}}
		},
		"same instant, horizon forward": func(s *runner.Spec) {
			s.Retentions = []runner.Retention{{At: 10 * time.Minute, Keep: time.Minute}, {At: 10 * time.Minute, Keep: 30 * time.Second}}
		},
		"horizon standing still": func(s *runner.Spec) {
			s.Retentions = []runner.Retention{{At: 10 * time.Minute, Keep: time.Minute}, {At: 11 * time.Minute, Keep: 2 * time.Minute}}
		},
		"keeps all": func(s *runner.Spec) { s.Retentions = []runner.Retention{{At: 5 * time.Minute, Keep: 5 * time.Minute}} },
		"horizon back": func(s *runner.Spec) {
			s.Retentions = []runner.Retention{{At: 10 * time.Minute, Keep: time.Minute}, {At: 11 * time.Minute, Keep: 5 * time.Minute}}
		},
	} {
		s := good
		s.Retentions = append([]runner.Retention(nil), good.Retentions...)
		mod(&s)
		if err := s.Validate(); err == nil {
			t.Errorf("%s: accepted", name)
		}
	}
}

// What a run is measured on is fixed before any run: these are the digests of the
// decision rules and of the spec of each preset. A change here changes what a
// result means, so it is made on purpose and said in the change, never as a side
// effect of editing the generator or the queries.
func TestFrozenDigests(t *testing.T) {
	t.Parallel()

	rules, err := runner.DefaultRules().Digest()
	if err != nil {
		t.Fatal(err)
	}
	if want := "3084fa037c27ebea4dbe5ddb51974d9ce85451b5e9792ad9c312f8e5b5aeaa1f"; rules != want {
		t.Errorf("decision rules digest %s, frozen at %s", rules, want)
	}
	for name, w := range map[string]workload.Config{"ci": workload.CI(), "week": workload.Week(), "month": workload.Month()} {
		d, err := runner.DefaultSpec(w).Digest()
		if err != nil {
			t.Fatal(err)
		}
		want := frozenSpecs[name]
		if d != want {
			t.Errorf("spec %s digest %s, frozen at %s", name, d, want)
		}
	}
}

// A workload option that is off is left out of the spec, so the plans made before it
// existed still hash to the digests they record and can be loaded and judged again.
func TestAnOptionThatIsOffLeavesTheSpecAsItWas(t *testing.T) {
	t.Parallel()

	off, err := json.Marshal(runner.DefaultSpec(workload.CI()))
	if err != nil {
		t.Fatal(err)
	}
	w := workload.CI()
	w.PayloadPad = 456
	on, err := json.Marshal(runner.DefaultSpec(w))
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(off), "PayloadPad") || !strings.Contains(string(on), `"PayloadPad":456`) {
		t.Errorf("a spec without a payload pad: %s\nwith one: %s", off, on)
	}
}

var frozenSpecs = map[string]string{
	"ci":    "8c2f064625127aa6639ba2000bc61dbf7edff8beaa3524cdde88b3d205364da9",
	"week":  "98e2c1bc2f168fd71c90f6e378036c912fcc3ae2d4b09e518bc2f32dc43faf9b",
	"month": "d27b21ee0724df48d35bc61c6043bd5ff243e15b7b50ef42e7d7f4d3730e40c4",
}

// The old snapshot is taken three quarters through the period, or halfway from
// the last retention to the end if that is later; its token is the sequence number
// reached just before, which is above the token at the last retention, and its
// instant is the token's own, after the horizon.
func TestOldSnapshotIsTakenAfterTheLastRetention(t *testing.T) {
	t.Parallel()

	// With coalesced runs the refreshes of a node are extensions whose event time is the
	// run's start and whose Through is the refresh: the instant a record takes effect is
	// its Through, and an instant that ignored it would be earlier than it need be.
	for variant, mod := range map[string]func(c *workload.Config){
		"plain": func(*workload.Config) {},
		"coalesced": func(c *workload.Config) {
			c.CoalesceRuns, c.ExtendTTLFraction = true, 0.5
		},
		"coalesced and backed up": func(c *workload.Config) {
			c.CoalesceRuns, c.ExtendTTLFraction = true, 0.5
			c.LateProbability, c.LateMeanDelay = 0, 0
			c.BacklogEvery, c.BacklogMeanDelay, c.BacklogSpan = 3*time.Minute, 90*time.Second, 2*time.Minute
		},
	} {
		for name, retentions := range map[string][]runner.Retention{
			"no retention":         nil,
			"early retention":      {{At: 8 * time.Minute, Keep: 4 * time.Minute}},
			"retention after":      {{At: 18 * time.Minute, Keep: 2 * time.Minute}},
			"two, the last late":   {{At: 6 * time.Minute, Keep: 3 * time.Minute}, {At: 17 * time.Minute, Keep: 5 * time.Minute}},
			"retention at the 3/4": {{At: 15 * time.Minute, Keep: 5 * time.Minute}},
		} {
			name := variant + ", " + name
			spec := tinySpec()
			mod(&spec.Workload)
			spec.Retentions = retentions
			var rec recorder
			info, err := runner.Drive(context.Background(), spec, &rec)
			if err != nil {
				t.Fatal(err)
			}
			wantAt := spec.Workload.Start.Add(spec.Workload.Duration / 4 * 3)
			if n := len(retentions); n > 0 {
				last := spec.Workload.Start.Add(retentions[n-1].At)
				if mid := last.Add(spec.Workload.Duration - retentions[n-1].At).Add(-(spec.Workload.Duration - retentions[n-1].At) / 2); mid.After(wantAt) {
					wantAt = mid
				}
			}
			// The batch that reaches the instant is the first whose last record does; the token
			// is the one before it, and the instant it is read at is the newest event time
			// the batches before it reached, or before the instant the first record from the
			// token on takes effect.
			var want, before uint64
			var frontier, unseen time.Time
			found := false
			for _, b := range rec.batches {
				if !found && !b[len(b)-1].EventTime.Before(wantAt) {
					want, found = before, true
				}
				if found {
					for _, r := range b {
						at := r.EventTime
						if r.Through.After(at) {
							at = r.Through
						}
						if unseen.IsZero() || at.Before(unseen) {
							unseen = at
						}
					}
					continue
				}
				before = b[len(b)-1].Seq
				for _, r := range b {
					if r.EventTime.After(frontier) {
						frontier = r.EventTime
					}
				}
			}
			wantRead := frontier
			if unseen.Add(-time.Nanosecond).Before(wantRead) {
				wantRead = unseen.Add(-time.Nanosecond)
			}
			if wantRead.Before(info.Horizon) { // a store answers from the horizon on
				wantRead = info.Horizon
			}
			if !info.OldAt.Equal(wantRead) || info.OldAt.After(wantAt) {
				t.Errorf("%s: the old snapshot is read at %s, want %s (the frontier before its token is %s and what arrives after it takes effect at %s; not after %s)", name, info.OldAt, wantRead, frontier, unseen, wantAt)
			}
			if info.OldAt.Before(info.Horizon) {
				t.Errorf("%s: the old snapshot is read at %s, before the horizon %s", name, info.OldAt, info.Horizon)
			}
			if info.OldToken != want {
				t.Errorf("%s: old token %d, want %d", name, info.OldToken, want)
			}
			if info.OldToken < info.TokenFloor {
				t.Errorf("%s: the old token %d is below the token at the last retention, %d", name, info.OldToken, info.TokenFloor)
			}
		}
	}
}

// A record that takes effect at the horizon itself (a run that began before it,
// restarted there) and comes after the old token would put the snapshot's instant
// before the horizon, which a store refuses: it is read at the horizon.
func TestOldSnapshotIsNotReadBeforeTheHorizon(t *testing.T) {
	t.Parallel()

	atHorizon := 0
	for _, batch := range []int{1, 7, 64, 100, 333} {
		w := workload.Small()
		w.Duration = 8 * time.Minute
		spec := runner.DefaultSpec(w)
		spec.BatchSize = batch
		spec.MinNonEmpty = 0
		spec.Retentions = []runner.Retention{{At: 5 * time.Minute, Keep: 3 * time.Minute}}
		info, err := runner.Drive(context.Background(), spec)
		if err != nil {
			t.Fatal(err)
		}
		if info.OldAt.Before(info.Horizon) {
			t.Errorf("batch %d: the old snapshot is read at %s, before the horizon %s", batch, info.OldAt, info.Horizon)
		}
		if info.OldAt.Equal(info.Horizon) {
			atHorizon++
		}
	}
	if atHorizon == 0 {
		t.Error("no batch size puts the instant at the horizon: the stream does not test the rule")
	}
}

// A pipeline that is backed up at the old token has delivered the refreshes of an
// edge only up to a while ago, so the snapshot is read at an instant before every
// record from the token on takes effect, whatever the lateness model. Without it a
// read at the newest instant finds the refreshed edges lapsed.
func TestOldSnapshotIsReadBeforeAnythingAfterItTakesEffect(t *testing.T) {
	t.Parallel()

	for name, mod := range map[string]func(c *workload.Config){
		"per-record lateness": func(c *workload.Config) {},
		"a backed up pipeline": func(c *workload.Config) {
			c.LateProbability, c.LateMeanDelay = 0, 0
			c.BacklogEvery, c.BacklogMeanDelay, c.BacklogSpan = 3*time.Minute, 90*time.Second, 2*time.Minute
		},
	} {
		spec := tinySpec()
		spec.Retentions = nil
		mod(&spec.Workload)
		var rec recorder
		info, err := runner.Drive(context.Background(), spec, &rec)
		if err != nil {
			t.Fatal(err)
		}
		var frontier time.Time
		behind := 0 // records after the token that take effect no later than the newest instant before it
		for _, b := range rec.batches {
			if b[0].Seq <= info.OldToken {
				for _, r := range b {
					if r.EventTime.After(frontier) {
						frontier = r.EventTime
					}
				}
				continue
			}
			for _, r := range b {
				at := r.EventTime
				if r.Through.After(at) {
					at = r.Through
				}
				if !at.After(info.OldAt) {
					t.Errorf("%s: a record at seq %d takes effect at %s, not after the old snapshot's instant %s", name, r.Seq, at, info.OldAt)
				}
				if !at.After(frontier) {
					behind++
				}
			}
		}
		if behind == 0 {
			t.Errorf("%s: no record after the token takes effect before the newest instant it saw, %s: the stream does not test the rule", name, frontier)
		}
	}
}

// After a retention, a coalesced run is continued by a new one rather than
// extended at its start, which is before the horizon, so with nothing late
// nothing is dropped.
func TestDriveTellsTheGeneratorWhereTheHorizonIs(t *testing.T) {
	t.Parallel()

	spec := tinySpec()
	spec.Workload.CoalesceRuns = true
	spec.Workload.LateProbability = 0
	spec.Retentions = []runner.Retention{{At: 12 * time.Minute, Keep: 6 * time.Minute}}
	info, err := runner.Drive(context.Background(), spec, &recorder{})
	if err != nil {
		t.Fatal(err)
	}
	if info.Dropped != 0 || info.Records == 0 {
		t.Errorf("%d records written and %d dropped: the generator extended runs that started before the horizon", info.Records, info.Dropped)
	}
}

// The questions asked of a stream are fixed with the rules and the spec: the
// digest of the questions and of the plan for a small stream, whose choice of
// prefixes, ages and reads is everything the full-size plans are made from. A
// change here changes what every comparison asks, so it is made on purpose.
func TestFrozenQueries(t *testing.T) {
	t.Parallel()

	for _, c := range []struct {
		name         string
		spec         runner.Spec
		skipTrimmed  bool
		queries      string
		plan         string
		wantQueryNum int
	}{
		{"tiny", tinySpec(), false, frozenTiny.queries, frozenTiny.plan, frozenTiny.n},
		{"an hour", hourSpec(), true, frozenHour.queries, frozenHour.plan, frozenHour.n},
	} {
		t.Run(c.name, func(t *testing.T) {
			t.Parallel()
			if c.skipTrimmed {
				conformance.SkipWhenTrimmed(t)
			}
			p := mustPlan(t, c.spec)
			d, err := p.Digest()
			if err != nil {
				t.Fatal(err)
			}
			if p.QueriesDigest != c.queries || d != c.plan || len(p.Queries) != c.wantQueryNum {
				t.Errorf("%d queries with digest %s, plan digest %s; frozen at %d, %s and %s",
					len(p.Queries), p.QueriesDigest, d, c.wantQueryNum, c.queries, c.plan)
			}
		})
	}
}

var (
	frozenTiny = struct {
		queries, plan string
		n             int
	}{"15d6b98f645994902b942f3f5e3ea314b835211dfb0688a6663c23d3b5040bbb", "b7e5d0a4fbfa236074625b53f3c937d6fcd68d3fc4aa65bb9c3f2a8723636862", 193}
	frozenHour = struct {
		queries, plan string
		n             int
	}{"2902eb347c8b4c17259ba3d450249bf5703e0912ebddd2acfa5813d4af35896c", "e606c5d9b15c5e361a2bdab9d3a0daf2dff27509caac9b4c1c4045dd1c5f80c1", 364}
)

// Every placeholder names a field of the rules (Timing* stands for the fields
// that start so), and the rules are coherent with each other.
func TestRulesPlaceholdersNameFields(t *testing.T) {
	t.Parallel()

	r := runner.DefaultRules()
	typ := reflect.TypeOf(r)
	has := func(name string) bool {
		if prefix, ok := strings.CutSuffix(name, "*"); ok {
			for i := range typ.NumField() {
				if strings.HasPrefix(typ.Field(i).Name, prefix) {
					return true
				}
			}
			return false
		}
		_, ok := typ.FieldByName(name)
		return ok
	}
	for _, p := range r.Placeholders {
		if !has(p) {
			t.Errorf("placeholder %q is not a field of the rules", p)
		}
	}
	if r.Windows[len(r.Windows)-1] != r.TargetWindow || !slices.IsSorted(r.Windows) {
		t.Errorf("windows %v end at %d, not at the target window %d", r.Windows, r.Windows[len(r.Windows)-1], r.TargetWindow)
	}
	for _, c := range r.G1Counters {
		if !slices.Contains(r.OrderCounters, c) {
			t.Errorf("G1 judges %q, which does not order cells", c)
		}
	}
	for _, c := range r.OrderCounters {
		if r.CounterFloors[c] == 0 {
			t.Errorf("counter %q has no floor", c)
		}
	}
	if r.BoundedSlope >= r.LinearSlope || r.OrderTolerance <= 1 || r.TimingMinRatio <= 1 {
		t.Errorf("incoherent thresholds: %+v", r)
	}
}

// A step that is told to stop stops between batches, with the reason.
func TestDriveStopsWhenToldTo(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	n := 0
	stopper := sinkFunc(func([]engine.Record) error {
		if n++; n == 3 {
			cancel()
		}
		return nil
	})
	if _, err := runner.Drive(ctx, tinySpec(), stopper); !errors.Is(err, context.Canceled) || n != 3 {
		t.Errorf("a stream cancelled at its third batch: %v after %d batches", err, n)
	}
	if _, err := runner.MakePlan(ctx, tinySpec(), nil); !errors.Is(err, context.Canceled) {
		t.Errorf("a plan made with a cancelled context: %v", err)
	}
}

type sinkFunc func([]engine.Record) error

func (f sinkFunc) Write(b []engine.Record) error { return f(b) }
func (sinkFunc) Retain(time.Time) error          { return nil }

// However the batches fall, the token of the old snapshot is never below the token
// at the last retention (below which a store promises nothing): a stream either
// comes out right or is refused.
func TestOldTokenIsNeverBelowTheFloor(t *testing.T) {
	t.Parallel()

	refused, ok := 0, 0
	for batch := 50; batch <= 3000; batch += 49 {
		spec := tinySpec()
		spec.BatchSize = batch
		spec.Retentions = []runner.Retention{{At: 17*time.Minute + 59*time.Second, Keep: 3 * time.Minute}}
		info, err := runner.Drive(context.Background(), spec, &recorder{})
		if err != nil {
			refused++
			if !strings.Contains(err.Error(), "old snapshot") && !strings.Contains(err.Error(), "ended before retention") {
				t.Fatalf("batch %d: %v", batch, err)
			}
			continue
		}
		ok++
		if info.OldToken < info.TokenFloor {
			t.Fatalf("batch %d: the old token %d is below the token at the last retention %d", batch, info.OldToken, info.TokenFloor)
		}
	}
	if ok == 0 {
		t.Error("no batch size gave a stream")
	}
}
