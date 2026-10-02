package lifecycle_test

import (
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/lotannauo/toposhift/internal/catalog"
	"github.com/lotannauo/toposhift/internal/identity"
	"github.com/lotannauo/toposhift/internal/lifecycle"
)

const minute = time.Minute

func at(d time.Duration) time.Time { return base.Add(d) }

func attr(k string, v any) identity.Attr {
	return identity.Attr{Key: catalog.AttributeKey(k), Value: v}
}

func obs(p lifecycle.Producer, seq uint64, t, ttl time.Duration, attrs ...identity.Attr) lifecycle.Assertion {
	return lifecycle.Assertion{Producer: p, EventTime: at(t), Seq: seq, Kind: lifecycle.Observe, TTL: ttl, Attrs: attrs}
}

// run is an Observe that stands for refreshes every ttl or sooner from t
// through end.
func run(p lifecycle.Producer, seq uint64, t, end, ttl time.Duration, attrs ...identity.Attr) lifecycle.Assertion {
	a := obs(p, seq, t, ttl, attrs...)
	a.Through = at(end)
	return a
}

func del(p lifecycle.Producer, seq uint64, t time.Duration) lifecycle.Assertion {
	return lifecycle.Assertion{Producer: p, EventTime: at(t), Seq: seq, Kind: lifecycle.Delete}
}

// open marks an interval with no end.
const open = time.Duration(-1)

func iv(start, end time.Duration, src lifecycle.EndSource) lifecycle.Interval {
	i := lifecycle.Interval{Start: at(start), EndSource: src}
	if end != open {
		i.End = at(end)
	}
	return i
}

func fold(t *testing.T, pol lifecycle.Policy, as ...lifecycle.Assertion) lifecycle.Timeline {
	t.Helper()
	tl, err := lifecycle.Fold(as, pol)
	if err != nil {
		t.Fatalf("Fold: %v", err)
	}
	return tl
}

func wantExistence(t *testing.T, tl lifecycle.Timeline, want ...lifecycle.Interval) {
	t.Helper()
	got := tl.Existence()
	if !sameIntervals(got, want) {
		t.Errorf("existence =\n  %v\nwant\n  %v", fmtIntervals(got), fmtIntervals(want))
	}
}

func fmtIntervals(ivs []lifecycle.Interval) string {
	var parts []string
	for _, i := range ivs {
		end := "open"
		if !i.Open() {
			end = i.End.Sub(base).String()
		}
		parts = append(parts, "["+i.Start.Sub(base).String()+", "+end+") "+i.EndSource.String())
	}
	return strings.Join(parts, "; ")
}

func TestExistenceScenarios(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		as   []lifecycle.Assertion
		want []lifecycle.Interval
	}{
		{
			// The worked example in the design: refreshed at 3 minutes, then silent.
			"expires at the last deadline",
			[]lifecycle.Assertion{obs("P", 1, 0, 5*minute), obs("P", 2, 3*minute, 5*minute)},
			[]lifecycle.Interval{iv(0, 8*minute, lifecycle.EndLivenessExpiry)},
		},
		{
			"explicit delete ends it",
			[]lifecycle.Assertion{obs("P", 1, 0, 5*minute), obs("P", 2, 3*minute, 5*minute), del("P", 3, 4*minute)},
			[]lifecycle.Interval{iv(0, 4*minute, lifecycle.EndProducer)},
		},
		{
			"another producer keeps it alive through the first one's silence",
			[]lifecycle.Assertion{obs("P", 1, 0, 5*minute), obs("Q", 2, 1*minute, 0), del("Q", 3, 20*minute)},
			[]lifecycle.Interval{iv(0, 20*minute, lifecycle.EndProducer)},
		},
		{
			// The flap that reference counting exists to prevent: A releases,
			// B still sees it, so nothing ends.
			"one producer's delete does not remove what another still sees",
			[]lifecycle.Assertion{obs("A", 1, 0, 0), obs("B", 2, 1*minute, 0), del("A", 3, 5*minute)},
			[]lifecycle.Interval{iv(0, open, lifecycle.EndUnknown)},
		},
		{
			"it ends when the last producer releases",
			[]lifecycle.Assertion{obs("A", 1, 0, 0), obs("B", 2, 1*minute, 0), del("A", 3, 5*minute), del("B", 4, 10*minute)},
			[]lifecycle.Interval{iv(0, 10*minute, lifecycle.EndProducer)},
		},
		{
			"watch mode never lapses",
			[]lifecycle.Assertion{obs("W", 1, 0, 0)},
			[]lifecycle.Interval{iv(0, open, lifecycle.EndUnknown)},
		},
		{
			"silence then return is two lives",
			[]lifecycle.Assertion{obs("P", 1, 0, 5*minute), obs("P", 2, 20*minute, 5*minute)},
			[]lifecycle.Interval{iv(0, 5*minute, lifecycle.EndLivenessExpiry), iv(20*minute, 25*minute, lifecycle.EndLivenessExpiry)},
		},
		{
			"a refresh exactly at the deadline continues without a gap",
			[]lifecycle.Assertion{obs("P", 1, 0, 5*minute), obs("P", 2, 5*minute, 5*minute)},
			[]lifecycle.Interval{iv(0, 10*minute, lifecycle.EndLivenessExpiry)},
		},
		{
			"a refresh one nanosecond late leaves a gap",
			[]lifecycle.Assertion{obs("P", 1, 0, 5*minute), obs("P", 2, 5*minute+1, 5*minute)},
			[]lifecycle.Interval{iv(0, 5*minute, lifecycle.EndLivenessExpiry), iv(5*minute+1, 10*minute+1, lifecycle.EndLivenessExpiry)},
		},
		{
			"a later observation replaces the deadline, it does not extend it",
			[]lifecycle.Assertion{obs("P", 1, 0, 10*minute), obs("P", 2, 2*minute, 1*minute)},
			[]lifecycle.Interval{iv(0, 3*minute, lifecycle.EndLivenessExpiry)},
		},
		{
			"a delete after the deadline changes nothing",
			[]lifecycle.Assertion{obs("P", 1, 0, 5*minute), del("P", 2, 7*minute)},
			[]lifecycle.Interval{iv(0, 5*minute, lifecycle.EndLivenessExpiry)},
		},
		{
			"a delete at the deadline is still the producer's word",
			[]lifecycle.Assertion{obs("P", 1, 0, 5*minute), del("P", 2, 5*minute)},
			[]lifecycle.Interval{iv(0, 5*minute, lifecycle.EndProducer)},
		},
		{
			"a delete with nothing to release never existed",
			[]lifecycle.Assertion{del("P", 1, 0)},
			nil,
		},
		{
			"observe then delete at one instant is zero length",
			[]lifecycle.Assertion{obs("P", 1, 3*minute, 5*minute), del("P", 2, 3*minute)},
			nil,
		},
		{
			"delete then observe at one instant exists",
			[]lifecycle.Assertion{del("P", 1, 3*minute), obs("P", 2, 3*minute, 5*minute)},
			[]lifecycle.Interval{iv(3*minute, 8*minute, lifecycle.EndLivenessExpiry)},
		},
		{
			"an end at one instant: the explicit delete outranks the deadline",
			[]lifecycle.Assertion{obs("P", 1, 0, 5*minute), obs("Q", 2, 0, 0), del("Q", 3, 5*minute)},
			[]lifecycle.Interval{iv(0, 5*minute, lifecycle.EndProducer)},
		},
		{
			"a replaced reference that is then deleted at once ended by the producer",
			[]lifecycle.Assertion{obs("P", 1, 0, 5*minute), obs("P", 2, 3*minute, 5*minute), del("P", 3, 3*minute)},
			[]lifecycle.Interval{iv(0, 3*minute, lifecycle.EndProducer)},
		},
		{
			"a run keeps the reference alive through its last refresh plus the TTL",
			[]lifecycle.Assertion{run("P", 1, 0, 20*minute, 5*minute)},
			[]lifecycle.Interval{iv(0, 25*minute, lifecycle.EndLivenessExpiry)},
		},
		{
			"a later version of a run, at the same event time, replaces the earlier one",
			[]lifecycle.Assertion{run("P", 1, 0, 10*minute, 5*minute), run("P", 2, 0, 30*minute, 5*minute)},
			[]lifecycle.Interval{iv(0, 35*minute, lifecycle.EndLivenessExpiry)},
		},
		{
			// Found by the property tests: a later version at the same event
			// time replaces the earlier one entirely, including its Through.
			"a later version of a run can also shorten it",
			[]lifecycle.Assertion{run("P", 1, 0, 30*minute, 5*minute), obs("P", 2, 0, 5*minute)},
			[]lifecycle.Interval{iv(0, 5*minute, lifecycle.EndLivenessExpiry)},
		},
		{
			"a delete inside a run ends it there",
			[]lifecycle.Assertion{run("P", 1, 0, 20*minute, 5*minute), del("P", 2, 8*minute)},
			[]lifecycle.Interval{iv(0, 8*minute, lifecycle.EndProducer)},
		},
		{
			"a later observation inside a run replaces it",
			[]lifecycle.Assertion{run("P", 1, 0, 20*minute, 5*minute), obs("P", 2, 8*minute, 1*minute)},
			[]lifecycle.Interval{iv(0, 9*minute, lifecycle.EndLivenessExpiry)},
		},
		{
			"an unbounded reference swallows a bounded one",
			[]lifecycle.Assertion{obs("P", 1, 0, 5*minute), obs("W", 2, 2*minute, 0)},
			[]lifecycle.Interval{iv(0, open, lifecycle.EndUnknown)},
		},
		{
			"overlapping bounded references extend to the later deadline",
			[]lifecycle.Assertion{obs("P", 1, 0, 5*minute), obs("Q", 2, 3*minute, 5*minute)},
			[]lifecycle.Interval{iv(0, 8*minute, lifecycle.EndLivenessExpiry)},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			wantExistence(t, fold(t, lifecycle.Policy{}, tt.as...), tt.want...)
		})
	}
}

func TestAliveAtBoundaries(t *testing.T) {
	t.Parallel()

	tl := fold(t, lifecycle.Policy{}, obs("P", 1, 0, 5*minute), obs("P", 2, 3*minute, 5*minute))
	for _, tt := range []struct {
		at   time.Duration
		want bool
	}{
		{-1, false}, {0, true}, {5 * minute, true}, {8*minute - 1, true}, {8 * minute, false}, {time.Hour, false},
	} {
		if got := tl.AliveAt(at(tt.at)); got != tt.want {
			t.Errorf("AliveAt(%s) = %v, want %v", tt.at, got, tt.want)
		}
	}

	var zero lifecycle.Timeline
	if zero.AliveAt(at(0)) || zero.Existence() != nil || zero.Boots() != nil {
		t.Error("the zero timeline describes a subject that existed")
	}
}

// TestLateAssertionsRewriteHistory is why expiry is derived and not stored:
// an assertion that arrives late, with an earlier event time, changes what was
// true, and nothing recorded in advance has to be corrected.
func TestLateAssertionsRewriteHistory(t *testing.T) {
	t.Parallel()

	t.Run("a late observation fills a gap", func(t *testing.T) {
		t.Parallel()
		known := []lifecycle.Assertion{obs("P", 1, 0, 5*minute), obs("P", 2, 20*minute, 5*minute)}
		wantExistence(t, fold(t, lifecycle.Policy{}, known...),
			iv(0, 5*minute, lifecycle.EndLivenessExpiry), iv(20*minute, 25*minute, lifecycle.EndLivenessExpiry))

		// Arrives last (highest Seq), but happened at 5 minutes with a long TTL.
		late := append(known, obs("P", 3, 5*minute, 15*minute))
		wantExistence(t, fold(t, lifecycle.Policy{}, late...), iv(0, 25*minute, lifecycle.EndLivenessExpiry))
	})

	t.Run("a late delete ends an interval retroactively", func(t *testing.T) {
		t.Parallel()
		known := []lifecycle.Assertion{obs("P", 1, 0, 0)}
		wantExistence(t, fold(t, lifecycle.Policy{}, known...), iv(0, open, lifecycle.EndUnknown))

		late := append(known, del("P", 2, 7*minute))
		wantExistence(t, fold(t, lifecycle.Policy{}, late...), iv(0, 7*minute, lifecycle.EndProducer))
	})

	t.Run("a late observation revives a subject for the time it was seen", func(t *testing.T) {
		t.Parallel()
		known := []lifecycle.Assertion{obs("P", 1, 0, 2*minute)}
		late := append(known, obs("Q", 2, 10*minute, 1*minute))
		wantExistence(t, fold(t, lifecycle.Policy{}, late...),
			iv(0, 2*minute, lifecycle.EndLivenessExpiry), iv(10*minute, 11*minute, lifecycle.EndLivenessExpiry))
	})
}

func TestSnapshotTokens(t *testing.T) {
	t.Parallel()

	as := []lifecycle.Assertion{obs("P", 1, 0, 0), del("P", 5, 7*minute)}
	// A query pinned before the delete arrived still sees the subject alive,
	// even though the delete's event time is earlier than the query's instant.
	wantExistence(t, fold(t, lifecycle.Policy{}, lifecycle.Visible(as, 4)...), iv(0, open, lifecycle.EndUnknown))
	wantExistence(t, fold(t, lifecycle.Policy{}, lifecycle.Visible(as, 5)...), iv(0, 7*minute, lifecycle.EndProducer))
}

func TestFoldDoesNotModifyItsInput(t *testing.T) {
	t.Parallel()

	as := []lifecycle.Assertion{
		obs("B", 2, 5*minute, 0, attr("a", 1)),
		obs("A", 1, 0, 5*minute, attr("a", 2), attr("b", 3)),
	}
	before := []lifecycle.Assertion{
		obs("B", 2, 5*minute, 0, attr("a", 1)),
		obs("A", 1, 0, 5*minute, attr("a", 2), attr("b", 3)),
	}
	tl := fold(t, lifecycle.Policy{Rank: map[lifecycle.Producer]int{"A": 1}}, as...)
	for i := range as {
		if as[i].Producer != before[i].Producer || !as[i].EventTime.Equal(before[i].EventTime) ||
			len(as[i].Attrs) != len(before[i].Attrs) {
			t.Fatalf("Fold changed its input at %d: %+v", i, as[i])
		}
	}

	// And the timeline owns its data: mutating what Existence returns, or the
	// input afterwards, cannot change later answers.
	ex := tl.Existence()
	ex[0].Start = at(99 * minute)
	as[1].TTL = 0
	as[1].Attrs[0].Value = "changed"
	if !tl.AliveAt(at(0)) {
		t.Error("mutating a returned slice changed the timeline")
	}
	if d, _ := tl.DescribeAt(at(1 * minute)); d["a"].Value != 2 {
		t.Errorf("mutating the input attrs changed the description: %v", d)
	}
}

func TestExistenceIsUTC(t *testing.T) {
	t.Parallel()

	cet := time.FixedZone("CET", 3600)
	a := obs("P", 1, 0, 5*minute)
	a.EventTime = a.EventTime.In(cet)
	tl := fold(t, lifecycle.Policy{}, a)
	if loc := tl.Existence()[0].Start.Location(); loc != time.UTC {
		t.Errorf("interval is in %v, want UTC", loc)
	}
}

func TestValidationRejects(t *testing.T) {
	t.Parallel()

	good := obs("P", 1, 0, 5*minute)
	tests := []struct {
		name   string
		as     []lifecycle.Assertion
		pol    lifecycle.Policy
		offend string
	}{
		{"empty producer", []lifecycle.Assertion{obs("", 1, 0, 0)}, lifecycle.Policy{}, "producer is empty"},
		{"unset event time", []lifecycle.Assertion{{Producer: "P", Seq: 1, Kind: lifecycle.Observe}}, lifecycle.Policy{}, "event time is unset"},
		{"negative TTL", []lifecycle.Assertion{obs("P", 1, 0, -time.Second)}, lifecycle.Policy{}, "negative TTL"},
		{"unset kind", []lifecycle.Assertion{{Producer: "P", EventTime: at(0), Seq: 1}}, lifecycle.Policy{}, "Kind(0)"},
		{"unknown kind", []lifecycle.Assertion{{Producer: "P", EventTime: at(0), Seq: 1, Kind: 9}}, lifecycle.Policy{}, "Kind(9)"},
		{"delete with TTL", []lifecycle.Assertion{{Producer: "P", EventTime: at(0), Seq: 1, Kind: lifecycle.Delete, TTL: minute}}, lifecycle.Policy{}, "delete cannot carry a TTL"},
		{"delete with attributes", []lifecycle.Assertion{{Producer: "P", EventTime: at(0), Seq: 1, Kind: lifecycle.Delete, Attrs: []identity.Attr{attr("a", 1)}}}, lifecycle.Policy{}, "delete cannot carry attributes"},
		{"duplicate seq", []lifecycle.Assertion{good, obs("Q", 1, 1*minute, 0)}, lifecycle.Policy{}, "seq already used by assertion 0"},
		{"duplicate attribute", []lifecycle.Assertion{obs("P", 1, 0, 0, attr("a", 1), attr("a", 2))}, lifecycle.Policy{}, `attribute "a" appears twice`},
		{"boot ID not a string", []lifecycle.Assertion{obs("P", 1, 0, 0, identity.Attr{Key: lifecycle.BootID, Value: 7})}, lifecycle.Policy{BootKey: lifecycle.BootID}, "non-empty string"},
		{"boot ID empty", []lifecycle.Assertion{obs("P", 1, 0, 0, identity.Attr{Key: lifecycle.BootID, Value: ""})}, lifecycle.Policy{BootKey: lifecycle.BootID}, "non-empty string"},
		{"boot ID whitespace", []lifecycle.Assertion{obs("P", 1, 0, 0, identity.Attr{Key: lifecycle.BootID, Value: " \t"})}, lifecycle.Policy{BootKey: lifecycle.BootID}, "non-empty string"},
		{"through before the event time", []lifecycle.Assertion{run("P", 1, 5*minute, 2*minute, minute)}, lifecycle.Policy{}, "precedes the event time"},
		{"delete with a through time", []lifecycle.Assertion{{Producer: "P", EventTime: at(0), Seq: 1, Kind: lifecycle.Delete, Through: at(minute)}}, lifecycle.Policy{}, "delete cannot carry a through time"},
		{"negative skew", []lifecycle.Assertion{good}, lifecycle.Policy{Skew: -time.Second}, "negative skew"},
		{"bad boot key", []lifecycle.Assertion{good}, lifecycle.Policy{BootKey: "Boot ID"}, "not a valid attribute name"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			tl, err := lifecycle.Fold(tt.as, tt.pol)
			if !errors.Is(err, lifecycle.ErrInvalid) {
				t.Fatalf("err = %v, want ErrInvalid", err)
			}
			if !strings.Contains(err.Error(), tt.offend) {
				t.Errorf("error does not mention %q: %v", tt.offend, err)
			}
			if tl.AliveAt(at(0)) || tl.Existence() != nil {
				t.Error("a rejected fold returned a timeline")
			}
		})
	}
}

func TestValidationReportsEveryViolation(t *testing.T) {
	t.Parallel()

	_, err := lifecycle.Fold([]lifecycle.Assertion{
		obs("", 1, 0, 0),
		obs("P", 1, 0, -time.Second),
	}, lifecycle.Policy{})
	for _, want := range []string{"producer is empty", "seq already used", "negative TTL"} {
		if err == nil || !strings.Contains(err.Error(), want) {
			t.Errorf("error does not mention %q: %v", want, err)
		}
	}
}

func TestEnumStrings(t *testing.T) {
	t.Parallel()

	for _, tt := range []struct{ got, want string }{
		{lifecycle.Observe.String(), "observe"},
		{lifecycle.Delete.String(), "delete"},
		{lifecycle.Kind(0).String(), "Kind(0)"},
		{lifecycle.EndUnknown.String(), "unknown"},
		{lifecycle.EndProducer.String(), "producer"},
		{lifecycle.EndLivenessExpiry.String(), "liveness_expiry"},
		{lifecycle.EndCascade.String(), "cascade"},
		{lifecycle.EndRetention.String(), "retention"},
		{lifecycle.EndOperator.String(), "operator"},
		{lifecycle.EndSource(99).String(), "EndSource(99)"},
	} {
		if tt.got != tt.want {
			t.Errorf("String() = %q, want %q", tt.got, tt.want)
		}
	}
}
