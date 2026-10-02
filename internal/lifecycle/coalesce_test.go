package lifecycle_test

import (
	"errors"
	"testing"
	"time"

	"pgregory.net/rapid"

	"github.com/lotannauo/toposhift/internal/identity"
	"github.com/lotannauo/toposhift/internal/lifecycle"
)

// heartbeats is a producer refreshing the same description every step for
// count beats, with the given TTL, starting at seq.
func heartbeats(p lifecycle.Producer, seq uint64, start, step time.Duration, count int, ttl time.Duration, attrs ...any) []lifecycle.Assertion {
	var out []lifecycle.Assertion
	for i := range count {
		var as []attrPair
		for j := 0; j+1 < len(attrs); j += 2 {
			as = append(as, attrPair{attrs[j].(string), attrs[j+1]})
		}
		a := obs(p, seq+uint64(i), start+time.Duration(i)*step, ttl)
		for _, pr := range as {
			a.Attrs = append(a.Attrs, attr(pr.k, pr.v))
		}
		out = append(out, a)
	}
	return out
}

type attrPair struct {
	k string
	v any
}

func TestCoalesceCollapsesARunIntoOneAssertion(t *testing.T) {
	t.Parallel()

	// One day of minute heartbeats from one producer with a three-minute TTL.
	as := heartbeats("node", 1, 0, minute, 1440, 3*minute, "host.name", "web-1")
	got := lifecycle.Coalesce(as)
	if len(got) != 1 {
		t.Fatalf("a day of heartbeats coalesced to %d assertions, want 1", len(got))
	}
	run := got[0]
	if !run.EventTime.Equal(at(0)) || !run.Through.Equal(at(1439*minute)) || run.Seq != 1440 || run.TTL != 3*minute {
		t.Errorf("run = %+v, want from 0 through 1439m, seq 1440, TTL 3m", run)
	}
	// And the answers are the same.
	want := iv(0, 1439*minute+3*minute, lifecycle.EndLivenessExpiry)
	wantExistence(t, fold(t, lifecycle.Policy{}, got...), want)
	wantExistence(t, fold(t, lifecycle.Policy{}, as...), want)
}

func TestCoalesceKeepsWhatChangesSomething(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		as   []lifecycle.Assertion
		want int // assertions left
	}{
		{
			"an attribute change starts a new run",
			append(heartbeats("n", 1, 0, minute, 5, 3*minute, "v", 1), heartbeats("n", 10, 5*minute, minute, 5, 3*minute, "v", 2)...), 2,
		},
		{
			"a gap longer than the TTL starts a new run",
			append(heartbeats("n", 1, 0, minute, 5, 3*minute, "v", 1), heartbeats("n", 10, 20*minute, minute, 5, 3*minute, "v", 1)...), 2,
		},
		{
			"a refresh exactly at the deadline continues the run",
			[]lifecycle.Assertion{obs("n", 1, 0, 3*minute, attr("v", 1)), obs("n", 2, 3*minute, 3*minute, attr("v", 1))},
			1,
		},
		{
			"a refresh one nanosecond past the deadline does not",
			[]lifecycle.Assertion{obs("n", 1, 0, 3*minute, attr("v", 1)), obs("n", 2, 3*minute+1, 3*minute, attr("v", 1))},
			2,
		},
		{
			// Found by the property tests: such an assertion replaces the run's
			// claim from its own instant on, and may shorten it.
			"an assertion inside a run's span replaces it, so it is not merged",
			[]lifecycle.Assertion{
				run("n", 1, 0, 4*minute, 5*minute, attr("v", 1)),
				obs("n", 2, minute, 5*minute, attr("v", 1)),
			},
			2,
		},
		{
			"an overwritten assertion is dropped before runs are formed",
			[]lifecycle.Assertion{
				obs("n", 1, 0, 3*minute, attr("v", 1)),
				obs("n", 2, 0, 3*minute, attr("v", 2)), // replaces the first at the same instant
				obs("n", 3, minute, 3*minute, attr("v", 2)),
			},
			1,
		},
		{
			"a delete ends a run and is kept",
			append(heartbeats("n", 1, 0, minute, 5, 3*minute, "v", 1), del("n", 10, 5*minute)), 2,
		},
		{
			"a TTL change starts a new run",
			[]lifecycle.Assertion{
				obs("n", 1, 0, 3*minute, attr("v", 1)), obs("n", 2, minute, 5*minute, attr("v", 1)),
				obs("n", 3, 2*minute, 5*minute, attr("v", 1)), obs("n", 4, 3*minute, 5*minute, attr("v", 1)),
			},
			2,
		},
		{
			"watch mode: identical refreshes with no TTL are one assertion",
			heartbeats("w", 1, 0, minute, 10, 0, "v", 1), 1,
		},
		{
			"watch mode and bounded refreshes do not mix",
			[]lifecycle.Assertion{obs("n", 1, 0, 0, attr("v", 1)), obs("n", 2, minute, 3*minute, attr("v", 1))},
			2,
		},
		{
			"the same description from two producers is two runs",
			append(heartbeats("a", 1, 0, minute, 5, 3*minute, "v", 1), heartbeats("b", 10, 0, minute, 5, 3*minute, "v", 1)...), 2,
		},
		{"a single assertion is untouched", heartbeats("n", 1, 0, minute, 1, 3*minute, "v", 1), 1},
		{
			"attribute order does not matter",
			[]lifecycle.Assertion{
				obs("n", 1, 0, 3*minute, attr("a", 1), attr("b", 2)), obs("n", 2, minute, 3*minute, attr("b", 2), attr("a", 1)),
			},
			1,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			if got := len(lifecycle.Coalesce(tt.as)); got != tt.want {
				t.Errorf("left %d assertions, want %d", got, tt.want)
			}
		})
	}
}

func TestCoalesceDoesNotModifyItsInput(t *testing.T) {
	t.Parallel()

	as := heartbeats("n", 1, 0, minute, 6, 3*minute, "v", 1)
	rev := make([]lifecycle.Assertion, len(as))
	for i := range as {
		rev[len(as)-1-i] = as[i]
	}
	_ = lifecycle.Coalesce(rev)
	for i := range as {
		if rev[len(as)-1-i].Seq != as[i].Seq {
			t.Fatal("Coalesce reordered its input")
		}
	}
	// Whatever the input order, the run is the same: first to last, greatest seq.
	if got := lifecycle.Coalesce(rev); len(got) != 1 || !got[0].EventTime.Equal(at(0)) || !got[0].Through.Equal(at(5*minute)) || got[0].Seq != 6 {
		t.Errorf("run = %+v", got)
	}
}

// equivalent fails the test unless folding the coalesced assertions answers
// exactly as folding the originals: same existence, boots and
// description at every probe, and a collision in one is a collision in the
// other.
func equivalent(t *rapid.T, as []lifecycle.Assertion, pol lifecycle.Policy) {
	co := lifecycle.Coalesce(as)
	full, errFull := lifecycle.Fold(as, pol)
	short, errShort := lifecycle.Fold(co, pol)
	if errors.Is(errFull, lifecycle.ErrCloneCollision) != errors.Is(errShort, lifecycle.ErrCloneCollision) ||
		(errFull == nil) != (errShort == nil) {
		t.Fatalf("coalescing changed whether the fold fails: %v vs %v", errFull, errShort)
	}
	if errFull != nil {
		return
	}
	if !sameIntervals(full.Existence(), short.Existence()) {
		t.Fatalf("existence differs:\n%s\n%s", fmtIntervals(full.Existence()), fmtIntervals(short.Existence()))
	}
	if !sameBoots(full.Boots(), short.Boots()) {
		t.Fatalf("boots differ: %+v vs %+v", full.Boots(), short.Boots())
	}
	for _, x := range probes(as) {
		if full.AliveAt(x) != short.AliveAt(x) {
			t.Fatalf("AliveAt(%s) differs", x.Sub(base))
		}
		df, _ := full.DescribeAt(x)
		ds, _ := short.DescribeAt(x)
		if !sameDescription(df, ds) {
			t.Fatalf("DescribeAt(%s) differs: %v vs %v", x.Sub(base), df, ds)
		}
	}
}

func TestPropertyCoalesceChangesNothing(t *testing.T) {
	t.Parallel()
	rapid.Check(t, func(t *rapid.T) {
		equivalent(t, genAssertions(true).Draw(t, "assertions"), genPolicy(true).Draw(t, "policy"))
	})
}

// genHeartbeatRuns draws what Coalesce is for: producers sending regular
// refreshes of mostly the same description, with the occasional change, gap,
// delete and TTL change.
func genHeartbeatRuns() *rapid.Generator[[]lifecycle.Assertion] {
	return rapid.Custom(func(t *rapid.T) []lifecycle.Assertion {
		var as []lifecycle.Assertion
		seq := uint64(0)
		next := func() uint64 { seq++; return seq }
		for _, p := range genProducers {
			if !rapid.Bool().Draw(t, "produces") {
				continue
			}
			ttl := time.Duration(rapid.SampledFrom([]int64{0, 3, 6, 10}).Draw(t, "ttl")) * time.Second
			at := rapid.Int64Range(0, 10).Draw(t, "start")
			attrs := genAttrs(t, true)
			for range rapid.IntRange(1, 25).Draw(t, "beats") {
				switch rapid.IntRange(0, 11).Draw(t, "event") {
				case 0:
					attrs = genAttrs(t, true) // a change
				case 1:
					at += 12 // a gap, past any TTL
				case 2:
					as = append(as, lifecycle.Assertion{Producer: p, EventTime: sec(at), Seq: next(), Kind: lifecycle.Delete})
					at++
					continue
				case 3:
					ttl = time.Duration(rapid.SampledFrom([]int64{0, 3, 6, 10}).Draw(t, "new ttl")) * time.Second
				}
				as = append(as, lifecycle.Assertion{
					Producer: p, EventTime: sec(at), Seq: next(), Kind: lifecycle.Observe, TTL: ttl,
					Attrs: append([]identity.Attr(nil), attrs...),
				})
				at += rapid.Int64Range(1, 4).Draw(t, "step")
			}
		}
		return as
	})
}

func TestPropertyCoalesceChangesNothingForHeartbeatRuns(t *testing.T) {
	t.Parallel()
	rapid.Check(t, func(t *rapid.T) {
		as := genHeartbeatRuns().Draw(t, "runs")
		equivalent(t, as, genPolicy(true).Draw(t, "policy"))
	})
}

// TestPropertyCoalesceIsIdempotent checks that a store that coalesces what it
// has already coalesced changes nothing.
func TestPropertyCoalesceIsIdempotent(t *testing.T) {
	t.Parallel()
	rapid.Check(t, func(t *rapid.T) {
		as := genHeartbeatRuns().Draw(t, "runs")
		once := lifecycle.Coalesce(as)
		twice := lifecycle.Coalesce(once)
		if len(once) != len(twice) {
			t.Fatalf("coalescing again changed the count: %d then %d", len(once), len(twice))
		}
		for i := range once {
			a, b := once[i], twice[i]
			if a.Producer != b.Producer || a.Seq != b.Seq || a.Kind != b.Kind || a.TTL != b.TTL ||
				!a.EventTime.Equal(b.EventTime) || !a.Through.Equal(b.Through) {
				t.Fatalf("coalescing again changed assertion %d: %+v vs %+v", i, a, b)
			}
		}
	})
}

// TestPropertyCoalesceActuallyShrinks guards against a Coalesce that is
// correct only because it drops nothing: n regular heartbeats are one record.
func TestPropertyCoalesceActuallyShrinks(t *testing.T) {
	t.Parallel()
	for n := 3; n <= 50; n += 7 {
		as := heartbeats("n", 1, 0, minute, n, 3*minute, "v", 1)
		if got := len(lifecycle.Coalesce(as)); got != 1 {
			t.Errorf("%d identical heartbeats coalesced to %d, want 1", n, got)
		}
	}
}
