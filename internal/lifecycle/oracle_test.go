package lifecycle_test

import (
	"errors"
	"fmt"
	"slices"
	"sort"
	"strings"
	"testing"
	"time"

	"pgregory.net/rapid"

	"github.com/lotannauo/toposhift/internal/catalog"
	"github.com/lotannauo/toposhift/internal/identity"
	"github.com/lotannauo/toposhift/internal/lifecycle"
)

// The oracle answers every question straight from the definitions in the
// package documentation, one probe instant at a time, sharing no code with
// Fold. Fold computes intervals with a sweep; the oracle never builds one.

var base = time.Date(2026, 1, 1, 10, 0, 0, 0, time.UTC)

func sec(n int64) time.Time { return base.Add(time.Duration(n) * time.Second) }

// latest is a producer's latest assertion at or before x: the one with the
// greatest (event time, seq) among those not after x.
func latest(as []lifecycle.Assertion, p lifecycle.Producer, x time.Time) (lifecycle.Assertion, bool) {
	var best lifecycle.Assertion
	found := false
	for _, a := range as {
		if a.Producer != p || a.EventTime.After(x) {
			continue
		}
		if !found || a.EventTime.After(best.EventTime) ||
			(a.EventTime.Equal(best.EventTime) && a.Seq > best.Seq) {
			best, found = a, true
		}
	}
	return best, found
}

// lastSeen is the latest instant an Observe says the subject was seen: the end
// of its run, if it is one.
func lastSeen(a lifecycle.Assertion) time.Time {
	if a.Through.After(a.EventTime) {
		return a.Through
	}
	return a.EventTime
}

// live reports whether an assertion holds a live reference at x.
func live(a lifecycle.Assertion, x time.Time) bool {
	return a.Kind == lifecycle.Observe && (a.TTL == 0 || x.Before(lastSeen(a).Add(a.TTL)))
}

func producersOf(as []lifecycle.Assertion) []lifecycle.Producer {
	seen := map[lifecycle.Producer]bool{}
	var out []lifecycle.Producer
	for _, a := range as {
		if !seen[a.Producer] {
			seen[a.Producer] = true
			out = append(out, a.Producer)
		}
	}
	return out
}

func oracleAlive(as []lifecycle.Assertion, x time.Time) bool {
	for _, p := range producersOf(as) {
		if a, ok := latest(as, p, x); ok && live(a, x) {
			return true
		}
	}
	return false
}

// oracleOutranks is the authority order, written out again.
func oracleOutranks(pol lifecycle.Policy, a, b lifecycle.Producer) bool {
	ra, okA := pol.Rank[a]
	rb, okB := pol.Rank[b]
	if okA && !okB {
		return true
	}
	if !okA && okB {
		return false
	}
	if okA && okB && ra != rb {
		return ra > rb
	}
	return a < b
}

func oracleDescribe(as []lifecycle.Assertion, pol lifecycle.Policy, x time.Time) (lifecycle.Description, bool) {
	var desc lifecycle.Description
	for _, p := range producersOf(as) {
		a, ok := latest(as, p, x)
		if !ok || !live(a, x) {
			continue
		}
		if desc == nil {
			desc = lifecycle.Description{}
		}
		for _, attr := range a.Attrs {
			cur, has := desc[attr.Key]
			if !has || oracleOutranks(pol, p, cur.Producer) {
				desc[attr.Key] = lifecycle.Attribute{Value: attr.Value, Producer: p}
			}
		}
	}
	return desc, desc != nil
}

// oracleEndSource says how existence ended at e: Producer if some reference
// that was live just before e was released by a Delete, else expiry.
func oracleEndSource(as []lifecycle.Assertion, e time.Time) lifecycle.EndSource {
	before := e.Add(-time.Nanosecond)
	for _, p := range producersOf(as) {
		a, ok := latest(as, p, before)
		if !ok || !live(a, before) {
			continue
		}
		if b, _ := latest(as, p, e); b.Kind == lifecycle.Delete {
			return lifecycle.EndProducer
		}
	}
	return lifecycle.EndLivenessExpiry
}

// oracleCollision is the clone-collision rule as a quadratic scan over the
// observations in processing order.
func oracleCollision(as []lifecycle.Assertion, pol lifecycle.Policy) bool {
	if pol.BootKey == "" {
		return false
	}
	// An assertion that a later one by the same producer at the same event
	// time replaces has no effect, not even on which boots were seen.
	var sorted []lifecycle.Assertion
	for _, a := range as {
		replaced := false
		for _, b := range as {
			if b.Producer == a.Producer && b.EventTime.Equal(a.EventTime) && b.Seq > a.Seq {
				replaced = true
			}
		}
		if !replaced {
			sorted = append(sorted, a)
		}
	}
	slices.SortFunc(sorted, func(a, b lifecycle.Assertion) int {
		if c := a.EventTime.Compare(b.EventTime); c != 0 {
			return c
		}
		return int(a.Seq) - int(b.Seq)
	})
	type obs struct {
		boot string
		at   time.Time
	}
	var seq []obs
	for _, a := range sorted {
		if a.Kind != lifecycle.Observe {
			continue
		}
		for _, attr := range a.Attrs {
			if attr.Key == pol.BootKey {
				seq = append(seq, obs{attr.Value.(string), a.EventTime})
				if end := lastSeen(a); end.After(a.EventTime) {
					seq = append(seq, obs{attr.Value.(string), end})
				}
			}
		}
	}
	// Observation points in time order, and by boot ID at one instant.
	slices.SortFunc(seq, func(x, y obs) int {
		if c := x.at.Compare(y.at); c != 0 {
			return c
		}
		return strings.Compare(x.boot, y.boot)
	})
	first := map[string]int{} // boot -> index of its first observation
	for i, o := range seq {
		if _, ok := first[o.boot]; !ok {
			first[o.boot] = i
		}
	}
	for _, o := range seq {
		for _, idx := range first {
			if idx > first[o.boot] && o.at.Sub(seq[idx].at) > pol.Skew {
				return true
			}
		}
	}
	return false
}

// probes returns the instants worth asking about: every assertion time and
// deadline, one nanosecond either side.
func probes(as []lifecycle.Assertion) []time.Time {
	set := map[time.Time]bool{}
	for _, a := range as {
		for _, t := range []time.Time{a.EventTime, lastSeen(a), lastSeen(a).Add(a.TTL)} {
			for _, d := range []time.Duration{-time.Nanosecond, 0, time.Nanosecond} {
				set[t.Add(d)] = true
			}
		}
	}
	out := make([]time.Time, 0, len(set)+2)
	out = append(out, base.Add(-time.Hour), base.Add(24*time.Hour))
	for t := range set {
		out = append(out, t)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Before(out[j]) })
	return out
}

// --- generators ---------------------------------------------------------

var (
	genProducers = []lifecycle.Producer{"p1", "p2", "p3"}
	genTTLs      = []int64{0, 0, 3, 5, 5, 10} // seconds; 0 is watch mode
	genKeys      = []catalog.AttributeKey{"a", "b"}
	genValues    = []any{1, 2, "x"}
)

func genAttrs(t *rapid.T, withBoot bool) []identity.Attr {
	var attrs []identity.Attr
	for _, k := range genKeys {
		if rapid.Bool().Draw(t, "has "+string(k)) {
			attrs = append(attrs, identity.Attr{Key: k, Value: rapid.SampledFrom(genValues).Draw(t, "value")})
		}
	}
	if withBoot && rapid.IntRange(0, 3).Draw(t, "has boot") > 0 {
		attrs = append(attrs, identity.Attr{
			Key: lifecycle.BootID, Value: rapid.SampledFrom([]string{"A", "B", "C"}).Draw(t, "boot"),
		})
	}
	return attrs
}

// genAssertions draws a valid assertion set: small time and TTL domains, so
// ties, touching deadlines and equal event times are common.
func genAssertions(withBoot bool) *rapid.Generator[[]lifecycle.Assertion] {
	return rapid.Custom(func(t *rapid.T) []lifecycle.Assertion {
		n := rapid.IntRange(0, 24).Draw(t, "n")
		seqs := rapid.Permutation(func() []uint64 {
			s := make([]uint64, n)
			for i := range s {
				s[i] = uint64(i + 1)
			}
			return s
		}()).Draw(t, "seqs")
		as := make([]lifecycle.Assertion, n)
		for i := range as {
			a := lifecycle.Assertion{
				Producer:  rapid.SampledFrom(genProducers).Draw(t, "producer"),
				EventTime: sec(rapid.Int64Range(0, 60).Draw(t, "t")),
				Seq:       seqs[i],
			}
			if rapid.IntRange(0, 4).Draw(t, "kind") == 0 {
				a.Kind = lifecycle.Delete
			} else {
				a.Kind = lifecycle.Observe
				a.TTL = time.Duration(rapid.SampledFrom(genTTLs).Draw(t, "ttl")) * time.Second
				a.Attrs = genAttrs(t, withBoot)
				if rapid.IntRange(0, 3).Draw(t, "is a run") == 0 {
					a.Through = a.EventTime.Add(time.Duration(rapid.Int64Range(0, 15).Draw(t, "run length")) * time.Second)
				}
			}
			as[i] = a
		}
		return as
	})
}

func genPolicy(withBoot bool) *rapid.Generator[lifecycle.Policy] {
	return rapid.Custom(func(t *rapid.T) lifecycle.Policy {
		p := lifecycle.Policy{Rank: map[lifecycle.Producer]int{}}
		for _, producer := range genProducers {
			if rapid.Bool().Draw(t, "ranked") {
				p.Rank[producer] = rapid.IntRange(-1, 2).Draw(t, "rank")
			}
		}
		if withBoot {
			p.BootKey = lifecycle.BootID
			p.Skew = time.Duration(rapid.SampledFrom([]int64{0, 0, 1, 5}).Draw(t, "skew")) * time.Second
		}
		return p
	})
}

// --- comparisons --------------------------------------------------------

func sameIntervals(a, b []lifecycle.Interval) bool {
	return slices.EqualFunc(a, b, func(x, y lifecycle.Interval) bool {
		return x.Start.Equal(y.Start) && x.End.Equal(y.End) && x.EndSource == y.EndSource
	})
}

func sameBoots(a, b []lifecycle.Boot) bool {
	return slices.EqualFunc(a, b, func(x, y lifecycle.Boot) bool {
		return x.ID == y.ID && x.FirstSeen.Equal(y.FirstSeen) && x.LastSeen.Equal(y.LastSeen)
	})
}

func sameDescription(a, b lifecycle.Description) bool {
	if len(a) != len(b) {
		return false
	}
	for k, x := range a {
		y, ok := b[k]
		if !ok || x.Producer != y.Producer || fmt.Sprint(x.Value) != fmt.Sprint(y.Value) {
			return false
		}
	}
	return true
}

// agree checks a timeline against the oracle at every probe instant.
func agree(t *rapid.T, tl lifecycle.Timeline, as []lifecycle.Assertion, pol lifecycle.Policy) {
	for _, x := range probes(as) {
		if got, want := tl.AliveAt(x), oracleAlive(as, x); got != want {
			t.Fatalf("AliveAt(%s) = %v, oracle says %v\nassertions: %+v", x.Sub(base), got, want, as)
		}
		gd, gok := tl.DescribeAt(x)
		wd, wok := oracleDescribe(as, pol, x)
		if gok != wok || !sameDescription(gd, wd) {
			t.Fatalf("DescribeAt(%s) = %v %v, oracle says %v %v", x.Sub(base), gd, gok, wd, wok)
		}
	}
}

// --- properties ---------------------------------------------------------

func TestPropertyFoldMatchesOracle(t *testing.T) {
	t.Parallel()
	rapid.Check(t, func(t *rapid.T) {
		as := genAssertions(false).Draw(t, "assertions")
		pol := genPolicy(false).Draw(t, "policy")
		tl, err := lifecycle.Fold(as, pol)
		if err != nil {
			t.Fatal(err)
		}
		agree(t, tl, as, pol)
	})
}

// TestPropertyIntervalsAreMaximalAndExplained checks the shape of Existence
// against the oracle: sorted, disjoint, not touching, starting and ending
// exactly where the oracle's answer flips, and ending for the stated reason.
func TestPropertyIntervalsAreMaximalAndExplained(t *testing.T) {
	t.Parallel()
	rapid.Check(t, func(t *rapid.T) {
		as := genAssertions(false).Draw(t, "assertions")
		tl, err := lifecycle.Fold(as, lifecycle.Policy{})
		if err != nil {
			t.Fatal(err)
		}
		ivs := tl.Existence()
		for i, iv := range ivs {
			if !oracleAlive(as, iv.Start) || oracleAlive(as, iv.Start.Add(-time.Nanosecond)) {
				t.Fatalf("interval %d starts at %s, not where existence begins", i, iv.Start.Sub(base))
			}
			if i > 0 && !iv.Start.After(ivs[i-1].End) {
				t.Fatalf("intervals %d and %d overlap or touch: %+v", i-1, i, ivs)
			}
			if iv.Open() {
				if i != len(ivs)-1 || iv.EndSource != lifecycle.EndUnknown {
					t.Fatalf("a malformed open interval: %+v", ivs)
				}
				continue
			}
			if !iv.End.After(iv.Start) {
				t.Fatalf("interval %d is empty or inverted: %+v", i, iv)
			}
			if oracleAlive(as, iv.End) || !oracleAlive(as, iv.End.Add(-time.Nanosecond)) {
				t.Fatalf("interval %d ends at %s, not where existence ends", i, iv.End.Sub(base))
			}
			if want := oracleEndSource(as, iv.End); iv.EndSource != want {
				t.Fatalf("interval %d ends %s, oracle says %s", i, iv.EndSource, want)
			}
		}
	})
}

// TestPropertyArrivalOrderIsIrrelevant checks that shuffling the input never
// changes the timeline: a late assertion lands where its event time puts it.
func TestPropertyArrivalOrderIsIrrelevant(t *testing.T) {
	t.Parallel()
	rapid.Check(t, func(t *rapid.T) {
		as := genAssertions(true).Draw(t, "assertions")
		pol := genPolicy(true).Draw(t, "policy")
		shuffled := rapid.Permutation(as).Draw(t, "shuffled")

		a, errA := lifecycle.Fold(as, pol)
		b, errB := lifecycle.Fold(shuffled, pol)
		if (errA == nil) != (errB == nil) {
			t.Fatalf("shuffling changed whether the fold fails: %v vs %v", errA, errB)
		}
		if errA != nil {
			return
		}
		if !sameIntervals(a.Existence(), b.Existence()) || !sameBoots(a.Boots(), b.Boots()) {
			t.Fatalf("shuffling changed the timeline:\n%+v\n%+v", a.Existence(), b.Existence())
		}
		for _, x := range probes(as) {
			da, _ := a.DescribeAt(x)
			db, _ := b.DescribeAt(x)
			if !sameDescription(da, db) {
				t.Fatalf("shuffling changed the description at %s", x.Sub(base))
			}
		}
	})
}

func TestPropertyBootsAndCollisionsMatchOracle(t *testing.T) {
	t.Parallel()
	rapid.Check(t, func(t *rapid.T) {
		as := genAssertions(true).Draw(t, "assertions")
		pol := genPolicy(true).Draw(t, "policy")
		tl, err := lifecycle.Fold(as, pol)

		if want := oracleCollision(as, pol); want != errors.Is(err, lifecycle.ErrCloneCollision) {
			t.Fatalf("collision = %v, oracle says %v (err %v)\nassertions: %+v", errors.Is(err, lifecycle.ErrCloneCollision), want, err, as)
		}
		if err != nil {
			var ce *lifecycle.CloneCollisionError
			if !errors.As(err, &ce) || ce.StaleBoot == "" || ce.NewerBoot == "" {
				t.Fatalf("a collision without detail: %v", err)
			}
			return
		}
		agree(t, tl, as, pol)

		// Every boot appears once, in order of first appearance, and the
		// tracked span covers its observations.
		seen := map[string]bool{}
		var prev time.Time
		for _, b := range tl.Boots() {
			if seen[b.ID] || b.FirstSeen.Before(prev) || b.LastSeen.Before(b.FirstSeen) {
				t.Fatalf("a malformed boot list: %+v", tl.Boots())
			}
			seen[b.ID], prev = true, b.FirstSeen
		}
	})
}

func TestPropertyVisibleKeepsExactlyTheSnapshot(t *testing.T) {
	t.Parallel()
	rapid.Check(t, func(t *rapid.T) {
		as := genAssertions(false).Draw(t, "assertions")
		token := rapid.Uint64Range(0, 30).Draw(t, "token")
		got := lifecycle.Visible(as, token)
		n := 0
		for _, a := range as {
			if a.Seq <= token {
				n++
			}
		}
		if len(got) != n {
			t.Fatalf("Visible kept %d, want %d", len(got), n)
		}
		for _, a := range got {
			if a.Seq > token {
				t.Fatalf("Visible kept seq %d above token %d", a.Seq, token)
			}
		}
	})
}
