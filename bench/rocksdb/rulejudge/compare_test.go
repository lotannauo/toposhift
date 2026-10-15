package rulejudge

import (
	"strings"
	"testing"
)

// on is values as samples on one CPU model.
func on(model string, vs ...float64) []sample {
	out := make([]sample, len(vs))
	for i, v := range vs {
		out[i] = sample{v, model}
	}
	return out
}

func five(v float64) []float64 { return []float64{v, v, v, v, v} }

// cmpOn compares RocksDB's values with Pebble's on arch, all on its CPU model.
func cmpOn(arch, gate string, rocks, pebble []float64) Comparison {
	return compare(Comparison{Arch: arch, Window: VerdictWindow, Candidate: "RL/p", Against: "L/p", Gate: gate}, on(cpuOf[arch], rocks...), on(cpuOf[arch], pebble...))
}

// Better and worse are by more than the margin: a ratio of exactly 1.10 is neither,
// in both directions, and one just beyond it is decided.
func TestTheMarginIsExclusive(t *testing.T) {
	t.Parallel()
	for _, c := range []struct {
		rocks, pebble float64
		want          Outcome
	}{
		{110, 100, Neither}, {111, 100, Worse}, {100, 110, Neither}, {100, 111, Better}, {100, 100, Neither},
	} {
		for _, arch := range []string{DecidingArch, GuardArch} {
			if got := cmpOn(arch, GateG3, five(c.rocks), five(c.pebble)); got.Outcome != c.want || got.Limit != margin {
				t.Errorf("%s: %v against %v: %s (limit %v), want %s", arch, c.rocks, c.pebble, got.Outcome, got.Limit, c.want)
			}
		}
	}
}

// The noise floor is the larger of the two engines': a RocksDB candidate whose own
// values spread by 1.3 is not better by 1.25, though Pebble's do not spread at all;
// and the other way round.
func TestTheFloorIsTheLargerOfBothEngines(t *testing.T) {
	t.Parallel()
	spreadOut := []float64{70, 75, 80, 85, 91}
	got := cmpOn(DecidingArch, GateG3, spreadOut, five(100))
	if got.Outcome != Neither || got.Floor != 1.3 || got.Value != 80 {
		t.Errorf("RocksDB's values spread by 1.3: %s, floor %v, value %v", got.Outcome, got.Floor, got.Value)
	}
	got = cmpOn(DecidingArch, GateG3, five(100), []float64{110, 117.5, 125, 132.5, 143})
	if got.Outcome != Neither || got.Floor != 1.3 {
		t.Errorf("Pebble's values spread by 1.3: %s, floor %v", got.Outcome, got.Floor)
	}
	if got := cmpOn(DecidingArch, GateG3, five(80), five(100)); got.Outcome != Better || got.Floor != 1 {
		t.Errorf("no spread: %s, floor %v", got.Outcome, got.Floor)
	}
}

// The value of a candidate is its median, not its least or its mean.
func TestTheValueIsTheMedian(t *testing.T) {
	t.Parallel()
	got := cmpOn(GuardArch, GateG2, []float64{116, 100, 108, 112, 104}, []float64{100, 200, 150, 120, 130})
	if got.Value != 108 || got.AgainstValue != 130 {
		t.Errorf("medians %v and %v, want 108 and 130", got.Value, got.AgainstValue)
	}
	if m := median([]float64{4, 1, 3, 2}); m != 2 {
		t.Errorf("the median of four values is %v, want the lower middle one, 2", m)
	}
}

// The floor is the 95th percentile of the pair ratios by nearest rank: for five
// repetitions (ten pairs) the largest; for eight (28 pairs) the 27th, which leaves
// out a single pair of outliers.
func TestTheFloorIsTheNinetyFifthPercentile(t *testing.T) {
	t.Parallel()
	for _, c := range []struct {
		vs   []float64
		want float64
	}{
		{[]float64{1, 2, 1.5, 1.5, 1.5}, 2},
		{[]float64{1, 2, 1.5, 1.5, 1.5, 1.5, 1.5, 1.5}, 1.5},
		{[]float64{3}, 1},
	} {
		if got := floorOf(c.vs); got != c.want {
			t.Errorf("floorOf(%v) = %v, want %v", c.vs, got, c.want)
		}
	}
}

// On the deciding architecture a candidate whose own values vary by more than 1.5
// is not decided, and one that varies by exactly 1.5 is; the guard has no such
// rule, its floor carries the spread.
func TestASpreadAboveOneAndAHalfIsNotDecided(t *testing.T) {
	t.Parallel()
	wide := []float64{100, 100, 100, 100, 151}
	if got := cmpOn(DecidingArch, GateG3, wide, five(200)); got.Outcome != NotDecided || !strings.Contains(got.Why, "vary by x1.51") {
		t.Errorf("RocksDB's values vary by 1.51: %s (%s)", got.Outcome, got.Why)
	}
	if got := cmpOn(DecidingArch, GateG3, five(200), wide); got.Outcome != NotDecided {
		t.Errorf("Pebble's values vary by 1.51: %s", got.Outcome)
	}
	if got := cmpOn(DecidingArch, GateG3, []float64{100, 100, 100, 100, 150}, five(200)); got.Outcome != Better {
		t.Errorf("a spread of exactly 1.5: %s (%s)", got.Outcome, got.Why)
	}
	if got := cmpOn(GuardArch, GateG3, wide, five(200)); got.Outcome != Better || got.Floor != 1.51 {
		t.Errorf("on the guard: %s, floor %v", got.Outcome, got.Floor)
	}
}

// Builds on several CPU models are pooled only where each candidate has three
// repetitions; otherwise the comparison is not decided.
func TestCPUModelsArePooledOnlyWithThreeRepetitions(t *testing.T) {
	t.Parallel()
	c := Comparison{Arch: GuardArch, Candidate: "RL/p", Against: "L/p", Gate: GateG3}
	mixed := func(n int, v float64) []sample {
		return append(on("a", v), on("b", five(v)[:n-1]...)...)
	}
	if got := compare(c, mixed(2, 50), mixed(3, 100)); got.Outcome != NotDecided || !strings.Contains(got.Why, "2 CPU models") {
		t.Errorf("two repetitions over two models: %s (%s)", got.Outcome, got.Why)
	}
	if got := compare(c, mixed(3, 50), mixed(3, 100)); got.Outcome != Better || got.Scope != "pooled over a, b" {
		t.Errorf("three over two models: %s, scope %q", got.Outcome, got.Scope)
	}
	if got := compare(c, on("a", 50, 50), on("a", 100, 100)); got.Outcome != Better || got.Scope != "a" {
		t.Errorf("two on one model: %s, scope %q", got.Outcome, got.Scope)
	}
}

// A profile is the values of RocksDB and Pebble on each gate on one architecture; a
// gate it does not name is alike. A nil profile is an architecture with no build.
type profile map[string][2][]float64

func (p profile) of(gate string) (rocks, pebble []float64) {
	if v, ok := p[gate]; ok {
		return v[0], v[1]
	}
	return five(100), five(100)
}

func classOf(cand string, deciding, guarding profile) Class {
	var gates []GateOutcome
	for _, gate := range Gates {
		r, p := deciding.of(gate)
		d := cmpOn(DecidingArch, gate, r, p)
		x := Comparison{Arch: GuardArch, Gate: gate, Outcome: NotDecided, Why: "no build"}
		if guarding != nil {
			r, p := guarding.of(gate)
			x = cmpOn(GuardArch, gate, r, p)
		}
		gates = append(gates, guard(gate, d, x))
	}
	return classify(cand, gates)
}

// The verdict table: constructed medians for each class, RL and RL4 together, with
// the guard.
func TestTheVerdictTable(t *testing.T) {
	t.Parallel()
	var (
		alike    = profile{}
		decisive = profile{GateG3: {five(50), five(100)}}
		atLeast  = profile{GateG3: {five(100), five(150)}}
		better   = profile{GateG3: {five(80), five(100)}} // by 1.25: better, not decisively
		worse    = profile{GateG3: {five(130), five(100)}}
		rssWorse = profile{GateG3: {five(50), five(100)}, GateRSS: {five(130), five(100)}}
		g2Worse  = profile{GateG3: {five(50), five(100)}, GateG2: {five(130), five(100)}}
		g4Better = profile{GateG4: {five(80), five(100)}}
		wide     = profile{GateG3: {[]float64{50, 50, 50, 50, 80}, five(100)}}
		g3Worse  = profile{GateG3: {five(130), five(100)}}
	)
	for _, c := range []struct {
		name               string
		rl, rlX, rl4, rl4X profile
		want               Verdict
	}{
		{"decisive under RL and RL4, x86 agreeing", decisive, decisive, decisive, decisive, VerdictDecisive},
		{"decisive at exactly 1.5", atLeast, atLeast, atLeast, atLeast, VerdictDecisive},
		{"decisive on aarch64, x86 not decided", decisive, nil, decisive, nil, VerdictDecisive},
		{"decisive on aarch64, x86 neither", decisive, alike, decisive, alike, VerdictDecisive},
		{"RL decisive and RL4 not", decisive, decisive, better, better, VerdictNotDecided},
		{"RL4 decisive and RL not", better, better, decisive, decisive, VerdictNotDecided},
		{"RL decisive and RL4 not decided", decisive, decisive, wide, wide, VerdictNotDecided},
		{"x86 disagreeing with aarch64 on G3", decisive, g3Worse, decisive, decisive, VerdictNotDecided},
		{"x86 disagreeing with aarch64 under RL4", decisive, decisive, decisive, g3Worse, VerdictNotDecided},
		{"a variance above 1.5 on aarch64", wide, wide, wide, wide, VerdictNotDecided},
		{"comparable", alike, alike, alike, alike, VerdictComparable},
		{"comparable, RL4 not decided", alike, alike, wide, wide, VerdictComparable},
		{"comparable on aarch64, x86 worse", alike, worse, alike, worse, VerdictComparable},
		{"better on G3 by less than 1.5", better, better, better, better, VerdictPebble},
		{"worse on G3", worse, worse, worse, worse, VerdictPebble},
		{"decisive on G3 and worse on the resident set", rssWorse, rssWorse, rssWorse, rssWorse, VerdictPebble},
		{"decisive on G3 and worse on G2", g2Worse, g2Worse, decisive, decisive, VerdictNotDecided},
		{"better on G4 alone", g4Better, g4Better, g4Better, g4Better, VerdictPebble},
	} {
		got, why := verdictOf(classOf(rl, c.rl, c.rlX), classOf(rl4, c.rl4, c.rl4X))
		if got != c.want {
			t.Errorf("%s: %q (%q), want %q", c.name, got, why, c.want)
		}
	}
}

// An out-of-memory cell, which has no value, leaves its gates not decided on the
// deciding architecture and does not block on the guard.
func TestAGateWithNoValueIsNotDecided(t *testing.T) {
	t.Parallel()
	missing := Comparison{Arch: DecidingArch, Gate: GateG3, Outcome: NotDecided, Why: "repetition 3 left no manifest"}
	var gates []GateOutcome
	for _, gate := range Gates {
		d := cmpOn(DecidingArch, gate, five(100), five(100))
		if gate == GateG3 {
			d = missing
		}
		gates = append(gates, guard(gate, d, cmpOn(GuardArch, gate, five(50), five(100))))
	}
	if c := classify(rl, gates); c.Verdict != VerdictNotDecided || !strings.Contains(strings.Join(c.Why, " "), "left no manifest") {
		t.Errorf("%q: %q", c.Verdict, c.Why)
	}
}
