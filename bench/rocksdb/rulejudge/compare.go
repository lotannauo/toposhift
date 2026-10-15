package rulejudge

import (
	"fmt"
	"maps"
	"slices"
	"strings"
)

// The gates the rule compares, in the order they are shown. G3 decides; G2, G4 and
// the peak resident set guard.
const (
	GateG3  = "G3"  // the 99th percentile of the commit of a batch, in nanoseconds
	GateG2  = "G2"  // the largest retention plus the excess of the batches after it, in nanoseconds
	GateG4  = "G4"  // the bytes of tables per record, after a full compaction
	GateRSS = "RSS" // the peak resident set of the build process, in KiB
)

// Gates are the gates, in the order they are shown.
var Gates = []string{GateG3, GateG2, GateG4, GateRSS}

// Outcome is how RocksDB does against Pebble on one gate.
type Outcome string

// The outcomes. Better and worse are RocksDB's, by more than the larger of the
// margin and the noise floor; neither is within it.
const (
	NotDecided Outcome = "not decided"
	Better     Outcome = "better"
	Worse      Outcome = "worse"
	Neither    Outcome = "neither"
)

func (o Outcome) decided() bool { return o == Better || o == Worse }

// sample is one build's value of a gate and the CPU model it ran on.
type sample struct {
	v     float64
	model string
}

// Comparison is RocksDB's candidate against Pebble's of the same policy on one gate,
// at one window, on one architecture.
type Comparison struct {
	Window                   int
	Arch, Candidate, Against string
	Gate                     string
	// Scope is the CPU model the builds ran on, or the models pooled; "" when there
	// was nothing to compare.
	Scope string
	// Reps and AgainstReps are the values compared of each; Value and AgainstValue
	// their medians, Ratio the first over the second.
	Reps, AgainstReps   int
	Value, AgainstValue float64
	Ratio               float64
	// Floor is the larger of the two candidates' noise floors, and Limit the larger
	// of it and the margin: the factor an outcome must be beyond.
	Floor, Limit float64
	Outcome      Outcome
	// Why says why the outcome is not decided; "" when it is.
	Why string
}

// compare compares RocksDB's values with Pebble's on one architecture (Rule R, item
// 7): within one CPU model, or pooled over several only where each candidate has
// [minReps] values; not decided, on the deciding architecture, where either
// candidate's values vary by more than [spread]; then RocksDB's median over Pebble's
// against the larger of [margin] and the noise floor. c carries what the comparison
// is of; the rest of it is filled in.
func compare(c Comparison, rocks, pebble []sample) Comparison {
	notDecided := func(format string, args ...any) Comparison {
		c.Outcome, c.Why = NotDecided, fmt.Sprintf(format, args...)
		return c
	}
	c.Reps, c.AgainstReps = len(rocks), len(pebble)
	if len(rocks) == 0 || len(pebble) == 0 {
		return notDecided("no value to compare")
	}
	models := map[string]bool{}
	for _, s := range slices.Concat(rocks, pebble) {
		models[s.model] = true
	}
	ms := slices.Sorted(maps.Keys(models))
	c.Scope = ms[0]
	if len(ms) > 1 {
		c.Scope = "pooled over " + strings.Join(ms, ", ")
		if len(rocks) < minReps || len(pebble) < minReps {
			return notDecided("the builds ran on %d CPU models, and %s and %s have %d and %d repetitions, fewer than %d each", len(ms), c.Candidate, c.Against, len(rocks), len(pebble), minReps)
		}
	}
	rv, pv := valuesOf(rocks), valuesOf(pebble)
	c.Value, c.AgainstValue = median(rv), median(pv)
	if c.Value <= 0 || c.AgainstValue <= 0 {
		return notDecided("a median of zero")
	}
	c.Ratio = c.Value / c.AgainstValue
	c.Floor = max(floorOf(rv), floorOf(pv))
	c.Limit = max(margin, c.Floor)
	if c.Arch == DecidingArch {
		for _, x := range []struct {
			name string
			vs   []float64
		}{{c.Candidate, rv}, {c.Against, pv}} {
			if lo, hi := slices.Min(x.vs), slices.Max(x.vs); hi > spread*lo {
				return notDecided("%s's own values vary by x%.2f over its repetitions, more than x%.1f", x.name, hi/lo, spread)
			}
		}
	}
	switch {
	case c.Value/c.AgainstValue > c.Limit:
		c.Outcome = Worse
	case c.AgainstValue/c.Value > c.Limit:
		c.Outcome = Better
	default:
		c.Outcome = Neither
	}
	return c
}

func valuesOf(ss []sample) []float64 {
	out := make([]float64, len(ss))
	for i, s := range ss {
		out[i] = s.v
	}
	return out
}

// median is the median of vs, the lower of the two middle values for an even number,
// as the runner takes it.
func median(vs []float64) float64 {
	s := slices.Clone(vs)
	slices.Sort(s)
	return s[(len(s)-1)/2]
}

// floorOf is a candidate's noise floor: the [floorPercent] percentile, by nearest
// rank, of the ratio of the larger value to the smaller over every pair of its
// repetitions, which is the exponential of the percentile of their absolute log
// ratios; 1 for fewer than two values. Up to twenty pairs (seven repetitions), it is
// the largest ratio.
func floorOf(vs []float64) float64 {
	var rs []float64
	for i := range vs {
		for k := i + 1; k < len(vs); k++ {
			rs = append(rs, max(vs[i], vs[k])/min(vs[i], vs[k]))
		}
	}
	if len(rs) == 0 {
		return 1
	}
	slices.Sort(rs)
	return rs[(floorPercent*len(rs)+99)/100-1]
}

// GateOutcome is one gate of a candidate at the verdict's scope: the comparison on
// the deciding architecture, the one on the guarding architecture, and the outcome
// the guard leaves.
type GateOutcome struct {
	Gate            string
	Deciding, Guard Comparison
	Outcome         Outcome
	Why             string
}

// guard is the outcome of a gate: the deciding architecture's, unless the guarding
// one is decided in the other direction, which leaves it not decided. A guard that
// is not decided does not block.
func guard(gate string, deciding, guarding Comparison) GateOutcome {
	g := GateOutcome{Gate: gate, Deciding: deciding, Guard: guarding, Outcome: deciding.Outcome, Why: deciding.Why}
	if deciding.Outcome.decided() && guarding.Outcome.decided() && deciding.Outcome != guarding.Outcome {
		g.Outcome = NotDecided
		g.Why = fmt.Sprintf("%s is %s on %s and %s on %s", deciding.Candidate, deciding.Outcome, DecidingArch, guarding.Outcome, GuardArch)
	}
	return g
}

// Verdict is what the rule says of RocksDB against Pebble.
type Verdict string

// The verdicts (Rule R, item 8).
const (
	VerdictDecisive   Verdict = "RocksDB decisively better"
	VerdictComparable Verdict = "comparable"
	VerdictPebble     Verdict = "Pebble stands"
	VerdictNotDecided Verdict = "not decided"
)

// Class is the verdict one RocksDB candidate's gates give at the verdict's scope.
type Class struct {
	Candidate string
	Verdict   Verdict
	Gates     []GateOutcome
	Why       []string
}

// classify is the class of a candidate from its gates, in the order of [Gates]:
// not decided if any gate is; decisively better if it is better on G3, by at least
// [decisive] on the deciding architecture, and worse on no other gate; comparable if
// no gate is better or worse; and Pebble stands otherwise.
func classify(cand string, gates []GateOutcome) Class {
	c := Class{Candidate: cand, Gates: gates}
	var better, worse []string
	for _, g := range gates {
		switch g.Outcome {
		case NotDecided:
			c.Why = append(c.Why, fmt.Sprintf("%s of %s is not decided: %s", g.Gate, cand, g.Why))
		case Better:
			better = append(better, g.Gate)
		case Worse:
			worse = append(worse, g.Gate)
		}
	}
	switch {
	case len(c.Why) > 0:
		c.Verdict = VerdictNotDecided
	case len(better) == 0 && len(worse) == 0:
		c.Verdict = VerdictComparable
	case len(worse) == 0 && slices.Contains(better, GateG3) && g3Factor(gates) >= decisive:
		c.Verdict = VerdictDecisive
	default:
		c.Verdict = VerdictPebble
		if len(worse) > 0 {
			c.Why = append(c.Why, fmt.Sprintf("%s is worse on %s", cand, strings.Join(worse, ", ")))
		} else {
			c.Why = append(c.Why, fmt.Sprintf("%s is better on %s, and on G3 by x%.2f on %s, less than x%.1f", cand, strings.Join(better, ", "), g3Factor(gates), DecidingArch, decisive))
		}
	}
	return c
}

// g3Factor is Pebble's median G3 over RocksDB's on the deciding architecture: by how
// much RocksDB is better on G3 there.
func g3Factor(gates []GateOutcome) float64 {
	for _, g := range gates {
		if g.Gate == GateG3 && g.Deciding.Value > 0 {
			return g.Deciding.AgainstValue / g.Deciding.Value
		}
	}
	return 0
}

// verdictOf is the rule's verdict from the classes of RL and RL4 at the verdict's
// scope. RocksDB is decisively better only if both are; if one is and the other is
// not, or the other is not decided, the verdict is not decided. Otherwise it is RL's
// class: RL4 changes the files RocksDB writes and nothing else, so it can only take
// a decisive verdict away.
func verdictOf(rl, rl4 Class) (Verdict, []string) {
	switch {
	case rl.Verdict == VerdictNotDecided:
		return VerdictNotDecided, rl.Why
	case rl.Verdict == VerdictDecisive && rl4.Verdict == VerdictDecisive:
		return VerdictDecisive, nil
	case rl.Verdict == VerdictDecisive || rl4.Verdict == VerdictDecisive:
		why := []string{fmt.Sprintf("%s is %s and %s is %s: a decisive verdict must hold for both", rl.Candidate, rl.Verdict, rl4.Candidate, rl4.Verdict)}
		if rl4.Verdict == VerdictNotDecided {
			why = append(why, rl4.Why...)
		}
		return VerdictNotDecided, why
	}
	return rl.Verdict, rl.Why
}
