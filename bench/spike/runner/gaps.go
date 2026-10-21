package runner

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"slices"
	"sort"

	"github.com/lotannauo/toposhift/bench/spike/candidates"
	"github.com/lotannauo/toposhift/bench/spike/pebblekv"
	"github.com/lotannauo/toposhift/bench/spike/pebblelog"
)

// GapsGroup is the group of queries whose prefixes [Gaps] looks at: the reads of
// the busiest node reverse prefixes, which are the cells G1 fails or nearly fails.
const GapsGroup = "node<- hot-records neighbors"

// HoleFactor is how many times a prefix's median gap a gap must exceed to be a hole.
const HoleFactor = 1.6

// GapsFile is where [Gaps] is written, in the candidate's directory, for tools.
const GapsFile = "gaps.json"

// Tally is how many checkpoints of a prefix a record deleted, and a write put back
// or did not (see [TalliesFile]).
type Tally struct {
	Invalidated, Replaced, Capped int64
}

// QueryGap is one pinned read of a prefix as the model of the read walks it.
type QueryGap struct {
	Age string
	// ModelSteps is how many times the model steps the iterator, from the first
	// key at or before the read's instant to the checkpoint or baseline it stops at;
	// ReadSteps is "read.neighbors.internal_steps" of the build's results, or -1
	// where there are none. On a canonical store they are equal.
	ModelSteps, ReadSteps int64
	// Ends says where the walk stops: "checkpoint", "baseline", "end" (the prefix
	// ran out) or "none" (no key at or before the instant). Skipped counts the
	// checkpoints it walked past because the token is below their W.
	Ends    string
	Skipped int
	// Where says where the instant lies among the checkpoints: "gap" (between two),
	// "tail" (after the newest) or "below" (before the oldest). For a "gap",
	// Gap is its index from the newest (0), GapRecords the records in it and
	// Multiple that over the prefix's median gap.
	Where      string
	Gap        int
	GapRecords int
	Multiple   float64
}

// PrefixGaps is where the checkpoints of one pinned prefix lie.
type PrefixGaps struct {
	Fp          string
	Rank        int
	Checkpoints int
	Records     int
	// Gaps are the spans between consecutive checkpoints, in records (the tail
	// above the newest checkpoint and the part below the oldest are not gaps).
	// Holes are the gaps over [HoleFactor] times the median, whose upper middle value
	// is taken.
	Gaps             int
	Median, P90, Max int
	HoleOver         float64
	Holes            int
	Queries          []QueryGap
	Tallies          *Tally `json:",omitempty"`
}

// GapReport is what [Gaps] found in one build.
type GapReport struct {
	Candidate string
	// Canonical says the build was rewritten in the canonical layout, where the
	// model's steps are exact; elsewhere they are a lower bound (the iterator also
	// steps over versions a compaction has not dropped and over tombstones).
	Canonical bool
	// HasTallies says the build has a tallies file, so that the prefixes' tallies
	// are filled in; HasResults that it has a results file, so that ReadSteps are.
	HasTallies, HasResults bool
	Prefixes               []PrefixGaps
	// Holes and Gaps are summed over the prefixes, and MedianOfMedians is the median
	// of the prefixes' median gaps (the mean of the two middle ones for an even
	// count).
	Holes, Gaps     int
	MedianOfMedians float64
	// The sums of the tallies over the prefixes that have them.
	Invalidated, Replaced, Capped int64
	// Mismatched counts the queries whose ModelSteps differ from ReadSteps where
	// both are known.
	Mismatched int
}

// gapKey is the little a prefix's keys are kept as: enough to walk a read.
type gapKey struct {
	kind    byte
	ns      int64
	w       uint64
	current bool
}

// GapStats are the figures of a list of gap lengths: the upper middle value, the
// nearest-rank 90th percentile, the largest, and how many are over [HoleFactor]
// times the median.
func GapStats(lengths []int) (median, p90, largest, holes int) {
	if len(lengths) == 0 {
		return 0, 0, 0, 0
	}
	s := slices.Sorted(slices.Values(lengths))
	median = s[len(s)/2]
	p90 = s[(9*len(s)+9)/10-1]
	largest = s[len(s)-1]
	for _, n := range s {
		if float64(n) > HoleFactor*float64(median) {
			holes++
		}
	}
	return median, p90, largest, holes
}

// walkSteps is the read of the model: the iterator is put at the first key whose
// event time is at or before tNs and stepped toward older keys, past the records and
// past the checkpoints a token below their W (or another fold version) cannot use,
// to the first checkpoint it can, or the baseline. The steps are the keys it moves
// over; if the prefix runs out first they are those left.
func walkSteps(keys []gapKey, tNs int64, asOf uint64) (steps int64, ends string, skipped int) {
	start := sort.Search(len(keys), func(i int) bool { return keys[i].ns <= tNs }) // newest first
	if start == len(keys) {
		return 0, "none", 0
	}
	for i := start; i < len(keys); i++ {
		k := keys[i]
		switch {
		case k.kind == pebblelog.KeyCheckpoint && (!k.current || k.w > asOf):
			skipped++
		case k.kind == pebblelog.KeyCheckpoint:
			return int64(i - start), "checkpoint", skipped
		case k.kind == pebblelog.KeyBaseline:
			return int64(i - start), "baseline", skipped
		}
	}
	return int64(len(keys) - start), "end", skipped
}

// gapsOf lists the lengths, in records, of the spans between consecutive
// checkpoints of keys (newest first), and where the checkpoints are among them.
func gapsOf(keys []gapKey) (lengths []int, checkpoints []int) {
	n := 0
	for i, k := range keys {
		switch k.kind {
		case pebblelog.KeyRecord:
			n++
		case pebblelog.KeyCheckpoint:
			if len(checkpoints) > 0 {
				lengths = append(lengths, n)
			}
			checkpoints = append(checkpoints, i)
			n = 0
		}
	}
	return lengths, checkpoints
}

// place says where an instant lies among the checkpoints at positions cks of keys:
// after the walk starts at the first key at or before tNs, the checkpoint
// positions before it are newer than the instant and those at or after it are
// older.
func place(keys []gapKey, cks []int, tNs int64) (where string, gap int) {
	start := sort.Search(len(keys), func(i int) bool { return keys[i].ns <= tNs })
	newer := sort.SearchInts(cks, start) // checkpoints before the start are newer than the instant
	switch {
	case newer == 0:
		return "tail", -1
	case newer == len(cks):
		return "below", -1
	}
	return "gap", newer - 1 // between cks[newer-1] and cks[newer]
}

// Gaps opens the build of the candidate under dir (the candidate's own directory;
// plan is the one it was built from) read-only and, for each
// prefix the plan's [GapsGroup] queries are asked of, says where its checkpoints lie:
// the lengths of the spans between them, which of those are holes, and for each
// pinned read the steps the model walks and the span it sits in. If the directory
// has a tallies file, a prefix's checkpoints deleted, put back and not put back are
// given, and with a results file each read's steps next to the model's.
func Gaps(plan *Plan, v candidates.Variant, dir string) (*GapReport, error) {
	m, _, err := readManifest(dir)
	if err != nil {
		return nil, err
	}
	planDigest, err := plan.Digest()
	if err != nil {
		return nil, err
	}
	switch {
	case m.PlanDigest != planDigest:
		return nil, fmt.Errorf("runner: %s was built from another plan: %w", dir, errMismatch)
	case m.Candidate != v.Name:
		return nil, fmt.Errorf("runner: %s holds %s, not %s: %w", dir, m.Candidate, v.Name, errMismatch)
	}
	e, err := openReadOnly(plan, v, dir, NewCapture(), m.Describe[SyncKey] == "true", m.Describe[ServeOptionsKey] == "true")
	if err != nil {
		return nil, err
	}
	defer func() { _ = e.Close() }()
	sc, ok := e.(pebblelog.PrefixScanner)
	if !ok {
		return nil, fmt.Errorf("runner: %s is not a candidate of the spike's layout L: it has no prefixes to scan", v.Name)
	}
	tallies, haveTallies, err := LoadTallies(dir)
	if err != nil {
		return nil, err
	}
	readSteps, haveResults, err := loadReadSteps(dir, plan, v)
	if err != nil {
		return nil, err
	}
	rep := &GapReport{Candidate: v.Name, Canonical: m.Describe[CanonicalKey] == "true", HasTallies: haveTallies, HasResults: haveResults}

	// The pinned prefixes, in the order of the plan, each with its queries.
	type pinned struct {
		q  Query
		qs []int // indexes into plan.Queries
	}
	var order []*pinned
	byFp := map[string]*pinned{}
	for i, q := range plan.Queries {
		if q.Group != GapsGroup || q.Op != OpNeighbors || len(q.Fps) != 1 {
			continue
		}
		id := fmt.Sprintf("%d/%d/%s", q.Layer, q.Dir, q.Fps[0])
		p := byFp[id]
		if p == nil {
			p = &pinned{q: q}
			byFp[id] = p
			order = append(order, p)
		}
		p.qs = append(p.qs, i)
	}
	if len(order) == 0 {
		return nil, fmt.Errorf("runner: the plan has no %q queries: there are no prefixes to look at", GapsGroup)
	}
	var medians []float64
	for _, p := range order {
		var keys []gapKey
		err := sc.ScanPrefix(p.q.Layer, p.q.Fps[0], p.q.Dir, func(k pebblelog.StoredKey) bool {
			keys = append(keys, gapKey{kind: k.Kind, ns: k.EventNs, w: k.W, current: k.Current})
			return true
		})
		if err != nil {
			return nil, fmt.Errorf("runner: %s: scanning %s: %w", v.Name, p.q.Fps[0], err)
		}
		lengths, cks := gapsOf(keys)
		pg := PrefixGaps{Fp: p.q.Fps[0].String(), Rank: p.q.Rank, Checkpoints: len(cks), Gaps: len(lengths)}
		for _, k := range keys {
			if k.kind == pebblelog.KeyRecord {
				pg.Records++
			}
		}
		pg.Median, pg.P90, pg.Max, pg.Holes = GapStats(lengths)
		pg.HoleOver = HoleFactor * float64(pg.Median)
		if haveTallies {
			if prefix, ok := sc.PrefixOf(p.q.Layer, p.q.Fps[0], p.q.Dir); ok {
				t := tallies[string(prefix)]
				pg.Tallies = &Tally{Invalidated: t[0], Replaced: t[1], Capped: t[2]}
				rep.Invalidated += t[0]
				rep.Replaced += t[1]
				rep.Capped += t[2]
			}
		}
		for _, qi := range p.qs {
			q := plan.Queries[qi]
			qg := QueryGap{Age: q.Age, ReadSteps: -1, Gap: -1}
			tNs, where := pebblekv.Locate(q.At)
			switch where {
			case pebblekv.Before:
				tNs = -1 // nothing is at or before it
			case pebblekv.After:
				tNs = math.MaxInt64
			}
			qg.ModelSteps, qg.Ends, qg.Skipped = walkSteps(keys, tNs, q.AsOf)
			qg.Where, qg.Gap = place(keys, cks, tNs)
			if qg.Where == "gap" {
				qg.GapRecords = lengths[qg.Gap]
				if pg.Median > 0 {
					qg.Multiple = float64(qg.GapRecords) / float64(pg.Median)
				}
			}
			if haveResults {
				if n, ok := readSteps[qi]; ok {
					qg.ReadSteps = n
					if n != qg.ModelSteps {
						rep.Mismatched++
					}
				}
			}
			pg.Queries = append(pg.Queries, qg)
		}
		rep.Prefixes = append(rep.Prefixes, pg)
		rep.Holes += pg.Holes
		rep.Gaps += pg.Gaps
		medians = append(medians, float64(pg.Median))
	}
	slices.Sort(medians)
	switch n := len(medians); {
	case n%2 == 1:
		rep.MedianOfMedians = medians[n/2]
	case n > 0:
		rep.MedianOfMedians = (medians[n/2-1] + medians[n/2]) / 2
	}
	return rep, nil
}

// loadReadSteps reads, if the build has a results file, the internal steps of each
// neighbors query by its index in the plan. A results file of another candidate or
// plan is refused.
func loadReadSteps(dir string, plan *Plan, v candidates.Variant) (steps map[int]int64, ok bool, err error) {
	path := filepath.Join(dir, ResultsFile)
	b, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, err
	}
	var r Results
	if err := json.Unmarshal(b, &r); err != nil {
		return nil, false, fmt.Errorf("runner: %s: %w", path, err)
	}
	planDigest, err := plan.Digest()
	if err != nil {
		return nil, false, err
	}
	if r.Candidate != v.Name || r.PlanDigest != planDigest || len(r.Queries) != len(plan.Queries) {
		return nil, false, fmt.Errorf("runner: %s is not the results of %s on this plan: %w", path, v.Name, errMismatch)
	}
	steps = map[int]int64{}
	for i, q := range plan.Queries {
		if q.Op != OpNeighbors {
			continue
		}
		if n, has := r.Queries[i].Counters["read.neighbors.internal_steps"]; has {
			steps[i] = n
		}
	}
	return steps, true, nil
}

// Write prints the report: for each prefix its checkpoints, gaps, holes and
// tallies, then its reads of the given ages (the 9 h and 1 d ones if there are none
// given), and the totals. The file for tools has every age.
func (r *GapReport) Write(w io.Writer, ages ...string) {
	if len(ages) == 0 {
		ages = []string{Age9h, Age1d}
	}
	fmt.Fprintf(w, "gaps of %s\n", r.Candidate)
	fmt.Fprintln(w, "  hole counts use the gaps between checkpoints only; the earlier evidence's scan also counted the span down to the baseline, so the two are not comparable")
	if r.Canonical {
		fmt.Fprintln(w, "  the build is canonical: the model's steps are exact")
	} else {
		fmt.Fprintln(w, "  the build is NOT canonical: the model's steps are a lower bound (the iterator also steps over versions and tombstones a compaction has not dropped)")
	}
	if !r.HasTallies {
		fmt.Fprintln(w, "  no tallies file: the build was not untimed, or is an earlier one")
	}
	if !r.HasResults {
		fmt.Fprintln(w, "  no results file: the model's steps are not set beside the read's")
	}
	for _, p := range r.Prefixes {
		fmt.Fprintf(w, "%s (rank %d): %d checkpoints, %d records, %d gaps: median %d, p90 %d, max %d; holes (over %.1f): %d",
			p.Fp, p.Rank, p.Checkpoints, p.Records, p.Gaps, p.Median, p.P90, p.Max, p.HoleOver, p.Holes)
		if t := p.Tallies; t != nil {
			fmt.Fprintf(w, "; invalidated %d, replaced %d, capped %d", t.Invalidated, t.Replaced, t.Capped)
		}
		fmt.Fprintln(w)
		for _, q := range p.Queries {
			if !slices.Contains(ages, q.Age) {
				continue
			}
			fmt.Fprintf(w, "    %-3s model %d steps", q.Age, q.ModelSteps)
			if q.ReadSteps >= 0 {
				fmt.Fprintf(w, " (read %d)", q.ReadSteps)
			}
			fmt.Fprintf(w, ", to the %s", q.Ends)
			if q.Skipped > 0 {
				fmt.Fprintf(w, " (past %d)", q.Skipped)
			}
			switch q.Where {
			case "gap":
				fmt.Fprintf(w, "; in gap %d of %d records, %.2fx the median", q.Gap, q.GapRecords, q.Multiple)
			default:
				fmt.Fprintf(w, "; in the %s", q.Where)
			}
			fmt.Fprintln(w)
		}
	}
	fmt.Fprintf(w, "all %d prefixes: %d gaps, %d holes, median of the prefixes' median gaps %.1f", len(r.Prefixes), r.Gaps, r.Holes, r.MedianOfMedians)
	if r.HasTallies {
		fmt.Fprintf(w, "; invalidated %d, replaced %d, capped %d", r.Invalidated, r.Replaced, r.Capped)
	}
	fmt.Fprintln(w)
	if r.HasResults {
		fmt.Fprintf(w, "model against the read: %d queries differ in steps\n", r.Mismatched)
	}
}

// WriteGapsFile writes the report as JSON to path.
func WriteGapsFile(path string, r *GapReport) error { return writeJSON(path, r) }
