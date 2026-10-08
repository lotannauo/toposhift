package runner

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"slices"
	"sort"
	"strings"
	"text/tabwriter"
	"time"
)

// Candidate is a built and read candidate: its manifest and its results.
type Candidate struct {
	Manifest *Manifest
	Results  *Results
}

// LoadCandidate reads the manifest and results under dir.
func LoadCandidate(dir string) (*Candidate, error) {
	m, manifestDigest, err := readManifest(dir)
	if err != nil {
		return nil, err
	}
	b, err := os.ReadFile(filepath.Join(dir, ResultsFile))
	if err != nil {
		return nil, fmt.Errorf("runner: %s has no results: it was built but not read: %w", dir, err)
	}
	var r Results
	if err := json.Unmarshal(b, &r); err != nil {
		return nil, fmt.Errorf("runner: %s: %w", filepath.Join(dir, ResultsFile), err)
	}
	if r.ManifestDigest != manifestDigest {
		return nil, fmt.Errorf("runner: %s: the results were read from another manifest than the one there now: %w", dir, errMismatch)
	}
	return &Candidate{Manifest: m, Results: &r}, nil
}

// Check returns the reasons the candidates cannot be compared, or nil: results
// from different plans, from different binaries, from different Pebble options,
// from a binary a result may not come from, or with an answer that is not the
// reference engine's. A validation run is never a result, so it is always one
// reason; allowUntimed leaves that one out, for a caller who is asking whether the
// run was valid and not whether it can be compared.
func Check(plan *Plan, cs []*Candidate, allowUntimed bool) []string {
	planDigest, err := plan.Digest()
	if err != nil {
		return []string{err.Error()}
	}
	var out []string
	for i, c := range cs {
		name := c.Results.Candidate
		if c.Results.PlanDigest != planDigest || c.Manifest.PlanDigest != planDigest {
			out = append(out, name+": built or read from another plan")
		}
		if len(c.Results.Queries) != len(plan.Queries) {
			out = append(out, fmt.Sprintf("%s: %d results for %d queries", name, len(c.Results.Queries), len(plan.Queries)))
		}
		if !allowUntimed && (c.Results.Untimed || c.Manifest.Untimed) {
			out = append(out, name+": built or read by a binary a result may not come from (an untimed validation run)")
		}
		if n := len(c.Results.Mismatches); n > 0 {
			out = append(out, fmt.Sprintf("%s: %d answers differ from the reference engine's, first: %s", name, n, c.Results.Mismatches[0]))
		}
		if n := len(c.Manifest.UncompactedWrong); n > 0 {
			out = append(out, fmt.Sprintf("%s: %d answers differ from the reference engine's before the build was compacted, first: %s", name, n, c.Manifest.UncompactedWrong[0]))
		}
		if n := len(c.Results.Unstable); n > 0 {
			out = append(out, fmt.Sprintf("%s: %d queries cost something different on the second pass, first: %s", name, n, c.Results.Unstable[0]))
		}
		if c.Manifest.Stream.Digest != plan.Stream.Digest {
			out = append(out, name+": built from another stream")
		}
		if !sameBinary(c.Manifest.Build, c.Results.Build) {
			out = append(out, name+": built and read by different binaries")
		}
		if i == 0 {
			continue
		}
		ref := cs[0]
		if !sameBinary(c.Manifest.Build, ref.Manifest.Build) {
			out = append(out, fmt.Sprintf("%s and %s were built by different binaries", ref.Results.Candidate, name))
		}
		// A build that rested after each retention and one that did not have timings that
		// are not alike (the batches after a retention carry no catch-up in the first), so
		// the commit gates would hold one to the other.
		if a, b := ref.Manifest.Describe[RestKey], c.Manifest.Describe[RestKey]; a != b {
			out = append(out, fmt.Sprintf("%s and %s were built with and without a rest after each retention (%q and %q): their timings are not comparable", ref.Results.Candidate, name, a, b))
		}
		// A retention that settles its tombstones before it returns costs the writer what
		// the settling takes, and one that does not leaves them to slow the batches after
		// it: the timings of the two are not alike.
		if a, b := describedOr(ref.Manifest, SettleKey, "false"), describedOr(c.Manifest, SettleKey, "false"); a != b {
			out = append(out, fmt.Sprintf("%s and %s were built with and without settling a retention's tombstones (%q and %q): their timings are not comparable", ref.Results.Candidate, name, a, b))
		}
		// Canonical tables are cut where the data says and the others where the
		// compactions happened to cut, so the blocks their reads load are not alike.
		if a, b := ref.Manifest.Describe[CanonicalKey], c.Manifest.Describe[CanonicalKey]; a != b {
			out = append(out, fmt.Sprintf("%s and %s were built with and without the canonical layout (%q and %q): the blocks their reads load are not comparable", ref.Results.Candidate, name, a, b))
		}
		// A commit that waits for the log to reach the disk takes a different time from
		// one that does not, whatever else is alike.
		if a, b := describedOr(ref.Manifest, SyncKey, "false"), describedOr(c.Manifest, SyncKey, "false"); a != b {
			out = append(out, fmt.Sprintf("%s and %s were built with and without a sync of every commit (%q and %q): their timings are not comparable", ref.Results.Candidate, name, a, b))
		}
		// Sampling the database's metrics takes its metrics lock, so builds that sampled
		// at different intervals (or one that did not) were not slowed alike.
		if a, b := describedOr(ref.Manifest, MetricsKey, "0s"), describedOr(c.Manifest, MetricsKey, "0s"); a != b {
			out = append(out, fmt.Sprintf("%s and %s were built with different metric sampling (%q and %q): their timings are not comparable", ref.Results.Candidate, name, a, b))
		}
		if diff := optionsDiff(ref.Manifest.Describe["pebble_options"], c.Manifest.Describe["pebble_options"]); diff != "" {
			out = append(out, fmt.Sprintf("%s and %s ran with different Pebble options: %s", ref.Results.Candidate, name, diff))
		}
	}
	return out
}

// sameBinary is whether two builds are the same toolchain, platform and source.
func sameBinary(a, b BuildInfo) bool {
	return a.GoVersion == b.GoVersion && a.GOOS == b.GOOS && a.GOARCH == b.GOARCH && a.Revision == b.Revision &&
		a.CGO == b.CGO && a.Race == b.Race && a.Invariants == b.Invariants && a.Modified == b.Modified && a.Executable == b.Executable
}

// optionsDiff compares two Pebble option texts, leaving out the lines that name
// the comparer, the key schema and the collectors, which are what a candidate is
// allowed to differ in. It returns the first difference, or "".
func optionsDiff(a, b string) string {
	keep := func(s string) []string {
		var out []string
		for _, l := range strings.Split(s, "\n") {
			t := strings.TrimSpace(l)
			if strings.HasPrefix(t, "comparer=") || strings.HasPrefix(t, "key_schema=") || strings.Contains(t, "collector") || strings.Contains(t, "block_property") {
				continue
			}
			out = append(out, t)
		}
		return out
	}
	la, lb := keep(a), keep(b)
	for i := 0; i < max(len(la), len(lb)); i++ {
		var x, y string
		if i < len(la) {
			x = la[i]
		}
		if i < len(lb) {
			y = lb[i]
		}
		if x != y {
			return fmt.Sprintf("%q against %q", x, y)
		}
	}
	return ""
}

// metrics are the per-read counters the report sets side by side, all in the unit
// every layout shares (Pebble's iterator statistics). None of them is the cost of a
// read on its own, and they do not agree: a layout that positions the iterator at
// every key (seeks) and one that walks a run (steps) are each cheap in the unit
// the other pays in, and a block that is reloaded at every seek is counted at every
// seek. The decision rule names a combination of them; the report does not choose one.
type metricSpec struct {
	title, counter string
	// perOp says the counter is named for the read ("read.neighbors.steps") and not
	// for the layout ("read.checkpoint_entries_decoded").
	perOp bool
}

var metrics = []metricSpec{
	{"seeks (calls that position the iterator)", "seeks", true},
	{"internal seeks (what the iterator did underneath)", "internal_seeks", true},
	{"steps (Next calls the layout made; a layout that skips with NextPrefix counts it once)", "steps", true},
	{"internal steps (what the iterator did underneath)", "internal_steps", true},
	{"points iterated", "points", true},
	{"key bytes of the points iterated", "key_bytes", true},
	{"value bytes of the points iterated", "value_bytes", true},
	{"block bytes loaded (every load counts, a cached block too, and again at each seek that reloads it)", "block_bytes", true},
	{"blocks loaded with the cache empty and the tables open (the distinct blocks a read needs; a reload within it is a hit)", CounterBlockLoads, false},
	{"compressed bytes of the index, filter and data blocks loaded with the cache empty", CounterColdBlockBytes, false},
	{"bytes the cache holds after a read from empty (uncompressed, values included; a diagnostic: near the cache size, loads were repeated)", CounterColdCacheBytes, false},
	{"allocations of a read once the pools are full (it counts what the runner does with the answer; depends on the process)", CounterAllocs, false},
	{"bytes allocated by a read, the same way", CounterAllocBytes, false},
	{"points a range tombstone covered", "covered_by_tombstones", true},
	{"values Pebble reports in value blocks (a diagnostic: it drifts for the batched reads of layout M)", "separated_values", true},
	{"bytes fetched from value blocks", "separated_value_bytes_fetched", true},
	{"records stepped over (layout L's own count: every record, those above the token too)", "read.records_stepped", false},
	{"versions stepped over (layout M's own count: versions decoded, not the seeks between them)", "read.versions_stepped", false},
	{"checkpoint entries decoded (layout L with checkpoints)", "read.checkpoint_entries_decoded", false},
	{"baseline entries decoded (layout L after a retention)", "read.baseline_entries_decoded", false},
	{"checkpoints used (layout L)", "read.checkpoint_hits", false},
	{"checkpoints skipped for a token below their snapshot (layout L)", "read.checkpoint_skipped_w", false},
	{"checkpoints skipped for another fold version (layout L)", "read.checkpoint_skipped_version", false},
}

// headline are the ages the short report shows: now, an hour, three hours, nine
// hours and a day back, the old snapshot, after every refresh lapsed and a day's
// window. The full report has every age (an hour's window besides).
var headline = map[string]bool{AgeNow: true, Age1h: true, Age3h: true, Age9h: true, Age1d: true, AgeOldToken: true, AgeDead: true, AgeWindow1d: true}

// Write prints the comparison: what the plan is, whether the candidates can be
// compared, what each holds, and what the reads cost. Unless full is set, the
// tables of reads show only the headline ages.
func Write(w io.Writer, plan *Plan, cs []*Candidate, full bool) {
	slices.SortFunc(cs, func(a, b *Candidate) int { return strings.Compare(a.Results.Candidate, b.Results.Candidate) })
	fmt.Fprintf(w, "spec %.12s, rules %.12s: %d records (%d dropped before the horizon), %d queries over %d entities, cache %d MiB, %d retention(s)\n",
		plan.SpecDigest, plan.RulesDigest, plan.Stream.Records, plan.Stream.Dropped, len(plan.Queries), plan.Shadow, plan.CacheBytes>>20, len(plan.Stream.Retentions))
	if now, err := DefaultRules().Digest(); err == nil && now != plan.RulesDigest {
		fmt.Fprintf(w, "(the rules of this build are %.12s: they have changed since the plan was made, which leaves it valid)\n", now)
	}
	if problems := Check(plan, cs, false); len(problems) > 0 {
		fmt.Fprintln(w, "\nTHESE RESULTS ARE NOT TO BE COMPARED:")
		for _, p := range problems {
			fmt.Fprintln(w, "  -", p)
		}
	} else {
		fmt.Fprintf(w, "every answer of every candidate is the reference engine's (%d candidates)\n", len(cs))
	}

	tw := tabwriter.NewWriter(w, 2, 0, 2, ' ', tabwriter.AlignRight)
	row := func(label string, f func(c *Candidate) string) {
		fmt.Fprint(tw, label)
		for _, c := range cs {
			fmt.Fprint(tw, "\t", f(c))
		}
		fmt.Fprintln(tw, "\t")
	}
	fmt.Fprintln(w, "\nwhat each candidate holds, after everything is compacted:")
	row("", func(c *Candidate) string { return c.Results.Candidate })
	row("table bytes", func(c *Candidate) string { return bytesStr(c.Manifest.StatsCompacted["live_table_bytes"]) })
	row("table bytes per record written (those retained away too)", func(c *Candidate) string {
		return fmt.Sprintf("%.0f", float64(c.Manifest.StatsCompacted["live_table_bytes"])/float64(max(c.Manifest.Stream.Records, 1)))
	})
	row("tables", func(c *Candidate) string { return fmt.Sprint(sumPrefix(c.Manifest.StatsCompacted, "tables_l")) })
	row("tombstones left", func(c *Candidate) string { return fmt.Sprint(c.Manifest.StatsCompacted["tombstones"]) })
	for _, layer := range []string{"L0", "L1", "L2", "L3"} {
		row("  "+layer+" table bytes", func(c *Candidate) string { return bytesStr(c.Manifest.SizeByLayer[layer]) })
	}
	for _, kind := range partKinds(cs) {
		row("  "+kind+" (logical)", func(c *Candidate) string { return bytesStr(sumKind(c.Manifest.Breakdown, kind)) })
	}
	row("write amplification built (depends on when compactions ran)", func(c *Candidate) string {
		s := c.Manifest.StatsBuilt
		if s["bytes_in"] == 0 {
			return "-"
		}
		return fmt.Sprintf("%.2f", float64(s["bytes_flushed"]+s["bytes_compacted"])/float64(s["bytes_in"]))
	})
	row("flushes, compactions while building (also depend on when compactions ran)", func(c *Candidate) string {
		return fmt.Sprintf("%d, %d", c.Manifest.StatsBuilt["flushes"], c.Manifest.StatsBuilt["compactions"])
	})
	for _, name := range []string{"retain.records_replayed", "retain.keys_visited", "retain.seeks", "retain.range_deletes", "checkpoint.written", "checkpoint.invalidated", "checkpoint.build_records_walked"} {
		if anyHas(cs, name) {
			row("  "+name, func(c *Candidate) string { return fmt.Sprint(c.Manifest.Counters[name]) })
		}
	}
	_ = tw.Flush()
	writeTiming(w, cs)

	fmt.Fprintln(w, "\nWhat a read cost is all of the tables below together, not any one: they count different things and can disagree in direction.")
	for _, m := range metrics {
		if !m.perOp && !anyQueryHas(cs, m.counter) {
			continue
		}
		fmt.Fprintf(w, "\n%s, mean per query (ratio to the smallest in brackets):\n", m.title)
		writeMetric(w, plan, cs, m, full)
	}
	writeColdCacheWarning(w, plan, cs)
	writeUncompacted(w, plan, cs)
	if plan.Spec.Projected() {
		fmt.Fprintln(w, "\nA stream of pins is a projection of the full one: only G0 is judged here; the stall, commit, bytes and checkpoint gates are those of a full build, and the block counters (which a projection has fewer index blocks in) are not a basis for choosing between candidates in the verdicts below.")
	}
	WriteGates(w, GatesOf(plan, cs, DefaultRules()))
	writeVerdicts(w, plan, cs, full)
}

// writeTiming prints how long the writes of each build took. It is informational:
// a timing off a laptop says where the time goes and chooses nothing.
func writeTiming(w io.Writer, cs []*Candidate) {
	any := false
	for _, c := range cs {
		if c.Manifest.Timing.Writes.Count > 0 {
			any = true
		}
	}
	if !any {
		return
	}
	fmt.Fprintln(w, "\nhow long the writes of the build took (timing on the machine that built it: informational, not a result):")
	tw := tabwriter.NewWriter(w, 2, 0, 2, ' ', tabwriter.AlignRight)
	fmt.Fprint(tw, "")
	for _, c := range cs {
		fmt.Fprint(tw, "\t", c.Results.Candidate)
	}
	fmt.Fprintln(tw, "\t")
	row := func(label string, f func(t Timing) string) {
		fmt.Fprint(tw, label)
		for _, c := range cs {
			fmt.Fprint(tw, "\t", f(c.Manifest.Timing))
		}
		fmt.Fprintln(tw, "\t")
	}
	dur := func(ns int64) string { return time.Duration(ns).Round(time.Microsecond).String() }
	row("batches written", func(t Timing) string { return fmt.Sprint(t.Writes.Count) })
	row("a batch: median (bucket bound)", func(t Timing) string { return dur(t.Writes.Quantile(0.5)) })
	row("a batch: 99th percentile (bucket bound)", func(t Timing) string { return dur(t.Writes.Quantile(0.99)) })
	row("a batch: longest", func(t Timing) string { return dur(t.Writes.MaxNs) })
	row("a batch: mean", func(t Timing) string {
		if t.Writes.Count == 0 {
			return "-"
		}
		return dur(t.Writes.TotalNs / t.Writes.Count)
	})
	row("retentions: longest", func(t Timing) string {
		if len(t.Retains) == 0 {
			return "-"
		}
		return dur(slices.Max(t.Retains))
	})
	row("retentions: total", func(t Timing) string {
		var n int64
		for _, r := range t.Retains {
			n += r
		}
		return dur(n)
	})
	row("the first batch after a retention: longest", func(t Timing) string {
		if len(t.AfterRetention) == 0 {
			return "-"
		}
		return dur(slices.Max(t.AfterRetention))
	})
	fmt.Fprint(tw, "retention plus the slowness after it: longest (G2)")
	for _, c := range cs {
		if m := measureOf(c.Manifest); m.hasStall {
			fmt.Fprint(tw, "\t", dur(m.stall))
		} else {
			fmt.Fprint(tw, "\t-")
		}
	}
	fmt.Fprintln(tw, "\t")
	row("settling of a retention: longest", func(t Timing) string {
		longest, _, ok := settlingOf(t)
		if !ok {
			return "-"
		}
		return dur(int64(longest))
	})
	row("settle deadline reached", func(t Timing) string {
		_, hits, ok := settlingOf(t)
		if !ok {
			return "-"
		}
		return fmt.Sprint(hits)
	})
	_ = tw.Flush()
}

// writeUncompacted compares, for the cells of reads of "now", what a read cost at the
// end of the build before anything was compacted and what it cost after: the tables as
// the stream left them (the memtable, level 0, the tombstones of retention) against
// the few large ones a compaction makes. These are labelled cells, kept out of the
// verdicts and of the mixed cells, because the shape of the tables at the end of a build
// depends on when compactions happened to run.
func writeUncompacted(w io.Writer, plan *Plan, cs []*Candidate) {
	type sums struct{ before, after [3]float64 }
	exprs := [3]string{"seeks", "internal_steps+checkpoint_entries_decoded+baseline_entries_decoded", "block_bytes"}
	rows := map[string][]sums{}
	counts := map[string][]int{}
	var order []string
	byName := make([]map[string]int, len(cs))
	for j, c := range cs {
		byName[j] = map[string]int{}
		for i, u := range c.Manifest.Uncompacted {
			byName[j][u.Query] = i
		}
	}
	for i, q := range plan.Queries {
		if q.Age != AgeNow {
			continue
		}
		key := q.Group
		for j, c := range cs {
			ui, ok := byName[j][q.Name()]
			if !ok || i >= len(c.Results.Queries) {
				continue
			}
			if _, ok := rows[key]; !ok {
				rows[key], counts[key] = make([]sums, len(cs)), make([]int, len(cs))
				order = append(order, key)
			}
			u := UncompactedRead{Query: q.Name(), Counters: c.Manifest.Uncompacted[ui].Counters}
			for k, e := range exprs {
				rows[key][j].before[k] += float64(counterValue(QueryResult{Counters: u.Counters}, q, e))
				rows[key][j].after[k] += float64(counterValue(c.Results.Queries[i], q, e))
			}
			counts[key][j]++
		}
	}
	if len(order) == 0 {
		return
	}
	fmt.Fprintln(w, "\nreads of \"now\" before and after the compaction, mean per query (seeks / steps + decoded entries / gross block bytes), a labelled cell outside the verdicts: the tables as the stream left them, then as everything was compacted into few:")
	tw := tabwriter.NewWriter(w, 2, 0, 2, ' ', tabwriter.AlignRight)
	for _, c := range cs {
		fmt.Fprint(tw, "\t", c.Results.Candidate)
	}
	fmt.Fprintln(tw, "\t")
	for _, key := range order {
		fmt.Fprint(tw, key)
		for j := range cs {
			n := float64(counts[key][j])
			if n == 0 {
				fmt.Fprint(tw, "\t-")
				continue
			}
			var parts []string
			for k := range exprs {
				parts = append(parts, fmt.Sprintf("%.0f>%.0f", rows[key][j].before[k]/n, rows[key][j].after[k]/n))
			}
			fmt.Fprint(tw, "\t", strings.Join(parts, " "))
		}
		fmt.Fprintln(tw, "\t")
	}
	_ = tw.Flush()
}

// writeColdCacheWarning says when a read from an empty cache filled more than half
// of it: the cache was then too small for the read to count every block once, and
// its cold counts may include blocks loaded twice.
func writeColdCacheWarning(w io.Writer, plan *Plan, cs []*Candidate) {
	for _, c := range cs {
		n := 0
		for _, q := range c.Results.Queries {
			if 2*q.Counters[CounterColdCacheBytes] > plan.CacheBytes {
				n++
			}
		}
		if n > 0 {
			fmt.Fprintf(w, "\nWARNING: %s: %d queries filled more than half of the %d MiB cache from empty; their cold counts may include blocks loaded twice.\n", c.Results.Candidate, n, plan.CacheBytes>>20)
		}
	}
}

// writeVerdicts says how the counters of the decision rules put the candidates in
// order, cell by cell, and lists the cells in which they disagree: the ones that
// timing on CI hardware settles. The list is a function of the counters, so it can
// be fixed (it has a digest) before anything is timed.
func writeVerdicts(w io.Writer, plan *Plan, cs []*Candidate, full bool) {
	if len(cs) < 2 {
		return
	}
	rules := DefaultRules()
	vs := Verdicts(plan, cs, rules)
	mixed := MixedCells(vs)
	ordered := 0
	for _, v := range vs {
		if v.Ordered() {
			ordered++
		}
	}
	fmt.Fprintf(w, "\nHow the counters of the rules (%s; a counter orders two candidates in a cell if the larger mean is above its floor and more than x%.2f the smaller) put the candidates in order, over %d pairs of candidates in %d cells:\n",
		strings.Join(rules.OrderCounters, ", "), rules.OrderTolerance, len(vs), len(vs)/max(len(cs)*(len(cs)-1)/2, 1))
	fmt.Fprintf(w, "  ordered %d, mixed %d (counters disagree), tied on counters %d\n", ordered, len(mixed), len(vs)-ordered-len(mixed))
	if len(mixed) == 0 {
		return
	}
	rulesDigest, _ := rules.Digest()
	fmt.Fprintf(w, "The mixed cells are the ones timing settles; their list has digest %.16s under rules %.12s.\n", MixedDigest(mixed), rulesDigest)
	shown := 0
	for _, v := range mixed {
		if !full && !headline[v.Cell[strings.LastIndexByte(v.Cell, ' ')+1:]] {
			continue
		}
		shown++
		fmt.Fprintf(w, "  %s: %s costs less in %s; %s costs less in %s (of %d queries: %d put %s first, %d put %s first, %d mixed)\n",
			v.Cell, v.A, strings.Join(v.BetterA, ", "), v.B, strings.Join(v.BetterB, ", "), v.Queries, v.AheadA, v.A, v.AheadB, v.B, v.MixedQueries)
	}
	if shown < len(mixed) {
		fmt.Fprintf(w, "  (%d more at the other ages, in the full report)\n", len(mixed)-shown)
	}
}

// anyQueryHas is whether any query of any candidate recorded the counter.
func anyQueryHas(cs []*Candidate, counter string) bool {
	for _, c := range cs {
		for _, q := range c.Results.Queries {
			if q.Counters[counter] != 0 {
				return true
			}
		}
	}
	return false
}

func anyHas(cs []*Candidate, counter string) bool {
	for _, c := range cs {
		if _, ok := c.Manifest.Counters[counter]; ok {
			return true
		}
	}
	return false
}

func bytesStr(n int64) string {
	switch {
	case n >= 1<<30:
		return fmt.Sprintf("%.2f GiB", float64(n)/(1<<30))
	case n >= 1<<20:
		return fmt.Sprintf("%.1f MiB", float64(n)/(1<<20))
	case n >= 1<<10:
		return fmt.Sprintf("%.1f KiB", float64(n)/(1<<10))
	}
	return fmt.Sprintf("%d B", n)
}

func sumPrefix(m map[string]int64, prefix string) int64 {
	var n int64
	for k, v := range m {
		if strings.HasPrefix(k, prefix) {
			n += v
		}
	}
	return n
}

func sumKind(breakdown map[string]int64, kind string) int64 {
	var n int64
	for k, v := range breakdown {
		if _, part, _ := strings.Cut(k, "/"); part == kind {
			n += v
		}
	}
	return n
}

// partKinds lists the kinds of part any candidate reports, in a fixed order.
func partKinds(cs []*Candidate) []string {
	seen := map[string]bool{}
	for _, c := range cs {
		for k := range c.Manifest.Breakdown {
			_, part, _ := strings.Cut(k, "/")
			seen[part] = true
		}
	}
	out := make([]string, 0, len(seen))
	for k := range seen {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

// writeMetric prints one counter's mean per query for each row of queries (a
// group, at an age) and each candidate, with the ratio to the smallest.
func writeMetric(w io.Writer, plan *Plan, cs []*Candidate, m metricSpec, full bool) {
	type cell struct{ sum, n int64 }
	rows := map[string][]cell{}
	var order []string
	for i, q := range plan.Queries {
		if !full && !headline[q.Age] {
			continue
		}
		key := q.Group + " " + q.Age
		if _, ok := rows[key]; !ok {
			rows[key] = make([]cell, len(cs))
			order = append(order, key)
		}
		name := m.counter
		if m.perOp {
			name = "read." + string(q.Op) + "." + m.counter
		}
		for j, c := range cs {
			if i >= len(c.Results.Queries) {
				continue
			}
			rows[key][j].sum += c.Results.Queries[i].Counters[name]
			rows[key][j].n++
		}
	}
	tw := tabwriter.NewWriter(w, 2, 0, 2, ' ', tabwriter.AlignRight)
	for _, c := range cs {
		fmt.Fprint(tw, "\t", c.Results.Candidate)
	}
	fmt.Fprintln(tw, "\t")
	for _, key := range order {
		cells := rows[key]
		fmt.Fprint(tw, key)
		bestMean := -1.0
		means := make([]float64, len(cells))
		for j, c := range cells {
			if c.n == 0 {
				means[j] = -1
				continue
			}
			means[j] = float64(c.sum) / float64(c.n)
			if bestMean < 0 || means[j] < bestMean {
				bestMean = means[j]
			}
		}
		for _, m := range means {
			switch {
			case m < 0:
				fmt.Fprint(tw, "\t-")
			case bestMean <= 0:
				fmt.Fprintf(tw, "\t%.0f", m)
			default:
				fmt.Fprintf(tw, "\t%.0f (x%.1f)", m, m/bestMean)
			}
		}
		fmt.Fprintln(tw, "\t")
	}
	_ = tw.Flush()
}
