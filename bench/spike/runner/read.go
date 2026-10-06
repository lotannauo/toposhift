package runner

import (
	"context"
	"fmt"
	"maps"
	"path/filepath"
	"slices"
	"strings"

	"github.com/lotannauo/toposhift/bench/spike/candidates"
)

// QueryResult is what one query did on one candidate.
type QueryResult struct {
	// Digest and Size are the answer.
	Digest string
	Size   int
	// Counters are what the read cost, summed over the iterators it made, from the
	// first pass over the queries: Pebble's own statistics under
	// "read.<op>.<name>" and the layout's counters; and, from the passes after
	// it, the cold counters (block_loads, cold_block_bytes, cold_cache_bytes) and
	// the allocations (allocs, alloc_bytes) under their own names.
	Counters map[string]int64
	// Warm are the counters that depend on the block cache (bytes served from it)
	// and on time, from the second pass.
	Warm map[string]int64
}

// Results are what a read of one built candidate came to.
type Results struct {
	Candidate  string
	PlanDigest string
	// ManifestDigest is the SHA-256 of the manifest file the database was opened
	// against.
	ManifestDigest string
	Build          BuildInfo
	Untimed        bool
	Describe       map[string]string
	// Queries is parallel to the plan's queries.
	Queries []QueryResult
	// Mismatches names the queries whose answer is not the reference engine's, and
	// Unstable those whose counters differed between the two passes or between
	// the two cold reads (their names end in "(cold)").
	Mismatches, Unstable []string
	// StatsBefore and StatsAfter are the database's counters around the passes;
	// the flushes and compactions among them are equal, or the read is refused.
	StatsBefore, StatsAfter map[string]int64
}

// stable lists the descriptions that a database must give the same whenever it
// is opened with the same cache: what makes the variant what it is. The others
// (the compaction switches, the options text that contains them) differ between
// the build and the read on purpose.
var stable = []string{
	"layout", "comparer", "key_schema_in_tables", "collectors_in_tables", "time_filter_asked", "checkpoints",
	"block_bytes", "memtable_bytes", "target_file_bytes", "l_base_max_bytes", "l0_compaction_threshold", "cache_bytes", "sync",
}

// Read opens the database built under dir in a fresh engine, read-only, and puts
// every query of the plan to it: twice as it comes, for the answer and for the
// counters of Pebble's iterator, which must repeat; twice more with the block cache
// emptied, for the blocks a first read needs; and a few times more for its
// allocations. It records the answer and the cost of each. A database that is not
// the plan's, whose tables are not the ones the build left, or that works while it
// is being read, is refused.
func Read(ctx context.Context, plan *Plan, v candidates.Variant, dir string, g Guards, say Progress) (*Results, error) {
	if err := g.Check(); err != nil {
		return nil, err
	}
	m, manifestDigest, err := readManifest(dir)
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

	rec := NewCapture()
	e, err := openReadOnly(plan, v, dir, rec)
	if err != nil {
		return nil, err
	}
	defer func() { _ = e.Close() }()

	desc, err := e.Describe()
	if err != nil {
		return nil, err
	}
	if err := sameAsBuilt(e, desc, m, plan); err != nil {
		return nil, fmt.Errorf("runner: %s %w", v.Name, err)
	}

	// A database that is not read-only reads the tables it has just opened in the
	// background, to estimate what their deletions hold, and until it is done the
	// blocks it loads compete with the first queries. A read-only one has nothing
	// running, and a candidate that opens read-write (a test double) waits.
	if err := e.Quiesce(ctx); err != nil {
		return nil, fmt.Errorf("runner: %s: %w", v.Name, err)
	}

	res := &Results{
		Candidate: v.Name, PlanDigest: planDigest, ManifestDigest: manifestDigest,
		Build: g.Info, Untimed: g.Untimed || m.Untimed, Describe: desc,
		Queries: make([]QueryResult, len(plan.Queries)), StatsBefore: e.Stats(),
	}
	say.say("read %s: %d queries, two passes", v.Name, len(plan.Queries))
	rec.Take()
	for i, q := range plan.Queries {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		a, err := Ask(e, q)
		if err != nil {
			return nil, fmt.Errorf("runner: %s: %s: %w", v.Name, q.Name(), err)
		}
		res.Queries[i] = QueryResult{Digest: a.Digest, Size: a.Size, Counters: rec.Take()}
		if a.Digest != q.Expect {
			res.Mismatches = append(res.Mismatches, q.Name())
		}
	}
	for i, q := range plan.Queries {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		a, err := Ask(e, q)
		if err != nil {
			return nil, fmt.Errorf("runner: %s: %s (second pass): %w", v.Name, q.Name(), err)
		}
		second := rec.Take()
		first := res.Queries[i]
		if a.Digest != first.Digest || !sameCounters(first.Counters, second) {
			res.Unstable = append(res.Unstable, q.Name())
		}
		res.Queries[i].Warm = map[string]int64{}
		for name, n := range second {
			if volatile(name) {
				res.Queries[i].Warm[name] = n
			}
		}
	}
	say.say("read %s: cold and allocation passes", v.Name)
	for i, q := range plan.Queries {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		first, c1, err := coldAsk(e, rec, q)
		if err != nil {
			return nil, fmt.Errorf("runner: %s: %s (cold): %w", v.Name, q.Name(), err)
		}
		second, c2, err := coldAsk(e, rec, q)
		if err != nil {
			return nil, fmt.Errorf("runner: %s: %s (cold, again): %w", v.Name, q.Name(), err)
		}
		if first.Digest != res.Queries[i].Digest || second.Digest != res.Queries[i].Digest || !sameValues(c1, c2) {
			res.Unstable = append(res.Unstable, q.Name()+" (cold)")
		}
		maps.Copy(res.Queries[i].Counters, c1)
	}
	for i, q := range plan.Queries {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		allocs, err := allocAsk(e, rec, q)
		if err != nil {
			return nil, fmt.Errorf("runner: %s: %s (allocations): %w", v.Name, q.Name(), err)
		}
		maps.Copy(res.Queries[i].Counters, allocs)
	}
	res.StatsAfter = e.Stats()
	for _, k := range []string{"flushes", "compactions", "read_compactions"} {
		if res.StatsBefore[k] != res.StatsAfter[k] {
			return nil, fmt.Errorf("runner: %s did work while being read (%s went from %d to %d): the shape of its tables changed under the reads",
				v.Name, k, res.StatsBefore[k], res.StatsAfter[k])
		}
	}
	if err := writeJSON(filepath.Join(dir, ResultsFile), res); err != nil {
		return nil, err
	}
	return res, nil
}

// openReadOnly opens the database a build left under dir as a read opens it:
// read-only, with the plan's block cache, nothing compacting.
func openReadOnly(plan *Plan, v candidates.Variant, dir string, rec *Capture) (measurable, error) {
	opened, err := v.Open(filepath.Join(dir, DBDir), candidates.Options{
		CacheBytes: plan.CacheBytes, Recorder: rec, DisableAutoCompactions: true, DisableReadCompactions: true, ReadOnly: true,
	})
	if err != nil {
		return nil, err
	}
	e, ok := opened.(measurable)
	if !ok {
		_ = opened.Close()
		return nil, fmt.Errorf("runner: %s cannot report what a measurement needs", v.Name)
	}
	return e, nil
}

// sameAsBuilt returns the first way a database opened read-only, which desc
// describes, is not the one the manifest records: a setting that makes the
// variant what it is, its tables, or the sequence number it opens at. The error
// reads after the candidate's name.
func sameAsBuilt(e measurable, desc map[string]string, m *Manifest, plan *Plan) error {
	for _, k := range stable {
		if desc[k] != m.Describe[k] {
			return fmt.Errorf("is not what it was built as: %s is %q, was %q: %w", k, desc[k], m.Describe[k], errMismatch)
		}
	}
	// A read-only database does nothing when it is opened, so what it holds must be
	// what the build left: the same tables, in the same levels.
	have := e.Stats()
	keys := slices.Sorted(maps.Keys(m.StatsCompacted))
	for _, k := range keys {
		if want := m.StatsCompacted[k]; (k == "live_table_bytes" || strings.HasPrefix(k, "tables_l")) && have[k] != want {
			return fmt.Errorf("is not what the build left: %s is %d, was %d: %w", k, have[k], want, errMismatch)
		}
	}
	if got := e.LastSeq(); got != plan.Stream.LastSeq {
		return fmt.Errorf("opens at seq %d, the plan at %d: %w", got, plan.Stream.LastSeq, errMismatch)
	}
	return nil
}

// sameCounters compares the counters that count what a read did, leaving out
// the ones that depend on the cache or on time.
func sameCounters(a, b map[string]int64) bool {
	keys := func(m map[string]int64) []string {
		var out []string
		for k := range m {
			if !volatile(k) {
				out = append(out, k)
			}
		}
		slices.Sort(out)
		return out
	}
	ka, kb := keys(a), keys(b)
	if !slices.Equal(ka, kb) {
		return false
	}
	for _, k := range ka {
		if a[k] != b[k] {
			return false
		}
	}
	return true
}
