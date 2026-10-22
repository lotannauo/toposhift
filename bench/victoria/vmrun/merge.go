package vmrun

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/lotannauo/toposhift/bench/victoria/vmengine"
)

// ErrNotMerged marks a merge that is not valid.
var ErrNotMerged = errors.New("vmrun: the merge is INVALID")

// DefaultSettle is how often the server's counters are read while waiting for it to be
// idle, and DefaultStable how many equal reads in a row say it is.
const (
	DefaultSettle = time.Second
	DefaultStable = 3
)

// MergeOptions say how long to look at the server between reads.
type MergeOptions struct {
	// Settle and Stable are [DefaultSettle] and [DefaultStable] if zero.
	Settle time.Duration
	Stable int
	Env    EnvOptions
}

// MergeReport is the state of the server after its data was merged.
type MergeReport struct {
	Format int
	Env    Env
	// Load is the load it follows: its counts and the server's counters before it.
	Rows, Series uint64
	// After are the server's counters when it was idle after the forced merge. Stored is
	// the rows held in parts, which must be the rows the load wrote.
	After vmengine.Counters
	// Stored is the rows the merge left in storage that the load wrote.
	Stored  uint64
	Invalid string `json:",omitempty"`
}

// Merge brings the loaded data to its merged state, by the protocol the baseline is
// measured under: flush; wait until the server is idle (no row waits to be put in a part
// and none is in memory); force a merge, which covers only the parts there are then; wait
// until it is idle again. The merge is INVALID unless the server then holds exactly the
// rows the load wrote.
func Merge(ctx context.Context, c *vmengine.Client, load *LoadReport, o MergeOptions) (*MergeReport, error) {
	rep := &MergeReport{Format: ReportFormat}
	fail := func(err error) (*MergeReport, error) {
		rep.Invalid = err.Error()
		return rep, err
	}
	if load.Invalid != "" {
		return fail(fmt.Errorf("%w: the load it follows is INVALID: %s", ErrNotMerged, load.Invalid))
	}
	env, err := CollectEnv(ctx, c, o.Env)
	rep.Env = env
	if err != nil {
		return fail(err)
	}
	if o.Settle == 0 {
		o.Settle = DefaultSettle
	}
	if o.Stable == 0 {
		o.Stable = DefaultStable
	}
	rep.Rows, rep.Series = load.Counts.Samples+load.Counts.Markers, uint64(load.Counts.Series)
	done, err := c.Compact(ctx, o.Settle, o.Stable)
	if err != nil {
		return fail(err)
	}
	rep.After = done
	rep.Stored = done.Stored - load.Before.Stored
	if rep.Stored != rep.Rows || done.PendingRows != 0 || done.InMemoryParts != 0 || done.Merging != 0 {
		return fail(fmt.Errorf("%w: the load wrote %d rows and the idle server holds %d more than before (pending %d, in memory %d parts, merging %d)",
			ErrNotMerged, rep.Rows, rep.Stored, done.PendingRows, done.InMemoryParts, done.Merging))
	}
	return rep, nil
}

// SizesNote says what the bytes are, in every output that carries them.
const SizesNote = "bytes of a merged data directory are not byte-stable: a figure only from CI hardware, never from a laptop"

// SizesReport is the size of what the server holds.
type SizesReport struct {
	Format int
	Env    Env
	// Rows and Series are the rows the load wrote (samples and markers) and the series.
	Rows, Series uint64
	// DataBytes is the bytes of the parts the server counts, and Dir what the data
	// directory holds on disk: the sum of the sizes of its files, in total and under each
	// of its top-level entries.
	DataBytes uint64
	Dir       DirSize
	// PerRow and PerSeries are the directory's bytes over the rows and over the series.
	PerRow, PerSeries float64
	Note              string
}

// DirSize is the bytes of a directory.
type DirSize struct {
	Total int64
	// ByEntry is the bytes under each top-level entry of the directory, by name.
	ByEntry map[string]int64
	Files   int
}

// WalkSize sums the sizes of the regular files under a directory, in total and under each
// of its top-level entries.
func WalkSize(dir string) (DirSize, error) {
	out := DirSize{ByEntry: map[string]int64{}}
	err := filepath.WalkDir(dir, func(p string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if !d.Type().IsRegular() {
			return nil
		}
		info, err := d.Info()
		if err != nil {
			return err
		}
		rel, err := filepath.Rel(dir, p)
		if err != nil {
			return err
		}
		top, _, _ := strings.Cut(rel, string(filepath.Separator))
		out.ByEntry[top] += info.Size()
		out.Total += info.Size()
		out.Files++
		return nil
	})
	return out, err
}

// Entries returns the top-level entries of the directory in name order.
func (d DirSize) Entries() []string {
	names := make([]string, 0, len(d.ByEntry))
	for n := range d.ByEntry {
		names = append(names, n)
	}
	sort.Strings(names)
	return names
}

// Sizes reads the size of the loaded and merged data: the server's own count of the bytes
// of its parts, and the data directory it keeps them in. The merge must be valid.
func Sizes(ctx context.Context, c *vmengine.Client, load *LoadReport, merge *MergeReport, dataDir string, o MergeOptions) (*SizesReport, error) {
	if load.Invalid != "" || merge.Invalid != "" {
		return nil, errors.New("vmrun: the load or the merge it follows is INVALID: no size is read")
	}
	env, err := CollectEnv(ctx, c, o.Env)
	if err != nil {
		return nil, err
	}
	if o.Settle == 0 {
		o.Settle = DefaultSettle
	}
	if o.Stable == 0 {
		o.Stable = DefaultStable
	}
	// What is on disk is what the idle server holds.
	idle, err := c.Settle(ctx, o.Settle, o.Stable)
	if err != nil {
		return nil, err
	}
	dir, err := WalkSize(dataDir)
	if err != nil {
		return nil, err
	}
	rep := &SizesReport{
		Format: ReportFormat, Env: env, Rows: merge.Rows, Series: merge.Series,
		DataBytes: idle.DataBytes, Dir: dir, Note: SizesNote,
	}
	if rep.Rows > 0 {
		rep.PerRow = float64(dir.Total) / float64(rep.Rows)
	}
	if rep.Series > 0 {
		rep.PerSeries = float64(dir.Total) / float64(rep.Series)
	}
	return rep, nil
}
