package pebblekv

import (
	"testing"

	"github.com/cockroachdb/pebble/v2"
)

// A table awaits its statistics until they count its entries, and only then.
func TestATableAwaitsItsStatisticsUntilTheyCountItsEntries(t *testing.T) {
	t.Parallel()
	table := func(entries uint64) pebble.SSTableInfo {
		var info pebble.SSTableInfo
		info.TableStats.NumEntries = entries
		return info
	}
	for _, c := range []struct {
		name   string
		levels [][]pebble.SSTableInfo
		want   int
	}{
		{"no table", nil, 0},
		{"loaded", [][]pebble.SSTableInfo{{table(5)}}, 0},
		{"not loaded", [][]pebble.SSTableInfo{{table(0)}}, 1},
		{"one of two levels", [][]pebble.SSTableInfo{{table(5)}, nil, nil, nil, nil, nil, {table(1), table(0)}}, 1},
		{"every level", [][]pebble.SSTableInfo{{table(0)}, {table(0), table(0)}}, 3},
	} {
		if got := awaitingStats(c.levels); got != c.want {
			t.Errorf("%s: %d tables await their statistics, want %d", c.name, got, c.want)
		}
	}
}
