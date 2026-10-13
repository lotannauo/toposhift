package pebblestore

import (
	"errors"
	"fmt"
	"slices"
	"testing"

	"github.com/cockroachdb/pebble/v2/vfs"

	"github.com/lotannauo/toposhift/internal/store"
)

// The tests of Options.RetainFlushDels, the threshold a measurement sets.

// Zero is the default, a positive value is the threshold, the test seam wins over it,
// and a negative value, which would turn the flushes off, is refused at Open.
func TestTheFlushThresholdOption(t *testing.T) {
	t.Parallel()
	if got := DefaultRetainFlushDels; got != 32768 {
		t.Errorf("the exported default is %d", got)
	}
	for _, tc := range []struct {
		option, seam int
		want         int64
	}{{0, 0, defaultRetainFlushDels}, {24576, 0, 24576}, {16384, 100, 100}, {16384, -1, 0}} {
		o := chunkOptions(vfs.NewMem(), nil)
		o.RetainFlushDels, o.retainFlushDels = tc.option, tc.seam
		s := mustOpen(t, o)
		if s.retainFlushDels != tc.want {
			t.Errorf("RetainFlushDels %d with the seam at %d opens with %d, want %d", tc.option, tc.seam, s.retainFlushDels, tc.want)
		}
	}
	o := chunkOptions(vfs.NewMem(), nil)
	o.RetainFlushDels = -1
	if s, err := Open("db", o); !errors.Is(err, store.ErrInvalid) {
		if err == nil {
			_ = s.Close()
		}
		t.Errorf("a negative threshold opened: %v", err)
	}
}

// A pass flushes at the threshold the option gives: the range deletions a chunk
// starts with stay under twice it, and it starts flushes in proportion to the
// deletions; at the default, the same stream, whose deletions are far fewer than the
// default, starts none.
func TestAPassFlushesAtTheThresholdTheOptionGives(t *testing.T) {
	t.Parallel()
	const threshold = 64
	for _, mode := range []RetentionMode{Synchronous, Background} {
		t.Run(fmt.Sprintf("mode %d", mode), func(t *testing.T) {
			t.Parallel()
			rec := newProbeRecorder()
			o := boundOptions(vfs.NewMem(), mode, 0, nil, rec)
			o.RetainFlushDels = threshold
			s := openBound(t, o)
			retainBound(t, s)
			dels, flushes := rec.count("retain.range_deletes"), rec.count("retain.flushes")
			if dels < 16*threshold {
				t.Fatalf("the pass committed %d range deletions, too few to see a threshold of %d", dels, threshold)
			}
			if want := dels / (4 * threshold); flushes < want {
				t.Errorf("%d flushes for %d range deletions at a threshold of %d, want at least %d", flushes, dels, threshold, want)
			}
			if m := slices.Max(rec.sampled("retain.unflushed_range_deletes")); m >= 2*threshold {
				t.Errorf("a chunk started with %d unflushed range deletions", m)
			}

			rec = newProbeRecorder()
			s = openBound(t, boundOptions(vfs.NewMem(), mode, 0, nil, rec))
			retainBound(t, s)
			if dels, n := rec.count("retain.range_deletes"), rec.count("retain.flushes"); dels >= defaultRetainFlushDels || n != 0 {
				t.Errorf("at the default threshold, %d flushes for %d range deletions, want none", n, dels)
			}
		})
	}
}
