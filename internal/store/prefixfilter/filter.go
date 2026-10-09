package prefixfilter

import (
	"errors"
	"fmt"
	"iter"
	"math"
	"slices"
)

const (
	// initialSeed is the first seed Build tries; the next comes from splitmix64.
	initialSeed = 0x9e3779b97f4a7c15

	// maxSeeds is how many seeds Build tries before it gives up. Distinct hashes
	// need one or two in practice, so reaching it means a defect.
	maxSeeds = 1000
)

// Filter is a static set of byte strings that answers membership approximately:
// Contains is true for every key the filter was built from, and for about 1 in
// 256 other keys. It is immutable and safe for concurrent reads.
type Filter struct {
	seed         uint64
	blockLength  uint32
	fingerprints []uint8
	n            int
}

// Build makes a filter of the keys keys yields. Keys may come in any order and
// may repeat; equal keys count once. The same keys always give the same filter
// (the construction is deterministic). An empty input gives a filter that
// contains nothing.
//
// Build hashes each key as it arrives and keeps no reference to it. It returns
// an error if there are too many keys for the array to be addressed, or if
// construction fails with every one of 1,000 seeds, which does not happen for
// distinct hashes.
func Build(keys iter.Seq[[]byte]) (*Filter, error) {
	var hashes []uint64
	for key := range keys {
		hashes = append(hashes, keyHash(key))
	}
	slices.Sort(hashes)
	hashes = slices.Clip(slices.Compact(hashes))

	blockLength, err := blockLengthFor(len(hashes))
	if err != nil {
		return nil, err
	}
	f, _, err := construct(hashes, blockLength, maxSeeds)
	return f, err
}

// blockLengthFor is the size of each third of the array for n distinct keys:
// a third of 32 + ceil(1.23*n) cells.
func blockLengthFor(n int) (uint32, error) {
	// ceil(1.23*n) in integers, so no rounding depends on floating point.
	if n < 0 || n > (math.MaxInt-99)/123 {
		return 0, fmt.Errorf("prefixfilter: %d keys is too many to build a filter from", n)
	}
	capacity := 32 + (123*n+99)/100
	blockLength := capacity / 3
	if blockLength > math.MaxUint32/3 {
		return 0, fmt.Errorf("prefixfilter: %d keys is too many to build a filter from", n)
	}
	return uint32(blockLength), nil
}

// errNoSeed is returned when construction fails with every seed it was allowed.
var errNoSeed = errors.New("prefixfilter: construction failed with every seed")

// construct builds a filter over hashes, which must be sorted and free of
// duplicates, with arrays of 3*blockLength cells. It tries up to attempts seeds
// and returns how many it used.
func construct(hashes []uint64, blockLength uint32, attempts int) (*Filter, int, error) {
	cells := 3 * int(blockLength)
	counts := make([]uint32, cells)
	xors := make([]uint64, cells)
	queue := make([]uint32, 0, cells)
	stackKeys := make([]uint64, 0, len(hashes))
	stackCells := make([]uint32, 0, len(hashes))

	seed := uint64(initialSeed)
	for attempt := 1; attempt <= attempts; attempt++ {
		clear(counts)
		clear(xors)
		for _, h := range hashes {
			x := seeded(h, seed)
			h0, h1, h2 := positions(x, blockLength)
			counts[h0]++
			counts[h1]++
			counts[h2]++
			xors[h0] ^= x
			xors[h1] ^= x
			xors[h2] ^= x
		}

		queue = queue[:0]
		for c, count := range counts {
			if count == 1 {
				queue = append(queue, uint32(c))
			}
		}

		// Peel: a cell with one key left names it. Set the key aside with that cell
		// and take it out of its three cells.
		stackKeys = stackKeys[:0]
		stackCells = stackCells[:0]
		for head := 0; head < len(queue); head++ {
			c := queue[head]
			if counts[c] != 1 {
				continue // its key was taken out through another cell
			}
			x := xors[c]
			stackKeys = append(stackKeys, x)
			stackCells = append(stackCells, c)
			h0, h1, h2 := positions(x, blockLength)
			for _, cell := range [3]uint32{h0, h1, h2} {
				counts[cell]--
				xors[cell] ^= x
				if counts[cell] == 1 {
					queue = append(queue, cell)
				}
			}
		}

		if len(stackKeys) != len(hashes) {
			seed = splitmix64(seed)
			continue
		}

		// Assign in reverse order of peeling. A key's cell was alone when the key was
		// peeled, so none of the keys peeled before it uses either of its other two
		// cells as its own: those cells are final by the time this key's cell is
		// written, being either the cell of a key peeled later (already written) or
		// the cell of no key (zero for good). Each cell is written once.
		fingerprints := make([]uint8, cells)
		for i := len(stackKeys) - 1; i >= 0; i-- {
			x, c := stackKeys[i], stackCells[i]
			h0, h1, h2 := positions(x, blockLength)
			v := fingerprint(x)
			for _, cell := range [3]uint32{h0, h1, h2} {
				if cell != c {
					v ^= fingerprints[cell]
				}
			}
			fingerprints[c] = v
		}
		return &Filter{
			seed:         seed,
			blockLength:  blockLength,
			fingerprints: fingerprints,
			n:            len(hashes),
		}, attempt, nil
	}
	return nil, attempts, errNoSeed
}

// Contains reports whether key may be in the set: true for every key built in.
func (f *Filter) Contains(key []byte) bool {
	if f.n == 0 {
		return false
	}
	x := seeded(keyHash(key), f.seed)
	h0, h1, h2 := positions(x, f.blockLength)
	return fingerprint(x) == f.fingerprints[h0]^f.fingerprints[h1]^f.fingerprints[h2]
}

// Len is the number of distinct keys built in: distinct 64-bit hashes, so two
// keys that share a hash count once.
func (f *Filter) Len() int {
	return f.n
}

// SizeBytes is the memory the fingerprints take.
func (f *Filter) SizeBytes() int {
	return len(f.fingerprints)
}
