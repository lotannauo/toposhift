package segmenttest

import (
	"slices"
	"testing"

	"github.com/lotannauo/toposhift/internal/segment"
)

// TestHonestDoublePasses runs the suite as an implementation would, on a store
// that keeps the contract.
func TestHonestDoublePasses(t *testing.T) {
	Run(t, func(*testing.T) segment.Store { return newDouble(flawNone) }, WithMaxSize(doubleMax))
}

// TestBrokenDoublesAreCaught runs every check on each store that breaks one
// property and requires the check for that property, and no other, to fail.
func TestBrokenDoublesAreCaught(t *testing.T) {
	tests := []struct {
		name string
		flaw flaw
		// failing lists the checks that must fail, and nothing else may.
		failing []string
	}{
		{"Put overwrites", flawOverwrite, []string{"immutability", "concurrency", "digest"}},
		{"partial segment visible after a read error", flawPartialOnReadError, []string{"atomicity"}},
		{"List in insertion order", flawInsertionOrder, []string{"list"}},
		{"dot dot accepted", flawDotDot, []string{"names"}},
		{"cancelled context ignored by Put", flawIgnoreContextInPut, []string{"atomicity", "context"}},
		{"Delete of a missing name fails", flawDeleteMissingErr, []string{"not_found"}},
		{"concurrent Puts of one name both succeed", flawRacyPut, []string{"concurrency"}},
		{"bytes returned with EOF dropped", flawDropDataWithEOF, []string{"round_trip"}},
		{"announced size ignored", flawIgnoreSize, []string{"size"}},
		{"MaxSize not enforced", flawNoLimit, []string{"size"}},
		{"Stat reports a wrong digest", flawWrongSHAOnStat, []string{"size", "digest"}},
		{"Put reports a wrong digest", flawWrongSHAOnPut, []string{"size", "digest"}},
		{"the limit itself is refused", flawLimitOffByOne, []string{"size"}},
		{"upper-case names accepted", flawUpperCase, []string{"names"}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var failing []string
			for _, c := range checksFor(doubleMax) {
				if err := c.run(newDouble(tc.flaw)); err != nil {
					failing = append(failing, c.name)
					t.Logf("check %s fails as expected: %v", c.name, err)
				}
			}
			if !slices.Equal(failing, tc.failing) {
				t.Errorf("failing checks = %v, want %v", failing, tc.failing)
			}
		})
	}
}

// TestChecksAreRegistered keeps the list of checks to the ten the suite
// promises, each named once.
func TestChecksAreRegistered(t *testing.T) {
	want := []string{"round_trip", "immutability", "not_found", "names", "list", "atomicity", "context", "concurrency", "size", "digest"}
	var got []string
	for _, c := range checksFor(doubleMax) {
		got = append(got, c.name)
	}
	if !slices.Equal(got, want) {
		t.Errorf("checks = %v, want %v", got, want)
	}
}
