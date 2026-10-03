package pebblekv_test

import (
	"os"
	"testing"
)

// The bench tests are meant to run with Pebble's invariant checks on: they found
// real bugs, and the fast tier gets them from the invariants build tag instead of
// the race detector. A task that sets the variable below says so, and a build
// without the checks then fails here, instead of passing quietly with less
// checking (a plain `go test` without the tag is fine and is not asked to).
func TestInvariantChecksAreOnWhenAskedFor(t *testing.T) {
	if os.Getenv("TOPOSHIFT_REQUIRE_INVARIANTS") == "" {
		t.Skip("TOPOSHIFT_REQUIRE_INVARIANTS is not set")
	}
	if !invariantsOn {
		t.Fatal("Pebble's invariant checks are off: build the tests with -tags invariants (or -race)")
	}
}
