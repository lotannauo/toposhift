package storetest_test

import (
	"flag"
	"os"
	"testing"

	"github.com/lotannauo/toposhift/internal/store/storetest"
)

// TestMain sets how many cases rapid runs for each property. The environment
// variable TOPOSHIFT_RAPID_CHECKS raises it, and a -rapid.checks given on the
// command line still wins, because it is parsed after this.
func TestMain(m *testing.M) {
	if err := flag.Set("rapid.checks", storetest.RapidChecks("30")); err != nil {
		panic(err)
	}
	if storetest.RaceEnabled {
		// The suites run their trimmed tier (one workload, no random workloads, no
		// reopening) under the race detector: it is one goroutine almost throughout,
		// where the detector finds nothing a plain build does not and costs minutes. The
		// checks with goroutines stay in it, at the reduced size of that tier (the
		// concurrent-read check writes 200 batches instead of 600). Plain builds run
		// the full tier.
		if err := flag.Set("test.short", "true"); err != nil {
			panic(err)
		}
	}
	os.Exit(m.Run())
}
