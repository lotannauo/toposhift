package doltengine

import (
	"flag"
	"os"
	"testing"

	"github.com/lotannauo/toposhift/bench/spike/conformance"
)

// TestMain sets how many random workloads rapid runs, as the spike's own candidates do: a
// few by default, TOPOSHIFT_RAPID_CHECKS for more, and a -rapid.checks on the command line
// wins.
func TestMain(m *testing.M) {
	if err := flag.Set("rapid.checks", conformance.RapidChecks("4")); err != nil {
		panic(err)
	}
	os.Exit(m.Run())
}
