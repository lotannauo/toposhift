package extract_test

import (
	"flag"
	"os"
	"testing"

	"github.com/lotannauo/toposhift/internal/store/storetest"
)

// TestMain sets how many cases rapid runs for each property: 100, or 30 under
// the race detector, where a case costs several times more. The environment
// variable TOPOSHIFT_RAPID_CHECKS changes it, and a -rapid.checks given on the
// command line still wins, because it is parsed after this.
func TestMain(m *testing.M) {
	def := "100"
	if raceEnabled {
		def = "30"
	}
	if err := flag.Set("rapid.checks", storetest.RapidChecks(def)); err != nil {
		panic(err)
	}
	os.Exit(m.Run())
}
