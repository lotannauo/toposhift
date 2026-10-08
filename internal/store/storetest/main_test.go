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
	os.Exit(m.Run())
}
