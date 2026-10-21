package activity_test

import (
	"flag"
	"os"
	"testing"

	"github.com/lotannauo/toposhift/internal/store/storetest"
)

// TestMain sets how many cases rapid draws for each property: rapid's own 100,
// or 30 under the race detector, where the Parquet encoders and decoders the
// properties drive cost about fifty times what they do without it (the two
// properties were half of this package's time in CI's race shard). The other
// tiers draw the full 100. The environment variable TOPOSHIFT_RAPID_CHECKS changes
// it, and a -rapid.checks given on the command line still wins, because it is
// parsed after this.
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
