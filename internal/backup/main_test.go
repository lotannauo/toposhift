package backup_test

import (
	"flag"
	"os"
	"testing"
)

// TestMain runs the trimmed tier under the race detector, as the store suites do:
// the tests here are one goroutine almost throughout, where the detector finds
// nothing a plain build does not, and the equal-answers test would cost minutes.
func TestMain(m *testing.M) {
	flag.Parse()
	if raceEnabled {
		if err := flag.Set("test.short", "true"); err != nil {
			panic(err)
		}
	}
	os.Exit(m.Run())
}
