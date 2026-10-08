package storetest

import "os"

// RapidChecks is how many cases a package's TestMain should ask rapid for: the
// environment variable TOPOSHIFT_RAPID_CHECKS if it is set, otherwise def. A
// -rapid.checks on the command line still wins, because it is parsed after
// TestMain.
func RapidChecks(def string) string {
	if v := os.Getenv("TOPOSHIFT_RAPID_CHECKS"); v != "" {
		return v
	}
	return def
}
