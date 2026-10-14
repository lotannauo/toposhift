//go:build race

package extract_test

// raceEnabled says the tests are built with the race detector, under which the
// properties draw fewer cases (see main_test.go).
const raceEnabled = true
