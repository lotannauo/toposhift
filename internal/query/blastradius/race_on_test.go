//go:build race

package blastradius_test

// raceEnabled says the tests are built with the race detector, under which the
// properties draw fewer cases and the streams are checked at fewer windows (see
// main_test.go and stream_test.go).
const raceEnabled = true
