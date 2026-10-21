//go:build race

package activity_test

// raceEnabled says the tests are built with the race detector, under which the
// properties draw fewer cases (see TestMain).
const raceEnabled = true
