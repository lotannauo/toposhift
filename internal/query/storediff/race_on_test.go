//go:build race

package storediff_test

// raceEnabled says the tests are built with the race detector, under which the
// large scenarios are smaller.
const raceEnabled = true
