//go:build race

package pebblestore

// raceEnabled says the tests are built with the race detector, which makes the
// single-goroutine random histories several times slower and finds nothing in them.
const raceEnabled = true

// RaceEnabled is raceEnabled for the tests of package pebblestore_test.
const RaceEnabled = raceEnabled
