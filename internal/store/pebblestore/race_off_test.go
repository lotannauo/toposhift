//go:build !race

package pebblestore

// raceEnabled says the tests are built with the race detector.
const raceEnabled = false

// RaceEnabled is raceEnabled for the tests of package pebblestore_test.
const RaceEnabled = raceEnabled
