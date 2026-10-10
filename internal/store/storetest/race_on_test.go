//go:build race

package storetest

// RaceEnabled says the tests are built with the race detector, under which the
// suites run their trimmed tier (see main_test.go).
const RaceEnabled = true
