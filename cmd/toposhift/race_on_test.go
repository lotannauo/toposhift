//go:build race

package main

// raceEnabled says the tests are built with the race detector, under which a
// deadline on the command's work is longer (see slow).
const raceEnabled = true
