//go:build race

package main

// raceTag says the tests are built with the race detector: the deep tier, the
// only race run that reaches the end-to-end tests (the trimmed race tier skips
// them), where the windows family is built with every candidate type.
const raceTag = true
