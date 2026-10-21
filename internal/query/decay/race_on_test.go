//go:build race

package decay_test

// raceEnabled says the tests are built with the race detector, under which the
// sweeps draw fewer worlds.
const raceEnabled = true
