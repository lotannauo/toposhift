//go:build race

package sequencer_test

// dstRaceOn says the tests run with the race detector, which takes the first eight
// seeds only.
const dstRaceOn = true
