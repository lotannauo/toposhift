//go:build !race

package sequencer_test

// midPassPrefixes is how many prefixes P2 writes so that the first chunk of a
// pass cannot finish them.
const midPassPrefixes = 80_000
