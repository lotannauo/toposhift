//go:build race

package sequencer_test

// midPassPrefixes is how many prefixes P2 writes so that the first chunk of a
// pass cannot finish them: under the race detector a chunk visits several times
// fewer.
const midPassPrefixes = 5_000
