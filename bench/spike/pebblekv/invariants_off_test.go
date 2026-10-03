//go:build !invariants && !race

package pebblekv_test

// invariantsOn reports whether Pebble's own invariant checks are compiled in
// (it turns them on with the build tags invariants or race).
const invariantsOn = false
