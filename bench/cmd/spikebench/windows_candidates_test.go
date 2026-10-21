package main

import (
	"slices"
	"testing"
)

// The windows family is built with every candidate type in the deep tier and
// with the two of layout L on a pull request: the M candidate is what a pull
// request leaves to the deep run, and nothing else.
func TestWindowsCandidatesByTier(t *testing.T) {
	t.Parallel()
	deep := windowsCandidatesFor(true)
	pr := windowsCandidatesFor(false)
	if !slices.Equal(deep, []string{"L/off", "L/k64a4l1ns", "M/crdb1"}) {
		t.Errorf("deep tier candidates = %v", deep)
	}
	if !slices.Equal(pr, []string{"L/off", "L/k64a4l1ns"}) {
		t.Errorf("pull request candidates = %v", pr)
	}
	if !slices.Equal(deep[:len(pr)], pr) {
		t.Errorf("the pull request's candidates %v are not a prefix of the deep tier's %v", pr, deep)
	}
	if !slices.Contains(deep, "M/crdb1") || slices.Contains(pr, "M/crdb1") {
		t.Error("M/crdb1 must be in the deep tier's family and only there")
	}
}
