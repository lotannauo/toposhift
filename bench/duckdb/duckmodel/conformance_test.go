package duckmodel

import (
	"flag"
	"os"
	"testing"

	"github.com/lotannauo/toposhift/bench/spike/conformance"
	"github.com/lotannauo/toposhift/bench/spike/engine"
)

// TestMain sets how many random workloads rapid runs, as the spike's own candidates do:
// a few by default, TOPOSHIFT_RAPID_CHECKS for more, and a -rapid.checks on the command
// line wins.
func TestMain(m *testing.M) {
	if err := flag.Set("rapid.checks", conformance.RapidChecks("4")); err != nil {
		panic(err)
	}
	os.Exit(m.Run())
}

var arms = []Arm{ArmRecords, ArmIntervals}

// factory opens the model a directory names, so that an engine closed and opened again
// over the same directory finds what it left.
func factory(t *testing.T, arm Arm) conformance.Factory {
	return func(dir string) (engine.Engine, error) {
		t.Cleanup(func() { Forget(dir, arm) })
		return Open(dir, arm), nil
	}
}

// TestConformance is the engine contract: each arm's model is checked against the
// reference engine on random and fixed workloads with mid-stream retentions, on the
// scripted checks, on a reopen and on concurrent reads, with no file and no database.
func TestConformance(t *testing.T) {
	for _, arm := range arms {
		t.Run(arm.String(), func(t *testing.T) {
			conformance.Run(t, factory(t, arm))
		})
	}
}
