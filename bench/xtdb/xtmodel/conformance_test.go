package xtmodel_test

import (
	"flag"
	"os"
	"testing"

	"github.com/lotannauo/toposhift/bench/spike/conformance"
	"github.com/lotannauo/toposhift/bench/spike/engine"
	"github.com/lotannauo/toposhift/bench/xtdb/xtmodel"
)

// TestMain sets how many random workloads rapid runs, as the spike's own candidates
// do: a few by default, TOPOSHIFT_RAPID_CHECKS for more, and a -rapid.checks on the
// command line wins.
func TestMain(m *testing.M) {
	if err := flag.Set("rapid.checks", conformance.RapidChecks("4")); err != nil {
		panic(err)
	}
	os.Exit(m.Run())
}

// factory opens the model a directory names, so that an engine closed and opened
// again over the same directory finds what it left.
func factory(t *testing.T) conformance.Factory {
	return func(dir string) (engine.Engine, error) {
		t.Cleanup(func() { xtmodel.Forget(dir) })
		return xtmodel.Open(dir), nil
	}
}

// TestConformance is the engine contract: the model is checked against the reference
// engine on random and fixed workloads with mid-stream retentions, on the scripted
// checks, on reopens (state rebuilt from what the directory holds) and on concurrent
// reads, with no database server.
func TestConformance(t *testing.T) {
	conformance.Run(t, factory(t))
}
