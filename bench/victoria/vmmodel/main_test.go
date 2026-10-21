package vmmodel_test

import (
	"flag"
	"os"
	"testing"

	"github.com/lotannauo/toposhift/bench/spike/conformance"
)

func TestMain(m *testing.M) {
	if err := flag.Set("rapid.checks", conformance.RapidChecks("25")); err != nil {
		panic(err)
	}
	os.Exit(m.Run())
}
