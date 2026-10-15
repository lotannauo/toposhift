package svcgraph_test

import (
	"flag"
	"os"
	"testing"

	"github.com/lotannauo/toposhift/internal/store/storetest"
)

func TestMain(m *testing.M) {
	if err := flag.Set("rapid.checks", storetest.RapidChecks("100")); err != nil {
		panic(err)
	}
	os.Exit(m.Run())
}
