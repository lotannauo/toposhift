package storetest_test

import (
	"testing"

	"github.com/lotannauo/toposhift/internal/lifecycle"
	"github.com/lotannauo/toposhift/internal/store"
	"github.com/lotannauo/toposhift/internal/store/memstore"
	"github.com/lotannauo/toposhift/internal/store/storetest"
)

// A backend runs the suite from a test of its own, with a factory that opens its
// store in the directory the suite gives it, folding entity existence with the
// policy the suite gives it. This one opens the reference store, which keeps
// nothing in the directory and so is not durable.
func Example() {
	var t *testing.T // the test of the backend

	storetest.Run(t, storetest.Factory{
		Open: func(dir string, policy lifecycle.Policy) (store.Store, error) {
			return memstore.Open(memstore.Options{Policy: policy})
		},
		// Durable: true, for a backend that comes back from being closed.
	})
}
