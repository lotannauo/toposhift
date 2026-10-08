package storetest_test

import (
	"testing"

	"github.com/lotannauo/toposhift/internal/lifecycle"
	"github.com/lotannauo/toposhift/internal/store"
	"github.com/lotannauo/toposhift/internal/store/memstore"
	"github.com/lotannauo/toposhift/internal/store/storetest"
)

// The reference satisfies its own suite. This only shows that the checks are
// consistent with one another: the suite compares a store with memstore, so
// memstore passing says nothing about memstore being right. That is what the
// reference's own tests, and the broken stores of this package, are for.
func TestMemstoreIsAConformingStore(t *testing.T) {
	t.Parallel()
	storetest.Run(t, storetest.Factory{
		Open: func(_ string, p lifecycle.Policy) (store.Store, error) {
			return memstore.Open(memstore.Options{Policy: p})
		},
	})
}
