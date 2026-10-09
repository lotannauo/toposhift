package pebblestore_test

import (
	"testing"
	"time"

	"github.com/cockroachdb/pebble/v2/vfs"

	"github.com/lotannauo/toposhift/internal/lifecycle"
	"github.com/lotannauo/toposhift/internal/store"
	"github.com/lotannauo/toposhift/internal/store/pebblestore"
	"github.com/lotannauo/toposhift/internal/store/storetest"
)

// layered is the factory of a store with a retention offset for each layer and
// kept layers, on an in-memory file system (one per directory, so a store closed and
// opened again finds what it left) or on real files.
func layered(settle, files bool, ck *pebblestore.CheckpointOptions) storetest.LayeredFactory {
	return storetest.LayeredFactory{
		Open: func(dir string, p lifecycle.Policy, offsets [4]time.Duration, keep [4]bool) (store.Store, error) {
			o := options(p, settle, ck)
			if !files {
				fs, _ := memFS.LoadOrStore(dir, vfs.NewMem())
				o.FS = fs.(vfs.FS)
			}
			o.Offsets, o.Keep = offsets, keep
			return pebblestore.Open(dir, o)
		},
		Durable: true,
	}
}

// Retention offsets and kept layers: the store agrees with the contract on the
// horizon of each layer, on what each layer refuses and answers, and on the Horizon
// after Retains that move some layers and not others, across reopenings with other
// offsets, with a retention that settles and one that does not, with a checkpoint
// policy that writes many checkpoints and one that writes none, on real files and in
// memory.
func TestLayerOffsetsConform(t *testing.T) {
	t.Parallel()
	matrix := map[string]storetest.LayeredFactory{
		"default/in memory":  layered(false, false, policyDefault),
		"k8/in memory":       layered(false, false, policyK8),
		"k8/files":           layered(false, true, policyK8),
		"k8/settling/memory": layered(true, false, policyK8),
		"no checkpoints":     layered(false, false, &pebblestore.CheckpointOptions{}),
	}
	if pebblestore.RaceEnabled {
		// One goroutine, so the race detector finds nothing here that a plain build
		// does not; it keeps the in-memory store with the default policy.
		for name := range matrix {
			if name != "default/in memory" {
				delete(matrix, name)
			}
		}
	}
	for name, f := range matrix {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			storetest.RunLayers(t, f)
		})
	}
}
