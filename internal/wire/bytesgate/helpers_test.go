package bytesgate_test

import (
	"context"
	"testing"
	"time"

	"github.com/lotannauo/toposhift/internal/wire"
	"github.com/lotannauo/toposhift/internal/wire/bytesgate"
)

var ctx = context.Background()

// small is a scale with the shape of the reference one and a few dozen entities
// in each fixture, for the tests that run on every push.
func small() bytesgate.Scale {
	return bytesgate.Scale{Seed: 7, Pods: 12, Hosts: 3, ReplaceEvery: 90 * time.Second, SmallScope: 60, LargeScope: 120}
}

// build builds every fixture of a scale.
func build(t testing.TB, s bytesgate.Scale) map[string]wire.Document {
	t.Helper()
	out := map[string]wire.Document{}
	for _, f := range bytesgate.Fixtures(s) {
		d, err := f.Build(ctx)
		if err != nil {
			t.Fatalf("fixture %s: %v", f.Name, err)
		}
		out[f.Name] = d
	}
	return out
}
