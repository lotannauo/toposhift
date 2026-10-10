package workload

import (
	"strings"
	"testing"

	"github.com/lotannauo/toposhift/internal/lifecycle"
)

// A producer the padding does not know is a bug in this package, and says so; with no
// padding every producer is its own name.
func TestAProducerWithNoPaddedNameIsABug(t *testing.T) {
	t.Parallel()

	cfg := Tiny()
	plain, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if got := plain.producer("unknown"); got != "unknown" {
		t.Errorf("with no padding an unknown producer is %q", got)
	}
	cfg.ProducerLen = 18
	g, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	for _, p := range []lifecycle.Producer{ProducerFabric, ProducerK8sObjects, ProducerNodeCollector, ProducerTraces, ProducerKubelet, ProducerPodHeartbeat} {
		if got := g.producer(p); len(got) != 18 {
			t.Errorf("%q is padded to %q", p, got)
		}
	}
	defer func() {
		if r := recover(); r == nil || !strings.Contains(r.(string), `producer "unknown" has no padded name`) {
			t.Errorf("recovered %v, want the panic for an unknown producer", r)
		}
	}()
	g.producer("unknown")
}
