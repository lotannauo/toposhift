package workload_test

import (
	"encoding/hex"
	"testing"
	"time"

	"github.com/lotannauo/toposhift/bench/spike/conformance"
	"github.com/lotannauo/toposhift/bench/spike/workload"
)

// The streams the conformance test and the earlier tests were built on must stay
// exactly what they were: a new option may add to the generator only if a config
// that does not set it draws the same random numbers in the same order and gets
// the same records. These are the digests (see digest) of those streams before
// the options for identities, pod heartbeats, node capacity and backlog lateness
// existed. A change here is a change to every result computed from the old
// streams, and has to be made on purpose.
func TestExistingStreamsAreUnchanged(t *testing.T) {
	t.Parallel()
	want := map[string]string{
		"conformance config 0": "a83edbdb2921650c5bb8f5d324cee045419a93bc58948318a05ef450dc1aaa41",
		"conformance config 1": "d3d120806f9dbd481ca7be48314193b3fb7d3280e3b9b60a0a36e9ec0d6051c7",
		"conformance config 2": "f8e49ff5cf2a4eff1f0882c9afe56626e08749a2a86e0ec50aa7736ad91fa0e2",
		"conformance config 3": "c709d93e3d3f6406d00886f8859f3980846132e5b33e0a7ed8bf14f4a016eaf9",
		"conformance config 4": "650eae0fdb810e64f6a66b5f0bb41d9af849e5d455e919492868f143ef7e1f56",
		"conformance config 5": "fbb60d82e74ffea9621ac472203691d5593c36adbd161ee34dfa52306173f774",
		"small, coalesced":     "514d41be3dba3a5af3a4e0e286122c7162335d6919b72e7a96a9e504a61e68f3",
	}
	cfgs := map[string]workload.Config{}
	// The first six: the configs added after them use the new options and have no
	// earlier stream to keep.
	for i, cfg := range conformance.Configs()[:6] {
		cfgs["conformance config "+string(rune('0'+i))] = cfg
	}
	small := workload.Small()
	small.Duration = 10 * time.Minute
	small.Pods = 200
	small.CoalesceRuns, small.ExtendEvery = true, 2*time.Minute
	small.ConfirmProbability, small.ConfirmTTL = 0.3, 3*time.Minute
	cfgs["small, coalesced"] = small

	if len(cfgs) != len(want) {
		t.Fatalf("%d configs, %d digests", len(cfgs), len(want))
	}
	for name, cfg := range cfgs {
		got := digest(generate(t, cfg))
		if hex.EncodeToString(got[:]) != want[name] {
			t.Errorf("%s: the stream is %x, it was %s", name, got, want[name])
		}
	}
}
