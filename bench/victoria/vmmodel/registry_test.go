package vmmodel_test

import (
	"errors"
	"testing"
	"time"

	"github.com/lotannauo/toposhift/bench/spike/engine"
	"github.com/lotannauo/toposhift/bench/spike/workload"
	"github.com/lotannauo/toposhift/bench/victoria/internal/fixture"
	"github.com/lotannauo/toposhift/bench/victoria/vmmodel"
	"github.com/lotannauo/toposhift/internal/catalog"
	"github.com/lotannauo/toposhift/internal/lifecycle"
)

// registeredCanonical is the registered table of windows, as text. It is frozen: a
// change to it is a change to a registration.
const registeredCanonical = `producer fabric window 0s
producer k8s-cluster window 1h0m0s
producer k8sobjects window 0s
producer kubelet window 5m0s
producer node-collector window 4m0s
producer traces window 20m0s
relation depends_on held by traces
relation instance_of held by k8sobjects
relation located_in held by fabric
relation part_of held by k8sobjects
relation runs_on held by node-collector
relation scheduled_on held by k8s-cluster,k8sobjects,kubelet
entity container held by k8sobjects
entity host held by fabric
entity k8s.node held by node-collector
entity k8s.pod held by k8sobjects
entity service held by k8sobjects
entity service.instance held by k8sobjects
entity topo.rack held by fabric
`

// The windows of the registered table are the producers' TTLs: 240 s for the node
// collector, 3,600 s for the pod collector, 1,200 s for the traces rollup and 300 s for
// the kubelet's confirmation. The fabric and the Kubernetes watch have none.
func TestTheRegisteredWindowsAreTheProducersTTLs(t *testing.T) {
	for name, cfg := range map[string]workload.Config{"scaled": fixture.Registered(time.Hour), "the modeled cluster": workload.Cluster(7)} {
		t.Run(name, func(t *testing.T) {
			reg, err := vmmodel.RegistryFor(cfg)
			if err != nil {
				t.Fatal(err)
			}
			want := map[lifecycle.Producer]time.Duration{
				workload.ProducerNodeCollector: 240 * time.Second,
				workload.ProducerPodHeartbeat:  3600 * time.Second,
				workload.ProducerTraces:        1200 * time.Second,
				workload.ProducerKubelet:       300 * time.Second,
				workload.ProducerFabric:        0,
				workload.ProducerK8sObjects:    0,
			}
			got := map[lifecycle.Producer]time.Duration{}
			for _, h := range reg.Producers() {
				got[h.Producer] = h.Window
			}
			if len(got) != len(want) {
				t.Fatalf("producers %v, want %v", got, want)
			}
			for p, w := range want {
				if got[p] != w {
					t.Errorf("window of %s is %s, want %s", p, got[p], w)
				}
			}
			if reg.Canonical() != registeredCanonical {
				t.Errorf("the table is\n%s\nwant\n%s", reg.Canonical(), registeredCanonical)
			}
		})
	}
}

// The registry derives its windows from the spec: a workload with other intervals gets
// other windows, and one with no heartbeat has the node collector in watch mode.
func TestWindowsFollowTheConfiguration(t *testing.T) {
	cfg := fixture.Registered(time.Hour)
	cfg.HeartbeatInterval = 30 * time.Second
	cfg.HeartbeatTTLFactor = 3
	cfg.PodHeartbeatInterval = 0
	cfg.ConfirmProbability = 0
	reg, err := vmmodel.RegistryFor(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if w, _ := reg.Window(workload.ProducerNodeCollector); w != 90*time.Second {
		t.Errorf("node collector window %s", w)
	}
	for _, h := range reg.EdgeHolders(catalog.ScheduledOn) {
		if h.Producer == workload.ProducerPodHeartbeat || h.Producer == workload.ProducerKubelet {
			t.Errorf("%s holds scheduled_on though it asserts nothing", h.Producer)
		}
	}
	cfg.HeartbeatInterval = 0
	reg, err = vmmodel.RegistryFor(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if w, ok := reg.Window(workload.ProducerNodeCollector); !ok || w != 0 {
		t.Errorf("node collector without a heartbeat has window %s, known %v", w, ok)
	}
}

func TestTheBandOfASubjectIsItsLongestWindow(t *testing.T) {
	reg, err := vmmodel.RegistryFor(fixture.Registered(time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	pod, node, svc := fixture.FP(catalog.K8sPod, 1), fixture.FP(catalog.K8sNode, 2), fixture.FP(catalog.Service, 3)
	placement := engine.EdgeSubject(pod, node, catalog.ScheduledOn)
	nodeS := engine.EntitySubject(node)
	cases := []struct {
		name string
		subs []engine.Subject
		want time.Duration
	}{
		{"a pod's placement", []engine.Subject{placement}, 3600*time.Second + time.Millisecond},
		{"a node", []engine.Subject{nodeS}, 240*time.Second + time.Millisecond},
		{"both: the largest", []engine.Subject{nodeS, placement}, 3600*time.Second + time.Millisecond},
		{"a dependency", []engine.Subject{engine.EdgeSubject(svc, svc, catalog.DependsOn)}, 1200*time.Second + time.Millisecond},
		{"an unregistered relation", []engine.Subject{engine.EdgeSubject(pod, node, catalog.HasAddress)}, time.Millisecond},
		{"no subject", nil, time.Millisecond},
	}
	for _, c := range cases {
		if got := vmmodel.Band(reg, c.subs); got != c.want {
			t.Errorf("%s: band %s, want %s", c.name, got, c.want)
		}
	}
}

func TestRegistryRefusals(t *testing.T) {
	cases := []struct {
		name     string
		edges    map[catalog.RelationType][]vmmodel.Holder
		entities map[catalog.EntityType][]vmmodel.Holder
	}{
		{"one producer with two windows", map[catalog.RelationType][]vmmodel.Holder{
			catalog.ScheduledOn: {{Producer: "p", Window: time.Minute}},
			catalog.RunsOn:      {{Producer: "p", Window: 2 * time.Minute}},
		}, nil},
		{"two windows across an edge and an entity", map[catalog.RelationType][]vmmodel.Holder{
			catalog.ScheduledOn: {{Producer: "p", Window: time.Minute}},
		}, map[catalog.EntityType][]vmmodel.Holder{catalog.K8sNode: {{Producer: "p", Window: 0}}}},
		{"a negative window", map[catalog.RelationType][]vmmodel.Holder{catalog.RunsOn: {{Producer: "p", Window: -time.Second}}}, nil},
		{"a window that is not whole milliseconds", map[catalog.RelationType][]vmmodel.Holder{catalog.RunsOn: {{Producer: "p", Window: time.Microsecond}}}, nil},
		{"a producer twice in one list", map[catalog.RelationType][]vmmodel.Holder{catalog.RunsOn: {{Producer: "p", Window: time.Second}, {Producer: "p", Window: time.Second}}}, nil},
		{"a holder with no name", nil, map[catalog.EntityType][]vmmodel.Holder{catalog.K8sNode: {{Window: time.Second}}}},
	}
	for _, c := range cases {
		if _, err := vmmodel.NewRegistry(c.edges, c.entities); !errors.Is(err, vmmodel.ErrRegistry) {
			t.Errorf("%s: err = %v, want ErrRegistry", c.name, err)
		}
	}
}

func TestRegistryDigestIsStable(t *testing.T) {
	a, _ := vmmodel.RegistryFor(fixture.Registered(time.Hour))
	b, _ := vmmodel.RegistryFor(fixture.Registered(72 * time.Hour))
	if a.Digest() != b.Digest() || len(a.Digest()) != 64 {
		t.Errorf("digests %s and %s", a.Digest(), b.Digest())
	}
}
