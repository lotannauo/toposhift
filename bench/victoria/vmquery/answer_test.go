package vmquery_test

import (
	"strings"
	"testing"

	"github.com/lotannauo/toposhift/bench/spike/engine"
	"github.com/lotannauo/toposhift/bench/victoria/internal/fixture"
	"github.com/lotannauo/toposhift/bench/victoria/vmmodel"
	"github.com/lotannauo/toposhift/bench/victoria/vmquery"
	"github.com/lotannauo/toposhift/internal/catalog"
	"github.com/lotannauo/toposhift/internal/identity"
)

func edgeResult(src, dst, rel, producer, value string) vmquery.Result {
	return vmquery.Result{Labels: map[string]string{"layer": "L2", "src": src, "dst": dst, "rel": rel, "producer": producer}, Value: value}
}

var (
	podA = fixture.FP(catalog.K8sPod, 0x0a)
	podB = fixture.FP(catalog.K8sPod, 0x0b)
	podC = fixture.FP(catalog.K8sPod, 0x0c)
)

func TestParseResponse(t *testing.T) {
	body := `{"status":"success","isPartial":false,"data":{"resultType":"vector","result":[
		{"metric":{"__name__":"topo_edge","dst":"k8s.node:x","rel":"scheduled_on"},"value":[1767225600.25,"1"]},
		{"metric":{"dst":"k8s.node:y"},"value":[1767225600.25,"NaN"]}]}}`
	rs, err := vmquery.ParseResponse([]byte(body))
	if err != nil || len(rs) != 2 {
		t.Fatalf("%v, %v", rs, err)
	}
	if rs[0].Labels["rel"] != "scheduled_on" || rs[0].Value != "1" || rs[1].Value != "NaN" {
		t.Errorf("results %+v", rs)
	}
	empty, err := vmquery.ParseResponse([]byte(`{"status":"success","data":{"resultType":"vector","result":[]}}`))
	if err != nil || empty == nil || len(empty) != 0 {
		t.Errorf("an empty vector: %v, %v", empty, err)
	}
	for name, bad := range map[string]string{
		"not JSON":                     `{`,
		"an error status":              `{"status":"error","errorType":"bad_data","error":"parse error"}`,
		"a partial answer":             `{"status":"success","isPartial":true,"data":{"resultType":"vector","result":[]}}`,
		"a matrix":                     `{"status":"success","data":{"resultType":"matrix","result":[]}}`,
		"no result type":               `{"status":"success","data":{}}`,
		"a value that is not text":     `{"status":"success","data":{"resultType":"vector","result":[{"metric":{},"value":[1,2]}]}}`,
		"a value that is not a number": `{"status":"success","data":{"resultType":"vector","result":[{"metric":{},"value":[1,"abc"]}]}}`,
	} {
		if _, err := vmquery.ParseResponse([]byte(bad)); err == nil {
			t.Errorf("%s: accepted", name)
		}
	}
	if _, err := vmquery.ParseResponse([]byte(`{"status":"error","errorType":"bad_data","error":"parse error"}`)); err == nil || !strings.Contains(err.Error(), "parse error") {
		t.Errorf("the store's message is lost: %v", err)
	}
}

// Peers come back in the engine's order (by peer fingerprint, then relation), each edge
// once however many producers hold it, whatever the order the store gave.
func TestNeighborsAreInTheEnginesOrder(t *testing.T) {
	n := node.String()
	rs := []vmquery.Result{
		edgeResult(podC.String(), n, "scheduled_on", "k8s-cluster", "1"),
		edgeResult(podA.String(), n, "scheduled_on", "kubelet", "1"),
		edgeResult(podB.String(), n, "scheduled_on", "k8s-cluster", "1"),
		edgeResult(podA.String(), n, "scheduled_on", "k8s-cluster", "1"), // held by two producers
		edgeResult(podA.String(), n, "part_of", "k8s-cluster", "1"),      // another relation, same peer
	}
	got, err := vmquery.Neighbors(rs, node, engine.Reverse)
	if err != nil {
		t.Fatal(err)
	}
	want := []engine.Neighbor{
		{Peer: podA, Relation: "part_of"},
		{Peer: podA, Relation: "scheduled_on"},
		{Peer: podB, Relation: "scheduled_on"},
		{Peer: podC, Relation: "scheduled_on"},
	}
	if len(got) != len(want) {
		t.Fatalf("got %v", got)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("neighbor %d is %v, want %v", i, got[i], want[i])
		}
	}
	// A stale marker the store returned is the series having ended: not a neighbor.
	got, err = vmquery.Neighbors([]vmquery.Result{edgeResult(podA.String(), n, "scheduled_on", "kubelet", "NaN")}, node, engine.Reverse)
	if err != nil || len(got) != 0 {
		t.Errorf("a marker is a neighbor: %v, %v", got, err)
	}
	// A forward read: the peer is the target, the entity is on the source.
	fwd, err := vmquery.Neighbors([]vmquery.Result{edgeResult(podA.String(), n, "scheduled_on", "kubelet", "1")}, podA, engine.Forward)
	if err != nil || len(fwd) != 1 || fwd[0].Peer != node {
		t.Errorf("forward: %v, %v", fwd, err)
	}
}

func TestNeighborsRefuseAnAnswerForAnotherEntityOrAnotherEnd(t *testing.T) {
	n := node.String()
	good := edgeResult(podA.String(), n, "scheduled_on", "kubelet", "1")
	for name, c := range map[string]struct {
		r   vmquery.Result
		dir engine.Direction
	}{
		"a reverse read answered with edges on the source end": {good, engine.Forward},
		"an edge of another entity":                            {edgeResult(podA.String(), podB.String(), "scheduled_on", "kubelet", "1"), engine.Reverse},
		"no relation":                                          {edgeResult(podA.String(), n, "", "kubelet", "1"), engine.Reverse},
		"a peer that is not a fingerprint":                     {edgeResult("nonsense", n, "scheduled_on", "kubelet", "1"), engine.Reverse},
	} {
		if _, err := vmquery.Neighbors([]vmquery.Result{c.r}, node, c.dir); err == nil {
			t.Errorf("%s: accepted", name)
		}
	}
}

// A batch answers with a list for each entity asked about, in the order asked, a repeated
// entity twice, and an entity with no edge with an empty list, not none.
func TestBatchKeepsOrderRepeatsAndEmptyLists(t *testing.T) {
	n1, n2, n3 := fixture.FP(catalog.K8sNode, 1), fixture.FP(catalog.K8sNode, 2), fixture.FP(catalog.K8sNode, 3)
	rs := []vmquery.Result{
		edgeResult(podB.String(), n2.String(), "scheduled_on", "k8s-cluster", "1"),
		edgeResult(podA.String(), n1.String(), "scheduled_on", "k8s-cluster", "1"),
		edgeResult(podC.String(), n1.String(), "scheduled_on", "kubelet", "1"),
		edgeResult(podC.String(), n1.String(), "scheduled_on", "k8s-cluster", "1"),
	}
	got, err := vmquery.NeighborsBatch(rs, []identity.Fingerprint{n2, n3, n1, n2}, engine.Reverse)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 4 {
		t.Fatalf("%d lists for 4 entities (an empty list dropped?): %v", len(got), got)
	}
	if len(got[0]) != 1 || got[0][0].Peer != podB {
		t.Errorf("n2: %v", got[0])
	}
	if got[1] == nil || len(got[1]) != 0 {
		t.Errorf("n3 has no edge: %#v", got[1])
	}
	if len(got[2]) != 2 || got[2][0].Peer != podA || got[2][1].Peer != podC {
		t.Errorf("n1 (sorted, once for two producers): %v", got[2])
	}
	if len(got[3]) != 1 || got[3][0].Peer != podB {
		t.Errorf("n2 asked twice: %v", got[3])
	}
	// An answer for an entity not in the batch is refused.
	if _, err := vmquery.NeighborsBatch([]vmquery.Result{edgeResult(podA.String(), n1.String(), "scheduled_on", "k", "1")}, []identity.Fingerprint{n2}, engine.Reverse); err == nil {
		t.Error("an answer for an entity the batch did not ask about")
	}
	// A forward batch reads the source end.
	f, err := vmquery.NeighborsBatch([]vmquery.Result{edgeResult(podA.String(), n1.String(), "scheduled_on", "k", "1")}, []identity.Fingerprint{podA, podB}, engine.Forward)
	if err != nil || len(f) != 2 || len(f[0]) != 1 || f[0][0].Peer != n1 || len(f[1]) != 0 {
		t.Errorf("forward batch: %v, %v", f, err)
	}
}

func TestAliveReadsAnyProducersSeries(t *testing.T) {
	ent := func(fp identity.Fingerprint, producer, value string) vmquery.Result {
		return vmquery.Result{Labels: map[string]string{"layer": "L2", "type": string(fp.Type()), "fp": vmmodel.FPLabel(fp), "producer": producer}, Value: value}
	}
	if ok, err := vmquery.Alive(nil, node); err != nil || ok {
		t.Errorf("no series: %v, %v", ok, err)
	}
	if ok, err := vmquery.Alive([]vmquery.Result{ent(node, "node-collector", "1")}, node); err != nil || !ok {
		t.Errorf("one series: %v, %v", ok, err)
	}
	if ok, err := vmquery.Alive([]vmquery.Result{ent(node, "node-collector", "NaN")}, node); err != nil || ok {
		t.Errorf("a marker: %v, %v", ok, err)
	}
	if ok, err := vmquery.Alive([]vmquery.Result{ent(node, "a", "NaN"), ent(node, "b", "1")}, node); err != nil || !ok {
		t.Errorf("one marker, one sample: %v, %v", ok, err)
	}
	if _, err := vmquery.Alive([]vmquery.Result{ent(podA, "node-collector", "1")}, node); err == nil {
		t.Error("an answer for another entity")
	}
}
