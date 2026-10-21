package vmmodel_test

import (
	"errors"
	"strings"
	"testing"

	"github.com/lotannauo/toposhift/bench/spike/engine"
	"github.com/lotannauo/toposhift/bench/victoria/internal/fixture"
	"github.com/lotannauo/toposhift/bench/victoria/vmmodel"
	"github.com/lotannauo/toposhift/internal/catalog"
	"github.com/lotannauo/toposhift/internal/lifecycle"
)

func labelsOf(s vmmodel.Series) string {
	var parts []string
	for _, l := range s.Labels() {
		parts = append(parts, l.Name+"="+l.Value)
	}
	return strings.Join(parts, " ")
}

// An entity is topo_entity{layer, type, fp, producer} and an edge topo_edge{layer, rel,
// src, dst, producer}, labels sorted by name with the metric name first, and an edge's
// ends written type:hash, so a reverse read is a match on dst.
func TestTheSeriesModelHasIdentityLabelsOnly(t *testing.T) {
	node := fixture.FP(catalog.K8sNode, 0xab)
	pod := fixture.FP(catalog.K8sPod, 0x01)

	e := vmmodel.EntitySeries(catalog.L2, node, "node-collector")
	wantE := "__name__=topo_entity fp=ab000000000000000000000000000000 layer=L2 producer=node-collector type=k8s.node"
	if got := labelsOf(e); got != wantE {
		t.Errorf("entity series\n got %s\nwant %s", got, wantE)
	}
	g := vmmodel.EdgeSeries(catalog.L2, pod, node, catalog.ScheduledOn, "k8s-cluster")
	wantG := "__name__=topo_edge dst=k8s.node:ab000000000000000000000000000000 layer=L2 producer=k8s-cluster rel=scheduled_on src=k8s.pod:01000000000000000000000000000000"
	if got := labelsOf(g); got != wantG {
		t.Errorf("edge series\n got %s\nwant %s", got, wantG)
	}
	if g.Metric() != vmmodel.MetricEdge || e.Metric() != vmmodel.MetricEntity {
		t.Errorf("metrics %q and %q", g.Metric(), e.Metric())
	}
	if g.Get("nope") != "" || g.Get(vmmodel.LabelProducer) != "k8s-cluster" {
		t.Errorf("Get misreads")
	}
	// Labels is a copy.
	ls := g.Labels()
	ls[0].Value = "changed"
	if g.Metric() != vmmodel.MetricEdge {
		t.Error("Labels shares its storage")
	}
}

func TestSeriesOfAndItsRefusals(t *testing.T) {
	node, pod := fixture.FP(catalog.K8sNode, 1), fixture.FP(catalog.K8sPod, 2)
	ok := engine.Record{Layer: catalog.L2, Subject: engine.EdgeSubject(pod, node, catalog.ScheduledOn), Producer: "p", Seq: 1, Kind: lifecycle.Observe}
	s, err := vmmodel.SeriesOf(ok)
	if err != nil || s.Get(vmmodel.LabelSrc) != pod.String() || s.Get(vmmodel.LabelDst) != node.String() {
		t.Fatalf("SeriesOf = %v, %v", s, err)
	}
	for name, r := range map[string]engine.Record{
		"an entity with no fingerprint": {Layer: catalog.L2, Subject: engine.Subject{Kind: engine.SubjectEntity}, Seq: 1},
		"an edge with no relation":      {Layer: catalog.L2, Subject: engine.Subject{Kind: engine.SubjectEdge, A: pod, B: node}, Seq: 2},
		"an edge with no target":        {Layer: catalog.L2, Subject: engine.Subject{Kind: engine.SubjectEdge, A: pod, Relation: catalog.ScheduledOn}, Seq: 3},
		"no subject kind":               {Layer: catalog.L2, Seq: 4},
	} {
		if _, err := vmmodel.SeriesOf(r); !errors.Is(err, vmmodel.ErrSeries) {
			t.Errorf("%s: err = %v", name, err)
		}
	}
}

// Key tells two series apart exactly when their labels differ, however the values are
// made: a value that contains the text of the next label does not equal the next label.
func TestKeyCannotBeForged(t *testing.T) {
	node := fixture.FP(catalog.K8sNode, 1)
	a := vmmodel.EntitySeries(catalog.L2, node, "a")
	b := vmmodel.EntitySeries(catalog.L2, node, "a")
	c := vmmodel.EntitySeries(catalog.L2, node, `a";type=x`)
	if a.Key() != b.Key() {
		t.Error("equal series have different keys")
	}
	if a.Key() == c.Key() {
		t.Error("different series share a key")
	}
	if !strings.Contains(c.String(), `\"`) {
		t.Errorf("String does not quote: %s", c)
	}
}
