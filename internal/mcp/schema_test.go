package mcp

import (
	"slices"
	"testing"

	"github.com/lotannauo/toposhift/internal/catalog"
	"github.com/lotannauo/toposhift/internal/query/blastradius"
	"github.com/lotannauo/toposhift/internal/query/coresidency"
	"github.com/lotannauo/toposhift/internal/query/graphdiff"
	"github.com/lotannauo/toposhift/internal/query/interval"
)

// describe_schema reads no store, and lists every type and relation of the
// catalog, the tables a name and a description are read through, and every
// policy version.
func TestDescribeSchema(t *testing.T) {
	w := newWorld(t, rankObjs)
	spy := &spyStore{Store: w.st}
	s, err := NewSource(Config{Store: spy, Rank: rankObjs})
	if err != nil {
		t.Fatal(err)
	}
	out, err := s.DescribeSchema(ctx, DescribeSchemaInput{})
	if err != nil {
		t.Fatal(err)
	}
	if spy.reads != 0 {
		t.Errorf("%d reads", spy.reads)
	}
	if out.RangeReads {
		t.Error("range reads on")
	}
	wantPolicy := Policy{
		Interval: interval.PolicyVersion, BlastRadius: blastradius.PolicyVersion, GraphDiff: graphdiff.PolicyVersion,
		CoResidency: coresidency.PolicyVersion, Names: NameTableVersion, Payload: PayloadTableVersion,
	}
	if out.Policy != wantPolicy || out.Names.Version != NameTableVersion || out.Payload.Version != PayloadTableVersion {
		t.Errorf("policy %+v", out.Policy)
	}

	var types []string
	for _, e := range out.Entities {
		types = append(types, e.Type)
	}
	var want []string
	for e := range catalog.Default().Entities() {
		want = append(want, string(e.Type()))
	}
	if !slices.Equal(types, want) {
		t.Errorf("entities %v, want %v", types, want)
	}
	for _, e := range out.Entities {
		if len(e.IdentifyingKeys) == 0 || e.Layer == "" || e.LayerMeaning == "" {
			t.Errorf("%s: %+v", e.Type, e)
		}
	}

	rels := map[string]SchemaRelation{}
	for _, r := range out.Relations {
		rels[r.Type] = r
	}
	if len(rels) != 7 {
		t.Fatalf("%d relations", len(rels))
	}
	if r := rels["scheduled_on"]; r.Propagation != "down" || r.Layer == nil || *r.Layer != "L2" || r.Derived || r.Subject != "asserted" || len(r.Endpoints) != 1 {
		t.Errorf("scheduled_on %+v", r)
	}
	if r := rels["depends_on"]; r.Propagation != "up" || *r.Layer != "L3" {
		t.Errorf("depends_on %+v", r)
	}
	if r := rels["same_as"]; !r.Derived || r.Layer != nil || r.Subject != "derived" || r.Propagation != "none" {
		t.Errorf("same_as %+v", r)
	}
	if r := rels["runs_on"]; *r.Layer != "L1" {
		t.Errorf("runs_on %+v", r)
	}

	named := map[string]bool{}
	for _, e := range out.Entities {
		named[e.Type] = e.Named
	}
	if !named["k8s.pod"] || !named["k8s.node"] || !named["container"] || named["host"] || named["service"] {
		t.Errorf("named types %v", named)
	}
	if !slices.Contains(out.Names.Unnamed, "host") {
		t.Errorf("unnamed %v", out.Names.Unnamed)
	}
	var payloadTypes []string
	for _, p := range out.Payload.Types {
		payloadTypes = append(payloadTypes, p.Type)
	}
	if !slices.Equal(payloadTypes, []string{"host", "k8s.node", "k8s.pod", "container"}) {
		t.Errorf("payload types %v", payloadTypes)
	}

	// Every word an answer uses is in the vocabulary.
	vocab := func(ws []Word) []string {
		var out []string
		for _, w := range ws {
			out = append(out, w.Term)
		}
		return out
	}
	for _, reason := range endVocabulary {
		if !slices.Contains(vocab(out.Vocabulary.EndReasons), reason) {
			t.Errorf("end reason %q is not in the vocabulary", reason)
		}
	}
	if !slices.Equal(vocab(out.Vocabulary.Basis), []string{"fact", "derived"}) || !slices.Equal(vocab(out.Vocabulary.Subject), []string{"asserted", "derived"}) {
		t.Errorf("vocabulary %+v", out.Vocabulary)
	}
}
