package store_test

import (
	"errors"
	"testing"
	"time"

	"github.com/lotannauo/toposhift/internal/catalog"
	"github.com/lotannauo/toposhift/internal/identity"
	"github.com/lotannauo/toposhift/internal/store"
)

func lookupFingerprint(t *testing.T, typ catalog.EntityType, first byte) identity.Fingerprint {
	t.Helper()
	var h [identity.FingerprintBytes]byte
	for i := range h {
		h[i] = first + byte(i)
	}
	fp, err := identity.FingerprintFromHash(typ, h)
	if err != nil {
		t.Fatal(err)
	}
	return fp
}

func TestLookupRefValidate(t *testing.T) {
	t.Parallel()
	pod, node := lookupFingerprint(t, catalog.K8sPod, 1), lookupFingerprint(t, catalog.K8sNode, 2)
	edge := store.EdgeSubject(pod, node, catalog.ScheduledOn)
	entity := store.EntitySubject(pod)
	for name, tc := range map[string]struct {
		ref store.LookupRef
		ok  bool
	}{
		"an edge":                    {store.LookupRef{Layer: catalog.L2, Subject: edge, Producer: "k8sobjects"}, true},
		"an entity":                  {store.LookupRef{Layer: catalog.L0, Subject: entity, Producer: "k8s.cluster:1"}, true},
		"no producer":                {store.LookupRef{Layer: catalog.L2, Subject: edge}, false},
		"a producer with a space":    {store.LookupRef{Layer: catalog.L2, Subject: edge, Producer: "a b"}, false},
		"a layer below L0":           {store.LookupRef{Layer: catalog.L0 - 1, Subject: edge, Producer: "p"}, false},
		"a layer above L3":           {store.LookupRef{Layer: catalog.L3 + 1, Subject: edge, Producer: "p"}, false},
		"no subject":                 {store.LookupRef{Layer: catalog.L2, Producer: "p"}, false},
		"an edge with no relation":   {store.LookupRef{Layer: catalog.L2, Subject: store.Subject{Kind: store.SubjectEdge, A: pod, B: node}, Producer: "p"}, false},
		"an edge with no target":     {store.LookupRef{Layer: catalog.L2, Subject: store.Subject{Kind: store.SubjectEdge, A: pod, Relation: catalog.ScheduledOn}, Producer: "p"}, false},
		"an entity with a target":    {store.LookupRef{Layer: catalog.L2, Subject: store.Subject{Kind: store.SubjectEntity, A: pod, B: node}, Producer: "p"}, false},
		"an entity with a relation":  {store.LookupRef{Layer: catalog.L2, Subject: store.Subject{Kind: store.SubjectEntity, A: pod, Relation: catalog.PartOf}, Producer: "p"}, false},
		"an entity with no identity": {store.LookupRef{Layer: catalog.L2, Subject: store.EntitySubject(identity.Fingerprint{}), Producer: "p"}, false},
	} {
		err := tc.ref.Validate()
		if tc.ok && err != nil {
			t.Errorf("%s: %v", name, err)
		}
		if !tc.ok && !errors.Is(err, store.ErrInvalid) {
			t.Errorf("%s: %v, want an error wrapping ErrInvalid", name, err)
		}
	}
}

func TestLookupOutcomeZeroValueIsNotAnAnswer(t *testing.T) {
	t.Parallel()
	var zero store.LastAssertion
	if zero.Outcome != store.LookupInvalid {
		t.Errorf("the zero LastAssertion has outcome %s, want invalid", zero.Outcome)
	}
	seen := map[string]store.LookupOutcome{}
	for _, o := range []store.LookupOutcome{store.LookupInvalid, store.LookupFound, store.LookupNone, store.LookupExhausted, store.LookupBaselineOnly} {
		name := o.String()
		if prev, dup := seen[name]; dup {
			t.Errorf("outcomes %d and %d are both %q", prev, o, name)
		}
		seen[name] = o
	}
	if store.LookupOutcome(200).String() != "LookupOutcome(200)" {
		t.Errorf("an unknown outcome is named %q", store.LookupOutcome(200))
	}
}

func TestLastOf(t *testing.T) {
	t.Parallel()
	at := time.Unix(1_700_000_000, 0).UTC()
	for name, tc := range map[string]struct {
		event, through, want time.Time
		fromThrough          bool
	}{
		"no through":            {at, time.Time{}, at, false},
		"a through after":       {at, at.Add(time.Minute), at.Add(time.Minute), true},
		"a through equal":       {at, at, at, false},
		"a through before":      {at, at.Add(-time.Minute), at, false},
		"a through before zero": {at, time.Time{}, at, false},
	} {
		got, from := store.LastOf(tc.event, tc.through)
		if !got.Equal(tc.want) || from != tc.fromThrough {
			t.Errorf("%s: LastOf = %s, %v; want %s, %v", name, got, from, tc.want, tc.fromThrough)
		}
	}
}
