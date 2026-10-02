package catalog_test

import (
	"slices"
	"testing"

	"github.com/lotannauo/toposhift/internal/catalog"
)

// TestDefaultIsValid fails the build if the shipped specs violate an
// invariant, since the package would otherwise panic at initialization.
func TestDefaultIsValid(t *testing.T) {
	t.Parallel()

	if _, err := catalog.New(catalog.DefaultEntitySpecs(), catalog.DefaultRelationSpecs()); err != nil {
		t.Fatal(err)
	}
	if catalog.Default() == nil {
		t.Fatal("Default() = nil")
	}
}

// The expectations below are written out by hand, independently of
// default.go, so a change to the shipped catalog shows up as a deliberate
// edit to this spec.

type wantKey struct {
	name     catalog.AttributeKey
	optional bool
}

func TestDefaultEntities(t *testing.T) {
	t.Parallel()

	tests := []struct {
		typ   catalog.EntityType
		layer catalog.Layer
		keys  []wantKey
	}{
		{catalog.Rack, catalog.L0, []wantKey{{"topo.rack.id", false}}},
		{catalog.Switch, catalog.L0, []wantKey{{"topo.switch.id", false}}},
		{catalog.Port, catalog.L0, []wantKey{{"topo.port.id", false}}},
		{catalog.Host, catalog.L1, []wantKey{{"host.id", false}}},
		{catalog.Zone, catalog.L1, []wantKey{{"topo.zone.id", false}}},
		{catalog.K8sNode, catalog.L2, []wantKey{{"k8s.node.uid", false}}},
		{catalog.K8sPod, catalog.L2, []wantKey{{"k8s.pod.uid", false}}},
		{catalog.Container, catalog.L2, []wantKey{{"container.id", false}}},
		{catalog.Process, catalog.L2, []wantKey{
			{"host.id", false}, {"process.pid", false}, {"process.creation.time", false},
		}},
		{catalog.ServiceInstance, catalog.L2, []wantKey{
			{"service.namespace", true}, {"service.name", false}, {"service.instance.id", false},
		}},
		{catalog.Service, catalog.L3, []wantKey{
			{"service.namespace", true}, {"service.name", false},
		}},
	}

	c := catalog.Default()
	for _, tt := range tests {
		t.Run(string(tt.typ), func(t *testing.T) {
			t.Parallel()
			e, ok := c.Entity(tt.typ)
			if !ok {
				t.Fatal("not registered")
			}
			if e.Layer() != tt.layer {
				t.Errorf("layer = %s, want %s", e.Layer(), tt.layer)
			}
			var got []wantKey
			for k := range e.Keys() {
				got = append(got, wantKey{k.Name, k.Optional})
			}
			if !slices.Equal(got, tt.keys) {
				t.Errorf("keys = %v, want %v", got, tt.keys)
			}
		})
	}

	// The table must cover the whole registry, so a new type cannot be added
	// without a spec here.
	n := 0
	for range c.Entities() {
		n++
	}
	if n != len(tests) {
		t.Errorf("catalog has %d entity types, spec covers %d", n, len(tests))
	}
}

func TestDefaultRelations(t *testing.T) {
	t.Parallel()

	tests := []struct {
		typ         catalog.RelationType
		endpoints   []catalog.Endpoint
		propagation catalog.Propagation
		storage     catalog.Storage
	}{
		{
			catalog.PartOf,
			[]catalog.Endpoint{{From: catalog.Container, To: catalog.K8sPod}},
			catalog.PropagateDown, catalog.StorageFrom,
		},
		{
			catalog.ScheduledOn,
			[]catalog.Endpoint{{From: catalog.K8sPod, To: catalog.K8sNode}},
			catalog.PropagateDown, catalog.StorageFrom,
		},
		{
			catalog.RunsOn,
			[]catalog.Endpoint{{From: catalog.K8sNode, To: catalog.Host}, {From: catalog.Process, To: catalog.Host}},
			catalog.PropagateDown, catalog.StorageFrom,
		},
		{
			catalog.LocatedIn,
			[]catalog.Endpoint{{From: catalog.Host, To: catalog.Rack}, {From: catalog.Host, To: catalog.Zone}},
			catalog.PropagateDown, catalog.StorageFrom,
		},
		{
			catalog.InstanceOf,
			[]catalog.Endpoint{{From: catalog.ServiceInstance, To: catalog.Service}},
			catalog.PropagateNone, catalog.StorageFrom,
		},
		{
			catalog.DependsOn,
			[]catalog.Endpoint{{From: catalog.Service, To: catalog.Service}},
			catalog.PropagateUp, catalog.StorageFrom,
		},
		{catalog.SameAs, nil, catalog.PropagateNone, catalog.StorageDerived},
	}

	c := catalog.Default()
	for _, tt := range tests {
		t.Run(string(tt.typ), func(t *testing.T) {
			t.Parallel()
			r, ok := c.Relation(tt.typ)
			if !ok {
				t.Fatal("not registered")
			}
			if got := slices.Collect(r.Endpoints()); !slices.Equal(got, tt.endpoints) {
				t.Errorf("endpoints = %v, want %v", got, tt.endpoints)
			}
			if r.Propagation() != tt.propagation {
				t.Errorf("propagation = %s, want %s", r.Propagation(), tt.propagation)
			}
			if r.Storage() != tt.storage {
				t.Errorf("storage = %s, want %s", r.Storage(), tt.storage)
			}
			if r.Derived() != (tt.storage == catalog.StorageDerived) {
				t.Errorf("Derived() = %v, inconsistent with storage %s", r.Derived(), tt.storage)
			}
		})
	}

	n := 0
	for range c.Relations() {
		n++
	}
	if n != len(tests) {
		t.Errorf("catalog has %d relation types, spec covers %d", n, len(tests))
	}
}

// TestDefaultInvariants checks properties of the shipped catalog as a whole
// rather than per type.
func TestDefaultInvariants(t *testing.T) {
	t.Parallel()
	c := catalog.Default()

	t.Run("service.namespace is the only optional key", func(t *testing.T) {
		t.Parallel()
		for e := range c.Entities() {
			for k := range e.Keys() {
				if k.Optional && k.Name != catalog.ServiceNamespace {
					t.Errorf("%s: key %s is optional", e.Type(), k.Name)
				}
			}
		}
	})

	t.Run("every layer has an entity type", func(t *testing.T) {
		t.Parallel()
		seen := map[catalog.Layer]bool{}
		for e := range c.Entities() {
			seen[e.Layer()] = true
		}
		for _, l := range []catalog.Layer{catalog.L0, catalog.L1, catalog.L2, catalog.L3} {
			if !seen[l] {
				t.Errorf("layer %s has no entity type", l)
			}
		}
	})

	t.Run("every entity type is in a relation", func(t *testing.T) {
		t.Parallel()
		// topo.switch and topo.port get their cabling relations with
		// l1-placement; until then they are legitimately unreferenced.
		pending := map[catalog.EntityType]bool{catalog.Switch: true, catalog.Port: true}

		used := map[catalog.EntityType]bool{}
		for r := range c.Relations() {
			for ep := range r.Endpoints() {
				used[ep.From], used[ep.To] = true, true
			}
		}
		for e := range c.Entities() {
			if !used[e.Type()] && !pending[e.Type()] {
				t.Errorf("%s is not referenced by any relation", e.Type())
			}
		}
	})
}
