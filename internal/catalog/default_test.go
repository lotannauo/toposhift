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
	kind     catalog.Kind
	optional bool
}

func TestDefaultEntities(t *testing.T) {
	t.Parallel()

	tests := []struct {
		typ   catalog.EntityType
		layer catalog.Layer
		keys  []wantKey
	}{
		{catalog.Rack, catalog.L0, []wantKey{{"topo.rack.id", catalog.KindString, false}}},
		{catalog.Switch, catalog.L0, []wantKey{{"topo.switch.id", catalog.KindString, false}}},
		{catalog.Port, catalog.L0, []wantKey{{"topo.port.id", catalog.KindString, false}}},
		{catalog.Host, catalog.L1, []wantKey{{"host.id", catalog.KindString, false}}},
		{catalog.Zone, catalog.L1, []wantKey{{"topo.zone.id", catalog.KindString, false}}},
		{catalog.K8sNode, catalog.L2, []wantKey{{"k8s.node.uid", catalog.KindString, false}}},
		{catalog.K8sPod, catalog.L2, []wantKey{{"k8s.pod.uid", catalog.KindString, false}}},
		{catalog.Container, catalog.L2, []wantKey{{"container.id", catalog.KindString, false}}},
		{catalog.Process, catalog.L2, []wantKey{
			{"host.id", catalog.KindString, false}, {"process.pid", catalog.KindInt, false}, {"process.creation.time", catalog.KindTime, false},
		}},
		{catalog.ServiceInstance, catalog.L2, []wantKey{
			{"service.namespace", catalog.KindString, true}, {"service.name", catalog.KindString, false}, {"service.instance.id", catalog.KindString, false},
		}},
		{catalog.Service, catalog.L3, []wantKey{
			{"service.namespace", catalog.KindString, true}, {"service.name", catalog.KindString, false},
		}},
		{catalog.NetworkEndpoint, catalog.L2, []wantKey{
			{"topo.endpoint.realm", catalog.KindString, false}, {"topo.endpoint.address", catalog.KindString, false},
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
				got = append(got, wantKey{k.Name, k.Kind, k.Optional})
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
		layer       catalog.Layer
	}{
		{
			catalog.PartOf,
			[]catalog.Endpoint{{From: catalog.Container, To: catalog.K8sPod}, {From: catalog.ServiceInstance, To: catalog.K8sPod}},
			catalog.PropagateDown, catalog.StorageFrom, catalog.L2,
		},
		{
			catalog.ScheduledOn,
			[]catalog.Endpoint{{From: catalog.K8sPod, To: catalog.K8sNode}},
			catalog.PropagateDown, catalog.StorageFrom, catalog.L2,
		},
		{
			catalog.RunsOn,
			[]catalog.Endpoint{{From: catalog.K8sNode, To: catalog.Host}, {From: catalog.Process, To: catalog.Host}},
			catalog.PropagateDown, catalog.StorageFrom, catalog.L1,
		},
		{
			catalog.LocatedIn,
			[]catalog.Endpoint{{From: catalog.Host, To: catalog.Rack}, {From: catalog.Host, To: catalog.Zone}},
			catalog.PropagateDown, catalog.StorageFrom, catalog.L1,
		},
		{
			catalog.InstanceOf,
			[]catalog.Endpoint{{From: catalog.ServiceInstance, To: catalog.Service}},
			catalog.PropagateNone, catalog.StorageFrom, catalog.L2,
		},
		{
			catalog.DependsOn,
			[]catalog.Endpoint{{From: catalog.Service, To: catalog.Service}},
			catalog.PropagateUp, catalog.StorageFrom, catalog.L3,
		},
		{catalog.SameAs, nil, catalog.PropagateNone, catalog.StorageDerived, 0},
		{
			catalog.HasAddress,
			[]catalog.Endpoint{{From: catalog.K8sPod, To: catalog.NetworkEndpoint}, {From: catalog.K8sNode, To: catalog.NetworkEndpoint}},
			catalog.PropagateNone, catalog.StorageFrom, catalog.L2,
		},
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
			if r.Layer() != tt.layer {
				t.Errorf("layer = %s, want %s", r.Layer(), tt.layer)
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

// A service instance is part of the pod it runs in, and nothing else about
// part_of changes: its id, layer and propagation stay those of a container's
// edge, and the pair is allowed in one direction only.
func TestPartOfAllowsAServiceInstanceInAPod(t *testing.T) {
	t.Parallel()

	r, ok := catalog.Default().Relation(catalog.PartOf)
	if !ok {
		t.Fatal("part_of is not registered")
	}
	tests := []struct {
		name     string
		from, to catalog.EntityType
		want     bool
	}{
		{"a service instance in a pod", catalog.ServiceInstance, catalog.K8sPod, true},
		{"a container in a pod", catalog.Container, catalog.K8sPod, true},
		{"a pod of a service instance", catalog.K8sPod, catalog.ServiceInstance, false},
		{"a pod of a container", catalog.K8sPod, catalog.Container, false},
		{"a service instance in a container", catalog.ServiceInstance, catalog.Container, false},
		{"a service instance in a node", catalog.ServiceInstance, catalog.K8sNode, false},
		{"a service instance in a service", catalog.ServiceInstance, catalog.Service, false},
		{"a process in a pod", catalog.Process, catalog.K8sPod, false},
		{"a service in a pod", catalog.Service, catalog.K8sPod, false},
	}
	for _, tt := range tests {
		if got := r.Allows(tt.from, tt.to); got != tt.want {
			t.Errorf("%s: Allows(%s, %s) = %v, want %v", tt.name, tt.from, tt.to, got, tt.want)
		}
	}
	if r.ID() != 1 || r.Layer() != catalog.L2 || r.Propagation() != catalog.PropagateDown {
		t.Errorf("part_of has id %d, layer %s, propagation %s; want 1, L2, down", r.ID(), r.Layer(), r.Propagation())
	}
}

// A pod or a node claims an address, in that direction only, and nothing else
// does. The claim is no failure path, so a blast radius never reaches an owner
// from an address, and it is stored with the owner, in L2.
func TestHasAddressIsAClaimByAPodOrANode(t *testing.T) {
	t.Parallel()

	r, ok := catalog.Default().Relation(catalog.HasAddress)
	if !ok {
		t.Fatal("has_address is not registered")
	}
	tests := []struct {
		name     string
		from, to catalog.EntityType
		want     bool
	}{
		{"a pod's address", catalog.K8sPod, catalog.NetworkEndpoint, true},
		{"a node's address", catalog.K8sNode, catalog.NetworkEndpoint, true},
		{"an address of a pod", catalog.NetworkEndpoint, catalog.K8sPod, false},
		{"an address of a node", catalog.NetworkEndpoint, catalog.K8sNode, false},
		{"a host's address", catalog.Host, catalog.NetworkEndpoint, false},
		{"a process's address", catalog.Process, catalog.NetworkEndpoint, false},
		{"a container's address", catalog.Container, catalog.NetworkEndpoint, false},
		{"a service instance's address", catalog.ServiceInstance, catalog.NetworkEndpoint, false},
		{"a service's address", catalog.Service, catalog.NetworkEndpoint, false},
		{"an address of an address", catalog.NetworkEndpoint, catalog.NetworkEndpoint, false},
	}
	for _, tt := range tests {
		if got := r.Allows(tt.from, tt.to); got != tt.want {
			t.Errorf("%s: Allows(%s, %s) = %v, want %v", tt.name, tt.from, tt.to, got, tt.want)
		}
	}
	if r.ID() != 8 || r.Layer() != catalog.L2 || r.Propagation() != catalog.PropagateNone || r.Storage() != catalog.StorageFrom {
		t.Errorf("has_address has id %d, layer %s, propagation %s, storage %s; want 8, L2, none, from", r.ID(), r.Layer(), r.Propagation(), r.Storage())
	}
	e, ok := catalog.Default().Entity(catalog.NetworkEndpoint)
	if !ok || e.ID() != 12 || e.Layer() != catalog.L2 {
		t.Errorf("topo.endpoint: %v, id %d, layer %s; want 12, L2", ok, e.ID(), e.Layer())
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
