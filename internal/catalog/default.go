package catalog

// defaultCatalog is built once at package initialization. The shipped specs
// are static, so a failure here is a programming error and fails fast;
// TestDefaultIsValid keeps it from reaching a release.
var defaultCatalog = mustNew(defaultEntities(), defaultRelations())

// Default returns the shipped catalog.
func Default() *Catalog { return defaultCatalog }

func mustNew(entities []EntitySpec, relations []RelationSpec) *Catalog {
	c, err := New(entities, relations)
	if err != nil {
		panic(err)
	}
	return c
}

// defaultEntities lists the shipped entity types. Their ids are written out,
// never computed from position, because they are stored in keys: append a new
// type with the next unused number, and never renumber or reuse one. The
// golden test in ids_test.go fails on any change that renumbers.
func defaultEntities() []EntitySpec {
	return []EntitySpec{
		// L0 fabric.
		{ID: 1, Type: Rack, Layer: L0, Keys: []Key{{Name: RackID, Kind: KindString}}},
		{ID: 2, Type: Switch, Layer: L0, Keys: []Key{{Name: SwitchID, Kind: KindString}}},
		{ID: 3, Type: Port, Layer: L0, Keys: []Key{{Name: PortID, Kind: KindString}}},

		// L1 host placement.
		{ID: 4, Type: Host, Layer: L1, Keys: []Key{{Name: HostID, Kind: KindString}}},
		{ID: 5, Type: Zone, Layer: L1, Keys: []Key{{Name: ZoneID, Kind: KindString}}},

		// L2 workload placement. A process registers the raw host.id rather
		// than a host fingerprint: the identity layer computes the hash from
		// registered keys, and registering a fingerprint would hash twice and
		// tie the catalog to the identity implementation.
		{ID: 6, Type: K8sNode, Layer: L2, Keys: []Key{{Name: K8sNodeUID, Kind: KindString}}},
		{ID: 7, Type: K8sPod, Layer: L2, Keys: []Key{{Name: K8sPodUID, Kind: KindString}}},
		{ID: 8, Type: Container, Layer: L2, Keys: []Key{{Name: ContainerID, Kind: KindString}}},
		{ID: 9, Type: Process, Layer: L2, Keys: []Key{
			{Name: HostID, Kind: KindString},
			{Name: ProcessPID, Kind: KindInt},
			{Name: ProcessCreationTime, Kind: KindTime},
		}},
		{ID: 10, Type: ServiceInstance, Layer: L2, Keys: []Key{
			{Name: ServiceNamespace, Kind: KindString, Optional: true},
			{Name: ServiceName, Kind: KindString},
			{Name: ServiceInstanceID, Kind: KindString},
		}},

		// L3 logical dependency.
		{ID: 11, Type: Service, Layer: L3, Keys: []Key{
			{Name: ServiceNamespace, Kind: KindString, Optional: true},
			{Name: ServiceName, Kind: KindString},
		}},
	}
}

// defaultRelations lists the shipped relations. Their ids are written out and
// follow the same rules as the entity ids above, in a space of their own.
func defaultRelations() []RelationSpec {
	return []RelationSpec{
		{
			ID:          1,
			Type:        PartOf,
			Endpoints:   []Endpoint{{From: Container, To: K8sPod}},
			Propagation: PropagateDown,
			Storage:     StorageFrom,
		},
		{
			ID:          2,
			Type:        ScheduledOn,
			Endpoints:   []Endpoint{{From: K8sPod, To: K8sNode}},
			Propagation: PropagateDown,
			Storage:     StorageFrom,
		},
		{
			ID:   3,
			Type: RunsOn,
			Endpoints: []Endpoint{
				{From: K8sNode, To: Host},
				{From: Process, To: Host},
			},
			Propagation: PropagateDown,
			Storage:     StorageFrom,
		},
		{
			ID:   4,
			Type: LocatedIn,
			Endpoints: []Endpoint{
				{From: Host, To: Rack},
				{From: Host, To: Zone},
			},
			Propagation: PropagateDown,
			Storage:     StorageFrom,
		},
		{
			ID:          5,
			Type:        InstanceOf,
			Endpoints:   []Endpoint{{From: ServiceInstance, To: Service}},
			Propagation: PropagateNone,
			Storage:     StorageFrom,
		},
		{
			// The caller stores the edge. A callee failure hits its callers.
			ID:          6,
			Type:        DependsOn,
			Endpoints:   []Endpoint{{From: Service, To: Service}},
			Propagation: PropagateUp,
			Storage:     StorageFrom,
		},
		{
			// Cross-source device reconciliation, computed at read time over
			// connected components and never written as a fact. Unconstrained
			// until the identity model decides which type pairs it may join.
			ID:          7,
			Type:        SameAs,
			Propagation: PropagateNone,
			Storage:     StorageDerived,
		},
	}
}
