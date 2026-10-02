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

func defaultEntities() []EntitySpec {
	return []EntitySpec{
		// L0 fabric.
		{Type: Rack, Layer: L0, Keys: []Key{{Name: RackID, Kind: KindString}}},
		{Type: Switch, Layer: L0, Keys: []Key{{Name: SwitchID, Kind: KindString}}},
		{Type: Port, Layer: L0, Keys: []Key{{Name: PortID, Kind: KindString}}},

		// L1 host placement.
		{Type: Host, Layer: L1, Keys: []Key{{Name: HostID, Kind: KindString}}},
		{Type: Zone, Layer: L1, Keys: []Key{{Name: ZoneID, Kind: KindString}}},

		// L2 workload placement. A process registers the raw host.id rather
		// than a host fingerprint: the identity layer computes the hash from
		// registered keys, and registering a fingerprint would hash twice and
		// tie the catalog to the identity implementation.
		{Type: K8sNode, Layer: L2, Keys: []Key{{Name: K8sNodeUID, Kind: KindString}}},
		{Type: K8sPod, Layer: L2, Keys: []Key{{Name: K8sPodUID, Kind: KindString}}},
		{Type: Container, Layer: L2, Keys: []Key{{Name: ContainerID, Kind: KindString}}},
		{Type: Process, Layer: L2, Keys: []Key{
			{Name: HostID, Kind: KindString},
			{Name: ProcessPID, Kind: KindInt},
			{Name: ProcessCreationTime, Kind: KindTime},
		}},
		{Type: ServiceInstance, Layer: L2, Keys: []Key{
			{Name: ServiceNamespace, Kind: KindString, Optional: true},
			{Name: ServiceName, Kind: KindString},
			{Name: ServiceInstanceID, Kind: KindString},
		}},

		// L3 logical dependency.
		{Type: Service, Layer: L3, Keys: []Key{
			{Name: ServiceNamespace, Kind: KindString, Optional: true},
			{Name: ServiceName, Kind: KindString},
		}},
	}
}

func defaultRelations() []RelationSpec {
	return []RelationSpec{
		{
			Type:        PartOf,
			Endpoints:   []Endpoint{{From: Container, To: K8sPod}},
			Propagation: PropagateDown,
			Storage:     StorageFrom,
		},
		{
			Type:        ScheduledOn,
			Endpoints:   []Endpoint{{From: K8sPod, To: K8sNode}},
			Propagation: PropagateDown,
			Storage:     StorageFrom,
		},
		{
			Type: RunsOn,
			Endpoints: []Endpoint{
				{From: K8sNode, To: Host},
				{From: Process, To: Host},
			},
			Propagation: PropagateDown,
			Storage:     StorageFrom,
		},
		{
			Type: LocatedIn,
			Endpoints: []Endpoint{
				{From: Host, To: Rack},
				{From: Host, To: Zone},
			},
			Propagation: PropagateDown,
			Storage:     StorageFrom,
		},
		{
			Type:        InstanceOf,
			Endpoints:   []Endpoint{{From: ServiceInstance, To: Service}},
			Propagation: PropagateNone,
			Storage:     StorageFrom,
		},
		{
			// The caller stores the edge. A callee failure hits its callers.
			Type:        DependsOn,
			Endpoints:   []Endpoint{{From: Service, To: Service}},
			Propagation: PropagateUp,
			Storage:     StorageFrom,
		},
		{
			// Cross-source device reconciliation, computed at read time over
			// connected components and never written as a fact. Unconstrained
			// until the identity model decides which type pairs it may join.
			Type:        SameAs,
			Propagation: PropagateNone,
			Storage:     StorageDerived,
		},
	}
}
