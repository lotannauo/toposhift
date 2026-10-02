package catalog

// EntityType names a kind of entity, such as "host" or "k8s.pod". Types that
// OpenTelemetry defines keep OpenTelemetry's name; types it lacks use a
// "topo." prefix.
type EntityType string

// RelationType names a kind of relation, such as "runs_on".
type RelationType string

// AttributeKey names an identifying attribute, such as "host.id".
type AttributeKey string

// MaxNameLen is the longest a type, relation or attribute name may be, in
// bytes. Bounding it lets decoders of persisted or wire data reject absurd
// lengths before allocating.
const MaxNameLen = 255

// Valid reports whether the name has the syntax every catalog name must have:
// at most [MaxNameLen] bytes of lowercase dotted segments. That excludes ":"
// and whitespace, because a fingerprint is the entity type, a colon, then a
// hash, and names also end up in storage keys and on the wire.
func (t EntityType) Valid() bool { return validName(string(t)) }

// Valid reports whether the name has the catalog name syntax; see
// [EntityType.Valid].
func (t RelationType) Valid() bool { return validName(string(t)) }

// Valid reports whether the name has the catalog name syntax; see
// [EntityType.Valid].
func (k AttributeKey) Valid() bool { return validName(string(k)) }

// Entity types in the shipped catalog.
const (
	Rack            EntityType = "topo.rack"
	Switch          EntityType = "topo.switch"
	Port            EntityType = "topo.port"
	Host            EntityType = "host"
	Zone            EntityType = "topo.zone"
	K8sNode         EntityType = "k8s.node"
	K8sPod          EntityType = "k8s.pod"
	Container       EntityType = "container"
	Process         EntityType = "process"
	ServiceInstance EntityType = "service.instance"
	Service         EntityType = "service"
)

// Relation types in the shipped catalog.
const (
	PartOf      RelationType = "part_of"
	ScheduledOn RelationType = "scheduled_on"
	RunsOn      RelationType = "runs_on"
	LocatedIn   RelationType = "located_in"
	InstanceOf  RelationType = "instance_of"
	DependsOn   RelationType = "depends_on"
	SameAs      RelationType = "same_as"
)

// Identifying attribute keys in the shipped catalog.
const (
	RackID              AttributeKey = "topo.rack.id"
	SwitchID            AttributeKey = "topo.switch.id"
	PortID              AttributeKey = "topo.port.id"
	HostID              AttributeKey = "host.id"
	ZoneID              AttributeKey = "topo.zone.id"
	K8sNodeUID          AttributeKey = "k8s.node.uid"
	K8sPodUID           AttributeKey = "k8s.pod.uid"
	ContainerID         AttributeKey = "container.id"
	ProcessPID          AttributeKey = "process.pid"
	ProcessCreationTime AttributeKey = "process.creation.time"
	ServiceNamespace    AttributeKey = "service.namespace"
	ServiceName         AttributeKey = "service.name"
	ServiceInstanceID   AttributeKey = "service.instance.id"
)
