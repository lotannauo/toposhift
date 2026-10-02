package catalog

// EntityType names a kind of entity, such as "host" or "k8s.pod". Types that
// OpenTelemetry defines keep OpenTelemetry's name; types it lacks use a
// "topo." prefix.
type EntityType string

// RelationType names a kind of relation, such as "runs_on".
type RelationType string

// AttributeKey names an identifying attribute, such as "host.id".
type AttributeKey string

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
