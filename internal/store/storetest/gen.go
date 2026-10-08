package storetest

import (
	"errors"
	"fmt"
	"math"
	"math/bits"
	"math/rand/v2"
	"slices"
	"time"

	"github.com/lotannauo/toposhift/internal/catalog"
	"github.com/lotannauo/toposhift/internal/identity"
	"github.com/lotannauo/toposhift/internal/lifecycle"
	"github.com/lotannauo/toposhift/internal/store"
)

// Producers in the generated stream.
const (
	ProducerFabric        lifecycle.Producer = "fabric"         // racks and host placement, watch mode
	ProducerNodeCollector lifecycle.Producer = "node-collector" // hosts, nodes, node placement; heartbeats
	ProducerClone         lifecycle.Producer = "clone"          // a cloned machine reporting an old boot
	ProducerK8sObjects    lifecycle.Producer = "k8sobjects"     // pods, containers, their placement; watch mode
	ProducerKubelet       lifecycle.Producer = "kubelet"        // a second opinion on pod placement
	ProducerTraces        lifecycle.Producer = "traces"         // services and their dependencies; heartbeats
)

// Config describes a generated stream. [NewGenerator] validates it and fills in
// nothing: a zero field means zero. Start from [Tiny] and change what you need.
type Config struct {
	// Seed fixes every random choice: one Config yields one stream.
	Seed uint64
	// Start is when the cluster is first seen. It is set, on a whole second, at or
	// after [store.MinEventTime].
	Start time.Time
	// Duration is how long events are generated for: positive, in whole seconds.
	// Start plus Duration plus LateMax plus the longest TTL (and at least an
	// hour) must be at or before [store.MaxEventTime]. With CloneProbability
	// above 0 the longest TTL counts a clone's record, which reports half an
	// interval after a beat and holds for a whole interval.
	Duration time.Duration
	// FirstSeq is the Seq of the first record; the rest count up from it. It is in
	// [1, 1<<63], because 0 means "nothing" as a snapshot token. A layout that
	// stores Seq in too few bits is only found by a stream that crosses a
	// boundary, so a config can start just below one.
	FirstSeq uint64

	// Racks, Hosts, Pods and Services are the sizes of the cluster, each at least
	// 1. There is one node per host and one container per pod.
	Racks, Hosts, Pods, Services int

	// ChurnPerMinute is the rate of pod churn events per minute of event time, a
	// Poisson process; at least 0 and finite.
	ChurnPerMinute float64

	// HeartbeatInterval is how often node-collector and traces refresh what they
	// see: 0 (watch mode: they assert it once, without a TTL) or at least two
	// seconds, in whole seconds. TTLFactor says how long a refresh holds: the TTL
	// is TTLFactor times the interval, and TTLFactor is at least 1 when
	// HeartbeatInterval is above 0.
	HeartbeatInterval time.Duration
	TTLFactor         int

	// ConfirmProbability, in [0, 1], is the chance that a pod placement is also
	// asserted by the kubelet, at the same instant, with a TTL of ConfirmTTL
	// (positive, whole seconds, when the probability is above 0). When the
	// placement ends the kubelet withdraws its assertion half the time and
	// otherwise lets it expire, so an edge can be held up by one producer while
	// the other has deleted it.
	ConfirmProbability float64
	ConfirmTTL         time.Duration

	// LateProbability, in [0, 1], is the chance that a churn record (k8sobjects
	// and kubelet) arrives late. A late record is delayed by a whole number of
	// seconds in [1s, LateMax], which is positive and in whole seconds when the
	// probability is above 0. Its event time is unchanged.
	LateProbability float64
	LateMax         time.Duration

	// Runs coalesces heartbeat refreshes into runs: a refresh that repeats a
	// producer's current run is returned as an extension of it, an Observe at the
	// run's first event time with Through set to the refresh, and a later Seq.
	// The stream then answers every question as the stream without runs does.
	Runs bool

	// RebootProbability, in [0, 1], is the chance, for each host at each
	// heartbeat, that the host rebooted just before it: its boot ID changes. It
	// needs HeartbeatInterval above 0. CloneProbability, in [0, 1], is the
	// chance, for each reboot, that a cloned machine keeps reporting the old
	// boot ID half an interval later, which is a clone collision for a store
	// opened with a lifecycle.Policy whose BootKey is lifecycle.BootID. A host
	// observation carries its boot ID in store.Record.Boot; no other record
	// carries one, so the only quarantines in a stream are the ones
	// CloneProbability makes. It needs RebootProbability above 0.
	RebootProbability float64
	CloneProbability  float64

	// PayloadMin and PayloadMax bound the length of random payloads:
	// 1 <= PayloadMin <= PayloadMax <= 256. Every Observe carries one.
	PayloadMin, PayloadMax int
}

// Tiny is the default small workload for tests: twenty minutes of a handful of
// entities.
func Tiny() Config {
	return Config{
		Seed:     1,
		Start:    time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC),
		Duration: 20 * time.Minute,
		FirstSeq: 1,
		Racks:    2, Hosts: 6, Pods: 24, Services: 4,
		ChurnPerMinute:     30,
		HeartbeatInterval:  time.Minute,
		TTLFactor:          3,
		ConfirmProbability: 0.3,
		ConfirmTTL:         3 * time.Minute,
		LateProbability:    0.1,
		LateMax:            3 * time.Minute,
		PayloadMin:         1,
		PayloadMax:         24,
	}
}

func isProbability(p float64) bool { return p >= 0 && p <= 1 } // false for NaN

func wholeSeconds(d time.Duration) bool { return d%time.Second == 0 }

// Validate reports the first rule the config breaks, or nil. The errors are
// plain errors that name the field.
func (c Config) Validate() error {
	bad := func(format string, args ...any) error {
		return errors.New("storetest: " + fmt.Sprintf(format, args...))
	}
	switch {
	case c.Start.IsZero() || c.Start.Nanosecond() != 0:
		return bad("Start must be set, on a whole second: event times have one-second resolution")
	case c.Start.Before(store.MinEventTime):
		return bad("Start %s is before the earliest event time", c.Start.Format(time.RFC3339))
	case c.Duration <= 0 || !wholeSeconds(c.Duration):
		return bad("Duration must be positive, in whole seconds")
	case c.FirstSeq < 1 || c.FirstSeq > 1<<63:
		return bad("FirstSeq must be in [1, 1<<63]")
	case c.Racks < 1 || c.Hosts < 1 || c.Pods < 1 || c.Services < 1:
		return bad("Racks, Hosts, Pods and Services must each be at least 1")
	case !(c.ChurnPerMinute >= 0) || math.IsInf(c.ChurnPerMinute, 0):
		return bad("ChurnPerMinute must be finite and at least 0")
	case c.HeartbeatInterval != 0 && (c.HeartbeatInterval < 2*time.Second || !wholeSeconds(c.HeartbeatInterval)):
		return bad("HeartbeatInterval must be 0 or at least 2s, in whole seconds")
	case c.TTLFactor < 0 || (c.HeartbeatInterval > 0 && c.TTLFactor < 1):
		return bad("TTLFactor must be at least 1 when HeartbeatInterval is above 0, and never negative")
	case c.HeartbeatInterval > math.MaxInt64/2 || (c.HeartbeatInterval > 0 && int64(c.TTLFactor) > math.MaxInt64/int64(c.HeartbeatInterval)):
		return bad("TTLFactor times HeartbeatInterval overflows")
	case !isProbability(c.ConfirmProbability):
		return bad("ConfirmProbability must be in [0, 1]")
	case c.ConfirmTTL < 0 || !wholeSeconds(c.ConfirmTTL) || (c.ConfirmProbability > 0 && c.ConfirmTTL == 0):
		return bad("ConfirmTTL must be in whole seconds, and positive when ConfirmProbability is above 0")
	case !isProbability(c.LateProbability):
		return bad("LateProbability must be in [0, 1]")
	case c.LateMax < 0 || !wholeSeconds(c.LateMax) || (c.LateProbability > 0 && c.LateMax == 0):
		return bad("LateMax must be in whole seconds, and positive when LateProbability is above 0")
	case !isProbability(c.RebootProbability) || (c.RebootProbability > 0 && c.HeartbeatInterval == 0):
		return bad("RebootProbability must be in [0, 1], and needs HeartbeatInterval above 0")
	case !isProbability(c.CloneProbability) || (c.CloneProbability > 0 && c.RebootProbability == 0):
		return bad("CloneProbability must be in [0, 1], and needs RebootProbability above 0")
	case c.PayloadMin < 1 || c.PayloadMax < c.PayloadMin || c.PayloadMax > 256:
		return bad("payload bounds are %d to %d: want 1 <= PayloadMin <= PayloadMax <= 256", c.PayloadMin, c.PayloadMax)
	}
	longest := max(time.Hour, c.heartbeatTTL(), c.ConfirmTTL)
	if c.CloneProbability > 0 {
		// A clone reports half an interval after a beat, with a TTL of one interval.
		longest = max(longest, c.HeartbeatInterval+c.HeartbeatInterval/2)
	}
	if end := c.Start.Add(c.Duration).Add(c.LateMax).Add(longest); end.After(store.MaxEventTime) {
		return bad("the period from %s, with lateness and the longest TTL, ends at %s, after the latest event time",
			c.Start.Format(time.RFC3339), end.Format(time.RFC3339))
	}
	return nil
}

// heartbeatTTL is the TTL of a heartbeat refresh: zero in watch mode.
func (c Config) heartbeatTTL() time.Duration {
	if c.HeartbeatInterval <= 0 {
		return 0
	}
	return time.Duration(c.TTLFactor) * c.HeartbeatInterval
}

// origin says where in the model an item came from, for the tests that count
// what a record alone does not show.
type origin uint8

const (
	originStart     origin = iota + 1 // the cluster at Start
	originHeartbeat                   // a heartbeat refresh
	originClone                       // a cloned machine reporting an old boot
	originChurn                       // pod churn
)

// item is one record of the stream before it is returned: when it arrives, and
// what is known about where it came from.
type item struct {
	arrival time.Time
	rec     store.Record // its Seq is not yet assigned
	origin  origin
	late    bool // a churn record that was delayed
	event   int  // the churn event it belongs to, or -1
	refresh bool // a heartbeat refresh, which a run can absorb
}

// Generator produces the stream. It is not safe for concurrent use.
type Generator struct {
	cfg      Config
	items    []item // in arrival order
	next     int
	seq      uint64 // the Seq of the last record returned
	entities []identity.Fingerprint

	runs    map[runKey]*run
	horizon time.Time // the store's retention horizon, once told

	confirmEnds int // confirmed placements that ended (a withdrawal or an expiry)
}

// NewGenerator builds a generator for a valid config. It refuses an invalid one.
func NewGenerator(c Config) (*Generator, error) {
	if err := c.Validate(); err != nil {
		return nil, err
	}
	c.Start = c.Start.UTC()
	b := newBuilder(c)
	b.buildCluster()
	b.emitStart()
	b.emitEvents()

	slices.SortStableFunc(b.items, func(x, y item) int { return x.arrival.Compare(y.arrival) })
	return &Generator{
		cfg:         c,
		items:       b.items,
		seq:         c.FirstSeq - 1,
		entities:    b.entities(),
		runs:        make(map[runKey]*run),
		confirmEnds: b.confirmEnds,
	}, nil
}

// Next returns the next record in arrival order, with its Seq assigned, or false
// when the stream is over.
func (g *Generator) Next() (store.Record, bool) {
	if g.next >= len(g.items) {
		return store.Record{}, false
	}
	it := g.items[g.next]
	g.next++
	rec := g.coalesce(it)
	g.seq++
	rec.Seq = g.seq
	rec.Payload = slices.Clone(rec.Payload) // the generator keeps its own
	return rec, true
}

// Batch returns up to n records, fewer at the end of the stream.
func (g *Generator) Batch(n int) []store.Record {
	var out []store.Record
	for len(out) < n {
		r, ok := g.Next()
		if !ok {
			break
		}
		out = append(out, r)
	}
	return out
}

// All drains the stream.
func (g *Generator) All() []store.Record {
	var out []store.Record
	for {
		r, ok := g.Next()
		if !ok {
			return out
		}
		out = append(out, r)
	}
}

// Entities returns every entity of the cluster, in a fixed order: racks,
// hosts, nodes, services, pods, then containers. It returns a new slice.
func (g *Generator) Entities() []identity.Fingerprint { return slices.Clone(g.entities) }

// SetHorizon tells the generator the store retained everything before h. From
// then on a run of refreshes that began before h is not extended (the store
// would refuse the record), and the next refresh starts a new run at its own
// instant instead. It only moves forward, and changes nothing unless Runs is set.
//
// It does not stop late records. A churn record that arrives late keeps its
// event time, which can be before the horizon, and the store then refuses it with
// store.ErrBeforeHorizon. So no record is before the horizon once it is set only
// when LateProbability is 0. A caller that retains while the stream has lateness
// should either generate without lateness around the Retain, or expect that
// refusal for the late records that straddle it.
func (g *Generator) SetHorizon(h time.Time) {
	if h.After(g.horizon) {
		g.horizon = h
	}
}

// Start is the start of the simulated period.
func (g *Generator) Start() time.Time { return g.cfg.Start }

// End is the end of the simulated period: Start plus Duration. Late records can
// still arrive after it, and so can the one a clone sends after the last
// heartbeat.
func (g *Generator) End() time.Time { return g.cfg.Start.Add(g.cfg.Duration) }

// builder generates the whole uncoalesced stream, in event order, from one
// random source.
type builder struct {
	cfg Config
	rng *rand.Rand
	res *identity.Resolver
	end time.Time

	items []item
	event int // the churn event being generated

	racks, hosts, nodes, services, pods, containers []identity.Fingerprint
	hostPayload, nodePayload, runsOnPayload         [][]byte
	servicePayload, dependsPayload                  [][]byte
	boot                                            []int  // the boot number of each host
	podNode                                         []int  // the node of each pod, or -1 while it does not exist
	confirmed                                       []bool // the kubelet also asserts the pod's placement

	confirmEnds int
}

func newBuilder(c Config) *builder {
	return &builder{
		cfg:   c,
		rng:   rand.New(rand.NewPCG(c.Seed, c.Seed^0x9e3779b97f4a7c15)),
		res:   identity.NewResolver(catalog.Default()),
		end:   c.Start.Add(c.Duration),
		event: -1,
	}
}

func (b *builder) fingerprint(t catalog.EntityType, key catalog.AttributeKey, format string, a ...any) identity.Fingerprint {
	id, err := b.res.Resolve(t, []identity.Attr{{Key: key, Value: fmt.Sprintf(format, a...)}})
	if err != nil {
		panic(fmt.Sprintf("storetest: building a %s fingerprint: %v", t, err)) // a bug in this package
	}
	return id.Fingerprint()
}

// buildCluster names every entity and draws the payloads that heartbeats repeat.
func (b *builder) buildCluster() {
	c := b.cfg
	for i := range c.Racks {
		b.racks = append(b.racks, b.fingerprint(catalog.Rack, catalog.RackID, "rack-%d", i))
	}
	for i := range c.Hosts {
		b.hosts = append(b.hosts, b.fingerprint(catalog.Host, catalog.HostID, "host-%d", i))
		b.nodes = append(b.nodes, b.fingerprint(catalog.K8sNode, catalog.K8sNodeUID, "node-%d", i))
		b.hostPayload = append(b.hostPayload, b.payload())
		b.nodePayload = append(b.nodePayload, b.payload())
		b.runsOnPayload = append(b.runsOnPayload, b.payload())
	}
	b.boot = make([]int, c.Hosts)
	for i := range c.Services {
		b.services = append(b.services, b.fingerprint(catalog.Service, catalog.ServiceName, "service-%d", i))
		b.servicePayload = append(b.servicePayload, b.payload())
		b.dependsPayload = append(b.dependsPayload, b.payload())
	}
	for i := range c.Pods {
		b.pods = append(b.pods, b.fingerprint(catalog.K8sPod, catalog.K8sPodUID, "pod-%d", i))
		b.containers = append(b.containers, b.fingerprint(catalog.Container, catalog.ContainerID, "container-%d", i))
	}
	b.podNode = make([]int, c.Pods)
	for i := range b.podNode {
		b.podNode[i] = -1
	}
	b.confirmed = make([]bool, c.Pods)
}

func (b *builder) entities() []identity.Fingerprint {
	var out []identity.Fingerprint
	for _, group := range [][]identity.Fingerprint{b.racks, b.hosts, b.nodes, b.services, b.pods, b.containers} {
		out = append(out, group...)
	}
	return out
}

// payload draws a random payload within the configured bounds.
func (b *builder) payload() []byte {
	n := b.cfg.PayloadMin
	if b.cfg.PayloadMax > b.cfg.PayloadMin {
		n += b.rng.IntN(b.cfg.PayloadMax - b.cfg.PayloadMin + 1)
	}
	out := make([]byte, n)
	for i := 0; i < n; i += 8 {
		v := b.rng.Uint64()
		for j := 0; j < 8 && i+j < n; j++ {
			out[i+j] = byte(v >> (8 * j))
		}
	}
	return out
}

// bootID is the boot ID of host i at its current boot.
func bootID(host, boot int) string { return fmt.Sprintf("boot-host-%d-%d", host, boot) }

// layerOf is the churn layer of a relation: slow placement is L1, workload
// placement L2, service dependency L3.
func layerOf(rel catalog.RelationType) catalog.Layer {
	switch rel {
	case catalog.LocatedIn, catalog.RunsOn:
		return catalog.L1
	case catalog.DependsOn:
		return catalog.L3
	default:
		return catalog.L2
	}
}

func entityLayer(t catalog.EntityType) catalog.Layer {
	e, ok := catalog.Default().Entity(t)
	if !ok {
		panic("storetest: unknown entity type " + string(t))
	}
	return e.Layer()
}

// post queues a record. Its arrival is its event time, or later if it is a late
// churn record. The late draw is made only for churn records, and only if lateness
// is on.
func (b *builder) post(rec store.Record, o origin, refresh bool) {
	it := item{arrival: rec.EventTime, rec: rec, origin: o, event: -1, refresh: refresh}
	if o == originChurn {
		it.event = b.event
		if b.cfg.LateProbability > 0 && b.rng.Float64() < b.cfg.LateProbability {
			steps := int(b.cfg.LateMax / time.Second)
			it.arrival = rec.EventTime.Add(time.Duration(1+b.rng.IntN(steps)) * time.Second)
			it.late = true
		}
	}
	b.items = append(b.items, it)
}

func (b *builder) entityRecord(at time.Time, o origin, p lifecycle.Producer, fp identity.Fingerprint, kind lifecycle.Kind, ttl time.Duration, payload []byte) {
	b.bootRecord(at, o, p, fp, kind, ttl, payload, "")
}

// bootRecord is entityRecord for a record that carries a boot ID, which only the
// observation of a host does.
func (b *builder) bootRecord(at time.Time, o origin, p lifecycle.Producer, fp identity.Fingerprint, kind lifecycle.Kind, ttl time.Duration, payload []byte, boot string) {
	r := store.Record{
		Layer: entityLayer(fp.Type()), Subject: store.EntitySubject(fp), Producer: p, EventTime: at, Kind: kind, TTL: ttl,
	}
	if kind == lifecycle.Observe {
		r.Payload, r.Boot = payload, boot
	}
	b.post(r, o, isRefresh(p, kind, ttl))
}

func (b *builder) edgeRecord(at time.Time, o origin, p lifecycle.Producer, from, to identity.Fingerprint, rel catalog.RelationType, kind lifecycle.Kind, ttl time.Duration, payload []byte) {
	spec, ok := catalog.Default().Relation(rel)
	if !ok || spec.Derived() || !spec.Allows(from.Type(), to.Type()) {
		panic(fmt.Sprintf("storetest: %s does not allow %s -> %s", rel, from.Type(), to.Type())) // a bug in this package
	}
	r := store.Record{
		Layer: layerOf(rel), Subject: store.EdgeSubject(from, to, rel), Producer: p, EventTime: at, Kind: kind, TTL: ttl,
	}
	if kind == lifecycle.Observe {
		r.Payload = payload
	}
	b.post(r, o, isRefresh(p, kind, ttl))
}

// isRefresh reports whether a record is a heartbeat refresh a run can absorb.
func isRefresh(p lifecycle.Producer, kind lifecycle.Kind, ttl time.Duration) bool {
	return (p == ProducerNodeCollector || p == ProducerTraces) && kind == lifecycle.Observe && ttl > 0
}

// emitStart asserts the whole cluster at Start, as an initial list would.
func (b *builder) emitStart() {
	at, ttl := b.cfg.Start, b.cfg.heartbeatTTL()
	for _, r := range b.racks {
		b.entityRecord(at, originStart, ProducerFabric, r, lifecycle.Observe, 0, b.payload())
	}
	for i := range b.hosts {
		b.edgeRecord(at, originStart, ProducerFabric, b.hosts[i], b.racks[i%len(b.racks)], catalog.LocatedIn, lifecycle.Observe, 0, b.payload())
		b.hostBeat(at, originStart, i, ttl)
	}
	for j := range b.services {
		b.serviceBeat(at, originStart, j, ttl)
	}
	for p := range b.pods {
		b.createPod(at, originStart, p)
	}
}

// hostBeat has node-collector assert host i, its node, and the node's placement.
func (b *builder) hostBeat(at time.Time, o origin, i int, ttl time.Duration) {
	b.bootRecord(at, o, ProducerNodeCollector, b.hosts[i], lifecycle.Observe, ttl, b.hostPayload[i], bootID(i, b.boot[i]))
	b.entityRecord(at, o, ProducerNodeCollector, b.nodes[i], lifecycle.Observe, ttl, b.nodePayload[i])
	b.edgeRecord(at, o, ProducerNodeCollector, b.nodes[i], b.hosts[i], catalog.RunsOn, lifecycle.Observe, ttl, b.runsOnPayload[i])
}

// serviceBeat has traces assert service j and its dependency on the next one.
func (b *builder) serviceBeat(at time.Time, o origin, j int, ttl time.Duration) {
	b.entityRecord(at, o, ProducerTraces, b.services[j], lifecycle.Observe, ttl, b.servicePayload[j])
	b.edgeRecord(at, o, ProducerTraces, b.services[j], b.services[(j+1)%len(b.services)], catalog.DependsOn, lifecycle.Observe, ttl, b.dependsPayload[j])
}

// emitEvents generates heartbeats and pod churn in event order, up to End.
func (b *builder) emitEvents() {
	c := b.cfg
	var nextBeat, nextChurn time.Time
	never := b.end
	nextBeat, nextChurn = never, never
	if c.HeartbeatInterval > 0 {
		nextBeat = c.Start.Add(c.HeartbeatInterval)
	}
	churnClock := c.Start
	// Gaps are exponential and accumulate on a continuous clock; only the event time
	// itself is truncated to the second, so several events can share a second.
	// Truncating the clock instead would stall it: a gap under a second would never
	// move it on.
	schedule := func() time.Time {
		gap := float64(exponential(b.rng) / (c.ChurnPerMinute / 60) * float64(time.Second))
		if gap >= float64(b.end.Sub(churnClock)) { // also covers an overflowing gap
			return never
		}
		churnClock = churnClock.Add(time.Duration(gap))
		return churnClock.Truncate(time.Second)
	}
	if c.ChurnPerMinute > 0 {
		nextChurn = schedule()
	}
	for {
		switch {
		case nextBeat.Before(b.end) && !nextChurn.Before(nextBeat):
			b.heartbeat(nextBeat)
			nextBeat = nextBeat.Add(c.HeartbeatInterval)
		case nextChurn.Before(b.end):
			b.event++
			b.churn(nextChurn)
			nextChurn = schedule()
		default:
			return
		}
	}
}

// heartbeat is one round of refreshes at the instant at. Before a host's
// refresh it may reboot, and a reboot may leave a clone reporting the old boot.
func (b *builder) heartbeat(at time.Time) {
	c := b.cfg
	ttl := c.heartbeatTTL()
	for i := range b.hosts {
		if c.RebootProbability > 0 && b.rng.Float64() < c.RebootProbability {
			old := b.boot[i]
			b.boot[i]++
			if c.CloneProbability > 0 && b.rng.Float64() < c.CloneProbability {
				seen := at.Add((c.HeartbeatInterval / 2).Truncate(time.Second))
				b.bootRecord(seen, originClone, ProducerClone, b.hosts[i], lifecycle.Observe, c.HeartbeatInterval, b.hostPayload[i], bootID(i, old))
			}
		}
		b.hostBeat(at, originHeartbeat, i, ttl)
	}
	for j := range b.services {
		b.serviceBeat(at, originHeartbeat, j, ttl)
	}
}

// createPod asserts a pod, its container, their relation and the pod's
// placement on a node drawn uniformly.
func (b *builder) createPod(at time.Time, o origin, p int) {
	node := b.rng.IntN(len(b.nodes))
	b.podNode[p] = node
	b.entityRecord(at, o, ProducerK8sObjects, b.pods[p], lifecycle.Observe, 0, b.payload())
	b.entityRecord(at, o, ProducerK8sObjects, b.containers[p], lifecycle.Observe, 0, b.payload())
	b.edgeRecord(at, o, ProducerK8sObjects, b.containers[p], b.pods[p], catalog.PartOf, lifecycle.Observe, 0, b.payload())
	b.place(at, o, p, node)
}

// place has k8sobjects assert pod p's placement on node, and the kubelet, with
// ConfirmProbability, assert it too. The kubelet's draw is made only if the
// probability is above zero.
func (b *builder) place(at time.Time, o origin, p, node int) {
	b.edgeRecord(at, o, ProducerK8sObjects, b.pods[p], b.nodes[node], catalog.ScheduledOn, lifecycle.Observe, 0, b.payload())
	if b.cfg.ConfirmProbability <= 0 || b.rng.Float64() >= b.cfg.ConfirmProbability {
		return
	}
	b.edgeRecord(at, o, ProducerKubelet, b.pods[p], b.nodes[node], catalog.ScheduledOn, lifecycle.Observe, b.cfg.ConfirmTTL, b.payload())
	b.confirmed[p] = true
}

// unplace ends pod p's placement on node: k8sobjects deletes its edge, and the
// kubelet, if it confirmed the placement, withdraws its assertion half the time
// and otherwise leaves it to expire after ConfirmTTL.
func (b *builder) unplace(at time.Time, p, node int) {
	b.edgeRecord(at, originChurn, ProducerK8sObjects, b.pods[p], b.nodes[node], catalog.ScheduledOn, lifecycle.Delete, 0, nil)
	if !b.confirmed[p] {
		return
	}
	b.confirmed[p] = false
	b.confirmEnds++
	if b.rng.IntN(2) == 0 {
		b.edgeRecord(at, originChurn, ProducerKubelet, b.pods[p], b.nodes[node], catalog.ScheduledOn, lifecycle.Delete, 0, nil)
	}
}

// churn is one pod event at the instant at: a pod is picked uniformly, and a pod
// that exists is rescheduled or deleted, half the time each, while one that does
// not is created again under the same identity with new payloads.
func (b *builder) churn(at time.Time) {
	p := b.rng.IntN(len(b.pods))
	switch {
	case b.podNode[p] < 0:
		b.createPod(at, originChurn, p)
	case b.rng.IntN(2) == 0:
		old := b.podNode[p]
		node := old
		if n := len(b.nodes); n > 1 {
			node = b.rng.IntN(n - 1)
			if node >= old {
				node++
			}
		}
		b.unplace(at, p, old)
		b.podNode[p] = node
		b.place(at, originChurn, p, node)
	default:
		b.entityRecord(at, originChurn, ProducerK8sObjects, b.pods[p], lifecycle.Delete, 0, nil)
		b.entityRecord(at, originChurn, ProducerK8sObjects, b.containers[p], lifecycle.Delete, 0, nil)
		b.edgeRecord(at, originChurn, ProducerK8sObjects, b.containers[p], b.pods[p], catalog.PartOf, lifecycle.Delete, 0, nil)
		b.unplace(at, p, b.podNode[p])
		b.podNode[p] = -1
	}
}

// exponential draws from the exponential distribution with mean 1, using the
// four basic operations of IEEE arithmetic and nothing else: no math.Log, no
// math.Exp, and no rand.ExpFloat64, whose slow path calls them. Those can differ
// in the last bit between architectures (some have assembly for them), and the
// stream must be the same on all of them. Each product is converted to float64
// explicitly, because the Go specification lets the compiler fuse a multiply and
// an add into one operation, which rounds once instead of twice and differs
// between architectures, and an explicit conversion forbids that.
func exponential(rng *rand.Rand) float64 {
	return negLog(rng.Uint64()>>11 + 1) // k in [1, 2^53]
}

// negLog is -ln(k / 2^53) for k in [1, 2^53]. It writes k / 2^53 as m * 2^(e-53)
// with m in [0.5, 1), exact because both are powers of two, so that
// -ln(k / 2^53) = (53-e) ln 2 - ln m. And ln m = 2 atanh(s) with s = (m-1)/(m+1),
// which is in [-1/3, 0], summed as the series s + s^3/3 + s^5/5 + ... a fixed
// number of terms, enough that the next one is below the rounding of the sum.
func negLog(k uint64) float64 {
	const ln2 = math.Ln2 // rounded to float64 when it is used
	e := bits.Len64(k)
	m := float64(k) / float64(uint64(1)<<e)
	s := (m - 1) / (m + 1)
	s2 := float64(s * s)
	sum, pow := 0.0, s
	for n := 1; n <= 2*seriesTerms; n += 2 {
		sum = float64(sum + float64(pow/float64(n)))
		pow = float64(pow * s2)
	}
	// For k = 2^53 the two terms cancel, and rounding may leave a tiny negative.
	return max(0, float64(float64(53-e)*ln2)-float64(2*sum))
}

// seriesTerms is the number of terms of the series of ln m. The terms shrink by
// a factor of at least 9 each, so the 30th is below 1e-29.
const seriesTerms = 30
