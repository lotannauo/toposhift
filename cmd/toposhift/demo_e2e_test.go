package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/lotannauo/toposhift/internal/catalog"
	"github.com/lotannauo/toposhift/internal/demo"
	"github.com/lotannauo/toposhift/internal/identity"
)

// The end-to-end test of the demo: toposhift demo is started on a loopback port
// and asked a name, a node's neighbours, its blast radius, a pod's co-residents,
// the change over the eviction, an address claim seen from both ends, and five
// questions about the services (a name, three blast radii and a change), over the
// MCP route, with raw JSON-RPC and nothing else.
//
// What each answer must be is computed from the generated world alone (the
// oracle below): its pods, nodes, workloads and instants, and the ids the
// identity resolver gives their identifying attributes. The oracle reads no store,
// no translator and no tool type. The live answers are projected into the words
// of the golden file (names in place of ids, labels, derived flags, counts, the
// bounds of every listed span), and the projection is compared with the oracle's
// first and with the file second. The file is written, with -update, only when the
// oracle agreed and nothing else failed.

var update = flag.Bool("update", false, "rewrite testdata/demo_e2e.golden.json; only after the oracle agrees, and never to make a test pass")

const e2eGolden = "testdata/demo_e2e.golden.json"

const (
	// seenAfter is how long after a change the collectors report it: the watch
	// event of the objects receiver and the delete event of the cluster receiver are
	// both sent a second after (emit.go), and a store's span ends at the instant it
	// was seen going.
	seenAfter = time.Second
	// margin is how far an instant that is asked about is from every event of an
	// entity that can appear in its answer, so that no answer depends on which side
	// of an event it falls.
	margin = 5 * time.Minute
)

// forever is later than the history, for a pod that is never deleted.
var forever = time.Date(2100, 1, 1, 0, 0, 0, 0, time.UTC)

func fmtTime(t time.Time) string { return t.UTC().Format(time.RFC3339Nano) }

// iv is the half-open interval [from, to).
type iv struct{ from, to time.Time }

func (a iv) and(b iv) iv {
	if b.from.After(a.from) {
		a.from = b.from
	}
	if b.to.Before(a.to) {
		a.to = b.to
	}
	return a
}

func (a iv) empty() bool { return !a.from.Before(a.to) }

func (a iv) has(t time.Time) bool { return !t.Before(a.from) && t.Before(a.to) }

// span writes an interval as the answers do, with the reason it ended, if it did.
func span(a iv, reason string) string {
	s := fmtTime(a.from) + ".." + fmtTime(a.to)
	if reason != "" {
		s += " (" + reason + ")"
	}
	return s
}

// classOf is the class and the number of transitions of something that lasts
// over reach inside the window win.
func classOf(reach, win iv) (class string, transitions int) {
	switch {
	case !reach.from.After(win.from) && !reach.to.Before(win.to):
		return "stable", 0
	case !reach.from.After(win.from):
		return "disappeared", 1
	case !reach.to.Before(win.to):
		return "appeared", 1
	}
	return "transient", 2
}

// reachEnd is the reason a stretch inside win ended, if it did before win does.
func reachEnd(reach, win iv) string {
	if reach.to.Before(win.to) {
		return "reach_ended"
	}
	return ""
}

// view is one answer in the words of the golden file. Ids are replaced by what
// the world calls their entity; lists are sorted, except where the tool's order
// is part of what it promises.
type view struct {
	Tool    string              `json:"tool"`
	Args    map[string]any      `json:"args"`
	Policy  map[string]any      `json:"policy,omitempty"`
	Derived bool                `json:"derived"`
	Meta    map[string]any      `json:"meta,omitempty"`
	Counts  map[string]any      `json:"counts,omitempty"`
	Lists   map[string][]string `json:"lists"`
}

// ---- the oracle ----

// refInfo is what the world says an id is.
type refInfo struct {
	typ, name, ns string
	// pod is the pod of a pod and of a container.
	pod *demo.Pod
	// unnamed is set for what the tools never name: a host, an endpoint and a
	// service instance, whose description is its version.
	unnamed bool
	// from is the producer whose description names the entity when it is not the
	// objects receiver's: a service is described by both translators of the trace
	// collector's metrics, which no rank orders, so the smaller name wins.
	from string
}

// oracle holds the world and the ids of everything in it.
type oracle struct {
	t     testing.TB
	w     *demo.World
	story demo.Story
	res   *identity.Resolver
	refs  map[string]refInfo
	byID  map[string]*demo.Pod
	byNm  map[string]*demo.Pod
	// nodeID, hostID and containerID are the ids by index.
	nodeID, hostID []string
	podID          map[*demo.Pod]string
	containerID    map[*demo.Pod]string
	// svcID are the services' ids by "namespace/name", and instID the ids of the
	// service instances the pods are.
	svcID  map[string]string
	instID map[*demo.Pod]string
}

// fp is the id the tools print for the entity of the type with the attribute.
// It goes through the identity resolver on the world's own attribute, as the
// world does for the ids it names, and through no store.
func (o *oracle) fp(typ catalog.EntityType, attrs ...identity.Attr) string {
	o.t.Helper()
	id, err := o.res.Resolve(typ, attrs)
	if err != nil {
		o.t.Fatalf("the identity of a %s: %v", typ, err)
	}
	return id.Fingerprint().String()
}

func newOracle(t testing.TB) *oracle {
	w := demo.NewWorld(demo.DefaultSeed)
	o := &oracle{
		t: t, w: w, story: w.Story(), res: identity.NewResolver(catalog.Default()),
		refs: map[string]refInfo{}, byID: map[string]*demo.Pod{}, byNm: map[string]*demo.Pod{},
		podID: map[*demo.Pod]string{}, containerID: map[*demo.Pod]string{},
		svcID: map[string]string{}, instID: map[*demo.Pod]string{},
	}
	for _, n := range w.Nodes {
		id := o.fp(catalog.K8sNode, identity.Attr{Key: catalog.K8sNodeUID, Value: n.UID})
		o.nodeID = append(o.nodeID, id)
		o.refs[id] = refInfo{typ: "k8s.node", name: n.Name}
	}
	for i, m := range w.Machines {
		id := o.fp(catalog.Host, identity.Attr{Key: catalog.HostID, Value: m.HostID})
		o.hostID = append(o.hostID, id)
		o.refs[id] = refInfo{typ: "host", name: "machine-" + strconv.Itoa(i+1), unnamed: true}
	}
	for _, p := range w.Pods {
		wl := w.Workloads[p.Workload]
		id := o.fp(catalog.K8sPod, identity.Attr{Key: catalog.K8sPodUID, Value: p.UID})
		cid := o.fp(catalog.Container, identity.Attr{Key: catalog.ContainerID, Value: p.ContainerID})
		o.podID[p], o.containerID[p] = id, cid
		o.byID[id], o.byNm[p.Name] = p, p
		o.refs[id] = refInfo{typ: "k8s.pod", name: p.Name, ns: wl.Namespace, pod: p}
		o.refs[cid] = refInfo{typ: "container", name: wl.Name, pod: p}
	}
	// A service is its namespace and name, and an instance is a pod of one: the
	// identity is made of the attributes the translators read, and of nothing else.
	for _, wl := range w.Workloads {
		if wl.Service == "" {
			continue
		}
		id := o.fp(catalog.Service,
			identity.Attr{Key: catalog.ServiceNamespace, Value: wl.Namespace}, identity.Attr{Key: catalog.ServiceName, Value: wl.Service})
		o.svcID[wl.Namespace+"/"+wl.Service] = id
		o.refs[id] = refInfo{typ: "service", name: wl.Service, ns: wl.Namespace, from: "spancount/" + demo.ClusterName}
	}
	for _, p := range w.Pods {
		in, ok := w.Instance(p)
		if !ok {
			continue
		}
		id := o.fp(catalog.ServiceInstance,
			identity.Attr{Key: catalog.ServiceNamespace, Value: in.Namespace}, identity.Attr{Key: catalog.ServiceName, Value: in.Service},
			identity.Attr{Key: catalog.ServiceInstanceID, Value: in.ID})
		o.instID[p] = id
		o.refs[id] = refInfo{typ: "service.instance", name: in.ID, unnamed: true}
	}
	return o
}

// The trace collector, as the documentation of the two translators and of the demo
// describe it: its span counts come once a minute, 50 seconds past the anchor's
// minute, 360 times in the window, and an instance is part of its pod from its first
// one to its last plus the expiry. Its service graph flushes 20 seconds past, as
// many times.
const (
	spanPast  = 50 * time.Second
	flushPast = 20 * time.Second
	flushes   = 360
	// connectorIdle is how long the service graph connector keeps a series that is
	// not called: its own removal rule, not toposhift's dependency expiry.
	connectorIdle = 15 * time.Minute
)

// sends says whether the pod's instance sends a span count at s: its container
// runs, and its node is ready, because a node whose runtime is down runs nothing.
func (o *oracle) sends(p *demo.Pod, s time.Time) bool {
	if s.Before(p.Started) || (!p.Deleted.IsZero() && !s.Before(p.Deleted)) {
		return false
	}
	for _, out := range o.w.Outages {
		if out.Node == p.Node && !s.Before(out.From) && s.Before(out.To) {
			return false
		}
	}
	return true
}

// sightings are the first and the last span count of the pod's instance, if it
// has an instance that sends any.
func (o *oracle) sightings(p *demo.Pod) (first, last time.Time, ok bool) {
	if _, isInstance := o.instID[p]; !isInstance {
		return
	}
	for k := range flushes {
		s := o.story.Anchor.Add(spanPast + time.Duration(k)*time.Minute)
		if o.sends(p, s) {
			if !ok {
				first = s
			}
			last, ok = s, true
		}
	}
	return
}

// instLives is when the pod's instance exists: from its first sighting to its last
// plus the expiry. bridge is when it is part of its pod, which is that, and only
// while the pod exists.
func (o *oracle) instLives(p *demo.Pod) (iv, bool) {
	first, last, ok := o.sightings(p)
	return iv{first, last.Add(demo.Expiry)}, ok
}

func (o *oracle) bridge(p *demo.Pod) (iv, bool) {
	in, ok := o.instLives(p)
	b := in.and(o.lives(p))
	return b, ok && !b.empty()
}

// endpoint is the id of the address in the realm the demo's cluster gives its
// private addresses, and the name the golden file calls it.
func (o *oracle) endpoint(ip string) (id, name string) {
	const realm = "k8s/" + demo.ClusterName
	id = o.fp(catalog.NetworkEndpoint,
		identity.Attr{Key: catalog.EndpointRealm, Value: realm},
		identity.Attr{Key: catalog.EndpointAddress, Value: ip})
	name = ip + " in " + realm
	o.refs[id] = refInfo{typ: "topo.endpoint", name: name, unnamed: true}
	return id, name
}

func (o *oracle) gone(p *demo.Pod) time.Time {
	if p.Deleted.IsZero() {
		return forever
	}
	return p.Deleted.Add(seenAfter)
}

// lives is when the pod exists, running is when its container does and the pod
// holds its address, and placed is when it is scheduled on its node.
func (o *oracle) lives(p *demo.Pod) iv   { return iv{p.Created, o.gone(p)} }
func (o *oracle) running(p *demo.Pod) iv { return iv{p.Started, o.gone(p)} }
func (o *oracle) placed(p *demo.Pod) iv  { return iv{p.Scheduled, o.gone(p)} }

// failed is the index of the node that fails.
func (o *oracle) failed() int { return o.w.Outages[0].Node }

// ref writes the entity of the id the way a listing of the window win names it: its
// type and name, its namespace if it has one, and "(unnamed)" when no name is
// printed. A name is read from the entity's description at an end of the window, so
// it is not printed for a pod or container that is at neither.
func (o *oracle) ref(id string, win iv) string {
	r, ok := o.refs[id]
	if !ok {
		o.t.Fatalf("the oracle has no entity %s", id)
	}
	return r.typ + " " + o.show(r, win)
}

func (o *oracle) show(r refInfo, win iv) string {
	if r.unnamed {
		return r.name + " (unnamed)"
	}
	if r.pod != nil {
		live := o.lives(r.pod)
		if r.typ == "container" {
			live = o.running(r.pod)
		}
		last := win.to.Add(-time.Nanosecond)
		if !live.has(win.from) && !live.has(last) {
			return r.name + " (unnamed)"
		}
	}
	if r.ns != "" {
		return r.name + " [" + r.ns + "]"
	}
	return r.name
}

// label is the name the golden file uses for an id in a request.
func (o *oracle) label(id string) string {
	r := o.refs[id]
	return "<" + r.typ + " " + r.name + ">"
}

// argLabels is a request with the ids it names replaced by labels.
func (o *oracle) argLabels(args map[string]any) map[string]any {
	out := map[string]any{}
	for k, v := range args {
		switch x := v.(type) {
		case string:
			if _, ok := o.refs[x]; ok {
				v = o.label(x)
			}
		case []string:
			l := make([]string, len(x))
			for i, s := range x {
				l[i] = s
				if _, ok := o.refs[s]; ok {
					l[i] = o.label(s)
				}
			}
			v = l
		}
		out[k] = v
	}
	return out
}

// checkMargin fails the test if the instant is within margin of an event of one of
// the pods, or of one of the extra instants. An answer that depended on which side
// of an event an instant fell would otherwise change with a change of the script,
// not with a change of the code.
func (o *oracle) checkMargin(question, what string, at time.Time, pods []*demo.Pod, extra ...time.Time) {
	o.t.Helper()
	near := func(name string, e time.Time) {
		if e.IsZero() {
			return
		}
		d := at.Sub(e)
		if d < 0 {
			d = -d
		}
		if d < margin {
			o.t.Errorf("%s: %s %s is %s from %s, under the margin of %s; pick the nearest whole minute that is not", question, what, fmtTime(at), d, name, margin)
		}
	}
	for _, p := range pods {
		near(p.Name+" created", p.Created)
		near(p.Name+" scheduled", p.Scheduled)
		near(p.Name+" started", p.Started)
		near(p.Name+" finished", p.Finished)
		near(p.Name+" deleted", p.Deleted)
		if !p.Deleted.IsZero() {
			near(p.Name+" seen deleted", p.Deleted.Add(seenAfter))
		}
	}
	for _, e := range extra {
		near("an event", e)
	}
}

// nodeClaims and podClaims are how many addresses the world gives the entity, one
// edge to an endpoint each: a node holds its InternalIP (its Hostname is a name,
// not an address), and a pod holds its IP from the instant its container runs
// until it is gone. Each claim is to an endpoint, whose id the request must name
// to see it listed.
func nodeClaims(n demo.Node) int {
	if n.IP == "" {
		return 0
	}
	return 1
}

func (o *oracle) podClaims(p *demo.Pod, win iv) int {
	if p.IP == "" || o.running(p).and(win).empty() {
		return 0
	}
	return 1
}

func (o *oracle) podsOnNode(node int) []*demo.Pod {
	var out []*demo.Pod
	for _, p := range o.w.Pods {
		if p.Node == node {
			out = append(out, p)
		}
	}
	return out
}

// sorted returns a sorted copy that is never nil.
func sorted(xs []string) []string {
	out := slices.Clone(xs)
	if out == nil {
		out = []string{}
	}
	slices.Sort(out)
	return out
}

func row(parts ...string) string { return strings.Join(parts, " | ") }

const (
	existenceLabels = "asserted/derived/existence"
	reachLabels     = "derived/derived/blast_radius.reach"
)

// ---- the questions ----

// questions are the instants and windows asked about. All are whole minutes and
// derive from the story, so that they move with the script and the oracle's
// margin check says when one lands on an event.
type questions struct {
	i1       time.Time // before the node fails
	w1       iv        // the node fails, and its pods are evicted
	coFrom   time.Time
	coTo     time.Time
	e0, e1   time.Time // the change over the eviction
	story    demo.Story
	checkout demo.PodRef

	// The questions about the services: a name at svcAt; the blast radius of ledger
	// over the window of its failure, and of legacy-pay while checkout still calls it
	// and after it stopped; and the change of checkout while it moves from one callee
	// to the other.
	svcAt                      time.Time
	fault, legacyOn, legacyOff iv
	moveFrom, moveTo           time.Time
}

func askedAbout(o *oracle) questions {
	s := o.story
	return questions{
		i1:     s.NotReady.Add(-15 * time.Minute),
		w1:     iv{s.NotReady.Add(-5 * time.Minute), s.NotReady.Add(30 * time.Minute)},
		coFrom: s.Anchor, coTo: s.NotReady.Add(-5 * time.Minute),
		e0: s.Evict.Add(-10 * time.Minute), e1: s.Evict.Add(10 * time.Minute),
		story: s, checkout: s.Checkout,
		svcAt:     s.Anchor.Add(time.Hour),
		fault:     iv{s.FaultFrom, s.FaultTo},
		legacyOn:  iv{s.Anchor.Add(time.Hour), s.Anchor.Add(time.Hour + 30*time.Minute)},
		legacyOff: iv{s.Anchor.Add(3 * time.Hour), s.Anchor.Add(3*time.Hour + 30*time.Minute)},
		moveFrom:  s.Migrate.Add(-5 * time.Minute), moveTo: s.Migrate.Add(35 * time.Minute),
	}
}

func (q questions) findArgs() map[string]any {
	return map[string]any{"type": "k8s.pod", "name": "checkout", "at": q.i1.Format(time.RFC3339)}
}

func (q questions) neighborsArgs() map[string]any {
	return map[string]any{"entity": q.story.Failed.ID, "at": q.i1.Format(time.RFC3339)}
}

func (q questions) blastArgs() map[string]any {
	return map[string]any{"entity": q.story.Failed.ID, "from": q.w1.from.Format(time.RFC3339), "to": q.w1.to.Format(time.RFC3339)}
}

func (q questions) coArgs() map[string]any {
	return map[string]any{"pod": q.checkout.ID, "from": q.coFrom.Format(time.RFC3339), "to": q.coTo.Format(time.RFC3339)}
}

func (q questions) changedArgs() map[string]any {
	var scope []string
	for i, v := range q.story.Evicted {
		scope = append(scope, v.ID, q.story.Replacements[i].ID)
	}
	return map[string]any{"scope": scope, "from": q.e0.Format(time.RFC3339), "to": q.e1.Format(time.RFC3339)}
}

func (q questions) podNeighborsArgs() map[string]any {
	return map[string]any{"entity": q.checkout.ID, "at": q.i1.Format(time.RFC3339)}
}

func (q questions) resolveArgs(ip string) map[string]any {
	return map[string]any{
		"type": "topo.endpoint",
		"attributes": map[string]any{
			"topo.endpoint.realm":   "k8s/" + demo.ClusterName,
			"topo.endpoint.address": ip,
		},
		"at": q.i1.Format(time.RFC3339),
	}
}

func (q questions) findServicesArgs() map[string]any {
	return map[string]any{"type": "service", "name": "pay", "at": q.svcAt.Format(time.RFC3339)}
}

func (q questions) blastServiceArgs(id string, win iv) map[string]any {
	return map[string]any{"entity": id, "from": win.from.Format(time.RFC3339), "to": win.to.Format(time.RFC3339)}
}

func (q questions) changedServiceArgs() map[string]any {
	return map[string]any{
		"scope": []string{q.story.Service("shop", "checkout").ID},
		"from":  q.moveFrom.Format(time.RFC3339), "to": q.moveTo.Format(time.RFC3339),
	}
}

func (q questions) endpointNeighborsArgs(id string) map[string]any {
	return map[string]any{"entity": id, "at": q.i1.Format(time.RFC3339)}
}

// The oracle's answers. Each reads the world and the instants of the question and
// nothing else. A pod is on one node for the whole of its life, so what a window
// holds of an edge is the pod's interval clipped to it.

func (o *oracle) wantFind(q questions) view {
	at := q.i1
	win := iv{at, at.Add(time.Nanosecond)}
	o.checkMargin("find", "the instant", at, o.pods(func(p *demo.Pod) bool { return strings.Contains(p.Name, "checkout") }))
	var ps []*demo.Pod
	for _, p := range o.w.Pods {
		if strings.Contains(strings.ToLower(p.Name), "checkout") && o.lives(p).has(at) {
			ps = append(ps, p)
		}
	}
	slices.SortFunc(ps, func(a, b *demo.Pod) int { return strings.Compare(a.Name, b.Name) })
	var rows []string
	for _, p := range ps {
		reach := o.lives(p).and(win)
		class, n := classOf(reach, win)
		rows = append(rows, row(o.ref(o.podID[p], win), existenceLabels, class+" "+strconv.Itoa(n), span(reach, "")))
	}
	return view{
		Tool: "find_entities", Args: o.argLabels(q.findArgs()), Derived: true,
		Meta: map[string]any{"type": "k8s.pod", "window": map[string]any{"from": fmtTime(win.from), "to": fmtTime(win.to)}, "complete": true, "next_after": nil},
		Counts: map[string]any{
			"checked": len(o.w.Pods), "matched": len(ps), "not_printed": 0, "quarantined": 0, "retained": 0, "undescribed": 0,
		},
		Lists: map[string][]string{"matches": rows},
	}
}

func (o *oracle) pods(keep func(*demo.Pod) bool) []*demo.Pod {
	var out []*demo.Pod
	for _, p := range o.w.Pods {
		if keep(p) {
			out = append(out, p)
		}
	}
	return out
}

func (o *oracle) wantNeighbors(q questions) view {
	at := q.i1
	win := iv{at, at.Add(time.Nanosecond)}
	failed := o.failed()
	on := o.podsOnNode(failed)
	o.checkMargin("neighbors", "the instant", at, on, o.w.Outages[0].From, o.w.Outages[0].To)
	node := o.w.Nodes[failed]
	rows := []string{row("forward", "runs_on", "L1", o.ref(o.hostID[node.Machine], win), existenceLabels)}
	for _, p := range on {
		if o.placed(p).has(at) {
			rows = append(rows, row("reverse", "scheduled_on", "L2", o.ref(o.podID[p], win), existenceLabels))
		}
	}
	return view{
		Tool: "neighbors_at", Args: o.argLabels(q.neighborsArgs()), Derived: true,
		Meta: map[string]any{"at": fmtTime(at), "direction": "both", "entity": o.ref(o.nodeID[failed], win)},
		Counts: map[string]any{
			"edges": len(rows), "edges_shown": len(rows),
			"withheld": map[string]any{"edges": nodeClaims(node), "quarantined": 0},
		},
		Lists: map[string][]string{"edges": sorted(rows), "quarantined": {}},
	}
}

func (o *oracle) wantBlast(q questions) view {
	win := q.w1
	failed := o.failed()
	on := o.podsOnNode(failed)
	events := []time.Time{o.w.Outages[0].From, o.w.Outages[0].To}
	for _, p := range on {
		if in, ok := o.instLives(p); ok {
			events = append(events, in.from, in.to)
		}
	}
	o.checkMargin("blast_radius", "the window start", win.from, on, events...)
	o.checkMargin("blast_radius", "the window end", win.to, on, events...)
	var rows []string
	for _, p := range on {
		reach := o.placed(p).and(win)
		if reach.empty() {
			continue
		}
		class, n := classOf(reach, win)
		rows = append(rows, row("depth 1", o.ref(o.podID[p], win), class+" "+strconv.Itoa(n), reachLabels, "reach "+span(reach, reachEnd(reach, win)), "via scheduled_on "+o.ref(o.nodeID[failed], win)))
		// The container is part of the pod from the pod's start, and is reached
		// where both the pod and the container exist.
		creach := reach.and(o.running(p))
		if !creach.empty() {
			cclass, cn := classOf(creach, win)
			rows = append(rows, row("depth 2", o.ref(o.containerID[p], win), cclass+" "+strconv.Itoa(cn), reachLabels, "reach "+span(creach, reachEnd(creach, win)), "via part_of "+o.ref(o.podID[p], win)))
		}
		// The service instance of a pod is part of it, and a node's failure reaches it
		// through the pod, where both the pod and the bridge exist. It stops there:
		// an instance of a service is never followed to the service.
		if b, ok := o.bridge(p); ok {
			if ireach := reach.and(b); !ireach.empty() {
				iclass, in := classOf(ireach, win)
				rows = append(rows, row("depth 2", o.ref(o.instID[p], win), iclass+" "+strconv.Itoa(in), reachLabels, "reach "+span(ireach, reachEnd(ireach, win)), "via part_of "+o.ref(o.podID[p], win)))
			}
		}
	}
	return view{
		Tool: "blast_radius", Args: o.argLabels(q.blastArgs()), Derived: true,
		Meta: map[string]any{
			"seed": o.ref(o.nodeID[failed], win), "window": map[string]any{"from": fmtTime(win.from), "to": fmtTime(win.to)}, "depth_limit": 6,
			"truncation": map[string]any{"depth_cut": 0, "frontier_cuts": []any{}, "incomplete": false, "members_unlisted": 0},
		},
		Counts: map[string]any{"members": len(rows), "members_shown": len(rows), "quarantined": 0, "unattributed": 0},
		Lists:  map[string][]string{"members": sorted(rows), "quarantined": {}, "unattributed": {}},
	}
}

func (o *oracle) wantCoResidents(q questions) view {
	win := iv{q.coFrom, q.coTo}
	seed := o.byID[q.checkout.ID]
	var all []*demo.Pod
	for _, p := range o.w.Pods {
		if o.w.Nodes[p.Node].Machine == o.w.Nodes[seed.Node].Machine {
			all = append(all, p)
		}
	}
	// The window starts at the anchor, which is exempt; its end is not.
	o.checkMargin("co_residents", "the window end", win.to, all)
	seedPlaced := o.placed(seed).and(win)
	var rows []string
	for _, p := range all {
		if p == seed {
			continue
		}
		overlap := o.placed(p).and(seedPlaced)
		if overlap.empty() {
			continue
		}
		mech, via, nodes := "shared_node", o.nodeID[seed.Node], []string{o.w.Nodes[seed.Node].Name}
		if p.Node != seed.Node {
			mech, via = "shared_host", o.hostID[o.w.Nodes[seed.Node].Machine]
			nodes = append(nodes, o.w.Nodes[p.Node].Name)
		}
		slices.Sort(nodes)
		class, n := classOf(overlap, win)
		rows = append(rows, row(mech, o.ref(o.podID[p], win), class+" "+strconv.Itoa(n), "derived/derived/co_residency."+mech,
			"via "+o.ref(via, win), "nodes "+strings.Join(nodes, ","), "overlap "+span(overlap, reachEnd(overlap, win))))
	}
	placement := []string{row(o.ref(o.podID[seed], win)+" scheduled_on "+o.ref(o.nodeID[seed.Node], win), existenceLabels, span(seedPlaced, reachEnd(seedPlaced, win)))}
	return view{
		Tool: "co_residents", Args: o.argLabels(q.coArgs()), Derived: true,
		Meta: map[string]any{
			"seed": o.ref(o.podID[seed], win), "window": map[string]any{"from": fmtTime(win.from), "to": fmtTime(win.to)}, "node_only": false,
			"truncation": map[string]any{"hosts_not_read": 0, "incomplete": false, "nodes_not_read": 0, "per_via_dropped": 0, "results_dropped": 0, "suppressed_dropped": 0},
		},
		Counts: map[string]any{"co_residents": len(rows), "co_residents_shown": len(rows), "quarantined": 0, "suppressed": 0, "unattributed": 0},
		Lists:  map[string][]string{"co_residents": sorted(rows), "placement": placement, "quarantined": {}, "suppressed": {}, "unattributed": {}},
	}
}

func (o *oracle) wantChanged(q questions) view {
	// The window read is [e0, e1+1ns): both instants are inside it.
	win := iv{q.e0, q.e1.Add(time.Nanosecond)}
	var scope []string
	var inScope []*demo.Pod
	for i, v := range q.story.Evicted {
		scope = append(scope, o.ref(v.ID, win), o.ref(q.story.Replacements[i].ID, win))
		inScope = append(inScope, o.byID[v.ID], o.byID[q.story.Replacements[i].ID])
	}
	// The ends of the instances of the evicted pods are not listed: a bridge ends
	// where its pod does, and the instance's own end touches no entity of the scope.
	// The beginnings of the replacements' bridges are, and are checked after they
	// are known.
	o.checkMargin("what_changed", "the first instant", q.e0, inScope)
	o.checkMargin("what_changed", "the second instant", q.e1, inScope)

	var created, deleted []string
	nCreated, nDeleted, withheldCreated, withheldDeleted := 0, 0, 0, 0
	// instCreated and instDeleted count the bridges of service instances among them,
	// and bridgeEvents are the instants they begin at, which are listed.
	instCreated, instDeleted := 0, 0
	var bridgeEvents []time.Time
	for i, v := range q.story.Evicted {
		old, repl := o.byID[v.ID], o.byID[q.story.Replacements[i].ID]
		// The evicted pod is gone inside the window, its container and its place
		// on the node with it.
		end := o.gone(old)
		gone := func(a iv) string { return span(a.and(win), "producer") }
		deleted = append(deleted,
			row("entity", o.ref(o.podID[old], win), "asserted/derived/diff.deleted", "disappeared", gone(o.lives(old)), "deleted_at "+fmtTime(end)),
			row("relation", o.ref(o.containerID[old], win)+" part_of "+o.ref(o.podID[old], win), "asserted/derived/diff.deleted", "disappeared", gone(o.running(old)), "deleted_at "+fmtTime(end)),
			row("relation", o.ref(o.podID[old], win)+" scheduled_on "+o.ref(o.nodeID[old.Node], win), "asserted/derived/diff.deleted", "disappeared", gone(o.placed(old)), "deleted_at "+fmtTime(end)))
		// The pod's service instance was part of it, and the bridge ends where the pod
		// does: the edge is cut by its endpoint, so the end is the cascade's.
		if b, ok := o.bridge(old); ok && b.and(win).to.Before(win.to) {
			cut := b.and(win)
			deleted = append(deleted, row("relation", o.ref(o.instID[old], win)+" part_of "+o.ref(o.podID[old], win), "asserted/derived/diff.deleted", "disappeared", span(cut, "cascade"), "deleted_at "+fmtTime(cut.to)))
			nDeleted++
			instDeleted++
		}
		// Its replacement appears and stays.
		stay := func(a iv) string { return span(a.and(win), "") }
		created = append(created,
			row("entity", o.ref(o.podID[repl], win), "asserted/derived/diff.created", "appeared", stay(o.lives(repl))),
			row("relation", o.ref(o.containerID[repl], win)+" part_of "+o.ref(o.podID[repl], win), "asserted/derived/diff.created", "appeared", stay(o.running(repl))),
			row("relation", o.ref(o.podID[repl], win)+" scheduled_on "+o.ref(o.nodeID[repl.Node], win), "asserted/derived/diff.created", "appeared", stay(o.placed(repl))))
		// Its replacement's instance is part of it from its first span count.
		if b, ok := o.bridge(repl); ok && b.and(win).from.After(win.from) {
			created = append(created, row("relation", o.ref(o.instID[repl], win)+" part_of "+o.ref(o.podID[repl], win), "asserted/derived/diff.created", "appeared", stay(b)))
			nCreated++
			instCreated++
			first, _, _ := o.sightings(repl)
			bridgeEvents = append(bridgeEvents, first)
		}
		nCreated += 3
		nDeleted += 3
		// Each pod's address claim is an edge to an endpoint the scope does not
		// name: withheld and counted.
		withheldDeleted += o.podClaims(old, win)
		withheldCreated += o.podClaims(repl, win)
	}
	o.checkMargin("what_changed", "the first instant", q.e0, nil, bridgeEvents...)
	o.checkMargin("what_changed", "the second instant", q.e1, nil, bridgeEvents...)
	events := nCreated + nDeleted + withheldCreated + withheldDeleted
	zero := map[string]any{"entities": 0, "relations": 0}
	return view{
		Tool: "what_changed", Args: o.argLabels(q.changedArgs()), Derived: true,
		Meta: map[string]any{"window": map[string]any{"from": fmtTime(win.from), "to": fmtTime(win.to)}},
		Counts: map[string]any{
			"changed":     zero,
			"transient":   zero,
			"quarantined": zero,
			"created":     map[string]any{"entities": len(q.story.Replacements), "relations": 2*len(q.story.Replacements) + instCreated + withheldCreated},
			"deleted":     map[string]any{"entities": len(q.story.Evicted), "relations": 2*len(q.story.Evicted) + instDeleted + withheldDeleted},
			"events":      events,
			"withheld": map[string]any{
				"changed": zero, "transient": zero, "quarantined": zero,
				"created": map[string]any{"entities": 0, "relations": withheldCreated},
				"deleted": map[string]any{"entities": 0, "relations": withheldDeleted},
				"events":  withheldCreated + withheldDeleted,
			},
		},
		Lists: map[string][]string{
			"scope": sorted(scope), "created": sorted(created), "deleted": sorted(deleted),
			"changed": {}, "transient": {}, "quarantined": {},
		},
	}
}

func (o *oracle) wantPodNeighbors(q questions) view {
	at := q.i1
	win := iv{at, at.Add(time.Nanosecond)}
	p := o.byID[q.checkout.ID]
	o.checkMargin("endpoint", "the instant", at, []*demo.Pod{p})
	rows := []string{
		row("forward", "scheduled_on", "L2", o.ref(o.nodeID[p.Node], win), existenceLabels),
		row("reverse", "part_of", "L2", o.ref(o.containerID[p], win), existenceLabels),
	}
	// The pod is a pod of a service: its instance is part of it.
	if b, ok := o.bridge(p); ok && b.has(at) {
		rows = append(rows, row("reverse", "part_of", "L2", o.ref(o.instID[p], win), existenceLabels))
		first, _, _ := o.sightings(p)
		o.checkMargin("endpoint", "the instant", at, nil, first, b.to)
	}
	// The pod's address is a claim to an endpoint the request did not name.
	return view{
		Tool: "neighbors_at", Args: o.argLabels(q.podNeighborsArgs()), Derived: true,
		Meta: map[string]any{"at": fmtTime(at), "direction": "both", "entity": o.ref(o.podID[p], win)},
		Counts: map[string]any{
			"edges": len(rows), "edges_shown": len(rows),
			"withheld": map[string]any{"edges": o.podClaims(p, win), "quarantined": 0},
		},
		Lists: map[string][]string{"edges": sorted(rows), "quarantined": {}},
	}
}

func (o *oracle) wantResolve(q questions) (v view, id string) {
	p := o.byID[q.checkout.ID]
	id, _ = o.endpoint(p.IP)
	win := iv{q.i1, q.i1.Add(time.Nanosecond)}
	return view{
		Tool: "resolve_entity", Args: o.argLabels(q.resolveArgs(p.IP)), Derived: true,
		Meta: map[string]any{
			"entity": o.ref(id, win), "attributes": map[string]any{"topo.endpoint.address": p.IP, "topo.endpoint.realm": "k8s/" + demo.ClusterName},
			"existence": map[string]any{"at": fmtTime(q.i1), "status": "alive", "labels": existenceLabels},
		},
		Lists: map[string][]string{},
	}, id
}

func (o *oracle) wantEndpointNeighbors(q questions, endpointID string) view {
	at := q.i1
	win := iv{at, at.Add(time.Nanosecond)}
	p := o.byID[q.checkout.ID]
	rows := []string{row("reverse", "has_address", "L2", o.ref(o.podID[p], win), existenceLabels)}
	return view{
		Tool: "neighbors_at", Args: o.argLabels(q.endpointNeighborsArgs(endpointID)), Derived: true,
		Meta: map[string]any{"at": fmtTime(at), "direction": "both", "entity": o.ref(endpointID, win)},
		Counts: map[string]any{
			"edges": len(rows), "edges_shown": len(rows),
			"withheld": map[string]any{"edges": 0, "quarantined": 0},
		},
		Lists: map[string][]string{"edges": rows, "quarantined": {}},
	}
}

// ---- the services ----

// callIv is when the edge of a call between two services exists, by the rules of
// the service graph translator: it is first sighted at the first flush that sees the
// call, which for a call that is live when the translator starts is the second flush,
// because the first sets the series aside while it warms up; it is sighted at every
// flush after, up to the one after the last call; and it ends one expiry after the
// last sighting.
func (o *oracle) callIv(c demo.Call) iv {
	flush := func(k int) time.Time { return o.story.Anchor.Add(flushPast + time.Duration(k)*time.Minute) }
	first := c.From.Add(flushPast)
	if !c.From.After(flush(0)) {
		first = flush(1)
	}
	last := c.To.Add(flushPast)
	if last.After(flush(flushes - 1)) {
		last = flush(flushes - 1)
	}
	return iv{first, last.Add(demo.Expiry)}
}

// callsTo are the calls between services whose server is the service, and the key
// of a service is its namespace and name.
func (o *oracle) callsTo(server string) []demo.Call {
	var out []demo.Call
	for _, c := range o.w.Calls {
		if !c.Database && c.ServerNamespace+"/"+c.Server == server {
			out = append(out, c)
		}
	}
	return out
}

// checkServiceMargin fails the test if the instant is within margin of the
// beginning or the end of any call between services.
func (o *oracle) checkServiceMargin(question, what string, at time.Time) {
	o.t.Helper()
	var events []time.Time
	for _, c := range o.w.Calls {
		if !c.Database {
			r := o.callIv(c)
			events = append(events, r.from, r.to)
		}
	}
	o.checkMargin(question, what, at, nil, events...)
}

// wantServiceBlast is the blast radius of a service over a window: the services
// that call it, those that call them, and so on to the depth limit, each for as long
// as the calls on its path all exist inside the window. The questions asked here
// reach every service by one path only, and the oracle stops if one does not.
func (o *oracle) wantServiceBlast(q questions, seed string, win iv) view {
	o.checkServiceMargin("blast_radius", "the window start", win.from)
	o.checkServiceMargin("blast_radius", "the window end", win.to)
	type member struct {
		key    string
		parent string
		reach  iv
	}
	var rows []string
	seen := map[string]bool{seed: true}
	frontier := []member{{key: seed, reach: win}}
	for depth := 1; depth <= 6 && len(frontier) > 0; depth++ {
		var next []member
		for _, f := range frontier {
			for _, c := range o.callsTo(f.key) {
				reach := f.reach.and(o.callIv(c))
				client := c.ClientNamespace + "/" + c.Client
				if reach.empty() {
					continue
				}
				if seen[client] {
					o.t.Fatalf("the oracle: %s is reached by more than one path", client)
				}
				seen[client] = true
				next = append(next, member{key: client, parent: f.key, reach: reach})
				class, n := classOf(reach, win)
				rows = append(rows, row("depth "+strconv.Itoa(depth), o.ref(o.svcID[client], win), class+" "+strconv.Itoa(n), reachLabels,
					"reach "+span(reach, reachEnd(reach, win)), "via depends_on "+o.ref(o.svcID[f.key], win)))
			}
		}
		frontier = next
	}
	return view{
		Tool: "blast_radius", Args: o.argLabels(q.blastServiceArgs(o.svcID[seed], win)), Derived: true,
		Meta: map[string]any{
			"seed": o.ref(o.svcID[seed], win), "window": map[string]any{"from": fmtTime(win.from), "to": fmtTime(win.to)}, "depth_limit": 6,
			"truncation": map[string]any{"depth_cut": 0, "frontier_cuts": []any{}, "incomplete": false, "members_unlisted": 0},
		},
		Counts: map[string]any{"members": len(rows), "members_shown": len(rows), "quarantined": 0, "unattributed": 0},
		Lists:  map[string][]string{"members": sorted(rows), "quarantined": {}, "unattributed": {}},
	}
}

// wantFindServices is the services whose name holds the text, at the instant.
func (o *oracle) wantFindServices(q questions) view {
	at := q.svcAt
	win := iv{at, at.Add(time.Nanosecond)}
	o.checkServiceMargin("find", "the instant", at)
	// A service exists from its first sighting to its last plus the expiry; the
	// first span count is the first sighting of every service, and the pods that
	// send them run to the end.
	var keys []string
	for key, id := range o.svcID {
		if strings.Contains(strings.ToLower(o.refs[id].name), "pay") {
			keys = append(keys, key)
		}
	}
	slices.SortFunc(keys, func(a, b string) int { return strings.Compare(o.refs[o.svcID[a]].name, o.refs[o.svcID[b]].name) })
	first := o.story.Anchor.Add(spanPast)
	var rows []string
	for _, key := range keys {
		lives := iv{first, forever}
		reach := lives.and(win)
		class, n := classOf(reach, win)
		rows = append(rows, row(o.ref(o.svcID[key], win), existenceLabels, class+" "+strconv.Itoa(n), span(reach, "")))
	}
	return view{
		Tool: "find_entities", Args: o.argLabels(q.findServicesArgs()), Derived: true,
		Meta: map[string]any{"type": "service", "window": map[string]any{"from": fmtTime(win.from), "to": fmtTime(win.to)}, "complete": true, "next_after": nil},
		Counts: map[string]any{
			"checked": len(o.svcID), "matched": len(keys), "not_printed": 0, "quarantined": 0, "retained": 0, "undescribed": 0,
		},
		Lists: map[string][]string{"matches": rows},
	}
}

// wantServiceChanged is the change of checkout over the minutes in which it stops
// calling one service and starts calling another: the new call appears and the old
// one ends by the silence of its last sighting, one expiry after.
func (o *oracle) wantServiceChanged(q questions) view {
	win := iv{q.moveFrom, q.moveTo.Add(time.Nanosecond)}
	o.checkServiceMargin("what_changed", "the first instant", q.moveFrom)
	o.checkServiceMargin("what_changed", "the second instant", q.moveTo)
	checkout := "shop/checkout"
	var created, deleted []string
	for _, c := range o.w.Calls {
		if c.Database || c.ClientNamespace+"/"+c.Client != checkout {
			continue
		}
		r := o.callIv(c)
		edge := o.ref(o.svcID[checkout], win) + " depends_on " + o.ref(o.svcID[c.ServerNamespace+"/"+c.Server], win)
		reach := r.and(win)
		atStart, atEnd := !r.from.After(win.from), !r.to.Before(win.to)
		switch {
		case reach.empty(), atStart && atEnd:
			// Not there, or there for the whole window: no change.
		case atEnd:
			created = append(created, row("relation", edge, "asserted/derived/diff.created", "appeared", span(reach, "")))
		case atStart:
			deleted = append(deleted, row("relation", edge, "asserted/derived/diff.deleted", "disappeared", span(reach, "liveness_expiry"), "deleted_at "+fmtTime(reach.to)))
		default:
			o.t.Fatalf("the oracle: the call %s comes and goes inside the window", edge)
		}
	}
	zero := map[string]any{"entities": 0, "relations": 0}
	rel := func(n int) map[string]any { return map[string]any{"entities": 0, "relations": n} }
	return view{
		Tool: "what_changed", Args: o.argLabels(q.changedServiceArgs()), Derived: true,
		Meta: map[string]any{"window": map[string]any{"from": fmtTime(win.from), "to": fmtTime(win.to)}},
		Counts: map[string]any{
			"changed": zero, "transient": zero, "quarantined": zero,
			"created": rel(len(created)), "deleted": rel(len(deleted)),
			"events": len(created) + len(deleted),
			"withheld": map[string]any{
				"changed": zero, "transient": zero, "quarantined": zero, "created": zero, "deleted": zero, "events": 0,
			},
		},
		Lists: map[string][]string{
			"scope": {o.ref(o.svcID[checkout], win)}, "created": sorted(created), "deleted": sorted(deleted),
			"changed": {}, "transient": {}, "quarantined": {},
		},
	}
}

// ---- reading the live answers ----

func obj(v any) map[string]any { m, _ := v.(map[string]any); return m }

func arr(v any) []any { l, _ := v.([]any); return l }

func str(v any) string {
	s, _ := v.(string)
	return s
}

func labelsOf(m map[string]any) string {
	return str(m["subject"]) + "/" + str(m["basis"]) + "/" + str(m["rule"])
}

// reader turns live answers into views: ids become the oracle's names, and an id
// the oracle does not know is an error.
type reader struct {
	t testing.TB
	o *oracle
}

// ref writes a Ref of an answer as the oracle writes the same entity. The id must be
// the oracle's, with the type the oracle gives it, and a name that is printed must
// be the oracle's name and be read from the highest-ranked producer's description.
func (r reader) ref(v any, win iv) string {
	r.t.Helper()
	m := obj(v)
	id := str(m["id"])
	info, ok := r.o.refs[id]
	if !ok {
		r.t.Errorf("an answer names %s, which the world does not have", id)
		return "?"
	}
	if str(m["type"]) != info.typ {
		r.t.Errorf("%s is a %s in the answer and a %s in the world", id, str(m["type"]), info.typ)
	}
	if name, ok := m["name"].(string); ok {
		if name != info.name {
			r.t.Errorf("%s is named %q in the answer and %q in the world", id, name, info.name)
		}
		want := "k8sobjects/" + demo.ClusterName
		if info.from != "" {
			want = info.from
		}
		if from := str(m["name_from"]); from != want {
			r.t.Errorf("%s: its name is read from %q, want %q", id, from, want)
		}
		if ns, has := m["namespace"].(string); has && ns != info.ns {
			r.t.Errorf("%s is in namespace %q in the answer and %q in the world", id, ns, info.ns)
		}
		if info.ns != "" && m["namespace"] == nil {
			r.t.Errorf("%s is named but has no namespace", id)
		}
		if info.ns != "" {
			return info.typ + " " + name + " [" + info.ns + "]"
		}
		return info.typ + " " + name
	}
	if m["name_note"] == nil && !info.unnamed {
		r.t.Errorf("%s has no name and no note saying why", id)
	}
	return info.typ + " " + info.name + " (unnamed)"
}

func (r reader) spans(v any) []string {
	var out []string
	for _, s := range arr(v) {
		m := obj(s)
		out = append(out, str(m["start"])+".."+str(m["end"])+reasonOf(m["end_reason"]))
	}
	return out
}

func reasonOf(v any) string {
	if s, ok := v.(string); ok {
		return " (" + s + ")"
	}
	return ""
}

func (r reader) policy(m map[string]any) map[string]any { return obj(m["policy"]) }

func (r reader) find(res map[string]any, args map[string]any, at time.Time) view {
	win := iv{at, at.Add(time.Nanosecond)}
	var rows []string
	for _, e := range arr(res["matches"]) {
		m := obj(e)
		rows = append(rows, row(r.ref(m["entity"], win), labelsOf(m), str(m["class"])+" "+fmt.Sprint(m["transitions"]), strings.Join(r.spans(m["spans"]), "; ")))
	}
	w := obj(res["window"])
	return view{
		Tool: "find_entities", Args: r.o.argLabels(args), Policy: r.policy(res), Derived: res["derived"] == true,
		Meta:   map[string]any{"type": res["type"], "window": w, "complete": res["complete"], "next_after": res["next_after"]},
		Counts: obj(res["counts"]),
		// Matches are listed by name: the order is part of the tool's rule.
		Lists: map[string][]string{"matches": append([]string{}, rows...)},
	}
}

func (r reader) edges(res map[string]any, win iv) []string {
	var rows []string
	for _, e := range arr(res["edges"]) {
		m := obj(e)
		rows = append(rows, row(str(m["direction"]), str(m["relation"]), str(m["layer"]), r.ref(m["peer"], win), labelsOf(m)))
	}
	return sorted(rows)
}

// listed is the entries of a list of the answer that is empty here, as text, so
// that an entry would show.
func (r reader) listed(res map[string]any, key string) []string {
	var rows []string
	for _, e := range arr(res[key]) {
		b, _ := json.Marshal(e)
		rows = append(rows, string(b))
	}
	return sorted(rows)
}

func (r reader) neighbors(res map[string]any, args map[string]any, win iv) view {
	return view{
		Tool: "neighbors_at", Args: r.o.argLabels(args), Policy: r.policy(res), Derived: res["derived"] == true,
		Meta:   map[string]any{"at": res["at"], "direction": res["direction"], "entity": r.ref(res["entity"], win)},
		Counts: obj(res["counts"]),
		Lists:  map[string][]string{"edges": r.edges(res, win), "quarantined": r.listed(res, "quarantined")},
	}
}

func (r reader) blast(res map[string]any, args map[string]any, win iv) view {
	var rows []string
	for _, e := range arr(res["members"]) {
		m := obj(e)
		var via []string
		for _, v := range arr(m["via"]) {
			vm := obj(v)
			via = append(via, str(vm["relation"])+" "+r.ref(vm["parent"], win))
		}
		var reach []string
		for _, s := range arr(m["reach"]) {
			sm := obj(s)
			reach = append(reach, str(sm["start"])+".."+str(sm["end"])+reasonOf(sm["end_reason"]))
		}
		rows = append(rows, row("depth "+fmt.Sprint(m["depth"]), r.ref(m["entity"], win), str(m["class"])+" "+fmt.Sprint(m["transitions"]), labelsOf(m),
			"reach "+strings.Join(reach, "; "), "via "+strings.Join(via, "; ")))
	}
	return view{
		Tool: "blast_radius", Args: r.o.argLabels(args), Policy: r.policy(res), Derived: res["derived"] == true,
		Meta: map[string]any{
			"seed": r.ref(res["seed"], win), "window": res["window"], "depth_limit": res["depth_limit"], "truncation": res["truncation"],
		},
		Counts: obj(res["counts"]),
		Lists:  map[string][]string{"members": sorted(rows), "quarantined": r.listed(res, "quarantined"), "unattributed": r.listed(res, "unattributed")},
	}
}

func (r reader) coResidents(res map[string]any, args map[string]any, win iv) view {
	var rows []string
	for _, e := range arr(res["co_residents"]) {
		m := obj(e)
		var nodes, overlaps []string
		for _, n := range arr(m["nodes"]) {
			nodes = append(nodes, str(obj(n)["name"]))
		}
		slices.Sort(nodes)
		for _, s := range arr(m["overlaps"]) {
			sm := obj(s)
			overlaps = append(overlaps, str(sm["start"])+".."+str(sm["end"])+reasonOf(sm["end_reason"]))
		}
		rows = append(rows, row(str(m["mechanism"]), r.ref(m["peer"], win), str(m["class"])+" "+fmt.Sprint(m["transitions"]), labelsOf(m),
			"via "+r.ref(m["via"], win), "nodes "+strings.Join(nodes, ","), "overlap "+strings.Join(overlaps, "; ")))
	}
	var placement []string
	for _, e := range arr(res["placement"]) {
		m := obj(e)
		ed := obj(m["edge"])
		placement = append(placement, row(r.ref(ed["from"], win)+" "+str(ed["relation"])+" "+r.ref(ed["to"], win), labelsOf(m), strings.Join(r.spans(m["spans"]), "; ")))
	}
	return view{
		Tool: "co_residents", Args: r.o.argLabels(args), Policy: r.policy(res), Derived: res["derived"] == true,
		Meta: map[string]any{
			"seed": r.ref(res["seed"], win), "window": res["window"], "node_only": res["node_only"], "truncation": res["truncation"],
		},
		Counts: obj(res["counts"]),
		Lists: map[string][]string{
			"co_residents": sorted(rows), "placement": sorted(placement),
			"quarantined": r.listed(res, "quarantined"), "suppressed": r.listed(res, "suppressed"), "unattributed": r.listed(res, "unattributed"),
		},
	}
}

func (r reader) changed(res map[string]any, args map[string]any, win iv) view {
	listing := func(key string) []string {
		var rows []string
		for _, e := range arr(res[key]) {
			m := obj(e)
			what := ""
			if ed := obj(m["edge"]); ed != nil {
				what = r.ref(ed["from"], win) + " " + str(ed["relation"]) + " " + r.ref(ed["to"], win)
			} else {
				what = r.ref(m["entity"], win)
			}
			cols := []string{str(m["kind"]), what, labelsOf(m), str(m["class"]), strings.Join(r.spans(m["spans"]), "; ")}
			if d := str(m["deleted_at"]); d != "" {
				cols = append(cols, "deleted_at "+d)
			}
			rows = append(rows, row(cols...))
		}
		return sorted(rows)
	}
	var scope []string
	for _, s := range arr(res["scope"]) {
		scope = append(scope, r.ref(s, win))
	}
	return view{
		Tool: "what_changed", Args: r.o.argLabels(args), Policy: r.policy(res), Derived: res["derived"] == true,
		Meta:   map[string]any{"window": res["window"]},
		Counts: obj(res["counts"]),
		Lists: map[string][]string{
			"scope": sorted(scope), "created": listing("created"), "deleted": listing("deleted"),
			"changed": listing("changed"), "transient": listing("transient"), "quarantined": r.listed(res, "quarantined"),
		},
	}
}

func (r reader) resolve(res map[string]any, args map[string]any, win iv) view {
	ex := obj(res["existence"])
	return view{
		Tool: "resolve_entity", Args: r.o.argLabels(args), Policy: r.policy(res), Derived: res["derived"] == true,
		Meta: map[string]any{
			"entity": r.ref(res["entity"], win), "attributes": res["attributes"],
			"existence": map[string]any{"at": ex["at"], "status": ex["status"], "labels": labelsOf(ex)},
		},
		Lists: map[string][]string{},
	}
}

// ---- the test ----

var (
	tokenInBanner = regexp.MustCompile(`records, token (\d+),`)
	buildLine     = regexp.MustCompile(`built the cluster in memory" .*skips=(\d+) skips_unexplained=(\d+) notes=(\d+) warnings=(\d+)`)
	// endpointIDText is an endpoint's id, not the placeholder that stands for a
	// withheld one.
	endpointIDText = regexp.MustCompile(`topo\.endpoint:[0-9a-f]{32}`)
)

// encode is a view as the golden file holds it, indented, with no HTML escaping.
func encode(t testing.TB, v any) []byte {
	t.Helper()
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	enc.SetEscapeHTML(false)
	enc.SetIndent("", "  ")
	if err := enc.Encode(v); err != nil {
		t.Fatal(err)
	}
	return buf.Bytes()
}

// firstDifference says where two encodings first differ.
func firstDifference(a, b []byte) string {
	la, lb := strings.Split(string(a), "\n"), strings.Split(string(b), "\n")
	for i := 0; i < len(la) || i < len(lb); i++ {
		var x, y string
		if i < len(la) {
			x = la[i]
		}
		if i < len(lb) {
			y = lb[i]
		}
		if x != y {
			return fmt.Sprintf("line %d:\n  live:   %s\n  wanted: %s", i+1, x, y)
		}
	}
	return "none"
}

// The demo answers the questions of a stranger's path, over the MCP route and over
// raw JSON-RPC: from a name to ids, a node's neighbours, its blast radius, a
// pod's co-residents, the change over the eviction, and an address claim seen
// from the pod, where it is counted and not listed, and from the address when the
// request names it, where it is listed. Each answer equals what the generated
// world says it must be, then the golden file; the token every answer carries is
// the banner's.
//
// The demo sends the trace collector's metrics through the sequencer's metrics
// path, so five of the questions are about services: a name, the blast radius of a
// service that is called, the blast radius of a service while it is called by one
// caller and after it is called by none, and the change of a caller while it moves
// from one callee to another.
func TestDemoEndToEnd(t *testing.T) {
	o := newOracle(t)
	q := askedAbout(o)
	s := startDemo(t)

	// What the command says about what it built.
	banner := tokenInBanner.FindStringSubmatch(s.stderr.String())
	if banner == nil {
		t.Fatalf("no token in the banner:\n%s", s.stderr)
	}
	bannerToken := banner[1]
	if line := buildLine.FindStringSubmatch(s.stderr.String()); line == nil {
		t.Errorf("the build's log line does not say what was skipped and noted:\n%s", s.stderr)
	} else {
		reports := 0
		for _, r := range o.w.Requests() {
			if r.Emitter == demo.EmitClusterReport {
				reports++
			}
		}
		// Every skip is one the world sent on purpose, and nothing else was
		// skipped, noted or warned about: a namespace event for each namespace in
		// each report of the cluster receiver; each series of a call that is live
		// when the trace collector's first flush arrives, which the translator sets
		// aside while it warms up; and each point of a call to a database.
		warm, databases := o.serviceSkips()
		if want := strconv.Itoa(reports*len(o.w.Namespaces) + warm + databases); line[1] != want || line[2] != "0" || line[3] != "0" || line[4] != "0" {
			t.Errorf("skips=%s skips_unexplained=%s notes=%s warnings=%s; want skips=%s and the rest 0", line[1], line[2], line[3], line[4], want)
		}
		if warm != 11 || databases != 720 {
			t.Errorf("the world sends %d series in the first flush and %d database points, want 11 and 720", warm, databases)
		}
	}

	type asked struct {
		name string
		res  mcpResult
		// named says the request named an endpoint, so its answer may print one.
		named bool
	}
	var all []asked
	call := func(name, tool string, args map[string]any, named bool) map[string]any {
		t.Helper()
		res := mcpOK(t, s.base, tool, args)
		if res.IsError || res.Structured == nil {
			t.Fatalf("%s %s: no answer (isError %v, text %q)", name, tool, res.IsError, res.Text)
		}
		all = append(all, asked{name, res, named})
		if tok := str(res.Structured["token"]); tok != bannerToken {
			t.Errorf("%s: token %q, want the banner's %q", name, tok, bannerToken)
		}
		return res.Structured
	}

	r := reader{t, o}
	point := iv{q.i1, q.i1.Add(time.Nanosecond)}
	got := map[string]view{}
	want := map[string]view{}

	got["q0_find"] = r.find(call("Q0", "find_entities", q.findArgs(), false), q.findArgs(), q.i1)
	want["q0_find"] = o.wantFind(q)

	got["q1_neighbors"] = r.neighbors(call("Q1", "neighbors_at", q.neighborsArgs(), false), q.neighborsArgs(), point)
	want["q1_neighbors"] = o.wantNeighbors(q)

	blastRes := call("Q2", "blast_radius", q.blastArgs(), false)
	got["q2_blast_radius"] = r.blast(blastRes, q.blastArgs(), q.w1)
	want["q2_blast_radius"] = o.wantBlast(q)

	got["q3_co_residents"] = r.coResidents(call("Q3", "co_residents", q.coArgs(), false), q.coArgs(), iv{q.coFrom, q.coTo})
	want["q3_co_residents"] = o.wantCoResidents(q)

	changedWin := iv{q.e0, q.e1.Add(time.Nanosecond)}
	got["q4_what_changed"] = r.changed(call("Q4", "what_changed", q.changedArgs(), false), q.changedArgs(), changedWin)
	want["q4_what_changed"] = o.wantChanged(q)

	got["q5a_pod_neighbors"] = r.neighbors(call("Q5a", "neighbors_at", q.podNeighborsArgs(), false), q.podNeighborsArgs(), point)
	want["q5a_pod_neighbors"] = o.wantPodNeighbors(q)

	checkout := o.byID[q.checkout.ID]
	resolveArgs := q.resolveArgs(checkout.IP)
	resolved := call("Q5b", "resolve_entity", resolveArgs, true)
	wantResolve, endpointID := o.wantResolve(q)
	if id := str(obj(resolved["entity"])["id"]); id != endpointID {
		t.Errorf("Q5b: resolve_entity gave the address %s the id %s, the world's identity is %s", checkout.IP, id, endpointID)
	}
	got["q5b_resolve_endpoint"] = r.resolve(resolved, resolveArgs, point)
	want["q5b_resolve_endpoint"] = wantResolve

	endpointArgs := q.endpointNeighborsArgs(endpointID)
	got["q5c_endpoint_neighbors"] = r.neighbors(call("Q5c", "neighbors_at", endpointArgs, true), endpointArgs, point)
	want["q5c_endpoint_neighbors"] = o.wantEndpointNeighbors(q, endpointID)

	askServices(t, o, q, call, got, want)

	// The oracle first: the live answer, less its policy, equals what the world
	// says. Only then is the file read.
	for name, w := range want {
		g := got[name]
		g.Policy = nil
		if a, b := encode(t, g), encode(t, w); !bytes.Equal(a, b) {
			t.Errorf("%s differs from what the world says:\n%s", name, firstDifference(a, b))
		}
		if !got[name].Derived || !w.Derived {
			t.Errorf("%s is not labelled derived", name)
		}
		if len(got[name].Policy) == 0 {
			t.Errorf("%s has no policy", name)
		}
	}

	// Asked again at its token, the blast radius is the same bytes.
	again := mcpOK(t, s.base, "blast_radius", func() map[string]any {
		a := map[string]any{"asof": bannerToken}
		for k, v := range q.blastArgs() {
			a[k] = v
		}
		return a
	}())
	if a, b := encode(t, again.Structured), encode(t, blastRes); !bytes.Equal(a, b) || again.IsError {
		t.Errorf("blast_radius at asof %s differs from the answer without it:\n%s", bannerToken, firstDifference(a, b))
	}

	// An endpoint's id is printed only where the request named the endpoint.
	for _, a := range all {
		b, _ := json.Marshal(a.res.Structured)
		for what, text := range map[string]string{"text": a.res.Text, "structured content": string(b)} {
			found := endpointIDText.MatchString(text)
			switch {
			case a.named && !found:
				t.Errorf("%s: the request named the endpoint and its %s does not print it", a.name, what)
			case !a.named && found:
				t.Errorf("%s: its %s prints an endpoint id: %.200s", a.name, what, endpointIDText.FindString(text))
			}
		}
	}

	if code := s.stop(t); code != 0 {
		t.Errorf("exit %d after the signal, want 0:\n%s", code, s.stderr)
	}
	if endpointIDText.MatchString(s.stderr.String()) {
		t.Error("the log prints an endpoint id")
	}

	// The golden file: the live projection, byte for byte.
	live := encode(t, got)
	if t.Failed() {
		if *update {
			t.Log("-update did not write the golden file: the test failed first")
		}
		return
	}
	if *update {
		if err := os.MkdirAll(filepath.Dir(e2eGolden), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(e2eGolden, live, 0o644); err != nil {
			t.Fatal(err)
		}
		t.Logf("wrote %s", e2eGolden)
		return
	}
	golden, err := os.ReadFile(e2eGolden)
	if err != nil {
		t.Fatalf("%v; write it with: go test -run 'TestDemoEndToEnd$' ./cmd/toposhift -update", err)
	}
	if !bytes.Equal(live, golden) {
		t.Errorf("the answers differ from %s:\n%s", e2eGolden, firstDifference(live, golden))
	}
}

// askServices asks the questions about the services of a started demo, and adds
// each answer, projected, and what the world says it must be, to got and want.
func askServices(t *testing.T, o *oracle, q questions, call func(name, tool string, args map[string]any, named bool) map[string]any, got, want map[string]view) {
	t.Helper()
	r := reader{t, o}
	// The id the banner and the story print for a service is its identity.
	for _, svc := range q.story.Services {
		if id := o.svcID[svc.Namespace+"/"+svc.Name]; svc.ID != id {
			t.Errorf("the story gives %s/%s the id %s, the identity is %s", svc.Namespace, svc.Name, svc.ID, id)
		}
	}
	ledger, legacy := q.story.Service("payments", "ledger"), q.story.Service("payments", "legacy-pay")

	findArgs := q.findServicesArgs()
	got["q6_find_services"] = r.find(call("Q6", "find_entities", findArgs, false), findArgs, q.svcAt)
	want["q6_find_services"] = o.wantFindServices(q)

	args := q.blastServiceArgs(ledger.ID, q.fault)
	got["q7_blast_ledger"] = r.blast(call("Q7", "blast_radius", args, false), args, q.fault)
	want["q7_blast_ledger"] = o.wantServiceBlast(q, "payments/ledger", q.fault)

	args = q.blastServiceArgs(legacy.ID, q.legacyOn)
	got["q8a_blast_legacy_pay_while_called"] = r.blast(call("Q8a", "blast_radius", args, false), args, q.legacyOn)
	want["q8a_blast_legacy_pay_while_called"] = o.wantServiceBlast(q, "payments/legacy-pay", q.legacyOn)

	args = q.blastServiceArgs(legacy.ID, q.legacyOff)
	got["q8b_blast_legacy_pay_after"] = r.blast(call("Q8b", "blast_radius", args, false), args, q.legacyOff)
	want["q8b_blast_legacy_pay_after"] = o.wantServiceBlast(q, "payments/legacy-pay", q.legacyOff)

	changedWin := iv{q.moveFrom, q.moveTo.Add(time.Nanosecond)}
	args = q.changedServiceArgs()
	got["q9_changed_checkout"] = r.changed(call("Q9", "what_changed", args, false), args, changedWin)
	want["q9_changed_checkout"] = o.wantServiceChanged(q)
}

// serviceSkips are what the service graph translator sets aside on purpose: the
// series of the calls that are live when the trace collector's first flush arrives,
// which it sets aside while it warms up, and every point of a call to a database,
// at each flush.
func (o *oracle) serviceSkips() (warmUp, databases int) {
	first := o.story.Anchor.Add(flushPast)
	for _, c := range o.w.Calls {
		for k := range flushes {
			f := first.Add(time.Duration(k) * time.Minute)
			calls, last := 0, time.Time{}
			for m := c.From; !m.After(c.To) && !m.After(f); m = m.Add(time.Minute) {
				calls++
				last = m
			}
			if calls == 0 || f.Sub(last) > connectorIdle {
				continue
			}
			switch {
			case c.Database:
				databases++
			case k == 0:
				warmUp++
			}
		}
	}
	return warmUp, databases
}

// The service questions over HTTP: a name finds the services that hold it; the
// blast radius of a service is the services that call it, and those that call them,
// and no instance; a service that nothing calls any more has no blast radius; and
// the change of checkout shows the call that began and the call that ended by the
// silence of its last sighting. Every expected set comes from the world, by the
// rules of the translators, and no answer prints an endpoint.
func TestDemoAnswersServiceQuestions(t *testing.T) {
	o := newOracle(t)
	q := askedAbout(o)
	s := startDemo(t)
	got, want := map[string]view{}, map[string]view{}
	var texts []string
	call := func(name, tool string, args map[string]any, _ bool) map[string]any {
		t.Helper()
		res := mcpOK(t, s.base, tool, args)
		if res.IsError || res.Structured == nil {
			t.Fatalf("%s %s: no answer (isError %v, text %q)", name, tool, res.IsError, res.Text)
		}
		b, _ := json.Marshal(res.Structured)
		texts = append(texts, res.Text, string(b))
		return res.Structured
	}
	askServices(t, o, q, call, got, want)
	for name, w := range want {
		g := got[name]
		g.Policy = nil
		if a, b := encode(t, g), encode(t, w); !bytes.Equal(a, b) {
			t.Errorf("%s differs from what the world says:\n%s", name, firstDifference(a, b))
		}
	}

	// The shapes written out here, on top of the exact comparison, so that an oracle
	// and an answer that were both empty would not agree: the failure of ledger
	// reaches five services and no instance, legacy-pay is called by three while
	// checkout calls it, and by none after.
	members := func(v view) []string {
		var names []string
		for _, row := range v.Lists["members"] {
			names = append(names, strings.Split(row, " | ")[1])
		}
		return names
	}
	for name, n := range map[string]int{"q7_blast_ledger": 5, "q8a_blast_legacy_pay_while_called": 3, "q8b_blast_legacy_pay_after": 0} {
		g, w := members(got[name]), members(want[name])
		if len(g) != n || len(w) != n {
			t.Errorf("%s lists %v, and the world says %v; want %d members", name, g, w, n)
		}
		for _, m := range g {
			if !strings.HasPrefix(m, "service ") {
				t.Errorf("%s reaches %q, want services only: an instance is never followed to its service", name, m)
			}
		}
	}
	for _, tx := range texts {
		if endpointIDText.MatchString(tx) {
			t.Errorf("an answer prints an endpoint id: %.200s", tx)
		}
	}
	if code := s.stop(t); code != 0 {
		t.Errorf("exit %d after the signal, want 0:\n%s", code, s.stderr)
	}
}
