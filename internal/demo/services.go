package demo

import (
	"fmt"
	"time"

	"github.com/lotannauo/toposhift/internal/ingest/otlpmetrics"
)

// The service layer is data laid over the cluster after it is drawn, so it draws
// nothing: every pod, node, uid, name and instant of a seed is the same with it
// and without it. Ten of the workloads are services, because their pods send
// traces; their calls are counted by the trace collector, which is modelled in
// connector.go.

// serviceWorkloads are the instrumented workloads, by namespace and name. The others send
// no traces: the metrics agent and the report Job have none to send, the two
// databases are called and do not call, and the cluster's own components are not
// part of the shop.
var serviceWorkloads = [][2]string{
	{"platform", "ingress"},
	{"shop", "frontend"},
	{"shop", "checkout"},
	{"shop", "cart"},
	{"shop", "catalog"},
	{"shop", "search"},
	{"shop", "recommend"},
	{"payments", "payments-api"},
	{"payments", "legacy-pay"},
	{"payments", "ledger"},
}

// Call is a stream of calls from one workload's service to another's, as the trace
// collector counts them. It fires Rate calls at every whole minute of the window
// [From, To], both ends included, counted from the anchor.
type Call struct {
	Client, ClientNamespace string
	Server, ServerNamespace string
	// From and To are the first and the last minute at which it fires, both whole
	// minutes after the anchor's. A call that is live for the whole window starts
	// when the trace collector does, two hours before the anchor, and ends with the
	// window.
	From, To time.Time
	// Rate is how many calls it fires at each of those minutes.
	Rate int
	// Database says the server is a database: the collector counts the call with
	// the connection type database, and toposhift sets it aside.
	Database bool
}

// collectorLead is how long before the anchor the trace collector started.
const collectorLead = 2 * time.Hour

// minutes is how many of the call's minutes are at or before t.
func (c Call) minutes(t time.Time) int {
	if t.Before(c.From) {
		return 0
	}
	if t.After(c.To) {
		t = c.To
	}
	return int(t.Sub(c.From)/time.Minute) + 1
}

// firedBy is how many calls the stream has fired at or before t, and the instant
// of the latest. The connector's counter at a flush is the first.
func (c Call) firedBy(t time.Time) (n int, last time.Time) {
	m := c.minutes(t)
	if m == 0 {
		return 0, time.Time{}
	}
	return m * c.Rate, c.From.Add(time.Duration(m-1) * time.Minute)
}

// callSpec is a call by offsets from the anchor.
type callSpec struct {
	client, clientNS, server, serverNS string
	rate                               int
	from, to                           time.Duration
	db                                 bool
}

// whole is the live range of a call that lasts for the whole window.
const (
	wholeFrom = -collectorLead
	wholeTo   = Span
)

// callTable is the demo's calls: thirteen calls between services and two to databases.
var callTable = []callSpec{
	// The entry: the ingress calls the frontend, which calls five services.
	{"ingress", "platform", "frontend", "shop", 12, wholeFrom, wholeTo, false},
	{"frontend", "shop", "catalog", "shop", 6, wholeFrom, wholeTo, false},
	{"frontend", "shop", "search", "shop", 4, wholeFrom, wholeTo, false},
	{"frontend", "shop", "recommend", "shop", 3, wholeFrom, wholeTo, false},
	{"frontend", "shop", "cart", "shop", 5, wholeFrom, wholeTo, false},
	{"frontend", "shop", "checkout", "shop", 2, wholeFrom, wholeTo, false},
	// Depth: the catalog is called by three services.
	{"search", "shop", "catalog", "shop", 4, wholeFrom, wholeTo, false},
	{"recommend", "shop", "catalog", "shop", 2, wholeFrom, wholeTo, false},
	// Checkout is a caller and a callee; the cart is called by two services.
	{"checkout", "shop", "cart", "shop", 2, wholeFrom, wholeTo, false},
	// The call that ends: checkout stops calling legacy-pay.
	{"checkout", "shop", "legacy-pay", "payments", 3, wholeFrom, legacyLastCallAfter, false},
	// The call that appears: checkout starts calling payments-api.
	{"checkout", "shop", "payments-api", "payments", 3, migrateAfter, wholeTo, false},
	// A reconciliation job keeps legacy-pay a caller after it loses its own.
	{"legacy-pay", "payments", "ledger", "payments", 1, wholeFrom, wholeTo, false},
	{"payments-api", "payments", "ledger", "payments", 3, migrateAfter, wholeTo, false},
	// The two databases: an endpoint is not a service.
	{"cart", "shop", "redis", "", 8, wholeFrom, wholeTo, true},
	{"ledger", "payments", "db", "", 2, wholeFrom, wholeTo, true},
}

// services lays the service layer over the finished cluster: it names the
// services and builds the calls. It reads the cluster and draws nothing.
func (b *builder) services() {
	for _, svc := range serviceWorkloads {
		b.w.Workloads[b.w.workloadIndex(svc[0], svc[1])].Service = svc[1]
	}
	for _, c := range callTable {
		b.w.Calls = append(b.w.Calls, Call{
			Client: c.client, ClientNamespace: c.clientNS, Server: c.server, ServerNamespace: c.serverNS,
			From: anchor.Add(c.from), To: anchor.Add(c.to), Rate: c.rate, Database: c.db,
		})
	}
}

// Instance is a pod seen as a service instance.
type Instance struct {
	// Namespace and Service name the service, ID is the instance's
	// service.instance.id, and Version its service.version. PodUID is the
	// k8s.pod.uid of the pod it runs in.
	Namespace, Service, ID, Version, PodUID string
}

// Instance is the service instance the pod is, if its workload is a service. An
// instance is a pod: the id is the namespace, the pod's name and the container's
// name, the shape the OpenTelemetry conventions recommend for Kubernetes, and no
// two pods have one id. The container is named for its workload. A pod that
// replaces another is another instance, and so is a pod of another rollout.
func (w *World) Instance(p *Pod) (Instance, bool) {
	wl := w.Workloads[p.Workload]
	if wl.Service == "" {
		return Instance{}, false
	}
	return Instance{
		Namespace: wl.Namespace, Service: wl.Service,
		ID:      fmt.Sprintf("%s.%s.%s", wl.Namespace, p.Name, wl.Name),
		Version: p.ImageTag, PodUID: p.UID,
	}, true
}

// serviceFingerprint is the id the tools print for the service of a namespace and
// a name: the identity of a service is made of both.
func serviceFingerprint(namespace, name string) string {
	fp, err := otlpmetrics.NewFingerprints(resolver).Service(namespace, name)
	if err != nil {
		// The names are the world's own, which the catalog accepts.
		panic("demo: " + err.Error())
	}
	return fp.String()
}

// serviceStory fills the part of the story that is about the service layer.
func (w *World) serviceStory(s *Story) {
	for _, wl := range w.Workloads {
		if wl.Service != "" {
			s.Services = append(s.Services, ServiceRef{Namespace: wl.Namespace, Name: wl.Service, ID: serviceFingerprint(wl.Namespace, wl.Service)})
		}
	}
	if len(w.Calls) == 0 {
		return
	}
	s.Migrate = anchor.Add(migrateAfter)
	s.LastLegacyCall = anchor.Add(legacyLastCallAfter)
	s.FaultFrom = anchor.Add(faultAfter)
	s.FaultTo = anchor.Add(faultAfter + faultFor)
	// The call to legacy-pay ends one expiry after its last sighting. That is the
	// first flush at or after its last call, because the connector's counter grows
	// there and nowhere later; the expiry is not typed in.
	for _, c := range w.Calls {
		if c.Client == "checkout" && c.Server == "legacy-pay" {
			s.LegacyCallEnds = flushAfter(c.To).Add(Expiry)
		}
	}
}
