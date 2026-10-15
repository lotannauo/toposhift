package itbench

import (
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"sort"
	"strings"
	"testing"
	"time"
)

// The fixtures here are invented: a small shop in a cluster of two nodes, in the
// format of the dataset's exports. None of it is a row of a dataset.

const (
	shopNS    = "shop"
	appNS     = "shop-app" // the service.namespace of the spans
	created   = "2026-03-04T08:00:00Z"
	snapshot  = "2026-03-04 10:12:00.250000000"
	winStart  = "2026-03-04T10:05:00Z"
	lastAlert = "2026-03-04T10-15-00.000000"
)

// quoteField writes a field as the exports do: quoted, with a quote doubled, when
// it holds a tab, a line break or a quote.
func quoteField(s string) string {
	if strings.ContainsAny(s, "\t\n\"") {
		return `"` + strings.ReplaceAll(s, `"`, `""`) + `"`
	}
	return s
}

func tsvLine(cols ...string) string {
	q := make([]string, len(cols))
	for i, c := range cols {
		q[i] = quoteField(c)
	}
	return strings.Join(q, "\t") + "\n"
}

var objectColumns = []string{
	"Timestamp", "TimestampTime", "TraceId", "SpanId", "TraceFlags", "SeverityText", "SeverityNumber", "ServiceName",
	"Body", "ResourceSchemaUrl", "ResourceAttributes", "ScopeSchemaUrl", "ScopeName", "ScopeVersion", "ScopeAttributes",
	"LogAttributes", "EventName",
}

var spanColumns = []string{
	"Timestamp", "TraceId", "SpanId", "ParentSpanId", "TraceState", "SpanName", "SpanKind", "ServiceName",
	"ResourceAttributes", "ScopeName", "ScopeVersion", "SpanAttributes", "Duration", "StatusCode", "StatusMessage",
	"Events.Timestamp", "Events.Name", "Events.Attributes", "Links.TraceId", "Links.SpanId", "Links.TraceState", "Links.Attributes",
}

// objectRow is a log record of the objects export.
func objectRow(resource, body, ts string) string {
	return tsvLine(ts, ts[:min(19, len(ts))], "", "", "0", "", "0", "", body, "", "{}", "", "scope", "1.0.0", "{}",
		fmt.Sprintf("{'k8s.resource.name': '%s'}", resource), "")
}

func meta(uid, name, ns string, labels map[string]string, owner string) string {
	var ls []string
	for k, v := range labels {
		ls = append(ls, fmt.Sprintf("%q:%q", k, v))
	}
	sort.Strings(ls)
	own := ""
	if owner != "" {
		parts := strings.SplitN(owner, "/", 3) // kind/name/uid
		own = fmt.Sprintf(`,"ownerReferences":[{"apiVersion":"apps/v1","kind":%q,"name":%q,"uid":%q,"controller":true}]`, parts[0], parts[1], parts[2])
	}
	nsf := ""
	if ns != "" {
		nsf = fmt.Sprintf(`,"namespace":%q`, ns)
	}
	return fmt.Sprintf(`"metadata":{"uid":%q,"name":%q,"creationTimestamp":%q,"labels":{%s}%s%s}`, uid, name, created, strings.Join(ls, ","), nsf, own)
}

func podBody(uid, name, node, component, owner string) string {
	labels := map[string]string{"pod-template-hash": "h"}
	if component != "" {
		labels[ComponentLabel] = component
	}
	spec := `"spec":{}`
	if node != "" {
		spec = fmt.Sprintf(`"spec":{"nodeName":%q}`, node)
	}
	return fmt.Sprintf(`{"apiVersion":"v1","kind":"Pod",%s,%s,"status":{"phase":"Running","containerStatuses":[{"name":"main","containerID":"containerd://c-%s"}]}}`,
		meta(uid, name, shopNS, labels, owner), spec, uid)
}

func nodeBody(uid, name string) string {
	return fmt.Sprintf(`{"apiVersion":"v1","kind":"Node",%s,"spec":{},"status":{}}`, meta(uid, name, "", nil, ""))
}

func ownerBody(kind, uid, name, owner string) string {
	return fmt.Sprintf(`{"apiVersion":"apps/v1","kind":%q,%s,"spec":{}}`, kind, meta(uid, name, shopNS, nil, owner))
}

func serviceBody(uid, name, component string) string {
	return fmt.Sprintf(`{"apiVersion":"v1","kind":"Service",%s,"spec":{"selector":{%q:%q}}}`, meta(uid, name, shopNS, nil, ""), ComponentLabel, component)
}

func configMapBody(uid, name string) string {
	return fmt.Sprintf(`{"apiVersion":"v1","kind":"ConfigMap",%s,"data":{"k":"v \"quoted\"\twith a tab"}}`, meta(uid, name, shopNS, nil, ""))
}

// shopObjects is the snapshot: two nodes, and for each of five workloads a
// controller chain down to its pods. The proxy's name starts with the front's.
func shopObjects() string {
	return tsvLine(objectColumns...) + shopPull(snapshot, nil)
}

// shopPull is the rows of one pull of the snapshot's objects, listed at ts, without
// the objects whose uid is in skip.
func shopPull(ts string, skip map[string]bool) string {
	var b strings.Builder
	add := func(resource, body string) {
		if strings.Contains(body, `"uid":"`) {
			for uid := range skip {
				if strings.Contains(body, `"uid":"`+uid+`"`) {
					return
				}
			}
		}
		b.WriteString(objectRow(resource, body, ts))
	}
	add("nodes", nodeBody("node-uid-a", "node-a"))
	add("nodes", nodeBody("node-uid-b", "node-b"))
	type wl struct {
		name, kind string
		pods       []struct{ uid, node string }
	}
	for _, w := range []wl{
		{"shop-front", "Deployment", []struct{ uid, node string }{{"pod-front-1", "node-a"}, {"pod-front-2", "node-b"}}},
		{"shop-cart", "Deployment", []struct{ uid, node string }{{"pod-cart-1", "node-a"}}},
		{"shop-pay", "Deployment", []struct{ uid, node string }{{"pod-pay-1", "node-b"}}},
		{"shop-front-proxy", "Deployment", []struct{ uid, node string }{{"pod-proxy-1", "node-a"}}},
		{"shop-db", "StatefulSet", []struct{ uid, node string }{{"pod-db-1", "node-b"}}},
	} {
		uid := "uid-" + w.name
		add(strings.ToLower(w.kind)+"s", ownerBody(w.kind, uid, w.name, ""))
		parent := w.kind + "/" + w.name + "/" + uid
		if w.kind == "Deployment" {
			rs := w.name + "-r1"
			add("replicasets", ownerBody("ReplicaSet", "uid-"+rs, rs, parent))
			parent = "ReplicaSet/" + rs + "/uid-" + rs
		}
		for i, p := range w.pods {
			name := fmt.Sprintf("%s-r1-p%d", w.name, i+1)
			if w.kind == "StatefulSet" {
				name = fmt.Sprintf("%s-%d", w.name, i)
			}
			add("pods", podBody(p.uid, name, p.node, w.name, parent))
		}
		add("services", serviceBody("svc-"+w.name, w.name, w.name))
	}
	// A pod that names a node the snapshot does not list.
	add("pods", podBody("pod-lost-1", "shop-lost-r1-p1", "node-ghost", "shop-lost", ""))
	add("configmaps", configMapBody("cm-flags", "shop-flags"))
	return b.String()
}

// resourceAttrs is a span's resource, as the exports write one.
func resourceAttrs(service, instance, pod string, withNS bool) string {
	attrs := []string{
		"'host.name': 'collector-pod-xyz'", // the collector's pod, not a host
		fmt.Sprintf("'k8s.namespace.name': '%s'", shopNS),
		"'k8s.node.name': 'node-from-the-span'",
		"'k8s.pod.ip': '10.0.0.7'",
		fmt.Sprintf("'k8s.pod.name': '%s-r1-p9'", service),
		fmt.Sprintf("'service.name': '%s'", service),
	}
	if pod != "" {
		attrs = append(attrs, fmt.Sprintf("'k8s.pod.uid': '%s'", pod))
	}
	if instance != "" {
		attrs = append(attrs, fmt.Sprintf("'service.instance.id': '%s'", instance))
	}
	if withNS {
		attrs = append(attrs, fmt.Sprintf("'service.namespace': '%s'", appNS))
	}
	sort.Strings(attrs)
	return "{" + strings.Join(attrs, ", ") + "}"
}

func spanRow(ts time.Time, trace, span, parent, kind, service, res string) string {
	return tsvLine(ts.UTC().Format("2006-01-02 15:04:05.000000000"), trace, span, parent, "", "op", kind, service, res,
		"scope", "1", "{}", "1000", "Unset", "", "[]", "[]", "[]", "[]", "[]", "[]", "[]")
}

type hop struct{ service, pod string }

// shopTraces is a call chain proxy -> front -> cart -> pay, once a minute for
// minutes 0 to 14 of the hour from 10:00, each service in its own pod, with the
// instance id the pod uid and a namespace on every span.
func shopTraces() string { return traces(false) }

// onlyTraces is a traces export of a single span, of the named service.
func onlyTraces(service string) string {
	return tsvLine(spanColumns...) + spanRow(time.Date(2026, 3, 4, 10, 1, 0, 0, time.UTC), "only", "only-s", "", "Server", service, resourceAttrs(service, "", "", true))
}

// traces is shopTraces, and with messaging also a message each minute from the
// cart to the payment service through a broker: a producer span and its consumer.
func traces(messaging bool, bare ...string) string {
	var b strings.Builder
	b.WriteString(tsvLine(spanColumns...))
	chain := []hop{{"shop-front-proxy", "pod-proxy-1"}, {"shop-front", "pod-front-1"}, {"shop-cart", "pod-cart-1"}, {"shop-pay", "pod-pay-1"}}
	base := time.Date(2026, 3, 4, 10, 0, 0, 0, time.UTC)
	for m := 0; m < 15; m++ {
		tr := fmt.Sprintf("trace%02d", m)
		at := base.Add(time.Duration(m)*time.Minute + 3*time.Second)
		for i, h := range chain {
			res := resourceAttrs(h.service, h.pod, h.pod, !slices.Contains(bare, h.service))
			parent := ""
			if i > 0 {
				parent = fmt.Sprintf("%s-c%d", tr, i-1)
			}
			// The server span of this hop, then its client span to the next.
			b.WriteString(spanRow(at, tr, fmt.Sprintf("%s-s%d", tr, i), parent, "Server", h.service, res))
			if i < len(chain)-1 {
				b.WriteString(spanRow(at, tr, fmt.Sprintf("%s-c%d", tr, i), fmt.Sprintf("%s-s%d", tr, i), "Client", h.service, res))
			}
		}
		if messaging {
			b.WriteString(spanRow(at, tr, tr+"-p", tr+"-s2", "Producer", "shop-cart", resourceAttrs("shop-cart", "pod-cart-1", "pod-cart-1", true)))
			b.WriteString(spanRow(at, tr, tr+"-m", tr+"-p", "Consumer", "shop-pay", resourceAttrs("shop-pay", "pod-pay-1", "pod-pay-1", true)))
		}
	}
	return b.String()
}

// scenario describes the files of one invented scenario.
type scenario struct {
	n       int
	truth   string
	objects string
	traces  string
	alerts  bool
}

func (s scenario) write(t testing.TB, root string) string {
	t.Helper()
	dir := filepath.Join(root, fmt.Sprintf("Scenario-%d", s.n))
	if err := os.MkdirAll(filepath.Join(dir, "alerts"), 0o755); err != nil {
		t.Fatal(err)
	}
	put := func(name, content string) {
		if err := os.WriteFile(filepath.Join(dir, name), []byte(content), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	put("ground_truth.yaml", s.truth)
	if s.alerts {
		put("alerts/alerts_at_2026-03-04T10-10-00.000000.json", fmt.Sprintf(`[{"labels":{"alertname":"a"},"state":"firing","activeAt":%q,"value":"1"}]`, winStart))
		put("alerts/alerts_at_"+lastAlert+".json", `[{"labels":{"alertname":"a"},"state":"firing","activeAt":"2026-03-04T10:07:00Z","value":"1"}]`)
		put("alerts/alerts_in_alerting_state_2026-03-04T101600.000000Z.json", `{"data":{"alerts":[{"activeAt":"2026-03-04T10:08:00Z"}]},"status":"success","timestamp":"2026-03-04T10:16:00Z"}`)
	}
	if s.objects != "" {
		put(ObjectsFile, s.objects)
	}
	if s.traces != "" {
		put(TracesFile, s.traces)
	}
	return dir
}
