package itbench

import (
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"testing"
	"time"
)

func shopCluster(t testing.TB) *Cluster {
	t.Helper()
	path := filepath.Join(t.TempDir(), ObjectsFile)
	if err := os.WriteFile(path, []byte(shopObjects()), 0o644); err != nil {
		t.Fatal(err)
	}
	cl, err := ReadCluster(path)
	if err != nil {
		t.Fatal(err)
	}
	return cl
}

func group(kind string, filter ...string) Group {
	return Group{ID: "g", Kind: kind, Namespace: shopNS, Filter: filter}
}

// A name maps to a service only through the component label of the pods an object
// is, owns or selects, matched exactly. The proxy's pods are named after the front's
// and are never the front's.
func TestMappingIsExactAndThroughTheComponentLabel(t *testing.T) {
	cl := shopCluster(t)
	for _, c := range []struct {
		name     string
		g        Group
		services []string
		objects  int
		why      string
	}{
		{"a service by a whole-name filter", group("Service", `shop-front\b`), []string{"shop-front"}, 1, ""},
		{"the proxy is not the front", group("Service", `shop-front-proxy\b`), []string{"shop-front-proxy"}, 1, ""},
		{"a deployment through its replica sets", group("Deployment", `shop-front\b`), []string{"shop-front"}, 1, ""},
		{"a stateful set through its pods", group("StatefulSet", `shop-db`), []string{"shop-db"}, 1, ""},
		{"a replica set", group("ReplicaSet", `shop-cart-r1`), []string{"shop-cart"}, 1, ""},
		{"pods by the filter as written", group("Pod", `shop-front-r1-.*`), []string{"shop-front"}, 2, ""},
		// A filter that over-matches maps to every service it names: nothing is
		// narrowed or guessed.
		{"an over-matching filter", group("Pod", `shop-front-.*`), []string{"shop-front", "shop-front-proxy"}, 3, ""},
		{"no prefix match", group("Service", `shop-f`), nil, 0, WhyNoObject},
		{"no match inside a name", group("Service", `front`), nil, 0, WhyNoObject},
		{"a pod with no label is not mapped by its name", group("Pod", `shop-lost-.*`), []string{"shop-lost"}, 1, ""},
		{"a kind that holds no pods", group("ConfigMap", `shop-flags`), nil, 0, WhyKind},
		{"a wildcard without a filter", Group{ID: "g", Kind: "Service", Namespace: shopNS, Name: "shop-*"}, nil, 0, "wildcard_name_without_filter"},
		{"a filter that is not an expression", group("Service", `*.*`), nil, 0, "filter_not_a_regular_expression"},
		{"no name or filter", Group{ID: "g", Kind: "Service", Namespace: shopNS}, nil, 0, "no_name_or_filter"},
		{"another namespace", Group{ID: "g", Kind: "Service", Namespace: "elsewhere", Filter: []string{`shop-front\b`}}, nil, 0, WhyNoObject},
	} {
		m := MapGroup(cl, c.g)
		if !slices.Equal(m.Services, c.services) || m.Objects != c.objects || m.Why != c.why {
			t.Errorf("%s: services %v, objects %d, why %q; want %v, %d, %q", c.name, m.Services, m.Objects, m.Why, c.services, c.objects, c.why)
		}
	}
}

// An object whose pods name no service, or several, maps to none.
func TestAnObjectWithNoSingleComponentMapsToNothing(t *testing.T) {
	cl := shopCluster(t)
	// A service that selects on a label every pod has selects pods of every component.
	all := &Object{Kind: "Service", Namespace: shopNS, Name: "everything", Selector: map[string]string{"pod-template-hash": "h"}}
	cl.Objects = append(cl.Objects, all)
	m := MapGroup(cl, group("Service", `everything`))
	if m.Mapped() || m.Why != WhyAmbiguous || m.Unmapped != 1 {
		t.Errorf("%+v", m)
	}
	// A service without a selector selects nothing.
	none := &Object{Kind: "Service", Namespace: shopNS, Name: "nothing"}
	cl.Objects = append(cl.Objects, none)
	if m := MapGroup(cl, group("Service", `nothing`)); m.Mapped() || m.Why != WhyNoComponent {
		t.Errorf("%+v", m)
	}
}

func rowFor(t *testing.T, truth string) ARow {
	t.Helper()
	root := t.TempDir()
	d := scenario{n: 1, truth: truth, alerts: true}.write(t, root)
	r, err := ReadTableA(d)
	if err != nil {
		t.Fatal(err)
	}
	if r.Unreadable != "" {
		t.Fatal(r.Unreadable)
	}
	return r
}

func TestSeedOfAPodRootIsTheServiceAndTheUniquePod(t *testing.T) {
	cl := shopCluster(t)
	t0 := utc(winStart)
	row := rowFor(t, scenario1Truth)
	s := PlanSeed(row, cl, t0)
	if !s.OK() || s.Kind != SeedPod || s.Moved || s.Service != "shop-pay" || len(s.Pods) != 1 || s.Pods[0].UID != "pod-pay-1" {
		t.Fatalf("%+v", s)
	}

	// Two pods of the group: the pod is not unique, and the pod level cannot say.
	two := strings.Replace(scenario1Truth, `"shop-pay-.*"`, `"shop-front-r1-.*"`, 1)
	s = PlanSeed(rowFor(t, two), cl, t0)
	if !s.OK() || s.Service != "shop-front" || len(s.Pods) != 0 || s.PodsWhy != "pod_not_unique_at_window_start" {
		t.Errorf("two pods: %+v", s)
	}

	// A pod created after the window starts is not alive at its start.
	s = PlanSeed(row, cl, utc("2026-03-04T07:00:00Z"))
	if !s.OK() || len(s.Pods) != 0 || s.PodsWhy != "pod_not_unique_at_window_start" {
		t.Errorf("before creation: %+v", s)
	}
	// And one listed more than the TTL ago is not alive either.
	s = PlanSeed(row, cl, cl.Observed.Add(5*time.Minute))
	if len(s.Pods) != 0 {
		t.Errorf("after the snapshot's TTL: %+v", s)
	}
	s = PlanSeed(row, cl, cl.Observed.Add(5*time.Minute-time.Nanosecond))
	if len(s.Pods) != 1 {
		t.Errorf("a nanosecond before the TTL: %+v", s)
	}

	// A group of pods that spans two services has no one service.
	over := strings.Replace(scenario1Truth, `"shop-pay-.*"`, `"shop-front-.*"`, 1)
	if s := PlanSeed(rowFor(t, over), cl, t0); s.OK() || s.Why != WhyRootUnmapped {
		t.Errorf("over-matching root: %+v", s)
	}
}

func TestSeedOfAnotherRootMovesToTheFirstTargetThatMaps(t *testing.T) {
	cl := shopCluster(t)
	row := rowFor(t, scenario2Truth)
	s := PlanSeed(row, cl, utc(winStart))
	if !s.OK() || !s.Moved || s.Kind != SeedWorkload || s.Service != "shop-cart" || s.Group != "cart-svc" || len(s.Pods) != 1 || s.Pods[0].UID != "pod-cart-1" {
		t.Fatalf("%+v", s)
	}

	// Targets that map to nothing are passed over, in order.
	skip := strings.Replace(scenario2Truth, `filter: ['shop-cart\b']`, `filter: ['nothing-like-it']`, 1)
	s = PlanSeed(rowFor(t, skip), cl, utc(winStart))
	if !s.OK() || s.Service != "shop-front" || !s.Moved {
		t.Errorf("second target: %+v", s)
	}
	// If none maps there is no seed.
	none := strings.NewReplacer(`['shop-cart\b']`, `['x1']`, `['shop-front\b']`, `['x2']`, `['shop-pay\b']`, `['x3']`).Replace(scenario2Truth)
	if s := PlanSeed(rowFor(t, none), cl, utc(winStart)); s.OK() || s.Why != WhyNoTarget {
		t.Errorf("no target maps: %+v", s)
	}
	// No single root, no seed.
	two := strings.Replace(scenario2Truth, "filter: ['shop-cart\\b']", "filter: ['shop-cart\\b']\n    root_cause: true", 1)
	if s := PlanSeed(rowFor(t, two), cl, utc(winStart)); s.OK() || s.Why != WhyNoRoot {
		t.Errorf("two roots: %+v", s)
	}
}

func TestSeedOfANodeRoot(t *testing.T) {
	cl := shopCluster(t)
	truth := `
groups:
  - {id: n, kind: Node, name: node-a, root_cause: true}
  - {id: p, kind: Pod, namespace: shop, filter: ["shop-cart-r1-.*"]}
propagations:
  - {source: n, target: p}
`
	row := rowFor(t, truth)
	s := PlanSeed(row, cl, utc(winStart))
	if !s.OK() || s.Kind != SeedNode || s.Node.UID != "node-uid-a" {
		t.Fatalf("%+v", s)
	}
	tr := ChainTruth(row, cl, s)
	if len(tr.Services) != 0 || len(tr.Pods) != 1 {
		t.Errorf("a node seed has pod-level truth only: %+v", tr)
	}
	if s := PlanSeed(rowFor(t, strings.Replace(truth, "node-a", "node-nowhere", 1)), cl, utc(winStart)); s.OK() {
		t.Error("a node that is not listed became a seed")
	}
	if s := PlanSeed(rowFor(t, strings.Replace(truth, "name: node-a", `filter: ["node-.*"]`, 1)), cl, utc(winStart)); s.OK() || s.Why != WhyAmbiguous {
		t.Errorf("a filter that names both nodes: %+v", s)
	}
}

// Truth is a set after mapping: a Deployment and a Pod that map to one service count
// once, the seed's own service is not truth, a group that maps to nothing is
// counted, and only pod groups are expanded into pods.
func TestTruthIsASetAfterMapping(t *testing.T) {
	cl := shopCluster(t)
	truth := `
groups:
  - {id: root, kind: Pod, namespace: shop, filter: ["shop-pay-.*"], root_cause: true}
  - {id: dep, kind: Deployment, namespace: shop, filter: ['shop-front\b']}
  - {id: pod, kind: Pod, namespace: shop, filter: ["shop-front-r1-p1"]}
  - {id: self, kind: Service, namespace: shop, filter: ['shop-pay\b']}
  - {id: cart, kind: Service, namespace: shop, filter: ['shop-cart\b']}
  - {id: cfg, kind: ConfigMap, namespace: shop, name: shop-flags}
  - {id: wild, kind: Service, namespace: shop, name: "shop-*"}
propagations:
  - {source: root, target: dep}
  - {source: dep, target: pod}
  - {source: pod, target: self}
  - {source: self, target: cart}
  - {source: cart, target: cfg}
  - {source: cfg, target: wild}
  - {source: wild, target: ghost}
`
	row := rowFor(t, truth)
	seed := PlanSeed(row, cl, utc(winStart))
	if !seed.OK() || seed.Service != "shop-pay" {
		t.Fatalf("%+v", seed)
	}
	tr := ChainTruth(row, cl, seed)
	if !slices.Equal(tr.Services, []string{"shop-cart", "shop-front"}) {
		t.Errorf("services %v: the deployment and the pod are one service, and the seed's is not truth", tr.Services)
	}
	if len(tr.Pods) != 1 || tr.Pods[0].UID != "pod-front-1" {
		t.Errorf("pods %v: only the pod group is expanded", tr.Pods)
	}
	// cfg (a kind that is not mapped), wild (no filter) and ghost (no group).
	if tr.Unmappable != 3 || tr.LabelFiltered != 4 || tr.NotExpanded != 3 {
		t.Errorf("unmappable %d, label-filtered %d, not expanded %d; want 3, 4, 3", tr.Unmappable, tr.LabelFiltered, tr.NotExpanded)
	}
	if want := map[string]int{"ConfigMap": 1, "Service": 1, NoGroup: 1}; !reflect.DeepEqual(tr.UnmappableKinds, want) {
		t.Errorf("unmappable by kind %v, want %v", tr.UnmappableKinds, want)
	}
	text := FormatTableB(row, cl)
	for _, want := range []string{"unmappable truth groups by kind: (no group) 1, ConfigMap 1, Service 1", "3 unmappable", "3 not expanded into pods"} {
		if !strings.Contains(text, want) {
			t.Errorf("table B lacks %q:\n%s", want, text)
		}
	}
}
