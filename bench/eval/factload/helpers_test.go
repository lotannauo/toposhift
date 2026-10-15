package factload

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/lotannauo/toposhift/internal/catalog"
	"github.com/lotannauo/toposhift/internal/identity"
	"github.com/lotannauo/toposhift/internal/store"
	"github.com/lotannauo/toposhift/internal/store/memstore"
)

var ctx = context.Background()

// t0 is 10:00 on an invented day; every time of a test is written as an offset.
var t0 = time.Date(2026, 3, 4, 10, 0, 0, 0, time.UTC)

func at(min int, sec float64) time.Time {
	return t0.Add(time.Duration(min)*time.Minute + time.Duration(sec*float64(time.Second)))
}

func resolver() *identity.Resolver { return identity.NewResolver(catalog.Default()) }

func fpOf(t testing.TB, typ catalog.EntityType, attrs ...identity.Attr) identity.Fingerprint {
	t.Helper()
	id, err := resolver().Resolve(typ, attrs)
	if err != nil {
		t.Fatal(err)
	}
	return id.Fingerprint()
}

func svcFP(t testing.TB, ns, name string) identity.Fingerprint {
	return fpOf(t, catalog.Service, identity.Attr{Key: catalog.ServiceNamespace, Value: ns}, identity.Attr{Key: catalog.ServiceName, Value: name})
}

func instFP(t testing.TB, ns, name, id string) identity.Fingerprint {
	return fpOf(t, catalog.ServiceInstance,
		identity.Attr{Key: catalog.ServiceNamespace, Value: ns}, identity.Attr{Key: catalog.ServiceName, Value: name},
		identity.Attr{Key: catalog.ServiceInstanceID, Value: id})
}

func podFP(t testing.TB, uid string) identity.Fingerprint {
	return fpOf(t, catalog.K8sPod, identity.Attr{Key: catalog.K8sPodUID, Value: uid})
}

func nodeFP(t testing.TB, uid string) identity.Fingerprint {
	return fpOf(t, catalog.K8sNode, identity.Attr{Key: catalog.K8sNodeUID, Value: uid})
}

// spy is a memstore that keeps every batch it was asked to write.
type spy struct {
	*memstore.Store
	mu      sync.Mutex
	batches [][]store.Record
}

func newSpy(t testing.TB) *spy {
	t.Helper()
	st, err := memstore.Open(memstore.Options{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = st.Close() })
	return &spy{Store: st}
}

func (s *spy) Write(c context.Context, rs []store.Record) error {
	s.mu.Lock()
	s.batches = append(s.batches, append([]store.Record(nil), rs...))
	s.mu.Unlock()
	return s.Store.Write(c, rs)
}

func (s *spy) records() []store.Record {
	var out []store.Record
	for _, b := range s.batches {
		out = append(out, b...)
	}
	return out
}

// Objects of invented clusters.

func podJSON(uid, name, ns, node, created string, containers ...string) string {
	var cs []string
	for i, c := range containers {
		cs = append(cs, fmt.Sprintf(`{"name":"c%d","containerID":"containerd://%s"}`, i, c))
	}
	nodeField := ""
	if node != "" {
		nodeField = fmt.Sprintf(`"nodeName":%q`, node)
	}
	return fmt.Sprintf(`{"apiVersion":"v1","kind":"Pod","metadata":{"uid":%q,"name":%q,"namespace":%q,"creationTimestamp":%q,"labels":{"app.kubernetes.io/component":"x"}},"spec":{%s},"status":{"phase":"Running","containerStatuses":[%s]}}`,
		uid, name, ns, created, nodeField, strings.Join(cs, ","))
}

func nodeJSON(uid, name, created string) string {
	return fmt.Sprintf(`{"apiVersion":"v1","kind":"Node","metadata":{"uid":%q,"name":%q,"creationTimestamp":%q},"spec":{},"status":{}}`, uid, name, created)
}

func mustObject(t testing.TB, resource, body string, observed time.Time) Object {
	t.Helper()
	o, err := NewObject(resource, []byte(body), observed)
	if err != nil {
		t.Fatal(err)
	}
	return o
}

func load(t testing.TB, sc Scenario) (*spy, Stats) {
	t.Helper()
	st := newSpy(t)
	stats, err := Load(ctx, st, sc)
	if err != nil {
		t.Fatal(err)
	}
	return st, stats
}
