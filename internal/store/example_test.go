package store_test

import (
	"fmt"
	"time"

	"github.com/lotannauo/toposhift/internal/catalog"
	"github.com/lotannauo/toposhift/internal/identity"
	"github.com/lotannauo/toposhift/internal/lifecycle"
	"github.com/lotannauo/toposhift/internal/store"
)

func Example() {
	resolver := identity.NewResolver(catalog.Default())
	pod, _ := resolver.Resolve(catalog.K8sPod, []identity.Attr{{Key: catalog.K8sPodUID, Value: "pod-1"}})
	node, _ := resolver.Resolve(catalog.K8sNode, []identity.Attr{{Key: catalog.K8sNodeUID, Value: "node-1"}})

	// A read pinned to token 42 in layer L2 sees the records committed up to 42.
	scope := store.Scope{Layer: catalog.L2, AsOf: 42}
	fmt.Println(scope.Layer, scope.AsOf, scope.Validate())

	// The edge "pod scheduled_on node", as one producer asserts it.
	rec := store.Record{
		Layer:     catalog.L2,
		Subject:   store.EdgeSubject(pod.Fingerprint(), node.Fingerprint(), catalog.ScheduledOn),
		Producer:  "k8s",
		EventTime: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC),
		Seq:       43,
		Kind:      lifecycle.Observe,
		TTL:       time.Minute,
		// The time came from a field of the object itself.
		EventTimeBasis: store.BasisObjectField,
	}
	fmt.Println(rec.Subject.Relation, rec.Validate())

	// Output:
	// L2 42 <nil>
	// scheduled_on <nil>
}
