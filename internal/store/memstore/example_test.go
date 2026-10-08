package memstore_test

import (
	"context"
	"fmt"
	"time"

	"github.com/lotannauo/toposhift/internal/catalog"
	"github.com/lotannauo/toposhift/internal/identity"
	"github.com/lotannauo/toposhift/internal/lifecycle"
	"github.com/lotannauo/toposhift/internal/store"
	"github.com/lotannauo/toposhift/internal/store/memstore"
)

func Example() {
	resolver := identity.NewResolver(catalog.Default())
	pod, _ := resolver.Resolve(catalog.K8sPod, []identity.Attr{{Key: catalog.K8sPodUID, Value: "pod-1"}})
	node, _ := resolver.Resolve(catalog.K8sNode, []identity.Attr{{Key: catalog.K8sNodeUID, Value: "node-1"}})

	s, err := memstore.Open(memstore.Options{})
	if err != nil {
		panic(err)
	}
	defer s.Close()
	ctx := context.Background()

	// The scheduler says "pod scheduled_on node" from noon, and the write is the
	// first thing the store learns, so its snapshot token is 1.
	noon := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	err = s.Write(ctx, []store.Record{{
		Layer:     catalog.L2,
		Subject:   store.EdgeSubject(pod.Fingerprint(), node.Fingerprint(), catalog.ScheduledOn),
		Producer:  "k8s",
		EventTime: noon,
		Seq:       1,
		Kind:      lifecycle.Observe,
		Payload:   []byte("scheduled"),
	}})
	if err != nil {
		panic(err)
	}

	// Read as of one in the afternoon, the pod is on the node. Read as of the
	// token before the write, the store knew nothing about it.
	for _, scope := range []store.Scope{store.Current(catalog.L2), {Layer: catalog.L2, AsOf: 0}} {
		peers, err := s.Neighbors(ctx, pod.Fingerprint(), store.Forward, noon.Add(time.Hour), scope)
		if err != nil {
			panic(err)
		}
		fmt.Printf("as of token %d: %d neighbors\n", min(scope.AsOf, s.LastSeq()), len(peers))
		for _, p := range peers {
			fmt.Println(p.Relation, p.Peer == node.Fingerprint())
		}
	}

	// Output:
	// as of token 1: 1 neighbors
	// scheduled_on true
	// as of token 0: 0 neighbors
}
