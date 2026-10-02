package catalog_test

import (
	"fmt"

	"github.com/lotannauo/toposhift/internal/catalog"
)

func Example() {
	c := catalog.Default()

	// Look up how a type is identified.
	svc, _ := c.Entity(catalog.Service)
	for k := range svc.Keys() {
		fmt.Printf("%s optional=%t\n", k.Name, k.Optional)
	}

	// Check an edge before accepting it.
	runsOn, _ := c.Relation(catalog.RunsOn)
	fmt.Println(runsOn.Allows(catalog.K8sNode, catalog.Host)) // asserted direction
	fmt.Println(runsOn.Allows(catalog.Host, catalog.K8sNode)) // reversed
	fmt.Println(runsOn.Propagation())

	// Output:
	// service.namespace optional=true
	// service.name optional=false
	// true
	// false
	// down
}
