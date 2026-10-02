package identity_test

import (
	"errors"
	"fmt"

	"github.com/lotannauo/toposhift/internal/catalog"
	"github.com/lotannauo/toposhift/internal/identity"
)

// A process is identified by its host, pid and creation time. Producers that
// spell the same values differently still agree on one entity.
func ExampleResolver_Resolve() {
	r := identity.NewResolver(catalog.Default())

	a, _ := r.Resolve(catalog.Process, []identity.Attr{
		{Key: catalog.HostID, Value: "i-0abc"},
		{Key: catalog.ProcessPID, Value: 1234},
		{Key: catalog.ProcessCreationTime, Value: "2023-11-21T09:25:34.853Z"},
	})
	b, _ := r.Resolve(catalog.Process, []identity.Attr{
		{Key: catalog.ProcessCreationTime, Value: "2023-11-21T10:25:34.853+01:00"},
		{Key: catalog.ProcessPID, Value: "1234"}, // OTLP/JSON carries int64 as a string
		{Key: catalog.HostID, Value: "i-0abc"},
	})

	fmt.Println(a.Fingerprint())
	fmt.Println(a == b)

	// An empty host.id is rejected, never merged with other empty ones.
	_, err := r.Resolve(catalog.Host, []identity.Attr{{Key: catalog.HostID, Value: ""}})
	fmt.Println(errors.Is(err, identity.ErrEmpty))

	// Output:
	// process:39f63099c3a070cea55c43db6138f7cf
	// true
	// true
}

// The registry is the in-memory form of the check the store makes on every
// fingerprint hit: one fingerprint, one identity.
func ExampleRegistry() {
	r := identity.NewResolver(catalog.Default())
	reg := identity.NewRegistry()

	id, _ := r.Resolve(catalog.Host, []identity.Attr{{Key: catalog.HostID, Value: "h1"}})
	fmt.Println(reg.Register(id))
	fmt.Println(reg.Register(id)) // the same identity again is fine
	fmt.Println(reg.Len())

	// Output:
	// <nil>
	// <nil>
	// 1
}
