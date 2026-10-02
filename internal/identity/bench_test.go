package identity_test

import (
	"testing"

	"github.com/lotannauo/toposhift/internal/catalog"
	"github.com/lotannauo/toposhift/internal/identity"
)

// Resolve is the ingest hot path: every observation is resolved to find its
// entity. These benchmarks are a local sanity check on allocations, not a
// published figure; performance numbers come only from CI hardware.

func BenchmarkResolveHost(b *testing.B) {
	r := newResolver()
	in := attrs(catalog.HostID, "i-0abcd1234ef567890")
	b.ReportAllocs()
	for b.Loop() {
		if _, err := r.Resolve(catalog.Host, in); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkResolveProcess(b *testing.B) {
	r := newResolver()
	in := attrs(
		catalog.HostID, "i-0abcd1234ef567890",
		catalog.ProcessPID, "1234",
		catalog.ProcessCreationTime, "2023-11-21T09:25:34.853Z",
	)
	b.ReportAllocs()
	for b.Loop() {
		if _, err := r.Resolve(catalog.Process, in); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkParse(b *testing.B) {
	r := newResolver()
	id, err := r.Resolve(catalog.Process, attrs(
		catalog.HostID, "i-0abc", catalog.ProcessPID, 1234, catalog.ProcessCreationTime, "2023-11-21T09:25:34.853Z"))
	if err != nil {
		b.Fatal(err)
	}
	canon := id.Canonical()
	b.ReportAllocs()
	for b.Loop() {
		if _, err := r.Parse(canon); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkRegistryRegister(b *testing.B) {
	r := newResolver()
	id, _ := r.Resolve(catalog.Host, attrs(catalog.HostID, "h1"))
	reg := identity.NewRegistry()
	b.ReportAllocs()
	for b.Loop() {
		if err := reg.Register(id); err != nil {
			b.Fatal(err)
		}
	}
}
