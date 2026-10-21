// Package fixture holds what the tests of the metrics-store baseline share: a small
// cluster with the registered producers and intervals, and fingerprints built from a
// byte. It is not used outside tests.
package fixture

import (
	"time"

	"github.com/lotannauo/toposhift/bench/spike/runner"
	"github.com/lotannauo/toposhift/bench/spike/workload"
	"github.com/lotannauo/toposhift/internal/catalog"
	"github.com/lotannauo/toposhift/internal/identity"
)

// Registered is the workload configuration of the metrics-store comparison, at the size
// of the modeled cluster scaled down to a few hosts: the producers, their intervals and TTLs,
// the pod churn model, the backlog model of lateness and the coalescer's settings are the
// registered ones, and only the counts of racks, hosts, pods and services, the rate of
// churn and the length differ. Node and host heartbeats are every 60 s with a TTL of 240
// s, pod heartbeats every 900 s with 3,600 s, service-dependency rollups every 300 s
// with 1,200 s, and confirmations have a TTL of 300 s.
func Registered(d time.Duration) workload.Config {
	w := workload.Cluster(1)
	w.Duration = d
	w.Racks, w.Hosts, w.Pods, w.Services = 3, 12, 120, 8
	w.EventsPerSecond = 0.02
	w.RunMaxAge = 30 * time.Minute
	return w
}

// Spec is the spec of a run over [Registered] for two days: long enough for every age a
// plan reads at, the day back included. The questions are chosen from the stream, and a
// group with mostly empty answers is allowed.
func Spec() runner.Spec {
	s := runner.DefaultSpec(Registered(48 * time.Hour))
	s.MinNonEmpty = 0
	return s
}

// RetainingSpec is a run of 60 hours with a retention at hour 40 that keeps 24 hours.
func RetainingSpec() runner.Spec {
	s := runner.DefaultSpec(Registered(60 * time.Hour))
	s.MinNonEmpty = 0
	s.Retentions = []runner.Retention{{At: 40 * time.Hour, Keep: 24 * time.Hour}}
	return s
}

// FP is the fingerprint of the type with a hash of the byte, in the first of its bytes.
func FP(typ catalog.EntityType, b byte) identity.Fingerprint {
	var h [identity.FingerprintBytes]byte
	h[0] = b
	f, err := identity.FingerprintFromHash(typ, h)
	if err != nil {
		panic(err)
	}
	return f
}
