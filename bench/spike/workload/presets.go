package workload

import "time"

// Scale presets: a cluster of the size the storage measurements run on, over a
// number of days. They are starting points, and [Analyzer] says what they
// amount to; none of the numbers here is a measurement of a real cluster.

// Cluster is a modeled cluster of about four hundred nodes and twenty thousand
// pods over days days, with the shapes of real churn switched on: every pod a
// new identity, nodes holding at most 110 pods, a cluster-level collector
// refreshing every pod's placement, a producer's pipeline backing up in order,
// and refreshes coalesced into runs that are re-asserted at most once per half
// their TTL.
func Cluster(days int) Config {
	return Config{
		Seed:     1,
		FirstSeq: 1,
		Start:    time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC),
		Duration: time.Duration(days) * 24 * time.Hour,

		Racks: 20, Hosts: 400, Pods: 20000, Services: 300,
		ContainersPerPod:  3,
		DependsPerService: 3,

		EventsPerSecond: 1,
		PodSkew:         1.15,
		NodeSkew:        1.1,
		FreshIdentities: true,
		MaxPodsPerNode:  110,

		HeartbeatInterval: time.Minute, HeartbeatTTLFactor: 4,
		PodHeartbeatInterval: 15 * time.Minute,
		OutageProbability:    0.0005, OutageLength: 10 * time.Minute,
		RollupInterval:     5 * time.Minute,
		ConfirmProbability: 0.3, ConfirmTTL: 5 * time.Minute,

		BacklogEvery: 6 * time.Hour, BacklogMeanDelay: 2 * time.Minute, BacklogSpan: 15 * time.Minute,

		CoalesceRuns:      true,
		ExtendTTLFraction: 0.5,

		PayloadMin: 16, PayloadMax: 96,
	}
}

// CI is three days of [Cluster]: what a benchmark job on a shared runner can
// afford.
func CI() Config { return Cluster(3) }

// Week is ten days of [Cluster]: enough to retain a week and still have history
// to retain away.
func Week() Config { return Cluster(10) }

// Month is thirty-one days of [Cluster], for the month windows only.
func Month() Config { return Cluster(31) }
