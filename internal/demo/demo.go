package demo

import (
	"fmt"
	"strings"
	"time"

	"github.com/lotannauo/toposhift/internal/ingest/k8scluster"
	"github.com/lotannauo/toposhift/internal/ingest/svcgraph"
	"github.com/lotannauo/toposhift/internal/lifecycle"
	"github.com/lotannauo/toposhift/internal/store/memstore"
)

// ClusterName is the cluster name every collector of the demo sets and the
// sequencer is configured with.
const ClusterName = "demo"

// Span is how long the history is: from the anchor to the anchor plus Span.
const Span = 6 * time.Hour

// Expiry is how long after its last sighting a call or an instance ends in the
// demo: fifteen minutes, which is the dependency expiry and the instance TTL the
// sequencer is given. It is a constant of the demo; serve's defaults are six hours,
// at which no call would end inside the window.
const Expiry = 15 * time.Minute

// DefaultSeed is the seed of the one canonical world.
const DefaultSeed uint64 = 1

// anchor is where the history begins: a fixed date in the past, so that no
// answer depends on the wall clock.
var anchor = time.Date(2026, 10, 1, 0, 0, 0, 0, time.UTC)

// Built is a world that went through ingest into a store.
type Built struct {
	// Store holds the history. The caller closes it.
	Store *memstore.Store
	// LastSeq is the store's last sequence number: the token of the latest
	// snapshot. Records is how many records the sequencer wrote, which is LastSeq
	// because the demo writes through the sequencer alone.
	LastSeq uint64
	Records int
	// Digest is a SHA-256 of every record written, in sequence order. The same
	// seed built by the same code gives the same digest.
	Digest string
	// Producers are the producers of the records written, sorted: the translators
	// and no one else.
	Producers []lifecycle.Producer
	// Warnings is how many lines of warn level or above the sequencer logged. A
	// world the translators take in whole logs none.
	Warnings int
	// Skips and Notes are what the translators set aside and remarked on, by
	// producer and reason, sorted. The world sends one namespace event for the
	// cluster translator to set aside each time it reports, so Skips is not empty;
	// NamespaceEvents is how many it sent, and [Built.UnexplainedSkips] is what is
	// left of Skips once those are taken out. A world the translators take in whole
	// has no note.
	Skips, Notes    []Count
	NamespaceEvents int
	// DatabasePoints is how many service graph points of calls to a database the
	// trace collector sent, and WarmUpSeries how many series of other calls its
	// first flush held: the service graph translator sets the first aside, as the
	// database is an endpoint and not a service, and the second while it warms up.
	DatabasePoints, WarmUpSeries int
	// Metrics are the sequencer's counters of the metrics path (as GET /status
	// names them), a copy taken when the build ended.
	Metrics map[string]int64
	// Took is how long the build took, by the wall clock. It is not part of the
	// history.
	Took  time.Duration
	Story Story
}

func rfc(t time.Time) string { return t.UTC().Format(time.RFC3339) }

// UnexplainedSkips is how many skips the world's own requests do not explain. Three
// things are set aside on purpose, each for one reason of one producer and in a
// number the world knows: the namespace events the cluster receiver sends for the
// detector of cluster identity, which are not in the catalog; the series of calls
// that are live when the trace collector's first flush arrives, which the service
// graph translator sets aside while it warms up; and the points of calls to a
// database, which are endpoints and not services. It counts the skips beyond those,
// and the number of each of those that is not what the world sent, so it is 0 only
// when every skip is explained and every one that should be there is. A world that
// sent a namespace event while the skips were not read at all is not explained
// either.
func (b *Built) UnexplainedSkips() int {
	cluster, graph := "k8scluster/"+ClusterName, "svcgraph/"+ClusterName
	explained := 0
	unexplained := 0
	for _, e := range []struct {
		producer, reason string
		sent             int
	}{
		{cluster, k8scluster.SkipTypeNotInCatalog, b.NamespaceEvents},
		{graph, svcgraph.SkipWarmUp, b.WarmUpSeries},
		{graph, svcgraph.SkipConnectionDatabase, b.DatabasePoints},
	} {
		got := 0
		for _, c := range b.Skips {
			if c.Producer == e.producer && c.Reason == e.reason {
				got += c.N
			}
		}
		explained += got
		unexplained += max(got-e.sent, e.sent-got)
	}
	return Total(b.Skips) - explained + unexplained
}

// Instructions is what the tools say about the demo to the client that connects:
// the window, since a client has no "now", and the nodes, since a client starts from the ids it is told.
// It names no pod and no endpoint, and does not tell the story.
func (b *Built) Instructions() string {
	s := b.Story
	var sb strings.Builder
	fmt.Fprintf(&sb, "This is the toposhift demo: a synthetic Kubernetes cluster named %s (seed %d), not real data. ", ClusterName, s.Seed)
	fmt.Fprintf(&sb, "Its history runs from %s to %s in event time; objects created earlier exist from their creation, so ask inside that window. ", rfc(s.Anchor), rfc(s.End))
	sb.WriteString("The nodes are ")
	for i, n := range s.Nodes {
		if i > 0 {
			sb.WriteString(", ")
		}
		fmt.Fprintf(&sb, "%s (%s)", n.Name, n.ID)
	}
	sb.WriteString(". ")
	byMachine := map[int][]string{}
	var machines []int
	for _, n := range s.Nodes {
		if len(byMachine[n.Machine]) == 0 {
			machines = append(machines, n.Machine)
		}
		byMachine[n.Machine] = append(byMachine[n.Machine], n.Name)
	}
	for i, m := range machines {
		if i > 0 {
			sb.WriteString("; ")
		}
		sb.WriteString(strings.Join(byMachine[m], " and ") + " share a host")
	}
	sb.WriteString(". neighbors_at on a node lists the pods scheduled on it.")
	fmt.Fprintf(&sb, " Services come from the trace pipeline's metrics; find_entities with type service finds them by name. A call or an instance is known from its first sighting and ends %d minutes after its last, the demo's expiry (serve's default is 6 hours). blast_radius from a service reaches the services that call it; from a node or a pod it stops at service instances.", int(Expiry/time.Minute))
	return sb.String()
}

// Banner is what a person who starts the demo reads: what it is, its window and
// its digest, the story in four lines, and three questions to ask. Everything in it
// is a function of the seed. It prints node and service ids and never an endpoint's,
// and claims no call fails: toposhift stores who calls whom, not whether it works.
func (b *Built) Banner() string {
	s := b.Story
	var sb strings.Builder
	fmt.Fprintf(&sb, "toposhift demo: a synthetic Kubernetes cluster (seed %d), built in memory; history %s to %s; %d records, token %d, digest %s\n",
		s.Seed, rfc(s.Anchor), rfc(s.End), b.Records, b.LastSeq, b.Digest[:16])
	fmt.Fprintf(&sb, "  %s: %s stops being ready; at %s its pods are deleted and created again on the other nodes, except those a DaemonSet keeps there, which stay; at %s it is ready again.\n",
		rfc(s.NotReady), s.Failed.Name, rfc(s.Evict), rfc(s.Ready))
	fmt.Fprintf(&sb, "  %s: the frontend is rolled out, one pod at a time; a report job runs every hour from %s.\n",
		rfc(s.Rollout), rfc(s.Anchor.Add(jobFirst)))
	fmt.Fprintf(&sb, "  %s: checkout starts calling payments-api; its last call to legacy-pay is at %s, last sighted at %s; that call ends at %s, one expiry (%dm) after.\n",
		rfc(s.Migrate), rfc(s.LastLegacyCall), rfc(s.LegacyCallEnds.Add(-Expiry)), rfc(s.LegacyCallEnds), int(Expiry/time.Minute))
	sb.WriteString("  Nothing here is real: the nodes, pods and times are made up, and one seed always gives the same cluster.\n")
	fmt.Fprintf(&sb, "  try blast_radius: entity %s, from %s to %s\n", s.Failed.ID, rfc(s.NotReady), rfc(s.NotReady.Add(30*time.Minute)))
	fmt.Fprintf(&sb, "  try neighbors_at: entity %s, at %s\n", s.Failed.ID, rfc(s.NotReady.Add(-time.Minute)))
	fmt.Fprintf(&sb, "  try blast_radius: entity %s, from %s to %s (what ledger's failure would reach)\n", s.Service("payments", "ledger").ID, rfc(s.FaultFrom), rfc(s.FaultTo))
	return sb.String()
}
