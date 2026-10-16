package demo

import (
	"fmt"
	"strings"
	"time"

	"github.com/lotannauo/toposhift/internal/lifecycle"
	"github.com/lotannauo/toposhift/internal/store/memstore"
)

// ClusterName is the cluster name every collector of the demo sets and the
// sequencer is configured with.
const ClusterName = "demo"

// Span is how long the history is: from the anchor to the anchor plus Span.
const Span = 6 * time.Hour

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
	// Took is how long the build took, by the wall clock. It is not part of the
	// history.
	Took  time.Duration
	Story Story
}

func rfc(t time.Time) string { return t.UTC().Format(time.RFC3339) }

// Instructions is what the tools say about the demo to the client that connects:
// the window, since a client has no "now", and the nodes, since there is no search.
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
	return sb.String()
}

// Banner is what a person who starts the demo reads: what it is, its window and
// its digest, the story in three lines, and two questions to ask. Everything in it
// is a function of the seed. It prints node and pod ids and never an endpoint's.
func (b *Built) Banner() string {
	s := b.Story
	var sb strings.Builder
	fmt.Fprintf(&sb, "toposhift demo: a synthetic Kubernetes cluster (seed %d), built in memory; history %s to %s; %d records, token %d, digest %s\n",
		s.Seed, rfc(s.Anchor), rfc(s.End), b.Records, b.LastSeq, b.Digest[:16])
	fmt.Fprintf(&sb, "  %s: %s stops being ready; at %s its pods are deleted and created again on the other nodes, except those a DaemonSet keeps there, which stay; at %s it is ready again.\n",
		rfc(s.NotReady), s.Failed.Name, rfc(s.Evict), rfc(s.Ready))
	fmt.Fprintf(&sb, "  %s: the frontend is rolled out, one pod at a time; a report job runs every hour from %s.\n",
		rfc(s.Rollout), rfc(s.Anchor.Add(jobFirst)))
	sb.WriteString("  Nothing here is real: the nodes, pods and times are made up, and one seed always gives the same cluster.\n")
	fmt.Fprintf(&sb, "  try blast_radius: entity %s, from %s to %s\n", s.Failed.ID, rfc(s.NotReady), rfc(s.NotReady.Add(30*time.Minute)))
	fmt.Fprintf(&sb, "  try neighbors_at: entity %s, at %s\n", s.Failed.ID, rfc(s.NotReady.Add(-time.Minute)))
	return sb.String()
}
