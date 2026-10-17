package sequencer

import (
	"maps"
	"slices"
	"strings"
	"time"

	"github.com/lotannauo/toposhift/internal/ingest/nodeagent"
)

// The counters of the node agents' records and of the legacy entity events, as
// GET /status names them. A name says what the sequencer did with the record, not
// that the record was lost: refused_* is a record, or a part of one, that was not
// written; substituted_* is a record that was written with something put in place
// of what it gave; skipped_* is a record set aside.
const (
	counterHostEvents   = "nodeagent.host_events"
	counterLegacyEvents = "sequencer.legacy_entity_events"
	counterNoHostID     = "nodeagent.refused_no_host_id"
	counterNoNodeUID    = "nodeagent.refused_no_node_uid"
)

// agentCounters name the translator's reasons as counters.
var agentCounters = map[string]string{
	nodeagent.SkipNoHostID:                      counterNoHostID,
	nodeagent.RefusedNoNodeUID:                  counterNoNodeUID,
	nodeagent.RefusedZoneID:                     "nodeagent.refused_zone_id",
	nodeagent.SubstitutedNoBoot:                 "nodeagent.substituted_no_boot",
	nodeagent.SubstitutedBadBoot:                "nodeagent.substituted_bad_boot",
	nodeagent.SubstitutedConfiguredInterval:     "nodeagent.substituted_configured_interval",
	nodeagent.SubstitutedReceiptNoTime:          "nodeagent.substituted_receipt_no_time",
	nodeagent.SubstitutedReceiptImplausibleTime: "nodeagent.substituted_receipt_implausible_time",
	nodeagent.SkipHostDelete:                    "nodeagent.skipped_host_delete",
	nodeagent.SkipNotHost:                       "nodeagent.skipped_not_host",
	nodeagent.SkipNotEntityEvent:                "nodeagent.skipped_not_entity_event",
	nodeagent.SkipLegacyEntityEvent:             counterLegacyEvents,
}

// counterOther counts a reason this table does not know.
const counterOther = "nodeagent.skipped_other"

// agentCounter is the counter a translator's reason is counted under.
func agentCounter(reason string) string {
	if c, ok := agentCounters[reason]; ok {
		return c
	}
	return counterOther
}

// countAgent adds n to a counter of a cluster name, for the request being
// translated.
func (w *work) countAgent(cluster, counter string, n int64) {
	if w.agent == nil {
		w.agent = make(map[string]map[string]int64)
	}
	m := w.agent[cluster]
	if m == nil {
		m = make(map[string]int64)
		w.agent[cluster] = m
	}
	m[counter] += n
}

// agentCluster is what the sequencer knows of the node agents of one cluster name.
type agentCluster struct {
	// last is the ingest instant of the latest group that held a host entity event
	// of the cluster, and silent that three node intervals have passed since.
	// warned says the silence has been warned about: one warning for each time a
	// cluster goes silent.
	last   time.Time
	silent bool
	warned bool
}

// NodeAgentStatus is a snapshot of the node agents' counters and of the latest
// host event of each cluster that has had one. The counters are those of every
// group answered without error since New, and also count the legacy entity
// events of both receivers.
type NodeAgentStatus struct {
	// Interval is the report interval the sequencer expects of the node agents.
	Interval time.Duration
	// Counters are named as the constants of this file say.
	Counters map[string]int64
	// Clusters are the clusters that have had a host entity event, by name.
	Clusters map[string]NodeAgentCluster
}

// NodeAgentCluster is the latest host event of a cluster.
type NodeAgentCluster struct {
	// LastHostEvent is the ingest instant of the latest group that held a host
	// entity event of the cluster.
	LastHostEvent time.Time
	// Silent says that more than three intervals have passed since, as of the
	// latest flush tick.
	Silent bool
}

// NodeAgentStatus returns the node agents' counters and last events. It may be
// called from any goroutine.
func (s *Sequencer) NodeAgentStatus() NodeAgentStatus {
	s.agentMu.Lock()
	defer s.agentMu.Unlock()
	st := NodeAgentStatus{
		Interval: s.cfg.NodeInterval,
		Counters: maps.Clone(s.agentCount),
		Clusters: make(map[string]NodeAgentCluster, len(s.agentSeen)),
	}
	for name, c := range s.agentSeen {
		st.Clusters[name] = NodeAgentCluster{LastHostEvent: c.last, Silent: c.silent}
	}
	return st
}

// noteAgents adds the counts of the groups' requests that were answered without
// error, notes the instant of each cluster's host events, and logs, at most once
// a minute for each kind and cluster, what the operator has to act on: host events
// that wrote no host or no placement, and legacy entity events, which are never
// translated.
func (s *Sequencer) noteAgents(ws []*work, now time.Time) {
	byCluster := map[string]map[string]int64{}
	for _, w := range ws {
		if w.refused != "" {
			continue
		}
		for name, m := range w.agent {
			t := byCluster[name]
			if t == nil {
				t = make(map[string]int64, len(m))
				byCluster[name] = t
			}
			for k, v := range m {
				t[k] += v
			}
		}
	}
	if len(byCluster) == 0 {
		return
	}
	names := slices.Sorted(maps.Keys(byCluster))
	s.agentMu.Lock()
	for _, name := range names {
		m := byCluster[name]
		for k, v := range m {
			s.agentCount[k] += v
		}
		if m[counterHostEvents] > 0 {
			c := s.agentSeen[name]
			if c == nil {
				c = &agentCluster{}
				s.agentSeen[name] = c
			}
			c.last, c.silent, c.warned = now, false, false
		}
	}
	s.agentMu.Unlock()
	for _, name := range names {
		m := byCluster[name]
		if n := m[counterNoHostID]; n > 0 && s.resumeLogDue("no_host_id/"+name, now) {
			s.log.Warn("sequencer: host events with no usable host.id are not stored; placement stops at the node and what runs on it is unattributed (the agent's pod needs a read-only mount of the host's /etc/machine-id, or a cloud detector)",
				"cluster", name, "count", n)
		}
		if n := m[counterNoNodeUID]; n > 0 && s.resumeLogDue("no_node_uid/"+name, now) {
			s.log.Warn("sequencer: host events with no k8s.node.uid store the host and no runs_on edge (the agent's service account needs get on nodes, and K8S_NODE_NAME must name its node; missing permission is silent in the collector)",
				"cluster", name, "count", n)
		}
		if n := m[counterLegacyEvents]; n > 0 && s.resumeLogDue("legacy/"+name, now) {
			s.log.Warn("sequencer: entity events in the legacy form are not translated; start the collectors with the feature gate pkg.experimentalmetricmetadata.useEntityEventsSpecification",
				"cluster", name, "count", n)
		}
	}
}

// agentKeep is how long a cluster's latest host event is kept in the status after
// it, and with it the throttles of its warnings, so that a cluster that went
// silent stays in the status (marked silent) for a day, past the freeing of its
// slot, and the memory of client-chosen names is bounded.
const agentKeep = 24 * time.Hour

// checkAgentsSilent is called at each flush tick. A cluster that has had host
// events and has had none for more than three node intervals is marked silent, and
// warned about, once for each time it goes silent: its hosts and runs_on edges
// lapse at three intervals. The ingest clock's present is the comparison, so a
// stopped agent and a stopped collector read alike. A cluster whose latest event is more than
// [agentKeep] old is forgotten.
func (s *Sequencer) checkAgentsSilent(now time.Time) {
	limit := nodeagent.DefaultTTLMultiple * s.cfg.NodeInterval
	type silent struct {
		name string
		last time.Time
	}
	var warn []silent
	var forget []string
	s.agentMu.Lock()
	for name, c := range s.agentSeen {
		if now.Sub(c.last) > agentKeep {
			delete(s.agentSeen, name)
			forget = append(forget, name)
			continue
		}
		c.silent = now.Sub(c.last) > limit
		if c.silent && !c.warned {
			c.warned = true
			warn = append(warn, silent{name, c.last})
		}
	}
	s.agentMu.Unlock()
	for _, name := range forget {
		s.forgetWarned(name)
	}
	slices.SortFunc(warn, func(a, b silent) int { return strings.Compare(a.name, b.name) })
	for _, w := range warn {
		s.log.Warn("sequencer: no host event for more than three node intervals; the cluster's hosts and runs_on edges lapse (the agents are down or cannot reach the cluster collector, the collector no longer sends entity events, or --node-interval is wrong)",
			"cluster", w.name, "last_host_event", w.last, "node_interval", s.cfg.NodeInterval)
	}
}

// forgetWarned drops the throttles of the warnings of a cluster name, which the
// loop alone keeps.
func (s *Sequencer) forgetWarned(name string) {
	for _, kind := range []string{"no_host_id/", "no_node_uid/", "legacy/", "svcgraph_overflow/", "no_instance_id/"} {
		delete(s.resumeWarned, kind+name)
	}
}
