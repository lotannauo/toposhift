package vmmodel

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/lotannauo/toposhift/bench/spike/engine"
	"github.com/lotannauo/toposhift/bench/spike/workload"
	"github.com/lotannauo/toposhift/internal/catalog"
	"github.com/lotannauo/toposhift/internal/lifecycle"
)

// ErrRegistry marks a registry that contradicts itself.
var ErrRegistry = errors.New("vmmodel: invalid registry")

// WatchScrape is the interval at which a metrics stack's exporter re-exports the state
// a watch-mode producer holds. A producer in watch mode (the Kubernetes watch, the
// fabric) asserts a fact once and ends it with an explicit delete; a metrics store holds
// that state because an exporter such as kube-state-metrics writes it again at every
// scrape of the store, about once a minute. The interval is registered here, before any
// data, with no tuning after.
const WatchScrape = time.Minute

// DefaultTTLFactor is the factor of the window of a watch-mode producer when the
// workload has no heartbeat TTL factor of its own (nothing in it refreshes): the factor
// of the registered workloads.
const DefaultTTLFactor = 4

// Holder is a producer that holds a kind of subject (a relation or an entity type) and
// the window a read names for it. A metrics store has no TTL of its own, so a read says,
// for each producer, how far back a sample still counts as the producer holding the
// subject: the window a user sets from the producer's scrape interval. It is the
// producer's TTL in the spike's streams.
//
// A producer whose records are refreshes (the node collector, the traces rollup, the
// cluster-level pod collector, the kubelet's confirmation) has Scrape zero: each refresh
// is a sample, and Window is the TTL its records carry.
//
// A producer in watch mode asserts a fact once and ends it by an explicit delete, and
// its records carry no TTL. It has a Scrape, the interval at which a scraper writes the
// fact again, from the assertion until a staleness marker at the delete, and a Window,
// the heartbeat TTL factor times Scrape, which is what a read names for it.
type Holder struct {
	Producer lifecycle.Producer
	Window   time.Duration
	Scrape   time.Duration
}

// Watch reports whether the holder is in watch mode: its records assert once and the
// samples between the assertion and the delete are a scraper's.
func (h Holder) Watch() bool { return h.Scrape > 0 }

// Registry is the registered table of who holds what, and the window each producer is
// read with. It is a value fixed before any data: the same producer has the same window
// wherever it appears, and the converter refuses a record that contradicts it.
type Registry struct {
	edges    map[catalog.RelationType][]Holder
	entities map[catalog.EntityType][]Holder
	// producers is each producer's one window and scrape interval.
	producers map[lifecycle.Producer]Holder
}

// NewRegistry builds a registry from the holders of each relation and entity type. A
// producer must have one window and one scrape interval everywhere (a whole number of
// milliseconds; the window above zero, and at least the scrape interval when there is
// one); each list is stored sorted by producer name.
func NewRegistry(edges map[catalog.RelationType][]Holder, entities map[catalog.EntityType][]Holder) (*Registry, error) {
	r := &Registry{
		edges:     map[catalog.RelationType][]Holder{},
		entities:  map[catalog.EntityType][]Holder{},
		producers: map[lifecycle.Producer]Holder{},
	}
	add := func(what string, hs []Holder) ([]Holder, error) {
		out := slices.Clone(hs)
		slices.SortFunc(out, func(a, b Holder) int { return strings.Compare(string(a.Producer), string(b.Producer)) })
		for i, h := range out {
			switch {
			case h.Producer == "":
				return nil, fmt.Errorf("%w: %s has a holder with no producer name", ErrRegistry, what)
			case h.Window <= 0 || h.Window%time.Millisecond != 0:
				return nil, fmt.Errorf("%w: %s: producer %q has window %s, which is not a positive whole number of milliseconds", ErrRegistry, what, h.Producer, h.Window)
			case h.Scrape < 0 || h.Scrape%time.Millisecond != 0:
				return nil, fmt.Errorf("%w: %s: producer %q has scrape interval %s, which is not a whole number of milliseconds", ErrRegistry, what, h.Producer, h.Scrape)
			case h.Scrape > h.Window:
				return nil, fmt.Errorf("%w: %s: producer %q has scrape interval %s, longer than its window %s: a fact would blink out between two samples", ErrRegistry, what, h.Producer, h.Scrape, h.Window)
			case i > 0 && out[i-1].Producer == h.Producer:
				return nil, fmt.Errorf("%w: %s lists producer %q twice", ErrRegistry, what, h.Producer)
			}
			if prev, seen := r.producers[h.Producer]; seen && prev != h {
				return nil, fmt.Errorf("%w: producer %q has window %s and scrape interval %s in %s, and %s and %s elsewhere", ErrRegistry, h.Producer, h.Window, h.Scrape, what, prev.Window, prev.Scrape)
			}
			r.producers[h.Producer] = h
		}
		return out, nil
	}
	// Maps are walked in sorted order so an error names the same place on every run.
	rels := make([]catalog.RelationType, 0, len(edges))
	for rel := range edges {
		rels = append(rels, rel)
	}
	slices.Sort(rels)
	for _, rel := range rels {
		hs, err := add("relation "+string(rel), edges[rel])
		if err != nil {
			return nil, err
		}
		r.edges[rel] = hs
	}
	kinds := make([]catalog.EntityType, 0, len(entities))
	for t := range entities {
		kinds = append(kinds, t)
	}
	slices.Sort(kinds)
	for _, t := range kinds {
		hs, err := add("entity type "+string(t), entities[t])
		if err != nil {
			return nil, err
		}
		r.entities[t] = hs
	}
	return r, nil
}

// RegistryFor is the registry of a synthetic workload: each refreshing producer's window
// is its TTL, which the generator derives from the configuration. The refreshing
// producers are the node collector (every HeartbeatInterval), the traces rollup (every
// RollupInterval), the cluster-level pod collector (every PodHeartbeatInterval), each
// with a TTL of HeartbeatTTLFactor times its interval, and the kubelet's confirmation
// (ConfirmTTL). The fabric and the Kubernetes watch assert once and delete explicitly:
// they are in watch mode, re-exported at [WatchScrape] with a window of the heartbeat TTL
// factor times it. A producer whose interval is zero is in watch mode too, as the
// generator makes it (its records carry no TTL).
func RegistryFor(c workload.Config) (*Registry, error) {
	factor := time.Duration(c.HeartbeatTTLFactor)
	if factor < 1 {
		factor = DefaultTTLFactor
	}
	refresh := func(p lifecycle.Producer, interval time.Duration) Holder {
		if interval <= 0 {
			return watch(p, factor)
		}
		return Holder{Producer: p, Window: time.Duration(c.HeartbeatTTLFactor) * interval}
	}
	node := refresh(workload.ProducerNodeCollector, c.HeartbeatInterval)
	traces := refresh(workload.ProducerTraces, c.RollupInterval)
	pods := Holder{Producer: workload.ProducerPodHeartbeat, Window: time.Duration(c.HeartbeatTTLFactor) * c.PodHeartbeatInterval}
	fabric := watch(workload.ProducerFabric, factor)
	objects := watch(workload.ProducerK8sObjects, factor)

	scheduled := []Holder{objects}
	if c.PodHeartbeatInterval > 0 {
		scheduled = append(scheduled, pods)
	}
	if c.ConfirmProbability > 0 {
		scheduled = append(scheduled, Holder{Producer: workload.ProducerKubelet, Window: c.ConfirmTTL})
	}
	return NewRegistry(map[catalog.RelationType][]Holder{
		catalog.ScheduledOn: scheduled,
		catalog.PartOf:      {objects},
		catalog.InstanceOf:  {objects},
		catalog.RunsOn:      {node},
		catalog.LocatedIn:   {fabric},
		catalog.DependsOn:   {traces},
	}, map[catalog.EntityType][]Holder{
		catalog.Rack:            {fabric},
		catalog.Host:            {fabric},
		catalog.K8sNode:         {node},
		catalog.K8sPod:          {objects},
		catalog.Container:       {objects},
		catalog.ServiceInstance: {objects},
		catalog.Service:         {objects},
	})
}

// watch is a producer in watch mode, re-exported at [WatchScrape].
func watch(p lifecycle.Producer, factor time.Duration) Holder {
	return Holder{Producer: p, Window: factor * WatchScrape, Scrape: WatchScrape}
}

// HasWatch reports whether any producer of the registry is in watch mode.
func (r *Registry) HasWatch() bool {
	for _, h := range r.producers {
		if h.Watch() {
			return true
		}
	}
	return false
}

// EdgeHolders returns the producers that hold the relation, sorted by name.
func (r *Registry) EdgeHolders(rel catalog.RelationType) []Holder {
	return slices.Clone(r.edges[rel])
}

// EntityHolders returns the producers that hold the entity type, sorted by name.
func (r *Registry) EntityHolders(t catalog.EntityType) []Holder {
	return slices.Clone(r.entities[t])
}

// Window returns the window of a producer and whether the registry knows it.
func (r *Registry) Window(p lifecycle.Producer) (time.Duration, bool) {
	h, ok := r.producers[p]
	return h.Window, ok
}

// Producers returns every producer of the registry with its window and scrape interval,
// sorted by name.
func (r *Registry) Producers() []Holder {
	out := make([]Holder, 0, len(r.producers))
	for _, h := range r.producers {
		out = append(out, h)
	}
	slices.SortFunc(out, func(a, b Holder) int { return strings.Compare(string(a.Producer), string(b.Producer)) })
	return out
}

// holders is the list for a subject, or nil for a subject the registry does not hold.
func (r *Registry) holders(s engine.Subject) []Holder {
	switch s.Kind {
	case engine.SubjectEntity:
		return r.entities[s.A.Type()]
	case engine.SubjectEdge:
		return r.edges[s.Relation]
	}
	return nil
}

// RefreshWindow is the largest window among the producers that hold the subject and
// whose records are refreshes (not watch mode), zero if there is none. A run of refreshes
// is what the ingest-side coalescer extends, so it is what bounds how early an end can
// come out in the coalesced stream.
func (r *Registry) RefreshWindow(s engine.Subject) time.Duration {
	var w time.Duration
	for _, h := range r.holders(s) {
		if !h.Watch() {
			w = max(w, h.Window)
		}
	}
	return w
}

// holds reports whether the producer is registered as a holder of the subject.
func (r *Registry) holds(s engine.Subject, p lifecycle.Producer) (Holder, bool) {
	for _, h := range r.holders(s) {
		if h.Producer == p {
			return h, true
		}
	}
	return Holder{}, false
}

// Canonical is the table as text, one line per producer, relation and entity type, in a
// fixed order: what a registration records.
func (r *Registry) Canonical() string {
	var b strings.Builder
	for _, h := range r.Producers() {
		fmt.Fprintf(&b, "producer %s window %s scrape %s\n", h.Producer, h.Window, h.Scrape)
	}
	rels := make([]catalog.RelationType, 0, len(r.edges))
	for rel := range r.edges {
		rels = append(rels, rel)
	}
	slices.Sort(rels)
	for _, rel := range rels {
		fmt.Fprintf(&b, "relation %s held by %s\n", rel, holderNames(r.edges[rel]))
	}
	kinds := make([]catalog.EntityType, 0, len(r.entities))
	for t := range r.entities {
		kinds = append(kinds, t)
	}
	slices.Sort(kinds)
	for _, t := range kinds {
		fmt.Fprintf(&b, "entity %s held by %s\n", t, holderNames(r.entities[t]))
	}
	return b.String()
}

// Digest is the SHA-256 of [Registry.Canonical], as hex.
func (r *Registry) Digest() string {
	sum := sha256.Sum256([]byte(r.Canonical()))
	return hex.EncodeToString(sum[:])
}

func holderNames(hs []Holder) string {
	names := make([]string, len(hs))
	for i, h := range hs {
		names[i] = string(h.Producer)
	}
	return strings.Join(names, ",")
}
