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

// Holder is a producer that holds a kind of subject (a relation or an entity type) and
// the window a read names for it. A metrics store has no TTL of its own, so a read says,
// for each producer, how far back a sample still counts as the producer holding the
// subject: the window a user sets from the producer's scrape interval. It is the
// producer's TTL in the spike's streams.
//
// A zero Window is a producer that asserts a fact once and ends it by an explicit
// delete (watch mode). It writes one sample, at the assertion, and no later sample says
// the fact still holds, so no bounded window can read it. A question whose only holders are
// such producers is not modelled yet (package vmquery counts it apart); it is not a row of
// [Capabilities].
type Holder struct {
	Producer lifecycle.Producer
	Window   time.Duration
}

// Watch reports whether the holder has no refresh and so no window.
func (h Holder) Watch() bool { return h.Window == 0 }

// Registry is the registered table of who holds what, and the window each producer is
// read with. It is a value fixed before any data: the same producer has the same window
// wherever it appears, and the converter refuses a record that contradicts it.
type Registry struct {
	edges    map[catalog.RelationType][]Holder
	entities map[catalog.EntityType][]Holder
	windows  map[lifecycle.Producer]time.Duration
}

// NewRegistry builds a registry from the holders of each relation and entity type. A
// producer must have one window everywhere (a whole number of milliseconds, zero for
// watch mode); each list is stored sorted by producer name.
func NewRegistry(edges map[catalog.RelationType][]Holder, entities map[catalog.EntityType][]Holder) (*Registry, error) {
	r := &Registry{
		edges:    map[catalog.RelationType][]Holder{},
		entities: map[catalog.EntityType][]Holder{},
		windows:  map[lifecycle.Producer]time.Duration{},
	}
	add := func(what string, hs []Holder) ([]Holder, error) {
		out := slices.Clone(hs)
		slices.SortFunc(out, func(a, b Holder) int { return strings.Compare(string(a.Producer), string(b.Producer)) })
		for i, h := range out {
			switch {
			case h.Producer == "":
				return nil, fmt.Errorf("%w: %s has a holder with no producer name", ErrRegistry, what)
			case h.Window < 0 || h.Window%time.Millisecond != 0:
				return nil, fmt.Errorf("%w: %s: producer %q has window %s, which is not a whole number of milliseconds", ErrRegistry, what, h.Producer, h.Window)
			case i > 0 && out[i-1].Producer == h.Producer:
				return nil, fmt.Errorf("%w: %s lists producer %q twice", ErrRegistry, what, h.Producer)
			}
			if w, seen := r.windows[h.Producer]; seen && w != h.Window {
				return nil, fmt.Errorf("%w: producer %q has window %s in %s and %s elsewhere", ErrRegistry, h.Producer, h.Window, what, w)
			}
			r.windows[h.Producer] = h.Window
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

// RegistryFor is the registry of a synthetic workload: each producer's window is its
// TTL, which the generator derives from the configuration. The refreshing producers are
// the node collector (every HeartbeatInterval), the traces rollup (every
// RollupInterval), the cluster-level pod collector (every PodHeartbeatInterval), each
// with a TTL of HeartbeatTTLFactor times its interval, and the kubelet's confirmation
// (ConfirmTTL). The fabric and the Kubernetes watch assert once and delete explicitly.
// A producer whose interval is zero is in watch mode, as the generator makes it.
func RegistryFor(c workload.Config) (*Registry, error) {
	ttl := func(interval time.Duration) time.Duration {
		if interval <= 0 {
			return 0
		}
		return time.Duration(c.HeartbeatTTLFactor) * interval
	}
	node := Holder{workload.ProducerNodeCollector, ttl(c.HeartbeatInterval)}
	traces := Holder{workload.ProducerTraces, ttl(c.RollupInterval)}
	pods := Holder{workload.ProducerPodHeartbeat, ttl(c.PodHeartbeatInterval)}
	fabric := Holder{workload.ProducerFabric, 0}
	objects := Holder{workload.ProducerK8sObjects, 0}

	scheduled := []Holder{objects}
	if c.PodHeartbeatInterval > 0 {
		scheduled = append(scheduled, pods)
	}
	if c.ConfirmProbability > 0 {
		scheduled = append(scheduled, Holder{workload.ProducerKubelet, c.ConfirmTTL})
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

// EdgeHolders returns the producers that hold the relation, sorted by name.
func (r *Registry) EdgeHolders(rel catalog.RelationType) []Holder {
	return slices.Clone(r.edges[rel])
}

// EntityHolders returns the producers that hold the entity type, sorted by name.
func (r *Registry) EntityHolders(t catalog.EntityType) []Holder {
	return slices.Clone(r.entities[t])
}

// Window returns the window of a producer and whether the registry knows it. A known
// producer in watch mode has the window zero.
func (r *Registry) Window(p lifecycle.Producer) (time.Duration, bool) {
	w, ok := r.windows[p]
	return w, ok
}

// Producers returns every producer of the registry with its window, sorted by name.
func (r *Registry) Producers() []Holder {
	out := make([]Holder, 0, len(r.windows))
	for p, w := range r.windows {
		out = append(out, Holder{p, w})
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

// SubjectWindow is the largest window among the producers that hold the subject, zero if
// none has one (or the registry does not know the subject). It is what a boundary band
// is made from.
func (r *Registry) SubjectWindow(s engine.Subject) time.Duration {
	var w time.Duration
	for _, h := range r.holders(s) {
		w = max(w, h.Window)
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
		fmt.Fprintf(&b, "producer %s window %s\n", h.Producer, h.Window)
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
