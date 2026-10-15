package blastscore

import (
	"fmt"
	"regexp"
	"time"

	"github.com/lotannauo/toposhift/internal/identity"
)

// Level is the kind of entity a prediction names.
type Level string

const (
	// LevelService scores services.
	LevelService Level = "service"
	// LevelPod scores pods.
	LevelPod Level = "pod"
)

// Class is what a case's labelled fault says about how it spreads. It comes from
// the labels alone (the fault mechanism and category), never from a graph the
// product builds, so that a recall against the call graph is not circular.
type Class string

const (
	// ClassCallGraph is a fault that spreads by calls between services.
	ClassCallGraph Class = "call-graph"
	// ClassCoResidency is resource exhaustion or interference on a pod or a node.
	ClassCoResidency Class = "co-residency"
)

// Predictor is one way of naming the entities a failing seed reaches.
type Predictor string

const (
	// PredictorW is the blast-radius walk, as the product runs it.
	PredictorW Predictor = "W"
	// PredictorWPod is the evaluation's projection of the walk's service members
	// onto pods. It is an evaluation rule, not a product rule.
	PredictorWPod Predictor = "W->pod"
	// PredictorC is pod co-residency of the seed's pods.
	PredictorC Predictor = "C"
	// PredictorCSvc is service co-residency of the seed service.
	PredictorCSvc Predictor = "C->svc"
	// PredictorWC is the walk's list followed by the co-residency list.
	PredictorWC Predictor = "W+C"
	// PredictorBAll names every entity of the level that existed in the window,
	// less the seed. Its precision is the base rate.
	PredictorBAll Predictor = "B_all"
	// PredictorB1Hop names the services one depends_on edge from the seed, in
	// either direction.
	PredictorB1Hop Predictor = "B_1hop"
	// PredictorBUnion is reverse depends_on reachability over every edge that has
	// a piece in the window, ignoring when. It is a deliberate union-graph
	// comparator, reported as a ceiling and never judged.
	PredictorBUnion Predictor = "B_union"
)

// judged reports whether a stratum of this predictor gets a verdict. The
// baselines are comparators.
func (p Predictor) judged() bool {
	switch p {
	case PredictorW, PredictorWPod, PredictorC, PredictorCSvc, PredictorWC:
		return true
	}
	return false
}

// coresidency reports whether the predictor's list comes from co-residency, so
// that the single-node rule applies to it.
func (p Predictor) coresidency() bool {
	return p == PredictorC || p == PredictorCSvc || p == PredictorWC
}

// Prediction is one entity a predictor names, with its hop distance from the
// seed. A list is ranked, best first, and holds an entity once.
type Prediction struct {
	Entity identity.Fingerprint
	// Hop is 1 for a direct neighbour. It is 0 for a baseline that has no hops.
	Hop int
}

// Truth is the entities a case's ground truth names at one level, after the
// loader has mapped them. It is a set: an entity named twice counts once, and a
// Deployment and a Pod that map to one service are one entry.
type Truth struct {
	// Entities are the mapped truth. They may include the seed, which the scorer
	// removes.
	Entities []identity.Fingerprint
	// Unmappable counts truth entities no exact rule maps to an entity, and
	// Unplaced truth pods the facts do not place. Neither can be predicted, so
	// each is a miss that stays in the truth count.
	Unmappable, Unplaced int
}

// Exclusion says why a case was left out before any fact was read.
type Exclusion string

const (
	// NotExcluded is a case that goes on to be scored.
	NotExcluded Exclusion = ""
	// ExcludedUninstrumentedSeed is a seed service whose pods emit no spans. It
	// is not a failure to load, so it does not count against the dataset rule.
	ExcludedUninstrumentedSeed Exclusion = "uninstrumented_seed"
	// ExcludedUnmappableRoot is a case no labelled entity of which maps to a seed.
	ExcludedUnmappableRoot Exclusion = "unmappable_root"
	// ExcludedNoWindow is a case whose start or end time cannot be found, or
	// whose start is not before its end.
	ExcludedNoWindow Exclusion = "no_window"
	// ExcludedUnreadableLabels is a scenario whose labels are missing or do not
	// parse, so that it has no seed, and ExcludedMissingObjects one with no
	// Kubernetes objects to map its labels through. Like an unmappable root, each
	// stays in the dataset's count and is not a load failure.
	ExcludedUnreadableLabels Exclusion = "unreadable_labels"
	ExcludedMissingObjects   Exclusion = "missing_objects"
)

// Provenance is what the loader knows about a case that the facts do not show.
// The scorer reports it and decides nothing from it.
type Provenance struct {
	// SeedMoved says the seed is the first propagation target that maps, not the
	// labelled fault entity. T0Proxy and T1Proxy say a window end was taken from
	// the alerts, not from the labels.
	SeedMoved bool `json:"seed_moved"`
	T0Proxy   bool `json:"t0_proxy"`
	T1Proxy   bool `json:"t1_proxy"`
	// LabelFilterMapped counts truth entities mapped through a filter the label
	// itself carries. MessagingEdgesOnPath counts producer-to-consumer pairs on
	// the labelled chain, which the failure direction of depends_on cannot carry.
	// NamespaceSplit counts spans whose service has two identities. SpansLoaded
	// and DependsOnPieces say how much of the traces the facts rest on.
	LabelFilterMapped    int `json:"label_filter_mapped"`
	MessagingEdgesOnPath int `json:"messaging_edges_on_path"`
	NamespaceSplit       int `json:"namespace_split"`
	SpansLoaded          int `json:"spans_loaded"`
	DependsOnPieces      int `json:"depends_on_pieces"`
}

// Case is one labelled scenario, ready to score against a store that holds its
// facts. Everything in it is fixed from the labels before the facts are loaded.
type Case struct {
	// ID names the case in results. It must match [CaseIDPattern].
	ID    string
	Class Class
	// From and To are the window [From, To).
	From, To time.Time
	// Seed is the failing entity: a service for a service-level case, or a node or
	// a host, whose k8s.pod members give a pod-level answer directly.
	Seed identity.Fingerprint
	// SeedPods are the pods of a service seed that were alive at From; they seed
	// co-residency. Empty means the pod level cannot be scored.
	SeedPods []identity.Fingerprint
	// Services and Pods are the entities the baselines choose among: every one the
	// facts could name. B_all keeps those that existed in the window.
	Services, Pods []identity.Fingerprint
	// Service and Pod are the truth at each level.
	Service, Pod Truth
	// Nodes is the number of nodes of the cluster, 0 if unknown. A cluster of one
	// node leaves co-residency naming everything, so it is not scored there.
	Nodes int
	// SnapshotTime is when the one Kubernetes snapshot was taken, or zero if the
	// facts are not a snapshot. Outside the window, pod-level facts cannot say.
	SnapshotTime time.Time
	// Exclusion, if not empty, leaves the case out before any fact is read.
	Exclusion Exclusion
	// Provenance is what the loader reports about the case.
	Provenance Provenance
}

// CaseIDPattern is what a case id looks like in results. Results key only by
// this id, so that no entity name can come to stand in for one.
var CaseIDPattern = regexp.MustCompile(`^Scenario-[0-9]+$`)

// validate checks what the scorer relies on, before any fact is read.
func (c Case) validate() error {
	if !CaseIDPattern.MatchString(c.ID) {
		return fmt.Errorf("blastscore: case id %q does not match %s", c.ID, CaseIDPattern)
	}
	if c.Class != ClassCallGraph && c.Class != ClassCoResidency {
		return fmt.Errorf("blastscore: case %s: unknown class %q", c.ID, c.Class)
	}
	if c.Exclusion != NotExcluded {
		return nil
	}
	if !c.From.Before(c.To) {
		return fmt.Errorf("blastscore: case %s: the window [%s, %s) is empty", c.ID, c.From.UTC().Format(time.RFC3339), c.To.UTC().Format(time.RFC3339))
	}
	if c.Seed.IsZero() {
		return fmt.Errorf("blastscore: case %s: no seed", c.ID)
	}
	return nil
}
