package itbench

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"slices"

	"github.com/lotannauo/toposhift/bench/eval/blastscore"
	"github.com/lotannauo/toposhift/bench/eval/factload"
	"github.com/lotannauo/toposhift/internal/catalog"
	"github.com/lotannauo/toposhift/internal/identity"
	"github.com/lotannauo/toposhift/internal/store"
	"github.com/lotannauo/toposhift/internal/store/memstore"
)

// Revision is the revision of the dataset this adapter was written against, and
// Dataset the label results carry for it.
const (
	Revision = "d0916b0"
	Dataset  = "itbench-lite@" + Revision
)

// The files of a scenario that the run reads besides its labels and alerts.
const (
	ObjectsFile = "k8s_objects_raw.tsv"
	TracesFile  = "otel_traces_raw.tsv"
)

// Result is the outcome of one scenario. Every scenario of a dataset has a Run, and
// so is in the dataset's count.
type Result struct {
	Row ARow
	// Unavailable says why the scenario could not be loaded fully: its labels are
	// missing or do not parse, or a file it needs is not there. The run is then an
	// exclusion, or, for a missing traces file, a load failure.
	Unavailable string
	Seed        Seed
	Truth       Truth
	Run         blastscore.CaseRun
	Stats       factload.Stats
}

// Why a scenario was unavailable.
const (
	UnavailableLabels  = "unreadable_ground_truth"
	UnavailableNoLabel = "missing_ground_truth"
	UnavailableObjects = "missing_" + ObjectsFile
	UnavailableTraces  = "missing_" + TracesFile
)

// exclude runs a case that is left out before any fact is read.
func exclude(ctx context.Context, c blastscore.Case, ex blastscore.Exclusion, cfg blastscore.Config) (blastscore.CaseRun, error) {
	c.Exclusion = ex
	return blastscore.RunCase(ctx, nil, c, cfg)
}

// RunScenario loads one scenario into a store of its own, scores it, and discards
// the store. It reads the files of the scenario's directory and nothing else.
//
// pooled is the set of service names that have a span in any trace data read for the
// evaluation (see [PooledServices]). A seed service outside it is uninstrumented and
// the case is excluded before the load check; a seed that has spans somewhere but
// not in this scenario's window fails the load check like any other.
//
// Every scenario yields a run, so that none drops out of the dataset's count: labels
// that are missing or do not parse, or objects that are missing, leave the case
// without a seed and exclude it; a missing traces file leaves the seed absent from
// the store, which is a load failure.
func RunScenario(ctx context.Context, dir string, pooled map[string]bool, cfg blastscore.Config) (Result, error) {
	row, err := ReadTableA(dir)
	if err != nil {
		return Result{}, err
	}
	res := Result{Row: row}
	c := blastscore.Case{ID: row.ID, Class: row.Class}
	if row.Unreadable != "" {
		res.Unavailable = UnavailableLabels
		if row.Missing {
			res.Unavailable = UnavailableNoLabel
		}
		c.Class = blastscore.ClassCallGraph
		res.Run, err = exclude(ctx, c, blastscore.ExcludedUnreadableLabels, cfg)
		return res, err
	}
	c.Provenance.T0Proxy, c.Provenance.T1Proxy = row.Window.T0Proxy, row.Window.T1Proxy
	// A case with no window is left out before anything is read.
	if !row.Window.OK() {
		res.Run, err = exclude(ctx, c, blastscore.ExcludedNoWindow, cfg)
		return res, err
	}
	c.From, c.To = row.Window.From, row.Window.To
	exists := func(f string) (bool, error) {
		_, err := os.Stat(filepath.Join(dir, f))
		if os.IsNotExist(err) {
			return false, nil
		}
		return err == nil, err
	}
	if ok, err := exists(ObjectsFile); err != nil || !ok {
		if err != nil {
			return Result{}, err
		}
		res.Unavailable = UnavailableObjects
		res.Run, err = exclude(ctx, c, blastscore.ExcludedMissingObjects, cfg)
		return res, err
	}

	cl, err := ReadCluster(filepath.Join(dir, ObjectsFile))
	if err != nil {
		return Result{}, err
	}
	res.Seed = PlanSeed(row, cl, c.From)
	if !res.Seed.OK() {
		c.Provenance.SeedMoved = res.Seed.Moved
		res.Run, err = exclude(ctx, c, blastscore.ExcludedUnmappableRoot, cfg)
		return res, err
	}
	if res.Seed.Kind != SeedNode && !pooled[res.Seed.Service] {
		res.Run, err = exclude(ctx, c, blastscore.ExcludedUninstrumentedSeed, cfg)
		return res, err
	}
	var spans []factload.Span
	if ok, err := exists(TracesFile); err != nil || !ok {
		if err != nil {
			return Result{}, err
		}
		res.Unavailable = UnavailableTraces
		if res.Seed.Kind != SeedNode {
			// No spans, so no service in the store: a service seed is absent, a load
			// failure.
			res.Run = blastscore.CaseRun{
				ID: row.ID, Class: row.Class, Outcome: blastscore.OutcomeCannotSay,
				Reason: blastscore.ReasonSeedAbsent, LoadFailed: true,
			}
			res.Run.Diag.Provenance = c.Provenance
			res.Run.Diag.Nodes = len(cl.Nodes)
			return res, nil
		}
		// A node seed's facts are the objects: it is loaded without spans, and the
		// node's existence decides the load check.
	} else {
		spans, err = ReadSpans(filepath.Join(dir, TracesFile))
		if err != nil {
			return Result{}, err
		}
	}
	res.Truth = ChainTruth(row, cl, res.Seed)
	st, err := memstore.Open(memstore.Options{})
	if err != nil {
		return Result{}, err
	}
	defer st.Close()
	res.Stats, err = factload.Load(ctx, st, factload.Scenario{Objects: cl.Load, Spans: spans, Source: Dataset})
	if err != nil {
		return Result{}, fmt.Errorf("%s: %w", row.ID, err)
	}
	c, err = BuildCase(row, cl, res.Seed, res.Truth, res.Stats, pooled)
	if err != nil {
		return Result{}, fmt.Errorf("%s: %w", row.ID, err)
	}
	res.Run, err = blastscore.RunCase(ctx, st, c, cfg)
	if err != nil {
		return Result{}, err
	}
	return res, nil
}

// namespaces resolves a service name to the namespace its spans carry. A name whose
// spans carry none is the service (none, name).
type namespaces struct {
	by     map[string][]string
	shared string
	one    bool
}

func newNamespaces(instrumented map[factload.ServiceKey]struct{}) namespaces {
	n := namespaces{by: map[string][]string{}}
	named := map[string]bool{}
	for k := range instrumented {
		n.by[k.Name] = append(n.by[k.Name], k.Namespace)
		if k.Namespace != "" {
			named[k.Namespace] = true
		}
	}
	if len(instrumented) > 0 && len(named) <= 1 {
		n.one = true
		for ns := range named {
			n.shared = ns
		}
	}
	return n
}

// of returns the namespace of a service name: the one its spans carry, or none if
// they carry none (a name seen both with and without one is the namespaced
// service); for a name with no spans in the scenario, the namespace every service of
// the scenario shares, if they share one. Anything else is not resolved (ok is
// false) and nothing is guessed. inScenario says the name has spans in the scenario.
func (n namespaces) of(name string) (ns string, inScenario, ok bool) {
	got := n.by[name]
	if len(got) == 0 {
		return n.shared, false, n.one
	}
	var named []string
	for _, g := range got {
		if g != "" {
			named = append(named, g)
		}
	}
	switch len(named) {
	case 0:
		return "", true, true
	case 1:
		return named[0], true, true
	}
	return "", true, false
}

// BuildCase makes the case of a scenario from its labels, its mapped objects and
// what the loader found; pooled is as for [RunScenario]. The window, the seed and the truth were fixed from the
// labels and the objects before any span was read.
func BuildCase(row ARow, cl *Cluster, seed Seed, tr Truth, st factload.Stats, pooled map[string]bool) (blastscore.Case, error) {
	rs := identity.NewResolver(catalog.Default())
	fp := func(typ catalog.EntityType, attrs ...identity.Attr) (identity.Fingerprint, error) {
		id, err := rs.Resolve(typ, attrs)
		if err != nil {
			return identity.Fingerprint{}, err
		}
		return id.Fingerprint(), nil
	}
	podFP := func(uid string) (identity.Fingerprint, error) {
		return fp(catalog.K8sPod, identity.Attr{Key: catalog.K8sPodUID, Value: uid})
	}
	svcFP := func(ns, name string) (identity.Fingerprint, error) {
		return fp(catalog.Service, identity.Attr{Key: catalog.ServiceNamespace, Value: ns}, identity.Attr{Key: catalog.ServiceName, Value: name})
	}

	c := blastscore.Case{
		ID: row.ID, Class: row.Class, From: row.Window.From, To: row.Window.To,
		Nodes: len(cl.Nodes), SnapshotTime: cl.Observed,
	}
	p := &c.Provenance
	p.T0Proxy, p.T1Proxy = row.Window.T0Proxy, row.Window.T1Proxy
	p.SeedMoved, p.LabelFilterMapped = seed.Moved, tr.LabelFiltered
	p.NamespaceSplit, p.SpansLoaded, p.DependsOnPieces = st.NamespaceSplit, st.Spans, st.DependsOnRuns

	ns := newNamespaces(st.Instrumented)

	// The entities the baselines choose among.
	for k := range st.Instrumented {
		f, err := svcFP(k.Namespace, k.Name)
		if err != nil {
			return blastscore.Case{}, err
		}
		c.Services = append(c.Services, f)
	}
	slices.SortFunc(c.Services, store.CompareFingerprints)
	for _, o := range cl.Objects {
		if o.Kind != "Pod" || o.UID == "" {
			continue
		}
		f, err := podFP(o.UID)
		if err != nil {
			return blastscore.Case{}, err
		}
		c.Pods = append(c.Pods, f)
	}

	// The seed.
	seedUIDs := map[string]bool{}
	switch seed.Kind {
	case SeedNode:
		f, err := fp(catalog.K8sNode, identity.Attr{Key: catalog.K8sNodeUID, Value: seed.Node.UID})
		if err != nil {
			return blastscore.Case{}, err
		}
		c.Seed = f
	default:
		space, _, ok := ns.of(seed.Service)
		switch {
		case !pooled[seed.Service]:
			// No span of the service in any trace data read.
			c.Exclusion = blastscore.ExcludedUninstrumentedSeed
			return c, nil
		case !ok:
			c.Exclusion = blastscore.ExcludedUnmappableRoot
			return c, nil
		}
		f, err := svcFP(space, seed.Service)
		if err != nil {
			return blastscore.Case{}, err
		}
		c.Seed = f
		for _, o := range seed.Pods {
			pf, err := podFP(o.UID)
			if err != nil {
				return blastscore.Case{}, err
			}
			c.SeedPods = append(c.SeedPods, pf)
			seedUIDs[o.UID] = true
		}
	}

	// The truth, as a set after mapping.
	unplaced := map[identity.Fingerprint]bool{}
	for _, u := range st.Unplaced {
		unplaced[u] = true
	}
	unmappable := tr.Unmappable
	for _, name := range tr.Services {
		space, _, ok := ns.of(name)
		if !ok {
			unmappable++
			continue
		}
		f, err := svcFP(space, name)
		if err != nil {
			return blastscore.Case{}, err
		}
		c.Service.Entities = append(c.Service.Entities, f)
	}
	if seed.Kind == SeedNode {
		c.Pod.Unmappable = unmappable
	} else {
		c.Service.Unmappable = unmappable
	}
	for _, o := range tr.Pods {
		if seedUIDs[o.UID] {
			continue
		}
		f, err := podFP(o.UID)
		if err != nil {
			return blastscore.Case{}, err
		}
		if unplaced[f] {
			c.Pod.Unplaced++
			continue
		}
		c.Pod.Entities = append(c.Pod.Entities, f)
	}

	p.MessagingEdgesOnPath = messagingOnPath(row, cl, ns, st)
	return c, nil
}

// messagingOnPath counts the steps of the labelled chain that are a producer-to-
// consumer pair in the spans, in either direction: a failure does not travel that
// way through depends_on.
func messagingOnPath(row ARow, cl *Cluster, ns namespaces, st factload.Stats) int {
	if len(st.Messaging) == 0 {
		return 0
	}
	services := map[string][]string{}
	of := func(id string) []string {
		if s, ok := services[id]; ok {
			return s
		}
		g, ok := row.GT.Group(id)
		if ok {
			services[id] = MapGroup(cl, g).Services
		}
		return services[id]
	}
	n := 0
	for _, pr := range row.GT.Propagations {
		found := false
		for _, a := range of(pr.Source) {
			for _, b := range of(pr.Target) {
				ka, kb := factload.ServiceKey{Name: a}, factload.ServiceKey{Name: b}
				if sp, _, ok := ns.of(a); ok {
					ka.Namespace = sp
				}
				if sp, _, ok := ns.of(b); ok {
					kb.Namespace = sp
				}
				_, fwd := st.Messaging[factload.PairKey{From: ka, To: kb}]
				_, rev := st.Messaging[factload.PairKey{From: kb, To: ka}]
				found = found || fwd || rev
			}
		}
		if found {
			n++
		}
	}
	return n
}
