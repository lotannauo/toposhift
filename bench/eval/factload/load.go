package factload

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"time"

	"github.com/lotannauo/toposhift/internal/catalog"
	"github.com/lotannauo/toposhift/internal/identity"
	"github.com/lotannauo/toposhift/internal/ingest/k8sobjects"
	"github.com/lotannauo/toposhift/internal/ingest/resume"
	"github.com/lotannauo/toposhift/internal/lifecycle"
	"github.com/lotannauo/toposhift/internal/store"
	"github.com/lotannauo/toposhift/internal/store/memstore"
)

// Version labels this loader in results. A change to what it writes, or to how it
// pairs spans, changes it.
const Version = "factload-2"

// The producers of the two parts of a scenario. Both pass store.WritableProducer.
const (
	// ProducerObjects asserts the pods, nodes and containers of the pulls, and the
	// addresses the pods and nodes claim.
	ProducerObjects lifecycle.Producer = "k8sobjects/eval"
	// ProducerTraces asserts services, instances and dependencies. It is the
	// evaluation's writer: no other producer writes those facts here.
	ProducerTraces lifecycle.Producer = "eval.traces"
)

// ObjectTTL is the TTL of every observation of the objects of a single pull. It is
// an evaluation parameter, not a property of the pull, whose own configuration is
// unknown.
const ObjectTTL = 5 * time.Minute

// Lifetime is how long an object lasts after the last pull that listed it: ObjectTTL
// for a single pull (interval zero), and for a series of pulls at an interval the
// translator's own default, a few intervals.
func Lifetime(interval time.Duration) time.Duration {
	if interval <= 0 {
		return ObjectTTL
	}
	return k8sobjects.DefaultTTLMultiple * interval
}

// ServiceTTL is the TTL of every observation of the service writer, a
// placeholder for the rollups' own expiry, fixed now.
const ServiceTTL = 5 * time.Minute

// batchSize is how many records of the service writer go in one Write.
const batchSize = 10000

// Scenario is what is loaded for one case.
type Scenario struct {
	// Objects are the pods and nodes of the pulls, in the order they were listed.
	Objects []Object
	// PullInterval is the interval between the pulls the objects come from, if they
	// are a series of two or more: the translator is then built with it and sets the
	// lifetime of an object itself, so that an object listed in each pull has no gap
	// between them. Zero is a single pull, whose objects last ObjectTTL.
	PullInterval time.Duration
	// Spans are the scenario's spans, in any order.
	Spans []Span
	// Source names the dataset and revision, for the label an address-associated
	// fact carries, such as "dataset@rev".
	Source string
}

// Target is a store the loader writes: a memstore, or a wrapper of one that
// forwards a lookup to it.
type Target interface {
	store.Store
	NewLookup() (*memstore.Lookup, error)
}

// Stats is what a load found. It holds counts and the fingerprints of pods; none
// is printed by the evaluation.
type Stats struct {
	// Resume is what the resume step decided for the marked records of the
	// snapshot.
	Resume resume.Stats
	// ObjectRecords is the number of records the translator and the resume step
	// left and the loader wrote; Skipped and Notes are what the translator set
	// aside, by reason.
	ObjectRecords int
	Skipped       map[string]int
	Notes         map[string]int
	// Unplaced are the pods whose node was never seen: they have no scheduled_on
	// edge, and a node is never made up for them.
	Unplaced []identity.Fingerprint
	// Spans is the number of spans read, and NamespaceSplit the number with no
	// service.namespace whose name also has spans with one: they are written to
	// nothing and never merged.
	Spans, NamespaceSplit int
	// Pairs are the calls found, SelfPairs those from a service to itself (not
	// written), and ClientsWithoutChild the client and producer spans with no
	// instrumented child.
	Pairs, SelfPairs, ClientsWithoutChild int
	// Services, Instances, Bridges and DependsOn count the records of the service
	// writer by what they are about. DependsOnRuns is the number of maximal runs
	// the dependency records make, ignoring their endpoints.
	ServiceRecords, InstanceRecords, BridgeRecords, DependsOnRecords int
	DependsOnRuns                                                    int
	// Instrumented are the services with at least one span, Calls the dependencies
	// found, and Messaging those found from a producer to a consumer.
	Instrumented map[ServiceKey]struct{}
	Calls        map[PairKey]struct{}
	Messaging    map[PairKey]struct{}
}

// Load writes a scenario into an empty store. The objects of the pulls go first,
// through the product's translator and the resume step; then the spans' records
// are written in event time order.
func Load(ctx context.Context, st Target, sc Scenario) (Stats, error) {
	for _, p := range []lifecycle.Producer{ProducerObjects, ProducerTraces} {
		if err := store.WritableProducer(p); err != nil {
			return Stats{}, err
		}
	}
	if st.LastSeq() != 0 {
		return Stats{}, errors.New("factload: the store is not empty")
	}
	stats := Stats{Skipped: map[string]int{}, Notes: map[string]int{}}
	w := &writer{st: st}
	if err := loadObjects(ctx, st, w, sc.Objects, sc.PullInterval, &stats); err != nil {
		return Stats{}, err
	}
	if err := loadSpans(ctx, w, sc, &stats); err != nil {
		return Stats{}, err
	}
	return stats, nil
}

// writer numbers records after the store's last sequence number and writes them.
type writer struct {
	st  store.Store
	seq uint64
}

func (w *writer) write(ctx context.Context, rs []store.Record) error {
	if len(rs) == 0 {
		return nil
	}
	if w.seq == 0 {
		w.seq = w.st.LastSeq()
	}
	out := slices.Clone(rs)
	for i := range out {
		w.seq++
		out[i].Seq = w.seq
	}
	if err := w.st.Write(ctx, out); err != nil {
		return fmt.Errorf("factload: write: %w", err)
	}
	return nil
}

// loadObjects takes each listed object through the translator, the resume step
// and Translator.Resumed, in that order, and writes what is left. It builds no
// record of its own.
func loadObjects(ctx context.Context, st Target, w *writer, objs []Object, pullInterval time.Duration, stats *Stats) error {
	cfg := k8sobjects.Config{
		Producer: ProducerObjects,
		Resolver: identity.NewResolver(catalog.Default()),
		// A single pull says nothing about an interval, and its objects last an
		// explicit TTL.
		EntityTTL: ObjectTTL,
	}
	if pullInterval > 0 {
		// A series of pulls: the interval is the translator's, and so is the lifetime
		// it gives from it.
		cfg.EntityTTL, cfg.PullInterval = 0, pullInterval
	}
	tr, err := k8sobjects.New(cfg)
	if err != nil {
		return err
	}
	horizon := func(l catalog.Layer) time.Time { return st.LayerHorizon(l).Time }
	for i, o := range objs {
		res, err := tr.Translate(o.Resource, o.Record)
		if err != nil {
			return fmt.Errorf("factload: object %d: %w", i, err)
		}
		for _, s := range res.Skipped {
			stats.Skipped[s.Reason] += s.Count
		}
		for _, n := range res.Notes {
			stats.Notes[n.Reason] += n.Count
		}
		rr := resume.Result{Records: res.Records, Marks: res.Backdated}
		if len(rr.Marks) > 0 {
			rs, err := resumeOne(ctx, st, horizon, &rr, tr)
			if err != nil {
				return fmt.Errorf("factload: object %d: %w", i, err)
			}
			stats.Resume.Add(rs)
		}
		if err := w.write(ctx, rr.Records); err != nil {
			return err
		}
		stats.ObjectRecords += len(rr.Records)
	}
	for _, p := range tr.Pending() {
		stats.Unplaced = append(stats.Unplaced, p.Pod)
	}
	slices.SortFunc(stats.Unplaced, store.CompareFingerprints)
	return nil
}

// resumeOne opens a lookup after the last write and applies the resume step to
// one translator's records of one request.
func resumeOne(ctx context.Context, st Target, horizon func(catalog.Layer) time.Time, rr *resume.Result, tr *k8sobjects.Translator) (resume.Stats, error) {
	lk, err := st.NewLookup()
	if err != nil {
		return resume.Stats{}, fmt.Errorf("resume: opening the lookup: %w", err)
	}
	rs, err := resume.Apply(ctx, lk, horizon, rr, tr, k8sobjects.Comparable)
	if cerr := lk.Close(); cerr != nil && err == nil {
		err = fmt.Errorf("resume: closing the lookup: %w", cerr)
	}
	return rs, err
}

// loadSpans writes the service writer's records, in event time order.
func loadSpans(ctx context.Context, w *writer, sc Scenario, stats *Stats) error {
	f, err := writeSpans(sc.Spans, sc.Source)
	if err != nil {
		return err
	}
	for i := 0; i < len(f.recs); i += batchSize {
		if err := w.write(ctx, f.recs[i:min(i+batchSize, len(f.recs))]); err != nil {
			return err
		}
	}
	stats.Spans, stats.NamespaceSplit = f.spans, f.namespaceSplit
	stats.Pairs, stats.SelfPairs, stats.ClientsWithoutChild = f.pairs, f.selfPairs, f.noChild
	stats.ServiceRecords, stats.InstanceRecords, stats.BridgeRecords, stats.DependsOnRecords = f.services, f.instances, f.bridges, f.dependsOn
	stats.DependsOnRuns = f.dependsOnRuns
	stats.Instrumented, stats.Calls, stats.Messaging = f.instrumented, f.calls, f.messaging
	return nil
}
