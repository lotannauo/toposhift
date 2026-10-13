package main

import (
	"context"
	"fmt"
	"maps"
	"time"

	"github.com/lotannauo/toposhift/internal/catalog"
	"github.com/lotannauo/toposhift/internal/coalesce"
	"github.com/lotannauo/toposhift/internal/identity"
	"github.com/lotannauo/toposhift/internal/ingest/k8scluster"
	"github.com/lotannauo/toposhift/internal/ingest/k8sobjects"
	"github.com/lotannauo/toposhift/internal/ingest/resume"
	"github.com/lotannauo/toposhift/internal/lifecycle"
	"github.com/lotannauo/toposhift/internal/replay/capture"
	"github.com/lotannauo/toposhift/internal/store"
)

// The pipeline of one pass: each log record goes to one of two translators, their
// records go through the production resume step and then one coalescer, and what is left
// is written, a batch for each line of the capture, to a store of each kind.

const (
	producerObjects lifecycle.Producer = "k8sobjects/k3scap"
	producerCluster lifecycle.Producer = "k8scluster/k3scap"

	// pullInterval is what the collector was told: the TTL of every observation is three of them.
	pullInterval = 5 * time.Minute
	// entityTTL is that TTL, 15 minutes, which the cluster translator derives on its own
	// from the interval its events carry.
	entityTTL = 3 * pullInterval
)

// source says which translator took a record.
type source uint8

const (
	srcObjects source = iota
	srcCluster
	numSources
)

func (s source) String() string {
	if s == srcCluster {
		return "k8scluster"
	}
	return "k8sobjects"
}

// dispatch says which translator a record goes to: an entity event of the cluster
// receiver to the cluster translator, and every other record to the objects translator.
func dispatch(lr capture.LogRecord) source {
	if lr.EventName == "entity.state" || lr.EventName == "entity.delete" {
		return srcCluster
	}
	return srcObjects
}

// coalesceConfig is the setting of a pull every five minutes: a refresh extends its run at
// most once per half its TTL, and a run lasts no longer than that.
func coalesceConfig() coalesce.Config {
	return coalesce.Config{ExtendTTLFraction: 0.5, RunAgeFromTTL: true}
}

// The kinds of subject a run's sets are kept for.
const (
	subjPod = iota
	subjNode
	subjContainer
	numSubjects
)

var subjectNames = [numSubjects]string{"pod", "node", "container"}

func subjectOf(t catalog.EntityType) (int, bool) {
	switch t {
	case catalog.K8sPod:
		return subjPod, true
	case catalog.K8sNode:
		return subjNode, true
	case catalog.Container:
		return subjContainer, true
	}
	return 0, false
}

// passRun is what a pass made of one run.
type passRun struct {
	N int
	// Translated is the store records the translators made, before the coalescer, by
	// source; Written is what the coalescer left, which went to the stores.
	Translated [numSources]int
	Written    int
	// Errors are the records a translator could not read.
	Errors      [numSources]int
	ErrExamples []string
	Skips       [numSources]map[string]int
	Notes       [numSources]map[string]int
	// Subjects are the entities each translator wrote an Observe or a Delete for in this
	// run, by source and kind.
	Subjects [numSources][numSubjects]map[identity.Fingerprint]struct{}
	Batches  int
	// Resume is what the resume step decided in this run.
	Resume resume.Stats
	// Deadlines are, at the end of the run, the latest instant at which the store has a
	// pod alive on the strength of what was written for it: the last recorded refresh
	// plus its TTL, over the records the coalescer left. A pod nothing was written for
	// has none.
	Deadlines map[identity.Fingerprint]time.Time
}

func newPassRun(n int) *passRun {
	pr := &passRun{N: n}
	for s := range numSources {
		pr.Skips[s] = map[string]int{}
		pr.Notes[s] = map[string]int{}
		for k := range numSubjects {
			pr.Subjects[s][k] = map[identity.Fingerprint]struct{}{}
		}
	}
	return pr
}

// deleteInfo says which producers wrote a Delete for an entity, and the first of them.
type deleteInfo struct {
	Mask  uint8 // bit s for source s
	First time.Time
}

// violation is a rule a record or an answer broke, with a few examples.
type violation struct {
	Count    int      `json:"count"`
	Examples []string `json:"examples,omitempty"`
}

const maxExamples = 5

func (v *violation) add(format string, args ...any) {
	v.Count++
	if len(v.Examples) < maxExamples {
		v.Examples = append(v.Examples, fmt.Sprintf(format, args...))
	}
}

// pass is one replay of the whole capture.
type pass struct {
	Name string
	// Restart says that a new pair of translators and a new coalescer are made at each
	// run boundary after the first, as a restart of toposhift together with the collector
	// would: the stores are kept.
	Restart bool

	resolver *identity.Resolver
	objects  *k8sobjects.Translator
	cluster  *k8scluster.Translator
	co       *coalesce.Coalescer
	cfg      coalesce.Config

	mem, peb store.Store
	// lookup opens the resume step's lookup on peb, once per batch, and rec is what peb
	// counted, if it counts.
	lookup func() (resume.Lookup, error)
	rec    *storeCounts
	seq    uint64

	// flushTimes are the times the coalescer is flushed at, in order, on the capture's
	// clock; nextFlush is how many of them have been used.
	flushTimes []time.Time
	nextFlush  int
	// FlushRecords are the records the flushes wrote, which are among those written.
	FlushRecords int

	Runs     []*passRun
	cur      *passRun
	Restarts int

	// Collected from the translators' output, before the coalescer.
	deletes    map[identity.Fingerprint]*deleteInfo
	lastAssert map[identity.Fingerprint]time.Time
	// seenAt is the time of the latest record of each translator that made an Observe
	// of each entity.
	seenAt [numSources]map[identity.Fingerprint]time.Time
	// podDeadline is the same as passRun.Deadlines, over the runs so far.
	podDeadline map[identity.Fingerprint]time.Time
	podsBy      [numSources]map[identity.Fingerprint]struct{}
	ctrsBy      [numSources]map[identity.Fingerprint]struct{}
	ctrPod      [numSources]map[identity.Fingerprint]identity.Fingerprint

	// Properties of the records.
	Invalid violation // a record that fails Validate, before or after the coalescer
	Lapsed  violation // an Observe that lapses before its observed time plus its TTL
}

// newPass makes a pass over the two stores, which it does not own; lookup opens the
// lookup of the resume step, on peb.
func newPass(name string, restart bool, mem, peb store.Store, lookup func() (resume.Lookup, error)) (*pass, error) {
	p := &pass{
		Name: name, Restart: restart, mem: mem, peb: peb, lookup: lookup, cfg: coalesceConfig(),
		resolver:    identity.NewResolver(catalog.Default()),
		deletes:     map[identity.Fingerprint]*deleteInfo{},
		lastAssert:  map[identity.Fingerprint]time.Time{},
		podDeadline: map[identity.Fingerprint]time.Time{},
	}
	for s := range numSources {
		p.seenAt[s] = map[identity.Fingerprint]time.Time{}
		p.podsBy[s] = map[identity.Fingerprint]struct{}{}
		p.ctrsBy[s] = map[identity.Fingerprint]struct{}{}
		p.ctrPod[s] = map[identity.Fingerprint]identity.Fingerprint{}
	}
	if err := p.reset(); err != nil {
		return nil, err
	}
	return p, nil
}

// reset makes the translators and the coalescer anew.
func (p *pass) reset() (err error) {
	if p.objects, err = k8sobjects.New(k8sobjects.Config{
		Producer: producerObjects, Resolver: p.resolver, PullInterval: pullInterval,
	}); err != nil {
		return err
	}
	if p.cluster, err = k8scluster.New(k8scluster.Config{
		Producer: producerCluster, Resolver: p.resolver, CollectionInterval: pullInterval,
	}); err != nil {
		return err
	}
	p.co, err = coalesce.New(p.cfg)
	return err
}

// startRun begins the records of run n. In a pass that restarts, every run after the
// first begins with new translators and a new coalescer, after a graceful stop: the
// coalescer writes every refresh it has absorbed, and those records belong to the run
// that ends, and to its deadlines.
func (p *pass) startRun(ctx context.Context, n int) error {
	restart := p.Restart && len(p.Runs) > 0
	if restart {
		if err := p.flushAll(ctx, fmt.Sprintf("flush before the restart that begins run %d", n)); err != nil {
			return err
		}
	}
	p.snapshot()
	if restart {
		if err := p.reset(); err != nil {
			return err
		}
		p.Restarts++
	}
	p.cur = newPassRun(n)
	p.Runs = append(p.Runs, p.cur)
	return nil
}

// noteDeadlines takes the records the coalescer left, which the stores are to be given,
// and keeps for each pod the latest instant at which the store has it alive on their
// strength: the last instant a record asserts it (its Through if it has one) plus its TTL.
func (p *pass) noteDeadlines(written []store.Record) {
	for _, r := range written {
		if r.Subject.Kind == store.SubjectEntity && r.Subject.A.Type() == catalog.K8sPod && r.Kind == lifecycle.Observe && r.TTL > 0 {
			if d := endOf(r).Add(r.TTL); d.After(p.podDeadline[r.Subject.A]) {
				p.podDeadline[r.Subject.A] = d
			}
		}
	}
}

// snapshot keeps the pods' deadlines as the current run leaves them.
func (p *pass) snapshot() {
	if p.cur != nil {
		p.cur.Deadlines = maps.Clone(p.podDeadline)
	}
}

// srcTime is the time a record says it describes: the observed time of an objects
// record, and the time of an entity event.
func srcTime(s source, lr capture.LogRecord) time.Time {
	if s == srcCluster {
		return time.Unix(0, int64(lr.TimeUnixNano)).UTC()
	}
	return time.Unix(0, int64(lr.ObservedTimeUnixNano)).UTC()
}

// endOf is the last instant an Observe asserts the subject at, before its TTL: its event
// time, or its Through if it has one.
func endOf(r store.Record) time.Time {
	if r.Through.After(r.EventTime) {
		return r.Through
	}
	return r.EventTime
}

// translated is what one record of a batch gave its translator: the store records,
// and those of them the translator marked backdated.
type translated struct {
	it    item
	src   source
	recs  []store.Record
	marks []resume.Mark
}

// translate takes one record through its translator.
func (p *pass) translate(it item) (translated, error) {
	src := dispatch(it.rec)
	pr := p.cur
	if src == srcCluster {
		res, err := p.cluster.Translate(it.res, it.rec, time.Time{})
		if err != nil {
			return translated{src: src}, err
		}
		for _, s := range res.Skipped {
			pr.Skips[src][s.Reason] += s.Count
		}
		for _, n := range res.Notes {
			pr.Notes[src][n.Reason] += n.Count
		}
		return translated{it: it, src: src, recs: res.Records, marks: res.Backdated}, nil
	}
	res, err := p.objects.Translate(it.res, it.rec)
	if err != nil {
		return translated{src: src}, err
	}
	for _, s := range res.Skipped {
		pr.Skips[src][s.Reason] += s.Count
	}
	for _, n := range res.Notes {
		pr.Notes[src][n.Reason] += n.Count
	}
	return translated{it: it, src: src, recs: res.Records, marks: res.Backdated}, nil
}

// resume takes each translator's records of a batch through the production resume step,
// as the sequencer does for a request: one lookup for the batch, opened after the last
// write, and one call per translator, with its records in the order they were translated.
// The records each call keeps go back to the record that gave them.
func (p *pass) resume(ctx context.Context, ts []translated) (err error) {
	if p.lookup == nil {
		return nil
	}
	lk, err := p.lookup()
	if err != nil {
		return fmt.Errorf("resume: opening the lookup: %w", err)
	}
	defer func() {
		if cerr := lk.Close(); cerr != nil && err == nil {
			err = fmt.Errorf("resume: closing the lookup: %w", cerr)
		}
	}()
	horizon := func(l catalog.Layer) time.Time { return p.peb.LayerHorizon(l).Time }
	for src := range numSources {
		var res resume.Result
		var idx []int
		for i := range ts {
			if ts[i].src != src {
				continue
			}
			base := len(res.Records)
			res.Records = append(res.Records, ts[i].recs...)
			for _, m := range ts[i].marks {
				m.Index += base
				res.Marks = append(res.Marks, m)
			}
			idx = append(idx, i)
		}
		if len(res.Marks) == 0 {
			continue
		}
		var tr resume.Resumer = p.objects
		described := k8sobjects.Comparable
		if src == srcCluster {
			tr, described = p.cluster, k8scluster.Comparable
		}
		st, err := resume.Apply(ctx, lk, horizon, &res, tr, described)
		if err != nil {
			return err
		}
		p.cur.Resume.Add(st)
		dropped := res.Dropped
		pos, k := 0, 0
		for _, i := range idx {
			var kept []store.Record
			for range ts[i].recs {
				if len(dropped) > 0 && dropped[0] == pos {
					dropped = dropped[1:]
				} else {
					kept = append(kept, res.Records[k])
					k++
				}
				pos++
			}
			ts[i].recs = kept
		}
	}
	return nil
}

// observeRecords takes note of what a translator wrote for a record.
//
// An Observe must not lapse before the time its source describes plus its TTL. For an
// entity that is the time of the record that made it, and so it is for an edge made by a
// record of the entity it is stored on. An edge made by another record (a pending edge,
// resolved when its node arrives) is asserted through the latest record of the entity it
// is stored on, a pod, because the node's arrival says nothing about the pod.
func (p *pass) observeRecords(src source, st time.Time, recs []store.Record) {
	pr := p.cur
	pr.Translated[src] += len(recs)
	inCall := map[identity.Fingerprint]struct{}{}
	for _, r := range recs {
		if r.Subject.Kind == store.SubjectEntity && r.Kind == lifecycle.Observe {
			inCall[r.Subject.A] = struct{}{}
		}
	}
	for _, r := range recs {
		if err := r.Validate(); err != nil {
			p.Invalid.add("%s before the coalescer: %v", src, err)
		}
		if r.Kind == lifecycle.Observe && r.TTL > 0 {
			due := st
			if _, ok := inCall[r.Subject.A]; !ok {
				due = p.seenAt[src][r.Subject.A]
			}
			if endOf(r).Before(due) {
				p.Lapsed.add("%s %s subject kind %d: event time %s, through %s, source time %s", src, r.Subject.A.Type(), r.Subject.Kind,
					r.EventTime.Format(time.RFC3339Nano), r.Through.Format(time.RFC3339Nano), due.Format(time.RFC3339Nano))
			}
		}
		switch r.Subject.Kind {
		case store.SubjectEntity:
			fp := r.Subject.A
			if k, ok := subjectOf(fp.Type()); ok {
				pr.Subjects[src][k][fp] = struct{}{}
			}
			switch fp.Type() {
			case catalog.K8sPod:
				switch r.Kind {
				case lifecycle.Delete:
					di := p.deletes[fp]
					if di == nil {
						di = &deleteInfo{First: r.EventTime}
						p.deletes[fp] = di
					}
					di.Mask |= 1 << src
					if r.EventTime.Before(di.First) {
						di.First = r.EventTime
					}
				case lifecycle.Observe:
					p.podsBy[src][fp] = struct{}{}
					if e := endOf(r); e.After(p.lastAssert[fp]) {
						p.lastAssert[fp] = e
					}
				}
			case catalog.Container:
				if r.Kind == lifecycle.Observe {
					p.ctrsBy[src][fp] = struct{}{}
				}
			}
		case store.SubjectEdge:
			if r.Subject.Relation == catalog.PartOf && r.Subject.A.Type() == catalog.Container && r.Kind == lifecycle.Observe {
				p.ctrPod[src][r.Subject.A] = r.Subject.B
			}
		}
	}
	for fp := range inCall {
		if st.After(p.seenAt[src][fp]) {
			p.seenAt[src][fp] = st
		}
	}
}

// feed takes the records of one line: translates them, resumes what the translators
// backdated, coalesces what they made, and writes the batch to both stores.
func (p *pass) feed(ctx context.Context, b batch) error {
	pr := p.cur
	pr.Batches++
	if t, ok := p.batchTime(b); ok {
		if err := p.flushTo(ctx, t); err != nil {
			return err
		}
	}
	var ts []translated
	for _, it := range b.Items {
		tr, err := p.translate(it)
		if err != nil {
			pr.Errors[tr.src]++
			if len(pr.ErrExamples) < maxExamples {
				pr.ErrExamples = append(pr.ErrExamples, fmt.Sprintf("line %d: %s: %v", p.lineNumber(b), tr.src, err))
			}
			continue
		}
		ts = append(ts, tr)
	}
	if err := p.resume(ctx, ts); err != nil {
		return fmt.Errorf("batch of line %d of run %d: %w", p.lineNumber(b), pr.N, err)
	}
	var out []store.Record
	for _, tr := range ts {
		p.observeRecords(tr.src, srcTime(tr.src, tr.it.rec), tr.recs)
		for _, r := range tr.recs {
			out = p.co.Add(r, out)
		}
	}
	return p.write(ctx, out, fmt.Sprintf("batch of line %d of run %d", p.lineNumber(b), pr.N))
}

// write gives the records the coalescer left to both stores: it numbers them in order,
// validates them, notes the pods' deadlines, and counts them in the current run. what
// says where they come from, for an error.
func (p *pass) write(ctx context.Context, out []store.Record, what string) error {
	if len(out) == 0 {
		return nil
	}
	for i := range out {
		p.seq++
		out[i].Seq = p.seq
		if err := out[i].Validate(); err != nil {
			p.Invalid.add("after the coalescer: %v", err)
		}
	}
	p.cur.Written += len(out)
	p.noteDeadlines(out)
	if err := p.mem.Write(ctx, out); err != nil {
		return fmt.Errorf("memstore: %s: %w", what, err)
	}
	if err := p.peb.Write(ctx, out); err != nil {
		return fmt.Errorf("pebblestore: %s: %w", what, err)
	}
	return nil
}

// batchTime is the time of a batch on the capture's clock: the latest time any of its
// records says it describes, which for the collector is what it observed.
func (p *pass) batchTime(b batch) (t time.Time, ok bool) {
	for _, it := range b.Items {
		if st := srcTime(dispatch(it.rec), it.rec); !ok || st.After(t) {
			t, ok = st, true
		}
	}
	return t, ok
}

// flushTo flushes the coalescer at each of the flush times, not yet used, that are at
// or before t, in order.
func (p *pass) flushTo(ctx context.Context, t time.Time) error {
	for p.nextFlush < len(p.flushTimes) && !p.flushTimes[p.nextFlush].After(t) {
		now := p.flushTimes[p.nextFlush]
		p.nextFlush++
		out := p.co.Flush(now, nil)
		p.FlushRecords += len(out)
		if err := p.write(ctx, out, fmt.Sprintf("flush at %s", now.Format(time.RFC3339))); err != nil {
			return err
		}
	}
	return nil
}

// flushRest flushes at every flush time not yet used: the end of the capture.
func (p *pass) flushRest(ctx context.Context) error {
	if n := len(p.flushTimes); n > 0 {
		return p.flushTo(ctx, p.flushTimes[n-1])
	}
	return nil
}

// flushAll writes every refresh the coalescer has absorbed, as it does when it stops.
func (p *pass) flushAll(ctx context.Context, what string) error {
	out := p.co.FlushAll(nil)
	p.FlushRecords += len(out)
	return p.write(ctx, out, what)
}

func (p *pass) lineNumber(b batch) int { return b.Line + 1 }
