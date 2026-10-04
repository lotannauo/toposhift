package workload

import (
	"cmp"
	"fmt"
	"io"
	"math/bits"
	"slices"
	"sort"
	"text/tabwriter"
	"time"

	"github.com/lotannauo/toposhift/bench/spike/engine"
	"github.com/lotannauo/toposhift/internal/catalog"
	"github.com/lotannauo/toposhift/internal/identity"
	"github.com/lotannauo/toposhift/internal/lifecycle"
)

// Stats about a stream: what it would make a store do, before any store is
// measured, so that whoever reads a result can first ask whether the workload
// is believable. An [Analyzer] watches records go by and keeps one small entry
// per prefix (a layer, an entity and a direction: what one read touches).

// Side is which of an entity's prefixes a record lands in.
type Side uint8

const (
	SideExistence Side = iota // the entity's own existence
	SideForward               // edges leaving the entity
	SideReverse               // edges arriving at the entity
)

func (s Side) String() string {
	switch s {
	case SideForward:
		return "->"
	case SideReverse:
		return "<-"
	default:
		return "exists"
	}
}

// Class is a kind of prefix: all the prefixes of one entity type, in one layer
// and on one side. Hubs (hosts, nodes, services and racks) are the classes that
// collect many peers.
type Class struct {
	Layer catalog.Layer
	Owner catalog.EntityType
	Side  Side
}

func (c Class) String() string { return fmt.Sprintf("%s %s %s", c.Layer, c.Owner, c.Side) }

func (c Class) hub() bool {
	switch c.Owner {
	case catalog.Host, catalog.K8sNode, catalog.Service, catalog.Rack:
		return c.Side != SideExistence
	}
	return false
}

type prefixKey struct {
	fp    identity.Fingerprint
	layer catalog.Layer
	side  Side
}

// prefixStat is what is kept for one prefix. It is small, because with fresh
// identities there are millions.
type prefixStat struct {
	observes, extensions, deletes uint32
	// kept and keptExt are the records, and of them the run extensions, with an event
	// time at or after the retained-from instant: what a store that has retained up
	// to it still holds in the prefix.
	kept, keptExt uint32
	behind        uint32 // records older than the newest already in the prefix
	ties          uint32 // records at exactly the newest instant
	maxEvent      int64
	days          []uint32
	peers         map[identity.Fingerprint]struct{} // hubs only
}

func (p *prefixStat) records() uint32 { return p.observes + p.extensions + p.deletes }

// edgeState is the latest word of one producer about one edge, for working out
// which edges are alive at the end.
type edgeState struct {
	event    int64
	deadline int64 // zero: until deleted
	dead     bool  // deleted; kept for a while so a late older record cannot revive it
}

type edgeKey struct {
	a, b     identity.Fingerprint
	rel      catalog.RelationType
	producer lifecycle.Producer
}

// Analyzer accumulates the statistics of a stream. It is not safe for concurrent
// use, and it is meant for streams that are written once and read once: feed it
// the records in arrival order.
type Analyzer struct {
	start    time.Time
	duration time.Duration
	days     int

	prefixes map[prefixKey]*prefixStat
	edges    map[edgeKey]edgeState             // possibly alive, and recently deleted
	entities map[identity.Fingerprint]struct{} // every entity seen

	records, observes, extensions, deletes uint64
	payloadBytes                           uint64
	perLayer                               [5]uint64 // indexed by catalog.Layer, which starts at 1
	frontier                               int64
	late                                   uint64
	lateness                               logHist
	lastPrune                              int64

	// retainedFrom is the instant before which a store discards history, if it was
	// told (see SetRetainedFrom).
	retainedFrom    int64
	hasRetainedFrom bool

	// what is live at the end, worked out when asked and kept until the next record
	liveEdges map[prefixKey]map[identity.Fingerprint]struct{}
	liveEnts  map[identity.Fingerprint]bool
}

// NewAnalyzer is for a stream that starts at start and lasts about duration;
// records outside that period are counted into the nearest day.
func NewAnalyzer(start time.Time, duration time.Duration) *Analyzer {
	return &Analyzer{
		start: start, duration: duration,
		days:     max(1, int((duration+24*time.Hour-1)/(24*time.Hour))),
		prefixes: make(map[prefixKey]*prefixStat),
		edges:    make(map[edgeKey]edgeState),
		entities: make(map[identity.Fingerprint]struct{}),
	}
}

func (a *Analyzer) prefix(k prefixKey, peer identity.Fingerprint, hub bool) *prefixStat {
	p := a.prefixes[k]
	if p == nil {
		p = &prefixStat{days: make([]uint32, a.days)}
		if hub {
			p.peers = make(map[identity.Fingerprint]struct{})
		}
		a.prefixes[k] = p
	}
	if hub {
		p.peers[peer] = struct{}{}
	}
	return p
}

// SetRetainedFrom tells the analyzer the instant before which a store will have
// discarded history by the end of the stream (the last retention horizon), so that
// [Analyzer.Pick] can rank prefixes by what a store still holds in them and not by
// the whole stream: a prefix with one unbroken run that a retention folds into its
// baseline is the busiest over the whole stream and the cheapest to read after it.
// Call it before the first record.
func (a *Analyzer) SetRetainedFrom(t time.Time) {
	a.retainedFrom, a.hasRetainedFrom = t.UnixNano(), true
}

// Add counts one record.
func (a *Analyzer) Add(r engine.Record) {
	a.liveEdges, a.liveEnts = nil, nil
	a.records++
	a.payloadBytes += uint64(len(r.Payload))
	if int(r.Layer) < len(a.perLayer) {
		a.perLayer[r.Layer]++
	}
	ev := r.EventTime.UnixNano()
	extension := r.Kind == lifecycle.Observe && !r.Through.IsZero()
	// An extension re-asserts a run at the run's first event time, so it is always
	// far behind the newest record; it is counted per prefix as behind, where it
	// matters, and is not lateness, which is about records arriving after later
	// ones.
	switch {
	case extension:
	case ev < a.frontier:
		a.late++
		a.lateness.add(a.frontier - ev)
	default:
		a.frontier = ev
	}
	switch {
	case extension:
		a.extensions++
	case r.Kind == lifecycle.Observe:
		a.observes++
	default:
		a.deletes++
	}

	day := min(max(int(r.EventTime.Sub(a.start)/(24*time.Hour)), 0), a.days-1)
	touch := func(owner, peer identity.Fingerprint, s Side) {
		c := Class{Layer: r.Layer, Owner: owner.Type(), Side: s}
		p := a.prefix(prefixKey{owner, r.Layer, s}, peer, c.hub())
		switch {
		case extension:
			p.extensions++
		case r.Kind == lifecycle.Observe:
			p.observes++
		default:
			p.deletes++
		}
		if p.records() > 1 { // not the first record of the prefix
			switch {
			case ev < p.maxEvent:
				p.behind++
			case ev == p.maxEvent:
				p.ties++
			}
		}
		if !a.hasRetainedFrom || ev >= a.retainedFrom {
			p.kept++
			if extension {
				p.keptExt++
			}
		}
		p.maxEvent = max(p.maxEvent, ev)
		p.days[day]++
	}

	if r.Subject.Kind == engine.SubjectEntity {
		touch(r.Subject.A, identity.Fingerprint{}, SideExistence)
		a.entities[r.Subject.A] = struct{}{}
	} else {
		touch(r.Subject.A, r.Subject.B, SideForward)
		touch(r.Subject.B, r.Subject.A, SideReverse)
	}

	// The latest word of this producer on this edge, or on this entity's existence
	// (an entity has no second end and no relation), kept while it may be alive.
	k := edgeKey{r.Subject.A, r.Subject.B, r.Subject.Relation, r.Producer}
	if old, ok := a.edges[k]; ok && ev < old.event {
		return
	}
	st := edgeState{event: ev, dead: r.Kind != lifecycle.Observe}
	if r.Kind == lifecycle.Observe && r.TTL > 0 {
		from := r.EventTime
		if r.Through.After(from) {
			from = r.Through
		}
		st.deadline = from.Add(r.TTL).UnixNano()
	}
	a.edges[k] = st
	if ev-a.lastPrune > int64(time.Hour) {
		a.lastPrune = ev
		for k, st := range a.edges {
			// An hour behind the newest record is later than any record in these
			// workloads arrives.
			if (st.dead && st.event < a.frontier-int64(time.Hour)) || (st.deadline != 0 && st.deadline < a.frontier-int64(time.Hour)) {
				delete(a.edges, k)
			}
		}
	}
}

// logHist is a histogram of nanosecond durations in powers of two, enough for
// the order of magnitude of a median and a 99th percentile.
type logHist struct {
	buckets [64]uint64
	n       uint64
}

func (h *logHist) add(ns int64) {
	h.buckets[bits.Len64(uint64(ns))]++
	h.n++
}

// quantile is the upper bound of the bucket holding the q-th quantile.
func (h *logHist) quantile(q float64) time.Duration {
	if h.n == 0 {
		return 0
	}
	target := uint64(q * float64(h.n))
	var seen uint64
	for i, c := range h.buckets {
		seen += c
		if seen > target {
			return time.Duration(uint64(1)<<i - 1)
		}
	}
	return 0
}

// Dist is the median, the 99th percentile and the maximum of a set of numbers.
type Dist struct{ P50, P99, Max uint32 }

func dist(xs []uint32) Dist {
	if len(xs) == 0 {
		return Dist{}
	}
	slices.Sort(xs)
	at := func(q float64) uint32 { return xs[min(int(q*float64(len(xs))), len(xs)-1)] }
	return Dist{at(0.5), at(0.99), xs[len(xs)-1]}
}

// History windows, in days, whose retained history is reported.
var historyDays = []int{2, 7, 14, 30}

// ClassStats are the numbers for one [Class].
type ClassStats struct {
	Class      Class
	Prefixes   int
	Observes   uint64
	Extensions uint64
	Deletes    uint64
	Behind     uint64 // records older than the newest in their prefix: what invalidates a later checkpoint
	Ties       uint64
	Peers      Dist // distinct peers ever seen per prefix; hubs only
	LiveDegree Dist // peers with a live reference at the end; edge classes only
	// History is, for each window in historyDays that the stream is long enough
	// for, the records held within that many days of the end, over the prefixes
	// that hold any (a prefix with nothing in the window keeps only a baseline
	// after a retention); HistoryActive is how many prefixes that is.
	History       map[int]Dist
	HistoryActive map[int]int
}

// PrefixTop is one of the busiest prefixes.
type PrefixTop struct {
	Class       Class
	Fingerprint identity.Fingerprint
	Records     uint32
	Extensions  uint32
	Behind      uint32
	// Kept and KeptExtensions are the records, and the run extensions among them,
	// that a store holds after the final retention (all of them if the analyzer was
	// not told one).
	Kept, KeptExtensions uint32
}

// Report is what an [Analyzer] found.
type Report struct {
	Duration                 time.Duration
	Records, Observes        uint64
	Extensions, Deletes      uint64
	PayloadBytes             uint64
	PerLayer                 [5]uint64 // indexed by catalog.Layer, which starts at 1
	LateShare                float64
	LatenessP50, LatenessP99 time.Duration
	Entities, EntitiesAlive  int
	Classes                  []ClassStats
	TopByRecords             []PrefixTop
	TopByExtensions          []PrefixTop

	// DeadlineIndexBytesPerDay estimates a secondary index ordered by deadline
	// that a run extension would have to rewrite: about 115 bytes (an entry and
	// the tombstone of the one it replaces) per extension.
	DeadlineIndexBytesPerDay float64
}

// liveAtEnd takes the end of the stream as now and returns, per prefix, the
// peers with a live reference (a peer held up by two producers counts once), and
// the entities that exist.
func (a *Analyzer) liveAtEnd() (map[prefixKey]map[identity.Fingerprint]struct{}, map[identity.Fingerprint]bool) {
	if a.liveEdges != nil {
		return a.liveEdges, a.liveEnts
	}
	end := a.start.Add(a.duration).UnixNano()
	live := map[prefixKey]map[identity.Fingerprint]struct{}{}
	for k, st := range a.edges {
		if k.rel == "" || st.dead || (st.deadline != 0 && st.deadline <= end) {
			continue
		}
		for _, pk := range []struct {
			key  prefixKey
			peer identity.Fingerprint
		}{{prefixKey{k.a, layerOf(k.rel), SideForward}, k.b}, {prefixKey{k.b, layerOf(k.rel), SideReverse}, k.a}} {
			if live[pk.key] == nil {
				live[pk.key] = map[identity.Fingerprint]struct{}{}
			}
			live[pk.key][pk.peer] = struct{}{}
		}
	}
	alive := map[identity.Fingerprint]bool{}
	for k, st := range a.edges {
		if k.rel == "" && !st.dead && (st.deadline == 0 || st.deadline > end) {
			alive[k.a] = true
		}
	}
	a.liveEdges, a.liveEnts = live, alive
	return live, alive
}

// Report summarizes the stream seen so far. The live degree takes the end of
// the stream as now.
func (a *Analyzer) Report() Report {
	live, aliveEntity := a.liveAtEnd()

	rep := Report{
		Duration: a.duration, Records: a.records, Observes: a.observes, Extensions: a.extensions, Deletes: a.deletes,
		PayloadBytes: a.payloadBytes, PerLayer: a.perLayer,
		LatenessP50: a.lateness.quantile(0.5), LatenessP99: a.lateness.quantile(0.99),
		Entities: len(a.entities),
	}
	if a.records > 0 {
		rep.LateShare = float64(a.late) / float64(a.records)
	}
	rep.EntitiesAlive = len(aliveEntity)
	if d := a.duration.Hours() / 24; d > 0 {
		rep.DeadlineIndexBytesPerDay = float64(a.extensions) * 115 / d
	}

	byClass := map[Class]*ClassStats{}
	values := map[Class]*struct {
		peers, degree []uint32
		history       map[int][]uint32
	}{}
	for k, p := range a.prefixes {
		c := Class{Layer: k.layer, Owner: k.fp.Type(), Side: k.side}
		cs := byClass[c]
		if cs == nil {
			cs = &ClassStats{Class: c, History: map[int]Dist{}, HistoryActive: map[int]int{}}
			byClass[c] = cs
			values[c] = &struct {
				peers, degree []uint32
				history       map[int][]uint32
			}{history: map[int][]uint32{}}
		}
		v := values[c]
		cs.Prefixes++
		cs.Observes += uint64(p.observes)
		cs.Extensions += uint64(p.extensions)
		cs.Deletes += uint64(p.deletes)
		cs.Behind += uint64(p.behind)
		cs.Ties += uint64(p.ties)
		if p.peers != nil {
			v.peers = append(v.peers, uint32(len(p.peers)))
		}
		if k.side != SideExistence {
			v.degree = append(v.degree, uint32(len(live[k])))
		}
		for _, r := range historyDays {
			if r > a.days {
				continue
			}
			var n uint32
			for _, d := range p.days[a.days-r:] {
				n += d
			}
			if n > 0 {
				v.history[r] = append(v.history[r], n)
			}
		}
		rep.consider(c, k.fp, p)
	}
	for c, cs := range byClass {
		v := values[c]
		cs.Peers, cs.LiveDegree = dist(v.peers), dist(v.degree)
		for r, xs := range v.history {
			cs.History[r], cs.HistoryActive[r] = dist(xs), len(xs)
		}
		rep.Classes = append(rep.Classes, *cs)
	}
	sort.Slice(rep.Classes, func(i, j int) bool {
		x, y := rep.Classes[i], rep.Classes[j]
		if x.Class.Layer != y.Class.Layer {
			return x.Class.Layer < y.Class.Layer
		}
		if x.Class.Owner != y.Class.Owner {
			return x.Class.Owner < y.Class.Owner
		}
		return x.Class.Side < y.Class.Side
	})
	rep.TopByRecords = topN(rep.TopByRecords, func(p PrefixTop) uint32 { return p.Records })
	rep.TopByExtensions = topN(rep.TopByExtensions, func(p PrefixTop) uint32 { return p.Extensions })
	return rep
}

// Picks are prefixes of one class chosen by how much history they hold, for a
// measurement that has to read the same prefixes in every store.
type Picks struct {
	// Prefixes is how many prefixes the class has.
	Prefixes int
	// ByRecords are the busiest by records held after the retention set with
	// SetRetainedFrom (all of them if none), most first; ByExtensions the busiest
	// by run extensions held (only prefixes with any), most first. Median are the
	// prefixes around the middle of the class when ranked by records held, busiest
	// first. Equal counts are ordered by fingerprint, so the choice is the same on
	// every run.
	ByRecords, ByExtensions, Median []PrefixTop
}

// Pick chooses up to hot prefixes of the class by records and by extensions, and
// median prefixes around the middle by records. With live set, only prefixes that
// exist at the end of the stream are considered: an edge prefix with a live peer,
// an entity that is alive.
func (a *Analyzer) Pick(c Class, hot, median int, live bool) Picks {
	var peers map[prefixKey]map[identity.Fingerprint]struct{}
	var alive map[identity.Fingerprint]bool
	if live {
		peers, alive = a.liveAtEnd()
	}
	var all []PrefixTop
	for k, p := range a.prefixes {
		if k.layer == c.Layer && k.fp.Type() == c.Owner && k.side == c.Side {
			if live && ((k.side == SideExistence && !alive[k.fp]) || (k.side != SideExistence && len(peers[k]) == 0)) {
				continue
			}
			all = append(all, PrefixTop{Class: c, Fingerprint: k.fp, Records: p.records(), Extensions: p.extensions, Behind: p.behind, Kept: p.kept, KeptExtensions: p.keptExt})
		}
	}
	rank := func(key func(PrefixTop) uint32) func(x, y PrefixTop) int {
		return func(x, y PrefixTop) int {
			if d := cmp.Compare(key(y), key(x)); d != 0 {
				return d
			}
			return engine.CompareFingerprints(x.Fingerprint, y.Fingerprint)
		}
	}
	slices.SortFunc(all, rank(func(p PrefixTop) uint32 { return p.Kept }))
	out := Picks{Prefixes: len(all), ByRecords: slices.Clone(all[:min(hot, len(all))])}
	if m := min(median, len(all)); m > 0 {
		from := min(max(len(all)/2-m/2, 0), len(all)-m)
		out.Median = slices.Clone(all[from : from+m])
	}
	withExt := slices.DeleteFunc(slices.Clone(all), func(p PrefixTop) bool { return p.KeptExtensions == 0 })
	slices.SortFunc(withExt, rank(func(p PrefixTop) uint32 { return p.KeptExtensions }))
	out.ByExtensions = withExt[:min(hot, len(withExt))]
	return out
}

// consider keeps the busiest prefixes as it goes, in two lists that are trimmed
// to ten when they pass a hundred.
func (r *Report) consider(c Class, fp identity.Fingerprint, p *prefixStat) {
	t := PrefixTop{Class: c, Fingerprint: fp, Records: p.records(), Extensions: p.extensions, Behind: p.behind, Kept: p.kept, KeptExtensions: p.keptExt}
	r.TopByRecords = append(r.TopByRecords, t)
	if len(r.TopByRecords) > 100 {
		r.TopByRecords = topN(r.TopByRecords, func(p PrefixTop) uint32 { return p.Records })
	}
	if p.extensions > 0 {
		r.TopByExtensions = append(r.TopByExtensions, t)
		if len(r.TopByExtensions) > 100 {
			r.TopByExtensions = topN(r.TopByExtensions, func(p PrefixTop) uint32 { return p.Extensions })
		}
	}
}

func topN(xs []PrefixTop, key func(PrefixTop) uint32) []PrefixTop {
	sort.Slice(xs, func(i, j int) bool {
		if key(xs[i]) != key(xs[j]) {
			return key(xs[i]) > key(xs[j])
		}
		return xs[i].Fingerprint.String() < xs[j].Fingerprint.String()
	})
	return xs[:min(len(xs), 10)]
}

// Write prints the report as tables.
func (r Report) Write(w io.Writer) {
	days := r.Duration.Hours() / 24
	perDay := func(n uint64) string { return fmt.Sprintf("%.0f", float64(n)/days) }
	fmt.Fprintf(w, "stream: %s, %d records (%s a day, %.1f a second), %.1f MB of payload (%.1f MB a day)\n",
		r.Duration, r.Records, perDay(r.Records), float64(r.Records)/r.Duration.Seconds(),
		float64(r.PayloadBytes)/1e6, float64(r.PayloadBytes)/1e6/days)
	fmt.Fprintf(w, "kinds: %s observes, %s extensions, %s deletes a day; by layer L0 %d, L1 %d, L2 %d, L3 %d\n",
		perDay(r.Observes), perDay(r.Extensions), perDay(r.Deletes), r.PerLayer[catalog.L0], r.PerLayer[catalog.L1], r.PerLayer[catalog.L2], r.PerLayer[catalog.L3])
	fmt.Fprintf(w, "late: %.1f%% of records (extensions aside) arrive after a later event time (lateness p50 %s, p99 %s)\n",
		100*r.LateShare, r.LatenessP50.Round(time.Second), r.LatenessP99.Round(time.Second))
	dead := r.Entities - r.EntitiesAlive
	fmt.Fprintf(w, "entities: %d ever, %d alive at the end, %d dead (%.1f dead for each live)\n",
		r.Entities, r.EntitiesAlive, dead, float64(dead)/float64(max(r.EntitiesAlive, 1)))
	fmt.Fprintf(w, "a deadline index would cost about %.1f MB a day in extra writes\n\n", r.DeadlineIndexBytesPerDay/1e6)

	tw := tabwriter.NewWriter(w, 0, 0, 2, ' ', tabwriter.AlignRight)
	fmt.Fprintln(tw, "class\tprefixes\trecords/day\tobserve\textend\tdelete\tbehind\tties\tpeers p50\tp99\tmax\tlive p50\tp99\tmax")
	for _, c := range r.Classes {
		total := c.Observes + c.Extensions + c.Deletes
		fmt.Fprintf(tw, "%s\t%d\t%s\t%d\t%d\t%d\t%d\t%d", c.Class, c.Prefixes, perDay(total), c.Observes, c.Extensions, c.Deletes, c.Behind, c.Ties)
		if c.Class.hub() {
			fmt.Fprintf(tw, "\t%d\t%d\t%d", c.Peers.P50, c.Peers.P99, c.Peers.Max)
		} else {
			fmt.Fprint(tw, "\t-\t-\t-")
		}
		if c.Class.Side != SideExistence {
			fmt.Fprintf(tw, "\t%d\t%d\t%d", c.LiveDegree.P50, c.LiveDegree.P99, c.LiveDegree.Max)
		} else {
			fmt.Fprint(tw, "\t-\t-\t-")
		}
		fmt.Fprintln(tw)
	}
	tw.Flush()

	// What a retention that keeps the last N days would leave in a prefix: the
	// work a read there has to do grows with it.
	fmt.Fprintln(w, "\nrecords held within the last N days, over the prefixes that hold any:")
	tw = tabwriter.NewWriter(w, 0, 0, 2, ' ', tabwriter.AlignRight)
	fmt.Fprint(tw, "class")
	for _, d := range historyDays {
		fmt.Fprintf(tw, "\t%dd prefixes\tp50\tp99\tmax", d)
	}
	fmt.Fprintln(tw)
	for _, c := range r.Classes {
		fmt.Fprintf(tw, "%s", c.Class)
		for _, d := range historyDays {
			if h, ok := c.History[d]; ok {
				fmt.Fprintf(tw, "\t%d\t%d\t%d\t%d", c.HistoryActive[d], h.P50, h.P99, h.Max)
			} else {
				fmt.Fprint(tw, "\t-\t-\t-\t-")
			}
		}
		fmt.Fprintln(tw)
	}
	tw.Flush()
	for _, top := range []struct {
		title string
		xs    []PrefixTop
	}{{"busiest prefixes by records", r.TopByRecords}, {"busiest prefixes by extensions", r.TopByExtensions}} {
		fmt.Fprintf(w, "\n%s:\n", top.title)
		for _, p := range top.xs {
			fmt.Fprintf(w, "  %-28s %s  records %d, extensions %d, behind the newest %d\n", p.Class, p.Fingerprint, p.Records, p.Extensions, p.Behind)
		}
	}
}
