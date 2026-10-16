package main

import (
	"encoding/json"
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/lotannauo/toposhift/internal/catalog"
	"github.com/lotannauo/toposhift/internal/lifecycle"
	"github.com/lotannauo/toposhift/internal/store"
)

// prow is one used placement sample, 12 bytes: the interned instance, node and
// msname. The minute is where the row is kept, in the rows of its minute.
type prow struct {
	inst, node, name uint32
}

// minuteRows are the used rows of one minute of an archive, in the order the
// archive had them.
type minuteRows struct {
	min  uint32 // the minute as the archive has it
	rows []prow
}

// minuteBuckets groups the rows of an archive by minute as they are read, so
// that no second copy of the archive's rows is needed to sort them. An archive
// that holds more minutes than it should is not tiled: its rows past the limit
// are dropped, and the layout check that follows refuses the archive.
type minuteBuckets struct {
	list []minuteRows
	cur  int
	max  int
}

func (b *minuteBuckets) add(min uint32, w prow) {
	if b.cur >= len(b.list) || b.list[b.cur].min != min {
		b.cur = -1
		for i := range b.list {
			if b.list[i].min == min {
				b.cur = i
				break
			}
		}
		if b.cur < 0 {
			if len(b.list) >= b.max {
				b.cur = 0
				return
			}
			b.list = append(b.list, minuteRows{min: min})
			b.cur = len(b.list) - 1
		}
	}
	b.list[b.cur].rows = append(b.list[b.cur].rows, w)
}

// minuteRow is one line of placement_minutes: what the C60 grid says of a minute.
type minuteRow struct {
	minute                    int32
	pods, nodes               int
	appear, disappear, moves  int
	p50, p90, p99, maxPerNode int64
}

// cadence is one reduction of the placement: the samples at its step, their
// raw intervals, and the stream the coalescer makes of them.
type cadence struct {
	spec cadenceSpec
	sink *sink
	// every is the stream of every assertion as it was made, with no coalescer.
	// It is nil unless the run writes it.
	every *sink
	// dupToC is a mutant: an assertion the every-sample stream takes also reaches
	// the coalesced stream a second time.
	dupToC bool
	pods   *ivTracker
	nodes  *ivTracker
	edges  *ivTracker
}

// assert hands an assertion to the cadence's coalesced stream and, if there is
// one, to its every-sample stream.
func (c *cadence) assert(r store.Record) error {
	if c.every != nil {
		if err := c.every.assert(r); err != nil {
			return err
		}
		if c.dupToC {
			if err := c.sink.assert(r); err != nil {
				return err
			}
		}
	}
	return c.sink.assert(r)
}

func newCadence(spec cadenceSpec, s *sink, strict bool) *cadence {
	c := &cadence{spec: spec, sink: s}
	for _, t := range []**ivTracker{&c.pods, &c.nodes, &c.edges} {
		*t = newTracker(spec.step, spec.ttlMinutes())
		(*t).strict = strict
	}
	return c
}

// placeRun reduces placement samples, minute by minute in ascending order,
// into every cadence's stream and into the statistics of the C60 grid.
type placeRun struct {
	mut     mutant
	epoch   time.Time
	pods    *interner // msinstanceid
	nodes   *interner // nodeid
	names   *interner // msname
	podFP   *fpTable
	nodeFP  *fpTable
	cads    []*cadence
	dirt    *dirtLog // nil: no statistics
	hourMin int32

	// Scratch and state, by interned id.
	podStamp     []int32 // minute+1 of the sample being reduced
	podNode1     []uint32
	podName1     []uint32
	podNameLast  []uint32
	podPrevMin   []int32 // minute+1 of the previous sample
	podPrevNode  []uint32
	podPrevName  []uint32
	podFirstName []uint32 // the msname of the first sample
	nodeStamp    []int32
	nodeCount    []int32

	extra    map[uint64]struct{}
	podList  []uint32
	nodeList []uint32
	edgeList []uint64
	prevList []uint32
	counts   []int64
	payloads map[uint32][]byte

	// Statistics.
	statistics bool
	minutes    []minuteRow
	perNode    map[int64]int64 // pods per node, over every (minute, node)
	gaps       map[int64]int64 // samples missed by an (instance, node) that came back
	prevPods   int
	used       int64 // used samples, after the duplicates one assertion per instant keeps
	dupRows    int64
}

func (r *placeRun) ensure() {
	n := r.pods.size() + 1
	r.podStamp = grow(r.podStamp, n)
	r.podNode1 = grow(r.podNode1, n)
	r.podName1 = grow(r.podName1, n)
	r.podNameLast = grow(r.podNameLast, n)
	r.podPrevMin = grow(r.podPrevMin, n)
	r.podPrevNode = grow(r.podPrevNode, n)
	r.podPrevName = grow(r.podPrevName, n)
	r.podFirstName = grow(r.podFirstName, n)
	m := r.nodes.size() + 1
	r.nodeStamp = grow(r.nodeStamp, m)
	r.nodeCount = grow(r.nodeCount, m)
}

type podPayload struct {
	MSName string `json:"msname"`
	CPU    string `json:"cpu_utilization,omitempty"`
}

// payload is the pod's payload for an msname id: {"msname": "<token>"}, and
// nothing that varies from sample to sample.
func (r *placeRun) payload(name uint32) []byte {
	if p, ok := r.payloads[name]; ok {
		return p
	}
	s := r.names.name(name)
	var v podPayload
	if r.mut == mutCPUInPayload {
		v.MSName, v.CPU, _ = strings.Cut(s, "\x00")
	} else {
		v.MSName = s
	}
	b, err := json.Marshal(v)
	if err != nil {
		panic(err)
	}
	r.payloads[name] = b
	return b
}

// restart readies the run for another pass over the same samples, with its own
// cadences and, if statistics is not set, without counting what the first pass
// counted. The scratch that marks the minute a pod or a node was last seen is
// cleared, because minute 0 comes again.
func (r *placeRun) restart(cads []*cadence, statistics bool) {
	r.cads, r.statistics = cads, statistics
	clear(r.podStamp)
	clear(r.nodeStamp)
	r.prevList = r.prevList[:0]
}

func (r *placeRun) at(m int32) time.Time {
	return r.epoch.Add(time.Duration(m) * sampleInterval)
}

// splitAll ends every open interval where it is, which is what a reduction
// that restarts at an archive boundary would do.
func (r *placeRun) splitAll() {
	for _, c := range r.cads {
		c.pods.closeAll()
		c.nodes.closeAll()
		c.edges.closeAll()
	}
}

// minute reduces the samples of one minute of the hour, in row order. The rows
// of an archive are the rows as the archive had them: duplicates, double
// placements and all.
func (r *placeRun) minute(m, file int32, rows []prow) error {
	cur := m + 1
	pods, nodes, edges := r.podList[:0], r.nodeList[:0], r.edgeList[:0]
	clear(r.extra)
	var moves int
	for _, w := range rows {
		inst := w.inst
		if r.podStamp[inst] != cur {
			r.podStamp[inst] = cur
			r.podNode1[inst], r.podName1[inst], r.podNameLast[inst] = w.node, w.name, w.name
			pods = append(pods, inst)
			edges = append(edges, pair(inst, w.node))
		} else {
			if w.name != r.podNameLast[inst] && r.statistics {
				r.dirt.add(dirtCase{dirtMSNameChange, r.pods.name(inst), m, r.names.name(r.podNameLast[inst]), r.names.name(w.name)})
			}
			r.podNameLast[inst] = w.name
			if w.node == r.podNode1[inst] {
				r.duplicate(m, inst, w.node)
			} else if _, seen := r.extra[pair(inst, w.node)]; seen {
				r.duplicate(m, inst, w.node)
			} else {
				r.extra[pair(inst, w.node)] = struct{}{}
				edges = append(edges, pair(inst, w.node))
				if r.statistics {
					r.dirt.add(dirtCase{dirtDoublePlacement, r.pods.name(inst), m, r.nodes.name(w.node), ""})
				}
			}
		}
		if r.nodeStamp[w.node] != cur {
			r.nodeStamp[w.node] = cur
			nodes = append(nodes, w.node)
		}
	}
	r.podList, r.nodeList, r.edgeList = pods, nodes, edges

	if r.statistics {
		both := 0
		for _, inst := range pods {
			if pm := r.podPrevMin[inst]; pm != 0 {
				if pm == m {
					both++
				}
				if r.podPrevNode[inst] != r.podNode1[inst] {
					moves++
					r.dirt.add(dirtCase{dirtMove, r.pods.name(inst), m, r.nodes.name(r.podPrevNode[inst]), r.nodes.name(r.podNode1[inst])})
				}
				if r.podPrevName[inst] != r.podName1[inst] {
					r.dirt.add(dirtCase{dirtMSNameChange, r.pods.name(inst), m, r.names.name(r.podPrevName[inst]), r.names.name(r.podName1[inst])})
				}
			} else {
				r.podFirstName[inst] = r.podName1[inst]
			}
			r.podPrevMin[inst], r.podPrevNode[inst], r.podPrevName[inst] = cur, r.podNode1[inst], r.podNameLast[inst]
		}
		for _, e := range edges {
			_, node := unpair(e)
			r.nodeCount[node]++
		}
		r.counts = r.counts[:0]
		for _, n := range nodes {
			c := int64(r.nodeCount[n])
			r.counts = append(r.counts, c)
			r.perNode[c]++
			r.nodeCount[n] = 0
		}
		slices.Sort(r.counts)
		r.minutes = append(r.minutes, minuteRow{
			minute: m, pods: len(pods), nodes: len(nodes), appear: len(pods) - both, disappear: r.prevPods - both,
			moves: moves, p50: quantile(r.counts, 0.5), p90: quantile(r.counts, 0.9), p99: quantile(r.counts, 0.99),
			maxPerNode: quantile(r.counts, 1),
		})
		r.prevPods = len(pods)
		r.used += int64(len(edges))
	}

	at := r.at(m)
	for _, c := range r.cads {
		if m%c.spec.step != 0 {
			continue
		}
		ttl := c.spec.ttl()
		for _, n := range nodes {
			c.nodes.observe(uint64(n), m, 0, file)
			if err := c.assert(store.Record{
				Layer: catalog.L2, Subject: store.EntitySubject(r.nodeFP.get(n)), Producer: placementProducer,
				EventTime: at, Kind: lifecycle.Observe, TTL: ttl, EventTimeBasis: store.BasisProducerEvent,
			}); err != nil {
				return err
			}
		}
		for _, p := range pods {
			c.pods.observe(uint64(p), m, r.podNameLast[p], file)
			if err := c.assert(store.Record{
				Layer: catalog.L2, Subject: store.EntitySubject(r.podFP.get(p)), Producer: placementProducer,
				EventTime: at, Kind: lifecycle.Observe, TTL: ttl, Payload: r.payload(r.podNameLast[p]),
				EventTimeBasis: store.BasisProducerEvent,
			}); err != nil {
				return err
			}
		}
		for _, e := range edges {
			p, n := unpair(e)
			c.edges.observe(e, m, r.podNameLast[p], file)
			if err := c.assert(store.Record{
				Layer: catalog.L2, Subject: store.EdgeSubject(r.podFP.get(p), r.nodeFP.get(n), catalog.ScheduledOn), Producer: placementProducer,
				EventTime: at, Kind: lifecycle.Observe, TTL: ttl, EventTimeBasis: store.BasisProducerEvent,
			}); err != nil {
				return err
			}
		}
		if r.mut == mutInferDelete && c.spec.name == cadenceC60.name {
			for _, p := range r.prevList {
				if r.podStamp[p] == cur {
					continue
				}
				if err := c.assert(store.Record{
					Layer: catalog.L2, Subject: store.EntitySubject(r.podFP.get(p)), Producer: placementProducer,
					EventTime: at, Kind: lifecycle.Delete, EventTimeBasis: store.BasisProducerEvent,
				}); err != nil {
					return err
				}
			}
			r.prevList = append(r.prevList[:0], pods...)
		}
	}
	return nil
}

func (r *placeRun) duplicate(m int32, inst, node uint32) {
	if r.statistics {
		r.dupRows++
		r.dirt.add(dirtCase{dirtDuplicate, r.pods.name(inst), m, r.nodes.name(node), ""})
	}
}

// placementLayout decides how a placement archive's timestamps sit in the hour
// and returns the offset to add to them, in minutes. A timestamp is either
// absolute (from the trace's start) or restarts at zero in each archive; any other
// layout stops the run.
func placementLayout(cfg *Config, idx int, rep *fileReport) (int32, error) {
	span := int64(cfg.PlacementFileMinutes) * 60000
	lo := int64(idx) * span
	switch {
	case rep.Used == 0:
		return 0, fmt.Errorf("%s: no row passed the checks", rep.Name)
	case rep.MinTimestamp >= lo && rep.MaxTimestamp < lo+span:
		rep.Layout, rep.OffsetMillis = "absolute", 0
	case idx > 0 && rep.MaxTimestamp < span:
		rep.Layout, rep.OffsetMillis = "per-file", lo
	default:
		return 0, fmt.Errorf("%s: timestamps %d to %d are not in [%d, %d), nor in [0, %d): the archives do not tile the hour",
			rep.Name, rep.MinTimestamp, rep.MaxTimestamp, lo, lo+span, span)
	}
	return int32(rep.OffsetMillis / 60000), nil
}

// readPlacement streams one archive into compact rows, checks the accounting,
// the digest and the layout, and returns the rows grouped by minute: the result's
// element i holds the rows of minute first+i of the hour, in the order the archive
// had them. The rows are grouped as they are read, so the archive's rows are held
// once; the caller drops a minute's slice when it is done with it.
func (r *placeRun) readPlacement(cfg *Config, a archive, sums map[string]string) (rep *fileReport, first int32, byMinute [][]prow, err error) {
	rep = &fileReport{Table: a.Table, Name: a.Rel, Index: a.Index}
	// An archive that tiles its half hour has one bucket for each of its minutes.
	buckets := minuteBuckets{max: cfg.PlacementFileMinutes}
	var layoutErr error
	sha, err := scanArchive(a.Path, placementHeader, func(f []string, bad bool) error {
		rep.Rows++
		if bad || len(f) != len(placementHeader) {
			rep.skip(skipColumns)
			return nil
		}
		ts, ok := parseMillis(f[0])
		if !ok {
			rep.skip(skipTimestamp)
			return nil
		}
		if !validID(f[1]) || !validID(f[2]) || !validID(f[3]) {
			rep.skip(skipEmptyID)
			return nil
		}
		if ts%60000 != 0 && layoutErr == nil {
			layoutErr = fmt.Errorf("%s: timestamp %d is not on the 60 s sampling grid", a.Rel, ts)
		}
		rep.Used++
		rep.span(ts)
		name := f[1]
		switch r.mut {
		case mutCPUInPayload:
			name = f[1] + "\x00" + f[4]
		case mutServiceFromToken:
			name, _, _ = strings.Cut(f[2], "_POD_")
		}
		buckets.add(uint32(ts/60000), prow{r.pods.id(f[2]), r.nodes.id(f[3]), r.names.id(name)})
		return nil
	})
	if err != nil {
		return nil, 0, nil, err
	}
	rep.SHA256 = sha
	if err := checkSum(a, sha, sums); err != nil {
		return nil, 0, nil, err
	}
	if layoutErr != nil {
		return nil, 0, nil, layoutErr
	}
	if err := rep.seal(); err != nil {
		return nil, 0, nil, err
	}
	off, err := placementLayout(cfg, a.Index, rep)
	if err != nil {
		return nil, 0, nil, err
	}
	span := int32(cfg.PlacementFileMinutes)
	first = int32(a.Index) * span
	byMinute = make([][]prow, span)
	for _, b := range buckets.list {
		j := int32(b.min) + off - first
		if j < 0 || j >= span {
			return nil, 0, nil, fmt.Errorf("%s: minute %d is outside its half hour", a.Rel, int32(b.min)+off)
		}
		byMinute[j] = b.rows
	}
	for i, rows := range byMinute {
		if len(rows) == 0 {
			return nil, 0, nil, fmt.Errorf("%s: no sample at minute %d: the archives do not tile the hour", a.Rel, first+int32(i))
		}
	}
	return rep, first, byMinute, nil
}
