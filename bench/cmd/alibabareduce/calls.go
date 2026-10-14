package main

import (
	"cmp"
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/lotannauo/toposhift/internal/catalog"
	"github.com/lotannauo/toposhift/internal/lifecycle"
	"github.com/lotannauo/toposhift/internal/store"
)

// The rpc types of the trace, in the order the tables count them.
var rpcTypes = [...]string{"rpc", "http", "mc", "db", "mq", "UNKNOWN"}

func rpcIndex(s string) int {
	for i, t := range rpcTypes {
		if s == t {
			return i
		}
	}
	return -1
}

// isUnknownService is whether a call's upstream or downstream service is a
// placeholder for one the trace could not name. "Unattributed" beats "guessed":
// such a call is counted and set aside, not given a service.
func isUnknownService(s string) bool { return s == "UNKNOWN" || s == "UNAVAILABLE" }

// isUnknownInstance is the same for an instance token: empty, one of the two
// placeholders, or the placeholder family UNKNOWN_POD_<n>.
func isUnknownInstance(s string) bool {
	return s == "" || isUnknownService(s) || strings.HasPrefix(s, "UNKNOWN_POD_")
}

// parseRT reads a response time as a decimal with at most three decimals and
// returns it in thousandths of the trace's unit, exactly. missing is set for an
// absent value ("" or "None"); a value that is neither is odd, and has no sum.
func parseRT(s string) (milli int64, ok, missing bool) {
	if s == "" || s == "None" {
		return 0, false, true
	}
	whole, frac, hasFrac := strings.Cut(s, ".")
	if whole == "" || len(whole) > 12 || (hasFrac && (frac == "" || len(frac) > 3)) {
		return 0, false, false
	}
	var n int64
	for i := 0; i < len(whole); i++ {
		if whole[i] < '0' || whole[i] > '9' {
			return 0, false, false
		}
		n = n*10 + int64(whole[i]-'0')
	}
	n *= 1000
	scale := int64(100)
	for i := 0; i < len(frac); i++ {
		if frac[i] < '0' || frac[i] > '9' {
			return 0, false, false
		}
		n += int64(frac[i]-'0') * scale
		scale /= 10
	}
	return n, true, false
}

// fnv64 is the 64-bit FNV-1a hash of s, the same on every run.
func fnv64(s string) uint64 {
	h := uint64(14695981039346656037)
	for i := 0; i < len(s); i++ {
		h ^= uint64(s[i])
		h *= 1099511628211
	}
	return h
}

// callKey is one rollup cell: a minute, an rpc type and two services.
type callKey struct {
	min    uint32 // the minute as the archive has it
	um, dm uint32
	rpc    uint8
}

type callAgg struct {
	calls, rtN uint32
	rtSum      int64 // thousandths
	traces     uint32
}

type traceHash struct {
	cell uint32
	hash uint64
}

// callRun reduces the CallGraph archives, one at a time in order, to a
// per-minute rollup and to the minute edges of the service graph.
type callRun struct {
	cfg    *Config
	mut    mutant
	epoch  time.Time
	svcs   *interner
	fps    *fpTable
	sink   *sink
	rollup *tsvWriter
	edges  *ivTracker
	// onEdge sees each edge of each minute, in ascending minute order.
	onEdge func(minute int32, um, dm uint32)

	files       []*fileReport
	depth       map[int64]int64 // used calls by the depth of their rpc_id
	byRPC       [len(rpcTypes)]int64
	fanOut      map[int64]int64 // distinct callees of a service in a minute
	fanIn       map[int64]int64
	selfCalls   int64
	rtMissing   int64
	rtOdd       int64
	rollupRows  int64
	edgeMinutes int64 // (minute, um, dm) edges asserted
	callsUsed   int64
}

func (c *callRun) readCalls(a archive, sums map[string]string) (*fileReport, error) {
	cfg := c.cfg
	rep := &fileReport{Table: a.Table, Name: a.Rel, Index: a.Index}
	cells := make(map[callKey]uint32)
	var aggs []callAgg
	var hashes []traceHash
	sha, err := scanArchive(a.Path, callsHeader, func(f []string, bad bool) error {
		rep.Rows++
		if bad || len(f) != len(callsHeader) {
			rep.skip(skipColumns)
			return nil
		}
		rpc := rpcIndex(f[4])
		if rpc < 0 {
			rep.skip(skipShifted)
			return nil
		}
		ts, ok := parseMillis(f[0])
		if !ok {
			rep.skip(skipTimestamp)
			return nil
		}
		rep.span(ts)
		if !validID(f[5]) || !validID(f[8]) {
			rep.skip(skipEmptyID)
			return nil
		}
		if c.mut != mutUnknownIsService &&
			(isUnknownService(f[5]) || isUnknownService(f[8]) || isUnknownInstance(f[6]) || isUnknownInstance(f[9])) {
			rep.skip(skipUnknownEndpoint)
			return nil
		}
		rep.Used++
		um, dm := f[5], f[8]
		if c.mut == mutServiceFromToken {
			um, _, _ = strings.Cut(f[6], "_POD_")
			dm, _, _ = strings.Cut(f[9], "_POD_")
		}
		k := callKey{min: uint32(ts / 60000), um: c.svcs.id(um), dm: c.svcs.id(dm), rpc: uint8(rpc)}
		cell, seen := cells[k]
		if !seen {
			if len(cells) >= cfg.MaxCallKeys {
				return fmt.Errorf("%s: more than %d rollup cells in one archive: stopping", a.Rel, cfg.MaxCallKeys)
			}
			cell = uint32(len(aggs))
			cells[k] = cell
			aggs = append(aggs, callAgg{})
		}
		g := &aggs[cell]
		g.calls++
		if milli, ok, missing := parseRT(f[10]); ok {
			g.rtN++
			g.rtSum += milli
		} else if missing {
			c.rtMissing++
		} else {
			c.rtOdd++
		}
		hashes = append(hashes, traceHash{cell, fnv64(f[1])})
		d := int64(0)
		if f[3] != "" {
			d = int64(strings.Count(f[3], ".") + 1)
		}
		c.depth[d]++
		c.byRPC[rpc]++
		c.callsUsed++
		return nil
	})
	if err != nil {
		return nil, err
	}
	rep.SHA256 = sha
	if err := checkSum(a, sha, sums); err != nil {
		return nil, err
	}
	if err := rep.seal(); err != nil {
		return nil, err
	}
	span := cfg.CallFileMillis
	lo := int64(a.Index) * span
	switch {
	case !rep.have:
		return nil, fmt.Errorf("%s: no row passed the timestamp check", a.Rel)
	case rep.MinTimestamp >= lo && rep.MaxTimestamp < lo+span:
		rep.Layout, rep.OffsetMillis = "absolute", 0
	case a.Index > 0 && rep.MaxTimestamp < span:
		rep.Layout, rep.OffsetMillis = "per-file", lo
	default:
		return nil, fmt.Errorf("%s: timestamps %d to %d are not in [%d, %d), nor in [0, %d): the archives do not tile the hour",
			a.Rel, rep.MinTimestamp, rep.MaxTimestamp, lo, lo+span, span)
	}
	off := int32(rep.OffsetMillis / 60000)

	// Distinct traces per cell, by 64-bit hash: exact up to hash collisions.
	slices.SortFunc(hashes, func(x, y traceHash) int {
		return cmp.Or(cmp.Compare(x.cell, y.cell), cmp.Compare(x.hash, y.hash))
	})
	for i, h := range hashes {
		if i == 0 || h != hashes[i-1] {
			aggs[h.cell].traces++
		}
	}
	hashes = nil

	keys := make([]callKey, 0, len(cells))
	for k := range cells {
		keys = append(keys, k)
	}
	slices.SortFunc(keys, func(x, y callKey) int {
		return cmp.Or(
			cmp.Compare(x.min, y.min),
			cmp.Compare(c.svcs.name(x.um), c.svcs.name(y.um)),
			cmp.Compare(c.svcs.name(x.dm), c.svcs.name(y.dm)),
			cmp.Compare(x.rpc, y.rpc),
		)
	})
	if err := c.emit(a.Index, off, keys, func(k callKey) callAgg { return aggs[cells[k]] }); err != nil {
		return nil, err
	}
	return rep, nil
}

// emit writes the rollup rows of one archive and asserts its edges, in minute order.
func (c *callRun) emit(file int, off int32, keys []callKey, agg func(callKey) callAgg) error {
	var (
		curMin   = int32(-1)
		curUM    uint32
		outDeg   int64
		inDeg    = make(map[uint32]int64)
		flushMin = func() {
			for _, d := range inDeg {
				c.fanIn[d]++
			}
			clear(inDeg)
		}
	)
	for i := 0; i < len(keys); {
		k := keys[i]
		gmin := int32(k.min) + off
		if gmin != curMin {
			if curMin >= 0 {
				if outDeg > 0 {
					c.fanOut[outDeg]++
				}
				flushMin()
			}
			curMin, curUM, outDeg = gmin, 0, 0
		}
		// All rpc types of one (minute, um, dm).
		var calls int64
		j := i
		for ; j < len(keys) && keys[j].min == k.min && keys[j].um == k.um && keys[j].dm == k.dm; j++ {
			g := agg(keys[j])
			calls += int64(g.calls)
			c.rollup.row(itoa(int64(gmin)), c.svcs.name(k.um), c.svcs.name(k.dm), rpcTypes[keys[j].rpc],
				itoa(int64(g.calls)), itoa(int64(g.rtN)), itoa(g.rtSum), itoa(int64(g.traces)))
			c.rollupRows++
		}
		if k.um == k.dm {
			c.selfCalls += calls
		} else {
			if k.um != curUM {
				if outDeg > 0 {
					c.fanOut[outDeg]++
				}
				curUM, outDeg = k.um, 0
			}
			outDeg++
			inDeg[k.dm]++
			c.edges.observe(pair(k.um, k.dm), gmin, 0, int32(file))
			c.edgeMinutes++
			if c.onEdge != nil {
				c.onEdge(gmin, k.um, k.dm)
			}
			if err := c.sink.assert(store.Record{
				Layer: catalog.L3, Subject: store.EdgeSubject(c.fps.get(k.um), c.fps.get(k.dm), catalog.DependsOn), Producer: callsProducer,
				EventTime: c.epoch.Add(time.Duration(gmin) * sampleInterval), Kind: lifecycle.Observe, TTL: callEdgeTTL,
				EventTimeBasis: store.BasisProducerEvent,
			}); err != nil {
				return err
			}
		}
		i = j
	}
	if curMin >= 0 {
		if outDeg > 0 {
			c.fanOut[outDeg]++
		}
		flushMin()
	}
	if c.edges.open() > c.cfg.MaxEdges {
		return fmt.Errorf("more than %d distinct service edges are open: stopping", c.cfg.MaxEdges)
	}
	return c.rollup.err
}
