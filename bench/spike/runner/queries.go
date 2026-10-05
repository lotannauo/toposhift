package runner

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"slices"
	"time"

	"github.com/lotannauo/toposhift/bench/spike/engine"
	"github.com/lotannauo/toposhift/bench/spike/workload"
	"github.com/lotannauo/toposhift/internal/catalog"
	"github.com/lotannauo/toposhift/internal/identity"
)

// Op is a kind of read.
type Op string

// The reads. The names are the ones the engines record their counters under
// ("read.<op>.<name>").
const (
	OpNeighbors Op = "neighbors"
	OpBatch     Op = "batch"
	OpAlive     Op = "alive"
	OpWindow    Op = "window"
)

// The ages a read can be made at, as the name of the age and what it means. A
// read "now" is as of the end of the period and the newest snapshot; the others
// look back from it. Windows end at "now" and span the age. A read of the "old
// token" is made at the instant of that token, with only the records it saw.
const (
	AgeNow      = "now"
	Age1h       = "1h"
	Age1d       = "1d"
	AgeOldToken = "old-token"
	AgeWindow1h = "window-1h"
	AgeWindow1d = "window-1d"
)

// Query is one read, with the answer the reference engine gave.
type Query struct {
	// Group names the population the query belongs to: a class of prefix, how its
	// members were chosen, and the read.
	Group string
	// Rank is the position of the prefix among those chosen.
	Rank  int
	Op    Op
	Layer catalog.Layer
	Dir   engine.Direction
	// Fps is the entity read, or for a batch the entities read together.
	Fps []identity.Fingerprint
	Age string
	// At is the instant of a neighbors or alive read; From and To bound a window.
	At, From, To time.Time
	// AsOf is the snapshot token.
	AsOf uint64
	// Expect is the digest of the reference engine's answer, and Size the number
	// of items in it (neighbors, records, or 1 for a live entity).
	Expect string
	Size   int
}

// Name identifies the query in a report.
func (q Query) Name() string { return fmt.Sprintf("%s #%d %s", q.Group, q.Rank, q.Age) }

// target is a class of prefix and the reads asked of the prefixes chosen from it.
type target struct {
	name  string
	class workload.Class
	// dir is the direction of the neighbors and window reads; for an existence
	// class the only read is alive.
	reads []Op
	// hub says the owner is an entity that exists from the start of the stream and is
	// never replaced, so that a run over any length of stream can be read at the
	// same prefix (see [Pins]).
	hub bool
}

// targets are the classes the queries are drawn from, among the prefixes that
// exist at the end of the stream: the prefixes a node or a service collects
// (where history grows with churn), a pod's own, the layer-one prefixes whose
// edges are only ever refreshed, the rollups, and the existence of a node (which
// every heartbeat extends).
//
// A window is asked only of the prefixes a node or a service collects. A
// refreshed edge is one run, and all of a run's refreshes are stamped with the
// instant it started, so the window of the last day or hour of a heartbeat prefix
// or of a live pod's own edge is empty for a run older than the window, and for a
// run that restarted inside it (the default retention restarts every run that began
// before the horizon) holds the new run and measures the restart, not the history.
// Neither is what a window read is for, and the first would be agreed on by every
// candidate without measuring anything.
var targets = []target{
	{"node<-", workload.Class{Layer: catalog.L2, Owner: catalog.K8sNode, Side: workload.SideReverse}, []Op{OpNeighbors, OpWindow, OpBatch}, true},
	{"service<-", workload.Class{Layer: catalog.L2, Owner: catalog.Service, Side: workload.SideReverse}, []Op{OpNeighbors, OpWindow, OpBatch}, true},
	{"pod->", workload.Class{Layer: catalog.L2, Owner: catalog.K8sPod, Side: workload.SideForward}, []Op{OpNeighbors}, false},
	{"host<-", workload.Class{Layer: catalog.L1, Owner: catalog.Host, Side: workload.SideReverse}, []Op{OpNeighbors}, true},
	{"node->L1", workload.Class{Layer: catalog.L1, Owner: catalog.K8sNode, Side: workload.SideForward}, []Op{OpNeighbors}, true},
	{"service<-L3", workload.Class{Layer: catalog.L3, Owner: catalog.Service, Side: workload.SideReverse}, []Op{OpNeighbors}, true},
	{"node exists", workload.Class{Layer: catalog.L2, Owner: catalog.K8sNode, Side: workload.SideExistence}, []Op{OpAlive}, true},
}

// Selector picks the prefixes to query from an analyzed stream.
type Selector interface {
	Pick(c workload.Class, hot, median int, live bool) workload.Picks
}

// BuildQueries chooses the queries of a spec from the stream the analyzer saw.
// They are the same for every candidate; the plan adds the expected answers.
func BuildQueries(spec Spec, sel Selector, info StreamInfo) []Query {
	// A store guarantees nothing before the horizon, and nothing was written
	// before the start: a question about either is not asked.
	earliest := info.Horizon
	if info.Start.After(earliest) {
		earliest = info.Start
	}
	var out []Query
	for _, t := range targets {
		p := sel.Pick(t.class, spec.Hot, spec.Median, true)
		groups := []struct {
			name string
			from []workload.PrefixTop
		}{{"hot-records", p.ByRecords}, {"hot-extensions", p.ByExtensions}, {"median", p.Median}}
		for _, g := range groups {
			if len(g.from) == 0 {
				continue
			}
			fps := make([]identity.Fingerprint, len(g.from))
			for i, pt := range g.from {
				fps[i] = pt.Fingerprint
			}
			dir := engine.Forward
			if t.class.Side == workload.SideReverse {
				dir = engine.Reverse
			}
			for _, op := range t.reads {
				name := t.name + " " + g.name + " " + string(op)
				base := Query{Group: name, Op: op, Layer: t.class.Layer, Dir: dir, AsOf: engine.Latest}
				switch op {
				case OpBatch:
					q := base
					q.Fps, q.Age, q.At = fps, AgeNow, info.End
					out = append(out, q)
				case OpNeighbors, OpAlive:
					for i, fp := range fps {
						for _, age := range []string{AgeNow, Age1h, Age1d, AgeOldToken} {
							q := base
							q.Rank, q.Fps, q.Age = i, []identity.Fingerprint{fp}, age
							q.At = info.End
							switch age {
							case Age1h:
								q.At = info.End.Add(-time.Hour)
							case Age1d:
								q.At = info.End.Add(-24 * time.Hour)
							case AgeOldToken:
								q.At, q.AsOf = info.OldAt, info.OldToken
							}
							if q.At.Before(earliest) {
								continue
							}
							// A read a day back needs a day of history behind it: in a stream
							// of a day it would be made at the first instant, before most
							// runs have a history, and show nothing of how a read grows.
							if age == Age1d && q.At.Sub(info.Start) < 24*time.Hour {
								continue
							}
							out = append(out, q)
						}
					}
				case OpWindow:
					for i, fp := range fps {
						for _, w := range []struct {
							age string
							d   time.Duration
						}{{AgeWindow1h, time.Hour}, {AgeWindow1d, 24 * time.Hour}} {
							q := base
							q.Rank, q.Fps, q.Age = i, []identity.Fingerprint{fp}, w.age
							q.From, q.To = info.End.Add(-w.d), info.End
							if q.From.Before(earliest) {
								continue
							}
							out = append(out, q)
						}
					}
				}
			}
		}
	}
	return out
}

// QueriesDigest is the SHA-256 of the queries without their expected answers, as
// hex: which questions are asked.
func QueriesDigest(qs []Query) (string, error) {
	stripped := make([]Query, len(qs))
	for i, q := range qs {
		q.Expect, q.Size = "", 0
		stripped[i] = q
	}
	b, err := json.Marshal(stripped)
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:]), nil
}

// Answer is what a read returned, reduced to a digest and a size.
type Answer struct {
	Digest string
	Size   int
}

// Ask puts the query to an engine and reduces the answer to its digest. The
// digest covers the answer in the order the engine returned it, which the
// engine contract fixes, so a wrong order is a wrong answer.
func Ask(e engine.Engine, q Query) (Answer, error) {
	sc := engine.Scope{Layer: q.Layer, AsOf: q.AsOf}
	h := sha256.New()
	var buf []byte
	size := 0
	switch q.Op {
	case OpNeighbors:
		ns, err := e.Neighbors(q.Fps[0], q.Dir, q.At, sc)
		if err != nil {
			return Answer{}, err
		}
		size = len(ns)
		for _, n := range ns {
			buf = appendNeighbor(buf[:0], n)
			h.Write(buf)
		}
	case OpBatch:
		all, err := e.NeighborsBatch(q.Fps, q.Dir, q.At, sc)
		if err != nil {
			return Answer{}, err
		}
		if len(all) != len(q.Fps) {
			return Answer{}, fmt.Errorf("runner: a batch of %d entities was answered with %d lists", len(q.Fps), len(all))
		}
		for _, ns := range all {
			size += len(ns)
			buf = binary.BigEndian.AppendUint32(buf[:0], uint32(len(ns)))
			h.Write(buf)
			for _, n := range ns {
				buf = appendNeighbor(buf[:0], n)
				h.Write(buf)
			}
		}
	case OpAlive:
		ok, err := e.Alive(q.Fps[0], q.At, sc)
		if err != nil {
			return Answer{}, err
		}
		if ok {
			size = 1
			h.Write([]byte{1})
		} else {
			h.Write([]byte{0})
		}
	case OpWindow:
		rs, err := e.Window(q.Fps[0], q.Dir, q.From, q.To, sc)
		if err != nil {
			return Answer{}, err
		}
		size = len(rs)
		for _, r := range rs {
			buf = appendRecord(buf[:0], r)
			h.Write(buf)
		}
	default:
		return Answer{}, fmt.Errorf("runner: unknown read %q", q.Op)
	}
	return Answer{Digest: hex.EncodeToString(h.Sum(nil)[:16]), Size: size}, nil
}

func appendNeighbor(b []byte, n engine.Neighbor) []byte {
	b = appendFingerprint(b, n.Peer)
	return appendString(b, string(n.Relation))
}

// fingerprintsOf lists the entities the queries read, without repeats.
func fingerprintsOf(qs []Query) []identity.Fingerprint {
	seen := map[identity.Fingerprint]struct{}{}
	var out []identity.Fingerprint
	for _, q := range qs {
		for _, fp := range q.Fps {
			if _, ok := seen[fp]; !ok {
				seen[fp] = struct{}{}
				out = append(out, fp)
			}
		}
	}
	slices.SortFunc(out, engine.CompareFingerprints)
	return out
}
