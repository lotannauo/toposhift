package main

import (
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"math/bits"
	"path/filepath"
	"slices"

	"github.com/lotannauo/toposhift/internal/catalog"
	"github.com/lotannauo/toposhift/internal/identity"
	"github.com/lotannauo/toposhift/internal/lifecycle"
	"github.com/lotannauo/toposhift/internal/store"
)

// nodeHash is the sampling hash of a node: the first 8 bytes of the SHA-256 of
// "alibaba-v2022/" and the node's token, as a big-endian number.
func nodeHash(token string) uint64 {
	sum := sha256.Sum256([]byte("alibaba-v2022/" + token))
	return binary.BigEndian.Uint64(sum[:8])
}

// keepThreshold is 2^64 times want over nodes, rounded down: a node is kept if
// its hash is below it. With no more nodes than wanted, every node is kept.
func keepThreshold(nodes, want int) uint64 {
	if nodes <= want {
		return ^uint64(0)
	}
	q, _ := bits.Div64(uint64(want), 0, uint64(nodes))
	return q
}

// fixture is the small stream that keeps the shape of the hour's placement:
// whole nodes, sampled by hash, with every sample of each, for the first minutes
// of the hour; and the calls between the services that have a pod on them.
type fixture struct {
	cfg   *Config
	rows  [][]prow // every sample of the first minutes, as read
	edges [][][2]uint32
	// names is the set of msnames with a pod on a kept node.
	names map[string]struct{}
	// limit is the sampling threshold.
	limit uint64
}

func newFixture(cfg *Config) *fixture {
	return &fixture{
		cfg: cfg, rows: make([][]prow, cfg.FixtureMinutes), edges: make([][][2]uint32, cfg.FixtureMinutes),
		names: make(map[string]struct{}),
	}
}

// retain keeps a copy of the first minutes' samples until the hour's node count is known.
func (fx *fixture) retain(m int32, rows []prow) {
	if int(m) < len(fx.rows) {
		fx.rows[m] = slices.Clone(rows)
	}
}

// selectRows keeps the samples of the kept nodes, and notes the msnames they carry.
func (fx *fixture) selectRows(pods, nodes, names *interner) {
	fx.limit = keepThreshold(nodes.size(), fx.cfg.FixtureNodes)
	for m, rows := range fx.rows {
		kept := rows[:0:0]
		for _, w := range rows {
			var h uint64
			if fx.cfg.mut == mutUniformDownsample {
				h = nodeHash(fmt.Sprintf("%d/%s/%s", m, pods.name(w.inst), nodes.name(w.node)))
			} else {
				h = nodeHash(nodes.name(w.node))
			}
			if h < fx.limit {
				kept = append(kept, w)
				fx.names[names.name(w.name)] = struct{}{}
			}
		}
		fx.rows[m] = kept
	}
}

// edge takes a service edge of a minute, and keeps it if both services have a pod
// on a kept node.
func (fx *fixture) edge(minute int32, svcs *interner, um, dm uint32) {
	if int(minute) >= len(fx.edges) {
		return
	}
	if _, ok := fx.names[svcs.name(um)]; !ok {
		return
	}
	if _, ok := fx.names[svcs.name(dm)]; !ok {
		return
	}
	fx.edges[minute] = append(fx.edges[minute], [2]uint32{um, dm})
}

// fixtureName is the file name of the fixture.
func (fx *fixture) fileName() string {
	return fmt.Sprintf("fixture_%dmin_%dnodes.parquet", fx.cfg.FixtureMinutes, fx.cfg.FixtureNodes)
}

// build writes the fixture's stream, through its own coalescer, and returns the
// result with the raw assertions it was made from.
func (fx *fixture) build(outDir string, d *ids) (sinkResult, []rawAssertion, error) {
	s, err := newSink("fixture", filepath.Join(outDir, fx.fileName()), true, 0, true)
	if err != nil {
		return sinkResult{}, nil, err
	}
	run := newPlaceRun(fx.cfg, d, []*cadence{newCadence(fx.cfg.cadences()[0], s, false)}, false)
	run.ensure()
	for m := range fx.rows {
		if err := run.minute(int32(m), 0, fx.rows[m]); err != nil {
			return sinkResult{}, nil, err
		}
		at := run.at(int32(m))
		for _, e := range fx.edges[m] {
			if err := s.assert(store.Record{
				Layer: catalog.L3, Subject: store.EdgeSubject(d.svcFP.get(e[0]), d.svcFP.get(e[1]), catalog.DependsOn),
				Producer: callsProducer, EventTime: at, Kind: lifecycle.Observe, TTL: callEdgeTTL,
				EventTimeBasis: store.BasisProducerEvent,
			}); err != nil {
				return sinkResult{}, nil, err
			}
		}
	}
	res, err := s.finish()
	return res, s.raw, err
}

// ids is the tokens of the hour and the fingerprints made of them.
type ids struct {
	pods, nodes, names, svcs *interner
	podFP, nodeFP, svcFP     *fpTable
}

func newIDs() *ids {
	res := identity.NewResolver(catalog.Default())
	d := &ids{pods: newInterner(), nodes: newInterner(), names: newInterner(), svcs: newInterner()}
	d.podFP = newFPTable(res, catalog.K8sPod, catalog.K8sPodUID, d.pods)
	d.nodeFP = newFPTable(res, catalog.K8sNode, catalog.K8sNodeUID, d.nodes)
	d.svcFP = newFPTable(res, catalog.Service, catalog.ServiceName, d.svcs)
	return d
}

func newPlaceRun(cfg *Config, d *ids, cads []*cadence, statistics bool) *placeRun {
	r := &placeRun{
		mut: cfg.mut, epoch: cfg.Epoch, pods: d.pods, nodes: d.nodes, names: d.names, podFP: d.podFP, nodeFP: d.nodeFP,
		cads: cads, hourMin: cfg.hourMinutes(), extra: make(map[uint64]struct{}), payloads: make(map[uint32][]byte),
		statistics: statistics, perNode: make(map[int64]int64), gaps: make(map[int64]int64),
	}
	if statistics {
		r.dirt = newDirtLog(1000)
	}
	return r
}
