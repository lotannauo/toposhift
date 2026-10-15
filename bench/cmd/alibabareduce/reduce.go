package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"time"
)

// outputs is where a run's files go: out/ for the data, check/ for the files
// an outside check reads, and logs/.
type outputs struct {
	root, out, check, logs string
}

func makeOutputs(root string) (outputs, error) {
	o := outputs{root: root, out: filepath.Join(root, "out"), check: filepath.Join(root, "check"), logs: filepath.Join(root, "logs")}
	if ents, err := os.ReadDir(o.out); err == nil && len(ents) > 0 {
		return o, fmt.Errorf("%s is not empty: a run starts in a new directory", o.out)
	}
	for _, d := range []string{o.out, o.check, o.logs} {
		if err := os.MkdirAll(d, 0o755); err != nil {
			return o, err
		}
	}
	return o, nil
}

// results is everything a run learned, for the files it writes and for tests.
type results struct {
	cfg             Config
	files           []*fileReport
	sinks           []sinkResult
	cads            []*cadence
	place           *placeRun
	calls           *callRun
	ids             *ids
	podIvs          map[string][]interval // by cadence name
	nodeIvs         map[string][]interval
	edgeIvs         map[string][]interval
	callIvs         []interval
	fixture         sinkResult
	fixtureRaw      []rawAssertion
	fixtureHist     map[int64]int64
	fixtureNodeRows map[string]int            // samples of each node the fixture kept
	asked           map[string]int64          // questions the in-process store check asked, by stream
	sinkRaw         map[string][]rawAssertion // the raw assertions of each stream, kept when VerifyStreams is set
	tables          map[string]int64          // rows of each table
}

// reduce runs the whole reduction of one hour into the output directory.
func reduce(cfg Config) (*results, error) {
	if err := cfg.validate(); err != nil {
		return nil, err
	}
	if err := refuseWorkTree(cfg.OutDir); err != nil {
		return nil, err
	}
	sums, err := readSums(cfg.InDir)
	if err != nil {
		return nil, err
	}
	parch, err := listArchives(cfg.InDir, placementTable, cfg.PlacementFiles)
	if err != nil {
		return nil, err
	}
	carch, err := listArchives(cfg.InDir, callsTable, cfg.CallFiles)
	if err != nil {
		return nil, err
	}
	o, err := makeOutputs(cfg.OutDir)
	if err != nil {
		return nil, err
	}
	res := &results{
		cfg: cfg, ids: newIDs(), podIvs: map[string][]interval{}, nodeIvs: map[string][]interval{}, edgeIvs: map[string][]interval{},
		asked: map[string]int64{}, sinkRaw: map[string][]rawAssertion{}, tables: map[string]int64{},
	}
	d := res.ids

	// Placement.
	c60, err := newSink("placement_c60", filepath.Join(o.out, "placement_c60.parquet"), cfg.WriteC60, cfg.MaxFileBytes, cfg.VerifyStreams && cfg.WriteC60)
	if err != nil {
		return nil, err
	}
	c300, err := newSink("placement_c300", filepath.Join(o.out, "placement_c300.parquet"), true, cfg.MaxFileBytes, cfg.VerifyStreams)
	if err != nil {
		return nil, err
	}
	res.cads = []*cadence{
		newCadence(cadenceC60, c60, cfg.mut == mutGapAtTTLBreaks),
		newCadence(cadenceC300, c300, cfg.mut == mutGapAtTTLBreaks),
	}
	pr := newPlaceRun(&cfg, d, res.cads, true)
	res.place = pr
	res.cads[0].edges.onGap = func(missed int32) { pr.gaps[int64(missed)]++ }
	fx := newFixture(&cfg)
	// The cadences are reduced one after the other, each from its own read of the
	// archives, so the coalescer of one cadence is gone before the next one's
	// starts. The first pass also counts the statistics of the grid and keeps the
	// fixture's rows; the later passes take the samples again and count nothing.
	for pass, c := range res.cads {
		pr.restart([]*cadence{c}, pass == 0)
		for _, a := range parch {
			cfg.logf("placement %s: %s", c.spec.name, a.Rel)
			rep, first, byMinute, err := pr.readPlacement(&cfg, a, sums)
			if err != nil {
				return nil, err
			}
			if pass == 0 {
				res.files = append(res.files, rep)
			}
			pr.ensure()
			if cfg.mut == mutSplitAtFileBoundary && a.Index > 0 {
				pr.splitAll()
			}
			for i, rows := range byMinute {
				m := first + int32(i)
				if err := pr.minute(m, int32(a.Index), rows); err != nil {
					return nil, err
				}
				if pass == 0 {
					fx.retain(m, rows)
				}
				byMinute[i] = nil
			}
			cfg.logf("placement %s: %s: %d rows, %d used", c.spec.name, a.Rel, rep.Rows, rep.Used)
		}
		if pass == 0 {
			// Every node of the hour is known, and the later passes meet no new token:
			// the fixture's rows are cut down now, not when the last pass is done.
			fx.selectRows(d.pods, d.nodes, d.names)
			res.fixtureNodeRows = map[string]int{}
			for _, rows := range fx.rows {
				for _, w := range rows {
					res.fixtureNodeRows[d.nodes.name(w.node)]++
				}
			}
		}
		r, err := c.sink.finish()
		if err != nil {
			return nil, err
		}
		res.sinks = append(res.sinks, r)
		res.podIvs[c.spec.name] = c.pods.finish()
		res.nodeIvs[c.spec.name] = c.nodes.finish()
		res.edgeIvs[c.spec.name] = c.edges.finish()
	}
	pr.cads, pr.statistics = res.cads, true

	// Calls.
	callSink, err := newSink("calls", filepath.Join(o.out, "calls.parquet"), true, cfg.MaxFileBytes, cfg.VerifyStreams)
	if err != nil {
		return nil, err
	}
	rollup, err := newTSV(filepath.Join(o.out, "call_rollup_minutes.tsv.gz"),
		"minute", "um", "dm", "rpctype", "calls", "rt_count", "rt_sum_milli", "traces")
	if err != nil {
		return nil, err
	}
	cr := &callRun{
		cfg: &cfg, mut: cfg.mut, epoch: cfg.Epoch, svcs: d.svcs, fps: d.svcFP, sink: callSink, rollup: rollup,
		edges: newTracker(1, int32(callEdgeTTL/sampleInterval)),
		depth: map[int64]int64{}, fanOut: map[int64]int64{}, fanIn: map[int64]int64{},
	}
	cr.edges.strict = cfg.mut == mutGapAtTTLBreaks
	cr.onEdge = func(minute int32, um, dm uint32) { fx.edge(minute, d.svcs, um, dm) }
	res.calls = cr
	for _, a := range carch {
		cfg.logf("calls: %s", a.Rel)
		rep, err := cr.readCalls(a, sums)
		if err != nil {
			return nil, err
		}
		cr.files = append(cr.files, rep)
		res.files = append(res.files, rep)
		cfg.logf("calls: %s: %d rows, %d used", a.Rel, rep.Rows, rep.Used)
	}
	if err := rollup.close(); err != nil {
		return nil, err
	}
	res.tables["call_rollup_minutes"] = rollup.rows
	cres, err := callSink.finish()
	if err != nil {
		return nil, err
	}
	res.sinks = append(res.sinks, cres)
	res.callIvs = cr.edges.finish()

	// The fixture, and the checks of the product's semantics on it.
	fres, raw, err := fx.build(o.out, d)
	if err != nil {
		return nil, err
	}
	res.fixture, res.fixtureRaw = fres, raw
	n, err := verifyStream(fres.File, raw)
	if err != nil {
		return nil, fmt.Errorf("the fixture does not say what its samples say: %w", err)
	}
	res.asked["fixture"] = n
	fxstats := newPlaceRun(&cfg, d, nil, true)
	fxstats.ensure()
	for m, rows := range fx.rows {
		if err := fxstats.minute(int32(m), 0, rows); err != nil {
			return nil, err
		}
	}
	res.fixtureHist = fxstats.perNode

	if cfg.VerifyStreams {
		streams := map[string]*sink{"placement_c300": c300, "calls": callSink}
		if cfg.WriteC60 {
			streams["placement_c60"] = c60
		}
		for _, name := range []string{"placement_c60", "placement_c300", "calls"} {
			s, ok := streams[name]
			if !ok || s.res.File == "" {
				continue
			}
			n, err := verifyStream(s.res.File, s.raw)
			if err != nil {
				return nil, fmt.Errorf("%s does not say what its samples say: %w", name, err)
			}
			res.asked[name] = n
			res.sinkRaw[name] = s.raw
		}
	}

	if err := res.writeTables(o); err != nil {
		return nil, err
	}
	if err := res.writeJSON(o); err != nil {
		return nil, err
	}
	if err := os.WriteFile(filepath.Join(o.root, "REDUCTION.md"), []byte(res.report()), 0o644); err != nil {
		return nil, err
	}
	return res, nil
}

// token is a token for a TSV: only what precedes the separator of a mutant's name.
func token(s string) string {
	t, _, _ := strings.Cut(s, "\x00")
	return t
}

func (r *results) ns(minute int32, extra time.Duration) string {
	return itoa(r.cfg.Epoch.Add(time.Duration(minute) * sampleInterval).Add(extra).UnixNano())
}

// writeTables writes the gzip'd TSV tables, the dirt cases and the skips.
func (r *results) writeTables(o outputs) error {
	pr, d := r.place, r.ids
	hour := r.cfg.hourMinutes()

	// placement_intervals: the intervals of each pod on each node, by cadence.
	iv, err := newTSV(filepath.Join(o.out, "placement_intervals.tsv.gz"),
		"cadence", "pod", "node", "msname", "start_ns", "end_ns", "samples", "left_censored", "right_censored")
	if err != nil {
		return err
	}
	for _, c := range r.cads {
		ivs := slices.Clone(r.edgeIvs[c.spec.name])
		slices.SortFunc(ivs, func(a, b interval) int {
			ap, an := unpair(a.key)
			bp, bn := unpair(b.key)
			return cmpStrings(d.pods.name(ap), d.pods.name(bp), d.nodes.name(an), d.nodes.name(bn), int64(a.start), int64(b.start))
		})
		for _, x := range ivs {
			p, n := unpair(x.key)
			left, right := censored(x, c.spec.ttlMinutes(), hour)
			iv.row(c.spec.name, d.pods.name(p), d.nodes.name(n), token(d.names.name(x.tag)), r.ns(x.start, 0),
				r.ns(x.last, c.spec.ttl()), itoa(int64(x.samples)), btoa(left), btoa(right))
		}
	}
	if err := iv.close(); err != nil {
		return err
	}
	r.tables["placement_intervals"] = iv.rows

	mn, err := newTSV(filepath.Join(o.out, "placement_minutes.tsv.gz"),
		"minute", "pods", "nodes", "appearances", "disappearances", "moves",
		"pods_per_node_p50", "pods_per_node_p90", "pods_per_node_p99", "pods_per_node_max")
	if err != nil {
		return err
	}
	for _, m := range pr.minutes {
		mn.row(itoa(int64(m.minute)), itoa(int64(m.pods)), itoa(int64(m.nodes)), itoa(int64(m.appear)), itoa(int64(m.disappear)),
			itoa(int64(m.moves)), itoa(m.p50), itoa(m.p90), itoa(m.p99), itoa(m.maxPerNode))
	}
	if err := mn.close(); err != nil {
		return err
	}
	r.tables["placement_minutes"] = mn.rows

	callHour := int32(int64(r.cfg.CallFiles) * r.cfg.CallFileMillis / 60000)
	ce, err := newTSV(filepath.Join(o.out, "call_edges_intervals.tsv.gz"),
		"um", "dm", "start_ns", "end_ns", "minutes", "files", "left_censored", "right_censored")
	if err != nil {
		return err
	}
	civs := slices.Clone(r.callIvs)
	slices.SortFunc(civs, func(a, b interval) int {
		au, ad := unpair(a.key)
		bu, bd := unpair(b.key)
		return cmpStrings(d.svcs.name(au), d.svcs.name(bu), d.svcs.name(ad), d.svcs.name(bd), int64(a.start), int64(b.start))
	})
	for _, x := range civs {
		u, dm := unpair(x.key)
		left, right := censored(x, int32(callEdgeTTL/sampleInterval), callHour)
		ce.row(d.svcs.name(u), d.svcs.name(dm), r.ns(x.start, 0), r.ns(x.last, callEdgeTTL), itoa(int64(x.samples)),
			itoa(int64(x.files)), btoa(left), btoa(right))
	}
	if err := ce.close(); err != nil {
		return err
	}
	r.tables["call_edges_intervals"] = ce.rows

	dt, err := newTSV(filepath.Join(o.out, "dirt_cases.tsv"), "kind", "token", "minute", "a", "b")
	if err != nil {
		return err
	}
	for _, k := range dirtKinds {
		for _, c := range pr.dirt.listed(k) {
			dt.row(c.kind, c.token, itoa(int64(c.minute)), token(c.a), token(c.b))
		}
	}
	if err := dt.close(); err != nil {
		return err
	}
	r.tables["dirt_cases"] = dt.rows

	sk, err := newTSV(filepath.Join(o.out, "skips.tsv"), "table", "file", "key", "value")
	if err != nil {
		return err
	}
	for _, f := range r.files {
		sk.row(f.Table, f.Name, "rows", itoa(f.Rows))
		sk.row(f.Table, f.Name, "used", itoa(f.Used))
		for i := reason(0); i < numReasons; i++ {
			sk.row(f.Table, f.Name, i.String(), itoa(f.skips[i]))
		}
	}
	if err := sk.close(); err != nil {
		return err
	}
	r.tables["skips"] = sk.rows
	return nil
}

// cmpStrings compares two pairs of strings and then two numbers, in turn.
func cmpStrings(a1, b1, a2, b2 string, n1, n2 int64) int {
	switch {
	case a1 != b1:
		return strings.Compare(a1, b1)
	case a2 != b2:
		return strings.Compare(a2, b2)
	case n1 < n2:
		return -1
	case n1 > n2:
		return 1
	}
	return 0
}

// writeJSON writes params.json and manifest.json.
func (r *results) writeJSON(o outputs) error {
	p := r.params()
	m := r.manifest()
	return errors.Join(
		writeJSONFile(filepath.Join(o.root, "params.json"), p),
		writeJSONFile(filepath.Join(o.root, "manifest.json"), m),
	)
}

func writeJSONFile(path string, v any) error {
	b, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(path, append(b, '\n'), 0o644)
}
