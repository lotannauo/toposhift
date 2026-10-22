package xtspike

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"reflect"

	"github.com/lotannauo/toposhift/bench/xtdb/g0"
	"github.com/lotannauo/toposhift/bench/xtdb/xtengine"
	"github.com/lotannauo/toposhift/bench/xtdb/xtreport"
)

// loadFlag is the flag that names the record of the load a run is held to.
func loadFlag(fs *flag.FlagSet, p *string) {
	fs.StringVar(p, "load", "", "the load.json of the load this is of (required)")
}

// readLoad reads a load record and returns it with its SHA-256.
func readLoad(path string) (*xtreport.Load, string, error) {
	if path == "" {
		return nil, "", errors.New("xtspike: -load is required: the record of the load this is held to")
	}
	var l xtreport.Load
	if err := xtreport.ReadFile(path, &l); err != nil {
		return nil, "", err
	}
	if l.Kind != "load" {
		return nil, "", fmt.Errorf("xtspike: %s is a record of kind %q, not a load", path, l.Kind)
	}
	sum, err := fileSHA(path)
	return &l, sum, err
}

// sameInput refuses a load that was not of the stream and questions this run names.
func sameInput(load, now xtreport.Input) error {
	for _, c := range []struct {
		name      string
		got, want any
	}{
		{"kind of input", load.Kind, now.Kind},
		{"plan digest", load.PlanSHA256, now.PlanSHA256},
		{"question set digest", load.QuestionsSHA256, now.QuestionsSHA256},
		{"content digest", load.ContentDigest, now.ContentDigest},
		{"stream digest", load.StreamDigest, now.StreamDigest},
		{"queries digest", load.QueriesDigest, now.QueriesDigest},
		{"records", load.Records, now.Records},
		{"last seq", load.LastSeq, now.LastSeq},
	} {
		if c.got != c.want {
			return fmt.Errorf("xtspike: the load record is not of this input: its %s is %v, this run's is %v", c.name, c.got, c.want)
		}
	}
	return nil
}

// sameBuild refuses a load made by another build of xtspike, against another image, options or
// configuration, or with another query mode, from this run's: two records are comparable
// only if one thing made them.
func sameBuild(load, now xtreport.Env) error {
	for _, c := range []struct {
		name      string
		got, want any
	}{
		{"commit", load.Commit, now.Commit},
		{"modified flag", load.Modified, now.Modified},
		{"binary", load.BinarySHA256, now.BinarySHA256},
		{"query mode", load.QueryMode, now.QueryMode},
		{"registered image", load.Image, now.Image},
		{"registered JVM options", load.JavaOptions, now.JavaOptions},
		{"configuration file", load.ConfigSHA256, now.ConfigSHA256},
		{"container", load.Container, now.Container},
		{"overrides", load.Overrides, now.Overrides},
	} {
		if !reflect.DeepEqual(c.got, c.want) {
			return fmt.Errorf("xtspike: the load record was made by another build or node: its %s is %v, this run's is %v", c.name, show(c.got), show(c.want))
		}
	}
	return nil
}

func show(v any) any {
	if v == nil {
		return "none"
	}
	return v
}

func (a *App) runRead(ctx context.Context, args []string) error {
	fs := newFlags("read", a.Stderr)
	var c common
	var in inputFlags
	var loadPath string
	a.register(&c, fs)
	in.register(fs, false)
	loadFlag(fs, &loadPath)
	if err := fs.Parse(args); err != nil {
		return err
	}
	if fs.NArg() != 0 {
		return errors.New("usage: xtspike read -script s -dsn d -out dir -load load.json (-plan p -expect-plan sha | -questions q -expect-questions sha)")
	}
	if err := c.validate(true, true); err != nil {
		return err
	}
	if err := checkFree(c.out, "read.json"); err != nil {
		return err
	}
	s, err := in.resolve(false)
	if err != nil {
		return err
	}
	load, loadSHA, err := readLoad(loadPath)
	if err != nil {
		return err
	}
	if err := sameInput(load.Input, s.input()); err != nil {
		return err
	}
	if load.Prefix != c.prefix {
		return fmt.Errorf("xtspike: the load was written under the prefix %q, this read is of %q", load.Prefix, c.prefix)
	}
	env, err := a.buildEnv(ctx, &c)
	if err != nil {
		return err
	}
	if err := sameBuild(load.Env, env); err != nil {
		return err
	}
	n, err := a.Open(ctx, xtengine.Config{DSN: c.dsn, Prefix: c.prefix})
	if err != nil {
		return err
	}
	defer n.Close()
	if n.LastSeq() != load.Input.LastSeq || n.Transactions() != load.Writer.Transactions {
		return fmt.Errorf("xtspike: the node holds last seq %d in %d transactions, the load wrote %d in %d: it is not the load's stream",
			n.LastSeq(), n.Transactions(), load.Input.LastSeq, load.Writer.Transactions)
	}
	res, err := a.settle(ctx, &c)
	if err != nil {
		return err
	}
	rep, err := g0.Ask(ctx, s.queries(), n, a.say)
	if err != nil {
		return err
	}
	rec := xtreport.Read{
		Version: xtreport.Version, Kind: "read", Env: env, Input: s.input(), Prefix: c.prefix, LoadSHA256: loadSHA,
		Settled: res.Settled, SettleReason: res.Reason, G0: g0Of(rep),
	}
	rec.Reads.Native, rec.Reads.ClientFold = n.Reads()
	rec.Capability = capabilityRows(load)
	if err := writeNew(c.out, "read.json", rec); err != nil {
		return err
	}
	for _, g := range rep.Groups {
		a.out("G0 %-40s %-10s %d of %d", g.Group, g.Age, g.Matched, g.Queries)
	}
	a.out("G0 total: %d of %d answers equal the reference engine's (answers digest %s)", rep.Matched, rep.Queries, rep.AnswersDigest)
	a.out("reads: %d answered at a system time or the newest, %d by a client fold of the log", rec.Reads.Native, rec.Reads.ClientFold)
	for _, cp := range rec.Capability {
		a.out("capability row: %s: %d %s: %s", cp.Family, cp.Rows, cp.Unit, cp.Note)
	}
	if !res.Settled {
		a.out("settle: DID NOT SETTLE (%s); the answers are checked, no size or timing is to be taken", res.Reason)
	}
	for _, m := range rep.Mismatches {
		a.out("MISMATCH %s: got %+v, reference %+v %s", m.Query, m.Got, m.Want, m.AskError)
	}
	a.out("record: %s/read.json", c.out)
	if !rep.OK() {
		return fmt.Errorf("xtspike: %d of %d answers differ from the reference engine's", rep.Queries-rep.Matched, rep.Queries)
	}
	return nil
}

func g0Of(r *g0.Report) xtreport.G0 {
	out := xtreport.G0{Queries: r.Queries, Matched: r.Matched, OK: r.OK(), AnswersDigest: r.AnswersDigest}
	for _, g := range r.Groups {
		out.Groups = append(out.Groups, xtreport.Group{Group: g.Group, Age: g.Age, Queries: g.Queries, Matched: g.Matched})
	}
	for _, m := range r.Mismatches {
		out.Mismatches = append(out.Mismatches, xtreport.Mismatch{
			Query: m.Query, Op: string(m.Op), Got: fmt.Sprintf("%+v", m.Got), Want: fmt.Sprintf("%+v", m.Want), Error: m.AskError,
		})
	}
	return out
}

// capabilityRows are what XTDB, in this mapping, did not do or could not hold, as rows and
// never as failures. The first two are counted from the load; the others are what the
// mapping and the node's version cannot give. No question of the plan is in a row: the plan
// asks nothing before a horizon, and a stream with a record XTDB cannot hold is not loaded.
func capabilityRows(load *xtreport.Load) []xtreport.Capability {
	return []xtreport.Capability{
		{
			Family: "sub-microsecond boundary", Rows: 0, Unit: "records refused",
			Note: "XTDB's temporal columns hold microseconds; a stream with a boundary off a whole microsecond is refused whole, and this one was loaded",
		},
		{
			Family: "retention", Rows: uint64(load.Input.Retentions), Unit: "horizons not applied",
			Note: "XTDB kept the whole stream; retention was not measured (its only removal is the erasure of a whole document)",
		},
		{
			Family: "read work", Rows: 0, Unit: "counters",
			Note: "no read-work counter: the node's EXPLAIN ANALYZE reports durations and two integers that were not shown to measure work",
		},
		{
			Family: "multi-hop reads", Rows: 0, Unit: "queries",
			Note: "WITH RECURSIVE is not supported by XTDB 2.1; the multi-hop reads were not built",
		},
	}
}
