package xtspike

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/lotannauo/toposhift/bench/spike/engine"
	"github.com/lotannauo/toposhift/bench/spike/runner"
	"github.com/lotannauo/toposhift/bench/xtdb/g0"
	"github.com/lotannauo/toposhift/bench/xtdb/xtengine"
	"github.com/lotannauo/toposhift/bench/xtdb/xtinput"
	"github.com/lotannauo/toposhift/bench/xtdb/xtreport"
)

func (a *App) runLoad(ctx context.Context, args []string) error {
	fs := newFlags("load", a.Stderr)
	var c common
	var in inputFlags
	var base string
	a.register(&c, fs)
	in.register(fs, true)
	fs.StringVar(&base, "base", "", "the system time of the zeroth transaction, RFC 3339 on a whole microsecond, for a test that shares a node (default and registered: 2000-01-01T00:00:00Z)")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if fs.NArg() != 0 {
		return errors.New("usage: xtspike load -script s -dsn d -out dir (-plan p -expect-plan sha | -questions q -expect-questions sha -activity f -expect-activity digest)")
	}
	if err := c.validate(true, true); err != nil {
		return err
	}
	if err := checkFree(c.out, "load.json"); err != nil {
		return err
	}
	baseTime, err := parseBase(base, c.registered)
	if err != nil {
		return err
	}
	s, err := in.resolve(true)
	if err != nil {
		return err
	}
	if s.set != nil && s.set.Stats.OffMicrosecond > 0 {
		return fmt.Errorf("xtspike: %d records of the stream have a boundary that is not a whole microsecond, which XTDB's temporal columns cannot hold: the load is refused, and no record is dropped", s.set.Stats.OffMicrosecond)
	}
	env, err := a.buildEnv(ctx, &c)
	if err != nil {
		return err
	}
	n, err := a.openFresh(ctx, xtengine.Config{DSN: c.dsn, Prefix: c.prefix, Base: baseTime})
	if err != nil {
		return err
	}
	defer n.Close()

	rec := xtreport.Load{Version: xtreport.Version, Kind: "load", Env: env, Input: s.input(), Prefix: c.prefix, SystemBase: baseName(baseTime)}
	if s.plan != nil {
		if err := a.loadPlan(ctx, n, s); err != nil {
			return err
		}
	} else {
		if rec.Dropped, err = a.loadActivity(n, s); err != nil {
			return err
		}
	}
	rec.Writer = writerOf(n.Counts(), n.Transactions())
	if got := n.RefusedOffMicrosecond(); got != 0 {
		return fmt.Errorf("xtspike: the engine counted %d records refused for a boundary off a whole microsecond in a load that ended without error", got)
	}
	res, err := a.finishAndSettle(ctx, &c)
	if err != nil {
		return err
	}
	rec.BlockFinished, rec.Settled, rec.SettleReason = res.BlockFinished, res.Settled, res.Reason
	if err := writeNew(c.out, "load.json", rec); err != nil {
		return err
	}
	w := rec.Writer
	a.out("load: %d records in %d batches and %d retentions (%d transactions), last seq %d", w.Records, w.Batches, w.Retentions, w.Transactions, n.LastSeq())
	a.out("writer: %d statements (%d inserts, %d portion deletes), %.2f documents per record; %d records in order, %d late, %d segment statements merged away",
		w.Statements, w.Documents, w.Statements-w.Documents, w.DocumentsPerRecord, w.InOrder, w.Late, w.MergedAway)
	for _, t := range w.Tables {
		a.out("table %-6s inserts %d, portion deletes %d", t.Name, t.Inserts, t.PortionDeletes)
	}
	if rec.Dropped != (xtreport.Dropped{}) {
		a.out("dropped: %d records with an event time basis, %d with a boot id", rec.Dropped.Basis, rec.Dropped.Boot)
	}
	if !rec.BlockFinished {
		a.out("block: the node DID NOT FINISH ITS BLOCK (%s); its sizes are not to be taken", rec.SettleReason)
	}
	if rec.Settled {
		a.out("settle: the node finished its block and was quiet for a whole window")
	} else {
		a.out("settle: DID NOT SETTLE (%s); its sizes and timings are not to be taken", rec.SettleReason)
	}
	a.out("record: %s/load.json", c.out)
	return nil
}

// progressSink writes a stream to a node and says how far it has got, in batches and records,
// every so often.
type progressSink struct {
	e               engine.Engine
	say             func(string, ...any)
	batches, record uint64
}

func (p *progressSink) Write(b []engine.Record) error {
	if err := p.e.Write(b); err != nil {
		return err
	}
	p.batches++
	p.record += uint64(len(b))
	if p.batches%1000 == 0 {
		p.say("load: %d batches, %d records, last seq %d", p.batches, p.record, b[len(b)-1].Seq)
	}
	return nil
}

func (p *progressSink) Retain(h time.Time) error { return p.e.Retain(h) }

// loadPlan regenerates the stream of the plan into the node and checks it is the plan's.
func (a *App) loadPlan(ctx context.Context, n Node, s *stream) error {
	p := s.plan
	a.say("load: writing the stream of the plan (%d records, last seq %d)", p.Stream.Records, p.Stream.LastSeq)
	info, err := runner.Drive(ctx, p.Spec, &progressSink{e: n, say: a.say})
	if err != nil {
		return fmt.Errorf("xtspike: the load failed: %w", err)
	}
	if err := g0.CheckStream(p, info, n.LastSeq()); err != nil {
		return err
	}
	return nil
}

// loadActivity writes an activity file into the node in the batches its questions were made
// for, and checks that what was written is what the question set was made from.
func (a *App) loadActivity(n Node, s *stream) (xtreport.Dropped, error) {
	set := s.set
	a.say("load: writing %d records of the activity file in batches of %d", set.Records, xtinput.QuestionBatch)
	var (
		st      xtinput.Stats
		written uint64
		batches uint64
	)
	info, err := xtinput.ForEach(s.activity, xtinput.QuestionBatch, s.opts, &st, xtinput.StopAfter(set.Limit, func(rs []engine.Record) error {
		if err := n.Write(rs); err != nil {
			return err
		}
		written += uint64(len(rs))
		if batches++; batches%1000 == 0 {
			a.say("load: %d batches, %d records, last seq %d", batches, written, rs[len(rs)-1].Seq)
		}
		return nil
	}))
	if err != nil {
		return xtreport.Dropped{}, fmt.Errorf("xtspike: the load failed: %w", err)
	}
	if err := CheckContentDigest(info.Digest, s.expect); err != nil {
		return xtreport.Dropped{}, err
	}
	if info.Digest != set.ContentDigest || written != set.Records || n.LastSeq() != set.LastSeq || st != set.Stats {
		return xtreport.Dropped{}, fmt.Errorf("xtspike: the activity file is not the one the questions were made for: content digest %s, %d records, last seq %d, %+v; the set's: %s, %d, %d, %+v",
			info.Digest, written, n.LastSeq(), st, set.ContentDigest, set.Records, set.LastSeq, set.Stats)
	}
	return xtreport.Dropped{Basis: st.BasisDropped, Boot: st.BootDropped}, nil
}
