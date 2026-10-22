package pgspike

import (
	"context"
	"errors"
	"fmt"
	"io"

	"github.com/jackc/pgx/v5"

	"github.com/lotannauo/toposhift/bench/postgres/g0"
	"github.com/lotannauo/toposhift/bench/postgres/pgengine"
	"github.com/lotannauo/toposhift/bench/postgres/pgreport"
	"github.com/lotannauo/toposhift/bench/postgres/pgschema"
	"github.com/lotannauo/toposhift/bench/spike/runner"
)

func streamArm(name string) (pgschema.Arm, error) {
	switch a := pgschema.Arm(name); a {
	case pgschema.Stream, pgschema.StreamNoPartition, pgschema.StreamNaive:
		return a, nil
	}
	return "", fmt.Errorf("pgspike: -arm must be pg-stream, pg-stream-nopart or pg-stream-naive, not %q", name)
}

func runStream(ctx context.Context, args []string, stdout, stderr io.Writer) (err error) {
	fs := newFlags("stream", stderr)
	var c common
	var in inputFlags
	var replace, autovacuum bool
	c.register(fs, true)
	in.register(fs, false)
	fs.BoolVar(&replace, "replace", false, "drop the schema if it exists (without it, a schema that exists is an error)")
	fs.BoolVar(&autovacuum, "autovacuum", false, "leave vacuuming to the server's autovacuum, which is on in the regime time is measured in (by default the writer runs its own schedule, for the regime counters are taken in)")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if fs.NArg() != 0 || c.schema == "" {
		return errors.New("usage: pgspike stream -settings settings.conf -dsn dsn -arm pg-stream|pg-stream-nopart|pg-stream-naive -schema name -out dir -plan plan.json -expect-plan sha [-replace] [-autovacuum]")
	}
	arm, err := streamArm(c.arm)
	if err != nil {
		return err
	}
	// Everything that can be refused is, before the server is touched.
	if err := c.validate(true); err != nil {
		return err
	}
	if err := checkFree(c.out, "stream.json"); err != nil {
		return err
	}
	st, err := in.resolve(false)
	if err != nil {
		return err
	}
	if st.plan == nil {
		return errors.New("pgspike: stream is written from a plan (-plan with -expect-plan)")
	}
	s, err := c.begin(ctx, true)
	if err != nil {
		return err
	}
	defer s.close(ctx)

	var exists bool
	if err := s.conn.QueryRow(ctx, "SELECT to_regnamespace($1) IS NOT NULL", c.schema).Scan(&exists); err != nil {
		return fmt.Errorf("pgspike: %w", err)
	}
	if exists && !replace {
		return fmt.Errorf("pgspike: schema %s exists: a stream never adds to what another left (-replace drops it)", c.schema)
	}
	if replace {
		if err := pgengine.DropSchema(ctx, c.dsn, c.schema); err != nil {
			return err
		}
	}
	// The schema is this run's: one that fails leaves nothing half written to be mistaken
	// for a stream.
	defer func() {
		if err != nil {
			if derr := pgengine.DropSchema(context.WithoutCancel(ctx), c.dsn, c.schema); derr != nil {
				err = errors.Join(err, derr)
			}
		}
	}()
	e, err := pgengine.Open(ctx, pgengine.Config{DSN: c.dsn, Schema: c.schema, Arm: arm, NoVacuum: autovacuum})
	if err != nil {
		return err
	}
	defer e.Close()

	say := progress(stderr)
	meter := pgengine.NewStreamMeter(ctx, e, s.conn)
	if err := meter.Begin(); err != nil {
		return err
	}
	say("stream: writing the stream of the plan (%d records) to %s", st.plan.Stream.Records, arm)
	info, err := runner.Drive(ctx, st.plan.Spec, meter)
	if err != nil {
		return err
	}
	phases, err := meter.Finish()
	if err != nil {
		return err
	}
	if err := g0.CheckStream(st.plan, info, e.LastSeq()); err != nil {
		return err
	}
	counts := e.Counts()
	runs := e.VacuumRuns()
	// The tables as the stream left them, before any question touches a page.
	dig, err := pgengine.DigestSchema(ctx, c.dsn, c.schema, arm)
	if err != nil {
		return err
	}
	sz, err := pgengine.Sizes(ctx, s.conn, c.schema)
	if err != nil {
		return err
	}
	rows, err := tableRows(ctx, s, c.schema)
	if err != nil {
		return err
	}
	say("stream: asking the %d questions of the plan (G0)", len(st.plan.Queries))
	g, err := g0.Ask(ctx, st.plan.Queries, info, e, say)
	if err != nil {
		return err
	}
	rep := &pgreport.Stream{
		Version: pgreport.Version, Env: s.env, Arm: string(arm), Schema: c.schema, Input: st.input(),
		Retentions: len(info.Retentions), HorizonNS: dig.Horizon, Queries: g.Queries, Matched: g.Matched, Autovacuum: autovacuum,
		Writer: pgreport.WriterCounts{
			Records: counts.Records, FastSubjects: counts.FastSubjects, Refolds: counts.Refolds, RecordsFolded: counts.RecordsFolded,
			RowsInserted: counts.RowsInserted, RowsUpdated: counts.RowsUpdated, RowsDeleted: counts.RowsDeleted,
			RowsHeapOnly: counts.RowsHeapOnly, RowsFinalized: counts.RowsFinalized, Vacuums: counts.Vacuums,
			PartitionsDropped: counts.PartitionsDropped, VacuumRuns: runs, Analyzes: counts.Analyzes, AnalyzeRuns: e.AnalyzeRuns(),
		},
		Rows: rows, TablesDigest: dig.Digest, TablesRows: dig.Rows, Sizes: pgreport.NewSizes(relations(sz)),
	}
	for _, m := range g.Mismatches {
		x := pgreport.Mismatch{Query: m.Query, Op: string(m.Op), GotDigest: m.Got.Digest, WantDigest: m.Want.Digest, GotSize: m.Got.Size, WantSize: m.Want.Size, Error: m.AskError}
		rep.Mismatches = append(rep.Mismatches, x)
	}
	for _, p := range phases {
		rep.Phases = append(rep.Phases, pgreport.Phase{
			Name: p.Name, Contiguous: p.Contiguous, LogBytes: p.LogBytes, Records: p.Records, FPIs: p.FPIs,
			RecordBytes: p.RecordBytes, CheckpointsInside: p.CheckpointsInside, Spans: p.Spans,
		})
	}
	if err := writeNew(c.out, "stream.json", rep); err != nil {
		return err
	}
	fmt.Fprintf(stdout, "stream %s into schema %s: %d records, last seq %d, %d retentions\n", arm, c.schema, counts.Records, e.LastSeq(), len(info.Retentions))
	fmt.Fprintf(stdout, "G0: %d of %d answers equal the reference's\n", g.Matched, g.Queries)
	fmt.Fprintf(stdout, "writer: %d subjects without a read, %d re-folded (%d records folded)\n", counts.FastSubjects, counts.Refolds, counts.RecordsFolded)
	fmt.Fprintf(stdout, "interval rows: %d inserted, %d updated (%d heap-only, %d finalized), %d deleted\n", counts.RowsInserted, counts.RowsUpdated, counts.RowsHeapOnly, counts.RowsFinalized, counts.RowsDeleted)
	fmt.Fprintf(stdout, "vacuums: %d, analyzes: %d by the writer's schedule; partitions dropped: %d\n", counts.Vacuums, counts.Analyzes, counts.PartitionsDropped)
	fmt.Fprintf(stdout, "interval tables digest %s (%d rows, clipped at the horizon)\n", dig.Digest, dig.Rows)
	for _, p := range rep.Phases {
		if p.Spans > 0 {
			fmt.Fprintf(stdout, "log %-8s %d bytes of log position over %d spans, %d checkpoints inside\n", p.Name, p.LogBytes, p.Spans, p.CheckpointsInside)
			continue
		}
		fmt.Fprintf(stdout, "log %-8s %d bytes of log position, %d records, %d full-page images, %d record bytes, %d checkpoints inside\n", p.Name, p.LogBytes, p.Records, p.FPIs, p.RecordBytes, p.CheckpointsInside)
	}
	fmt.Fprintf(stdout, "bytes as left: %d (intervals %d, log %d)\n", rep.Sizes.Total, rep.Sizes.ByGroup[pgreport.GroupIntervals], rep.Sizes.ByGroup[pgreport.GroupLog])
	fmt.Fprintf(stdout, "wrote %s/stream.json\n", c.out)
	if g.Matched != g.Queries {
		return fmt.Errorf("pgspike: %d of %d answers differ from the reference's: the stream is not a figure", g.Queries-g.Matched, g.Queries)
	}
	return nil
}

// tableRows counts the rows of each table the stream left, by the server: the log is the
// sum over its partitions.
func tableRows(ctx context.Context, s *session, schema string) (pgreport.Rows, error) {
	var r pgreport.Rows
	for _, t := range []struct {
		table string
		into  *int64
	}{{"edges", &r.Edges}, {"entities", &r.Entities}, {"log_base", &r.LogBase}} {
		if err := s.conn.QueryRow(ctx, "SELECT count(*) FROM "+pgx.Identifier{schema, t.table}.Sanitize()).Scan(t.into); err != nil {
			return r, fmt.Errorf("pgspike: counting %s: %w", t.table, err)
		}
	}
	// A partitioned table is counted through its parent, and the rows of every partition
	// are in it.
	if err := s.conn.QueryRow(ctx, "SELECT count(*) FROM "+pgx.Identifier{schema, "log"}.Sanitize()).Scan(&r.Log); err != nil {
		return r, fmt.Errorf("pgspike: counting the log: %w", err)
	}
	return r, nil
}
