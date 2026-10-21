package pgspike

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strings"

	"github.com/jackc/pgx/v5"

	"github.com/lotannauo/toposhift/bench/postgres/g0"
	"github.com/lotannauo/toposhift/bench/postgres/pgengine"
	"github.com/lotannauo/toposhift/bench/postgres/pgreport"
	"github.com/lotannauo/toposhift/bench/postgres/pgschema"
	"github.com/lotannauo/toposhift/bench/spike/runner"
)

func runRead(ctx context.Context, args []string, stdout, stderr io.Writer) error {
	fs := newFlags("read", stderr)
	var c common
	var in inputFlags
	var repeats int
	c.register(fs, true)
	in.register(fs, false)
	fs.IntVar(&repeats, "repeats", 2, "how many times each question is explained cold; the counters of the runs must be equal")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if fs.NArg() != 0 || c.schema == "" || repeats < 1 {
		return errors.New("usage: pgspike read -settings settings.conf -dsn dsn -arm arm -schema name -out dir (-plan plan.json -expect-plan sha | -questions q.json -expect-questions sha) [-repeats n]")
	}
	arm, err := bulkArm(c.arm, false)
	if err != nil {
		return err
	}
	if err := c.validate(true); err != nil {
		return err
	}
	if err := checkFree(c.out, "read.json"); err != nil {
		return err
	}
	st, err := in.resolve(false)
	if err != nil {
		return err
	}
	s, err := c.begin(ctx, true)
	if err != nil {
		return err
	}
	defer s.close(ctx)
	rep, err := readQuestions(ctx, readConfig{DSN: c.dsn, Schema: c.schema, Arm: arm, Repeats: repeats, Env: s.env, Stream: st}, progress(stderr))
	if rep != nil {
		if werr := writeNew(c.out, "read.json", rep); werr != nil {
			return errors.Join(err, werr)
		}
	}
	if err != nil {
		return err
	}
	var touched, read int64
	for _, r := range rep.Results {
		touched += r.BytesTouched
		read += r.BytesRead
	}
	fmt.Fprintf(stdout, "G0: %d of %d answers equal the reference's\n", rep.Matched, rep.Queries)
	fmt.Fprintf(stdout, "cold reads: %d questions, %d statements, each explained %d times with equal counters; %d bytes of blocks read, %d touched\n",
		len(rep.Results), len(rep.Statements), rep.Repeats, read, touched)
	fmt.Fprintf(stdout, "wrote %s/read.json\n", c.out)
	return nil
}

// readConfig says what is read.
type readConfig struct {
	DSN, Schema string
	Arm         pgschema.Arm
	Repeats     int
	Env         pgreport.Env
	Stream      *stream
}

// readQuestions asks every question of the stream of the tables a load left, checks each answer
// against the reference's (G0), and then, only if every answer matched, explains the
// statements each question ran, cold, as many times as asked, holding the runs to one
// another. A record of answers that do not match has no counters.
func readQuestions(ctx context.Context, cfg readConfig, say func(string, ...any)) (*pgreport.Read, error) {
	st := cfg.Stream
	queries := st.queries()
	rec := &pgengine.Recorder{}
	e, err := pgengine.Open(ctx, pgengine.Config{DSN: cfg.DSN, Schema: cfg.Schema, Arm: cfg.Arm, MaxConns: 2, Tracer: rec})
	if err != nil {
		return nil, err
	}
	defer e.Close()
	if got := e.LastSeq(); got != st.lastSeq() {
		return nil, fmt.Errorf("pgspike: schema %s holds a stream that ends at seq %d, not the input's %d", cfg.Schema, got, st.lastSeq())
	}
	rep := &pgreport.Read{Version: pgreport.Version, Env: cfg.Env, Arm: string(cfg.Arm), Schema: cfg.Schema, Input: st.input(), Queries: len(queries), Repeats: cfg.Repeats}

	say("read: asking %d questions", len(queries))
	stmts := make([][]pgengine.Statement, len(queries))
	for i, q := range queries {
		rec.Start()
		got, err := runner.Ask(e, q)
		stmts[i] = rec.Stop()
		want := runner.Answer{Digest: q.Expect, Size: q.Size}
		if err == nil && got == want {
			rep.Matched++
			continue
		}
		m := pgreport.Mismatch{Query: q.Name(), Op: string(q.Op), GotDigest: got.Digest, GotSize: got.Size, WantDigest: want.Digest, WantSize: want.Size}
		if err != nil {
			m.Error = err.Error()
		}
		if len(rep.Mismatches) < g0.MaxMismatches {
			rep.Mismatches = append(rep.Mismatches, m)
		}
	}
	if rep.Matched != rep.Queries {
		return rep, fmt.Errorf("pgspike: %d of %d answers differ from the reference's: no counter is kept", rep.Queries-rep.Matched, rep.Queries)
	}

	cc, err := pgx.ParseConfig(cfg.DSN)
	if err != nil {
		return nil, err
	}
	cc.RuntimeParams["search_path"] = pgx.Identifier{cfg.Schema}.Sanitize() + ", public"
	conn, err := pgx.ConnectConfig(ctx, cc)
	if err != nil {
		return nil, fmt.Errorf("pgspike: %w", err)
	}
	defer func() { _ = conn.Close(ctx) }()
	// Pages the load dirtied are written now, so that no read pays for them.
	if _, err := conn.Exec(ctx, "CHECKPOINT"); err != nil {
		return nil, fmt.Errorf("pgspike: checkpoint: %w", err)
	}
	lastSeq := e.LastSeq()
	index := map[string]int{}
	for i, q := range queries {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		runs, err := coldRuns(ctx, conn, cfg.Schema, stmts[i], cfg.Repeats)
		if err != nil {
			return nil, fmt.Errorf("pgspike: %s: %w", q.Name(), err)
		}
		res := pgreport.Result{Name: q.Name(), Group: q.Group, Age: q.Age, Op: string(q.Op), Digest: q.Expect, Size: q.Size}
		// A read of the interval tables as of an old token is not possible, since the
		// intervals have no ingest time: the reader folds the log. Windows read the log by
		// design and are not naive.
		res.Naive = q.Op != runner.OpWindow && q.AsOf < lastSeq
		folded := false
		for j, r := range runs {
			sql := strings.Join(strings.Fields(stmts[i][j].SQL), " ")
			k, ok := index[sql]
			if !ok {
				k = len(rep.Statements)
				index[sql] = k
				rep.Statements = append(rep.Statements, sql)
			}
			c := counters(r.Counters)
			res.Runs = append(res.Runs, pgreport.StatementRun{Statement: k, Counters: c, Shape: r.Counters.Shape()})
			res.Total = res.Total.Add(c)
			if pgengine.FoldStatement(stmts[i][j].SQL) {
				folded = true
				res.RowsFetched += c.RowsReturned
			}
		}
		if folded != res.Naive {
			return nil, fmt.Errorf("pgspike: %s: the read as of seq %d (the last is %d) ran %d statements, a fold of the log among them: %v", q.Name(), q.AsOf, lastSeq, len(runs), folded)
		}
		res.BytesRead = res.Total.SharedRead * int64(cfg.Env.BlockSize)
		res.BytesTouched = (res.Total.SharedRead + res.Total.SharedHit) * int64(cfg.Env.BlockSize)
		rep.Results = append(rep.Results, res)
	}
	return rep, nil
}

func counters(c pgengine.Counters) pgreport.Counters {
	return pgreport.Counters{
		SharedHit: c.SharedHit, SharedRead: c.SharedRead, RowsReturned: c.RowsReturned, IndexEntries: c.IndexEntries,
		HeapFetches: c.HeapFetches, RowsRemovedByFilter: c.RowsRemovedByFilter, RowsRemovedByRecheck: c.RowsRemovedByRecheck,
	}
}

// coldRuns explains the statements of one read cold, repeats times, each in a
// repeatable-read read-only transaction as the read ran, and returns the first run once
// it is known that every run counted the same.
func coldRuns(ctx context.Context, conn *pgx.Conn, schema string, stmts []pgengine.Statement, repeats int) ([]pgengine.ColdRun, error) {
	var first []pgengine.ColdRun
	for rep := 0; rep < repeats; rep++ {
		tx, err := conn.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.RepeatableRead, AccessMode: pgx.ReadOnly})
		if err != nil {
			return nil, err
		}
		runs, err := pgengine.ColdExplain(ctx, tx, schema, nil, stmts)
		_ = tx.Rollback(ctx)
		if err != nil {
			return nil, err
		}
		if rep == 0 {
			first = runs
			continue
		}
		for j := range runs {
			if counters(runs[j].Counters) != counters(first[j].Counters) || runs[j].Counters.Shape() != first[j].Counters.Shape() {
				return nil, fmt.Errorf("the cold counters of statement %d differ between run 1 (%+v, %s) and run %d (%+v, %s)",
					j, first[j].Counters, first[j].Counters.Shape(), rep+1, runs[j].Counters, runs[j].Counters.Shape())
			}
		}
	}
	return first, nil
}
