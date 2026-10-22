package pgspike

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"

	"github.com/jackc/pgx/v5"

	"github.com/lotannauo/toposhift/bench/postgres/pgengine"
	"github.com/lotannauo/toposhift/bench/postgres/pginput"
	"github.com/lotannauo/toposhift/bench/postgres/pgreport"
	"github.com/lotannauo/toposhift/bench/spike/runner"
)

func runQuestions(_ context.Context, args []string, stdout, stderr io.Writer) error {
	fs := newFlags("questions", stderr)
	var out, activity, expect, label string
	var limit int64
	var basis, boot bool
	fs.StringVar(&out, "out", "", "the directory questions.json is written to; never inside a git work tree")
	fs.StringVar(&activity, "activity", "", "the activity file (Parquet)")
	fs.StringVar(&expect, "expect-activity", "", "the content digest of the activity file, or a prefix of at least 12 hex digits (required)")
	fs.StringVar(&label, "label", "", "what the stream is, recorded with every figure from it")
	fs.Int64Var(&limit, "limit", 0, "read only about this many records (for trying the pipeline; the set says so)")
	fs.BoolVar(&basis, "drop-basis", false, "accept records that have an event time basis, and drop it")
	fs.BoolVar(&boot, "drop-boot", false, "accept records that have a boot id, and drop it")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if fs.NArg() != 0 || activity == "" || out == "" {
		return errors.New("usage: pgspike questions -activity file -expect-activity digest -out dir [-limit n] [-label text] [-drop-basis] [-drop-boot]")
	}
	if err := RefuseInWorkTree(out); err != nil {
		return err
	}
	if expect == "" {
		return errors.New("pgspike: the activity file must be named by its content digest (-expect-activity)")
	}
	if err := checkFree(out, "questions.json"); err != nil {
		return err
	}
	q, err := pginput.BuildQuestions(activity, pginput.Options{DropBasis: basis, DropBoot: boot}, limit, label, progress(stderr))
	if err != nil {
		return err
	}
	if limit == 0 {
		// The digest in the footer is of the rows, checked against them by the reader at
		// the end of the file.
		if err := CheckContentDigest(q.ContentDigest, expect); err != nil {
			return err
		}
	}
	if err := os.MkdirAll(out, 0o700); err != nil {
		return err
	}
	if err := q.Save(out + "/questions.json"); err != nil {
		return err
	}
	b, err := os.ReadFile(out + "/questions.json")
	if err != nil {
		return err
	}
	sum := sha256.Sum256(b)
	fmt.Fprintf(stdout, "%d questions over %d records (last seq %d), old token %d\n", len(q.Queries), q.Records, q.LastSeq, q.OldToken)
	fmt.Fprintf(stdout, "activity content digest %s\nquestions digest %s\nquestions file sha256 %s\n", q.ContentDigest, q.QueriesDigest, hex.EncodeToString(sum[:]))
	return nil
}

func runExplain(ctx context.Context, args []string, stdout, stderr io.Writer) error {
	fs := newFlags("explain", stderr)
	var c common
	var in inputFlags
	var query string
	c.register(fs, true)
	in.register(fs, false)
	fs.StringVar(&query, "query", "", "the question: its number in the plan, or a part of its name")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if fs.NArg() != 0 || c.schema == "" || query == "" {
		return errors.New("usage: pgspike explain -settings settings.conf -dsn dsn -arm arm -schema name -query n|name (-plan plan.json -expect-plan sha | -questions q.json -expect-questions sha)")
	}
	arm, err := bulkArm(c.arm, false)
	if err != nil {
		return err
	}
	if err := c.validate(false); err != nil {
		return err
	}
	st, err := in.resolve(false)
	if err != nil {
		return err
	}
	var picked []int
	if n, err := strconv.Atoi(query); err == nil {
		if n < 0 || n >= len(st.queries()) {
			return fmt.Errorf("pgspike: there is no question %d among %d", n, len(st.queries()))
		}
		picked = []int{n}
	} else {
		for i, q := range st.queries() {
			if strings.Contains(q.Name(), query) {
				picked = append(picked, i)
			}
		}
		if len(picked) == 0 {
			return fmt.Errorf("pgspike: no question has %q in its name", query)
		}
	}
	s, err := c.begin(ctx, false)
	if err != nil {
		return err
	}
	defer s.close(ctx)
	rec := &pgengine.Recorder{}
	e, err := pgengine.Open(ctx, pgengine.Config{DSN: c.dsn, Schema: c.schema, Arm: arm, MaxConns: 2, Tracer: rec})
	if err != nil {
		return err
	}
	defer e.Close()
	if _, err := s.conn.Exec(ctx, "CHECKPOINT"); err != nil {
		return err
	}
	cc, err := pgx.ParseConfig(c.dsn)
	if err != nil {
		return err
	}
	cc.RuntimeParams["search_path"] = pgx.Identifier{c.schema}.Sanitize() + ", public"
	conn, err := pgx.ConnectConfig(ctx, cc)
	if err != nil {
		return err
	}
	defer func() { _ = conn.Close(ctx) }()
	for _, i := range picked {
		q := st.queries()[i]
		rec.Start()
		if _, err := runner.Ask(e, q); err != nil {
			return err
		}
		stmts := rec.Stop()
		tx, err := conn.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.RepeatableRead, AccessMode: pgx.ReadOnly})
		if err != nil {
			return err
		}
		runs, err := pgengine.ColdExplain(ctx, tx, c.schema, nil, stmts)
		_ = tx.Rollback(ctx)
		if err != nil {
			return err
		}
		fmt.Fprintf(stdout, "-- question %d: %s\n", i, q.Name())
		for j, r := range runs {
			var pretty bytes.Buffer
			if err := json.Indent(&pretty, r.JSON, "", " "); err != nil {
				return err
			}
			fmt.Fprintf(stdout, "-- statement %d: %s\n-- plan: %s\n%s\n", j, strings.Join(strings.Fields(stmts[j].SQL), " "), r.Counters.Shape(), pretty.String())
		}
	}
	return nil
}

func runSizes(ctx context.Context, args []string, stdout, stderr io.Writer) error {
	fs := newFlags("sizes", stderr)
	var c common
	var records uint64
	var compact bool
	c.register(fs, true)
	fs.Uint64Var(&records, "records", 0, "the records of the stream in the tables (required): sizes are given per record")
	fs.BoolVar(&compact, "compact", false, "also rewrite every table and rebuild its indexes (VACUUM FULL, REINDEX) and record the sizes after; this changes the pages reads are counted on")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if fs.NArg() != 0 || c.schema == "" || records == 0 {
		return errors.New("usage: pgspike sizes -settings settings.conf -dsn dsn -arm arm -schema name -records n -out dir [-compact]")
	}
	arm, err := bulkArm(c.arm, true)
	if err != nil {
		return err
	}
	if err := c.validate(true); err != nil {
		return err
	}
	if err := checkFree(c.out, "sizes.json"); err != nil {
		return err
	}
	s, err := c.begin(ctx, true)
	if err != nil {
		return err
	}
	defer s.close(ctx)
	if _, err := s.conn.Exec(ctx, "CHECKPOINT"); err != nil {
		return err
	}
	left, err := pgengine.Sizes(ctx, s.conn, c.schema)
	if err != nil {
		return err
	}
	rep := &pgreport.SizesReport{Version: pgreport.Version, Env: s.env, Arm: string(arm), Schema: c.schema, Records: records, AsLeft: pgreport.NewSizes(relations(left))}
	if len(left) == 0 {
		return fmt.Errorf("pgspike: schema %s has no relation", c.schema)
	}
	if compact {
		if err := pgengine.Compact(ctx, s.conn, c.schema); err != nil {
			return err
		}
		after, err := pgengine.Sizes(ctx, s.conn, c.schema)
		if err != nil {
			return err
		}
		sz := pgreport.NewSizes(relations(after))
		rep.Compacted = &sz
	}
	if err := writeNew(c.out, "sizes.json", rep); err != nil {
		return err
	}
	show := func(label string, z pgreport.Sizes) {
		fmt.Fprintf(stdout, "%s: %d bytes (intervals %d, log %d, dictionary %d), %d records\n", label, z.Total,
			z.ByGroup[pgreport.GroupIntervals], z.ByGroup[pgreport.GroupLog], z.ByGroup[pgreport.GroupDictionary], records)
	}
	show("as left", rep.AsLeft)
	if rep.Compacted != nil {
		show("compacted", *rep.Compacted)
	}
	fmt.Fprintf(stdout, "wrote %s/sizes.json\n", c.out)
	return nil
}

func runDiff(args []string, stdout, stderr io.Writer) error {
	fs := newFlags("diff", stderr)
	var kind string
	var tol float64
	fs.StringVar(&kind, "kind", "", "load, stream, read or sizes")
	fs.Float64Var(&tol, "wal-tolerance", 0, "the relative difference of a phase's log counts that is not a difference (default 0: exact)")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if fs.NArg() != 2 || kind == "" {
		return errors.New("usage: pgspike diff -kind load|stream|read|sizes [-wal-tolerance f] a.json b.json")
	}
	a, b := fs.Arg(0), fs.Arg(1)
	var res pgreport.Comparison
	var err error
	switch kind {
	case "load":
		var x, y pgreport.Load
		if err = errors.Join(pgreport.ReadFile(a, &x), pgreport.ReadFile(b, &y)); err == nil {
			res, err = pgreport.DiffLoads(x, y, pgreport.Options{WALTolerance: tol})
		}
	case "stream":
		var x, y pgreport.Stream
		if err = errors.Join(pgreport.ReadFile(a, &x), pgreport.ReadFile(b, &y)); err == nil {
			res, err = pgreport.DiffStreams(x, y, pgreport.Options{WALTolerance: tol})
		}
	case "read":
		var x, y pgreport.Read
		if err = errors.Join(pgreport.ReadFile(a, &x), pgreport.ReadFile(b, &y)); err == nil {
			res, err = pgreport.DiffReads(x, y)
		}
	case "sizes":
		var x, y pgreport.SizesReport
		if err = errors.Join(pgreport.ReadFile(a, &x), pgreport.ReadFile(b, &y)); err == nil {
			res, err = pgreport.DiffSizes(x, y)
		}
	default:
		return fmt.Errorf("pgspike: -kind must be load, stream, read or sizes, not %q", kind)
	}
	if err != nil {
		return err
	}
	for _, s := range res.Skipped {
		fmt.Fprintf(stdout, "not compared: %s\n", s)
	}
	if len(res.EnvDiffers) > 0 {
		fmt.Fprintf(stdout, "made by another %s (not a difference of the runs)\n", strings.Join(res.EnvDiffers, ", "))
	}
	for _, d := range res.Differences {
		fmt.Fprintln(stdout, d)
	}
	if !res.Equal() {
		return fmt.Errorf("pgspike: the %d differences above: the runs are not equal", len(res.Differences))
	}
	fmt.Fprintf(stdout, "the two %ss are equal\n", kind)
	return nil
}
