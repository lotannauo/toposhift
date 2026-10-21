package pgspike

import (
	"context"
	"errors"
	"fmt"
	"io"
	"math"
	"strconv"
	"strings"

	"github.com/lotannauo/toposhift/bench/postgres/g0"
	"github.com/lotannauo/toposhift/bench/postgres/pgengine"
	"github.com/lotannauo/toposhift/bench/postgres/pginput"
	"github.com/lotannauo/toposhift/bench/postgres/pgreport"
	"github.com/lotannauo/toposhift/bench/postgres/pgschema"
	"github.com/lotannauo/toposhift/bench/spike/runner"
)

func bulkArm(name string, withID bool) (pgschema.Arm, error) {
	switch a := pgschema.Arm(name); a {
	case pgschema.Bulk, pgschema.BulkB:
		return a, nil
	case pgschema.BulkID:
		if withID {
			return a, nil
		}
		return "", fmt.Errorf("pgspike: the arm %s is for sizes: it is loaded and measured by `load` and `sizes`, and has no reads", a)
	}
	return "", fmt.Errorf("pgspike: -arm must be pg-bulk, pg-bulk-b or pg-bulk-id, not %q", name)
}

func runLoad(ctx context.Context, args []string, stdout, stderr io.Writer) (err error) {
	fs := newFlags("load", stderr)
	var c common
	var in inputFlags
	var replace bool
	var maxMem string
	c.register(fs, true)
	in.register(fs, true)
	fs.StringVar(&maxMem, "max-interval-mem", "2GiB", "the most memory the load may need to hold its interval rows (bytes, or with KiB, MiB, GiB, TiB); a load that would need more does not run")
	fs.BoolVar(&replace, "replace", false, "drop the schema if it exists (without it, a schema that exists is an error)")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if fs.NArg() != 0 || c.schema == "" {
		return errors.New("usage: pgspike load -settings settings.conf -dsn dsn -arm arm -schema name -out dir (-plan plan.json -expect-plan sha | -activity f -expect-activity digest -questions q.json -expect-questions sha)")
	}
	arm, err := bulkArm(c.arm, true)
	if err != nil {
		return err
	}
	limit, err := ParseSize(maxMem)
	if err != nil {
		return fmt.Errorf("pgspike: -max-interval-mem: %w", err)
	}
	// Everything that can be refused is, before the server is touched.
	if err := c.validate(true); err != nil {
		return err
	}
	if err := checkFree(c.out, "load.json"); err != nil {
		return err
	}
	st, err := in.resolve(true)
	if err != nil {
		return err
	}
	s, err := c.begin(ctx, true)
	if err != nil {
		return err
	}
	defer s.close(ctx)

	say := progress(stderr)
	l, err := pgengine.NewLoader(ctx, pgengine.LoadConfig{DSN: c.dsn, Schema: c.schema, Arm: arm, Replace: replace, MaxIntervalMem: limit})
	if err != nil {
		return err
	}
	defer l.Close()
	// The schema is this run's: the loader refuses one that exists. A load that fails
	// leaves nothing half loaded to be mistaken for a load.
	defer func() {
		if err != nil {
			_ = l.Close()
			if derr := pgengine.DropSchema(context.WithoutCancel(ctx), c.dsn, c.schema); derr != nil {
				err = errors.Join(err, derr)
			}
		}
	}()
	if err := st.stream(ctx, l, say); err != nil {
		return err
	}
	say("load: building the intervals, the indexes and the vacuum")
	stats, err := l.Finish()
	if errors.Is(err, pgengine.ErrIntervalMemory) {
		return fmt.Errorf("pgspike: the load did not run (no schema and no record were left): %w", err)
	}
	if err != nil {
		return err
	}
	if err := st.checkLoaded(stats); err != nil {
		return err
	}
	dig, err := pgengine.DigestSchema(ctx, c.dsn, c.schema, arm)
	if err != nil {
		return err
	}
	sz, err := pgengine.Sizes(ctx, s.conn, c.schema)
	if err != nil {
		return err
	}
	rep := &pgreport.Load{
		Version: pgreport.Version, Env: s.env, Arm: string(arm), Schema: c.schema, Input: st.input(),
		Retentions: stats.Retentions, HorizonNS: stats.Horizon, PastEnd: stats.PastEnd, StreamVacuums: stats.StreamVacuums, ExtraVacuums: stats.ExtraVacuums,
		Rows:         pgreport.Rows{Log: stats.LogRows, LogBase: stats.LogBaseRows, Edges: stats.EdgeRows, Entities: stats.EntityRows, Dictionary: stats.DictionaryRows},
		TablesDigest: dig.Digest, TablesRows: dig.Rows, Sizes: pgreport.NewSizes(relations(sz)),
	}
	for _, p := range stats.Phases {
		rep.Phases = append(rep.Phases, pgreport.Phase{
			Name: p.Name, Contiguous: p.Contiguous, LogBytes: p.LogBytes, Records: p.Records, FPIs: p.FPIs,
			RecordBytes: p.RecordBytes, CheckpointsInside: p.CheckpointsInside,
		})
	}
	if err := writeNew(c.out, "load.json", rep); err != nil {
		return err
	}
	fmt.Fprintf(stdout, "load %s into schema %s: %d records, last seq %d, %d retentions\n", arm, c.schema, stats.Records, stats.LastSeq, stats.Retentions)
	fmt.Fprintf(stdout, "rows: %d log, %d baselines, %d edge intervals, %d entity intervals, %d dictionary\n", stats.LogRows, stats.LogBaseRows, stats.EdgeRows, stats.EntityRows, stats.DictionaryRows)
	fmt.Fprintf(stdout, "interval tables digest %s (%d rows, clipped at the horizon)\n", dig.Digest, dig.Rows)
	for _, p := range rep.Phases {
		fmt.Fprintf(stdout, "log %-14s %d records, %d full-page images, %d record bytes, %d checkpoints inside\n", p.Name, p.Records, p.FPIs, p.RecordBytes, p.CheckpointsInside)
	}
	fmt.Fprintf(stdout, "bytes as left: %d (intervals %d, log %d, dictionary %d)\n", rep.Sizes.Total,
		rep.Sizes.ByGroup[pgreport.GroupIntervals], rep.Sizes.ByGroup[pgreport.GroupLog], rep.Sizes.ByGroup[pgreport.GroupDictionary])
	fmt.Fprintf(stdout, "wrote %s/load.json\n", c.out)
	return nil
}

func relations(rs []pgengine.RelationSize) []pgreport.Relation {
	out := make([]pgreport.Relation, len(rs))
	for i, r := range rs {
		out[i] = pgreport.Relation{Name: r.Name, Kind: r.Kind, Table: r.Table, Main: r.Main, FSM: r.FSM, VM: r.VM}
	}
	return out
}

// stream writes the input's stream to the loader.
func (s *stream) stream(ctx context.Context, l *pgengine.Loader, say func(string, ...any)) error {
	if s.plan != nil {
		say("load: regenerating the stream of the plan (%d records)", s.plan.Stream.Records)
		info, err := runner.Drive(ctx, s.plan.Spec, l)
		if err != nil {
			return err
		}
		s.info = info
		return nil
	}
	say("load: reading %s (%d records)", s.activity, s.set.Records)
	info, err := pginput.ForEach(s.activity, pginput.QuestionBatch, s.opts, pginput.StopAfter(s.set.Limit, l.Write))
	if err != nil {
		return err
	}
	if s.set.Limit == 0 {
		// The digest in the footer is of the rows, and has been checked against them by
		// the reader at the end of the file.
		if err := CheckContentDigest(info.Digest, s.expect); err != nil {
			return err
		}
	}
	s.content = info.Digest
	return nil
}

// checkLoaded checks that what was loaded is the input's stream: the plan's digest and
// counts, or the question set's record count, last sequence number and content digest.
func (s *stream) checkLoaded(stats *pgengine.LoadStats) error {
	if s.plan != nil {
		return g0.CheckStream(s.plan, s.info, stats.LastSeq)
	}
	if stats.Records != s.set.Records || stats.LastSeq != s.set.LastSeq || s.content != s.set.ContentDigest {
		return fmt.Errorf("pgspike: the file loaded is not the one the questions were made for: %d records to seq %d, digest %s; the questions': %d, %d, %s",
			stats.Records, stats.LastSeq, s.content, s.set.Records, s.set.LastSeq, s.set.ContentDigest)
	}
	return nil
}

var sizeUnits = map[string]int64{"": 1, "b": 1, "kib": 1 << 10, "mib": 1 << 20, "gib": 1 << 30, "tib": 1 << 40}

// ParseSize reads a size in bytes: a positive whole number with an optional unit (B, KiB,
// MiB, GiB or TiB, in any case). A size of zero or less, a fraction and a unit that is
// not one of these are refused: a limit that is silently another is no limit.
func ParseSize(s string) (int64, error) {
	t := strings.TrimSpace(s)
	i := 0
	for i < len(t) && t[i] >= '0' && t[i] <= '9' {
		i++
	}
	n, err := strconv.ParseInt(t[:i], 10, 64)
	if err != nil || i == 0 {
		return 0, fmt.Errorf("%q is not a size: a whole number of bytes with an optional unit (KiB, MiB, GiB, TiB)", s)
	}
	unit, ok := sizeUnits[strings.ToLower(strings.TrimSpace(t[i:]))]
	if !ok {
		return 0, fmt.Errorf("%q has the unit %q, which is not B, KiB, MiB, GiB or TiB", s, strings.TrimSpace(t[i:]))
	}
	if n <= 0 || n > math.MaxInt64/unit {
		return 0, fmt.Errorf("%q is not a size from 1 byte up to what an int64 holds", s)
	}
	return n * unit, nil
}
