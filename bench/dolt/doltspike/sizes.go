package doltspike

import (
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"regexp"
	"strings"

	"github.com/lotannauo/toposhift/bench/dolt/doltengine"
	"github.com/lotannauo/toposhift/bench/dolt/doltmodel"
)

var (
	tableFile   = regexp.MustCompile(`^[0-9a-v]{32}(\.darc)?$`)
	journalFile = regexp.MustCompile(`^v{32}$`)
)

// MeasureDir is the bytes of the files under a database's directory, by kind: the chunk
// journal, the table files (archives included), the nested repository the server keeps its
// statistics in, and the rest (the manifest, locks, configuration). The bytes are the files'
// sizes, not the blocks the disk gives them. Symbolic links are not followed.
func MeasureDir(dir string) (Bytes, error) {
	var b Bytes
	st, err := os.Stat(dir)
	if err != nil {
		return b, err
	}
	if !st.IsDir() {
		return b, fmt.Errorf("doltspike: %s is not a directory", dir)
	}
	err = filepath.WalkDir(dir, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if !d.Type().IsRegular() {
			return nil
		}
		info, err := d.Info()
		if err != nil {
			return err
		}
		n := info.Size()
		rel, _ := filepath.Rel(dir, path)
		b.Total += n
		b.Files++
		name := d.Name()
		switch {
		case strings.HasPrefix(filepath.ToSlash(rel), ".dolt/stats/"):
			b.Statistics += n
		case journalFile.MatchString(name) || name == "journal.idx":
			b.Journal += n
		case tableFile.MatchString(name):
			b.Tables += n
		default:
			b.Other += n
		}
		return nil
	})
	return b, err
}

// sizesOpts say what the bytes of a loaded database are asked of.
type sizesOpts struct {
	c       *common
	load    string
	arm     doltmodel.Arm
	dataDir string
	gc      bool
	say     func(string, ...any)
}

// sizes measures the database's directory as it is left, then, if asked, runs garbage
// collection (with no connection open in the database) and measures it again, and checks that
// the database is as it was: the same head commit, the same last sequence number and the same
// rows in the logs.
func sizes(ctx context.Context, o sizesOpts, env Env) (*SizesRecord, error) {
	dbDir := filepath.Join(o.dataDir, o.c.db)
	if _, err := os.Stat(filepath.Join(dbDir, ".dolt", "noms", "manifest")); err != nil {
		return nil, fmt.Errorf("doltspike: %s holds no Dolt database (-data must be the server's data directory): %w", dbDir, err)
	}
	l, err := openLoaded(ctx, o.c, o.load, o.arm, nil)
	if err != nil {
		return nil, err
	}
	edge, ent, err := l.e.Held(ctx)
	if err != nil {
		_ = l.e.Close()
		return nil, err
	}
	if edge%2 != 0 {
		_ = l.e.Close()
		return nil, fmt.Errorf("doltspike: the edge log holds %d rows, which cannot be two for each record", edge)
	}
	rec := &SizesRecord{
		Kind: KindSizes, Format: RecordFormat, Env: env, Input: l.load.Input, Arm: o.arm.String(), Database: o.c.db,
		EdgeLogRows: edge, EntityLogRows: ent, RecordsHeld: edge/2 + ent, Commits: l.load.Commits, HeadHash: l.load.HeadHash,
	}
	if rec.RecordsHeld == 0 {
		_ = l.e.Close()
		return nil, errors.New("doltspike: the database holds no record")
	}
	if err := l.e.Close(); err != nil {
		return nil, err
	}
	if rec.AsLeft, err = MeasureDir(dbDir); err != nil {
		return nil, err
	}
	rec.BytesPerRecord = float64(rec.AsLeft.Total) / float64(rec.RecordsHeld)
	rec.BytesPerRecordNoStats = float64(rec.AsLeft.Total-rec.AsLeft.Statistics) / float64(rec.RecordsHeld)
	if !o.gc {
		return rec, nil
	}
	tell(o.say, "sizes: garbage collection of %s", o.c.db)
	if err := doltengine.GC(ctx, o.c.dsn, o.c.db); err != nil {
		return nil, err
	}
	// The connections a collection breaks are the engine's own, closed above: the database
	// is opened again, and must be what it was.
	l2, err := openLoaded(ctx, o.c, o.load, o.arm, nil)
	if err != nil {
		return nil, fmt.Errorf("doltspike: opening the database after garbage collection: %w", err)
	}
	edge2, ent2, err := l2.e.Held(ctx)
	_ = l2.e.Close()
	if err != nil {
		return nil, err
	}
	if edge2 != edge || ent2 != ent {
		return nil, fmt.Errorf("doltspike: garbage collection changed the logs: %d and %d rows, then %d and %d", edge, ent, edge2, ent2)
	}
	after, err := MeasureDir(dbDir)
	if err != nil {
		return nil, err
	}
	rec.AfterGC = &after
	rec.GCBytesPerRecord = float64(after.Total) / float64(rec.RecordsHeld)
	rec.GCBytesPerRecordNoStats = float64(after.Total-after.Statistics) / float64(rec.RecordsHeld)
	return rec, nil
}

func runSizes(ctx context.Context, args []string, stdout, stderr io.Writer) error {
	fs := newFlags("sizes", stderr)
	var (
		c        common
		arm      string
		loadPath string
		data     string
		gc       bool
	)
	c.register(fs, true)
	fs.StringVar(&arm, "arm", "", "the arm: commit or rows")
	fs.StringVar(&loadPath, "load", "", "the load record (load-<arm>.json) of the database")
	fs.StringVar(&data, "data", os.Getenv("TOPOSHIFT_DOLT_DATA"), "the server's data directory on this machine (default $TOPOSHIFT_DOLT_DATA)")
	fs.BoolVar(&gc, "gc", false, "also run garbage collection (no other connection may be open in the database) and measure again")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if loadPath == "" || data == "" || fs.NArg() != 0 {
		return errors.New("usage: doltspike sizes -dsn dsn -image image -db name -arm commit|rows -load load.json -data dir [-gc] [-out dir]")
	}
	a, err := parseArm(arm, false)
	if err != nil {
		return err
	}
	if err := c.validate(false, true); err != nil {
		return err
	}
	name := "sizes-" + arm + ".json"
	if c.out != "" {
		if err := checkFree(c.out, name); err != nil {
			return err
		}
	}
	env, err := serverEnv(ctx, c.dsn, c.image)
	if err != nil {
		return err
	}
	rec, err := sizes(ctx, sizesOpts{c: &c, load: loadPath, arm: a, dataDir: data, gc: gc, say: progress(stderr)}, env)
	if err != nil {
		return err
	}
	if c.out != "" {
		if err := writeNew(c.out, name, rec); err != nil {
			return err
		}
	}
	printSizes(stdout, rec)
	return nil
}

func printSizes(w io.Writer, r *SizesRecord) {
	fmt.Fprintf(w, "arm %s, database %s: %d commits, head %s\n", r.Arm, r.Database, r.Commits, r.HeadHash)
	fmt.Fprintf(w, "records held: %d (edge log %d rows, entity log %d rows)\n", r.RecordsHeld, r.EdgeLogRows, r.EntityLogRows)
	line := func(label string, b Bytes, perRecord, noStats float64) {
		fmt.Fprintf(w, "%s: %d bytes in %d files (journal %d, table files %d, statistics %d, other %d); %.2f bytes per record held, %.2f without the statistics\n",
			label, b.Total, b.Files, b.Journal, b.Tables, b.Statistics, b.Other, perRecord, noStats)
	}
	line("as left", r.AsLeft, r.BytesPerRecord, r.BytesPerRecordNoStats)
	if r.AfterGC != nil {
		line("after garbage collection", *r.AfterGC, r.GCBytesPerRecord, r.GCBytesPerRecordNoStats)
	}
}
