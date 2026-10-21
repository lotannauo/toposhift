package duckengine

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"errors"
	"math"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/lotannauo/toposhift/bench/duckdb/duckmodel"
	"github.com/lotannauo/toposhift/bench/spike/engine"
	"github.com/lotannauo/toposhift/bench/spike/runner"
	"github.com/lotannauo/toposhift/internal/catalog"
	"github.com/lotannauo/toposhift/internal/identity"
	"github.com/lotannauo/toposhift/internal/lifecycle"
)

const archiveSQLPath = "../../cmd/activitybytes/testdata/archive.sql"

// archiveSQLSHA256 is the digest of the statement that made the published archive. The
// file is not changed to follow this baseline: if it changes, the archive's bytes are
// of another statement.
const archiveSQLSHA256 = "530a8f9bec4726938459e5fc0eaf83e6e087aa62e1b7ceafb3d772826356ad72"

func seqsOf(t *testing.T, db *DB, file string) []uint64 {
	t.Helper()
	rows, err := db.QueryContext(context.Background(), "SELECT seq FROM read_parquet("+quote(file)+") ORDER BY seq")
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var out []uint64
	for rows.Next() {
		var s uint64
		if err := rows.Scan(&s); err != nil {
			t.Fatal(err)
		}
		out = append(out, s)
	}
	return out
}

func buildTiny(t *testing.T, plan *runner.Plan, dir string, arms []Arm, opts BuildOptions) (*Manifest, error) {
	t.Helper()
	db, err := Open(context.Background(), Config{Mode: ModeBuild, TempDir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	return Build(context.Background(), db, dir, PlanSource(plan, "tiny"), arms, opts)
}

// TestTheArchiveArmIsTheArchiveStatement: the statement of archive.sql, run as it is by
// a fresh DuckDB over the activity file, writes the very bytes the A1 arm holds, and the
// order and options in the code are that statement's.
func TestTheArchiveArmIsTheArchiveStatement(t *testing.T) {
	dir, _, man := sharedBuild(t)
	b, err := os.ReadFile(archiveSQLPath)
	if err != nil {
		t.Fatal(err)
	}
	sum := sha256.Sum256(b)
	if got := hex.EncodeToString(sum[:]); got != archiveSQLSHA256 {
		t.Fatalf("archive.sql has SHA-256 %s: the archive's bytes are of another statement than the baseline's", got)
	}
	text := string(b)
	if !strings.Contains(text, "ORDER BY "+ArchiveOrder+")") {
		t.Errorf("archive.sql does not order by %q", ArchiveOrder)
	}
	if !strings.Contains(text, "COMPRESSION zstd, ROW_GROUP_SIZE "+"1048576") || ArchiveRowGroup != 1048576 {
		t.Error("archive.sql's options are not zstd and 1,048,576 rows to a row group")
	}

	// Run it, statement by statement, on a DuckDB of our own.
	ctx := context.Background()
	out := filepath.Join(t.TempDir(), "archive.parquet")
	db, err := sql.Open("duckdb", "")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	db.SetMaxOpenConns(1)
	act := rolePathIn(dir, RoleActivity)
	for _, s := range []string{
		"SET VARIABLE src = " + quote(act), "SET VARIABLE dst = " + quote(out), "SET VARIABLE tmp = " + quote(t.TempDir()),
	} {
		if _, err := db.ExecContext(ctx, s); err != nil {
			t.Fatal(err)
		}
	}
	var stmts []string
	for _, line := range strings.Split(text, "\n") {
		if i := strings.Index(line, "--"); i >= 0 {
			line = line[:i]
		}
		stmts = append(stmts, line)
	}
	for _, s := range strings.Split(strings.Join(stmts, "\n"), ";") {
		if s = strings.TrimSpace(s); s == "" {
			continue
		}
		if strings.HasPrefix(s, "SELECT") {
			continue
		}
		if _, err := db.ExecContext(ctx, s); err != nil {
			t.Fatalf("%s: %v", s, err)
		}
	}
	got, err := fileSHA256(out)
	if err != nil {
		t.Fatal(err)
	}
	f, _ := man.File(RoleArchive)
	if got != f.SHA256 {
		t.Errorf("archive.sql wrote %s, the A1 arm holds %s", got, f.SHA256)
	}
}

// TestBuildsAreAFunctionOfTheStream: two builds of one stream are the same files.
func TestBuildsAreAFunctionOfTheStream(t *testing.T) {
	plan := tinyPlan(t, true)
	a, err := buildTiny(t, plan, filepath.Join(t.TempDir(), "a"), Arms, BuildOptions{})
	if err != nil {
		t.Fatal(err)
	}
	b, err := buildTiny(t, plan, filepath.Join(t.TempDir(), "b"), Arms, BuildOptions{VerifyModel: true})
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(a.Files, b.Files) || a.Stream != b.Stream {
		t.Fatalf("two builds differ:\n%+v\n%+v", a, b)
	}
}

// modelSink drives a model as the stream does, each retention in turn.
type modelSink struct{ m *duckmodel.Model }

func (s modelSink) Write(b []engine.Record) error { return s.m.Write(b) }
func (s modelSink) Retain(h time.Time) error      { return s.m.Retain(h) }

// TestTheFilesHoldWhatEachRetentionInTurnKeeps: the record files of a stream with two
// retentions hold the records a model keeps when it is retained at each horizon as the
// stream moves it, which is what applying the last once is to keep.
func TestTheFilesHoldWhatEachRetentionInTurnKeeps(t *testing.T) {
	dir, plan, man := sharedBuild(t)
	if man.Stream.Retentions != 2 || man.Stream.Kept >= man.Stream.Records || man.Stream.HorizonNanos == 0 {
		t.Fatalf("the fixture has no retention to speak of: %+v", man.Stream)
	}
	m := duckmodel.New(duckmodel.ArmRecords)
	if _, err := runner.Drive(context.Background(), plan.Spec, modelSink{m}); err != nil {
		t.Fatal(err)
	}
	var want []uint64
	for _, r := range m.Records() {
		want = append(want, r.Seq)
	}
	db := openRead(t, false)
	for _, role := range []Role{RoleActivity, RoleArchive, RoleFwd, RoleRev} {
		if got := seqsOf(t, db, rolePathIn(dir, role)); !slices.Equal(got, want) {
			t.Errorf("%s holds %d records, the retained model %d", role, len(got), len(want))
		}
	}
	if int(man.Stream.Kept) != len(want) {
		t.Errorf("the manifest says %d kept, the model holds %d", man.Stream.Kept, len(want))
	}
}

func TestBuildRefusesWhatItShould(t *testing.T) {
	ctx := context.Background()
	plan := tinyPlan(t, false)
	read := openRead(t, false)
	if _, err := Build(ctx, read, filepath.Join(t.TempDir(), "x"), PlanSource(plan, "t"), []Arm{ArmA2}, BuildOptions{}); err == nil {
		t.Error("a build on a read connection")
	}
	db, err := Open(ctx, Config{Mode: ModeBuild, TempDir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	build := func(dir string, src Source, arms ...Arm) error {
		_, err := Build(ctx, db, dir, src, arms, BuildOptions{})
		return err
	}
	if err := build(filepath.Join(t.TempDir(), "x"), PlanSource(plan, "t")); err == nil {
		t.Error("no arm")
	}
	if err := build(filepath.Join(t.TempDir(), "x"), Source{Label: "x"}, ArmA2); err == nil {
		t.Error("no stream")
	}
	full := t.TempDir()
	if err := os.WriteFile(filepath.Join(full, "something"), nil, 0o644); err != nil {
		t.Fatal(err)
	}
	if err := build(full, PlanSource(plan, "t"), ArmA2); err == nil || !strings.Contains(err.Error(), "not empty") {
		t.Errorf("a directory that holds something: %v", err)
	}

	// A stream that is not the same on both passes.
	retained := tinyPlan(t, true)
	src := PlanSource(retained, "t")
	calls := 0
	flaky := src
	flaky.Replay = func(ctx context.Context, sink runner.Sink) (string, error) {
		d, err := src.Replay(ctx, sink)
		calls++
		if calls == 2 {
			d = "another"
		}
		return d, err
	}
	if err := build(filepath.Join(t.TempDir(), "x"), flaky, ArmA2); !errors.Is(err, ErrStreamDiffers) {
		t.Errorf("a stream whose digest changes between passes: %v", err)
	}
	// A horizon the stream does not end with, told or not told.
	wrong := src
	wrong.Horizon = retained.Stream.Horizon.Add(time.Minute)
	if err := build(filepath.Join(t.TempDir(), "x"), wrong, ArmA2); !errors.Is(err, ErrStreamDiffers) {
		t.Errorf("a horizon that is not the stream's: %v", err)
	}
	none := PlanSource(plan, "t")
	none.Horizon = time.Unix(1, 0)
	if err := build(filepath.Join(t.TempDir(), "x"), none, ArmA2); !errors.Is(err, ErrStreamDiffers) {
		t.Errorf("a horizon told of a stream that never moves one: %v", err)
	}
	// A Seq a statement cannot be given as a token.
	e := engine.EdgeSubject(mustFP(t, catalog.K8sPod, catalog.K8sPodUID, "a"), mustFP(t, catalog.K8sNode, catalog.K8sNodeUID, "b"), catalog.ScheduledOn)
	big := Source{Label: "big", Replay: func(_ context.Context, sink runner.Sink) (string, error) {
		return "big", sink.Write([]engine.Record{{
			Layer: catalog.L2, Subject: e, Producer: "p", EventTime: time.Unix(10, 0).UTC(), Seq: math.MaxInt64 + 1, Kind: lifecycle.Observe,
		}})
	}}
	if err := build(filepath.Join(t.TempDir(), "x"), big, ArmA2); err == nil || !strings.Contains(err.Error(), "largest int64") {
		t.Errorf("a Seq above the largest int64: %v", err)
	}
}

func mustFP(t *testing.T, typ catalog.EntityType, key catalog.AttributeKey, v string) identity.Fingerprint {
	t.Helper()
	id, err := identity.NewResolver(catalog.Default()).Resolve(typ, []identity.Attr{{Key: key, Value: v}})
	if err != nil {
		t.Fatal(err)
	}
	return id.Fingerprint()
}

// TestPlanSourceRefusesAnotherStream: a stream of the plan's spec but another seed is not
// the plan's, and the build says so before any file is read.
func TestPlanSourceRefusesAnotherStream(t *testing.T) {
	plan := tinyPlan(t, false)
	other := *plan
	other.Spec.Workload.Seed++
	other.Stream = plan.Stream // the plan's claim of what the stream is
	src := PlanSource(&other, "t")
	if _, err := src.Replay(context.Background(), discard{}); err == nil {
		t.Fatal("a stream of another seed was taken for the plan's")
	}
}

type discard struct{}

func (discard) Write([]engine.Record) error { return nil }
func (discard) Retain(time.Time) error      { return nil }

// TestABuildWhoseFoldIsWrongFailsItsCheckAgainstTheModel: the interval files are checked
// row for row against the model's, so a fold that leaves touching intervals split, gives
// an open interval an end, or loses an end source is caught.
func TestABuildWhoseFoldIsWrongFailsItsCheckAgainstTheModel(t *testing.T) {
	plan := tinyPlan(t, true)
	for name, hook := range map[string]func([]duckmodel.Interval) []duckmodel.Interval{
		"an interval split in two where it touches itself": func(ivs []duckmodel.Interval) []duckmodel.Interval {
			var out []duckmodel.Interval
			for _, iv := range ivs {
				if !iv.Open && iv.End-iv.Start > 2 {
					mid := iv.Start + (iv.End-iv.Start)/2
					out = append(out, duckmodel.Interval{Start: iv.Start, End: mid, EndSource: iv.EndSource},
						duckmodel.Interval{Start: mid, End: iv.End, EndSource: iv.EndSource})
					continue
				}
				out = append(out, iv)
			}
			return out
		},
		"an open interval with an end": func(ivs []duckmodel.Interval) []duckmodel.Interval {
			out := slices.Clone(ivs)
			for i := range out {
				if out[i].Open {
					out[i].Open, out[i].End = false, math.MaxInt64
				}
			}
			return out
		},
		"an end source lost": func(ivs []duckmodel.Interval) []duckmodel.Interval {
			out := slices.Clone(ivs)
			for i := range out {
				out[i].EndSource = lifecycle.EndUnknown
			}
			return out
		},
		"an end one nanosecond late": func(ivs []duckmodel.Interval) []duckmodel.Interval {
			out := slices.Clone(ivs)
			for i := range out {
				if !out[i].Open {
					out[i].End++
				}
			}
			return out
		},
		"the last interval dropped": func(ivs []duckmodel.Interval) []duckmodel.Interval {
			if len(ivs) == 0 {
				return ivs
			}
			return ivs[:len(ivs)-1]
		},
	} {
		foldHook = hook
		_, err := buildTiny(t, plan, filepath.Join(t.TempDir(), "x"), []Arm{ArmI}, BuildOptions{VerifyModel: true})
		foldHook = nil
		if err == nil {
			t.Errorf("%s: the build passed", name)
		}
	}
	if _, err := buildTiny(t, plan, filepath.Join(t.TempDir(), "x"), []Arm{ArmI}, BuildOptions{VerifyModel: true}); err != nil {
		t.Fatalf("the honest fold: %v", err)
	}
}

func TestEqualRowsSeesEveryDifference(t *testing.T) {
	row := func(a string, start, end int64, open bool) duckmodel.IntervalRow {
		return duckmodel.IntervalRow{Layer: catalog.L2, A: a, Peer: "p", Relation: "r", Interval: duckmodel.Interval{Start: start, End: end, Open: open}}
	}
	base := []duckmodel.IntervalRow{row("a", 1, 5, false), row("a", 7, 0, true), row("b", 2, 3, false)}
	if err := EqualRows(base, slices.Clone(base)); err != nil {
		t.Fatal(err)
	}
	for name, mutate := range map[string]func([]duckmodel.IntervalRow) []duckmodel.IntervalRow{
		"a row less":     func(r []duckmodel.IntervalRow) []duckmodel.IntervalRow { return r[:2] },
		"a row more":     func(r []duckmodel.IntervalRow) []duckmodel.IntervalRow { return append(r, row("c", 1, 2, false)) },
		"rows swapped":   func(r []duckmodel.IntervalRow) []duckmodel.IntervalRow { r[0], r[1] = r[1], r[0]; return r },
		"an end changed": func(r []duckmodel.IntervalRow) []duckmodel.IntervalRow { r[0].End++; return r },
		"open to closed": func(r []duckmodel.IntervalRow) []duckmodel.IntervalRow { r[1].Open = false; return r },
		"an end source": func(r []duckmodel.IntervalRow) []duckmodel.IntervalRow {
			r[0].EndSource = lifecycle.EndProducer
			return r
		},
		"a peer":          func(r []duckmodel.IntervalRow) []duckmodel.IntervalRow { r[2].Peer = "q"; return r },
		"a layer":         func(r []duckmodel.IntervalRow) []duckmodel.IntervalRow { r[2].Layer = catalog.L3; return r },
		"a relation":      func(r []duckmodel.IntervalRow) []duckmodel.IntervalRow { r[2].Relation = "s"; return r },
		"touching merged": func(r []duckmodel.IntervalRow) []duckmodel.IntervalRow { r[0].End = 7; return r },
	} {
		if err := EqualRows(mutate(slices.Clone(base)), base); err == nil {
			t.Errorf("%s was not seen", name)
		}
	}
}

// TestIntervalFilesHoldTheRegisteredColumns: an interval file is layer, a, peer, relation,
// start and end, and an entity file the same without the peer and the relation. The end
// source of an interval is checked at build, in memory, and is no column of a file.
func TestIntervalFilesHoldTheRegisteredColumns(t *testing.T) {
	dir, _, _ := sharedBuild(t)
	db := openRead(t, false)
	for role, want := range map[Role][]string{
		RoleIFwd: {"layer", "a", "peer", "relation", "start_ns", "end_ns"},
		RoleIRev: {"layer", "a", "peer", "relation", "start_ns", "end_ns"},
		RoleIEnt: {"layer", "a", "start_ns", "end_ns"},
	} {
		rows, err := db.QueryContext(context.Background(), "SELECT DISTINCT path_in_schema, column_id FROM parquet_metadata("+quote(rolePathIn(dir, role))+") ORDER BY column_id")
		if err != nil {
			t.Fatal(err)
		}
		var got []string
		for rows.Next() {
			var n string
			var id int
			if err := rows.Scan(&n, &id); err != nil {
				t.Fatal(err)
			}
			got = append(got, n)
		}
		rows.Close()
		if !slices.Equal(got, want) {
			t.Errorf("%s has columns %v, want %v", role, got, want)
		}
	}
}
