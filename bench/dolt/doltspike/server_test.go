package doltspike

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/lotannauo/toposhift/bench/dolt/doltengine"
	"github.com/lotannauo/toposhift/bench/dolt/doltinput"
	"github.com/lotannauo/toposhift/bench/dolt/doltmodel"
	"github.com/lotannauo/toposhift/bench/dolt/scripts"
	"github.com/lotannauo/toposhift/bench/spike/engine"
	"github.com/lotannauo/toposhift/bench/spike/runner"
	"github.com/lotannauo/toposhift/bench/spike/workload"
	"github.com/lotannauo/toposhift/internal/catalog"
	"github.com/lotannauo/toposhift/internal/identity"
	"github.com/lotannauo/toposhift/internal/lifecycle"
	"github.com/lotannauo/toposhift/internal/replay/activity"
	"github.com/lotannauo/toposhift/internal/store"
)

// The tests in this file need a Dolt server: TOPOSHIFT_DOLT_DSN names it, as for the engine's
// server tests (scripts/dolt-server.sh starts the one the baseline is written for), and
// they skip without it. The tests of the commands that need the server's data directory
// (sizes) also need TOPOSHIFT_DOLT_DATA, the directory the server's container mounts.

func serverDSN(t testing.TB) string {
	t.Helper()
	dsn := os.Getenv("TOPOSHIFT_DOLT_DSN")
	if dsn == "" {
		t.Skip("TOPOSHIFT_DOLT_DSN is not set: no Dolt server to test against (scripts/dolt-server.sh starts one)")
	}
	return dsn
}

func dataDir(t testing.TB) string {
	t.Helper()
	d := os.Getenv("TOPOSHIFT_DOLT_DATA")
	if d == "" {
		t.Skip("TOPOSHIFT_DOLT_DATA is not set: the server's data directory is not known")
	}
	return d
}

// newDB names a database for the test and drops it when the test ends.
func newDB(t testing.TB) string {
	t.Helper()
	dsn := serverDSN(t)
	name := doltengine.DatabaseName(t.TempDir())
	t.Cleanup(func() { _ = doltengine.DropDatabase(context.Background(), dsn, name) })
	return name
}

// The synthetic plans of the tests: an hour of a handful of entities, with late records,
// one with a retention and one with none (a retention leaves no window).
var (
	plansOnce sync.Once
	plans     map[string]testPlan
	plansErr  error
)

type testPlan struct {
	path, sha string
	plan      *runner.Plan
}

func thePlans(t testing.TB) map[string]testPlan {
	t.Helper()
	plansOnce.Do(func() {
		dir, err := os.MkdirTemp("", "doltspike-plans-")
		if err != nil {
			plansErr = err
			return
		}
		plans = map[string]testPlan{}
		for name, retain := range map[string]bool{"plain": false, "retained": true} {
			// A little over an hour of a small cluster: long enough for the reads of an hour
			// back and of an old snapshot, and short enough to be loaded in seconds.
			w := workload.Tiny()
			w.Duration = time.Hour
			w.LateProbability, w.LateMeanDelay = 0.3, 4*time.Minute
			spec := runner.DefaultSpec(w)
			spec.BatchSize, spec.MinNonEmpty = 100, 0
			if retain {
				spec.Retentions = []runner.Retention{{At: 40 * time.Minute, Keep: 20 * time.Minute}}
			}
			p, err := runner.MakePlan(context.Background(), spec, nil)
			if err != nil {
				plansErr = err
				return
			}
			path := filepath.Join(dir, name+".json")
			if err := p.Save(path); err != nil {
				plansErr = err
				return
			}
			b, _ := os.ReadFile(path)
			sum := sha256.Sum256(b)
			plans[name] = testPlan{path: path, sha: hex.EncodeToString(sum[:]), plan: p}
		}
	})
	if plansErr != nil {
		t.Fatal(plansErr)
	}
	return plans
}

// cli runs a command and returns what it printed.
type cli struct {
	t   testing.TB
	dsn string
}

func newCLI(t testing.TB) cli { return cli{t: t, dsn: serverDSN(t)} }

func (c cli) run(args ...string) (string, error) {
	c.t.Helper()
	image, err := scripts.Image()
	if err != nil {
		c.t.Fatal(err)
	}
	var out, errOut bytes.Buffer
	full := append([]string{args[0], "-dsn", c.dsn, "-image", image}, args[1:]...)
	if args[0] == "diff" { // it reaches no server
		full = args
	}
	err = Run(context.Background(), full, &out, &errOut)
	if s := out.String() + errOut.String(); durationInText.MatchString(s) {
		c.t.Errorf("%s printed a time:\n%s", args[0], s)
	}
	return out.String(), err
}

func (c cli) must(args ...string) string {
	c.t.Helper()
	out, err := c.run(args...)
	if err != nil {
		c.t.Fatalf("%s: %v\n%s", args[0], err, out)
	}
	return out
}

func readJSON[R any](t testing.TB, path string) R {
	t.Helper()
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var r R
	if err := json.Unmarshal(b, &r); err != nil {
		t.Fatal(err)
	}
	return r
}

func TestEnvChecksTheServer(t *testing.T) {
	c := newCLI(t)
	out := c.must("env", "-out", t.TempDir())
	if !strings.Contains(out, "server: Dolt 2.4.2, the registered image was named") {
		t.Fatalf("%s", out)
	}
}

// TestLoadReadExplainAndSizesOfBothArms is the whole of a plan's pass through the commands,
// on both arms: a load held to the model, the answers to every question, the plan of every
// statement and the bytes, each record tied to the load that made the database.
func TestLoadReadExplainAndSizesOfBothArms(t *testing.T) {
	c := newCLI(t)
	data := dataDir(t)
	for name, p := range thePlans(t) {
		for _, arm := range []string{"commit", "rows"} {
			t.Run(name+"/"+arm, func(t *testing.T) {
				c := cli{t: t, dsn: c.dsn}
				db, out := newDB(t), t.TempDir()
				in := []string{"-plan", p.path, "-expect-plan", p.sha}

				load := c.must(append([]string{"load", "-db", db, "-arm", arm, "-out", out, "-model"}, in...)...)
				if !strings.Contains(load, "model: the same stream gave equal counts, commits and frontiers") {
					t.Fatalf("the load was not held to the model:\n%s", load)
				}
				lr := readJSON[LoadRecord](t, filepath.Join(out, "load-"+arm+".json"))
				if lr.Kind != KindLoad || lr.Input.PlanSHA256 != p.sha || lr.Input.StreamDigest != p.plan.Stream.Digest || !lr.ModelChecked {
					t.Fatalf("%+v", lr)
				}
				if lr.Counts.Records != p.plan.Stream.Records || lr.Counts.PayloadRefusals != 0 || len(lr.Capabilities) != 4 {
					t.Fatalf("%+v", lr.Counts)
				}
				if arm == "commit" {
					if lr.HeadHash == "" || lr.Commits != int(lr.Counts.Commits) || len(lr.Frontiers) != lr.Commits || lr.FrontiersSHA256 != frontiersDigest(lr.Frontiers) {
						t.Fatalf("%d commits, %d frontiers, head %q", lr.Commits, len(lr.Frontiers), lr.HeadHash)
					}
					if name == "retained" && lr.Retentions != 1 {
						t.Fatalf("%d retentions", lr.Retentions)
					}
				} else if lr.Commits != 0 || lr.HeadHash != "" || lr.Frontiers != nil {
					t.Fatalf("the arm with no commits made commits: %+v", lr)
				}
				loadPath := filepath.Join(out, "load-"+arm+".json")

				// Read: every answer is the plan's, the paths are the model's.
				read := c.must(append([]string{"read", "-db", db, "-arm", arm, "-load", loadPath, "-out", out, "-model"}, in...)...)
				want := fmt.Sprintf("G0 total: %d of %d answers equal the plan's", len(p.plan.Queries), len(p.plan.Queries))
				if !strings.Contains(read, want) || !strings.Contains(read, "model: the same stream gave the same answers, key counts and paths") {
					t.Fatalf("%s", read)
				}
				rr := readJSON[ReadRecord](t, filepath.Join(out, "read-"+arm+".json"))
				if rr.Matched != rr.Queries || rr.Queries != len(p.plan.Queries) || len(rr.Results) != rr.Queries || !rr.ModelChecked || rr.Diagnostic {
					t.Fatalf("%d of %d, %d results", rr.Matched, rr.Queries, len(rr.Results))
				}
				var asOf, needed, noCommit int
				var rowsState uint64
				for _, a := range rr.Ages {
					asOf, needed, noCommit, rowsState = asOf+a.AsOfAlone, needed+a.NeededLog, noCommit+a.NoCommit, rowsState+a.WorkRowsState
				}
				if arm == "commit" && (noCommit != 0 || asOf == 0 || needed == 0 || rowsState == 0) {
					t.Fatalf("as-of-alone %d, needed-log %d, no-commit %d, state rows %d", asOf, needed, noCommit, rowsState)
				}
				if arm == "rows" {
					var old GroupResult
					for _, a := range rr.Ages {
						if a.Age == runner.AgeOldToken {
							old = a
						}
					}
					if old.Queries == 0 || old.NoCommit != old.Queries {
						t.Fatalf("in the arm with no commits the old token has no commit to read: %+v", old)
					}
				}

				// Read again: the same record.
				c.must(append([]string{"read", "-db", db, "-arm", arm, "-load", loadPath, "-out", filepath.Join(out, "again"), "-model"}, in...)...)
				diff := c.must("diff", filepath.Join(out, "read-"+arm+".json"), filepath.Join(out, "again", "read-"+arm+".json"))
				if !strings.Contains(diff, "equal") {
					t.Fatalf("two reads of one database differ:\n%s", diff)
				}

				// Explain: every statement reads by a range, and the record is tied to the load.
				ex := c.must(append([]string{"explain", "-db", db, "-arm", arm, "-load", loadPath, "-out", out}, in...)...)
				if !strings.Contains(ex, "every statement reads its tables through the assumed indexes") {
					t.Fatalf("%s", ex)
				}
				er := readJSON[ExplainRecord](t, filepath.Join(out, "explain-"+arm+".json"))
				if len(er.Queries) != len(p.plan.Queries) || er.LoadSHA256 != rr.LoadSHA256 {
					t.Fatalf("%d questions explained", len(er.Queries))
				}
				for _, q := range er.Queries {
					for _, s := range q.Statements {
						if !s.Range || len(s.Accesses) == 0 {
							t.Fatalf("%+v", s)
						}
					}
				}

				// Sizes: as left and after garbage collection, and the database is as it was.
				sz := c.must("sizes", "-db", db, "-arm", arm, "-load", loadPath, "-data", data, "-gc", "-out", out)
				if !strings.Contains(sz, "after garbage collection") {
					t.Fatalf("%s", sz)
				}
				sr := readJSON[SizesRecord](t, filepath.Join(out, "sizes-"+arm+".json"))
				if sr.AfterGC == nil || sr.AsLeft.Total <= 0 || sr.AfterGC.Total <= 0 || sr.AfterGC.Tables <= 0 || sr.RecordsHeld <= 0 || sr.HeadHash != lr.HeadHash {
					t.Fatalf("%+v", sr)
				}
				if sr.EdgeLogRows%2 != 0 || sr.RecordsHeld != sr.EdgeLogRows/2+sr.EntityLogRows {
					t.Fatalf("%d records from %d and %d rows", sr.RecordsHeld, sr.EdgeLogRows, sr.EntityLogRows)
				}
				if name == "plain" && sr.RecordsHeld != int64(p.plan.Stream.Records) {
					t.Fatalf("a stream with no retention holds every record: %d of %d", sr.RecordsHeld, p.plan.Stream.Records)
				}
				if name == "retained" && sr.RecordsHeld >= int64(p.plan.Stream.Records) {
					t.Fatalf("a retention that deleted nothing: %d of %d", sr.RecordsHeld, p.plan.Stream.Records)
				}
				// The database reads as before after the collection.
				c.must(append([]string{"read", "-db", db, "-arm", arm, "-load", loadPath}, in...)...)

				// Nothing is overwritten, and a database is not loaded into twice.
				for _, args := range [][]string{
					append([]string{"load", "-db", db, "-arm", arm, "-out", out}, in...),
					append([]string{"read", "-db", db, "-arm", arm, "-load", loadPath, "-out", out}, in...),
					{"sizes", "-db", db, "-arm", arm, "-load", loadPath, "-data", data, "-out", out},
					append([]string{"explain", "-db", db, "-arm", arm, "-load", loadPath, "-out", out}, in...),
				} {
					if _, err := c.run(args...); err == nil || !strings.Contains(err.Error(), "already exists") {
						t.Errorf("%s: %v", args[0], err)
					}
				}
				if _, err := c.run(append([]string{"load", "-db", db, "-arm", arm, "-out", t.TempDir()}, in...)...); err == nil || !strings.Contains(err.Error(), "exists") {
					t.Errorf("a second load into the database: %v", err)
				}
			})
		}
	}
}

// TestTwoLoadsOfOneStreamEndAtOneHeadAndAreRepeatable is the determinism gate: the same stream
// loaded twice gives the same head commit, writer counts, frontiers and answers.
func TestTwoLoadsOfOneStreamEndAtOneHeadAndAreRepeatable(t *testing.T) {
	c := newCLI(t)
	p := thePlans(t)["retained"]
	in := []string{"-plan", p.path, "-expect-plan", p.sha}
	out := t.TempDir()
	var loads, reads, explains []string
	for i := range 2 {
		db := newDB(t)
		dir := filepath.Join(out, fmt.Sprint(i))
		c.must(append([]string{"load", "-db", db, "-arm", "commit", "-out", dir}, in...)...)
		c.must(append([]string{"read", "-db", db, "-arm", "commit", "-load", filepath.Join(dir, "load-commit.json"), "-out", dir}, in...)...)
		c.must(append([]string{"explain", "-db", db, "-arm", "commit", "-load", filepath.Join(dir, "load-commit.json"), "-out", dir, "-all-plans"}, in...)...)
		loads = append(loads, filepath.Join(dir, "load-commit.json"))
		reads = append(reads, filepath.Join(dir, "read-commit.json"))
		explains = append(explains, filepath.Join(dir, "explain-commit.json"))
	}
	a, b := readJSON[LoadRecord](t, loads[0]), readJSON[LoadRecord](t, loads[1])
	if a.HeadHash == "" || a.HeadHash != b.HeadHash || a.Database == b.Database {
		t.Fatalf("heads %s and %s of databases %s and %s", a.HeadHash, b.HeadHash, a.Database, b.Database)
	}
	if got := c.must("diff", loads[0], loads[1]); !strings.Contains(got, "equal") {
		t.Fatal(got)
	}
	// The reads differ in the load they name (a record of its own), and in nothing else.
	if got, err := c.run("diff", reads[0], reads[1]); err != nil || !strings.Contains(got, "equal") {
		t.Fatalf("%v: %s", err, got)
	}
	// The plans the server printed, statement by statement, are the same text.
	if got, err := c.run("diff", explains[0], explains[1]); err != nil || !strings.Contains(got, "equal") {
		t.Fatalf("%v: %s", err, got)
	}
}

// TestTheDiagnosticCountsWrongAnswersOnAPlanWithLateRecords: the commit-time reading of the
// commit arm's database is wrong somewhere (late records), is the model's answer for answer,
// and its wrong answers are its result and not a failure.
func TestTheDiagnosticCountsWrongAnswersOnAPlanWithLateRecords(t *testing.T) {
	c := newCLI(t)
	p := thePlans(t)["plain"]
	in := []string{"-plan", p.path, "-expect-plan", p.sha}
	db, out := newDB(t), t.TempDir()
	c.must(append([]string{"load", "-db", db, "-arm", "commit", "-out", out}, in...)...)
	loadPath := filepath.Join(out, "load-commit.json")
	text := c.must(append([]string{"read", "-db", db, "-arm", "commit-time", "-load", loadPath, "-out", out, "-model"}, in...)...)
	r := readJSON[ReadRecord](t, filepath.Join(out, "read-commit-time.json"))
	if !r.Diagnostic || r.Matched >= r.Queries || !r.ModelChecked || !strings.Contains(text, "DIAGNOSTIC total:") {
		t.Fatalf("%d of %d matched\n%s", r.Matched, r.Queries, text)
	}
	if strings.Contains(text, "G0 total") || strings.Contains(text, "MISMATCH") {
		t.Fatalf("the diagnostic's wrong answers were reported as the design's:\n%s", text)
	}
	// Wrong answers are of the neighbors, batched and existence reads at look-back instants:
	// the reads at the newest instant, and the windows (the log), are right.
	var wrongNow, wrongWindow int
	for _, g := range r.Groups {
		switch g.Age {
		case runner.AgeNow, runner.AgeDead:
			wrongNow += g.Wrong
		case runner.AgeWindow1h, runner.AgeWindow1d:
			wrongWindow += g.Wrong
		}
	}
	if wrongNow != 0 || wrongWindow != 0 {
		t.Fatalf("wrong answers at now %d, in windows %d", wrongNow, wrongWindow)
	}
	// The arm with no commits has nothing for it to read.
	rows := newDB(t)
	c.must(append([]string{"load", "-db", rows, "-arm", "rows", "-out", out}, in...)...)
	if _, err := c.run(append([]string{"read", "-db", rows, "-arm", "commit-time", "-load", filepath.Join(out, "load-rows.json")}, in...)...); err == nil {
		t.Fatal("the diagnostic read the arm with no commits")
	}
}

// TestADatabaseIsOnlyReadAsTheLoadThatMadeIt: a read refuses a load record of another stream,
// of another arm or database, and a database that has moved on or was loaded again.
func TestADatabaseIsOnlyReadAsTheLoadThatMadeIt(t *testing.T) {
	c := newCLI(t)
	plain, retained := thePlans(t)["plain"], thePlans(t)["retained"]
	inP := []string{"-plan", plain.path, "-expect-plan", plain.sha}
	inR := []string{"-plan", retained.path, "-expect-plan", retained.sha}
	dsn := c.dsn
	db, out := newDB(t), t.TempDir()
	c.must(append([]string{"load", "-db", db, "-arm", "commit", "-out", out}, inP...)...)
	loadPath := filepath.Join(out, "load-commit.json")
	c.must(append([]string{"read", "-db", db, "-arm", "commit", "-load", loadPath}, inP...)...)

	for name, args := range map[string][]string{
		"another plan":     append([]string{"read", "-db", db, "-arm", "commit", "-load", loadPath}, inR...),
		"another arm":      append([]string{"read", "-db", db, "-arm", "rows", "-load", loadPath}, inP...),
		"another database": append([]string{"read", "-db", db + "x", "-arm", "commit", "-load", loadPath}, inP...),
		"not a load":       append([]string{"read", "-db", db, "-arm", "commit", "-load", plain.path}, inP...),
	} {
		if _, err := c.run(args...); err == nil {
			t.Errorf("%s: a read of a database that is not the load's", name)
		}
	}

	// The database moves on after the load.
	e, err := doltengine.Open(context.Background(), doltengine.Config{DSN: dsn, Database: db, Arm: doltmodel.ArmCommit})
	if err != nil {
		t.Fatal(err)
	}
	// A retention is a commit and moves the head without moving the sequence number.
	if err := e.Retain(time.Unix(1, 0)); err != nil {
		t.Fatal(err)
	}
	_ = e.Close()
	if _, err := c.run(append([]string{"read", "-db", db, "-arm", "commit", "-load", loadPath}, inP...)...); err == nil || !strings.Contains(err.Error(), "ends at commit") {
		t.Fatalf("a database whose head is not the load's: %v", err)
	}
}

// TestALoadThatFailsLeavesNoDatabase: the database a failed load made is dropped.
func TestALoadThatFailsLeavesNoDatabase(t *testing.T) {
	dsn := serverDSN(t)
	p := thePlans(t)["plain"]
	s := &stream{plan: p.plan, planSHA: p.sha}
	db := newDB(t)
	ctx, cancel := context.WithTimeout(context.Background(), time.Nanosecond)
	defer cancel()
	_, err := load(ctx, loadOpts{DSN: dsn, Database: db, Arm: doltmodel.ArmCommit, Stream: s}, Env{})
	if err == nil {
		t.Fatal("a load that was out of time")
	}
	if ok, err := doltengine.DatabaseExists(context.Background(), dsn, db); err != nil || ok {
		t.Fatalf("the failed load left database %s (%v)", db, err)
	}
	// And a stream that is not the plan's fails the same way: the plan's digest is of the stream
	// it regenerates.
	other := *p.plan
	other.Stream.Digest = strings.Repeat("0", 64)
	_, err = load(context.Background(), loadOpts{DSN: dsn, Database: db, Arm: doltmodel.ArmCommit, Stream: &stream{plan: &other}}, Env{})
	if err == nil || !strings.Contains(err.Error(), "not the plan's") {
		t.Fatalf("a stream that is not the plan's: %v", err)
	}
	if ok, _ := doltengine.DatabaseExists(context.Background(), dsn, db); ok {
		t.Fatalf("the failed load left database %s", db)
	}
	// An input whose payloads are over the column is refused before any database is made.
	big := *p.plan
	big.Spec.Workload.PayloadMax = doltmodel.RegisteredPayloadCapacity + 1
	_, err = load(context.Background(), loadOpts{DSN: dsn, Database: db, Arm: doltmodel.ArmCommit, Stream: &stream{plan: &big}}, Env{})
	if !errors.Is(err, ErrCapacity) {
		t.Fatalf("payloads over the capacity: %v", err)
	}
	if ok, _ := doltengine.DatabaseExists(context.Background(), dsn, db); ok {
		t.Fatalf("a refused load left database %s", db)
	}
}

// TestReplaceDropsTheDatabaseFirst: -replace is how a database is loaded again.
func TestReplaceDropsTheDatabaseFirst(t *testing.T) {
	c := newCLI(t)
	p := thePlans(t)["plain"]
	in := []string{"-plan", p.path, "-expect-plan", p.sha}
	db := newDB(t)
	first := c.must(append([]string{"load", "-db", db, "-arm", "rows", "-out", t.TempDir()}, in...)...)
	second := c.must(append([]string{"load", "-db", db, "-arm", "rows", "-replace", "-out", t.TempDir()}, in...)...)
	if first != second {
		t.Fatalf("the load again printed other counts:\n%s\n%s", first, second)
	}
}

// TestSizingSaysPassOrFailAndNothingElse: the verdict has no time and no byte count in it, a
// load out of time fails by the time, a database over its share of the disk fails by the
// disk, and the database is dropped either way.
func TestSizingSaysPassOrFailAndNothingElse(t *testing.T) {
	dsn := serverDSN(t)
	data := dataDir(t)
	p := thePlans(t)["plain"]
	db := newDB(t)
	env := Env{}
	o := loadOpts{DSN: dsn, Database: db, Arm: doltmodel.ArmCommit, Stream: &stream{plan: p.plan, planSHA: p.sha}}

	v, err := sizing(context.Background(), o, env, data, SizingLimits{Budget: time.Hour, DiskShare: 0.9})
	if err != nil || !v.Fits || v.OverTime || v.OverDisk {
		t.Fatalf("%+v, %v", v, err)
	}
	if ok, _ := doltengine.DatabaseExists(context.Background(), dsn, db); ok {
		t.Fatal("sizing left its database")
	}
	v, err = sizing(context.Background(), o, env, data, SizingLimits{Budget: time.Nanosecond, DiskShare: 0.9})
	if err != nil || v.Fits || !v.OverTime || v.OverDisk {
		t.Fatalf("out of time: %+v, %v", v, err)
	}
	v, err = sizing(context.Background(), o, env, data, SizingLimits{Budget: time.Hour, DiskShare: 1e-12})
	if err != nil || v.Fits || v.OverTime || !v.OverDisk {
		t.Fatalf("over the disk: %+v, %v", v, err)
	}
	if ok, _ := doltengine.DatabaseExists(context.Background(), dsn, db); ok {
		t.Fatal("sizing left its database")
	}
	// A cancelled run is an error, not a verdict of the time.
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := sizing(ctx, o, env, data, SizingLimits{Budget: time.Hour, DiskShare: 0.9}); err == nil {
		t.Fatal("a cancelled sizing gave a verdict")
	}

	// Through the command, on a runner.
	t.Setenv("GITHUB_ACTIONS", "true")
	image, _ := scripts.Image()
	var out, errOut bytes.Buffer
	args := []string{"sizing", "-dsn", dsn, "-image", image, "-db", db, "-data", data, "-plan", p.path, "-expect-plan", p.sha}
	if err := Run(context.Background(), args, &out, &errOut); err != nil {
		t.Fatalf("%v\n%s", err, out.String())
	}
	lines := strings.Split(strings.TrimSpace(out.String()), "\n")
	if len(lines) != 2 || lines[1] != "sizing: pass" {
		t.Fatalf("%q", lines)
	}
	if durationInText.MatchString(out.String() + errOut.String()) {
		t.Fatalf("sizing printed a time:\n%s%s", out.String(), errOut.String())
	}
	out.Reset()
	err = Run(context.Background(), append(args, "-budget", "1ns"), &out, &errOut)
	if err == nil || !strings.Contains(out.String(), "sizing: fail: the load did not finish within its time budget") {
		t.Fatalf("%v\n%s", err, out.String())
	}
	out.Reset()
	err = Run(context.Background(), append(args, "-disk-share", "0.000000000001"), &out, &errOut)
	if err == nil || !strings.Contains(out.String(), "sizing: fail: the data directory is over its share of the free disk") {
		t.Fatalf("%v\n%s", err, out.String())
	}
	for _, bad := range [][]string{append(args, "-budget", "0s"), append(args, "-disk-share", "1"), append(args, "-disk-share", "0")} {
		if err := Run(context.Background(), bad, &out, &errOut); err == nil {
			t.Errorf("%v was accepted", bad[len(bad)-2:])
		}
	}
}

// hour is the records of the stream of an activity file: a cluster of a few nodes and pods
// over an hour and a half, with leases that lapse, placements replaced and deleted.
func hour() []store.Record {
	base := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	at := func(s int) time.Time { return base.Add(time.Duration(s) * time.Second) }
	fpN := func(typ catalog.EntityType, n int) identity.Fingerprint {
		var h [identity.FingerprintBytes]byte
		h[0], h[1] = byte(n>>8), byte(n)
		f, err := identity.FingerprintFromHash(typ, h)
		if err != nil {
			panic(err)
		}
		return f
	}
	var out []store.Record
	seq := uint64(0)
	add := func(r store.Record) {
		seq++
		r.Seq = seq
		r.Layer, r.Producer = catalog.L2, "k8s"
		out = append(out, r)
	}
	for n := range 10 {
		add(store.Record{Subject: store.EntitySubject(fpN(catalog.K8sNode, n)), EventTime: at(0), Kind: lifecycle.Observe, Payload: []byte("node")})
	}
	for p := range 150 {
		pod, node := fpN(catalog.K8sPod, p), fpN(catalog.K8sNode, p%10)
		start := p * 10
		add(store.Record{Subject: store.EntitySubject(pod), EventTime: at(start), Kind: lifecycle.Observe, TTL: 10 * time.Minute, Payload: []byte("pod")})
		edge := store.EdgeSubject(pod, node, catalog.ScheduledOn)
		for r := range 6 {
			add(store.Record{Subject: edge, EventTime: at(start + 1 + 600*r), Kind: lifecycle.Observe, TTL: 5 * time.Minute, Payload: []byte("e")})
		}
		if p%7 == 0 {
			add(store.Record{Subject: edge, EventTime: at(start + 3000), Kind: lifecycle.Delete})
		}
		if p%5 == 0 {
			add(store.Record{Subject: store.EdgeSubject(pod, fpN(catalog.K8sNode, (p+1)%10), catalog.ScheduledOn), EventTime: at(start + 3100), Kind: lifecycle.Observe, TTL: 5 * time.Minute, Payload: []byte("e2")})
		}
	}
	add(store.Record{Subject: store.EntitySubject(fpN(catalog.K8sNode, 0)), EventTime: at(5400), Kind: lifecycle.Observe, Payload: []byte("node")})
	return out
}

func writeActivity(t *testing.T, rs []store.Record) (path, digest string) {
	t.Helper()
	var buf bytes.Buffer
	w, err := activity.NewWriter(&buf, activity.WriterOptions{RowGroupRows: 500})
	if err != nil {
		t.Fatal(err)
	}
	for _, r := range rs {
		if err := w.Write(r); err != nil {
			t.Fatal(err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	path = filepath.Join(t.TempDir(), "x.parquet")
	if err := os.WriteFile(path, buf.Bytes(), 0o600); err != nil {
		t.Fatal(err)
	}
	_, info, err := activityInfo(path)
	if err != nil {
		t.Fatal(err)
	}
	return path, info
}

func activityInfo(path string) (int64, string, error) {
	info, err := doltinput.ForEach(path, 1000, doltinput.Options{}, func([]engine.Record) error { return nil })
	return info.Records, info.Digest, err
}

// TestAnActivityFileThroughQuestionsLoadReadAndSizes: the same commands on a stream that is a
// file, with the questions made before any load, on both arms and the diagnostic.
func TestAnActivityFileThroughQuestionsLoadReadAndSizes(t *testing.T) {
	c := newCLI(t)
	data := dataDir(t)
	rs := hour()
	file, digest := writeActivity(t, rs)
	qdir := t.TempDir()
	var out, errOut bytes.Buffer
	if err := Run(context.Background(), []string{"questions", "-activity", file, "-expect-activity", digest, "-out", qdir, "-label", "a test hour"}, &out, &errOut); err != nil {
		t.Fatalf("%v\n%s", err, errOut.String())
	}
	if !strings.Contains(out.String(), "longest payload: 4 bytes") || !strings.Contains(out.String(), "dropped: 0 records with an event time basis") {
		t.Fatalf("%s", out.String())
	}
	qfile := filepath.Join(qdir, "questions.json")
	qb, _ := os.ReadFile(qfile)
	qsum := sha256.Sum256(qb)
	qsha := hex.EncodeToString(qsum[:])
	set, err := doltinput.LoadQuestions(qfile)
	if err != nil {
		t.Fatal(err)
	}
	// A second set into the same place is refused, and a file that is not the one named is
	// refused.
	if err := Run(context.Background(), []string{"questions", "-activity", file, "-expect-activity", digest, "-out", qdir}, &out, &errOut); err == nil {
		t.Error("the question set was overwritten")
	}
	if err := Run(context.Background(), []string{"questions", "-activity", file, "-expect-activity", strings.Repeat("0", 16), "-out", t.TempDir()}, &out, &errOut); !errors.Is(err, ErrDigestDiffers) {
		t.Errorf("another content digest: %v", err)
	}
	in := []string{"-questions", qfile, "-expect-questions", qsha, "-activity", file, "-expect-activity", digest}
	inRead := []string{"-questions", qfile, "-expect-questions", qsha}

	dir := t.TempDir()
	for _, arm := range []string{"commit", "rows"} {
		db := newDB(t)
		c.must(append([]string{"load", "-db", db, "-arm", arm, "-out", dir, "-model"}, in...)...)
		lr := readJSON[LoadRecord](t, filepath.Join(dir, "load-"+arm+".json"))
		if lr.Input.Kind != "activity" || lr.Input.ContentDigest != set.ContentDigest || lr.Counts.Records != uint64(len(rs)) || lr.Input.MaxPayload != 4 || !lr.ModelChecked {
			t.Fatalf("%+v %+v", lr.Input, lr.Counts)
		}
		loadPath := filepath.Join(dir, "load-"+arm+".json")
		text := c.must(append([]string{"read", "-db", db, "-arm", arm, "-load", loadPath, "-out", dir, "-model"}, in...)...)
		if !strings.Contains(text, fmt.Sprintf("G0 total: %d of %d answers equal", len(set.Queries), len(set.Queries))) {
			t.Fatalf("%s", text)
		}
		c.must(append([]string{"explain", "-db", db, "-arm", arm, "-load", loadPath, "-out", dir}, inRead...)...)
		c.must("sizes", "-db", db, "-arm", arm, "-load", loadPath, "-data", data, "-gc", "-out", dir)
		if arm == "commit" {
			c.must(append([]string{"read", "-db", db, "-arm", "commit-time", "-load", loadPath, "-out", dir, "-model"}, in...)...)
		}
	}
	// The file must be the one the questions were made for.
	if _, err := c.run(append([]string{"load", "-db", newDB(t), "-arm", "rows", "-out", t.TempDir(), "-questions", qfile, "-expect-questions", qsha, "-activity", file, "-expect-activity", "ffffffffffff"}, []string{}...)...); !errors.Is(err, ErrDigestDiffers) {
		t.Errorf("another file: %v", err)
	}
	if _, err := c.run("load", "-db", newDB(t), "-arm", "rows", "-out", t.TempDir(), "-questions", qfile, "-expect-questions", qsha); err == nil || !strings.Contains(err.Error(), "-activity") {
		t.Errorf("questions with no file: %v", err)
	}
	if _, err := c.run("load", "-db", newDB(t), "-arm", "rows", "-out", t.TempDir(), "-questions", qfile, "-expect-questions", qsha, "-activity", file, "-expect-activity", digest, "-drop-basis"); err == nil || !strings.Contains(err.Error(), "made with") {
		t.Errorf("options other than the set's: %v", err)
	}
	// Records that fit no column are a capability row: the load is refused, and a database is
	// not made.
	big := hour()
	big[3].Payload = make([]byte, doltmodel.RegisteredPayloadCapacity+1)
	bigFile, bigDigest := writeActivity(t, big)
	bigDir := t.TempDir()
	if err := Run(context.Background(), []string{"questions", "-activity", bigFile, "-expect-activity", bigDigest, "-out", bigDir}, &out, &errOut); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(out.String(), "over the registered capacity") {
		t.Fatalf("the question set did not say the payloads are too long:\n%s", out.String())
	}
	bq := filepath.Join(bigDir, "questions.json")
	bb, _ := os.ReadFile(bq)
	bsum := sha256.Sum256(bb)
	bdb := newDB(t)
	_, err = c.run("load", "-db", bdb, "-arm", "commit", "-out", t.TempDir(), "-questions", bq, "-expect-questions", hex.EncodeToString(bsum[:]), "-activity", bigFile, "-expect-activity", bigDigest)
	if !errors.Is(err, ErrCapacity) {
		t.Fatalf("payloads over the capacity: %v", err)
	}
	if ok, _ := doltengine.DatabaseExists(context.Background(), c.dsn, bdb); ok {
		t.Fatal("a refused load made a database")
	}
}
