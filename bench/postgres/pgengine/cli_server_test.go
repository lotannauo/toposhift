package pgengine_test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/lotannauo/toposhift/bench/postgres/pgengine"
	"github.com/lotannauo/toposhift/bench/postgres/pginput"
	"github.com/lotannauo/toposhift/bench/postgres/pgreport"
	"github.com/lotannauo/toposhift/bench/postgres/pgschema"
	"github.com/lotannauo/toposhift/bench/postgres/pgspike"
	"github.com/lotannauo/toposhift/bench/spike/runner"
	"github.com/lotannauo/toposhift/internal/catalog"
	"github.com/lotannauo/toposhift/internal/identity"
	"github.com/lotannauo/toposhift/internal/lifecycle"
	"github.com/lotannauo/toposhift/internal/replay/activity"
	"github.com/lotannauo/toposhift/internal/store"
)

// e2eSpec is the tiny plan with fewer questions, for the commands, which explain each
// question cold.
func e2eSpec() runner.Spec {
	s := tinySpec()
	s.Hot, s.Median = 2, 1
	return s
}

func cli(t *testing.T, args ...string) (string, error) {
	t.Helper()
	var out, errs bytes.Buffer
	err := pgspike.Run(context.Background(), args, &out, &errs)
	return out.String(), err
}

func writePlan(t *testing.T, p *runner.Plan) (path, sum string) {
	t.Helper()
	path = filepath.Join(t.TempDir(), "plan.json")
	if err := p.Save(path); err != nil {
		t.Fatal(err)
	}
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	h := sha256.Sum256(b)
	return path, hex.EncodeToString(h[:])
}

// TestTheCommandsLoadReadAndHoldTwoLoadsToEachOther runs pgspike on a live server: it
// checks the server against settings.conf, loads a plan's stream into three arms, asks
// every question of two loads of the first and explains each cold, and compares the two.
func TestTheCommandsLoadReadAndHoldTwoLoadsToEachOther(t *testing.T) {
	dsn := serverDSN(t)
	p := cachedPlan(t, "e2e", e2eSpec())
	plan, sum := writePlan(t, p)
	ctx := context.Background()

	schemas := map[string]string{}
	schema := func(name string) string {
		s := pgschema.SchemaName(t.TempDir() + name)
		schemas[name] = s
		t.Cleanup(func() { _ = pgengine.DropSchema(ctx, dsn, s) })
		return s
	}
	out := func(name string) string { return filepath.Join(t.TempDir(), name) }
	common := func(arm, schema, dir string) []string {
		return []string{"-dsn", dsn, "-settings", settingsPath, "-arm", arm, "-schema", schema, "-out", dir, "-plan", plan, "-expect-plan", sum}
	}

	// env
	envDir := out("env")
	if txt, err := cli(t, "env", "-dsn", dsn, "-settings", settingsPath, "-out", envDir); err != nil || !strings.Contains(txt, "every setting of settings.conf") {
		t.Fatalf("env: %q, %v", txt, err)
	}
	var env pgreport.Env
	data, err := os.ReadFile(filepath.Join(envDir, "env.json"))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(data), "postgres:17.11-trixie@sha256:") || !strings.Contains(string(data), `"BlockSize": 8192`) {
		t.Errorf("env.json: %s", data)
	}
	_ = env

	// Three loads of the fingerprint arm and one of each of the others.
	loads := map[string]string{}
	for _, c := range []struct{ name, arm string }{{"a", "pg-bulk"}, {"a2", "pg-bulk"}, {"b", "pg-bulk-b"}, {"b2", "pg-bulk-b"}, {"id", "pg-bulk-id"}} {
		dir := out("load-" + c.name)
		txt, err := cli(t, append([]string{"load"}, common(c.arm, schema(c.name), dir)...)...)
		if err != nil {
			t.Fatalf("load %s: %v\n%s", c.name, err, txt)
		}
		loads[c.name] = dir
	}
	read := func(name string) pgreport.Load {
		var l pgreport.Load
		if err := pgreport.ReadFile(filepath.Join(loads[name], "load.json"), &l); err != nil {
			t.Fatal(err)
		}
		return l
	}
	a, id := read("a"), read("id")
	if a.TablesDigest == "" || a.TablesDigest != id.TablesDigest || a.TablesDigest != read("b").TablesDigest {
		t.Errorf("the interval tables of the three arms have digests %s, %s, %s", a.TablesDigest, read("b").TablesDigest, id.TablesDigest)
	}
	if a.Input.Records != p.Stream.Records || a.Input.PlanSHA256 != sum || a.Env.SettingsSHA256 == "" || a.Sizes.Total <= 0 || a.Sizes.ByGroup[pgreport.GroupDictionary] != 0 || id.Sizes.ByGroup[pgreport.GroupDictionary] <= 0 {
		t.Errorf("load a: %+v", a.Input)
	}
	// Two loads of one stream with index set B are equal in rows, digest and bytes. The log
	// of a phase of a stream this small moves by a third from one load to the next (catalog
	// pages in the state the earlier loads left them, which page a vacuum or a COPY got),
	// so the tolerance here is the whole of it: the log is not what this asks.
	if txt, err := cli(t, "diff", "-kind", "load", "-wal-tolerance", "1", filepath.Join(loads["b"], "load.json"), filepath.Join(loads["b2"], "load.json")); err != nil || !strings.Contains(txt, "are equal") {
		t.Errorf("two loads with index set B: %v\n%s", err, txt)
	}
	// Two loads with index set G hold the same rows and the same tables, but a GiST index
	// is built with random choices among equally good pages, so its size, and the log of
	// building it, are not the same from one build to the next: those are the only
	// differences.
	var a2 pgreport.Load
	if err := pgreport.ReadFile(filepath.Join(loads["a2"], "load.json"), &a2); err != nil {
		t.Fatal(err)
	}
	cmp, err := pgreport.DiffLoads(a, a2, pgreport.Options{})
	if err != nil {
		t.Fatal(err)
	}
	for _, d := range cmp.Differences {
		gist := strings.Contains(d.Where, "_g ") || strings.Contains(d.Where, "_g main") || strings.HasPrefix(d.Where, "phase indexes") || strings.HasPrefix(d.Where, "phase vacuum") || strings.HasPrefix(d.Where, "phase stream") || strings.HasPrefix(d.Where, "phase copy") || strings.HasPrefix(d.Where, "phase retain")
		if !gist {
			t.Errorf("two loads with index set G differ in %s", d)
		}
	}
	if _, err := cli(t, "diff", "-kind", "load", filepath.Join(loads["a"], "load.json"), filepath.Join(loads["b"], "load.json")); !errors.Is(err, pgreport.ErrNotComparable) {
		t.Errorf("loads of two arms: %v", err)
	}

	// read, twice cold on the first load, once on the second
	r1, r2 := out("read-b"), out("read-b2")
	if txt, err := cli(t, append([]string{"read", "-repeats", "2"}, common("pg-bulk-b", schemas["b"], r1)...)...); err != nil || !strings.Contains(txt, "equal the reference's") {
		t.Fatalf("read b: %v\n%s", err, txt)
	}
	if txt, err := cli(t, append([]string{"read", "-repeats", "1"}, common("pg-bulk-b", schemas["b2"], r2)...)...); err != nil {
		t.Fatalf("read b2: %v\n%s", err, txt)
	}
	// Index set G is read as well, with the same answers.
	if txt, err := cli(t, append([]string{"read", "-repeats", "1"}, common("pg-bulk", schemas["a"], out("read-a"))...)...); err != nil {
		t.Fatalf("read a: %v\n%s", err, txt)
	}
	var ra pgreport.Read
	if err := pgreport.ReadFile(filepath.Join(r1, "read.json"), &ra); err != nil {
		t.Fatal(err)
	}
	if ra.Matched != ra.Queries || len(ra.Results) != len(p.Queries) || ra.Repeats != 2 || len(ra.Mismatches) != 0 || len(ra.Statements) == 0 {
		t.Fatalf("read a: %d of %d matched, %d results, %d statements", ra.Matched, ra.Queries, len(ra.Results), len(ra.Statements))
	}
	naive, plain := 0, 0
	for i, res := range ra.Results {
		if res.Digest != p.Queries[i].Expect || res.Size != p.Queries[i].Size || res.Total.SharedRead <= 0 || res.BytesRead != res.Total.SharedRead*8192 ||
			res.BytesTouched != (res.Total.SharedRead+res.Total.SharedHit)*8192 || len(res.Runs) == 0 {
			t.Errorf("%s: %+v", res.Name, res)
		}
		if res.Naive {
			naive++
			if res.RowsFetched <= 0 || len(res.Runs) < 2 {
				t.Errorf("%s: a naive fold fetched %d rows in %d statements", res.Name, res.RowsFetched, len(res.Runs))
			}
		} else {
			plain++
			if res.RowsFetched != 0 {
				t.Errorf("%s: rows fetched %d for a read that is not a fold", res.Name, res.RowsFetched)
			}
		}
	}
	if naive == 0 || plain == 0 {
		t.Errorf("%d naive reads and %d others", naive, plain)
	}
	if txt, err := cli(t, "diff", "-kind", "read", filepath.Join(r1, "read.json"), filepath.Join(r2, "read.json")); err != nil || !strings.Contains(txt, "are equal") {
		t.Errorf("the reads of two loads: %v\n%s", err, txt)
	}

	// sizes, compacted
	s1, s2 := out("sizes-a"), out("sizes-b")
	for _, c := range []struct{ name, dir string }{{"b", s1}, {"b2", s2}} {
		args := []string{"sizes", "-dsn", dsn, "-settings", settingsPath, "-arm", "pg-bulk-b", "-schema", schemas[c.name], "-records", "1", "-out", c.dir, "-compact"}
		if txt, err := cli(t, args...); err != nil || !strings.Contains(txt, "compacted") {
			t.Fatalf("sizes %s: %v\n%s", c.name, err, txt)
		}
	}
	var sr pgreport.SizesReport
	if err := pgreport.ReadFile(filepath.Join(s1, "sizes.json"), &sr); err != nil || sr.Compacted == nil || sr.Compacted.Total >= sr.AsLeft.Total {
		t.Errorf("sizes: %+v, %v", sr, err)
	}
	if txt, err := cli(t, "diff", "-kind", "sizes", filepath.Join(s1, "sizes.json"), filepath.Join(s2, "sizes.json")); err != nil || !strings.Contains(txt, "are equal") {
		t.Errorf("the sizes of two loads: %v\n%s", err, txt)
	}

	// explain prints the cold plan of a question
	if txt, err := cli(t, "explain", "-dsn", dsn, "-settings", settingsPath, "-arm", "pg-bulk", "-schema", schemas["a"], "-plan", plan, "-expect-plan", sum, "-query", "0"); err != nil ||
		!strings.Contains(txt, `"Node Type"`) || !strings.Contains(txt, "-- plan: ") {
		t.Errorf("explain: %v\n%s", err, txt)
	}
}

// TestTheCommandsRefuseWhatWouldGiveNumbersThatAreNotTheRegisteredOnes: a server with
// other settings, a record that would overwrite another, a schema that is there, a load
// that is not the plan's stream, and answers that are not the reference's.
func TestTheCommandsRefuseWhatWouldGiveNumbersThatAreNotTheRegisteredOnes(t *testing.T) {
	dsn := serverDSN(t)
	p := cachedPlan(t, "e2e", e2eSpec())
	plan, sum := writePlan(t, p)
	ctx := context.Background()
	schema := pgschema.SchemaName(t.TempDir())
	t.Cleanup(func() { _ = pgengine.DropSchema(ctx, dsn, schema) })
	dir := filepath.Join(t.TempDir(), "o")
	args := func(extra ...string) []string {
		return append([]string{"-dsn", dsn, "-settings", settingsPath, "-schema", schema, "-out", dir, "-plan", plan, "-expect-plan", sum}, extra...)
	}

	// A server whose settings are not the file's: no record, and no schema either.
	other := filepath.Join(t.TempDir(), "settings.conf")
	text, _ := os.ReadFile(settingsPath)
	if err := os.WriteFile(other, []byte(strings.Replace(string(text), "work_mem = 64MB", "work_mem = 32MB", 1)), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(filepath.Dir(other), "pg-server.sh"), mustRead(t, "../scripts/pg-server.sh"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := cli(t, "load", "-dsn", dsn, "-settings", other, "-arm", "pg-bulk", "-schema", schema, "-out", dir, "-plan", plan, "-expect-plan", sum); !errors.Is(err, pgengine.ErrSettingsDiffer) {
		t.Errorf("a server with other settings: %v", err)
	}
	if _, err := os.Stat(dir); err == nil {
		t.Error("a refused run left an output directory")
	}
	var exists bool
	conn := plainConn(t)
	if err := conn.QueryRow(ctx, "SELECT to_regnamespace($1) IS NOT NULL", schema).Scan(&exists); err != nil || exists {
		t.Errorf("a refused run made the schema: %v, %v", exists, err)
	}

	// A load that would need more memory for its interval rows than it may use does not
	// run: no record, and no schema left behind.
	if _, err := cli(t, append([]string{"load", "-arm", "pg-bulk", "-max-interval-mem", "1"}, args()...)...); !errors.Is(err, pgengine.ErrIntervalMemory) || !strings.Contains(err.Error(), "did not run") {
		t.Errorf("a load over its memory limit: %v", err)
	}
	if _, err := os.Stat(filepath.Join(dir, "load.json")); err == nil {
		t.Error("a load that did not run left a record")
	}
	if err := conn.QueryRow(ctx, "SELECT to_regnamespace($1) IS NOT NULL", schema).Scan(&exists); err != nil || exists {
		t.Errorf("a load that did not run left its schema: %v, %v", exists, err)
	}

	if out, err := cli(t, append([]string{"load", "-arm", "pg-bulk"}, args()...)...); err != nil {
		t.Fatalf("load: %v\n%s", err, out)
	}
	// Neither a record nor a schema is overwritten.
	if _, err := cli(t, append([]string{"load", "-arm", "pg-bulk", "-replace"}, args()...)...); err == nil || !strings.Contains(err.Error(), "never overwritten") {
		t.Errorf("a second load into the same output: %v", err)
	}
	if _, err := cli(t, append([]string{"load", "-arm", "pg-bulk"}, withOut(args(), filepath.Join(t.TempDir(), "o2"))...)...); !errors.Is(err, pgengine.ErrSchemaExists) {
		t.Errorf("a load into a schema that is there: %v", err)
	}
	// A plan that is not the one loaded: the schema ends at another seq.
	other2 := runner.DefaultSpec(e2eSpec().Workload)
	other2.MinNonEmpty, other2.BatchSize, other2.Hot, other2.Median = 0, 100, 2, 1
	other2.Workload.Seed = 7
	pp := cachedPlan(t, "e2e-seed7", other2)
	plan2, sum2 := writePlan(t, pp)
	if _, err := cli(t, "read", "-dsn", dsn, "-settings", settingsPath, "-arm", "pg-bulk", "-schema", schema, "-out", filepath.Join(t.TempDir(), "r"), "-plan", plan2, "-expect-plan", sum2, "-repeats", "1"); err == nil || !strings.Contains(err.Error(), "ends at seq") {
		t.Errorf("reads of a plan that is not the stream loaded: %v", err)
	}

	// An answer that differs from the reference's: the record has the mismatch and no
	// counter, and the command fails.
	if _, err := conn.Exec(ctx, "DELETE FROM "+quoted(schema)+".edges"); err != nil {
		t.Fatal(err)
	}
	rdir := filepath.Join(t.TempDir(), "r")
	_, err := cli(t, "read", "-dsn", dsn, "-settings", settingsPath, "-arm", "pg-bulk", "-schema", schema, "-out", rdir, "-plan", plan, "-expect-plan", sum, "-repeats", "1")
	if err == nil || !strings.Contains(err.Error(), "differ from the reference's") {
		t.Fatalf("reads of tables that were changed: %v", err)
	}
	var rep pgreport.Read
	if err := pgreport.ReadFile(filepath.Join(rdir, "read.json"), &rep); err != nil {
		t.Fatal(err)
	}
	if rep.Matched >= rep.Queries || len(rep.Mismatches) == 0 || len(rep.Results) != 0 || len(rep.Statements) != 0 {
		t.Errorf("a record of wrong answers: %d of %d matched, %d mismatches, %d results", rep.Matched, rep.Queries, len(rep.Mismatches), len(rep.Results))
	}
}

func withOut(args []string, out string) []string {
	args = append([]string(nil), args...)
	for i := range args {
		if args[i] == "-out" {
			args[i+1] = out
		}
	}
	return args
}

func mustRead(t *testing.T, path string) []byte {
	t.Helper()
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func quoted(schema string) string { return pgx.Identifier{schema}.Sanitize() }

// plainConn is a connection to the server with no schema of its own.
func plainConn(t *testing.T) *pgx.Conn {
	t.Helper()
	c, err := pgx.Connect(context.Background(), serverDSN(t))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = c.Close(context.Background()) })
	return c
}

// activityFile writes a small activity file: an hour of a cluster of 20 nodes and 300
// pods, each pod placed on a node, refreshed, and some of them deleted or moved.
func activityFile(t *testing.T) string {
	t.Helper()
	base := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	at := func(s int) time.Time { return base.Add(time.Duration(s) * time.Second) }
	fp := func(typ catalog.EntityType, n int) identity.Fingerprint {
		var h [identity.FingerprintBytes]byte
		h[0], h[1] = byte(n>>8), byte(n)
		f, err := identity.FingerprintFromHash(typ, h)
		if err != nil {
			t.Fatal(err)
		}
		return f
	}
	var buf bytes.Buffer
	w, err := activity.NewWriter(&buf, activity.WriterOptions{RowGroupRows: 512})
	if err != nil {
		t.Fatal(err)
	}
	seq := uint64(0)
	add := func(r store.Record) {
		seq++
		r.Seq, r.Layer, r.Producer = seq, catalog.L2, "k8s"
		if err := w.Write(r); err != nil {
			t.Fatal(err)
		}
	}
	for n := 0; n < 20; n++ {
		add(store.Record{Subject: store.EntitySubject(fp(catalog.K8sNode, n)), EventTime: at(0), Kind: lifecycle.Observe, Payload: []byte("node")})
	}
	for p := 0; p < 300; p++ {
		pod := fp(catalog.K8sPod, p)
		start := p * 5
		add(store.Record{Subject: store.EntitySubject(pod), EventTime: at(start), Kind: lifecycle.Observe, TTL: 10 * time.Minute, Payload: []byte("pod")})
		edge := store.EdgeSubject(pod, fp(catalog.K8sNode, p%20), catalog.ScheduledOn)
		for r := 0; r < 6; r++ {
			add(store.Record{Subject: edge, EventTime: at(start + 1 + 400*r), Kind: lifecycle.Observe, TTL: 5 * time.Minute, Payload: []byte("e")})
		}
		if p%7 == 0 {
			add(store.Record{Subject: edge, EventTime: at(start + 2000), Kind: lifecycle.Delete})
		}
		if p%5 == 0 {
			add(store.Record{Subject: store.EdgeSubject(pod, fp(catalog.K8sNode, (p+1)%20), catalog.ScheduledOn), EventTime: at(start + 2100), Kind: lifecycle.Observe, TTL: 5 * time.Minute, Payload: []byte("e2")})
		}
	}
	add(store.Record{Subject: store.EntitySubject(fp(catalog.K8sNode, 0)), EventTime: at(3700), Kind: lifecycle.Observe, Payload: []byte("node")})
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "placement.parquet")
	if err := os.WriteFile(path, buf.Bytes(), 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

// TestTheCommandsLoadAnActivityFileAndAskItsQuestions is the path of the Alibaba inputs:
// the questions are made and answered before any load, the load is held to the file they
// were made for, and the reads are held to their answers.
func TestTheCommandsLoadAnActivityFileAndAskItsQuestions(t *testing.T) {
	dsn := serverDSN(t)
	ctx := context.Background()
	file := activityFile(t)
	qdir := filepath.Join(t.TempDir(), "q")
	if _, err := cli(t, "questions", "-activity", file, "-expect-activity", "sha256:000000000000", "-out", qdir); !errors.Is(err, pgspike.ErrDigestDiffers) {
		t.Fatalf("questions for a file that is not the one asked for: %v", err)
	}
	if _, err := os.Stat(qdir); err == nil {
		t.Fatal("a refused run wrote a question set")
	}
	set, err := pginput.BuildQuestions(file, pginput.Options{}, 0, "", nil)
	if err != nil {
		t.Fatal(err)
	}
	digest := strings.TrimPrefix(set.ContentDigest, "sha256:")[:16]
	txt, err := cli(t, "questions", "-activity", file, "-expect-activity", digest, "-out", qdir, "-label", "one synthetic hour")
	if err != nil {
		t.Fatalf("questions: %v\n%s", err, txt)
	}
	qfile := filepath.Join(qdir, "questions.json")
	b := mustRead(t, qfile)
	h := sha256.Sum256(b)
	qsum := hex.EncodeToString(h[:])
	if !strings.Contains(txt, qsum) {
		t.Errorf("the output does not name the digest of the question set %s: %s", qsum, txt)
	}
	if _, err := cli(t, "questions", "-activity", file, "-expect-activity", digest, "-out", qdir); err == nil || !strings.Contains(err.Error(), "never overwritten") {
		t.Errorf("a second question set in one directory: %v", err)
	}

	schema := pgschema.SchemaName(t.TempDir())
	t.Cleanup(func() { _ = pgengine.DropSchema(ctx, dsn, schema) })
	ldir, rdir := filepath.Join(t.TempDir(), "l"), filepath.Join(t.TempDir(), "r")
	base := []string{"-dsn", dsn, "-settings", settingsPath, "-arm", "pg-bulk", "-schema", schema, "-questions", qfile, "-expect-questions", qsum}
	// A load needs the file, named by its digest.
	if _, err := cli(t, append(append([]string{"load", "-out", ldir}, base...), "-activity", file)...); err == nil || !strings.Contains(err.Error(), "-expect-activity") {
		t.Errorf("a load of a file with no digest asked for: %v", err)
	}
	if _, err := cli(t, append(append([]string{"load", "-out", ldir}, base...), "-activity", file, "-expect-activity", "sha256:000000000000")...); !errors.Is(err, pgspike.ErrDigestDiffers) {
		t.Errorf("a load of a file that is not the one asked for: %v", err)
	}
	if txt, err := cli(t, append(append([]string{"load", "-out", ldir}, base...), "-activity", file, "-expect-activity", digest, "-label", "one synthetic hour")...); err != nil {
		t.Fatalf("load: %v\n%s", err, txt)
	}
	// Reads are held to the answers the question set recorded.
	if txt, err := cli(t, append([]string{"read", "-out", rdir, "-repeats", "1"}, base...)...); err != nil || !strings.Contains(txt, "equal the reference's") {
		t.Fatalf("read: %v\n%s", err, txt)
	}
	var l pgreport.Load
	var r pgreport.Read
	if err := pgreport.ReadFile(filepath.Join(ldir, "load.json"), &l); err != nil {
		t.Fatal(err)
	}
	if err := pgreport.ReadFile(filepath.Join(rdir, "read.json"), &r); err != nil {
		t.Fatal(err)
	}
	if l.Input.Kind != "activity" || l.Input.ContentDigest != set.ContentDigest || l.Input.QuestionsSHA256 != qsum || l.Input.Label != "one synthetic hour" ||
		l.Input.Records != set.Records || l.Rows.Log != int64(set.Records) || l.Retentions != 0 || len(r.Results) != len(set.Queries) || r.Matched != len(set.Queries) {
		t.Errorf("load %+v, rows %+v; read %d of %d", l.Input, l.Rows, r.Matched, len(set.Queries))
	}
}
