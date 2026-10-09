package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/lotannauo/toposhift/internal/catalog"
	"github.com/lotannauo/toposhift/internal/identity"
	"github.com/lotannauo/toposhift/internal/lifecycle"
	"github.com/lotannauo/toposhift/internal/replay/activity"
	"github.com/lotannauo/toposhift/internal/store"
	"github.com/lotannauo/toposhift/internal/store/memstore"
	"github.com/lotannauo/toposhift/internal/store/pebblestore"
	"github.com/lotannauo/toposhift/internal/store/storetest"
)

// stream is a generated workload with clones, so some hosts end up quarantined.
func stream(t *testing.T) (storetest.Config, []store.Record) {
	t.Helper()
	cfg := storetest.Tiny()
	cfg.RebootProbability, cfg.CloneProbability = 0.15, 0.6
	g, err := storetest.NewGenerator(cfg)
	if err != nil {
		t.Fatal(err)
	}
	return cfg, g.All()
}

// writeFile writes records as an activity file, four row groups or so.
func writeFile(t *testing.T, path string, recs []store.Record) {
	t.Helper()
	var buf bytes.Buffer
	w, err := activity.NewWriter(&buf, activity.WriterOptions{RowGroupRows: max(len(recs)/4, 1)})
	if err != nil {
		t.Fatal(err)
	}
	for _, r := range recs {
		if err := w.Write(r); err != nil {
			t.Fatalf("writing seq %d: %v", r.Seq, err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, buf.Bytes(), 0o600); err != nil {
		t.Fatal(err)
	}
}

// cli runs the command in this process. Every call opens the store afresh from
// disk, as a second process would.
func cli(t *testing.T, args ...string) (code int, stdout, stderr string) {
	t.Helper()
	var out, errb bytes.Buffer
	code = run(t.Context(), args, noEnv, &out, &errb)
	return code, out.String(), errb.String()
}

func mustCLI(t *testing.T, args ...string) string {
	t.Helper()
	code, out, errs := cli(t, args...)
	if code != 0 {
		t.Fatalf("toposhift %s: exit %d: %s", strings.Join(args, " "), code, errs)
	}
	return out
}

func ts(t time.Time) string { return t.UTC().Format(time.RFC3339Nano) }

// TestReplayThenQueryMatchesMemstore replays a file in two pieces into a
// directory, asks the same questions of the command line (the store reopening
// for each) and of a memstore fed the same records, and compares every answer
// line by line, errors included.
func TestReplayThenQueryMatchesMemstore(t *testing.T) {
	cfg, recs := stream(t)
	dir := t.TempDir()
	data := filepath.Join(dir, "data")
	half := len(recs) / 2
	fileA, fileB := filepath.Join(dir, "a.parquet"), filepath.Join(dir, "b.parquet")
	writeFile(t, fileA, recs[:half])
	writeFile(t, fileB, recs[half:])

	out := mustCLI(t, "replay", "activity", fileA, "--data-dir", data, "--batch", "250")
	wantBatches := (half + 249) / 250
	want := fmt.Sprintf("replayed %d records in %d batches, seq %d to %d, ", half, wantBatches, recs[0].Seq, recs[half-1].Seq)
	if !strings.HasPrefix(out, want) || !strings.HasSuffix(out, " records/s\n") || strings.Count(out, "\n") != 1 {
		t.Errorf("summary = %q, want one line starting %q and ending in records/s", out, want)
	}
	// A file that starts above the store's last seq continues it.
	mustCLI(t, "replay", "activity", "--data-dir", data, fileB)

	ref, err := memstore.Open(memstore.Options{Policy: lifecycle.Policy{BootKey: lifecycle.BootID}})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = ref.Close() }()
	if err := ref.Write(t.Context(), recs); err != nil {
		t.Fatal(err)
	}
	last := recs[len(recs)-1].Seq

	g, err := storetest.NewGenerator(cfg)
	if err != nil {
		t.Fatal(err)
	}
	var fps []identity.Fingerprint
	for i, fp := range g.Entities() {
		if i%9 == 0 {
			fps = append(fps, fp)
		}
	}
	times := []time.Time{g.Start(), g.Start().Add(5 * time.Minute), g.Start().Add(13 * time.Minute), g.End()}
	seen := map[string]int{}
	asked := 0
	check := func(args ...string) {
		t.Helper()
		asked++
		full := append(append([]string{}, args...), "--data-dir", data)
		q, err := parseQueryArgs(full, io.Discard, os.Stderr)
		if err != nil {
			t.Fatalf("%v: %v", args, err)
		}
		var want bytes.Buffer
		asof, _ := q.resolveAsOf(ref.LastSeq())
		lines, note, aerr := answer(t.Context(), ref, q, asof)
		if aerr == nil {
			if err := render(&want, lines); err != nil {
				t.Fatal(err)
			}
		}
		code, got, errs := cli(t, append([]string{"query"}, full...)...)
		if aerr != nil {
			if code != 1 || got != "" || errs != "toposhift query: "+aerr.Error()+"\n" {
				t.Errorf("%v: exit %d, stdout %q, stderr %q; want exit 1 and %q", args, code, got, errs, aerr)
			}
			seen["error"]++
			return
		}
		wantErrs := ""
		if note != "" {
			wantErrs = "toposhift query: note: " + note + "\n"
		}
		if code != 0 || got != want.String() || errs != wantErrs {
			t.Errorf("%v: exit %d, stderr %q (want %q)\n got: %s\nwant: %s", args, code, errs, wantErrs, got, want.String())
		}
		seen[args[0]] += len(lines)
	}
	for _, fp := range fps {
		for _, at := range times {
			check("alive", "--fp", fp.String(), "--at", ts(at))
			for _, d := range []string{"fwd", "rev"} {
				check("neighbors", "--fp", fp.String(), "--dir", d, "--at", ts(at))
			}
		}
		for _, d := range []string{"fwd", "rev"} {
			check("window", "--fp", fp.String(), "--dir", d, "--from", ts(times[0]), "--to", ts(times[3]))
		}
		check("history", "--fp", fp.String(), "--from", ts(times[0]), "--to", ts(times[3]))
	}
	// Other snapshot tokens and an explicit layer.
	for _, fp := range fps[:4] {
		check("neighbors", "--fp", fp.String(), "--dir", "fwd", "--at", ts(times[2]), "--asof", fmt.Sprint(last/3))
		check("window", "--fp", fp.String(), "--dir", "rev", "--from", ts(times[0]), "--to", ts(times[3]), "--layer", "L2", "--asof", fmt.Sprint(last/2))
		check("history", "--fp", fp.String(), "--from", ts(times[1]), "--to", ts(times[2]), "--asof", "latest")
	}
	t.Logf("%d questions asked of the command line from disk and of the reference store in memory; lines per op: %v", asked, seen)
	for _, k := range []string{"alive", "neighbors", "window", "history"} {
		if seen[k] == 0 {
			t.Errorf("no %s question had a non-empty answer: the comparison proved nothing about it", k)
		}
	}
}

// TestQuarantineIsAnErrorNotAnAnswer finds a host the stream quarantines and
// checks the command line says so as the store does.
func TestQuarantineIsAnErrorNotAnAnswer(t *testing.T) {
	cfg, recs := stream(t)
	dir := t.TempDir()
	data := filepath.Join(dir, "data")
	file := filepath.Join(dir, "all.parquet")
	writeFile(t, file, recs)
	mustCLI(t, "replay", "activity", file, "--data-dir", data)

	ref, err := memstore.Open(memstore.Options{Policy: lifecycle.Policy{BootKey: lifecycle.BootID}})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = ref.Close() }()
	if err := ref.Write(t.Context(), recs); err != nil {
		t.Fatal(err)
	}
	g, err := storetest.NewGenerator(cfg)
	if err != nil {
		t.Fatal(err)
	}
	for _, fp := range g.Entities() {
		if fp.Type() != catalog.Host {
			continue
		}
		_, qerr := ref.Alive(t.Context(), fp, g.Start().Add(10*time.Minute), store.Current(catalog.L1))
		if !errors.Is(qerr, store.ErrQuarantined) {
			continue
		}
		code, out, errs := cli(t, "query", "alive", "--data-dir", data, "--fp", fp.String(), "--at", ts(g.Start().Add(10*time.Minute)))
		if code != 1 || out != "" || !strings.Contains(errs, "quarantined") || !strings.Contains(errs, fp.String()) {
			t.Errorf("exit %d, stdout %q, stderr %q; want exit 1 naming %s as quarantined", code, out, errs, fp)
		}
		return
	}
	t.Skip("this stream quarantines no host")
}

// TestAnswerLinesCarryTheFields pins the shape of the output.
func TestAnswerLinesCarryTheFields(t *testing.T) {
	cfg, recs := stream(t)
	dir := t.TempDir()
	data := filepath.Join(dir, "data")
	file := filepath.Join(dir, "all.parquet")
	writeFile(t, file, recs)
	mustCLI(t, "replay", "activity", file, "--data-dir", data)
	last := recs[len(recs)-1].Seq

	// A pod's placement: the first edge record of a pod, forward.
	var pod store.Record
	for _, r := range recs {
		if r.Subject.Kind == store.SubjectEdge && r.Subject.A.Type() == catalog.K8sPod && len(r.Payload) > 0 {
			pod = r
			break
		}
	}
	if pod.Seq == 0 {
		t.Fatal("the stream has no pod edge with a payload")
	}
	g, _ := storetest.NewGenerator(cfg)
	out := mustCLI(t, "query", "window", "--data-dir", data, "--fp", pod.Subject.A.String(), "--dir", "forward",
		"--from", ts(g.Start()), "--to", ts(g.End()), "--layer", pod.Layer.String())
	var first map[string]any
	if err := json.Unmarshal([]byte(strings.SplitN(out, "\n", 2)[0]), &first); err != nil {
		t.Fatalf("not JSON: %q: %v", out, err)
	}
	for _, k := range []string{
		"op", "asof", "layer", "fp", "fp_type", "dir", "subject_kind", "source", "source_type", "target", "target_type",
		"relation", "producer", "kind", "event_time", "seq", "ttl_ns", "basis", "payload_len",
	} {
		if _, ok := first[k]; !ok {
			t.Errorf("a window line has no %q: %v", k, first)
		}
	}
	if first["asof"] != float64(last) || first["fp"] != pod.Subject.A.String() || first["fp_type"] != string(catalog.K8sPod) || first["dir"] != "forward" {
		t.Errorf("window line = %v; want asof %d, fp %s of type %s, dir forward", first, last, pod.Subject.A, catalog.K8sPod)
	}
	// A token past the last one resolves to the last, and says so.
	code, out, errs := cli(t, "query", "alive", "--data-dir", data, "--fp", pod.Subject.A.String(), "--at", ts(g.Start().Add(time.Minute)), "--asof", "999999999")
	if code != 0 || !strings.Contains(out, fmt.Sprintf(`"asof":%d,`, last)) || !strings.Contains(errs, fmt.Sprintf("answered at seq %d", last)) {
		t.Errorf("an asof past the end: exit %d, stdout %q, stderr %q; want it resolved to %d with a note", code, out, errs, last)
	}
}

func TestReplayRefusals(t *testing.T) {
	_, recs := stream(t)
	dir := t.TempDir()
	data := filepath.Join(dir, "data")
	file := filepath.Join(dir, "a.parquet")
	writeFile(t, file, recs[:400])
	mustCLI(t, "replay", "activity", file, "--data-dir", data)
	snapshot := func() string {
		t.Helper()
		return mustCLI(t, "query", "history", "--data-dir", data, "--fp", recs[0].Subject.A.String(),
			"--from", ts(store.MinEventTime), "--to", ts(store.MaxEventTime))
	}
	before := snapshot()

	t.Run("the same file twice", func(t *testing.T) {
		code, _, errs := cli(t, "replay", "activity", file, "--data-dir", data)
		if code != 1 || !strings.Contains(errs, "already holds records up to seq 400") || !strings.Contains(errs, "starts at seq 1") {
			t.Errorf("exit %d, stderr %q; want exit 1 saying the store holds seq 400 and the file starts at 1", code, errs)
		}
	})
	t.Run("an overlapping file", func(t *testing.T) {
		overlap := filepath.Join(dir, "overlap.parquet")
		writeFile(t, overlap, recs[399:800])
		code, _, errs := cli(t, "replay", "activity", overlap, "--data-dir", data)
		if code != 1 || !strings.Contains(errs, "through seq 400, which is inside") || !strings.Contains(errs, "remove "+data+" and replay every file into it again, in order") {
			t.Errorf("exit %d, stderr %q; want exit 1 saying the store stands inside the file and to remove the directory", code, errs)
		}
	})
	if got := snapshot(); got != before {
		t.Errorf("a refused replay changed the store:\nbefore: %s\nafter: %s", before, got)
	}

	fresh := func(name string) string { return filepath.Join(t.TempDir(), name) }
	t.Run("a file that is not Parquet", func(t *testing.T) {
		bad := filepath.Join(dir, "bad.parquet")
		if err := os.WriteFile(bad, []byte("this is not an activity file"), 0o600); err != nil {
			t.Fatal(err)
		}
		d := fresh("data")
		code, _, errs := cli(t, "replay", "activity", bad, "--data-dir", d)
		if code != 1 || !strings.Contains(errs, "is not a usable activity file") {
			t.Errorf("exit %d, stderr %q; want exit 1 and a usable-file sentence", code, errs)
		}
		if _, err := os.Stat(d); !errors.Is(err, os.ErrNotExist) {
			t.Errorf("a refused file created %s (%v)", d, err)
		}
	})
	t.Run("a damaged file leaves nothing behind", func(t *testing.T) {
		raw, err := os.ReadFile(file)
		if err != nil {
			t.Fatal(err)
		}
		raw[len(raw)/2] ^= 0xff
		bad := filepath.Join(dir, "flipped.parquet")
		if err := os.WriteFile(bad, raw, 0o600); err != nil {
			t.Fatal(err)
		}
		d := fresh("data")
		code, _, errs := cli(t, "replay", "activity", bad, "--data-dir", d)
		if code != 1 || !strings.Contains(errs, "is not a usable activity file") {
			t.Errorf("exit %d, stderr %q; want exit 1 and a usable-file sentence", code, errs)
		}
		if _, err := os.Stat(d); !errors.Is(err, os.ErrNotExist) {
			t.Errorf("a damaged file created %s (%v)", d, err)
		}
	})
	t.Run("an edge in two layers", func(t *testing.T) {
		var edge store.Record
		for _, r := range recs {
			if r.Subject.Kind == store.SubjectEdge {
				edge = r
				break
			}
		}
		other := edge
		other.Seq, other.EventTime = edge.Seq+1, edge.EventTime.Add(time.Second)
		other.Layer = catalog.L1
		if edge.Layer == catalog.L1 {
			other.Layer = catalog.L3
		}
		both := filepath.Join(dir, "layers.parquet")
		writeFile(t, both, []store.Record{edge, other})
		d := fresh("data")
		code, _, errs := cli(t, "replay", "activity", both, "--data-dir", d)
		if code != 1 || !strings.Contains(errs, "is in layer") {
			t.Errorf("exit %d, stderr %q; want exit 1 naming the two layers", code, errs)
		}
		if _, err := os.Stat(d); !errors.Is(err, os.ErrNotExist) {
			t.Errorf("a refused file created %s (%v)", d, err)
		}
	})
	t.Run("a missing file", func(t *testing.T) {
		code, _, errs := cli(t, "replay", "activity", filepath.Join(dir, "nope.parquet"), "--data-dir", fresh("data"))
		if code != 1 || !strings.Contains(errs, "cannot open the activity file") {
			t.Errorf("exit %d, stderr %q", code, errs)
		}
	})
	t.Run("no data dir", func(t *testing.T) {
		code, _, errs := cli(t, "replay", "activity", file)
		if code != 2 || !strings.Contains(errs, "--data-dir is required") {
			t.Errorf("exit %d, stderr %q; want exit 2 asking for --data-dir", code, errs)
		}
	})
	t.Run("a directory that is not a store", func(t *testing.T) {
		d := t.TempDir()
		if err := os.WriteFile(filepath.Join(d, "notes.txt"), []byte("mine"), 0o600); err != nil {
			t.Fatal(err)
		}
		code, _, errs := cli(t, "replay", "activity", file, "--data-dir", d)
		if code != 1 || !strings.Contains(errs, "does not hold a Pebble store") {
			t.Errorf("exit %d, stderr %q", code, errs)
		}
		if entries, _ := os.ReadDir(d); len(entries) != 1 {
			t.Errorf("a refused replay left %d entries in %s, want the 1 that was there", len(entries), d)
		}
	})
	t.Run("bad flags", func(t *testing.T) {
		for _, args := range [][]string{
			{"replay", "activity"},
			{"replay", "activity", file, file, "--data-dir", fresh("data")},
			{"replay", "activity", file, "--data-dir", fresh("data"), "--batch", "0"},
			{"replay", "activity", file, "--data-dir", fresh("data"), "--store", "rocks"},
			{"replay", "activity", file, "--data-dir", fresh("data"), "--store", "mem"},
			{"replay", "csv", file},
		} {
			if code, _, errs := cli(t, args...); code != 2 || errs == "" {
				t.Errorf("%v: exit %d, stderr %q; want exit 2 and a message", args, code, errs)
			}
		}
	})
}

func TestReplayIntoMemoryKeepsNothing(t *testing.T) {
	_, recs := stream(t)
	file := filepath.Join(t.TempDir(), "a.parquet")
	writeFile(t, file, recs[:300])
	before, _ := os.ReadDir(filepath.Dir(file))
	out := mustCLI(t, "replay", "activity", file, "--store", "mem", "--batch", "100")
	if !strings.HasPrefix(out, "replayed 300 records in 3 batches, seq 1 to 300, ") || !strings.Contains(out, "in memory; nothing was kept") {
		t.Errorf("summary = %q", out)
	}
	if after, _ := os.ReadDir(filepath.Dir(file)); len(after) != len(before) {
		t.Errorf("a replay into memory wrote files: %v", after)
	}
}

func TestReplayAnEmptyFile(t *testing.T) {
	dir := t.TempDir()
	file := filepath.Join(dir, "empty.parquet")
	writeFile(t, file, nil)
	data := filepath.Join(dir, "data")
	out := mustCLI(t, "replay", "activity", file, "--data-dir", data)
	if !strings.Contains(out, "holds no records") {
		t.Errorf("summary = %q", out)
	}
	if _, err := os.Stat(data); !errors.Is(err, os.ErrNotExist) {
		t.Errorf("an empty file created %s (%v)", data, err)
	}
}

func TestQueryRefusals(t *testing.T) {
	cfg, recs := stream(t)
	dir := t.TempDir()
	data := filepath.Join(dir, "data")
	file := filepath.Join(dir, "a.parquet")
	writeFile(t, file, recs)
	mustCLI(t, "replay", "activity", file, "--data-dir", data)
	g, _ := storetest.NewGenerator(cfg)
	fp := g.Entities()[10].String()
	at := ts(g.Start().Add(time.Minute))
	stranger := "k8s.pod:" + strings.Repeat("ab", 16)

	t.Run("an unknown fingerprint has no history", func(t *testing.T) {
		for _, args := range [][]string{
			{"alive", "--fp", stranger, "--at", at},
			{"history", "--fp", stranger, "--from", at, "--to", ts(g.End())},
		} {
			code, out, errs := cli(t, append([]string{"query", "--data-dir", data}, args...)...)
			if code != 1 || out != "" || !strings.Contains(errs, "no history: no entity records for "+stranger) {
				t.Errorf("%v: exit %d, stdout %q, stderr %q; want exit 1 and no history", args, code, out, errs)
			}
		}
	})
	t.Run("for neighbors and window it is an empty answer with a note", func(t *testing.T) {
		for _, args := range [][]string{
			{"neighbors", "--fp", stranger, "--dir", "fwd", "--at", at},
			{"window", "--fp", stranger, "--dir", "rev", "--from", at, "--to", ts(g.End())},
		} {
			code, out, errs := cli(t, append([]string{"query", "--data-dir", data}, args...)...)
			if code != 0 || out != "" || !strings.Contains(errs, "note: no entity records for "+stranger) || strings.Contains(errs, "no history") {
				t.Errorf("%v: exit %d, stdout %q, stderr %q; want exit 0, no output and a note", args, code, out, errs)
			}
		}
	})
	t.Run("a known fingerprint with nothing in the window is an empty answer", func(t *testing.T) {
		pod := ""
		for _, e := range g.Entities() {
			if e.Type() == catalog.K8sPod {
				pod = e.String()
				break
			}
		}
		code, out, errs := cli(t, "query", "history", "--data-dir", data, "--fp", pod, "--from", ts(store.MinEventTime), "--to", ts(store.MinEventTime.Add(time.Hour)))
		if code != 0 || out != "" || errs != "" {
			t.Errorf("exit %d, stdout %q, stderr %q; want an empty successful answer", code, out, errs)
		}
	})
	t.Run("usage", func(t *testing.T) {
		for _, args := range [][]string{
			{"query"},
			{"query", "shortest-path", "--data-dir", data},
			{"query", "alive", "--fp", fp, "--at", at},
			{"query", "alive", "--data-dir", data, "--at", at},
			{"query", "alive", "--data-dir", data, "--fp", fp},
			{"query", "alive", "--data-dir", data, "--fp", "nonsense", "--at", at},
			{"query", "alive", "--data-dir", data, "--fp", fp, "--at", "yesterday"},
			{"query", "alive", "--data-dir", data, "--fp", fp, "--at", at, "--dir", "fwd"},
			{"query", "neighbors", "--data-dir", data, "--fp", fp, "--at", at, "--dir", "sideways"},
			{"query", "alive", "--data-dir", data, "--fp", fp, "--at", at, "--layer", "L9"},
			{"query", "alive", "--data-dir", data, "--fp", fp, "--at", at, "--asof", "-3"},
			{"query", "history", "--data-dir", data, "--fp", fp, "--from", at},
		} {
			if code, out, errs := cli(t, args...); code != 2 || out != "" || errs == "" {
				t.Errorf("%v: exit %d, stdout %q, stderr %q; want exit 2 and a message", args, code, out, errs)
			}
		}
	})
	t.Run("no store is created", func(t *testing.T) {
		missing := filepath.Join(t.TempDir(), "nowhere")
		code, _, errs := cli(t, "query", "alive", "--data-dir", missing, "--fp", fp, "--at", at)
		if code != 1 || !strings.Contains(errs, "there is no store in "+missing) {
			t.Errorf("exit %d, stderr %q", code, errs)
		}
		if _, err := os.Stat(missing); !errors.Is(err, os.ErrNotExist) {
			t.Errorf("a query created %s (%v)", missing, err)
		}
		empty := t.TempDir()
		code, _, errs = cli(t, "query", "alive", "--data-dir", empty, "--fp", fp, "--at", at)
		if entries, _ := os.ReadDir(empty); code != 1 || len(entries) != 0 || !strings.Contains(errs, "there is no store in") {
			t.Errorf("empty directory: exit %d, stderr %q, %d entries left", code, errs, len(entries))
		}
	})
	t.Run("a question before the horizon names the horizon", func(t *testing.T) {
		d := filepath.Join(t.TempDir(), "data")
		mustCLI(t, "replay", "activity", file, "--data-dir", d)
		horizon := g.Start().Add(10 * time.Minute)
		st, err := pebblestore.Open(d, pebblestore.DefaultOptions())
		if err != nil {
			t.Fatal(err)
		}
		if err := st.Retain(t.Context(), horizon); err != nil {
			t.Fatal(err)
		}
		if err := st.Close(); err != nil {
			t.Fatal(err)
		}
		code, _, errs := cli(t, "query", "alive", "--data-dir", d, "--fp", fp, "--at", at)
		if code != 1 || !strings.Contains(errs, "has been trimmed to a horizon at "+ts(horizon)) {
			t.Errorf("exit %d, stderr %q; want exit 1 naming the horizon %s", code, errs, ts(horizon))
		}
	})
}

// TestSeparateProcesses runs the built binary twice: once to replay, once, in a
// new process, to query.
func TestSeparateProcesses(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping binary test in short mode")
	}
	cfg, recs := stream(t)
	dir := t.TempDir()
	file := filepath.Join(dir, "a.parquet")
	writeFile(t, file, recs)
	bin := filepath.Join(dir, "toposhift")
	if out, err := exec.CommandContext(t.Context(), "go", "build", "-o", bin, ".").CombinedOutput(); err != nil {
		t.Fatalf("go build: %v\n%s", err, out)
	}
	exe := func(ctx context.Context, args ...string) (string, error) {
		cmd := exec.CommandContext(ctx, bin, args...)
		cmd.Env = []string{}
		cmd.Dir = t.TempDir()
		out, err := cmd.Output()
		return string(out), err
	}
	data := filepath.Join(dir, "data")
	if _, err := exe(t.Context(), "replay", "activity", file, "--data-dir", data); err != nil {
		t.Fatal(err)
	}
	g, _ := storetest.NewGenerator(cfg)
	var rack identity.Fingerprint
	for _, e := range g.Entities() {
		if e.Type() == catalog.Rack {
			rack = e
			break
		}
	}
	out, err := exe(t.Context(), "query", "history", "--data-dir", data, "--fp", rack.String(), "--from", ts(g.Start()), "--to", ts(g.End()))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(out, `"op":"history"`) || !strings.Contains(out, rack.String()) {
		t.Errorf("history of %s = %q", rack, out)
	}

	// Another process holds the directory: the lock is the operating system's.
	held, err := pebblestore.Open(data, pebblestore.DefaultOptions())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = held.Close() }()
	cmd := exec.CommandContext(t.Context(), bin, "query", "history", "--data-dir", data, "--fp", rack.String(), "--from", ts(g.Start()), "--to", ts(g.End()))
	cmd.Env = []string{}
	cmd.Dir = t.TempDir()
	var errb bytes.Buffer
	cmd.Stderr = &errb
	if err := cmd.Run(); err == nil || !strings.Contains(errb.String(), "another toposhift process has "+data+" open") {
		t.Errorf("a query while the directory is held: %v, stderr %q", err, errb.String())
	}
}
