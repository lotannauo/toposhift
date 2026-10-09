package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/cockroachdb/pebble/v2"

	"github.com/lotannauo/toposhift/internal/catalog"
	"github.com/lotannauo/toposhift/internal/identity"
	"github.com/lotannauo/toposhift/internal/lifecycle"
	"github.com/lotannauo/toposhift/internal/store"
	"github.com/lotannauo/toposhift/internal/store/memstore"
	"github.com/lotannauo/toposhift/internal/store/pebblestore"
	"github.com/lotannauo/toposhift/internal/store/storetest"
)

func parseFP(t *testing.T, s string) identity.Fingerprint {
	t.Helper()
	fp, err := identity.ParseFingerprint(s)
	if err != nil {
		t.Fatal(err)
	}
	return fp
}

// treeDigest is a digest of every file name and content under dir.
func treeDigest(t *testing.T, dir string) string {
	t.Helper()
	var parts []string
	err := filepath.WalkDir(dir, func(p string, d fs.DirEntry, err error) error {
		if err != nil || d.IsDir() {
			return err
		}
		b, err := os.ReadFile(p)
		if err != nil {
			return err
		}
		rel, _ := filepath.Rel(dir, p)
		parts = append(parts, fmt.Sprintf("%s %x", rel, sha256.Sum256(b)))
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	sort.Strings(parts)
	return strings.Join(parts, "\n")
}

// cancelAfter is a store whose context is cancelled once it has taken n batches.
type cancelAfter struct {
	store.Store
	n      int
	cancel context.CancelFunc
}

func (c *cancelAfter) Write(ctx context.Context, b []store.Record) error {
	if c.n == 0 {
		c.cancel()
	}
	c.n--
	return c.Store.Write(ctx, b)
}

// withStoreWrapper makes the replay open its store through wrap, for the length
// of the test. It changes the package variable openReplayStore, so a test that
// uses it, or runs beside one that does, must not call t.Parallel.
func withStoreWrapper(t *testing.T, wrap func(store.Store) store.Store) {
	t.Helper()
	orig := openReplayStore
	openReplayStore = func(kind, dir string) (store.Store, error) {
		st, err := orig(kind, dir)
		if err != nil {
			return nil, err
		}
		return wrap(st), nil
	}
	t.Cleanup(func() { openReplayStore = orig })
}

func TestAnInterruptedReplayIsNamedAndCanBeRedone(t *testing.T) {
	_, recs := stream(t)
	dir := t.TempDir()
	file := filepath.Join(dir, "a.parquet")
	data := filepath.Join(dir, "data")
	writeFile(t, file, recs[:1000])
	o := replayOptions{file: file, dataDir: data, kind: "pebble", batch: 250}

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	withStoreWrapper(t, func(st store.Store) store.Store { return &cancelAfter{Store: st, n: 2, cancel: cancel} })
	err := replayActivity(ctx, o, io.Discard)
	if !errors.Is(err, context.Canceled) || !strings.Contains(err.Error(), "batch 3 (seq 501 to 750) failed") ||
		!strings.Contains(err.Error(), "records up to seq 500") || !strings.Contains(err.Error(), "partial replay of "+file+": remove "+data+" and replay every file into it again, in order (or use a new directory)") {
		t.Fatalf("an interrupted replay = %v; want batch 3 to have failed, the store at seq 500 and the advice to start again", err)
	}

	// The next replay of the file says where the store stands.
	code, _, errs := cli(t, "replay", "activity", file, "--data-dir", data)
	if code != 1 || !strings.Contains(errs, "through seq 500, which is inside "+file+" (seq 1 to 1000)") ||
		!strings.Contains(errs, "remove "+data+" and replay every file into it again, in order") {
		t.Errorf("exit %d, stderr %q; want the store's place inside the file and the advice", code, errs)
	}
	if err := os.RemoveAll(data); err != nil {
		t.Fatal(err)
	}
	openReplayStore = func(kind, dir string) (store.Store, error) { return openPebble(dir, false) }
	if out := mustCLI(t, "replay", "activity", file, "--data-dir", data); !strings.HasPrefix(out, "replayed 1000 records in 1 batches, seq 1 to 1000, ") {
		t.Errorf("summary after starting again = %q", out)
	}
}

func TestAReplayThatStoredNothingSaysSo(t *testing.T) {
	_, recs := stream(t)
	dir := t.TempDir()
	file := filepath.Join(dir, "a.parquet")
	writeFile(t, file, recs[:300])
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	withStoreWrapper(t, func(st store.Store) store.Store { return &cancelAfter{Store: st, cancel: cancel} })
	err := replayActivity(ctx, replayOptions{file: file, dataDir: filepath.Join(dir, "data"), kind: "pebble", batch: 100}, io.Discard)
	if err == nil || !strings.Contains(err.Error(), "nothing of "+file+" was stored") || strings.Contains(err.Error(), "partial") {
		t.Errorf("replay = %v; want it to say nothing was stored", err)
	}
}

func TestWriteFailureWhenTheCommitIsLost(t *testing.T) {
	ref, err := memstore.Open(memstore.Options{})
	if err != nil {
		t.Fatal(err)
	}
	cause := errors.New("pebblestore: Write: a commit failed (disk), so whether its batch is stored is decided only when the store is reopened: reopen the store")
	got := writeFailure(replayOptions{file: "f.parquet", dataDir: "d", kind: "pebble"}, 7, 31, 40, cause, ref, 0).Error()
	if !strings.Contains(got, "known only when the store is reopened") || strings.Contains(got, "remove d") {
		t.Errorf("writeFailure = %q; want it to say the outcome is known on reopening", got)
	}
}

func TestADirectoryAnotherOpenHoldsIsReported(t *testing.T) {
	_, recs := stream(t)
	dir := t.TempDir()
	file := filepath.Join(dir, "a.parquet")
	data := filepath.Join(dir, "data")
	writeFile(t, file, recs[:300])
	mustCLI(t, "replay", "activity", file, "--data-dir", data)

	st, err := pebblestore.Open(data, pebblestore.DefaultOptions())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = st.Close() }()
	want := "another toposhift process has " + data + " open (a replay or a query); try again when it finishes"
	g, _ := storetest.NewGenerator(storetest.Tiny())
	fp := g.Entities()[3].String()
	code, _, errs := cli(t, "query", "alive", "--data-dir", data, "--fp", fp, "--at", ts(g.Start()))
	if code != 1 || !strings.Contains(errs, want) {
		t.Errorf("query: exit %d, stderr %q; want %q", code, errs, want)
	}
	writeFile(t, file, recs[300:600])
	code, _, errs = cli(t, "replay", "activity", file, "--data-dir", data)
	if code != 1 || !strings.Contains(errs, want) {
		t.Errorf("replay: exit %d, stderr %q; want %q", code, errs, want)
	}
}

func TestNoHistoryIsOneBoundedRead(t *testing.T) {
	cfg, recs := stream(t)
	dir := t.TempDir()
	data := filepath.Join(dir, "data")
	file := filepath.Join(dir, "a.parquet")
	writeFile(t, file, recs)
	mustCLI(t, "replay", "activity", file, "--data-dir", data)
	g, _ := storetest.NewGenerator(cfg)
	var pod string
	for _, e := range g.Entities() {
		if e.Type() == catalog.K8sPod {
			pod = e.String()
			break
		}
	}
	before := ts(g.Start().Add(-time.Hour))

	t.Run("an entity that exists but is not alive then is an answer", func(t *testing.T) {
		out := mustCLI(t, "query", "alive", "--data-dir", data, "--fp", pod, "--at", before)
		if !strings.Contains(out, `"alive":false`) {
			t.Errorf("alive before the start = %q", out)
		}
	})
	t.Run("a wrong layer does not hide the entity", func(t *testing.T) {
		out := mustCLI(t, "query", "history", "--data-dir", data, "--fp", pod, "--layer", "L0", "--from", before, "--to", ts(g.End()))
		if out != "" {
			t.Errorf("history in L0 = %q, want nothing", out)
		}
	})
	t.Run("the message names the layer of the entity type", func(t *testing.T) {
		stranger := "k8s.pod:" + strings.Repeat("cd", 16)
		for _, args := range [][]string{
			{"history", "--from", before, "--to", ts(g.End())},
			{"neighbors", "--dir", "fwd", "--at", before},
		} {
			code, _, errs := cli(t, append([]string{"query", "--data-dir", data, "--fp", stranger, "--layer", "L3"}, args...)...)
			wantCode := 1
			if args[0] == "neighbors" {
				wantCode = 0
			}
			if code != wantCode || !strings.Contains(errs, "no entity records for "+stranger+" as of seq") || !strings.Contains(errs, "in layer L2 (a fingerprint that only appears as an edge endpoint has no entity history)") {
				t.Errorf("%s: exit %d (want %d), stderr %q; want L2, the pod's own layer", args[0], code, wantCode, errs)
			}
		}
	})
	t.Run("a type that is not in the catalog", func(t *testing.T) {
		odd := "no.such:" + strings.Repeat("ab", 16)
		code, _, errs := cli(t, "query", "alive", "--data-dir", data, "--fp", odd, "--at", before)
		if code != 1 || !strings.Contains(errs, `is not an entity type`) {
			t.Errorf("alive: exit %d, stderr %q", code, errs)
		}
		code, out, errs := cli(t, "query", "neighbors", "--data-dir", data, "--fp", odd, "--dir", "fwd", "--at", before)
		if code != 0 || out != "" || !strings.Contains(errs, `is not an entity type`) {
			t.Errorf("neighbors: exit %d, stdout %q, stderr %q; want a note", code, out, errs)
		}
	})
	t.Run("an edge endpoint with no entity record has neighbors where it has edges", func(t *testing.T) {
		var pods, nodes []string
		for _, e := range g.Entities() {
			switch e.Type() {
			case catalog.K8sPod:
				pods = append(pods, e.String())
			case catalog.K8sNode:
				nodes = append(nodes, e.String())
			}
		}
		p, n := pods[0], nodes[0]
		edge := store.Record{
			Layer: catalog.L2, Subject: store.EdgeSubject(parseFP(t, p), parseFP(t, n), catalog.ScheduledOn), Producer: "k8sobjects",
			EventTime: g.Start(), Seq: 1, Kind: lifecycle.Observe, Payload: []byte("x"),
		}
		edgeFile := filepath.Join(dir, "edge.parquet")
		edgeData := filepath.Join(dir, "edge-data")
		writeFile(t, edgeFile, []store.Record{edge})
		mustCLI(t, "replay", "activity", edgeFile, "--data-dir", edgeData)
		out := mustCLI(t, "query", "neighbors", "--data-dir", edgeData, "--fp", p, "--dir", "fwd", "--at", ts(g.Start().Add(time.Minute)))
		if !strings.Contains(out, n) {
			t.Errorf("neighbors after the edge = %q, want %s", out, n)
		}
		code, out, errs := cli(t, "query", "neighbors", "--data-dir", edgeData, "--fp", p, "--dir", "fwd", "--at", before)
		if code != 0 || out != "" || !strings.Contains(errs, "note: no entity records for "+p) {
			t.Errorf("neighbors before the edge: exit %d, stdout %q, stderr %q; want an empty answer and a note", code, out, errs)
		}
		code, _, errs = cli(t, "query", "alive", "--data-dir", edgeData, "--fp", p, "--at", ts(g.Start().Add(time.Minute)))
		if code != 1 || !strings.Contains(errs, "no history: no entity records for "+p) {
			t.Errorf("alive on an endpoint: exit %d, stderr %q; want no history", code, errs)
		}
	})
}

func TestTheEdgeLayerCheck(t *testing.T) {
	_, recs := stream(t)
	var edge store.Record
	for _, r := range recs {
		if r.Subject.Kind == store.SubjectEdge {
			edge = r
			break
		}
	}
	other := edge
	other.Seq, other.EventTime = edge.Seq+1, edge.EventTime.Add(time.Second)
	other.Layer = catalog.L3
	if edge.Layer == catalog.L3 {
		other.Layer = catalog.L1
	}
	dir := t.TempDir()
	file := filepath.Join(dir, "layers.parquet")
	writeFile(t, file, []store.Record{edge, other})

	code, _, errs := cli(t, "replay", "activity", file, "--data-dir", filepath.Join(dir, "a"))
	want := fmt.Sprintf("is in layer %s at seq %d and was in layer %s earlier in the file", other.Layer, other.Seq, edge.Layer)
	if code != 1 || !strings.Contains(errs, want) {
		t.Errorf("exit %d, stderr %q; want %q", code, errs, want)
	}
	// Without the check what remains is the store's own refusal, which sees
	// only one batch at a time: one batch of both records is refused, and one
	// record a batch is not, whatever the layer of the edge was.
	code, _, errs = cli(t, "replay", "activity", file, "--data-dir", filepath.Join(dir, "b1000"), "--no-layer-check", "--batch", "1000")
	if want := fmt.Sprintf("subject is stored in layer %s, not %s", edge.Layer, other.Layer); code != 1 || !strings.Contains(errs, want) {
		t.Errorf("--no-layer-check --batch 1000: exit %d, stderr %q; want the store's %q", code, errs, want)
	}
	if code, _, errs = cli(t, "replay", "activity", file, "--data-dir", filepath.Join(dir, "b1"), "--no-layer-check", "--batch", "1"); code != 0 {
		t.Errorf("--no-layer-check --batch 1: exit %d, stderr %q; want the store to take both records", code, errs)
	}
	// With the check on, the batch size makes no difference.
	if code, _, errs = cli(t, "replay", "activity", file, "--data-dir", filepath.Join(dir, "c1"), "--batch", "1"); code != 1 || !strings.Contains(errs, "earlier in the file") {
		t.Errorf("--batch 1 with the check: exit %d, stderr %q", code, errs)
	}
}

// quietLogger drops Pebble's messages, which a test run does not want.
type quietLogger struct{}

func (quietLogger) Infof(string, ...any)  {}
func (quietLogger) Errorf(string, ...any) {}
func (quietLogger) Fatalf(format string, args ...any) {
	panic(fmt.Sprintf(format, args...))
}

func TestAPebbleDatabaseThatIsNotAStoreIsLeftAlone(t *testing.T) {
	_, recs := stream(t)
	dir := t.TempDir()
	file := filepath.Join(dir, "a.parquet")
	writeFile(t, file, recs[:100])
	foreign := filepath.Join(dir, "foreign")
	db, err := pebble.Open(foreign, &pebble.Options{Logger: quietLogger{}})
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Set([]byte("their key"), []byte("their value"), pebble.Sync); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	before := treeDigest(t, foreign)
	code, _, errs := cli(t, "replay", "activity", file, "--data-dir", foreign)
	if code != 1 || !strings.Contains(errs, "holds a Pebble database that is not a usable toposhift store, and it was left as it was") {
		t.Errorf("replay: exit %d, stderr %q", code, errs)
	}
	g, _ := storetest.NewGenerator(storetest.Tiny())
	code, _, errs = cli(t, "query", "alive", "--data-dir", foreign, "--fp", g.Entities()[0].String(), "--at", ts(g.Start()))
	if code != 1 || errs == "" {
		t.Errorf("query: exit %d, stderr %q", code, errs)
	}
	if after := treeDigest(t, foreign); after != before {
		t.Errorf("the foreign database changed:\nbefore:\n%s\nafter:\n%s", before, after)
	}
}

func TestAQueryWritesNothing(t *testing.T) {
	cfg, recs := stream(t)
	dir := t.TempDir()
	file := filepath.Join(dir, "a.parquet")
	data := filepath.Join(dir, "data")
	writeFile(t, file, recs)
	mustCLI(t, "replay", "activity", file, "--data-dir", data)
	before := treeDigest(t, data)
	g, _ := storetest.NewGenerator(cfg)
	mustCLI(t, "query", "history", "--data-dir", data, "--fp", g.Entities()[0].String(), "--from", ts(g.Start()), "--to", ts(g.End()))
	if after := treeDigest(t, data); after != before {
		t.Errorf("a query changed the store:\nbefore:\n%s\nafter:\n%s", before, after)
	}
}

func TestAFinderFileDoesNotMakeADirectoryNonEmpty(t *testing.T) {
	_, recs := stream(t)
	dir := t.TempDir()
	file := filepath.Join(dir, "a.parquet")
	writeFile(t, file, recs[:100])
	data := filepath.Join(dir, "data")
	if err := os.Mkdir(data, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(data, ".DS_Store"), nil, 0o600); err != nil {
		t.Fatal(err)
	}
	mustCLI(t, "replay", "activity", file, "--data-dir", data)
}

func TestAFileCheckedThenChangedIsNotReplayed(t *testing.T) {
	_, recs := stream(t)
	dir := t.TempDir()
	file := filepath.Join(dir, "a.parquet")
	writeFile(t, file, recs[:100])
	withStoreWrapper(t, func(st store.Store) store.Store {
		if err := os.WriteFile(file, []byte("something else"), 0o600); err != nil {
			t.Fatal(err)
		}
		return st
	})
	err := replayActivity(t.Context(), replayOptions{file: file, dataDir: filepath.Join(dir, "data"), kind: "pebble", batch: 50}, io.Discard)
	if err == nil || !strings.Contains(err.Error(), "changed after it was checked; nothing was written") {
		t.Errorf("replay = %v", err)
	}
}

func TestAReplayedFileIsRefusedBeforeItIsReadThrough(t *testing.T) {
	_, recs := stream(t)
	dir := t.TempDir()
	file := filepath.Join(dir, "a.parquet")
	data := filepath.Join(dir, "data")
	writeFile(t, file, recs[:300])
	mustCLI(t, "replay", "activity", file, "--data-dir", data)
	raw, err := os.ReadFile(file)
	if err != nil {
		t.Fatal(err)
	}
	raw[len(raw)/2] ^= 0xff
	if err := os.WriteFile(file, raw, 0o600); err != nil {
		t.Fatal(err)
	}
	// Reading the damaged copy through would say it is unusable; the store's
	// place is looked at first.
	code, _, errs := cli(t, "replay", "activity", file, "--data-dir", data)
	if code != 1 || !strings.Contains(errs, "already holds records up to seq 300") {
		t.Errorf("exit %d, stderr %q; want the store's place to be what is refused", code, errs)
	}
}

func TestCommandLineForms(t *testing.T) {
	_, recs := stream(t)
	dir := t.TempDir()
	file := filepath.Join(dir, "a.parquet")
	writeFile(t, file, recs[:100])

	t.Run("flags before the source word", func(t *testing.T) {
		mustCLI(t, "replay", "--data-dir", filepath.Join(dir, "d1"), "activity", file)
	})
	t.Run("after two dashes everything is a file", func(t *testing.T) {
		raw, _ := os.ReadFile(file)
		odd := t.TempDir()
		if err := os.WriteFile(filepath.Join(odd, "-odd.parquet"), raw, 0o600); err != nil {
			t.Fatal(err)
		}
		t.Chdir(odd)
		mustCLI(t, "replay", "activity", "--data-dir", filepath.Join(dir, "d2"), "--", "-odd.parquet")
	})
	t.Run("help goes to stdout", func(t *testing.T) {
		for _, args := range [][]string{
			{"query", "-h"}, {"query", "alive", "-h"}, {"query", "help"}, {"replay", "-h"}, {"replay", "activity", "--help"}, {"replay", "help"},
		} {
			code, out, errs := cli(t, args...)
			if code != 0 || !strings.Contains(out, "Usage: toposhift") || errs != "" {
				t.Errorf("%v: exit %d, stdout %q, stderr %q; want usage on stdout", args, code, out, errs)
			}
		}
	})
	t.Run("a bad flag goes to stderr", func(t *testing.T) {
		code, out, errs := cli(t, "query", "alive", "--nope")
		if code != 2 || out != "" || !strings.Contains(errs, "flag provided but not defined") {
			t.Errorf("exit %d, stdout %q, stderr %q", code, out, errs)
		}
	})
	t.Run("the summary says what it timed", func(t *testing.T) {
		out := mustCLI(t, "replay", "activity", file, "--store", "mem")
		if !bytes.Contains([]byte(out), []byte(", written in ")) {
			t.Errorf("summary = %q", out)
		}
	})
}
