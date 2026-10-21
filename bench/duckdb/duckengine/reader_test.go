package duckengine

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/lotannauo/toposhift/bench/spike/engine"
	"github.com/lotannauo/toposhift/bench/spike/runner"
	"github.com/lotannauo/toposhift/internal/catalog"
	"github.com/lotannauo/toposhift/internal/identity"
)

func copyBuild(t *testing.T) (string, *Manifest) {
	t.Helper()
	src, _, m := sharedBuild(t)
	dst := filepath.Join(t.TempDir(), "build")
	if err := os.CopyFS(dst, os.DirFS(src)); err != nil {
		t.Fatal(err)
	}
	return dst, m
}

func TestAReaderTakesNoWrite(t *testing.T) {
	dir, plan, man := sharedBuild(t)
	r, err := OpenReader(context.Background(), openRead(t, false), dir, ArmA2, false)
	mustf(t, err, "open")
	if err := r.Write([]engine.Record{{}}); !errors.Is(err, ErrReadOnly) {
		t.Errorf("Write: %v", err)
	}
	if err := r.Retain(time.Unix(1, 0)); !errors.Is(err, ErrReadOnly) {
		t.Errorf("Retain: %v", err)
	}
	if r.LastSeq() != plan.Stream.LastSeq || r.LastSeq() != man.Stream.LastSeq {
		t.Errorf("LastSeq %d, the plan's %d", r.LastSeq(), plan.Stream.LastSeq)
	}
	f1, _ := man.File(RoleFwd)
	f2, _ := man.File(RoleRev)
	if n, err := r.Size(); err != nil || n != f1.Bytes+f2.Bytes {
		t.Errorf("Size %d, %v; the files take %d", n, err, f1.Bytes+f2.Bytes)
	}
	if err := r.Close(); err != nil {
		t.Error(err)
	}
}

func TestOpenReaderRefusesWhatItShould(t *testing.T) {
	ctx := context.Background()
	rewrite := func(dir string, f func(*Manifest)) {
		t.Helper()
		m, err := ReadManifest(dir)
		mustf(t, err, "manifest")
		f(m)
		mustf(t, writeManifest(dir, m), "write manifest")
	}
	for name, c := range map[string]struct {
		mutate func(dir string, m *Manifest)
		arm    Arm
		hashes bool
		want   error
	}{
		"another DuckDB version": {func(d string, m *Manifest) { rewrite(d, func(m *Manifest) { m.DuckDB = "v1.4.0" }) }, ArmA2, false, ErrManifest},
		"other settings": {func(d string, m *Manifest) {
			rewrite(d, func(m *Manifest) { m.SettingsSHA256 = strings.Repeat("0", 64) })
		}, ArmA2, false, ErrManifest},
		"an arm that was not built": {func(d string, m *Manifest) {
			rewrite(d, func(m *Manifest) { m.Arms = []Arm{ArmA1, ArmI} })
		}, ArmA2, false, ErrManifest},
		"the interval arm without the record files it reads": {func(d string, m *Manifest) {
			rewrite(d, func(m *Manifest) {
				m.Files = slices.DeleteFunc(m.Files, func(f FileInfo) bool { return f.Role == RoleRev })
			})
		}, ArmI, false, ErrManifest},
		"a file that is not there": {func(d string, m *Manifest) { _ = os.Remove(filepath.Join(d, "a2", "rev.parquet")) }, ArmA2, false, ErrFilesDiffer},
		"a file of another size": {func(d string, m *Manifest) {
			f, _ := os.OpenFile(filepath.Join(d, "a2", "fwd.parquet"), os.O_APPEND|os.O_WRONLY, 0)
			_, _ = f.Write([]byte{0})
			f.Close()
		}, ArmA2, false, ErrFilesDiffer},
		"a file of another content": {func(d string, m *Manifest) {
			p := filepath.Join(d, "a2", "fwd.parquet")
			b, _ := os.ReadFile(p)
			b[100] ^= 0xff
			_ = os.WriteFile(p, b, 0o644)
		}, ArmA2, true, ErrFilesDiffer},
	} {
		dir, m := copyBuild(t)
		c.mutate(dir, m)
		if _, err := OpenReader(ctx, openRead(t, false), dir, c.arm, c.hashes); !errors.Is(err, c.want) {
			t.Errorf("%s: %v", name, err)
		}
	}
	// The same changed content is not seen without the hashes, which is why a read checks
	// them.
	dir, _ := copyBuild(t)
	p := filepath.Join(dir, "a2", "fwd.parquet")
	b, _ := os.ReadFile(p)
	b[100] ^= 0xff
	mustf(t, os.WriteFile(p, b, 0o644), "write")
	r, err := OpenReader(ctx, openRead(t, false), dir, ArmA2, false)
	mustf(t, err, "without hashes")
	r.Close()

	dir, _, _ = sharedBuild(t)
	// A connection for a build, or one whose settings were changed, cannot read.
	bdb, err := Open(ctx, Config{Mode: ModeBuild, TempDir: t.TempDir()})
	mustf(t, err, "open build")
	defer bdb.Close()
	if _, err := OpenReader(ctx, bdb, dir, ArmA2, false); err == nil {
		t.Error("a reader on a build connection")
	}
	db := openRead(t, false)
	if _, err := db.ExecContext(ctx, "SET threads = 3"); err != nil {
		t.Fatal(err)
	}
	if _, err := OpenReader(ctx, db, dir, ArmA2, false); !errors.Is(err, ErrSettingsDiffer) {
		t.Errorf("a connection with a changed setting: %v", err)
	}
	if _, err := OpenReader(ctx, openRead(t, false), dir, Arm("Z"), false); err == nil {
		t.Error("an arm that does not exist")
	}
	if _, err := OpenReader(ctx, openRead(t, false), t.TempDir(), ArmA2, false); err == nil {
		t.Error("a directory with no manifest")
	}
}

func TestManifestsAreStrict(t *testing.T) {
	dir, m := copyBuild(t)
	good, _ := json.Marshal(m)
	for name, edit := range map[string]func(map[string]any){
		"an unknown field": func(x map[string]any) { x["Extra"] = 1 },
		"another format":   func(x map[string]any) { x["Format"] = 2 },
		"a path that is not the role's": func(x map[string]any) {
			x["Files"].([]any)[1].(map[string]any)["Path"] = "elsewhere.parquet"
		},
	} {
		var x map[string]any
		_ = json.Unmarshal(good, &x)
		edit(x)
		b, _ := json.Marshal(x)
		mustf(t, os.WriteFile(filepath.Join(dir, ManifestName), b, 0o644), "write")
		if _, err := ReadManifest(dir); err == nil {
			t.Errorf("%s was accepted", name)
		}
	}
}

// TestBatchedReadsAreOfOneSnapshotInOrder: a batch is the neighbors of each entity in
// the order and with the repeats it was given, empty for an entity with no neighbor, in
// every arm and as of the newest token and an old one.
func TestBatchedReadsAreOfOneSnapshotInOrder(t *testing.T) {
	dir, plan, _ := sharedBuild(t)
	var fps []identity.Fingerprint
	for _, q := range plan.Queries {
		if q.Op == runner.OpBatch && q.Dir == engine.Reverse && len(q.Fps) > 3 {
			fps = q.Fps
			break
		}
	}
	if len(fps) == 0 {
		t.Fatal("no batch to read")
	}
	stranger := mustFP(t, catalog.K8sNode, catalog.K8sNodeUID, "nobody")
	ask := slices.Concat(fps[:3], []identity.Fingerprint{stranger}, fps[1:2], fps[:1])
	for _, arm := range Arms {
		r, err := OpenReader(context.Background(), openRead(t, false), dir, arm, false)
		mustf(t, err, "open")
		for _, sc := range []engine.Scope{engine.Current(catalog.L2), {Layer: catalog.L2, AsOf: plan.Stream.OldToken}} {
			got, err := r.NeighborsBatch(ask, engine.Reverse, plan.Stream.End, sc)
			mustf(t, err, "batch")
			if len(got) != len(ask) {
				t.Fatalf("%s: %d lists for %d entities", arm, len(got), len(ask))
			}
			for i, fp := range ask {
				one, err := r.Neighbors(fp, engine.Reverse, plan.Stream.End, sc)
				mustf(t, err, "one")
				if !slices.Equal(got[i], one) {
					t.Errorf("%s: entity %d: the batch has %v, a read of it alone %v", arm, i, got[i], one)
				}
			}
			if len(got[3]) != 0 {
				t.Errorf("%s: an entity nothing is written about has neighbors %v", arm, got[3])
			}
		}
		if got, err := r.NeighborsBatch(nil, engine.Reverse, plan.Stream.End, engine.Current(catalog.L2)); err != nil || len(got) != 0 {
			t.Errorf("%s: an empty batch: %v, %v", arm, got, err)
		}
		// An instant before the first possible one finds nothing, and so does a batch.
		before := time.Unix(-1<<40, 0)
		if got, err := r.Neighbors(fps[0], engine.Reverse, before, engine.Current(catalog.L2)); err != nil || len(got) != 0 {
			t.Errorf("%s: before 1970: %v, %v", arm, got, err)
		}
		if ok, err := r.Alive(fps[0], before, engine.Current(catalog.L2)); err != nil || ok {
			t.Errorf("%s: alive before 1970: %v, %v", arm, ok, err)
		}
		if _, err := r.Neighbors(fps[0], engine.Reverse, plan.Stream.End, engine.Scope{}); !errors.Is(err, engine.ErrInvalid) {
			t.Errorf("%s: a scope with no layer: %v", arm, err)
		}
		r.Close()
	}
}
