package localdir_test

import (
	"bytes"
	"context"
	"errors"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"strings"
	"testing"

	"github.com/lotannauo/toposhift/internal/segment"
	"github.com/lotannauo/toposhift/internal/segment/localdir"
	"github.com/lotannauo/toposhift/internal/segment/segmenttest"
)

func open(t *testing.T) *localdir.Dir {
	t.Helper()
	d, err := localdir.New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	return d
}

// files lists every entry under root, directories included, as slash paths.
func files(t *testing.T, root string) []string {
	t.Helper()
	var out []string
	err := filepath.WalkDir(root, func(path string, e fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if path != root {
			rel, _ := filepath.Rel(root, path)
			out = append(out, filepath.ToSlash(rel))
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	return out
}

func listed(t *testing.T, s segment.Store, prefix string) []string {
	t.Helper()
	var names []string
	for info, err := range s.List(t.Context(), prefix) {
		if err != nil {
			t.Fatal(err)
		}
		names = append(names, info.Name)
	}
	return names
}

func TestSuite(t *testing.T) {
	segmenttest.Run(t, func(t *testing.T) segment.Store { return open(t) })
}

func TestNewRefusesRelativeRoot(t *testing.T) {
	for _, root := range []string{"", "relative", "./relative", "../up"} {
		if d, err := localdir.New(root); err == nil {
			t.Errorf("New(%q) = %v, nil; want an error", root, d)
		}
	}
	if entries, err := os.ReadDir("."); err == nil {
		for _, e := range entries {
			if e.Name() == "relative" {
				t.Error("New created a directory for a relative root")
			}
		}
	}
}

func TestNewCreatesRoot(t *testing.T) {
	root := filepath.Join(t.TempDir(), "a", "b")
	if _, err := localdir.New(root); err != nil {
		t.Fatal(err)
	}
	if info, err := os.Stat(root); err != nil || !info.IsDir() {
		t.Fatalf("root not created: %v", err)
	}
	// Opening an existing root is fine.
	if _, err := localdir.New(root); err != nil {
		t.Fatal(err)
	}
}

func TestNewRefusesRootThatIsAFile(t *testing.T) {
	file := filepath.Join(t.TempDir(), "file")
	if err := os.WriteFile(file, nil, 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := localdir.New(file); err == nil {
		t.Error("New on a file succeeded, want an error")
	}
}

func TestLeftoverTemporaryFiles(t *testing.T) {
	root := t.TempDir()
	d, err := localdir.New(root)
	if err != nil {
		t.Fatal(err)
	}
	// What a crash during Put can leave: a temporary file beside the final name.
	if err := os.MkdirAll(filepath.Join(root, "a"), 0o755); err != nil {
		t.Fatal(err)
	}
	for _, leftover := range []string{".seg.tmp-123456", filepath.Join("a", ".b.tmp-42")} {
		if err := os.WriteFile(filepath.Join(root, leftover), []byte("partial"), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	if got := listed(t, d, ""); len(got) != 0 {
		t.Fatalf("List shows %v, want the leftovers hidden", got)
	}
	for _, name := range []string{"seg", "a/b"} {
		if _, err := d.Stat(t.Context(), name); !errors.Is(err, segment.ErrNotFound) {
			t.Errorf("Stat(%q) = %v, want ErrNotFound", name, err)
		}
		if _, err := d.Put(t.Context(), name, strings.NewReader("whole"), int64(len("whole"))); err != nil {
			t.Errorf("Put(%q) blocked by a leftover: %v", name, err)
		}
	}
	if got, want := listed(t, d, ""), []string{"a/b", "seg"}; !slices.Equal(got, want) {
		t.Errorf("List = %v, want %v", got, want)
	}
}

func TestForeignFilesAreNotSegments(t *testing.T) {
	root := t.TempDir()
	d, err := localdir.New(root)
	if err != nil {
		t.Fatal(err)
	}
	// Files no valid name leads to, and a hidden directory.
	if err := os.MkdirAll(filepath.Join(root, ".trash", "inner"), 0o755); err != nil {
		t.Fatal(err)
	}
	for _, f := range []string{"has space", filepath.Join(".trash", "inner", "file"), ".DS_Store"} {
		if err := os.WriteFile(filepath.Join(root, f), []byte("x"), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := d.Put(t.Context(), "real", strings.NewReader("x"), int64(len("x"))); err != nil {
		t.Fatal(err)
	}
	if got, want := listed(t, d, ""), []string{"real"}; !slices.Equal(got, want) {
		t.Errorf("List = %v, want %v", got, want)
	}
}

func TestNoTemporaryFileRemains(t *testing.T) {
	root := t.TempDir()
	d, err := localdir.New(root)
	if err != nil {
		t.Fatal(err)
	}
	ctx := t.Context()

	if _, err := d.Put(ctx, "dir/ok", strings.NewReader("ok"), int64(len("ok"))); err != nil {
		t.Fatal(err)
	}
	if got, want := files(t, root), []string{"dir", "dir/ok"}; !slices.Equal(got, want) {
		t.Fatalf("after a Put the root holds %v, want %v", got, want)
	}

	// A refused Put, a failing reader and a cancelled context leave no file.
	if _, err := d.Put(ctx, "dir/ok", strings.NewReader("again"), int64(len("again"))); !errors.Is(err, segment.ErrExists) {
		t.Fatalf("second Put = %v, want ErrExists", err)
	}
	boom := errors.New("boom")
	failing := io.MultiReader(bytes.NewReader(make([]byte, 200<<10)), errReader{boom})
	if _, err := d.Put(ctx, "dir/bad", failing, -1); !errors.Is(err, boom) {
		t.Fatalf("Put with a failing reader = %v, want it to wrap the reader's error", err)
	}
	if got, want := files(t, root), []string{"dir", "dir/ok"}; !slices.Equal(got, want) {
		t.Errorf("after the failures the root holds %v, want %v", got, want)
	}
}

type errReader struct{ err error }

func (r errReader) Read([]byte) (int, error) { return 0, r.err }

func TestFileMode(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("file modes are not meaningful on Windows")
	}
	root := t.TempDir()
	d, err := localdir.New(root)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := d.Put(t.Context(), "m", strings.NewReader("x"), int64(len("x"))); err != nil {
		t.Fatal(err)
	}
	info, err := os.Stat(filepath.Join(root, "m"))
	if err != nil {
		t.Fatal(err)
	}
	if got := info.Mode().Perm(); got != 0o644 {
		t.Errorf("mode = %o, want 644", got)
	}
}

func TestSecondDirOnSameRoot(t *testing.T) {
	root := t.TempDir()
	first, err := localdir.New(root)
	if err != nil {
		t.Fatal(err)
	}
	second, err := localdir.New(root)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := first.Put(t.Context(), "shared/seg", strings.NewReader("written by the first"), int64(len("written by the first"))); err != nil {
		t.Fatal(err)
	}
	rc, err := second.Open(t.Context(), "shared/seg")
	if err != nil {
		t.Fatal(err)
	}
	defer rc.Close()
	got, err := io.ReadAll(rc)
	if err != nil || string(got) != "written by the first" {
		t.Errorf("second Dir read %q (error %v)", got, err)
	}
	if _, err := second.Put(t.Context(), "shared/seg", strings.NewReader("x"), int64(len("x"))); !errors.Is(err, segment.ErrExists) {
		t.Errorf("Put through the second Dir = %v, want ErrExists", err)
	}
	if err := second.Delete(t.Context(), "shared/seg"); err != nil {
		t.Fatal(err)
	}
	if _, err := first.Stat(t.Context(), "shared/seg"); !errors.Is(err, segment.ErrNotFound) {
		t.Errorf("Stat through the first Dir after the second deleted = %v, want ErrNotFound", err)
	}
}

func TestDirectoriesAreNotSegments(t *testing.T) {
	root := t.TempDir()
	d, err := localdir.New(root)
	if err != nil {
		t.Fatal(err)
	}
	ctx := t.Context()
	if _, err := d.Put(ctx, "a/b", strings.NewReader("x"), 1); err != nil {
		t.Fatal(err)
	}
	if rc, err := d.Open(ctx, "a"); !errors.Is(err, segment.ErrNotFound) {
		if rc != nil {
			rc.Close()
		}
		t.Errorf("Open of a directory = %v, want ErrNotFound", err)
	}
	if _, err := d.Stat(ctx, "a"); !errors.Is(err, segment.ErrNotFound) {
		t.Errorf("Stat of a directory = %v, want ErrNotFound", err)
	}
	// Deleting what is not a segment is not an error, and removes nothing: an
	// empty directory must survive a Delete of its name.
	if err := d.Delete(ctx, "a"); err != nil {
		t.Errorf("Delete of a directory = %v, want nil", err)
	}
	if err := d.Delete(ctx, "a/b"); err != nil {
		t.Fatal(err)
	}
	if err := d.Delete(ctx, "a"); err != nil {
		t.Errorf("Delete of an empty directory = %v, want nil", err)
	}
	if info, err := os.Stat(filepath.Join(root, "a")); err != nil || !info.IsDir() {
		t.Errorf("the empty directory is gone after Delete of its name: %v", err)
	}
	if got := listed(t, d, ""); len(got) != 0 {
		t.Errorf("List = %v, want nothing", got)
	}
}

func TestFileAndDirectoryOfOneName(t *testing.T) {
	d := open(t)
	ctx := t.Context()
	if _, err := d.Put(ctx, "a", strings.NewReader("file"), int64(len("file"))); err != nil {
		t.Fatal(err)
	}
	// "a" is a file, so "a/b" has no room, and cannot be found either.
	_, err := d.Put(ctx, "a/b", strings.NewReader("x"), int64(len("x")))
	if err == nil || errors.Is(err, segment.ErrExists) || errors.Is(err, segment.ErrInvalidName) {
		t.Errorf("Put under a file = %v, want a conflict that is neither ErrExists nor ErrInvalidName", err)
	}
	if _, err := d.Stat(ctx, "a/b"); !errors.Is(err, segment.ErrNotFound) {
		t.Errorf("Stat under a file = %v, want ErrNotFound", err)
	}
	if err := d.Delete(ctx, "a/b"); err != nil {
		t.Errorf("Delete under a file = %v, want nil", err)
	}

	if _, err := d.Put(ctx, "c/d", strings.NewReader("x"), int64(len("x"))); err != nil {
		t.Fatal(err)
	}
	_, err = d.Put(ctx, "c", strings.NewReader("x"), int64(len("x")))
	if err == nil || errors.Is(err, segment.ErrExists) || errors.Is(err, segment.ErrInvalidName) {
		t.Errorf("Put over a directory = %v, want a conflict that is neither ErrExists nor ErrInvalidName", err)
	}
	if got, want := listed(t, d, ""), []string{"a", "c/d"}; !slices.Equal(got, want) {
		t.Errorf("List = %v, want %v", got, want)
	}
}

func TestListStopsEarly(t *testing.T) {
	d := open(t)
	for _, name := range []string{"a", "b", "c"} {
		if _, err := d.Put(t.Context(), name, strings.NewReader(name), int64(len(name))); err != nil {
			t.Fatal(err)
		}
	}
	n := 0
	for _, err := range d.List(t.Context(), "") {
		if err != nil {
			t.Fatal(err)
		}
		n++
		break
	}
	if n != 1 {
		t.Errorf("saw %d segments before break, want 1", n)
	}
}

func TestPutRemovesNothingFromSiblings(t *testing.T) {
	root := t.TempDir()
	d, err := localdir.New(root)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, ".keep.tmp-1"), []byte("x"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := d.Put(t.Context(), "keep", strings.NewReader("y"), int64(len("y"))); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(root, ".keep.tmp-1")); err != nil {
		t.Errorf("Put removed another writer's temporary file: %v", err)
	}
}

// observingReader runs hook on its first Read and serves n zero bytes.
type observingReader struct {
	n    int
	read int
	hook func()
}

func (r *observingReader) Read(p []byte) (int, error) {
	if r.hook != nil {
		r.hook()
		r.hook = nil
	}
	if r.read >= r.n {
		return 0, io.EOF
	}
	m := min(len(p), r.n-r.read)
	clear(p[:m])
	r.read += m
	return m, nil
}

// The temporary file must sit beside the final name: a link cannot cross file
// systems, and a root may have other file systems mounted below it.
func TestTemporaryFileIsBesideTheName(t *testing.T) {
	root := t.TempDir()
	d, err := localdir.New(root)
	if err != nil {
		t.Fatal(err)
	}
	var seen []string
	r := &observingReader{n: 1000, hook: func() {
		seen = files(t, root)
	}}
	if _, err := d.Put(t.Context(), "deep/er/name", r, 1000); err != nil {
		t.Fatal(err)
	}
	var temps []string
	for _, f := range seen {
		if strings.Contains(f, ".tmp-") {
			temps = append(temps, f)
		}
	}
	if len(temps) != 1 || filepath.ToSlash(filepath.Dir(temps[0])) != "deep/er" || !strings.HasPrefix(filepath.Base(temps[0]), ".name.tmp-") {
		t.Errorf("temporary files during Put = %v, want one named .name.tmp-* in deep/er", temps)
	}
}

// A cancelled context must stop the copy at the next chunk, not after the
// whole segment has been read.
func TestPutStopsCopyingWhenContextEnds(t *testing.T) {
	d := open(t)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	const total, cancelAt = 64 << 20, 1 << 20
	r := &observingReader{n: total}
	cancelling := readerFunc(func(p []byte) (int, error) {
		if r.read >= cancelAt {
			cancel()
		}
		return r.Read(p)
	})
	_, err := d.Put(ctx, "big", cancelling, total)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("Put = %v, want an error wrapping context.Canceled", err)
	}
	if r.read > cancelAt+(256<<10) {
		t.Errorf("Put read %d bytes after the context ended at %d, want it to stop within a few chunks", r.read, cancelAt)
	}
	if _, err := d.Stat(t.Context(), "big"); !errors.Is(err, segment.ErrNotFound) {
		t.Errorf("Stat = %v, want ErrNotFound", err)
	}
}

type readerFunc func([]byte) (int, error)

func (f readerFunc) Read(p []byte) (int, error) { return f(p) }
