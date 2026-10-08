package localdir

import (
	"bytes"
	"crypto/sha256"
	"errors"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"slices"
	"strings"
	"testing"

	"github.com/lotannauo/toposhift/internal/segment"
)

var errInjected = errors.New("injected fault")

// recorder wraps the three durability operations of d so that it notes each
// call, and lets a test make one of them fail.
type recorder struct {
	events []string

	failSyncFile bool
	failLink     bool
	// failSyncDir, when set, is called with the slash path of each directory
	// flushed; a non-nil error fails the flush.
	failSyncDir func(dir string) error
}

func (rec *recorder) install(d *Dir) { d.hooks = recordingHooks(rec) }

func recordingHooks(rec *recorder) hooks {
	return hooks{
		syncFile: func(f *os.File) error {
			rec.events = append(rec.events, "syncFile")
			if rec.failSyncFile {
				return errInjected
			}
			return f.Sync()
		},
		link: func(root *os.Root, oldname, newname string) error {
			rec.events = append(rec.events, "link")
			if rec.failLink {
				return errInjected
			}
			return root.Link(oldname, newname)
		},
		syncDir: func(root *os.Root, dir string) error {
			dir = filepath.ToSlash(dir)
			rec.events = append(rec.events, "syncDir "+dir)
			if rec.failSyncDir != nil {
				if err := rec.failSyncDir(dir); err != nil {
					return err
				}
			}
			return syncDir(root, filepath.FromSlash(dir))
		},
	}
}

// The recording stand-ins hide the real operations; the real ones must still be
// what a Dir uses.
func TestDefaultHooksAreTheRealOperations(t *testing.T) {
	name := func(f any) string { return runtime.FuncForPC(reflect.ValueOf(f).Pointer()).Name() }
	h := defaultHooks()
	for _, tc := range []struct {
		got  any
		want string
	}{
		{h.syncFile, "os.(*File).Sync"},
		{h.link, "os.(*Root).Link"},
		{h.syncDir, "github.com/lotannauo/toposhift/internal/segment/localdir.syncDir"},
	} {
		if got := name(tc.got); got != tc.want {
			t.Errorf("hook is %s, want %s", got, tc.want)
		}
	}
	root := t.TempDir()
	r, err := os.OpenRoot(root)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	if err := syncDir(r, "."); err != nil {
		t.Errorf("syncDir of the root: %v", err)
	}
	if runtime.GOOS != "windows" {
		if err := syncDir(r, "missing"); err == nil {
			t.Error("syncDir of a missing directory succeeded")
		}
	}
}

func newRecorded(t *testing.T) (*Dir, *recorder, string) {
	t.Helper()
	root := t.TempDir()
	d, err := New(root)
	if err != nil {
		t.Fatal(err)
	}
	rec := &recorder{}
	rec.install(d)
	return d, rec, root
}

// entries lists everything under root, directories included.
func entries(t *testing.T, root string) []string {
	t.Helper()
	var out []string
	err := filepath.WalkDir(root, func(p string, _ fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if p != root {
			rel, _ := filepath.Rel(root, p)
			out = append(out, filepath.ToSlash(rel))
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	return out
}

func put(t *testing.T, d *Dir, name, content string) (segment.Info, error) {
	t.Helper()
	return d.Put(t.Context(), name, strings.NewReader(content), int64(len(content)))
}

// The file is flushed before it is linked, and the directories are flushed
// after the link: the one it sits in, then each one above it up to the root,
// including every directory Put had to create.
func TestPutFlushesFileThenLinksThenDirectoriesUpToTheRoot(t *testing.T) {
	d, rec, _ := newRecorded(t)
	tests := []struct {
		name string
		want []string
	}{
		{"a/b/c", []string{"syncFile", "link", "syncDir a/b", "syncDir a", "syncDir ."}},
		{"top", []string{"syncFile", "link", "syncDir ."}},
		// The directories exist now; they are flushed again, because a
		// concurrent Put may have created them without flushing their entries.
		{"a/b/d", []string{"syncFile", "link", "syncDir a/b", "syncDir a", "syncDir ."}},
		{"a/e", []string{"syncFile", "link", "syncDir a", "syncDir ."}},
	}
	for _, tc := range tests {
		rec.events = nil
		if _, err := put(t, d, tc.name, "x"); err != nil {
			t.Fatalf("Put(%q): %v", tc.name, err)
		}
		if !slices.Equal(rec.events, tc.want) {
			t.Errorf("Put(%q) did %v, want %v", tc.name, rec.events, tc.want)
		}
	}

	rec.events = nil
	if err := d.Delete(t.Context(), "a/b/c"); err != nil {
		t.Fatal(err)
	}
	if want := []string{"syncDir a/b"}; !slices.Equal(rec.events, want) {
		t.Errorf("Delete did %v, want %v", rec.events, want)
	}
	rec.events = nil
	if err := d.Delete(t.Context(), "a/b/c"); err != nil {
		t.Fatal(err)
	}
	if len(rec.events) != 0 {
		t.Errorf("Delete of a missing name did %v, want nothing", rec.events)
	}
}

func TestRefusedPutFlushesNothingAfterTheLink(t *testing.T) {
	d, rec, root := newRecorded(t)
	if _, err := put(t, d, "x/y", "first"); err != nil {
		t.Fatal(err)
	}
	rec.events = nil
	if _, err := put(t, d, "x/y", "second"); !errors.Is(err, segment.ErrExists) {
		t.Fatalf("second Put = %v, want ErrExists", err)
	}
	if want := []string{"syncFile", "link"}; !slices.Equal(rec.events, want) {
		t.Errorf("refused Put did %v, want %v", rec.events, want)
	}
	if got, want := entries(t, root), []string{"x", "x/y"}; !slices.Equal(got, want) {
		t.Errorf("root holds %v, want %v", got, want)
	}
}

func TestFailedFileFlushStoresNothing(t *testing.T) {
	d, rec, root := newRecorded(t)
	rec.failSyncFile = true
	if _, err := put(t, d, "a/b", "x"); !errors.Is(err, errInjected) {
		t.Fatalf("Put = %v, want the injected error", err)
	}
	if slices.Contains(rec.events, "link") {
		t.Errorf("the file was linked although its flush failed: %v", rec.events)
	}
	for _, e := range entries(t, root) {
		if strings.Contains(e, "tmp-") || e == "a/b" {
			t.Errorf("%q left behind", e)
		}
	}
}

func TestFailedLinkStoresNothing(t *testing.T) {
	d, rec, root := newRecorded(t)
	rec.failLink = true
	if _, err := put(t, d, "a/b", "x"); !errors.Is(err, errInjected) {
		t.Fatalf("Put = %v, want the injected error", err)
	}
	if _, err := d.Stat(t.Context(), "a/b"); !errors.Is(err, segment.ErrNotFound) {
		t.Errorf("Stat = %v, want ErrNotFound", err)
	}
	for _, e := range entries(t, root) {
		if strings.Contains(e, "tmp-") {
			t.Errorf("%q left behind", e)
		}
	}
}

// When a directory cannot be flushed, the name is removed again if it is still
// the file this Put wrote, wherever up the chain the flush failed.
func TestFailedDirectoryFlushLeavesNothingVisible(t *testing.T) {
	for _, failing := range []string{"a/b", "a", "."} {
		t.Run("fails at "+failing, func(t *testing.T) {
			d, rec, root := newRecorded(t)
			rec.failSyncDir = func(dir string) error {
				if dir == failing {
					return errInjected
				}
				return nil
			}
			_, err := put(t, d, "a/b/c", "x")
			if !errors.Is(err, errInjected) {
				t.Fatalf("Put = %v, want the injected error", err)
			}
			if errors.Is(err, errOutcomeUnknown) {
				t.Errorf("Put = %v: the name was still ours, so the outcome is known", err)
			}
			if _, err := d.Stat(t.Context(), "a/b/c"); !errors.Is(err, segment.ErrNotFound) {
				t.Errorf("Stat = %v, want ErrNotFound", err)
			}
			for _, e := range entries(t, root) {
				if strings.Contains(e, "tmp-") || e == "a/b/c" {
					t.Errorf("%q left behind", e)
				}
			}
			// The name can be written once the fault is gone.
			rec.failSyncDir = nil
			if _, err := put(t, d, "a/b/c", "y"); err != nil {
				t.Errorf("Put after the fault: %v", err)
			}
		})
	}
}

// Between this Put's link and its removal of the name, another writer may
// delete the name and write it again. Put must leave that segment alone and
// say that it does not know the outcome.
func TestFailedDirectoryFlushLeavesAConcurrentWritersSegment(t *testing.T) {
	d, rec, root := newRecorded(t)
	other, err := New(root)
	if err != nil {
		t.Fatal(err)
	}
	rec.failSyncDir = func(dir string) error {
		if dir != "a" {
			return nil
		}
		ctx := t.Context()
		if err := other.Delete(ctx, "a/seg"); err != nil {
			t.Error(err)
		}
		if _, err := other.Put(ctx, "a/seg", strings.NewReader("theirs"), 6); err != nil {
			t.Error(err)
		}
		return errInjected
	}
	_, err = put(t, d, "a/seg", "mine")
	if !errors.Is(err, errOutcomeUnknown) || !errors.Is(err, errInjected) {
		t.Fatalf("Put = %v, want an error wrapping the unknown outcome and the injected fault", err)
	}
	info, err := other.Stat(t.Context(), "a/seg")
	if err != nil {
		t.Fatalf("the concurrent writer's segment is gone: %v", err)
	}
	if want := sha256.Sum256([]byte("theirs")); info.SHA256 != want || info.Size != 6 {
		t.Errorf("the segment is %+v, want the concurrent writer's", info)
	}
	for _, e := range entries(t, root) {
		if strings.Contains(e, "tmp-") {
			t.Errorf("%q left behind", e)
		}
	}
}

func TestSizeLimitIsExactAndNeverTrustsTheAnnouncedSize(t *testing.T) {
	d, _, root := newRecorded(t)
	d.maxSize = 1000
	data := bytes.Repeat([]byte{7}, 1001)
	sum := func(b []byte) [32]byte { return sha256.Sum256(b) }

	for _, size := range []int64{1000, -1} {
		info, err := d.Put(t.Context(), "exact", bytes.NewReader(data[:1000]), size)
		if err != nil {
			t.Fatalf("Put of exactly the limit, announced %d: %v", size, err)
		}
		if info.Size != 1000 || info.SHA256 != sum(data[:1000]) {
			t.Errorf("Put returned %+v", info)
		}
		if err := d.Delete(t.Context(), "exact"); err != nil {
			t.Fatal(err)
		}
	}
	for _, size := range []int64{1001, -1} {
		_, err := d.Put(t.Context(), "over", bytes.NewReader(data), size)
		if !errors.Is(err, segment.ErrTooLarge) {
			t.Errorf("Put of limit+1 announced %d = %v, want ErrTooLarge", size, err)
		}
	}
	// A size above the limit is refused before a byte is read.
	untouched := readerFunc(func([]byte) (int, error) {
		t.Error("Put read from its reader although the announced size is over the limit")
		return 0, io.EOF
	})
	if _, err := d.Put(t.Context(), "over", untouched, 1001); !errors.Is(err, segment.ErrTooLarge) {
		t.Errorf("Put = %v, want ErrTooLarge", err)
	}
	// A reader that announces less than it yields is stopped by the size, one
	// that announces little and yields a lot by the limit as well.
	if _, err := d.Put(t.Context(), "over", bytes.NewReader(data), 10); err == nil {
		t.Error("Put of 1001 bytes announced as 10 succeeded")
	}
	// An endless reader announced as 10 bytes is stopped by the announcement,
	// long before the limit.
	if _, err := d.Put(t.Context(), "over", zeroes{}, 10); err == nil || errors.Is(err, segment.ErrTooLarge) {
		t.Errorf("Put of an endless reader announced as 10 bytes = %v, want a size mismatch", err)
	}
	big := io.LimitReader(zeroes{}, 1<<20)
	if _, err := d.Put(t.Context(), "over", big, -1); !errors.Is(err, segment.ErrTooLarge) {
		t.Errorf("Put of an endless reader = %v, want ErrTooLarge", err)
	}
	if got := entries(t, root); len(got) != 0 {
		t.Errorf("root holds %v after refused Puts, want nothing", got)
	}
}

type zeroes struct{}

func (zeroes) Read(p []byte) (int, error) { clear(p); return len(p), nil }

// A symbolic link inside the root that leads outside it must not let a name
// leave the root.
func TestSymbolicLinkOutOfTheRootIsNotFollowed(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("symbolic links need privileges on Windows")
	}
	outside := t.TempDir()
	root := t.TempDir()
	d, err := New(root)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(outside, filepath.Join(root, "esc")); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(outside, "secret"), []byte("s"), 0o644); err != nil {
		t.Fatal(err)
	}

	if _, err := put(t, d, "esc/x", "data"); err == nil {
		t.Error("Put through a link out of the root succeeded")
	}
	if got := entries(t, outside); !slices.Equal(got, []string{"secret"}) {
		t.Errorf("the directory outside the root holds %v, want only its own file", got)
	}
	if rc, err := d.Open(t.Context(), "esc/secret"); err == nil {
		rc.Close()
		t.Error("Open through a link out of the root succeeded")
	}
	if _, err := d.Stat(t.Context(), "esc/secret"); err == nil {
		t.Error("Stat through a link out of the root succeeded")
	}
	for info, err := range d.List(t.Context(), "") {
		if err != nil {
			t.Fatal(err)
		}
		t.Errorf("List yields %q through a link out of the root", info.Name)
	}
	// A link to a file inside the root is not a segment either.
	if _, err := put(t, d, "real", "x"); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink("real", filepath.Join(root, "alias")); err != nil {
		t.Fatal(err)
	}
	if _, err := d.Stat(t.Context(), "alias"); !errors.Is(err, segment.ErrNotFound) {
		t.Errorf("Stat of a link = %v, want ErrNotFound", err)
	}
	if rc, err := d.Open(t.Context(), "alias"); !errors.Is(err, segment.ErrNotFound) {
		if rc != nil {
			rc.Close()
		}
		t.Errorf("Open of a link = %v, want ErrNotFound", err)
	}
}

type readerFunc func([]byte) (int, error)

func (f readerFunc) Read(p []byte) (int, error) { return f(p) }

// New flushes the parents of the directories it creates, nearest first, and
// nothing when the root exists; the link is its probe for hard links.
func TestNewFlushesTheDirectoriesItCreated(t *testing.T) {
	base := t.TempDir()
	rec := &recorder{}
	h := recordingHooks(rec)

	root := filepath.Join(base, "x", "y", "store")
	d, err := newDir(root, h)
	if err != nil {
		t.Fatal(err)
	}
	if want := []string{"syncDir x/y", "syncDir x", "syncDir .", "link"}; !slices.Equal(rec.events, want) {
		t.Errorf("New of a root with two new parents did %v, want %v", rec.events, want)
	}
	_ = d

	rec.events = nil
	if _, err := newDir(root, h); err != nil {
		t.Fatal(err)
	}
	if want := []string{"link"}; !slices.Equal(rec.events, want) {
		t.Errorf("New of an existing root did %v, want only the probe %v", rec.events, want)
	}

	rec.events = nil
	if _, err := newDir(filepath.Join(base, "z"), h); err != nil {
		t.Fatal(err)
	}
	if want := []string{"syncDir .", "link"}; !slices.Equal(rec.events, want) {
		t.Errorf("New of a new root in an existing directory did %v, want %v", rec.events, want)
	}
}

func TestNewFailsWhenAFlushFails(t *testing.T) {
	rec := &recorder{failSyncDir: func(string) error { return errInjected }}
	_, err := newDir(filepath.Join(t.TempDir(), "a", "store"), recordingHooks(rec))
	if !errors.Is(err, errInjected) {
		t.Errorf("New = %v, want the injected error", err)
	}
}

// A file system without hard links is reported when the store is opened, with
// a clear error, and the probe leaves nothing behind.
func TestNewProbesForHardLinks(t *testing.T) {
	root := t.TempDir()
	rec := &recorder{failLink: true}
	_, err := newDir(root, recordingHooks(rec))
	if !errors.Is(err, errNoHardLinks) || !errors.Is(err, errInjected) {
		t.Fatalf("New = %v, want an error saying the file system lacks hard links, wrapping the cause", err)
	}
	if got := entries(t, root); len(got) != 0 {
		t.Errorf("the probe left %v", got)
	}

	// On a file system with links the probe passes, runs once, and leaves nothing.
	rec = &recorder{}
	if _, err := newDir(root, recordingHooks(rec)); err != nil {
		t.Fatal(err)
	}
	if n := strings.Count(strings.Join(rec.events, ","), "link"); n != 1 {
		t.Errorf("New linked %d times, want once", n)
	}
	if got := entries(t, root); len(got) != 0 {
		t.Errorf("the probe left %v", got)
	}
}
