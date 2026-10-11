//go:build unix

package datadir

import (
	"encoding/json"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"
)

const markerV1 = "toposhift data directory, layout 1\n"

// quiet answers for a machine with no temp directory worth a warning and no
// network filesystem.
func quiet() hooks {
	return hooks{
		networkFS: func(string) (string, error) { return "", nil },
		tempRoots: func() []string { return nil },
		now:       time.Now,
	}
}

func mode(t *testing.T, path string) fs.FileMode {
	t.Helper()
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	return info.Mode() & (fs.ModePerm | fs.ModeSticky)
}

func openAt(t *testing.T, dir string) *Dir {
	t.Helper()
	d, err := Open(Result{Dir: dir})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = d.Close() })
	return d
}

func TestOpenCreatesTheLayoutAt0700(t *testing.T) {
	for _, mask := range []int{0, 0o022, 0o277} {
		t.Run(fmt.Sprintf("umask %04o", mask), func(t *testing.T) {
			base := t.TempDir() // made before the umask, which may forbid it
			setUmask(t, mask)
			dir := filepath.Join(base, "a", "b", "data")
			d := openAt(t, dir)

			for _, p := range []string{filepath.Join(base, "a"), filepath.Join(base, "a", "b"), dir, d.Store, d.Bus, d.Meta} {
				if got := mode(t, p); got != 0o700 {
					t.Errorf("%s has mode %04o, want 0700", p, got)
				}
			}
			if d.Store != filepath.Join(dir, "store") || d.Bus != filepath.Join(dir, "bus") || d.Meta != filepath.Join(dir, "meta") {
				t.Errorf("paths = %q %q %q", d.Store, d.Bus, d.Meta)
			}
			b, err := os.ReadFile(filepath.Join(d.Meta, "format"))
			if err != nil || string(b) != markerV1 {
				t.Errorf("meta/format = %q, %v; want %q", b, err, markerV1)
			}
		})
	}
}

func TestOpenWritesTheHolderRecord(t *testing.T) {
	d := openAt(t, filepath.Join(t.TempDir(), "data"))
	b, err := os.ReadFile(filepath.Join(d.Meta, "holder"))
	if err != nil {
		t.Fatal(err)
	}
	var h holder
	if err := json.Unmarshal(b, &h); err != nil {
		t.Fatalf("holder %q: %v", b, err)
	}
	host, _ := os.Hostname()
	if h.PID != os.Getpid() || h.Host != host {
		t.Errorf("holder = %+v, want pid %d on %q", h, os.Getpid(), host)
	}
	if _, err := time.Parse(time.RFC3339, h.Started); err != nil {
		t.Errorf("started %q is not RFC 3339: %v", h.Started, err)
	}
}

func TestOpenInitialisesAnExistingEmptyDirectoryAndLeavesItsMode(t *testing.T) {
	dir := t.TempDir()
	if err := os.Chmod(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	d := openAt(t, dir)
	if got := mode(t, dir); got != 0o755 {
		t.Errorf("existing directory has mode %04o after Open, want 0755", got)
	}
	for _, p := range []string{d.Store, d.Bus, d.Meta} {
		if got := mode(t, p); got != 0o700 {
			t.Errorf("%s has mode %04o, want 0700", p, got)
		}
	}
	if b, err := os.ReadFile(filepath.Join(d.Meta, "format")); err != nil || string(b) != markerV1 {
		t.Errorf("marker = %q, %v", b, err)
	}
}

func TestOpenRefusesANonEmptyDirectoryWithoutAMarker(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "notes.txt"), []byte("mine"), 0o600); err != nil {
		t.Fatal(err)
	}
	_, err := Open(Result{Dir: dir})
	want := dir + " is not empty and is not a toposhift data directory"
	if err == nil || !strings.Contains(err.Error(), want) {
		t.Fatalf("Open = %v, want an error containing %q", err, want)
	}
	entries, _ := os.ReadDir(dir)
	if len(entries) != 1 {
		t.Errorf("a refused directory was written to: %v", entries)
	}
}

func TestOpenRefusesAnotherLayoutOfTheSameNames(t *testing.T) {
	// A directory that has a meta/ of its own, but not ours, is somebody's.
	dir := t.TempDir()
	if err := os.MkdirAll(filepath.Join(dir, "meta"), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "meta", "other.json"), []byte("{}"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := Open(Result{Dir: dir}); err == nil || !strings.Contains(err.Error(), "not a toposhift data directory") {
		t.Fatalf("Open = %v, want a refusal", err)
	}
}

func TestOpenRefusesANewerLayout(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "data")
	d := openAt(t, dir)
	if err := d.Close(); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "meta", "format"), []byte("toposhift data directory, layout 2\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	_, err := Open(Result{Dir: dir})
	if err == nil {
		t.Fatal("a layout-2 directory was opened")
	}
	for _, want := range []string{"layout 2", "layout 1"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error %q does not name %q", err, want)
		}
	}
	// It was refused before the lock: another Open is not told it is in use.
	if strings.Contains(err.Error(), "in use") {
		t.Errorf("error %q reports the directory in use", err)
	}
}

func TestOpenRefusesAMarkerItCannotRead(t *testing.T) {
	for _, content := range []string{"", "layout 1\n", markerPrefix + "x\n", markerPrefix + "0\n", markerPrefix + "-1\n"} {
		dir := t.TempDir()
		if err := os.MkdirAll(filepath.Join(dir, "meta"), 0o700); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(dir, "meta", "format"), []byte(content), 0o600); err != nil {
			t.Fatal(err)
		}
		if _, err := Open(Result{Dir: dir}); err == nil || !strings.Contains(err.Error(), "layout marker") {
			t.Errorf("marker %q: Open = %v, want a refusal", content, err)
		}
	}
}

func TestOpenReopensAndFinishesAnInterruptedInitialisation(t *testing.T) {
	dir := t.TempDir()
	for _, p := range []string{"store", "bus", "meta"} {
		if err := os.Mkdir(filepath.Join(dir, p), 0o700); err != nil {
			t.Fatal(err)
		}
	}
	if err := os.WriteFile(filepath.Join(dir, "meta", "lock"), nil, 0o600); err != nil {
		t.Fatal(err)
	}
	d := openAt(t, dir)
	if b, err := os.ReadFile(filepath.Join(d.Meta, "format")); err != nil || string(b) != markerV1 {
		t.Errorf("marker = %q, %v", b, err)
	}
}

func TestOpenCreatesAMissingPartOfAMarkedDirectory(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "data")
	d := openAt(t, dir)
	if err := d.Close(); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(filepath.Join(dir, "bus")); err != nil {
		t.Fatal(err)
	}
	d = openAt(t, dir)
	if got := mode(t, d.Bus); got != 0o700 {
		t.Errorf("bus has mode %04o, want 0700", got)
	}
}

func TestOpenRefusesWhatIsNotADirectory(t *testing.T) {
	file := filepath.Join(t.TempDir(), "file")
	if err := os.WriteFile(file, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	for _, dir := range []string{file, filepath.Join(file, "below")} {
		if _, err := Open(Result{Dir: dir}); err == nil {
			t.Errorf("Open(%s) succeeded", dir)
		}
	}
	if _, err := Open(Result{Dir: "relative"}); err == nil {
		t.Error("Open of a relative path succeeded")
	}
	if _, err := Open(Result{}); err == nil {
		t.Error("Open of no directory succeeded")
	}
}

func TestSecondOpenIsRefusedUntilTheFirstCloses(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "data")
	first, err := Open(Result{Dir: dir})
	if err != nil {
		t.Fatal(err)
	}

	_, err = Open(Result{Dir: dir})
	if err == nil {
		t.Fatal("a second Open of a held directory succeeded")
	}
	host, _ := os.Hostname()
	for _, want := range []string{"data directory " + dir + " is in use by", fmt.Sprintf("pid %d", os.Getpid()), "on " + host, "since "} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error %q does not contain %q", err, want)
		}
	}

	if err := first.Close(); err != nil {
		t.Fatal(err)
	}
	if err := first.Close(); err != nil {
		t.Errorf("second Close = %v, want nil", err)
	}
	second, err := Open(Result{Dir: dir})
	if err != nil {
		t.Fatalf("Open after Close: %v", err)
	}
	if err := second.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestARefusedSecondOpenKeepsTheFirstHoldingTheLock(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "data")
	openAt(t, dir)
	for range 2 {
		if _, err := Open(Result{Dir: dir}); err == nil {
			t.Fatal("Open of a held directory succeeded")
		}
	}
}

func TestInUseWithoutAReadableHolderRecord(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "data")
	d := openAt(t, dir)
	if err := os.Remove(filepath.Join(d.Meta, "holder")); err != nil {
		t.Fatal(err)
	}
	_, err := Open(Result{Dir: dir})
	if err == nil || !strings.Contains(err.Error(), "in use by another instance") {
		t.Fatalf("Open = %v, want a refusal that says another instance holds it", err)
	}
}

func TestWarnings(t *testing.T) {
	t.Run("temp directory", func(t *testing.T) {
		d := openAt(t, filepath.Join(t.TempDir(), "data"))
		if len(d.Warnings) != 1 || !strings.Contains(d.Warnings[0], "data may be lost at reboot") {
			t.Fatalf("warnings = %q, want the temp warning", d.Warnings)
		}
	})
	t.Run("outside the temp directory", func(t *testing.T) {
		d, err := open(Result{Dir: filepath.Join(t.TempDir(), "data")}, quiet())
		if err != nil {
			t.Fatal(err)
		}
		defer d.Close()
		if len(d.Warnings) != 0 {
			t.Errorf("warnings = %q, want none", d.Warnings)
		}
	})
	t.Run("temp directory reached through a symlink", func(t *testing.T) {
		root, err := filepath.EvalSymlinks(t.TempDir())
		if err != nil {
			t.Fatal(err)
		}
		link := filepath.Join(t.TempDir(), "link")
		if err := os.Symlink(root, link); err != nil {
			t.Fatal(err)
		}
		h := quiet()
		h.tempRoots = func() []string { return []string{link} }
		d, err := open(Result{Dir: filepath.Join(root, "data")}, h)
		if err != nil {
			t.Fatal(err)
		}
		defer d.Close()
		if len(d.Warnings) != 1 || !strings.Contains(d.Warnings[0], "temp directory") {
			t.Errorf("warnings = %q, want the temp warning", d.Warnings)
		}
	})
	t.Run("a sibling of the temp directory is not inside it", func(t *testing.T) {
		base := t.TempDir()
		h := quiet()
		h.tempRoots = func() []string { return []string{filepath.Join(base, "tmp")} }
		d, err := open(Result{Dir: filepath.Join(base, "tmp2")}, h)
		if err != nil {
			t.Fatal(err)
		}
		defer d.Close()
		if len(d.Warnings) != 0 {
			t.Errorf("warnings = %q, want none", d.Warnings)
		}
	})

	worldWritable := []struct {
		name string
		mode fs.FileMode
		warn bool
	}{
		{"world-writable without sticky", 0o777, true},
		{"world-writable with sticky", 0o777 | fs.ModeSticky, false},
		{"group-writable only", 0o770, false},
		{"private", 0o700, false},
	}
	for _, tt := range worldWritable {
		t.Run(tt.name, func(t *testing.T) {
			dir := t.TempDir()
			if err := os.Chmod(dir, tt.mode); err != nil {
				t.Fatal(err)
			}
			d, err := open(Result{Dir: dir}, quiet())
			if err != nil {
				t.Fatal(err)
			}
			defer d.Close()
			if got := mode(t, dir); got != tt.mode {
				t.Errorf("mode changed to %v from %v", got, tt.mode)
			}
			if got := len(d.Warnings) == 1 && strings.Contains(d.Warnings[0], "writable by every user"); got != tt.warn || (!tt.warn && len(d.Warnings) != 0) {
				t.Errorf("warnings = %q, want the world-writable warning: %v", d.Warnings, tt.warn)
			}
		})
	}

	t.Run("network filesystem", func(t *testing.T) {
		h := quiet()
		h.networkFS = func(string) (string, error) { return "nfs", nil }
		d, err := open(Result{Dir: filepath.Join(t.TempDir(), "data")}, h)
		if err != nil {
			t.Fatal(err)
		}
		defer d.Close()
		if len(d.Warnings) != 1 || !strings.Contains(d.Warnings[0], "network filesystem (nfs)") {
			t.Errorf("warnings = %q, want the network warning", d.Warnings)
		}
	})
	t.Run("a filesystem that cannot be asked is no warning", func(t *testing.T) {
		h := quiet()
		h.networkFS = func(string) (string, error) { return "", syscall.EIO }
		d, err := open(Result{Dir: filepath.Join(t.TempDir(), "data")}, h)
		if err != nil {
			t.Fatal(err)
		}
		defer d.Close()
		if len(d.Warnings) != 0 {
			t.Errorf("warnings = %q, want none", d.Warnings)
		}
	})
}

func TestAccessErrorNamesTheDirectoryAndBothUIDs(t *testing.T) {
	if os.Getuid() == 0 {
		t.Skip("root can create in a read-only directory")
	}
	locked := filepath.Join(t.TempDir(), "locked")
	if err := os.Mkdir(locked, 0o500); err != nil {
		t.Fatal(err)
	}
	dir := filepath.Join(locked, "below", "data")
	_, err := Open(Result{Dir: dir})
	if err == nil {
		t.Fatal("Open under a read-only directory succeeded")
	}
	uid := os.Getuid()
	for _, want := range []string{dir, locked + " is owned by uid " + fmt.Sprint(uid), "running as uid " + fmt.Sprint(uid)} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error %q does not contain %q", err, want)
		}
	}
	if got := mode(t, locked); got != 0o500 {
		t.Errorf("the directory's mode changed to %04o", got)
	}
}

func TestTempRootsIncludeTheSystemTemp(t *testing.T) {
	roots := tempRoots()
	found := false
	for _, r := range roots {
		if r == os.TempDir() {
			found = true
		}
	}
	if !found {
		t.Errorf("tempRoots() = %q, missing os.TempDir() = %q", roots, os.TempDir())
	}
}
