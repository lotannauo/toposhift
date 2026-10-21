package main

import (
	"bytes"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"testing"
)

func TestTheTwoPartsPartitionTheInput(t *testing.T) {
	t.Parallel()
	all := []string{"m/a", "m/b", "m/c", "m/d", "m/e"}
	heavy := []string{"m/d", "m/b"}
	h, err := split(all, heavy, true)
	if err != nil {
		t.Fatal(err)
	}
	r, err := split(all, heavy, false)
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(h, []string{"m/d", "m/b"}) {
		t.Errorf("heavy = %v, want the heavy list in its order", h)
	}
	if !slices.Equal(r, []string{"m/a", "m/c", "m/e"}) {
		t.Errorf("rest = %v, want the others in the input's order", r)
	}
	// Every package is in exactly one part.
	for _, p := range all {
		if n := count(h, p) + count(r, p); n != 1 {
			t.Errorf("%s is in %d parts, want 1", p, n)
		}
	}
	if len(h)+len(r) != len(all) {
		t.Errorf("%d + %d packages, want %d", len(h), len(r), len(all))
	}
}

func count(xs []string, p string) int {
	n := 0
	for _, x := range xs {
		if x == p {
			n++
		}
	}
	return n
}

func TestAStaleOrRepeatedHeavyNameFails(t *testing.T) {
	t.Parallel()
	all := []string{"m/a", "m/b"}
	for _, tc := range []struct {
		name  string
		heavy []string
		want  string
	}{
		{"not in the module", []string{"m/a", "m/gone"}, "m/gone is not in the module's package list"},
		{"listed twice", []string{"m/a", "m/a"}, "m/a is listed twice"},
	} {
		for _, wantHeavy := range []bool{true, false} {
			_, err := split(all, tc.heavy, wantHeavy)
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Errorf("%s (heavy=%v): err = %v, want %q", tc.name, wantHeavy, err, tc.want)
			}
		}
	}
}

func TestAnEmptyHeavyListPutsEverythingInRest(t *testing.T) {
	t.Parallel()
	all := []string{"m/a", "m/b"}
	h, err := split(all, nil, true)
	if err != nil || len(h) != 0 {
		t.Errorf("heavy = %v, %v; want none", h, err)
	}
	r, err := split(all, nil, false)
	if err != nil || !slices.Equal(r, all) {
		t.Errorf("rest = %v, %v; want all", r, err)
	}
}

func TestReadPackagesRefusesWhatIsNotAPackageList(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name, in, want string
	}{
		{"empty", "", "no packages"},
		{"blank line", "m/a\n\nm/b\n", `line 2: "" is not an import path`},
		{"spaces", "m/a\nok  m/b 1.0s\n", "line 2:"},
		{"duplicate", "m/a\nm/b\nm/a\n", "line 3: m/a is listed twice"},
	} {
		_, err := readPackages(strings.NewReader(tc.in))
		if err == nil || !strings.Contains(err.Error(), tc.want) {
			t.Errorf("%s: err = %v, want %q", tc.name, err, tc.want)
		}
	}
	got, err := readPackages(strings.NewReader("m/a\nm/b\n"))
	if err != nil || !slices.Equal(got, []string{"m/a", "m/b"}) {
		t.Errorf("readPackages = %v, %v", got, err)
	}
}

// The heavy list names packages of this module, each exactly once. The list is
// checked against go list ./... as CI's tasks check it, and each named package's
// directory is stat'ed: go test records the files a test stats and reruns the
// test when one changes, so a package renamed or removed fails here on the next
// run (a child process's output is not recorded, so go list alone would not do),
// in the shard that holds this package, and not silently in a shard that no
// longer tests it. The mise tasks are the guard that runs on every shard.
func TestHeavyNamesPackagesOfThisModule(t *testing.T) {
	t.Parallel()
	if _, err := exec.LookPath("go"); err != nil {
		t.Skip("go is not installed")
	}
	root, module := moduleRoot(t)
	if err := dirsExist(root, module, Heavy); err != nil {
		t.Error(err)
	}
	cmd := exec.CommandContext(t.Context(), "go", "list", "./...")
	cmd.Dir = root
	cmd.Env = append(os.Environ(), "GOFLAGS=")
	out, err := cmd.Output()
	if err != nil {
		t.Fatalf("go list: %v", err)
	}
	all, err := readPackages(bytes.NewReader(out))
	if err != nil {
		t.Fatalf("go list's output: %v", err)
	}
	if _, err := split(all, Heavy, true); err != nil {
		t.Error(err)
	}
	if len(Heavy) == 0 || len(Heavy) >= len(all)/2 {
		t.Errorf("Heavy names %d of %d packages; the heavy shard is the few slowest", len(Heavy), len(all))
	}
}

func TestDirsExistFailsOnAMissingPackage(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	if err := os.MkdirAll(filepath.Join(root, "internal", "a"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := dirsExist(root, "m", []string{"m/internal/a"}); err != nil {
		t.Errorf("an existing package: %v", err)
	}
	for _, tc := range []struct{ name, pkg string }{
		{"missing", "m/internal/gone"},
		{"another module", "other/internal/a"},
	} {
		if err := dirsExist(root, "m", []string{"m/internal/a", tc.pkg}); err == nil || !strings.Contains(err.Error(), tc.pkg) {
			t.Errorf("%s: err = %v, want one naming %s", tc.name, err, tc.pkg)
		}
	}
	if err := dirsExist(root, "m", []string{"m/internal/a/.."}); err == nil {
		t.Error("a path that leaves the package was accepted")
	}
}

// dirsExist fails unless every package of pkgs is a directory of module under
// root. It stats each directory, which is what makes go test's cached result of
// a test that calls it follow a rename or removal.
func dirsExist(root, module string, pkgs []string) error {
	for _, p := range pkgs {
		rel, ok := strings.CutPrefix(p, module+"/")
		if !ok || rel != filepath.ToSlash(filepath.Clean(rel)) || strings.HasPrefix(rel, "..") {
			return fmt.Errorf("heavy package %s is not under module %s", p, module)
		}
		st, err := os.Stat(filepath.Join(root, filepath.FromSlash(rel)))
		if err != nil || !st.IsDir() {
			return fmt.Errorf("heavy package %s has no directory under %s: %w", p, root, err)
		}
	}
	return nil
}

// moduleRoot is the directory of the root module's go.mod, and its module path,
// read from the file (a read go test records too).
func moduleRoot(t *testing.T) (root, module string) {
	t.Helper()
	out, err := exec.CommandContext(t.Context(), "go", "env", "GOMOD").Output()
	if err != nil {
		t.Fatalf("go env GOMOD: %v", err)
	}
	gomod := strings.TrimSpace(string(out))
	if gomod == "" || gomod == os.DevNull {
		t.Fatal("not in a module")
	}
	b, err := os.ReadFile(gomod)
	if err != nil {
		t.Fatal(err)
	}
	for line := range strings.Lines(string(b)) {
		if m, ok := strings.CutPrefix(strings.TrimSpace(line), "module "); ok {
			return filepath.Dir(gomod), strings.TrimSpace(m)
		}
	}
	t.Fatalf("no module line in %s", gomod)
	return "", ""
}
