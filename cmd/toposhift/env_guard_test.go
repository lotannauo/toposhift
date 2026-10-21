package main

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
)

// TestMain gives the whole test process an environment with no data directory
// in it: HOME is an empty temp directory, and the variables that name a data
// directory are unset. A test that starts a program, or a code path that reads
// the environment itself, still cannot reach the data directory of whoever runs
// the tests. The locations the go command keeps its caches and toolchains in
// are pinned first, because they default to places under HOME.
func TestMain(m *testing.M) { os.Exit(testMain(m)) }

func testMain(m *testing.M) int {
	pinGoDirs()
	home, err := os.MkdirTemp("", "toposhift-cmd-test-home-")
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 1
	}
	defer func() { _ = os.RemoveAll(home) }()
	_ = os.Setenv("HOME", home)
	for _, k := range []string{"XDG_DATA_HOME", "STATE_DIRECTORY", "TOPOSHIFT_DATA_DIR"} {
		_ = os.Unsetenv(k)
	}
	return m.Run()
}

// pinGoDirs sets the go command's cache, module and settings locations to
// where it would put them under the current HOME, unless they are set.
func pinGoDirs() {
	set := func(k, v string) {
		if os.Getenv(k) == "" && v != "" {
			_ = os.Setenv(k, v)
		}
	}
	if d, err := os.UserCacheDir(); err == nil {
		set("GOCACHE", filepath.Join(d, "go-build"))
	}
	if d, err := os.UserConfigDir(); err == nil {
		set("GOENV", filepath.Join(d, "go", "env"))
	}
	gopath := os.Getenv("GOPATH")
	if gopath == "" {
		if h, err := os.UserHomeDir(); err == nil {
			gopath = filepath.Join(h, "go")
			set("GOPATH", gopath)
		}
	}
	if first := filepath.SplitList(gopath); len(first) > 0 {
		set("GOMODCACHE", filepath.Join(first[0], "pkg", "mod"))
	}
}

// goTool is the go command that built this test, so that a boot test builds
// the binary with the same toolchain whatever PATH resolves "go" to. A version
// manager's shim on PATH reads its configuration and installs relative to
// HOME, which TestMain has pointed at an empty directory. It falls back to "go"
// when the toolchain's own file is not there.
func goTool() string {
	name := "go"
	if runtime.GOOS == "windows" {
		name += ".exe"
	}
	// The test runs where it was built, so the root it was built with is right.
	if root := runtime.GOROOT(); root != "" { //nolint:staticcheck // see above
		path := filepath.Join(root, "bin", name)
		if info, err := os.Stat(path); err == nil && !info.IsDir() {
			return path
		}
	}
	return "go"
}

// testEnv is the environment of a test of run: exactly kv, and nothing of the
// process's own. HOME may be given only as a directory in the temp directory,
// so no test can reach the HOME of whoever runs it.
func testEnv(t testing.TB, kv map[string]string) func(string) string {
	t.Helper()
	if home, ok := kv["HOME"]; ok && home != "" {
		real, err := filepath.EvalSymlinks(home)
		if err != nil {
			real = home
		}
		temp, err := filepath.EvalSymlinks(os.TempDir())
		if err != nil {
			temp = os.TempDir()
		}
		if !strings.HasPrefix(real, temp+string(filepath.Separator)) {
			t.Fatalf("testEnv: HOME=%q is not in the temp directory %q", home, temp)
		}
	}
	return func(k string) string { return kv[k] }
}

// testEnvHome is testEnv with nothing but HOME, a directory in the temp
// directory.
func testEnvHome(t testing.TB, home string) func(string) string {
	t.Helper()
	return testEnv(t, map[string]string{"HOME": home})
}

// TestRunIsNeverGivenARealEnvironment checks the tests of this package, so a
// test of serve cannot find or write the data directory under the HOME of the
// person running it. Every call of run takes its environment from noEnv, from
// a local getenv, or from testEnv; none reads the process's environment; and
// only this file names HOME.
func TestRunIsNeverGivenARealEnvironment(t *testing.T) {
	t.Parallel()
	files, err := filepath.Glob("*_test.go")
	if err != nil || len(files) == 0 {
		t.Fatalf("no test files found: %v", err)
	}
	calls := 0
	for _, name := range files {
		if name == "env_guard_test.go" {
			continue
		}
		fset := token.NewFileSet()
		f, err := parser.ParseFile(fset, name, nil, 0)
		if err != nil {
			t.Fatal(err)
		}
		ast.Inspect(f, func(n ast.Node) bool {
			switch n := n.(type) {
			case *ast.CallExpr:
				if id, ok := n.Fun.(*ast.Ident); ok && id.Name == "run" && len(n.Args) >= 3 {
					calls++
					if !allowedEnv(n.Args[2]) {
						t.Errorf("%s: run is given an environment that is not noEnv, getenv, testEnv(...) or testEnvHome(...)", fset.Position(n.Pos()))
					}
				}
			case *ast.SelectorExpr:
				if id, ok := n.X.(*ast.Ident); ok && id.Name == "os" {
					switch n.Sel.Name {
					case "Getenv", "LookupEnv", "Environ", "UserHomeDir":
						t.Errorf("%s: a test reads the real environment (os.%s)", fset.Position(n.Pos()), n.Sel.Name)
					}
				}
			case *ast.BasicLit:
				if n.Kind == token.STRING && strings.Trim(n.Value, "`\"") == "HOME" {
					t.Errorf("%s: a test names HOME; build the environment with testEnv", fset.Position(n.Pos()))
				}
			}
			return true
		})
	}
	if calls == 0 {
		t.Fatal("found no call of run in the tests: the guard is not looking at them")
	}
}

func allowedEnv(e ast.Expr) bool {
	switch e := e.(type) {
	case *ast.Ident:
		return e.Name == "noEnv" || e.Name == "getenv"
	case *ast.CallExpr:
		id, ok := e.Fun.(*ast.Ident)
		return ok && (id.Name == "testEnv" || id.Name == "testEnvHome")
	}
	return false
}
