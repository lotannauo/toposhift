package main

import (
	"go/parser"
	gotoken "go/token"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
)

// banned is what the root module's purego task keeps out of the product: gRPC,
// Arrow Flight, Arrow's cdata and pqarrow.
var banned = regexp.MustCompile(`^(google\.golang\.org/grpc|github\.com/apache/arrow-go/v18/(arrow/flight|arrow/cdata|parquet/pqarrow))(/|$)`)

// TestNoBannedPackageInTheGraph shows that this command reaches Arrow only through
// internal/replay/activity, and that none of the packages the product keeps out is
// in its dependency graph, with cgo off and on.
func TestNoBannedPackageInTheGraph(t *testing.T) {
	files, err := filepath.Glob("*.go")
	if err != nil || len(files) == 0 {
		t.Fatalf("no source files: %v", err)
	}
	for _, name := range files {
		f, err := parser.ParseFile(gotoken.NewFileSet(), name, nil, parser.ImportsOnly)
		if err != nil {
			t.Fatal(err)
		}
		for _, imp := range f.Imports {
			if p := strings.Trim(imp.Path.Value, `"`); strings.Contains(p, "apache/arrow-go") {
				t.Errorf("%s imports %s: Arrow comes only through internal/replay/activity", name, p)
			}
		}
	}
	goBin, err := exec.LookPath("go")
	if err != nil {
		t.Fatalf("go is not on the path: %v", err)
	}
	for _, cgo := range []string{"0", "1"} {
		cmd := exec.CommandContext(t.Context(), goBin, "list", "-deps", "-f", "{{.ImportPath}}", ".")
		cmd.Env = append(os.Environ(), "CGO_ENABLED="+cgo)
		out, err := cmd.Output()
		if err != nil {
			t.Fatalf("go list with CGO_ENABLED=%s: %v", cgo, err)
		}
		var sawParquet bool
		for _, p := range strings.Fields(string(out)) {
			if banned.MatchString(p) {
				t.Errorf("CGO_ENABLED=%s: %s is in the dependency graph", cgo, p)
			}
			sawParquet = sawParquet || p == "github.com/apache/arrow-go/v18/parquet/file"
		}
		if !sawParquet {
			t.Errorf("CGO_ENABLED=%s: the Parquet library is not in the graph, so this test proves nothing", cgo)
		}
	}
}
