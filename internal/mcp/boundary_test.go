package mcp

import (
	"go/build"
	"strings"
	"testing"
)

// The core imports no MCP protocol code: the transport adapter is the only
// package that does.
func TestCoreImportsNoSDK(t *testing.T) {
	pkg, err := build.ImportDir(".", 0)
	if err != nil {
		t.Fatal(err)
	}
	all := append(append(append([]string(nil), pkg.Imports...), pkg.TestImports...), pkg.XTestImports...)
	if len(pkg.Imports) < 10 {
		t.Fatalf("read %d imports; the test lost its input", len(pkg.Imports))
	}
	for _, imp := range all {
		if strings.HasPrefix(imp, "github.com/modelcontextprotocol/") {
			t.Errorf("the core imports %s", imp)
		}
	}
	// Nor does it serve anything itself.
	for _, imp := range pkg.Imports {
		if imp == "net/http" || imp == "net" {
			t.Errorf("the core imports %s", imp)
		}
	}
}
