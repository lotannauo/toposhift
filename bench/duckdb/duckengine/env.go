package duckengine

import (
	"context"
	"fmt"
	"runtime"
	"runtime/debug"

	"github.com/lotannauo/toposhift/bench/duckdb/scripts"
)

// Env is what a run records of where it ran: the commit and the versions of everything
// whose change would change a counter. It holds no time, no host name and no path.
type Env struct {
	// Commit is the revision the binary was built from and Modified whether the tree had
	// changes; both are empty and false for a binary built outside a repository.
	Commit   string
	Modified bool
	Go       string
	OS, Arch string
	CPUs     int
	// DuckDB is duckdb_version() as the connection reports it, and the next three the
	// versions of the Go modules that carry it, as linked.
	DuckDB   string
	Driver   string
	Bindings string
	Library  string
	// SettingsSHA256 is the digest of settings.sql, and ArchiveOrder and ArchiveRowGroup
	// the archive's statement.
	SettingsSHA256  string
	ArchiveOrder    string
	ArchiveRowGroup int
}

const (
	driverModule   = "github.com/duckdb/duckdb-go/v2"
	bindingsModule = "github.com/duckdb/duckdb-go-bindings"
)

// Environment reads the environment of the running binary, with DuckDB's version from
// an open connection.
func Environment(ctx context.Context, db *DB) (Env, error) {
	v, err := db.Version(ctx)
	if err != nil {
		return Env{}, err
	}
	e := Env{
		Go: runtime.Version(), OS: runtime.GOOS, Arch: runtime.GOARCH, CPUs: runtime.NumCPU(), DuckDB: v,
		SettingsSHA256: scripts.SettingsSHA256(), ArchiveOrder: ArchiveOrder, ArchiveRowGroup: ArchiveRowGroup,
	}
	bi, ok := debug.ReadBuildInfo()
	if !ok {
		return e, nil
	}
	lib := fmt.Sprintf("%s/lib/%s-%s", bindingsModule, runtime.GOOS, runtime.GOARCH)
	for _, d := range bi.Deps {
		switch d.Path {
		case driverModule:
			e.Driver = d.Version
		case bindingsModule:
			e.Bindings = d.Version
		case lib:
			e.Library = d.Version
		}
	}
	for _, s := range bi.Settings {
		switch s.Key {
		case "vcs.revision":
			e.Commit = s.Value
		case "vcs.modified":
			e.Modified = s.Value == "true"
		}
	}
	return e, nil
}
