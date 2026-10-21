package duckengine

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/lotannauo/toposhift/bench/duckdb/scripts"
)

func TestParseSettings(t *testing.T) {
	good := "-- a comment\n\nSET memory_limit = '4GB'; -- 3.7 GiB\nSET threads = 1;\nSET preserve_insertion_order = false;\n"
	got, err := ParseSettings(strings.NewReader(good))
	if err != nil {
		t.Fatal(err)
	}
	want := []Setting{
		{"memory_limit", "SET memory_limit = '4GB';", "3.7 GiB"},
		{"threads", "SET threads = 1;", "1"},
		{"preserve_insertion_order", "SET preserve_insertion_order = false;", "false"},
	}
	if len(got) != len(want) {
		t.Fatalf("%+v", got)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("setting %d is %+v, want %+v", i, got[i], want[i])
		}
	}
	for name, bad := range map[string]string{
		"a name twice":          "SET threads = 1;\nSET threads = 2;\n",
		"not a statement":       "threads = 1\n",
		"no semicolon":          "SET threads = 1\n",
		"a function as a value": "SET threads = greatest(1, 2);\n",
		"an upper case name":    "SET Threads = 1;\n",
		"two statements":        "SET threads = 1; SET memory_limit = '1GB';\n",
		"a reading of nothing":  "SET temp_directory = ''; --\n",
		"no setting":            "-- only comments\n",
		"an empty file":         "",
	} {
		if _, err := ParseSettings(strings.NewReader(bad)); err == nil {
			t.Errorf("%s was accepted", name)
		}
	}
}

// TestTheSettingsFileHasWhatTheBaselineRegisters pins what the guide fixes: one thread,
// a 4GB limit, no insertion order for reads, the caches on, and nothing installed or
// loaded.
func TestTheSettingsFileHasWhatTheBaselineRegisters(t *testing.T) {
	got, err := LoadSettings(scripts.SettingsSQL)
	if err != nil {
		t.Fatal(err)
	}
	have := map[string]Setting{}
	for _, s := range got {
		have[s.Name] = s
	}
	for name, want := range map[string]string{
		"memory_limit": "3.7 GiB", "threads": "1", "preserve_insertion_order": "false",
		"enable_object_cache": "true", "parquet_metadata_cache": "true", "enable_external_file_cache": "true",
		"autoinstall_known_extensions": "false", "autoload_known_extensions": "false", "allow_community_extensions": "false",
	} {
		if s, ok := have[name]; !ok || s.Want != want {
			t.Errorf("%s: %+v, want it to read back as %s", name, s, want)
		}
	}
	if len(got) != 9 {
		t.Errorf("the file has %d settings; a new one is a new version of the baseline", len(got))
	}
}

func TestOpenAppliesAndChecksTheSettings(t *testing.T) {
	ctx := context.Background()
	for _, mode := range []Mode{ModeRead, ModeBuild, ModeObserve} {
		db, err := Open(ctx, Config{Mode: mode, TempDir: t.TempDir(), Profile: true})
		if err != nil {
			t.Fatalf("%s: %v", mode, err)
		}
		var v string
		for name, want := range map[string]string{"threads": "1", "memory_limit": "3.7 GiB"} {
			if err := db.QueryRowContext(ctx, "SELECT value FROM duckdb_settings() WHERE name = ?", name).Scan(&v); err != nil || v != want {
				t.Errorf("%s: %s is %q (%v), want %s", mode, name, v, err, want)
			}
		}
		check := func(name, want string) {
			t.Helper()
			if err := db.QueryRowContext(ctx, "SELECT value FROM duckdb_settings() WHERE name = ?", name).Scan(&v); err != nil || v != want {
				t.Errorf("%s: %s is %q (%v), want %s", mode, name, v, err, want)
			}
		}
		check("preserve_insertion_order", map[Mode]string{ModeRead: "false", ModeBuild: "true", ModeObserve: "false"}[mode])
		check("enable_external_file_cache", map[Mode]string{ModeRead: "true", ModeBuild: "true", ModeObserve: "false"}[mode])
		check("enable_profiling", "no_output")
		if err := db.Check(ctx); err != nil {
			t.Errorf("%s: %v", mode, err)
		}
		db.Close()
	}
}

// TestAConnectionWithASettingChangedIsRefused: every setting of the file, changed from
// the connection, makes the check fail; so does the temp directory, and a setting the
// file does not name is none of its business.
func TestAConnectionWithASettingChangedIsRefused(t *testing.T) {
	ctx := context.Background()
	other := map[string]string{
		"memory_limit": "'2GB'", "threads": "2", "preserve_insertion_order": "true",
		"enable_object_cache": "false", "parquet_metadata_cache": "false", "enable_external_file_cache": "false",
		"autoinstall_known_extensions": "true", "autoload_known_extensions": "true",
		"temp_directory": "'/nowhere'",
	}
	db, err := Open(ctx, Config{Mode: ModeRead, TempDir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	settings, _ := LoadSettings(scripts.SettingsSQL)
	for _, s := range settings {
		// DuckDB itself refuses to change allow_community_extensions on a running
		// database, so no connection can differ in it; the read-back still checks it.
		if _, ok := other[s.Name]; !ok && s.Name != "allow_community_extensions" {
			t.Fatalf("no other value for %s: the test must change every setting", s.Name)
		}
	}
	for name, v := range other {
		if _, err := db.ExecContext(ctx, "SET "+name+" = "+v); err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		err := db.Check(ctx)
		var se *SettingsError
		if !errors.Is(err, ErrSettingsDiffer) || !errors.As(err, &se) || !strings.Contains(err.Error(), name) {
			t.Errorf("%s changed: %v", name, err)
		}
		// Put it back, by the statement the connection was opened with.
		for _, s := range db.Settings() {
			if s.Name == name {
				if _, err := db.ExecContext(ctx, s.Statement); err != nil {
					t.Fatal(err)
				}
			}
		}
		if err := db.Check(ctx); err != nil {
			t.Fatalf("%s put back: %v", name, err)
		}
	}
}

func TestOpenRefusesWhatItShould(t *testing.T) {
	ctx := context.Background()
	if _, err := Open(ctx, Config{Mode: ModeRead}); err == nil {
		t.Error("no temp directory was accepted")
	}
	if _, err := Open(ctx, Config{TempDir: t.TempDir()}); err == nil {
		t.Error("no mode was accepted")
	}
	old := wantVersion
	wantVersion = "v0.0.1"
	defer func() { wantVersion = old }()
	if _, err := Open(ctx, Config{Mode: ModeRead, TempDir: t.TempDir()}); err == nil || !strings.Contains(err.Error(), "written for v0.0.1") {
		t.Errorf("another DuckDB version: %v", err)
	}
}
