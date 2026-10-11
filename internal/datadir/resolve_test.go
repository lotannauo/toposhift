package datadir

import (
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
)

func env(kv map[string]string) func(string) string {
	return func(k string) string { return kv[k] }
}

func TestResolve(t *testing.T) {
	tests := []struct {
		name      string
		flag      string
		env       map[string]string
		cwd       string
		wantDir   string
		wantSrc   string
		wantNotes []string // substrings, one per note, in order
	}{
		{name: "nothing set", wantSrc: SourceNone},
		{name: "flag alone", flag: "/f", wantDir: "/f", wantSrc: SourceFlag},
		{name: "env alone", env: map[string]string{"TOPOSHIFT_DATA_DIR": "/e"}, wantDir: "/e", wantSrc: SourceEnv},
		{name: "state directory alone", env: map[string]string{"STATE_DIRECTORY": "/s"}, wantDir: "/s", wantSrc: SourceStateDirectory},
		{name: "home alone", env: map[string]string{"HOME": "/h"}, wantDir: "/h/.local/share/toposhift", wantSrc: SourceHome},
		{
			name: "all together, the flag wins", flag: "/f", wantDir: "/f", wantSrc: SourceFlag,
			env: map[string]string{"TOPOSHIFT_DATA_DIR": "/e", "STATE_DIRECTORY": "/s", "HOME": "/h"},
		},
		{
			name: "without the flag, the variable wins", wantDir: "/e", wantSrc: SourceEnv,
			env: map[string]string{"TOPOSHIFT_DATA_DIR": "/e", "STATE_DIRECTORY": "/s", "HOME": "/h"},
		},
		{
			name: "without either, the state directory wins", wantDir: "/s", wantSrc: SourceStateDirectory,
			env: map[string]string{"STATE_DIRECTORY": "/s", "HOME": "/h"},
		},
		{
			name: "two state directories give none, and say which flag to use", wantSrc: SourceNone,
			env:       map[string]string{"STATE_DIRECTORY": "/a:/b", "HOME": "/h"},
			wantNotes: []string{"--data-dir"},
		},
		{
			name: "a relative state directory gives none", wantSrc: SourceNone,
			env:       map[string]string{"STATE_DIRECTORY": "rel", "HOME": "/h"},
			wantNotes: []string{"not an absolute path"},
		},
		{name: "HOME unset gives none", env: map[string]string{"XDG_DATA_HOME": ""}, wantSrc: SourceNone},
		{name: "a relative HOME gives none", env: map[string]string{"HOME": "h"}, wantSrc: SourceNone},
		{
			name: "XDG_DATA_HOME is not used, and boot says so once", wantDir: "/h/.local/share/toposhift", wantSrc: SourceHome,
			env:       map[string]string{"HOME": "/h", "XDG_DATA_HOME": "/x"},
			wantNotes: []string{"XDG_DATA_HOME (/x) is not used"},
		},
		{
			name: "XDG_DATA_HOME at the default says nothing", wantDir: "/h/.local/share/toposhift", wantSrc: SourceHome,
			env: map[string]string{"HOME": "/h", "XDG_DATA_HOME": "/h/.local/share"},
		},
		{
			name: "XDG_DATA_HOME at the default with a slash says nothing", wantDir: "/h/.local/share/toposhift", wantSrc: SourceHome,
			env: map[string]string{"HOME": "/h", "XDG_DATA_HOME": "/h/.local/share/"},
		},
		{
			name: "XDG_DATA_HOME does not stand in for HOME", wantSrc: SourceNone,
			env:       map[string]string{"XDG_DATA_HOME": "/x"},
			wantNotes: []string{"XDG_DATA_HOME (/x) is not used"},
		},
		{name: "a relative flag is taken against the working directory", flag: "d", cwd: "/w", wantDir: "/w/d", wantSrc: SourceFlag},
		{name: "a dot-dot flag is cleaned", flag: "../d", cwd: "/w/v", wantDir: "/w/d", wantSrc: SourceFlag},
		{
			name: "a relative variable is taken against the working directory", cwd: "/w", wantDir: "/w/e/f", wantSrc: SourceEnv,
			env: map[string]string{"TOPOSHIFT_DATA_DIR": "e/f"},
		},
		{name: "an absolute flag is cleaned", flag: "/f//g/../h/", wantDir: "/f/h", wantSrc: SourceFlag},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cwd := tt.cwd
			if cwd == "" {
				cwd = "/cwd"
			}
			got, err := Resolve(Flags{DataDir: tt.flag}, env(tt.env), cwd)
			if err != nil {
				t.Fatal(err)
			}
			if got.Dir != tt.wantDir || got.Source != tt.wantSrc {
				t.Errorf("Resolve = {Dir: %q, Source: %q}, want {Dir: %q, Source: %q}", got.Dir, got.Source, tt.wantDir, tt.wantSrc)
			}
			if len(got.Notes) != len(tt.wantNotes) {
				t.Fatalf("notes = %q, want %d note(s) containing %q", got.Notes, len(tt.wantNotes), tt.wantNotes)
			}
			for i, want := range tt.wantNotes {
				if !strings.Contains(got.Notes[i], want) {
					t.Errorf("note %d = %q, want it to contain %q", i, got.Notes[i], want)
				}
			}
		})
	}
}

func TestResolveRefusesALeadingTilde(t *testing.T) {
	tests := []struct {
		name string
		flag string
		env  map[string]string
	}{
		{"flag with a path", "~/x", nil},
		{"flag alone", "~", nil},
		{"flag naming a user", "~root/x", nil},
		{"variable", "", map[string]string{"TOPOSHIFT_DATA_DIR": "~/x"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := Resolve(Flags{DataDir: tt.flag}, env(tt.env), "/cwd")
			if err == nil {
				t.Fatalf("Resolve = %+v, want an error", got)
			}
			if !strings.Contains(err.Error(), "~") {
				t.Errorf("error %q does not name the tilde", err)
			}
			if got.Dir != "" {
				t.Errorf("Dir = %q with an error, want none", got.Dir)
			}
		})
	}
}

func TestResolveTildeInsideAValueIsALiteralName(t *testing.T) {
	got, err := Resolve(Flags{DataDir: "/a/~b"}, env(nil), "/cwd")
	if err != nil || got.Dir != "/a/~b" {
		t.Fatalf("Resolve = %+v, %v; want /a/~b", got, err)
	}
}

func TestResolveRelativeNeedsAWorkingDirectory(t *testing.T) {
	if _, err := Resolve(Flags{DataDir: "d"}, env(nil), ""); err == nil {
		t.Fatal("a relative flag with no working directory resolved")
	}
	// A value that is absolute does not need one.
	if _, err := Resolve(Flags{DataDir: "/d"}, env(nil), ""); err != nil {
		t.Fatal(err)
	}
}

func TestResolveTouchesNothing(t *testing.T) {
	base := t.TempDir()
	dir := filepath.Join(base, "not", "there")
	if _, err := Resolve(Flags{DataDir: dir}, env(nil), base); err != nil {
		t.Fatal(err)
	}
	entries, err := os.ReadDir(base)
	if err != nil || len(entries) != 0 {
		t.Fatalf("Resolve created %v (err %v)", entries, err)
	}
}

func TestResolveSymlinkTarget(t *testing.T) {
	root, err := filepath.EvalSymlinks(t.TempDir()) // the temp directory is itself a link on macOS
	if err != nil {
		t.Fatal(err)
	}
	real := filepath.Join(root, "real")
	link := filepath.Join(root, "link")
	if err := os.Mkdir(real, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(real, link); err != nil {
		t.Fatal(err)
	}

	tests := []struct {
		name, flag, wantDir, wantTarget string
	}{
		{"a symlinked directory", link, link, real},
		{"a directory not yet created below a symlink", filepath.Join(link, "sub"), filepath.Join(link, "sub"), filepath.Join(real, "sub")},
		{"no symlink", real, real, ""},
		{"not yet created, no symlink", filepath.Join(real, "sub"), filepath.Join(real, "sub"), ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := Resolve(Flags{DataDir: tt.flag}, env(nil), root)
			if err != nil {
				t.Fatal(err)
			}
			if got.Dir != tt.wantDir || got.Target != tt.wantTarget {
				t.Errorf("Dir, Target = %q, %q; want %q, %q", got.Dir, got.Target, tt.wantDir, tt.wantTarget)
			}
		})
	}
}

func TestSourcesAreDistinct(t *testing.T) {
	all := []string{SourceFlag, SourceEnv, SourceStateDirectory, SourceHome, SourceNone}
	slices.Sort(all)
	if len(slices.Compact(all)) != len(all) {
		t.Fatalf("sources collide: %v", all)
	}
}
