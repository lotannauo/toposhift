package datadir

import (
	"errors"
	"fmt"
	"io/fs"
	"path/filepath"
	"strings"
)

// The sources a data directory comes from, as [Result.Source] names them.
const (
	SourceFlag           = "flag"
	SourceEnv            = "env"
	SourceStateDirectory = "state_directory"
	SourceHome           = "home"
	SourceNone           = "none"
)

// The environment variables [Resolve] reads.
const (
	EnvDataDir        = "TOPOSHIFT_DATA_DIR"
	envStateDirectory = "STATE_DIRECTORY"
	envHome           = "HOME"
	envXDGDataHome    = "XDG_DATA_HOME"
)

// NoDirectoryMessage is the sentence boot prints, and the receiver answers
// with, when no data directory resolves.
const NoDirectoryMessage = "ingest is off: no data directory; set --data-dir or " + EnvDataDir

// Flags are the command-line values that take part in resolution.
type Flags struct {
	// DataDir is the value of --data-dir, or empty when it was not given.
	DataDir string
}

// Result is where the data directory is, and how that was decided.
type Result struct {
	// Dir is the absolute directory, or empty when there is none.
	Dir string
	// Source is one of the Source constants.
	Source string
	// Target is where Dir leads when a symlink is involved, if that differs
	// from Dir. It is empty when nothing could be resolved or nothing differs.
	Target string
	// Notes are sentences to log once at boot.
	Notes []string
}

// Resolve decides the data directory from the flag, the environment and the
// working directory, in the order the package documents. It reads no file; the
// only disk access is resolving symlinks of a path that exists. getenv reads
// the environment and cwd is the working directory.
//
// It fails only for an explicit value that is refused: one that begins with
// "~", or a relative one when cwd is not absolute.
func Resolve(args Flags, getenv func(string) string, cwd string) (Result, error) {
	res, err := resolve(args, getenv, cwd)
	if err != nil {
		return Result{}, err
	}
	if x := getenv(envXDGDataHome); x != "" && filepath.Clean(x) != homeData(getenv) {
		res.Notes = append(res.Notes, fmt.Sprintf(
			"%s (%s) is not used; set %s to keep the data there", envXDGDataHome, x, EnvDataDir))
	}
	if res.Dir != "" {
		if t := symlinkTarget(res.Dir); t != res.Dir {
			res.Target = t
		}
	}
	return res, nil
}

func resolve(args Flags, getenv func(string) string, cwd string) (Result, error) {
	if v := args.DataDir; v != "" {
		dir, err := explicit("--data-dir", v, cwd)
		return Result{Dir: dir, Source: SourceFlag}, err
	}
	if v := getenv(EnvDataDir); v != "" {
		dir, err := explicit(EnvDataDir, v, cwd)
		return Result{Dir: dir, Source: SourceEnv}, err
	}
	if v := getenv(envStateDirectory); v != "" {
		// A unit with several state directories gets them joined by colons.
		// Taking the first would be a guess, so there is no directory and the
		// note says what to do. The same holds for a value that is not absolute.
		if strings.Contains(v, ":") {
			return none(fmt.Sprintf("%s lists several paths (%q), so none of them is taken as the data directory; name one with --data-dir or %s",
				envStateDirectory, v, EnvDataDir)), nil
		}
		if !filepath.IsAbs(v) {
			return none(fmt.Sprintf("%s (%q) is not an absolute path, so it is not taken as the data directory; name one with --data-dir or %s",
				envStateDirectory, v, EnvDataDir)), nil
		}
		return Result{Dir: filepath.Clean(v), Source: SourceStateDirectory}, nil
	}
	if home := getenv(envHome); filepath.IsAbs(home) {
		return Result{Dir: filepath.Join(home, ".local", "share", "toposhift"), Source: SourceHome}, nil
	}
	return Result{Source: SourceNone}, nil
}

func none(note string) Result {
	return Result{Source: SourceNone, Notes: []string{note}}
}

// homeData is $HOME/.local/share, the directory $XDG_DATA_HOME defaults to, or
// empty when HOME cannot be used.
func homeData(getenv func(string) string) string {
	home := getenv(envHome)
	if !filepath.IsAbs(home) {
		return ""
	}
	return filepath.Join(home, ".local", "share")
}

// explicit makes an explicitly given value absolute.
func explicit(label, v, cwd string) (string, error) {
	if strings.HasPrefix(v, "~") {
		return "", fmt.Errorf("%s %q begins with \"~\", which only a shell expands; write the absolute path, or ./%s for a directory really named so",
			label, v, v)
	}
	if filepath.IsAbs(v) {
		return filepath.Clean(v), nil
	}
	if !filepath.IsAbs(cwd) {
		return "", fmt.Errorf("%s %q is relative, and the working directory is not known", label, v)
	}
	return filepath.Join(cwd, v), nil
}

// symlinkTarget resolves the symlinks in path. When the path does not exist
// yet it resolves the deepest part that does and keeps the rest. It returns
// path itself when nothing resolves.
func symlinkTarget(path string) string {
	rest := ""
	for p := path; ; {
		real, err := filepath.EvalSymlinks(p)
		if err == nil {
			return filepath.Join(real, rest)
		}
		parent := filepath.Dir(p)
		if !errors.Is(err, fs.ErrNotExist) || parent == p {
			return path
		}
		rest = filepath.Join(filepath.Base(p), rest)
		p = parent
	}
}
