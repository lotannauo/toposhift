package runner

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"runtime/debug"
	"strings"
)

// BuildInfo is what the running binary was built with, as far as it changes what
// a measurement means.
type BuildInfo struct {
	GoVersion, GOOS, GOARCH string
	// CGO is whether cgo was enabled; without it Pebble's block cache lives on the
	// Go heap, as it does in toposhift.
	CGO bool
	// Race and Invariants are whether the race detector and Pebble's invariant
	// checks are compiled in. Both slow reads down by large factors and change
	// nothing about what a read counts.
	Race, Invariants bool
	Tags             string
	// Unoptimized is whether the compiler was told not to optimize or inline.
	Unoptimized bool
	// Revision is the commit the binary was built from, and Modified whether the
	// tree had changes; both are empty or false when the binary carries no version
	// control stamp (a test binary, or -buildvcs=false), which is not a clean tree.
	Revision string
	Modified bool
	// Executable is the SHA-256 of the running binary, so two builds from two
	// different trees are told apart even if neither carries a stamp.
	Executable string
}

// ReadBuildInfo reads the running binary's.
func ReadBuildInfo() BuildInfo {
	bi := BuildInfo{GoVersion: runtime.Version(), GOOS: runtime.GOOS, GOARCH: runtime.GOARCH, CGO: true}
	if path, err := os.Executable(); err == nil {
		if f, err := os.Open(path); err == nil {
			h := sha256.New()
			if _, err := io.Copy(h, f); err == nil {
				bi.Executable = hex.EncodeToString(h.Sum(nil))
			}
			_ = f.Close()
		}
	}
	info, ok := debug.ReadBuildInfo()
	if !ok {
		return bi
	}
	applySettings(&bi, info.Settings)
	return bi
}

// applySettings reads what a binary was built with from its build settings.
func applySettings(bi *BuildInfo, settings []debug.BuildSetting) {
	for _, s := range settings {
		switch s.Key {
		case "-gcflags":
			// -N turns optimization off and -l inlining; either changes what a
			// timing means and nothing a counter counts.
			for _, f := range strings.Fields(strings.NewReplacer("=", " ", ",", " ").Replace(s.Value)) {
				if f == "-N" || f == "-l" {
					bi.Unoptimized = true
				}
			}
		case "CGO_ENABLED":
			bi.CGO = s.Value != "0"
		case "-race":
			bi.Race = s.Value == "true"
		case "-tags":
			bi.Tags = s.Value
			for _, t := range strings.Split(s.Value, ",") {
				if t == "invariants" {
					bi.Invariants = true
				}
			}
		case "vcs.revision":
			bi.Revision = s.Value
		case "vcs.modified":
			bi.Modified = s.Value == "true"
		}
	}
}

// Guards are the checks before a build or a read. A run that breaks one is
// refused, unless the caller says it is untimed and will not be used as a result:
// then it may be instrumented, dirty or have cgo, which is for validating at
// full scale, and its report says so.
type Guards struct {
	Info BuildInfo
	// Untimed allows what a result may not be built on.
	Untimed bool
}

// Check returns the reason a binary may not be used for a result, or nil.
func (g Guards) Check() error {
	if g.Untimed {
		return nil
	}
	var why []string
	if g.Info.Race {
		why = append(why, "built with the race detector")
	}
	if g.Info.Invariants {
		why = append(why, "built with the invariants tag")
	}
	if g.Info.CGO {
		why = append(why, "built with cgo (build with CGO_ENABLED=0, as toposhift ships)")
	}
	if g.Info.Unoptimized {
		why = append(why, "built with optimization or inlining off (-gcflags=-N -l)")
	}
	if g.Info.Modified {
		why = append(why, "built from a tree with uncommitted changes")
	}
	if g.Info.Revision == "" {
		why = append(why, "built without a version control stamp, so not known to be from a clean tree (build inside the repository without -buildvcs=false)")
	}
	if len(why) == 0 {
		return nil
	}
	return fmt.Errorf("runner: this binary is not one a result may come from: %s (use -untimed for a validation run)", strings.Join(why, "; "))
}

// ErrInsideWorktree is returned for an output directory under a git worktree: a
// result file committed by accident would put numbers in the repository.
var ErrInsideWorktree = errors.New("the directory is inside a git worktree")

// CheckOutside refuses a directory that is, or lies under, a git worktree.
func CheckOutside(dir string) error {
	abs, err := filepath.Abs(dir)
	if err != nil {
		return err
	}
	// Resolve the part that exists, so a symlink into a worktree is seen.
	for p := abs; ; p = filepath.Dir(p) {
		if real, err := filepath.EvalSymlinks(p); err == nil {
			abs = filepath.Join(real, strings.TrimPrefix(abs, p))
			break
		}
		if filepath.Dir(p) == p {
			break
		}
	}
	for p := abs; ; p = filepath.Dir(p) {
		if _, err := os.Stat(filepath.Join(p, ".git")); err == nil {
			return fmt.Errorf("runner: %s: %w (%s): write results outside the repository", dir, ErrInsideWorktree, p)
		}
		if filepath.Dir(p) == p {
			return nil
		}
	}
}
