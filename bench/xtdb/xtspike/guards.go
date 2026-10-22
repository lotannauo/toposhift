package xtspike

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

// ErrInsideWorkTree is wrapped by the error [RefuseInWorkTree] returns.
var ErrInsideWorkTree = errors.New("xtspike: the output directory is inside a git work tree")

// RefuseInWorkTree refuses a directory that is inside a git work tree, existing or yet to be
// made: what xtspike writes is the figures of a run, and figures from a laptop must not be
// one `git add` from being published. A directory is inside a work tree if it, or any
// directory above it, holds a .git (a directory, or the file a linked work tree has).
// Symbolic links are followed, so a link out of a work tree into it is refused too.
func RefuseInWorkTree(dir string) error {
	abs, err := filepath.Abs(dir)
	if err != nil {
		return err
	}
	// Resolve the part that exists; what is below it does not exist yet, and is added back
	// unresolved.
	existing, rest := abs, ""
	for {
		if _, err := os.Lstat(existing); err == nil {
			break
		}
		parent := filepath.Dir(existing)
		if parent == existing {
			break
		}
		rest = filepath.Join(filepath.Base(existing), rest)
		existing = parent
	}
	if resolved, err := filepath.EvalSymlinks(existing); err == nil {
		existing = resolved
	}
	for d := filepath.Join(existing, rest); ; d = filepath.Dir(d) {
		if _, err := os.Lstat(filepath.Join(d, ".git")); err == nil {
			return fmt.Errorf("%w: %s holds a .git (use a directory outside the repository)", ErrInsideWorkTree, d)
		}
		if parent := filepath.Dir(d); parent == d {
			return nil
		}
	}
}

// ErrDigestDiffers is wrapped by the error [CheckDigest] returns.
var ErrDigestDiffers = errors.New("xtspike: the file is not the one that was asked for")

// fileSHA is the SHA-256 of a file, as hex.
func fileSHA(path string) (string, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:]), nil
}

// CheckDigest reads a file and checks its SHA-256 against the one expected, which is
// required: a run of a plan or a question set that was not named by its digest is not a run
// of the registered one. It returns the digest, as hex.
func CheckDigest(path, expect, what string) (string, error) {
	if expect == "" {
		return "", fmt.Errorf("xtspike: the %s %s must be named by its SHA-256 (-expect-%s)", what, path, what)
	}
	got, err := fileSHA(path)
	if err != nil {
		return "", err
	}
	if !strings.EqualFold(strings.TrimPrefix(expect, "sha256:"), got) {
		return got, fmt.Errorf("%w: %s has SHA-256 %s, not the expected %s", ErrDigestDiffers, path, got, expect)
	}
	return got, nil
}

// CheckContentDigest checks the content digest an activity file's footer gives against the
// one expected. The digest is of the rows, and is known only once the whole file has been
// read, so it is checked then; the expected value may be given as a prefix of at least 12
// hex digits, as a report quotes it.
func CheckContentDigest(got, expect string) error {
	g, e := strings.TrimPrefix(got, "sha256:"), strings.TrimPrefix(expect, "sha256:")
	if len(e) < 12 || !strings.HasPrefix(strings.ToLower(g), strings.ToLower(e)) {
		return fmt.Errorf("%w: the content digest is %s, not %s", ErrDigestDiffers, got, expect)
	}
	return nil
}

// ErrNotCI is wrapped by the error [RequireCI] returns.
var ErrNotCI = errors.New("xtspike: this measures a time, which is taken on CI hardware only")

// RequireCI refuses a command that measures a time unless it runs on a GitHub Actions runner
// (GITHUB_ACTIONS is "true"). Timings from a laptop or the Oracle Cloud VM are never
// published, so none is taken there.
func RequireCI(getenv func(string) (string, bool)) error {
	if v, _ := getenv("GITHUB_ACTIONS"); v != "true" {
		return ErrNotCI
	}
	return nil
}
