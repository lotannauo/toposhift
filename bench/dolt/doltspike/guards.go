package doltspike

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/lotannauo/toposhift/bench/dolt/scripts"
)

// ErrInsideWorkTree is wrapped by the error [RefuseInWorkTree] returns.
var ErrInsideWorkTree = errors.New("doltspike: the directory is inside a git work tree")

// RefuseInWorkTree refuses a directory that is inside a git work tree, existing or yet to
// be made: what doltspike writes is the counts of a run, and counts from a laptop must not
// be one `git add` from being published. A directory is inside a work tree if it, or any
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
var ErrDigestDiffers = errors.New("doltspike: the file is not the one that was asked for")

// CheckDigest reads a file and checks its SHA-256 against the one expected, which is
// required: a run of a plan or a question set that was not named by its digest is not a run
// of the registered one. It returns the digest, as hex.
func CheckDigest(path, expect, what string) (string, error) {
	if expect == "" {
		return "", fmt.Errorf("doltspike: the %s %s must be named by its SHA-256 (-expect-%s)", what, path, what)
	}
	b, err := os.ReadFile(path)
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(b)
	got := hex.EncodeToString(sum[:])
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

// ErrImage is wrapped by the error [CheckImage] returns.
var ErrImage = errors.New("doltspike: the server's image is not the registered one")

// CheckImage refuses an image that is not the one scripts/dolt-server.sh starts, tag and
// digest: the server cannot say which image it came from, so the caller names it
// (scripts/dolt-server.sh image prints it) and the record carries what it said. A
// server of another image is another server, and its counts are not the registered ones.
func CheckImage(image string) error {
	want, err := scripts.Image()
	if err != nil {
		return err
	}
	if image == "" {
		return fmt.Errorf("doltspike: name the image the server was started from (-image; scripts/dolt-server.sh image prints it): %w", ErrImage)
	}
	if image != want {
		return fmt.Errorf("doltspike: the server was said to run %s, the registered image is %s: %w", image, want, ErrImage)
	}
	return nil
}

// checkFree refuses, before any work is done, an output that would replace a record: a
// record that was overwritten by a later run is a count that cannot be traced.
func checkFree(dir, name string) error {
	if _, err := os.Lstat(filepath.Join(dir, name)); err == nil {
		return fmt.Errorf("doltspike: %s already exists in %s (a record is never overwritten)", name, dir)
	}
	return nil
}
