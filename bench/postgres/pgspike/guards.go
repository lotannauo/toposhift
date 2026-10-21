package pgspike

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/lotannauo/toposhift/bench/postgres/pgengine"
)

// ErrInsideWorkTree is wrapped by the error [RefuseInWorkTree] returns.
var ErrInsideWorkTree = errors.New("pgspike: the output directory is inside a git work tree")

// RefuseInWorkTree refuses a directory that is inside a git work tree, existing or yet to
// be made: what pgspike writes is the numbers of a run, and numbers from a laptop must
// not be one `git add` from being published. A directory is inside a work tree if it, or
// any directory above it, holds a .git (a directory, or the file a linked work tree has).
// Symbolic links are followed, so a link out of a work tree into it is refused too.
func RefuseInWorkTree(dir string) error {
	abs, err := filepath.Abs(dir)
	if err != nil {
		return err
	}
	// Resolve the part that exists; what is below it does not exist yet, and is added
	// back unresolved.
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
var ErrDigestDiffers = errors.New("pgspike: the file is not the one that was asked for")

// CheckDigest reads a file and checks its SHA-256 against the one expected, which is
// required: a run of a plan or a question set that was not named by its digest is not a
// run of the registered one. It returns the digest, as hex.
func CheckDigest(path, expect, what string) (string, error) {
	if expect == "" {
		return "", fmt.Errorf("pgspike: the %s %s must be named by its SHA-256 (-expect-%s)", what, path, what)
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

// CheckContentDigest checks the content digest an activity file's footer gives against
// the one expected. The digest is of the rows, and is known only once the whole file has
// been read, so it is checked then; the expected value may be given as a prefix of at
// least 12 hex digits, as a report quotes it.
func CheckContentDigest(got, expect string) error {
	g, e := strings.TrimPrefix(got, "sha256:"), strings.TrimPrefix(expect, "sha256:")
	if len(e) < 12 || !strings.HasPrefix(strings.ToLower(g), strings.ToLower(e)) {
		return fmt.Errorf("%w: the content digest is %s, not %s", ErrDigestDiffers, got, expect)
	}
	return nil
}

// RequireSettings reads settings.conf and refuses a server whose settings differ from it,
// returning the SHA-256 of the file. A server started by hand with other settings would
// give numbers that are not the registered regime's.
func RequireSettings(ctx context.Context, q pgengine.Querier, path string) (string, error) {
	want, sum, err := pgengine.LoadSettings(path)
	if err != nil {
		return "", err
	}
	if err := pgengine.CheckSettings(ctx, q, want); err != nil {
		return "", err
	}
	return sum, nil
}
