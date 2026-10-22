package vmrun

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
)

// ErrInWorkTree marks an output directory inside a git work tree.
var ErrInWorkTree = errors.New("vmrun: the directory is inside a git work tree")

// RefuseInWorkTree refuses a directory inside a git work tree: what the commands write is
// local scratch and never a result, so it does not belong where `git add` would find it.
// The directory need not exist; its nearest ancestor that does is looked at.
func RefuseInWorkTree(dir string) error {
	abs, err := filepath.Abs(dir)
	if err != nil {
		return err
	}
	p := abs
	for {
		if _, err := os.Lstat(p); err == nil {
			break
		}
		parent := filepath.Dir(p)
		if parent == p {
			break
		}
		p = parent
	}
	if real, err := filepath.EvalSymlinks(p); err == nil {
		p = real
	}
	for {
		if _, err := os.Lstat(filepath.Join(p, ".git")); err == nil {
			return fmt.Errorf("%w: %s is inside %s: write scratch outside it (for example under ~/spikebench/vm)", ErrInWorkTree, dir, p)
		}
		parent := filepath.Dir(p)
		if parent == p {
			return nil
		}
		p = parent
	}
}

// ErrDigest marks a file whose SHA-256 is not the one named in advance.
var ErrDigest = errors.New("vmrun: the file is not the one named in advance")

// FileSHA256 returns the SHA-256 of a file as hex.
func FileSHA256(path string) (string, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:]), nil
}

// checkSHA refuses a file whose SHA-256 is not the expected one. An empty expectation is
// an error: a count is only of an input that was named in advance.
func checkSHA(what, path, got, expect string) error {
	if expect == "" {
		return fmt.Errorf("%w: no expected SHA-256 for %s: a count is only of a file that was named in advance", ErrDigest, what)
	}
	if !strings.EqualFold(expect, got) {
		return fmt.Errorf("%w: %s %s has SHA-256 %s, not the expected %s", ErrDigest, what, path, got, expect)
	}
	return nil
}

// WriteJSON writes v as indented JSON to dir/name, creating the directory. The directory
// must not be inside a git work tree.
func WriteJSON(dir, name string, v any) error {
	if err := RefuseInWorkTree(dir); err != nil {
		return err
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}
	b, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(filepath.Join(dir, name), append(b, '\n'), 0o644)
}

// EncodeJSON writes v as indented JSON to w.
func EncodeJSON(w io.Writer, v any) error {
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	return enc.Encode(v)
}

// ReadJSON reads dir/name into v.
func ReadJSON(dir, name string, v any) error {
	b, err := os.ReadFile(filepath.Join(dir, name))
	if err != nil {
		return err
	}
	if err := json.Unmarshal(b, v); err != nil {
		return fmt.Errorf("vmrun: %s: %w", filepath.Join(dir, name), err)
	}
	return nil
}
