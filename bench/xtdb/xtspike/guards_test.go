package xtspike

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestAnOutputInsideAGitWorkTreeIsRefused(t *testing.T) {
	outside := t.TempDir()
	if err := RefuseInWorkTree(filepath.Join(outside, "a", "b")); err != nil {
		t.Fatalf("a directory outside any work tree: %v", err)
	}
	tree := t.TempDir()
	if err := os.Mkdir(filepath.Join(tree, ".git"), 0o700); err != nil {
		t.Fatal(err)
	}
	for _, dir := range []string{tree, filepath.Join(tree, "x"), filepath.Join(tree, "x", "y", "z")} {
		if err := RefuseInWorkTree(dir); !errors.Is(err, ErrInsideWorkTree) {
			t.Errorf("%s: %v", dir, err)
		}
	}
	// A linked work tree has a .git file, not a directory.
	linked := t.TempDir()
	if err := os.WriteFile(filepath.Join(linked, ".git"), []byte("gitdir: /x\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := RefuseInWorkTree(filepath.Join(linked, "out")); !errors.Is(err, ErrInsideWorkTree) {
		t.Errorf("a linked work tree: %v", err)
	}
	// A link from outside into a work tree is followed.
	link := filepath.Join(outside, "link")
	if err := os.Symlink(tree, link); err != nil {
		t.Fatal(err)
	}
	if err := RefuseInWorkTree(filepath.Join(link, "out")); !errors.Is(err, ErrInsideWorkTree) {
		t.Errorf("a link into a work tree: %v", err)
	}
}

func TestAFileIsHeldToTheDigestItWasNamedBy(t *testing.T) {
	path := filepath.Join(t.TempDir(), "f")
	if err := os.WriteFile(path, []byte("abc"), 0o600); err != nil {
		t.Fatal(err)
	}
	const abc = "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"
	if got, err := CheckDigest(path, abc, "plan"); err != nil || got != abc {
		t.Fatalf("%q, %v", got, err)
	}
	if _, err := CheckDigest(path, "sha256:"+strings.ToUpper(abc), "plan"); err != nil {
		t.Errorf("the prefix and the case: %v", err)
	}
	if _, err := CheckDigest(path, strings.Repeat("0", 64), "plan"); !errors.Is(err, ErrDigestDiffers) {
		t.Errorf("another digest: %v", err)
	}
	if _, err := CheckDigest(path, "", "plan"); err == nil || !strings.Contains(err.Error(), "-expect-plan") {
		t.Errorf("no digest: %v", err)
	}
	if _, err := CheckDigest(filepath.Join(t.TempDir(), "missing"), abc, "plan"); err == nil {
		t.Error("a missing file")
	}
}

func TestAContentDigestMayBeAPrefixOfAtLeastTwelveDigits(t *testing.T) {
	const got = "sha256:90bf6113aabbccddeeff00112233445566778899aabbccddeeff001122334455"
	for _, ok := range []string{got, "90bf6113aabb", "sha256:90bf6113aabb", "90BF6113AABB"} {
		if err := CheckContentDigest(got, ok); err != nil {
			t.Errorf("%s: %v", ok, err)
		}
	}
	for _, bad := range []string{"", "90bf6113aab", "90bf6113aabc", "sha256:", "ffffffffffff"} {
		if err := CheckContentDigest(got, bad); !errors.Is(err, ErrDigestDiffers) {
			t.Errorf("%q: %v", bad, err)
		}
	}
}

func TestATimingIsTakenOnCIOnly(t *testing.T) {
	env := map[string]string{}
	look := func(k string) (string, bool) { v, ok := env[k]; return v, ok }
	if err := RequireCI(look); !errors.Is(err, ErrNotCI) {
		t.Errorf("no GITHUB_ACTIONS: %v", err)
	}
	env["GITHUB_ACTIONS"] = "false"
	if err := RequireCI(look); !errors.Is(err, ErrNotCI) {
		t.Errorf("GITHUB_ACTIONS=false: %v", err)
	}
	env["GITHUB_ACTIONS"] = "true"
	if err := RequireCI(look); err != nil {
		t.Errorf("on CI: %v", err)
	}
}
