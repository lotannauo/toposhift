//go:build unix

package keys

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"
)

func writeRaw(t testing.TB, dir, content string, mode fs.FileMode) {
	t.Helper()
	p := filepath.Join(dir, FileName)
	if err := os.WriteFile(p, []byte(content), mode); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(p, mode); err != nil { // WriteFile applies the umask
		t.Fatal(err)
	}
}

func readFile(t testing.TB, dir string) string {
	t.Helper()
	b, err := os.ReadFile(filepath.Join(dir, FileName))
	if err != nil {
		t.Fatal(err)
	}
	return string(b)
}

func names(t testing.TB, dir string) []string {
	t.Helper()
	ents, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	var out []string
	for _, e := range ents {
		out = append(out, e.Name())
	}
	slices.Sort(out)
	return out
}

func TestACreatedKeyIsStoredAsItsHashAndShownOnce(t *testing.T) {
	t.Parallel()
	clk := newClock()
	dir := t.TempDir()
	st := openIn(t, dir, clk, 1)

	issued, err := st.Create(ScopeRead, CreateOptions{})
	if err != nil {
		t.Fatal(err)
	}
	secret := issued.Secret.Reveal()
	if _, err := Parse(secret); err != nil {
		t.Fatal(err)
	}
	k := issued.Key
	if !strings.HasPrefix(k.ID, "k_") || len(k.ID) != 14 || k.Scope != ScopeRead || !k.Created.Equal(clk.Now()) || k.Created.Location() != time.UTC || !k.Expires.IsZero() || k.Replaces != "" {
		t.Errorf("key = %+v", k)
	}

	file := readFile(t, dir)
	sum := sha256.Sum256([]byte(secret))
	want := hex.EncodeToString(sum[:])
	if !strings.Contains(file, want) {
		t.Errorf("the file does not hold the SHA-256 of the key:\n%s", file)
	}
	// Nothing of the key itself: not any 8-character run of it, as written or in hex.
	for i := 0; i+8 <= len(secret); i++ {
		run := secret[i : i+8]
		if strings.Contains(file, run) || strings.Contains(file, hex.EncodeToString([]byte(run))) {
			t.Fatalf("the file holds %q of the key, plain or in hex:\n%s", run, file)
		}
	}
	var doc document
	if err := json.Unmarshal([]byte(file), &doc); err != nil {
		t.Fatal(err)
	}
	if doc.Version != 1 || len(doc.Keys) != 1 || doc.Keys[0].ID != k.ID || doc.Keys[0].SHA256 != want {
		t.Errorf("document = %+v", doc)
	}

	// The only way back to a key is the one it came out of: listing and every
	// other read return what it is known by, never the key or its hash.
	list, err := st.List()
	if err != nil || len(list) != 1 || list[0] != k {
		t.Errorf("List = %+v, %v", list, err)
	}
	out := fmt.Sprintf("%+v %#v", list, list)
	if strings.Contains(out, secret) || strings.Contains(out, want) {
		t.Errorf("listing shows the key or its hash: %s", out)
	}

	// Permissions and leftovers.
	info, err := os.Stat(filepath.Join(dir, FileName))
	if err != nil {
		t.Fatal(err)
	}
	if info.Mode().Perm() != 0o600 {
		t.Errorf("keys.json has mode %04o, want 0600", info.Mode().Perm())
	}
	if got, want := names(t, dir), []string{FileName, lockName}; !slices.Equal(got, want) {
		t.Errorf("directory holds %v, want %v", got, want)
	}
	if linfo, err := os.Stat(filepath.Join(dir, lockName)); err != nil || linfo.Mode().Perm() != 0o600 {
		t.Errorf("lock file: %v, %v", linfo, err)
	}
}

func TestAuthenticateChecksTheKeyItsExpiryAndItsScope(t *testing.T) {
	t.Parallel()
	clk := newClock()
	st := openIn(t, t.TempDir(), clk, 1)
	read, err := st.Create(ScopeRead, CreateOptions{})
	if err != nil {
		t.Fatal(err)
	}
	ingest, err := st.Create(ScopeIngest, CreateOptions{})
	if err != nil {
		t.Fatal(err)
	}
	exp := clk.Now().Add(time.Hour)
	short, err := st.Create(ScopeRead, CreateOptions{Expires: exp})
	if err != nil {
		t.Fatal(err)
	}
	stranger, err := openIn(t, t.TempDir(), clk, 9).Create(ScopeRead, CreateOptions{})
	if err != nil {
		t.Fatal(err)
	}

	got, err := st.Authenticate(read.Secret.Reveal(), ScopeRead)
	if err != nil || got != read.Key {
		t.Errorf("read key: %+v, %v", got, err)
	}
	if got, err := st.Authenticate(ingest.Secret.Reveal(), ScopeIngest); err != nil || got != ingest.Key {
		t.Errorf("ingest key: %+v, %v", got, err)
	}
	// A key is for its scope and no other, whichever way.
	if _, err := st.Authenticate(read.Secret.Reveal(), ScopeIngest); !errors.Is(err, ErrScope) {
		t.Errorf("read key for ingest: %v", err)
	}
	if _, err := st.Authenticate(ingest.Secret.Reveal(), ScopeRead); !errors.Is(err, ErrScope) {
		t.Errorf("ingest key for read: %v", err)
	}
	if _, err := st.Authenticate(stranger.Secret.Reveal(), ScopeRead); !errors.Is(err, ErrUnknown) {
		t.Errorf("a well formed key nobody issued: %v", err)
	}

	// Expiry: valid until the instant, expired at it.
	clk.Advance(time.Hour - time.Nanosecond)
	if _, err := st.Authenticate(short.Secret.Reveal(), ScopeRead); err != nil {
		t.Errorf("one nanosecond before expiry: %v", err)
	}
	clk.Advance(time.Nanosecond)
	if _, err := st.Authenticate(short.Secret.Reveal(), ScopeRead); !errors.Is(err, ErrExpired) {
		t.Errorf("at expiry: %v", err)
	}
	// Other keys are unaffected.
	if _, err := st.Authenticate(read.Secret.Reveal(), ScopeRead); err != nil {
		t.Error(err)
	}
	if !short.Key.ExpiredAt(exp) || short.Key.ExpiredAt(exp.Add(-1)) || read.Key.ExpiredAt(exp.AddDate(1000, 0, 0)) {
		t.Error("ExpiredAt is wrong")
	}
}

func TestAMistypedKeyIsRefusedBeforeAnyFileIsRead(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	st := openIn(t, dir, nil, 1)
	issued, err := st.Create(ScopeRead, CreateOptions{})
	if err != nil {
		t.Fatal(err)
	}
	good := issued.Secret.Reveal()
	typo := good[:20] + string(rune(good[20])^1) + good[21:] // one character off
	if _, err := Parse(typo); !errors.Is(err, ErrMalformed) {
		t.Fatalf("test setup: the typo parses: %v", err)
	}

	// Break the file so that any read of it fails, and use a Store that has not
	// read it yet.
	writeRaw(t, dir, "not json", 0o600)
	fresh := openIn(t, dir, nil, 2)
	if _, err := fresh.Authenticate(typo, ScopeRead); !errors.Is(err, ErrMalformed) {
		t.Errorf("a typo with a broken file: %v, want ErrMalformed (no read)", err)
	}
	if _, err := fresh.Authenticate(good, ScopeRead); !errors.Is(err, ErrUnavailable) {
		t.Errorf("a good key with a broken file: %v, want ErrUnavailable", err)
	}
	for _, s := range []string{"", "x", "Bearer " + good} {
		if _, err := fresh.Authenticate(s, ScopeRead); !errors.Is(err, ErrMalformed) {
			t.Errorf("%q: %v", s, err)
		}
	}
}

func TestRevokeStopsAKeyAtOnceHereAndWithinASecondElsewhere(t *testing.T) {
	t.Parallel()
	clk := newClock()
	dir := t.TempDir()
	cli := openIn(t, dir, clk, 1)   // the command
	serve := openIn(t, dir, clk, 2) // the running server
	a, err := cli.Create(ScopeRead, CreateOptions{})
	if err != nil {
		t.Fatal(err)
	}
	b, err := cli.Create(ScopeRead, CreateOptions{})
	if err != nil {
		t.Fatal(err)
	}
	for _, k := range []Issued{a, b} {
		if _, err := serve.Authenticate(k.Secret.Reveal(), ScopeRead); err != nil {
			t.Fatal(err)
		}
	}

	if err := cli.Revoke(a.Key.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := cli.Authenticate(a.Secret.Reveal(), ScopeRead); !errors.Is(err, ErrUnknown) {
		t.Errorf("revoked key in the revoking Store: %v", err)
	}
	// The server has looked within the last second: it may still honour it.
	// After a second it must not.
	clk.Advance(DefaultCheckEvery - time.Nanosecond)
	if _, err := serve.Authenticate(a.Secret.Reveal(), ScopeRead); err != nil {
		t.Errorf("inside the check interval the server still had the old file: %v", err)
	}
	clk.Advance(time.Nanosecond)
	if _, err := serve.Authenticate(a.Secret.Reveal(), ScopeRead); !errors.Is(err, ErrUnknown) {
		t.Errorf("after the check interval: %v, want ErrUnknown", err)
	}
	if _, err := serve.Authenticate(b.Secret.Reveal(), ScopeRead); err != nil {
		t.Errorf("the other key: %v", err)
	}
	if err := cli.Revoke(a.Key.ID); !errors.Is(err, ErrNotFound) {
		t.Errorf("revoking twice: %v", err)
	}
	if err := cli.Revoke("nope"); !errors.Is(err, ErrNotFound) {
		t.Errorf("revoking an unknown id: %v", err)
	}
	list, _ := cli.List()
	if len(list) != 1 || list[0].ID != b.Key.ID {
		t.Errorf("List = %+v", list)
	}
	if strings.Contains(readFile(t, dir), a.Key.ID) {
		t.Error("the revoked key is still in the file")
	}
}

func TestRotateOverlapsThenEnds(t *testing.T) {
	t.Parallel()
	clk := newClock()
	st := openIn(t, t.TempDir(), clk, 1)
	old, err := st.Create(ScopeIngest, CreateOptions{})
	if err != nil {
		t.Fatal(err)
	}
	rot, err := st.Rotate(old.Key.ID, RotateOptions{Overlap: 24 * time.Hour})
	if err != nil {
		t.Fatal(err)
	}
	if rot.Key.Scope != ScopeIngest || rot.Key.Replaces != old.Key.ID || rot.Key.ID == old.Key.ID || !rot.Key.Expires.IsZero() {
		t.Errorf("new key = %+v", rot.Key)
	}
	if want := clk.Now().Add(24 * time.Hour); !rot.Old.Expires.Equal(want) || rot.Old.ID != old.Key.ID {
		t.Errorf("old key = %+v, want expiry %v", rot.Old, want)
	}
	if rot.Secret.Reveal() == old.Secret.Reveal() {
		t.Error("the rotation reissued the same key")
	}

	for _, sec := range []Secret{old.Secret, rot.Secret} {
		if _, err := st.Authenticate(sec.Reveal(), ScopeIngest); err != nil {
			t.Errorf("inside the overlap: %v", err)
		}
	}
	clk.Advance(24*time.Hour - time.Nanosecond)
	if _, err := st.Authenticate(old.Secret.Reveal(), ScopeIngest); err != nil {
		t.Errorf("just inside the overlap: %v", err)
	}
	clk.Advance(time.Nanosecond)
	if _, err := st.Authenticate(old.Secret.Reveal(), ScopeIngest); !errors.Is(err, ErrExpired) {
		t.Errorf("at the end of the overlap: %v, want ErrExpired", err)
	}
	if _, err := st.Authenticate(rot.Secret.Reveal(), ScopeIngest); err != nil {
		t.Errorf("the new key after the overlap: %v", err)
	}
	// An expired key cannot be rotated.
	if _, err := st.Rotate(old.Key.ID, RotateOptions{}); !errors.Is(err, ErrExpired) {
		t.Errorf("rotating an expired key: %v", err)
	}
}

func TestRotateNeverLengthensALifeAndZeroOverlapEndsTheOldKeyNow(t *testing.T) {
	t.Parallel()
	clk := newClock()
	st := openIn(t, t.TempDir(), clk, 1)
	soon := clk.Now().Add(time.Hour)
	k, err := st.Create(ScopeRead, CreateOptions{Expires: soon})
	if err != nil {
		t.Fatal(err)
	}
	rot, err := st.Rotate(k.Key.ID, RotateOptions{Overlap: 48 * time.Hour})
	if err != nil {
		t.Fatal(err)
	}
	if !rot.Old.Expires.Equal(soon) {
		t.Errorf("old expiry = %v, want it kept at %v", rot.Old.Expires, soon)
	}

	k2, err := st.Create(ScopeRead, CreateOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := st.Rotate(k2.Key.ID, RotateOptions{Overlap: 0}); err != nil {
		t.Fatal(err)
	}
	if _, err := st.Authenticate(k2.Secret.Reveal(), ScopeRead); !errors.Is(err, ErrExpired) {
		t.Errorf("a key rotated with no overlap: %v, want ErrExpired", err)
	}

	// The new key can have an expiry of its own.
	k3, err := st.Create(ScopeRead, CreateOptions{})
	if err != nil {
		t.Fatal(err)
	}
	exp := clk.Now().Add(72 * time.Hour)
	rot3, err := st.Rotate(k3.Key.ID, RotateOptions{Overlap: time.Hour, Expires: exp})
	if err != nil || !rot3.Key.Expires.Equal(exp) {
		t.Errorf("new key expiry: %+v, %v", rot3.Key, err)
	}

	for name, opt := range map[string]RotateOptions{
		"negative overlap": {Overlap: -time.Second},
		"overlap too long": {Overlap: MaxOverlap + time.Second},
		"expiry past":      {Expires: clk.Now()},
	} {
		before := readFile(t, filepath.Dir(st.Path()))
		if _, err := st.Rotate(k3.Key.ID, opt); err == nil {
			t.Errorf("%s: accepted", name)
		}
		if after := readFile(t, filepath.Dir(st.Path())); after != before {
			t.Errorf("%s: a refused rotation changed the file", name)
		}
	}
	if _, err := st.Rotate("nope", RotateOptions{}); !errors.Is(err, ErrNotFound) {
		t.Errorf("unknown id: %v", err)
	}
}

func TestCreateRefusesBadInput(t *testing.T) {
	t.Parallel()
	clk := newClock()
	dir := t.TempDir()
	st := openIn(t, dir, clk, 1)
	for name, tc := range map[string]struct {
		scope Scope
		opt   CreateOptions
	}{
		"unknown scope":    {"admin", CreateOptions{}},
		"empty scope":      {"", CreateOptions{}},
		"expiry now":       {ScopeRead, CreateOptions{Expires: clk.Now()}},
		"expiry past":      {ScopeRead, CreateOptions{Expires: clk.Now().Add(-time.Hour)}},
		"expiry past 9999": {ScopeRead, CreateOptions{Expires: time.Date(10000, 1, 1, 0, 0, 0, 0, time.UTC)}},
	} {
		if _, err := st.Create(tc.scope, tc.opt); err == nil {
			t.Errorf("%s: accepted", name)
		}
	}
	if _, err := os.Stat(filepath.Join(dir, FileName)); !errors.Is(err, fs.ErrNotExist) {
		t.Errorf("refused creations wrote a file: %v", err)
	}
}

func TestTheFileHoldsAtMostMaxKeys(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	var doc document
	doc.Version = 1
	for i := range MaxKeys {
		h := sha256.Sum256(fmt.Appendf(nil, "k%d", i))
		doc.Keys = append(doc.Keys, record{
			Key:    Key{ID: fmt.Sprintf("k_%012d", i), Scope: ScopeRead, Created: newClock().Now()},
			SHA256: hex.EncodeToString(h[:]),
		})
	}
	b, _ := json.Marshal(doc)
	writeRaw(t, dir, string(b), 0o600)
	st := openIn(t, dir, nil, 1)
	if _, err := st.Create(ScopeRead, CreateOptions{}); !errors.Is(err, ErrFull) {
		t.Errorf("Create on a full file: %v", err)
	}
	if _, err := st.Rotate("k_000000000001", RotateOptions{}); !errors.Is(err, ErrFull) {
		t.Errorf("Rotate on a full file: %v", err)
	}
	if err := st.Revoke("k_000000000001"); err != nil {
		t.Fatal(err)
	}
	if _, err := st.Create(ScopeRead, CreateOptions{}); err != nil {
		t.Errorf("Create after a revoke: %v", err)
	}
	// One more than the limit in a file is refused whole.
	doc.Keys = append(doc.Keys, doc.Keys[0])
	b, _ = json.Marshal(doc)
	writeRaw(t, dir, string(b), 0o600)
	if _, err := openIn(t, dir, nil, 3).List(); !errors.Is(err, ErrUnavailable) {
		t.Errorf("List of an oversized file: %v", err)
	}
}

func TestAnIdOrKeyDrawnTwiceIsNeverStoredTwice(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	// A broken random source that repeats itself.
	st, err := Open(dir, Options{Now: newClock().Now, Rand: bytes.NewReader(bytes.Repeat([]byte{7}, 4096))})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := st.Create(ScopeRead, CreateOptions{}); err != nil {
		t.Fatal(err)
	}
	if _, err := st.Create(ScopeRead, CreateOptions{}); err == nil {
		t.Error("a repeated draw was stored as a second key")
	}
	list, _ := st.List()
	if len(list) != 1 {
		t.Errorf("%d keys, want 1", len(list))
	}

	// A source that runs dry fails the call and leaves the file alone.
	dir2 := t.TempDir()
	dry, err := Open(dir2, Options{Now: newClock().Now, Rand: bytes.NewReader(make([]byte, 10))})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := dry.Create(ScopeRead, CreateOptions{}); err == nil {
		t.Error("Create succeeded with too few random bytes")
	}
	if _, err := os.Stat(filepath.Join(dir2, FileName)); !errors.Is(err, fs.ErrNotExist) {
		t.Errorf("a failed Create wrote a file: %v", err)
	}
}

func TestAStoreSeesAnotherProcessesChanges(t *testing.T) {
	t.Parallel()
	clk := newClock()
	dir := t.TempDir()
	cli := openIn(t, dir, clk, 1)
	serve := openIn(t, dir, clk, 2)

	// Before any file, every key is unknown, and that is not an error.
	other, err := openIn(t, t.TempDir(), clk, 5).Create(ScopeRead, CreateOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := serve.Authenticate(other.Secret.Reveal(), ScopeRead); !errors.Is(err, ErrUnknown) {
		t.Fatalf("no file: %v", err)
	}
	k, err := cli.Create(ScopeRead, CreateOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := serve.Authenticate(k.Secret.Reveal(), ScopeRead); !errors.Is(err, ErrUnknown) {
		t.Fatalf("inside the check interval the server has not looked: %v", err)
	}
	clk.Advance(DefaultCheckEvery)
	if _, err := serve.Authenticate(k.Secret.Reveal(), ScopeRead); err != nil {
		t.Fatalf("after the interval: %v", err)
	}

	// The file removed: no keys, not an error.
	if err := os.Remove(filepath.Join(dir, FileName)); err != nil {
		t.Fatal(err)
	}
	clk.Advance(DefaultCheckEvery)
	if _, err := serve.Authenticate(k.Secret.Reveal(), ScopeRead); !errors.Is(err, ErrUnknown) {
		t.Fatalf("file removed: %v, want ErrUnknown", err)
	}
	// And a new key is seen when it is back.
	k2, err := cli.Create(ScopeRead, CreateOptions{})
	if err != nil {
		t.Fatal(err)
	}
	clk.Advance(DefaultCheckEvery)
	if _, err := serve.Authenticate(k2.Secret.Reveal(), ScopeRead); err != nil {
		t.Fatalf("recreated: %v", err)
	}
}

func TestAnUntrustworthyFileDeniesEveryKeyUntilItIsRepaired(t *testing.T) {
	t.Parallel()
	clk := newClock()
	dir := t.TempDir()
	cli := openIn(t, dir, clk, 1)
	serve := openIn(t, dir, clk, 2)
	k, err := cli.Create(ScopeRead, CreateOptions{})
	if err != nil {
		t.Fatal(err)
	}
	good := readFile(t, dir)
	if _, err := serve.Authenticate(k.Secret.Reveal(), ScopeRead); err != nil {
		t.Fatal(err)
	}

	var doc map[string]any
	if err := json.Unmarshal([]byte(good), &doc); err != nil {
		t.Fatal(err)
	}
	rec := doc["keys"].([]any)[0].(map[string]any)
	withRec := func(edit func(r map[string]any)) string {
		c := map[string]any{}
		for key, v := range rec {
			c[key] = v
		}
		edit(c)
		b, _ := json.Marshal(map[string]any{"version": 1, "keys": []any{c}})
		return string(b)
	}
	dup, _ := json.Marshal(map[string]any{"version": 1, "keys": []any{rec, rec}})
	dupID := func() string {
		c := map[string]any{}
		for key, v := range rec {
			c[key] = v
		}
		c["sha256"] = strings.Repeat("ab", 32)
		b, _ := json.Marshal(map[string]any{"version": 1, "keys": []any{rec, c}})
		return string(b)
	}()
	dupHash := func() string {
		c := map[string]any{}
		for key, v := range rec {
			c[key] = v
		}
		c["id"] = "k_other"
		b, _ := json.Marshal(map[string]any{"version": 1, "keys": []any{rec, c}})
		return string(b)
	}()
	for name, content := range map[string]string{
		"empty file":           "",
		"not JSON":             "{",
		"an array":             "[]",
		"null":                 "null",
		"trailing data":        good + "{}",
		"unknown top member":   `{"version":1,"keys":[],"extra":1}`,
		"unknown key member":   withRec(func(r map[string]any) { r["note"] = "x" }),
		"version 0":            `{"keys":[]}`,
		"version 2":            `{"version":2,"keys":[]}`,
		"id empty":             withRec(func(r map[string]any) { r["id"] = "" }),
		"id with upper case":   withRec(func(r map[string]any) { r["id"] = "K_1" }),
		"id with a slash":      withRec(func(r map[string]any) { r["id"] = "k/1" }),
		"id too long":          withRec(func(r map[string]any) { r["id"] = strings.Repeat("a", 65) }),
		"scope unknown":        withRec(func(r map[string]any) { r["scope"] = "admin" }),
		"scope missing":        withRec(func(r map[string]any) { delete(r, "scope") }),
		"no created":           withRec(func(r map[string]any) { delete(r, "created") }),
		"created not a time":   withRec(func(r map[string]any) { r["created"] = "yesterday" }),
		"replaces malformed":   withRec(func(r map[string]any) { r["replaces"] = "A B" }),
		"hash short":           withRec(func(r map[string]any) { r["sha256"] = "ab" }),
		"hash missing":         withRec(func(r map[string]any) { delete(r, "sha256") }),
		"hash not hex":         withRec(func(r map[string]any) { r["sha256"] = strings.Repeat("z", 64) }),
		"hash in upper case":   withRec(func(r map[string]any) { r["sha256"] = strings.ToUpper(r["sha256"].(string)) }),
		"id and hash repeated": string(dup),
		"id repeated":          dupID,
		"hash repeated":        dupHash,
		"larger than 4 MiB":    `{"version":1,"keys":[],"pad":"` + strings.Repeat("a", maxFileSize) + `"}`,
	} {
		writeRaw(t, dir, content, 0o600)
		clk.Advance(DefaultCheckEvery)
		if _, err := serve.Authenticate(k.Secret.Reveal(), ScopeRead); !errors.Is(err, ErrUnavailable) {
			t.Errorf("%s: %v, want ErrUnavailable", name, err)
		}
		// A change to the keys is refused rather than written over the damage.
		if _, err := cli.Create(ScopeRead, CreateOptions{}); !errors.Is(err, ErrUnavailable) {
			t.Errorf("%s: Create over a damaged file: %v, want ErrUnavailable", name, err)
		}
		if got := readFile(t, dir); got != content {
			t.Errorf("%s: a refused Create changed the file", name)
		}
		if _, err := cli.List(); !errors.Is(err, ErrUnavailable) {
			t.Errorf("%s: List: %v", name, err)
		}
		// The error names no part of the key.
		_, err := serve.Authenticate(k.Secret.Reveal(), ScopeRead)
		if err != nil && strings.Contains(err.Error(), k.Secret.Reveal()[6:30]) {
			t.Errorf("%s: the error holds the key: %v", name, err)
		}
	}

	// Repaired, the key works again within the interval.
	writeRaw(t, dir, good, 0o600)
	clk.Advance(DefaultCheckEvery)
	if _, err := serve.Authenticate(k.Secret.Reveal(), ScopeRead); err != nil {
		t.Errorf("after the repair: %v", err)
	}
}

func TestAKeyFileThatOthersCanWriteIsNotTrusted(t *testing.T) {
	t.Parallel()
	clk := newClock()
	dir := t.TempDir()
	cli := openIn(t, dir, clk, 1)
	serve := openIn(t, dir, clk, 2)
	k, err := cli.Create(ScopeRead, CreateOptions{})
	if err != nil {
		t.Fatal(err)
	}
	p := filepath.Join(dir, FileName)
	for _, mode := range []fs.FileMode{0o620, 0o602, 0o666, 0o660} {
		if err := os.Chmod(p, mode); err != nil {
			t.Fatal(err)
		}
		clk.Advance(DefaultCheckEvery)
		_, err := serve.Authenticate(k.Secret.Reveal(), ScopeRead)
		if !errors.Is(err, ErrUnavailable) || !strings.Contains(err.Error(), "0600") {
			t.Errorf("mode %04o: %v, want ErrUnavailable naming 0600", mode, err)
		}
		if _, err := cli.Create(ScopeRead, CreateOptions{}); !errors.Is(err, ErrUnavailable) {
			t.Errorf("mode %04o: Create: %v", mode, err)
		}
	}
	// Readable by others is not a reason: the file holds hashes of 256-bit keys.
	for _, mode := range []fs.FileMode{0o600, 0o640, 0o644, 0o400} {
		if err := os.Chmod(p, mode); err != nil {
			t.Fatal(err)
		}
		clk.Advance(DefaultCheckEvery)
		if _, err := serve.Authenticate(k.Secret.Reveal(), ScopeRead); err != nil {
			t.Errorf("mode %04o: %v", mode, err)
		}
	}
}

func TestChangesFromManyStoresAreNotLost(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	const stores, each = 4, 12
	var wg sync.WaitGroup
	var mu sync.Mutex
	var all []Issued
	for s := range stores {
		st := openIn(t, dir, nil, byte(10+s)) // separate Stores: separate locks in process, one file lock between them
		for range 3 {                         // three goroutines on each Store
			wg.Add(1)
			go func() {
				defer wg.Done()
				for range each / 3 {
					k, err := st.Create(ScopeRead, CreateOptions{})
					if err != nil {
						t.Error(err)
						return
					}
					mu.Lock()
					all = append(all, k)
					mu.Unlock()
				}
			}()
		}
	}
	wg.Wait()
	if len(all) != stores*each {
		t.Fatalf("%d keys made, want %d", len(all), stores*each)
	}
	check := openIn(t, dir, nil, 99)
	list, err := check.List()
	if err != nil {
		t.Fatal(err)
	}
	if len(list) != len(all) {
		t.Fatalf("%d keys in the file, want %d: updates were lost", len(list), len(all))
	}
	for _, k := range all {
		if _, err := check.Authenticate(k.Secret.Reveal(), ScopeRead); err != nil {
			t.Errorf("key %s: %v", k.Key.ID, err)
		}
	}
	if got, want := names(t, dir), []string{FileName, lockName}; !slices.Equal(got, want) {
		t.Errorf("directory holds %v, want %v", got, want)
	}
}

func TestChangesWaitForTheFileLock(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	st := openIn(t, dir, nil, 1)
	st.lockWait = 200 * time.Millisecond

	f, err := os.OpenFile(filepath.Join(dir, lockName), os.O_RDWR|os.O_CREATE, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	if err := tryLock(f.Fd()); err != nil {
		t.Fatal(err)
	}
	start := time.Now()
	if _, err := st.Create(ScopeRead, CreateOptions{}); !errors.Is(err, ErrLocked) {
		t.Errorf("Create under a held lock: %v, want ErrLocked", err)
	}
	if time.Since(start) < 150*time.Millisecond {
		t.Error("gave up before waiting")
	}
	// Authentication does not wait for the lock a change holds.
	if _, err := st.Authenticate(strings.Repeat("x", 55), ScopeRead); !errors.Is(err, ErrMalformed) {
		t.Error(err)
	}
	other, _ := openIn(t, t.TempDir(), nil, 3).Create(ScopeRead, CreateOptions{})
	done := make(chan error, 1)
	go func() { _, err := st.Authenticate(other.Secret.Reveal(), ScopeRead); done <- err }()
	select {
	case err := <-done:
		if !errors.Is(err, ErrUnknown) {
			t.Error(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Authenticate waited on the file lock")
	}

	// Released, the next change goes through, and one that was waiting does.
	st.lockWait = 5 * time.Second
	res := make(chan error, 1)
	go func() { _, err := st.Create(ScopeRead, CreateOptions{}); res <- err }()
	time.Sleep(50 * time.Millisecond)
	_ = f.Close()
	if err := <-res; err != nil {
		t.Errorf("Create after the lock was released: %v", err)
	}
}

// Every stored hash is compared on every lookup, whether or not one matches and
// wherever it is in the file: the time does not say which key matched.
func TestEveryStoredHashIsComparedOnEveryLookup(t *testing.T) {
	st := openIn(t, t.TempDir(), nil, 1)
	var issued []Issued
	for range 5 {
		k, err := st.Create(ScopeRead, CreateOptions{})
		if err != nil {
			t.Fatal(err)
		}
		issued = append(issued, k)
	}
	stranger, _ := openIn(t, t.TempDir(), nil, 2).Create(ScopeRead, CreateOptions{})

	calls := 0
	orig := hashEqual
	hashEqual = func(a, b []byte) int { calls++; return orig(a, b) }
	t.Cleanup(func() { hashEqual = orig })

	for i, k := range issued {
		calls = 0
		if _, err := st.Authenticate(k.Secret.Reveal(), ScopeRead); err != nil {
			t.Fatal(err)
		}
		if calls != len(issued) {
			t.Errorf("key %d: %d comparisons, want %d", i, calls, len(issued))
		}
	}
	calls = 0
	if _, err := st.Authenticate(stranger.Secret.Reveal(), ScopeRead); !errors.Is(err, ErrUnknown) {
		t.Fatal(err)
	}
	if calls != len(issued) {
		t.Errorf("unknown key: %d comparisons, want %d", calls, len(issued))
	}
	// A typo is refused before any comparison.
	calls = 0
	typo := issued[0].Secret.Reveal()[:30] + "x" + issued[0].Secret.Reveal()[31:]
	_, _ = st.Authenticate(typo, ScopeRead)
	if calls != 0 {
		t.Errorf("a malformed key was compared %d times", calls)
	}
}

func TestAuthenticateRequestReadsTheBearerHeaderOnly(t *testing.T) {
	t.Parallel()
	st := newTestStore(t, nil)
	k, err := st.Create(ScopeRead, CreateOptions{})
	if err != nil {
		t.Fatal(err)
	}
	secret := k.Secret.Reveal()
	mk := func(edit func(r *http.Request)) *http.Request {
		r := httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/mcp", http.NoBody)
		edit(r)
		return r
	}
	got, err := st.AuthenticateRequest(mk(func(r *http.Request) { r.Header.Set("Authorization", "Bearer "+secret) }), ScopeRead)
	if err != nil || got != k.Key {
		t.Errorf("a request with the header: %+v, %v", got, err)
	}
	for name, r := range map[string]*http.Request{
		"no header":          mk(func(*http.Request) {}),
		"query access_token": mk(func(r *http.Request) { r.URL.RawQuery = "access_token=" + secret }),
		"query key":          mk(func(r *http.Request) { r.URL.RawQuery = "key=" + secret + "&token=" + secret }),
		"path":               mk(func(r *http.Request) { r.URL.Path = "/" + secret }),
		"another header":     mk(func(r *http.Request) { r.Header.Set("X-Api-Key", secret) }),
		"cookie":             mk(func(r *http.Request) { r.AddCookie(&http.Cookie{Name: "key", Value: secret}) }),
		"basic":              mk(func(r *http.Request) { r.SetBasicAuth("u", secret) }),
	} {
		if _, err := st.AuthenticateRequest(r, ScopeRead); !errors.Is(err, ErrMalformed) {
			t.Errorf("%s: %v, want ErrMalformed", name, err)
		}
	}
	if _, err := st.AuthenticateRequest(mk(func(r *http.Request) { r.Header.Set("Authorization", "Bearer "+secret) }), ScopeIngest); !errors.Is(err, ErrScope) {
		t.Errorf("wrong scope: %v", err)
	}
}

func TestNoErrorHoldsAKey(t *testing.T) {
	t.Parallel()
	clk := newClock()
	dir := t.TempDir()
	st := openIn(t, dir, clk, 1)
	live, _ := st.Create(ScopeRead, CreateOptions{})
	short, _ := st.Create(ScopeRead, CreateOptions{Expires: clk.Now().Add(time.Second)})
	gone, _ := st.Create(ScopeRead, CreateOptions{})
	if err := st.Revoke(gone.Key.ID); err != nil {
		t.Fatal(err)
	}
	clk.Advance(time.Minute)
	typo := live.Secret.Reveal()[:10] + "!" + live.Secret.Reveal()[11:]

	var errs []error
	for _, c := range []struct {
		key   string
		scope Scope
	}{
		{short.Secret.Reveal(), ScopeRead}, {gone.Secret.Reveal(), ScopeRead}, {live.Secret.Reveal(), ScopeIngest}, {typo, ScopeRead},
	} {
		_, err := st.Authenticate(c.key, c.scope)
		errs = append(errs, err)
	}
	_, err := st.Rotate(short.Key.ID, RotateOptions{})
	errs = append(errs, err)
	writeRaw(t, dir, "garbage "+live.Secret.Reveal()[:20], 0o600)
	_, err = openIn(t, dir, clk, 4).Authenticate(live.Secret.Reveal(), ScopeRead)
	errs = append(errs, err)
	for i, err := range errs {
		if err == nil {
			t.Errorf("case %d: no error", i)
			continue
		}
		msg := err.Error()
		for _, s := range []Secret{live.Secret, short.Secret, gone.Secret} {
			text := s.Reveal()
			for j := 0; j+10 <= len(text); j += 5 {
				if strings.Contains(msg, text[j:j+10]) {
					t.Errorf("case %d: error holds part of a key: %v", i, err)
				}
			}
		}
	}
}

func TestOpenNeedsAnExistingDirectory(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	if _, err := Open(filepath.Join(dir, "missing"), Options{}); err == nil {
		t.Error("a missing directory was accepted")
	}
	f := filepath.Join(dir, "file")
	if err := os.WriteFile(f, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := Open(f, Options{}); err == nil {
		t.Error("a file was accepted as the directory")
	}
	if _, err := Open(dir, Options{CheckEvery: -1}); err == nil {
		t.Error("a negative CheckEvery was accepted")
	}
	st, err := Open(dir, Options{})
	if err != nil {
		t.Fatal(err)
	}
	if st.Path() != filepath.Join(dir, FileName) {
		t.Errorf("Path = %s", st.Path())
	}
	// With the real clock and randomness a key can be made and used.
	k, err := st.Create(ScopeRead, CreateOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := st.Authenticate(k.Secret.Reveal(), ScopeRead); err != nil {
		t.Error(err)
	}
	other, err := Open(dir, Options{})
	if err != nil {
		t.Fatal(err)
	}
	k2, err := other.Create(ScopeRead, CreateOptions{})
	if err != nil || k2.Secret.Reveal() == k.Secret.Reveal() {
		t.Errorf("two Stores on crypto/rand made the same key: %v", err)
	}
}
