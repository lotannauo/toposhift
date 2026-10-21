package pgspike_test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5"

	"github.com/lotannauo/toposhift/bench/postgres/pgengine"
	"github.com/lotannauo/toposhift/bench/postgres/pgspike"
)

func TestAnOutputDirectoryInsideAGitWorkTreeIsRefused(t *testing.T) {
	root := t.TempDir()
	// A work tree has a .git directory; a linked work tree has a .git file.
	tree := filepath.Join(root, "tree")
	linked := filepath.Join(root, "linked")
	outside := filepath.Join(root, "outside")
	if err := os.MkdirAll(filepath.Join(tree, ".git"), 0o700); err != nil {
		t.Fatal(err)
	}
	for _, d := range []string{filepath.Join(linked, "sub"), outside} {
		if err := os.MkdirAll(d, 0o700); err != nil {
			t.Fatal(err)
		}
	}
	if err := os.WriteFile(filepath.Join(linked, ".git"), []byte("gitdir: elsewhere\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	link := filepath.Join(outside, "into-tree")
	if err := os.Symlink(tree, link); err != nil {
		t.Fatal(err)
	}
	for name, dir := range map[string]string{
		"the root of the tree":              tree,
		"a directory that does not exist":   filepath.Join(tree, "a", "b", "c"),
		"a directory of a linked work tree": filepath.Join(linked, "sub"),
		"a link into the tree":              filepath.Join(link, "out"),
	} {
		if err := pgspike.RefuseInWorkTree(dir); !errors.Is(err, pgspike.ErrInsideWorkTree) {
			t.Errorf("%s: %v", name, err)
		}
	}
	for name, dir := range map[string]string{
		"a directory beside it":           outside,
		"a directory that does not exist": filepath.Join(outside, "new", "dir"),
	} {
		if err := pgspike.RefuseInWorkTree(dir); err != nil {
			t.Errorf("%s: %v", name, err)
		}
	}
}

func TestAFileThatIsNotNamedByItsDigestIsRefused(t *testing.T) {
	path := filepath.Join(t.TempDir(), "plan.json")
	if err := os.WriteFile(path, []byte("a plan"), 0o600); err != nil {
		t.Fatal(err)
	}
	sum := sha256.Sum256([]byte("a plan"))
	good := hex.EncodeToString(sum[:])
	if got, err := pgspike.CheckDigest(path, good, "plan"); err != nil || got != good {
		t.Errorf("the right digest: %q, %v", got, err)
	}
	if _, err := pgspike.CheckDigest(path, "sha256:"+strings.ToUpper(good), "plan"); err != nil {
		t.Errorf("the right digest with its prefix, in capitals: %v", err)
	}
	if _, err := pgspike.CheckDigest(path, good[:63]+"0", "plan"); !errors.Is(err, pgspike.ErrDigestDiffers) {
		t.Errorf("a digest that differs in its last digit: %v", err)
	}
	if _, err := pgspike.CheckDigest(path, "", "plan"); err == nil || errors.Is(err, pgspike.ErrDigestDiffers) {
		t.Errorf("no digest asked for: %v", err)
	}
	if _, err := pgspike.CheckDigest(filepath.Join(t.TempDir(), "none"), good, "plan"); err == nil {
		t.Error("a missing file")
	}
}

func TestAContentDigestMayBeQuotedAsAPrefixOfTwelveDigitsOrMore(t *testing.T) {
	const got = "sha256:90bf6113aaaabbbbccccddddeeeeffff0000111122223333444455556666"
	for _, ok := range []string{got, "sha256:90bf6113aaaa", "90bf6113AAAA", "sha256:90bf6113aaaabbbb"} {
		if err := pgspike.CheckContentDigest(got, ok); err != nil {
			t.Errorf("%s: %v", ok, err)
		}
	}
	for _, bad := range []string{"sha256:90bf6113", "", "sha256:90bf6113aaab", "sha256:d2a61b1c0000"} {
		if err := pgspike.CheckContentDigest(got, bad); !errors.Is(err, pgspike.ErrDigestDiffers) {
			t.Errorf("%q: %v", bad, err)
		}
	}
}

type show map[string]string

type row struct{ v *string }

func (r row) Scan(dest ...any) error {
	*dest[0].(**string) = r.v
	return nil
}

func (s show) QueryRow(_ context.Context, _ string, args ...any) pgx.Row {
	if v, ok := s[args[0].(string)]; ok {
		return row{v: &v}
	}
	return row{}
}

func settingsFile(t *testing.T) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "settings.conf")
	if err := os.WriteFile(path, []byte("shared_buffers = 4GB\njit = off\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

func TestAServerWhoseSettingsDifferFromTheFileIsRefused(t *testing.T) {
	path := settingsFile(t)
	sum, err := pgspike.RequireSettings(context.Background(), show{"shared_buffers": "4GB", "jit": "off"}, path)
	if err != nil || len(sum) != 64 {
		t.Fatalf("a server with the settings: %q, %v", sum, err)
	}
	for name, srv := range map[string]show{
		"a different value": {"shared_buffers": "128MB", "jit": "off"},
		"a missing setting": {"jit": "off"},
	} {
		if _, err := pgspike.RequireSettings(context.Background(), srv, path); !errors.Is(err, pgengine.ErrSettingsDiffer) {
			t.Errorf("%s: %v", name, err)
		}
	}
	if _, err := pgspike.RequireSettings(context.Background(), show{}, filepath.Join(t.TempDir(), "none")); err == nil {
		t.Error("a settings file that is not there")
	}
}

// run runs pgspike with a server that is not there: a refusal that happens before the
// server is reached is the guard's, and is not a failure to connect.
func run(t *testing.T, args ...string) error {
	t.Helper()
	var out, errs bytes.Buffer
	return pgspike.Run(context.Background(), args, &out, &errs)
}

const noServer = "host=127.0.0.1 port=1 user=postgres dbname=postgres sslmode=disable connect_timeout=1"

func TestTheCommandsRefuseBeforeTheyReachTheServer(t *testing.T) {
	root := t.TempDir()
	tree := filepath.Join(root, "tree")
	if err := os.MkdirAll(filepath.Join(tree, ".git"), 0o700); err != nil {
		t.Fatal(err)
	}
	plan := filepath.Join(root, "plan.json")
	if err := os.WriteFile(plan, []byte("not a plan"), 0o600); err != nil {
		t.Fatal(err)
	}
	sum := sha256.Sum256([]byte("not a plan"))
	settings := settingsFile(t)
	base := []string{"-dsn", noServer, "-settings", settings, "-arm", "pg-bulk", "-schema", "s_x"}
	with := func(extra ...string) []string { return append(append([]string{}, base...), extra...) }

	for name, c := range map[string]struct {
		args []string
		is   error
	}{
		"load, out inside a work tree": {append([]string{"load"}, with("-out", filepath.Join(tree, "out"), "-plan", plan, "-expect-plan", hex.EncodeToString(sum[:]))...), pgspike.ErrInsideWorkTree},
		"read, out inside a work tree": {append([]string{"read"}, with("-out", filepath.Join(tree, "out"), "-plan", plan, "-expect-plan", hex.EncodeToString(sum[:]))...), pgspike.ErrInsideWorkTree},
		"questions, out inside a tree": {[]string{"questions", "-activity", plan, "-expect-activity", "sha256:0123456789ab", "-out", tree}, pgspike.ErrInsideWorkTree},
		"load, a plan that differs":    {append([]string{"load"}, with("-out", filepath.Join(root, "o"), "-plan", plan, "-expect-plan", strings.Repeat("0", 64))...), pgspike.ErrDigestDiffers},
		"read, a plan that differs":    {append([]string{"read"}, with("-out", filepath.Join(root, "o"), "-plan", plan, "-expect-plan", strings.Repeat("0", 64))...), pgspike.ErrDigestDiffers},
		"explain, a plan that differs": {append([]string{"explain"}, with("-query", "0", "-plan", plan, "-expect-plan", strings.Repeat("0", 64))...), pgspike.ErrDigestDiffers},
	} {
		if err := run(t, c.args...); !errors.Is(err, c.is) {
			t.Errorf("%s: %v", name, err)
		}
	}
	// Refusals with no sentinel: the message is the point.
	for name, c := range map[string]struct {
		args []string
		want string
	}{
		"a plan with no digest asked for": {append([]string{"load"}, with("-out", filepath.Join(root, "o"), "-plan", plan)...), "must be named by its SHA-256"},
		"no input":                        {append([]string{"load"}, with("-out", filepath.Join(root, "o"))...), "name the input"},
		"a plan and an activity file":     {append([]string{"load"}, with("-out", filepath.Join(root, "o"), "-plan", plan, "-activity", plan)...), "name one"},
		"no settings":                     {[]string{"load", "-dsn", noServer, "-arm", "pg-bulk", "-schema", "s_x", "-out", filepath.Join(root, "o"), "-plan", plan, "-expect-plan", hex.EncodeToString(sum[:])}, "-settings is required"},
		"an arm that is not a bulk arm":   {[]string{"load", "-arm", "pg-stream", "-schema", "s_x", "-out", filepath.Join(root, "o")}, "-arm must be"},
		"reads of the id arm":             {[]string{"read", "-arm", "pg-bulk-id", "-schema", "s_x", "-out", filepath.Join(root, "o")}, "has no reads"},
		"a plan that is not a plan":       {append([]string{"load"}, with("-out", filepath.Join(root, "o"), "-plan", plan, "-expect-plan", hex.EncodeToString(sum[:]))...), "invalid character"},
		"no command":                      {nil, "usage"},
		"a command that is not one":       {[]string{"time"}, "no command"},
		"a diff of nothing":               {[]string{"diff"}, "usage"},
		"a diff of no kind":               {[]string{"diff", "-kind", "plan", "a", "b"}, "-kind must be"},
	} {
		if err := run(t, c.args...); err == nil || !strings.Contains(err.Error(), c.want) {
			t.Errorf("%s: %v, want a message with %q", name, err, c.want)
		}
	}
}

func TestSizesAreWholeNumbersOfBytesWithAnOptionalUnit(t *testing.T) {
	for in, want := range map[string]int64{
		"1": 1, "4096": 4096, "1B": 1, "2KiB": 2048, "2 kib": 2048, "3MiB": 3 << 20, "2GiB": 2 << 30, "2gib": 2 << 30, "1TiB": 1 << 40,
	} {
		if got, err := pgspike.ParseSize(in); err != nil || got != want {
			t.Errorf("%q: %d, %v; want %d", in, got, err, want)
		}
	}
	for _, in := range []string{"", "GiB", "0", "0GiB", "-1", "1.5GiB", "2GB", "2 gigabytes", "9223372036854775807GiB", "99999999999999999999"} {
		if got, err := pgspike.ParseSize(in); err == nil {
			t.Errorf("%q: accepted as %d", in, got)
		}
	}
}

func TestALimitThatIsNotASizeIsRefusedBeforeTheServerIsReached(t *testing.T) {
	root := t.TempDir()
	plan := filepath.Join(root, "plan.json")
	if err := os.WriteFile(plan, []byte("not a plan"), 0o600); err != nil {
		t.Fatal(err)
	}
	sum := sha256.Sum256([]byte("not a plan"))
	err := run(t, "load", "-dsn", noServer, "-settings", settingsFile(t), "-arm", "pg-bulk", "-schema", "s_x", "-out", filepath.Join(root, "o"),
		"-plan", plan, "-expect-plan", hex.EncodeToString(sum[:]), "-max-interval-mem", "2GB")
	if err == nil || !strings.Contains(err.Error(), "-max-interval-mem") {
		t.Errorf("a limit in GB: %v", err)
	}
}
