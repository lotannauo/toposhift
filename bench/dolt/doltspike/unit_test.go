package doltspike

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"

	"github.com/lotannauo/toposhift/bench/dolt/doltmodel"
	"github.com/lotannauo/toposhift/bench/dolt/scripts"
)

// The tests in this file need no server. The ones that do are in server_test.go, and skip
// without TOPOSHIFT_DOLT_DSN.

func TestAnOutputInsideAGitWorkTreeIsRefused(t *testing.T) {
	root := t.TempDir()
	if err := os.Mkdir(filepath.Join(root, ".git"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Join(root, "a", "b"), 0o755); err != nil {
		t.Fatal(err)
	}
	for _, dir := range []string{root, filepath.Join(root, "a"), filepath.Join(root, "a", "b"), filepath.Join(root, "a", "b", "not", "made", "yet")} {
		if err := RefuseInWorkTree(dir); !errors.Is(err, ErrInsideWorkTree) {
			t.Errorf("%s: %v", dir, err)
		}
	}
	// A linked work tree has a .git file.
	linked := t.TempDir()
	if err := os.WriteFile(filepath.Join(linked, ".git"), []byte("gitdir: /elsewhere\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := RefuseInWorkTree(filepath.Join(linked, "out")); !errors.Is(err, ErrInsideWorkTree) {
		t.Errorf("a linked work tree: %v", err)
	}
	// A link from outside into a work tree is inside it.
	outside := t.TempDir()
	if err := os.Symlink(filepath.Join(root, "a"), filepath.Join(outside, "link")); err != nil {
		t.Fatal(err)
	}
	if err := RefuseInWorkTree(filepath.Join(outside, "link", "out")); !errors.Is(err, ErrInsideWorkTree) {
		t.Errorf("a symbolic link into a work tree: %v", err)
	}
	if err := RefuseInWorkTree(filepath.Join(outside, "out")); err != nil {
		t.Errorf("a directory outside any work tree: %v", err)
	}
}

func TestAFileMustBeNamedByItsDigestAndHaveIt(t *testing.T) {
	f := filepath.Join(t.TempDir(), "x.json")
	if err := os.WriteFile(f, []byte("{}\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	const sum = "ca3d163bab055381827226140568f3bef7eaac187cebd76878e0b63e9e442356"
	if got, err := CheckDigest(f, sum, "plan"); err != nil || got != sum {
		t.Fatalf("%q, %v", got, err)
	}
	if _, err := CheckDigest(f, "sha256:"+strings.ToUpper(sum), "plan"); err != nil {
		t.Fatalf("a digest in capitals with its prefix: %v", err)
	}
	if _, err := CheckDigest(f, "", "plan"); err == nil || !strings.Contains(err.Error(), "-expect-plan") {
		t.Fatalf("no digest named: %v", err)
	}
	if _, err := CheckDigest(f, strings.Repeat("0", 64), "plan"); !errors.Is(err, ErrDigestDiffers) {
		t.Fatalf("another digest: %v", err)
	}
	if _, err := CheckDigest(f+"x", sum, "plan"); err == nil {
		t.Fatal("a missing file")
	}
}

func TestAContentDigestMayBeNamedByAPrefixOfTwelveDigits(t *testing.T) {
	got := "sha256:0123456789abcdef0123456789abcdef"
	for _, ok := range []string{"0123456789ab", "sha256:0123456789ab", "0123456789ABCDEF0123", got, "0123456789abcdef0123456789abcdef"} {
		if err := CheckContentDigest(got, ok); err != nil {
			t.Errorf("%s: %v", ok, err)
		}
	}
	for _, bad := range []string{"", "0123456789a", "0123456789ac", "123456789abc", "sha256:", strings.Repeat("0", 12)} {
		if err := CheckContentDigest(got, bad); !errors.Is(err, ErrDigestDiffers) {
			t.Errorf("%q: %v", bad, err)
		}
	}
}

func TestTheImageMustBeTheRegisteredOne(t *testing.T) {
	image, err := scripts.Image()
	if err != nil {
		t.Fatal(err)
	}
	if err := CheckImage(image); err != nil {
		t.Fatal(err)
	}
	if err := CheckImage(""); err == nil || !strings.Contains(err.Error(), "-image") {
		t.Errorf("no image named: %v", err)
	}
	for _, bad := range []string{
		"", "dolthub/dolt-sql-server:2.4.2", "dolthub/dolt-sql-server:2.5.1",
		strings.Replace(image, "30c7e4fa", "30c7e4fb", 1), image + " ", strings.TrimPrefix(image, "dolthub/"),
		"dolthub/dolt-sql-server@" + image[strings.Index(image, "sha256"):],
	} {
		if err := CheckImage(bad); !errors.Is(err, ErrImage) {
			t.Errorf("%q: %v", bad, err)
		}
	}
}

// TestTheRegisteredServerIsReadFromTheFilesThatMakeIt: the image and the version are the
// script's, and the settings are the file's, as embedded.
func TestTheRegisteredServerIsReadFromTheFilesThatMakeIt(t *testing.T) {
	image, err := scripts.Image()
	if err != nil {
		t.Fatal(err)
	}
	version, err := scripts.Version()
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(image, "dolt-sql-server:"+version+"@sha256:") || version != "2.4.2" {
		t.Fatalf("image %s, version %s", image, version)
	}
	for name, text := range map[string]string{"server.yaml": scripts.ServerYAML, "global.json": scripts.GlobalJSON, "dolt-server.sh": scripts.ServerScript} {
		on, err := os.ReadFile(filepath.Join("..", "scripts", name))
		if err != nil {
			t.Fatal(err)
		}
		if string(on) != text || text == "" {
			t.Errorf("the embedded %s is not the file's", name)
		}
	}
}

func TestEveryCommandThatReachesAServerRefusesBeforeReachingIt(t *testing.T) {
	root := t.TempDir()
	if err := os.Mkdir(filepath.Join(root, ".git"), 0o755); err != nil {
		t.Fatal(err)
	}
	image, _ := scripts.Image()
	dsn := "root@tcp(127.0.0.1:1)/" // nothing listens: a command that reached it would say so
	for _, tc := range []struct {
		name string
		args []string
		want error
		text string
	}{
		{"no image", []string{"load", "-dsn", dsn, "-db", "d", "-arm", "commit", "-out", t.TempDir()}, ErrImage, ""},
		{"another image", []string{"load", "-dsn", dsn, "-image", "dolthub/dolt-sql-server:2.5.1", "-db", "d", "-arm", "commit", "-out", t.TempDir()}, ErrImage, ""},
		{"read: another image", []string{"read", "-dsn", dsn, "-image", "x", "-db", "d", "-arm", "commit", "-load", "l.json"}, ErrImage, ""},
		{"explain: another image", []string{"explain", "-dsn", dsn, "-image", "x", "-db", "d", "-arm", "rows", "-load", "l.json"}, ErrImage, ""},
		{"sizes: another image", []string{"sizes", "-dsn", dsn, "-image", "x", "-db", "d", "-arm", "rows", "-load", "l.json", "-data", t.TempDir()}, ErrImage, ""},
		{"env: another image", []string{"env", "-dsn", dsn, "-image", "x"}, ErrImage, ""},
		{"an output in a work tree", []string{"load", "-dsn", dsn, "-image", image, "-db", "d", "-arm", "commit", "-out", filepath.Join(root, "out")}, ErrInsideWorkTree, ""},
		{"read: an output in a work tree", []string{"read", "-dsn", dsn, "-image", image, "-db", "d", "-arm", "commit", "-load", "l.json", "-out", filepath.Join(root, "out")}, ErrInsideWorkTree, ""},
		{"env: an output in a work tree", []string{"env", "-out", filepath.Join(root, "out")}, ErrInsideWorkTree, ""},
		{"questions: an output in a work tree", []string{"questions", "-activity", "a", "-expect-activity", "0123456789ab", "-out", filepath.Join(root, "out")}, ErrInsideWorkTree, ""},
		{"no server", []string{"load", "-image", image, "-db", "d", "-arm", "commit", "-out", t.TempDir()}, nil, "-dsn is required"},
		{"no database", []string{"read", "-dsn", dsn, "-image", image, "-arm", "commit", "-load", "l.json"}, nil, "-db is required"},
		{"no output", []string{"load", "-dsn", dsn, "-image", image, "-db", "d", "-arm", "commit"}, nil, "-out is required"},
		{"no arm", []string{"load", "-dsn", dsn, "-image", image, "-db", "d", "-out", t.TempDir()}, nil, "no arm"},
		{"the diagnostic is not loaded", []string{"load", "-dsn", dsn, "-image", image, "-db", "d", "-arm", "commit-time", "-out", t.TempDir()}, nil, "a load is of commit or rows"},
		{"an arm of a read that does not exist", []string{"read", "-dsn", dsn, "-image", image, "-db", "d", "-arm", "both", "-load", "l.json"}, nil, "no arm"},
		{"no input", []string{"load", "-dsn", dsn, "-image", image, "-db", "d", "-arm", "commit", "-out", t.TempDir()}, nil, "name the input"},
		{"no command", nil, nil, "usage"},
		{"another command", []string{"walk"}, nil, "no command"},
		{"sizing off CI", []string{"sizing", "-data", t.TempDir()}, ErrNotOnCI, ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv("GITHUB_ACTIONS", "")
			t.Setenv("TOPOSHIFT_DOLT_DSN", "")
			t.Setenv("TOPOSHIFT_DOLT_IMAGE", "")
			var out, errOut bytes.Buffer
			err := Run(context.Background(), tc.args, &out, &errOut)
			switch {
			case err == nil:
				t.Fatal("no error")
			case tc.want != nil && !errors.Is(err, tc.want):
				t.Fatalf("%v, want %v", err, tc.want)
			case tc.text != "" && !strings.Contains(err.Error(), tc.text):
				t.Fatalf("%v, want it to say %q", err, tc.text)
			}
		})
	}
}

// TestAPlanThatIsNotTheOneNamedIsRefused: the digest is checked before the server is reached.
func TestAPlanThatIsNotTheOneNamedIsRefused(t *testing.T) {
	image, _ := scripts.Image()
	plan := filepath.Join(t.TempDir(), "plan.json")
	if err := os.WriteFile(plan, []byte("{}\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	for _, args := range [][]string{
		{"load", "-dsn", "root@tcp(127.0.0.1:1)/", "-image", image, "-db", "d", "-arm", "commit", "-out", t.TempDir(), "-plan", plan},
		{"load", "-dsn", "root@tcp(127.0.0.1:1)/", "-image", image, "-db", "d", "-arm", "commit", "-out", t.TempDir(), "-plan", plan, "-expect-plan", strings.Repeat("0", 64)},
		{"read", "-dsn", "root@tcp(127.0.0.1:1)/", "-image", image, "-db", "d", "-arm", "rows", "-load", "l.json", "-plan", plan, "-expect-plan", strings.Repeat("0", 64)},
		{"explain", "-dsn", "root@tcp(127.0.0.1:1)/", "-image", image, "-db", "d", "-arm", "rows", "-load", "l.json", "-plan", plan, "-expect-plan", strings.Repeat("0", 64)},
	} {
		var out bytes.Buffer
		err := Run(context.Background(), args, &out, &out)
		if err == nil || (!errors.Is(err, ErrDigestDiffers) && !strings.Contains(err.Error(), "-expect-plan")) {
			t.Errorf("%v: %v", args[0], err)
		}
	}
	// Two inputs are not one.
	var out bytes.Buffer
	err := Run(context.Background(), []string{"load", "-dsn", "root@tcp(127.0.0.1:1)/", "-image", image, "-db", "d", "-arm", "commit", "-out", t.TempDir(), "-plan", plan, "-questions", plan}, &out, &out)
	if err == nil || !strings.Contains(err.Error(), "two inputs") {
		t.Errorf("a plan and a question set: %v", err)
	}
}

func TestARecordIsNeverOverwritten(t *testing.T) {
	dir := t.TempDir()
	if err := writeNew(dir, "r.json", map[string]int{"a": 1}); err != nil {
		t.Fatal(err)
	}
	before, _ := os.ReadFile(filepath.Join(dir, "r.json"))
	if err := writeNew(dir, "r.json", map[string]int{"a": 2}); err == nil {
		t.Fatal("a record was overwritten")
	}
	if err := checkFree(dir, "r.json"); err == nil {
		t.Fatal("an existing record was called free")
	}
	if after, _ := os.ReadFile(filepath.Join(dir, "r.json")); !bytes.Equal(before, after) {
		t.Fatal("the record changed")
	}
	if err := checkFree(dir, "other.json"); err != nil {
		t.Fatal(err)
	}
	if err := writeNew(filepath.Join(dir, "new", "deeper"), "r.json", 1); err != nil {
		t.Fatalf("a directory that does not exist: %v", err)
	}
}

func TestTheFrontiersDigestFollowsTheOrderAndTheValues(t *testing.T) {
	a := frontiersDigest([]int64{1, 2, 3})
	for name, fs := range map[string][]int64{"order": {1, 3, 2}, "value": {1, 2, 4}, "length": {1, 2}, "extra": {1, 2, 3, 3}, "none": nil} {
		if frontiersDigest(fs) == a {
			t.Errorf("a different %s gave the same digest", name)
		}
	}
	if frontiersDigest([]int64{1, 2, 3}) != a || len(a) != 64 {
		t.Error("the digest is not a function of the frontiers")
	}
	if frontiersDigest([]int64{doltmodel.NoFrontier, 0}) == frontiersDigest([]int64{0, doltmodel.NoFrontier}) {
		t.Error("the digest ignores the sign")
	}
}

// durationInText finds a token that is a time: a number with a unit of time, on its own (or
// hours with minutes). A bare "1h" or "1d" is not one: they are the ages of a plan's questions.
var durationInText = regexp.MustCompile(`(?m)((^|[\s(])[0-9]+(\.[0-9]+)?(ns|µs|us|ms|s|m)([\s,;)]|$))|([0-9]h[0-9]+m)`)

func TestNothingPrintedHoldsATime(t *testing.T) {
	for _, bad := range []string{"took 3s", "12.5ms", "1h30m", "(4m)", "5µs\n", "line\n7ns", "2h5m10s"} {
		if !durationInText.MatchString(bad) {
			t.Errorf("the check does not find a time in %q", bad)
		}
	}
	for _, ok := range []string{"3597 records", "as-of-alone 10", "head gi1sc3qo6qtidu8qnsvq8075o88bek74 (ordinal 4)", "sha256 04856a8c5s", "seq 5", "2026-01-01T00:00:00Z", "G0 pod-> median neighbors 1h 5 of 5", "dead 1d window-1h"} {
		if durationInText.MatchString(ok) {
			t.Errorf("the check finds a time in %q", ok)
		}
	}
}

func TestEnvNamesTheServerWithoutReachingIt(t *testing.T) {
	t.Setenv("TOPOSHIFT_DOLT_DSN", "")
	t.Setenv("TOPOSHIFT_DOLT_IMAGE", "")
	var out, errOut bytes.Buffer
	if err := Run(context.Background(), []string{"env"}, &out, &errOut); err != nil {
		t.Fatal(err)
	}
	image, _ := scripts.Image()
	for _, want := range []string{
		"registered image: " + image, "registered Dolt: 2.4.2", "server: not reached", scripts.SHA256(scripts.ServerYAML),
		"payload capacity 256 bytes (registered)", "interpolateParams false", "schema version 1",
	} {
		if !strings.Contains(out.String(), want) {
			t.Errorf("env did not say %q:\n%s", want, out.String())
		}
	}
	if durationInText.MatchString(out.String()) {
		t.Errorf("env printed a time:\n%s", out.String())
	}
	// And writes the same to a file, once.
	dir := t.TempDir()
	if err := Run(context.Background(), []string{"env", "-out", dir}, &out, &errOut); err != nil {
		t.Fatal(err)
	}
	b, err := os.ReadFile(filepath.Join(dir, "env.json"))
	if err != nil {
		t.Fatal(err)
	}
	var e Env
	if err := json.Unmarshal(b, &e); err != nil || e.ServerYAMLSHA256 != scripts.SHA256(scripts.ServerYAML) || e.PayloadCapacity != doltmodel.RegisteredPayloadCapacity || e.InterpolateParams {
		t.Fatalf("%+v, %v", e, err)
	}
	if err := Run(context.Background(), []string{"env", "-out", dir}, &out, &errOut); err == nil {
		t.Fatal("env.json was overwritten")
	}
}

func TestParsePositionsReadsOneBasedPositions(t *testing.T) {
	got, err := parsePositions("1, 3,3", 5)
	if err != nil || len(got) != 2 || !got[0] || !got[2] {
		t.Fatalf("%v, %v", got, err)
	}
	if got, err := parsePositions("", 5); err != nil || got != nil {
		t.Fatalf("none named: %v, %v", got, err)
	}
	for _, bad := range []string{"0", "6", "x", "1,,2", "-1", "1.5"} {
		if _, err := parsePositions(bad, 5); err == nil {
			t.Errorf("%q was accepted", bad)
		}
	}
}
