package xtnode

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
)

const script = "../scripts/xt-server.sh"

func TestThePinnedImageAndOptionsAreReadFromTheServerScript(t *testing.T) {
	p, err := ReadPinned(script)
	if err != nil {
		t.Fatal(err)
	}
	cfg, err := os.ReadFile("../scripts/xtdb.yaml")
	if err != nil {
		t.Fatal(err)
	}
	sum := sha256.Sum256(cfg)
	if !strings.HasPrefix(p.Image, "ghcr.io/xtdb/xtdb:2.1.0@sha256:") || p.JavaOptions != "-Xmx4g -XX:MaxDirectMemorySize=4g" || p.ConfigSHA256 != hex.EncodeToString(sum[:]) {
		t.Fatalf("%+v", p)
	}
}

func TestAScriptWithoutAPinIsRefused(t *testing.T) {
	dir := t.TempDir()
	write := func(name, text string) string {
		path := filepath.Join(dir, name)
		if err := os.WriteFile(path, []byte(text), 0o600); err != nil {
			t.Fatal(err)
		}
		return path
	}
	write("xtdb.yaml", "x: 1\n")
	for name, text := range map[string]string{
		"a tag alone":     "image='ghcr.io/xtdb/xtdb:2.1.0'\nregistered_jvm_options='-Xmx1g'\n",
		"no options":      "image='ghcr.io/xtdb/xtdb:2.1.0@sha256:ab'\n",
		"no image":        "registered_jvm_options='-Xmx1g'\n",
		"an unquoted pin": "image=ghcr.io/xtdb/xtdb:2.1.0@sha256:ab\nregistered_jvm_options='-Xmx1g'\n",
	} {
		if _, err := ReadPinned(write(strings.ReplaceAll(name, " ", "_")+".sh", text)); err == nil {
			t.Errorf("%s was accepted", name)
		}
	}
	if _, err := ReadPinned(write("good.sh", "image='r:1@sha256:ab'\nregistered_jvm_options='-Xmx1g'\n")); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(filepath.Join(dir, "xtdb.yaml")); err != nil {
		t.Fatal(err)
	}
	if _, err := ReadPinned(filepath.Join(dir, "good.sh")); err == nil {
		t.Error("a script with no configuration beside it was accepted")
	}
	if _, err := ReadPinned(filepath.Join(dir, "missing.sh")); err == nil {
		t.Error("a missing script was read")
	}
}

func TestOverridesAreNamedInAFixedOrder(t *testing.T) {
	env := map[string]string{EnvJavaOptions: "-Xmx1g", EnvConfig: "/x.yaml"}
	look := func(k string) (string, bool) { v, ok := env[k]; return v, ok }
	if got := Overrides(look); !slices.Equal(got, []string{EnvConfig, EnvJavaOptions}) {
		t.Fatalf("%v", got)
	}
	env[EnvConfig] = "" // set and empty is not an override: the script takes its default
	if got := Overrides(look); !slices.Equal(got, []string{EnvJavaOptions}) {
		t.Fatalf("%v", got)
	}
	if got := Overrides(func(string) (string, bool) { return "", false }); got != nil {
		t.Fatalf("%v", got)
	}
}

// inspectJSON is what docker inspect printed for a node, reduced to what the package
// reads.
const inspectJSON = `[{"Name":"/toposhift-xtdb","Image":"sha256:15b6aa9d","State":{"Running":true},
"Config":{"Image":"ghcr.io/xtdb/xtdb:2.1.0@sha256:15b6","Env":["JDK_JAVA_OPTIONS=-Xmx4g -XX:MaxDirectMemorySize=4g","PATH=/bin","XTDB_VERSION=2.1.0","GIT_SHA=ca78d56"]},
"Mounts":[{"Type":"bind","Source":"/data/x","Destination":"/var/lib/xtdb"},{"Type":"bind","Source":"CONFIG","Destination":"/config/xtdb.yaml"}]}]`

func TestADockerInspectIsReadAndHeldToThePin(t *testing.T) {
	dir := t.TempDir()
	cfg := filepath.Join(dir, "xtdb.yaml")
	if err := os.WriteFile(cfg, []byte("a: 1\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	sum := sha256.Sum256([]byte("a: 1\n"))
	run := func(_ context.Context, args ...string) ([]byte, error) {
		if !slices.Equal(args, []string{"inspect", "node"}) {
			t.Errorf("docker %v", args)
		}
		return []byte(strings.ReplaceAll(inspectJSON, "CONFIG", cfg)), nil
	}
	c, err := Inspect(context.Background(), run, "node")
	if err != nil {
		t.Fatal(err)
	}
	want := Container{
		Name: "toposhift-xtdb", ImageRef: "ghcr.io/xtdb/xtdb:2.1.0@sha256:15b6", ImageID: "sha256:15b6aa9d",
		JavaOptions: "-Xmx4g -XX:MaxDirectMemorySize=4g", XTDBVersion: "2.1.0", GitSHA: "ca78d56",
		ConfigFile: cfg, DataDir: "/data/x", ConfigSHA256: hex.EncodeToString(sum[:]), Running: true,
	}
	if c != want {
		t.Fatalf("%+v, want %+v", c, want)
	}
	pin := Pinned{Image: c.ImageRef, JavaOptions: c.JavaOptions, ConfigSHA256: c.ConfigSHA256}
	if got := NotRegistered(pin, &c, nil); len(got) != 0 {
		t.Fatalf("the pinned node is not registered: %v", got)
	}
	// Each way a node can differ is named.
	for name, mut := range map[string]func(*Container, *Pinned) []string{
		"image":    func(c *Container, _ *Pinned) []string { c.ImageRef = "ghcr.io/xtdb/xtdb:latest"; return nil },
		"options":  func(c *Container, _ *Pinned) []string { c.JavaOptions = "-Xmx2g"; return nil },
		"config":   func(c *Container, _ *Pinned) []string { c.ConfigSHA256 = "00"; return nil },
		"stopped":  func(c *Container, _ *Pinned) []string { c.Running = false; return nil },
		"override": func(*Container, *Pinned) []string { return []string{EnvJavaOptions} },
	} {
		cc, pp := c, pin
		over := mut(&cc, &pp)
		got := NotRegistered(pp, &cc, over)
		if len(got) != 1 {
			t.Errorf("%s: %v", name, got)
		}
	}
	if got := NotRegistered(pin, nil, nil); len(got) != 1 || !strings.Contains(got[0], "not inspected") {
		t.Errorf("a node that was not inspected: %v", got)
	}
}

func TestInspectErrors(t *testing.T) {
	boom := errors.New("no such container")
	if _, err := Inspect(context.Background(), func(context.Context, ...string) ([]byte, error) { return nil, boom }, "x"); !errors.Is(err, boom) {
		t.Errorf("%v", err)
	}
	for _, bad := range []string{"", "[]", "{}", "[{},{}]"} {
		if _, err := ParseInspect([]byte(bad)); err == nil {
			t.Errorf("%q was parsed", bad)
		}
	}
}

func TestADirectoryIsMeasuredByItsRegularFilesAndListed(t *testing.T) {
	mk := func(files map[string]int) string {
		dir := t.TempDir()
		for name, n := range files {
			path := filepath.Join(dir, name)
			if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(path, make([]byte, n), 0o600); err != nil {
				t.Fatal(err)
			}
		}
		if err := os.Symlink(dir, filepath.Join(dir, "link")); err != nil {
			t.Fatal(err)
		}
		return dir
	}
	a := mk(map[string]int{"l00/a": 10, "b": 5, "l00/deep/c": 1})
	d, err := MeasureDir(a)
	if err != nil || d.Bytes != 16 || d.Files != 3 || d.Path != a {
		t.Fatalf("%+v, %v", d, err)
	}
	same, err := MeasureDir(mk(map[string]int{"b": 5, "l00/deep/c": 1, "l00/a": 10}))
	if err != nil || same.ListingDigest != d.ListingDigest {
		t.Fatalf("one listing, two digests: %+v, %v", same, err)
	}
	for name, files := range map[string]map[string]int{
		"a file of another size": {"l00/a": 11, "b": 5, "l00/deep/c": 1},
		"a file of another name": {"l00/a": 10, "b2": 5, "l00/deep/c": 1},
		"one file fewer":         {"l00/a": 10, "b": 5},
	} {
		other, err := MeasureDir(mk(files))
		if err != nil || other.ListingDigest == d.ListingDigest {
			t.Errorf("%s: digest %s equals the first: %v", name, other.ListingDigest, err)
		}
	}
	if _, err := MeasureDir(filepath.Join(a, "missing")); err == nil {
		t.Error("a missing directory was measured")
	}
}

func TestFreeBytes(t *testing.T) {
	n, err := FreeBytes(t.TempDir())
	if err != nil || n == 0 {
		t.Fatalf("%d, %v", n, err)
	}
	if _, err := FreeBytes(filepath.Join(t.TempDir(), "missing")); err == nil {
		t.Error("free space of a path that is not there")
	}
}

func TestTheProcessorIsNamedFromCPUInfo(t *testing.T) {
	x86 := "processor\t: 0\nmodel name\t: AMD EPYC 7763 64-Core Processor\n\nprocessor\t: 1\nmodel name\t: other\n"
	arm := "processor\t: 0\nBogoMIPS\t: 2000.00\nCPU implementer\t: 0x41\nCPU architecture: 8\nCPU variant\t: 0x3\nCPU part\t: 0xd0c\n\nprocessor\t: 1\nCPU part\t: 0xffff\n"
	if got := ParseCPUInfo(x86); got != "AMD EPYC 7763 64-Core Processor" {
		t.Errorf("x86: %q", got)
	}
	if got := ParseCPUInfo(arm); got != "CPU implementer 0x41, CPU architecture 8, CPU variant 0x3, CPU part 0xd0c" {
		t.Errorf("arm: %q", got)
	}
	if got := ParseCPUInfo("nothing here\n"); got != "unknown" {
		t.Errorf("none: %q", got)
	}
	if CPUModel() == "" {
		t.Error("an empty name")
	}
}
