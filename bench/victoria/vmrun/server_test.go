package vmrun_test

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/lotannauo/toposhift/bench/spike/runner"
	"github.com/lotannauo/toposhift/bench/victoria/internal/fixture"
	"github.com/lotannauo/toposhift/bench/victoria/vmengine"
	"github.com/lotannauo/toposhift/bench/victoria/vmrun"
)

// ---- the registered image and the server's version ----

func TestParseImage(t *testing.T) {
	script, err := os.ReadFile("../scripts/vm-server.sh")
	if err != nil {
		t.Fatal(err)
	}
	img, err := vmrun.ParseImage(script)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(img, "victoriametrics/victoria-metrics:v") || !strings.Contains(img, "@sha256:") || vmrun.ImageTag(img) == "" || strings.Contains(vmrun.ImageTag(img), "@") {
		t.Errorf("image %q, tag %q", img, vmrun.ImageTag(img))
	}
	if got := vmrun.ImageTag("repo/name:v1.2.3@sha256:abc"); got != "v1.2.3" {
		t.Errorf("tag %q", got)
	}
	for name, text := range map[string]string{
		"none":      "echo hi\n",
		"two":       "image='a:b@sha256:1'\nimage='a:b@sha256:2'\n",
		"no digest": "image='a:v1'\n",
		"no tag":    "image='a@sha256:1'\n",
	} {
		if _, err := vmrun.ParseImage([]byte(text)); err == nil {
			t.Errorf("%s: an image was read from %q", name, text)
		}
	}
}

func TestServerVersion(t *testing.T) {
	m := vmengine.Metrics{`vm_app_version{version="victoria-metrics-20260928-101010-tags-v1.153.0-0-gabc",short_version="v1.153.0"}`: 1, "x": 2}
	if v, err := vmrun.ServerVersion(m); err != nil || v != "v1.153.0" {
		t.Errorf("%q, %v", v, err)
	}
	if _, err := vmrun.ServerVersion(vmengine.Metrics{"x": 1}); err == nil {
		t.Error("a version from a server that reports none")
	}
}

// ---- the server is the one the baseline is written for ----

func TestCollectEnvChecksTheServer(t *testing.T) {
	ctx := context.Background()
	f := newFakeVM(t)
	env, err := vmrun.CollectEnv(ctx, f.client(), envOpts)
	if err != nil {
		t.Fatal(err)
	}
	if !env.FlagsChecked || env.Version != "v1.153.0" || env.ImageChecked || env.FlagsSHA256 == "" {
		t.Errorf("%+v", env)
	}

	// A flag with another value, a flag missing, and a flag flags.conf does not have.
	f.mu.Lock()
	f.flags["retentionPeriod"] = "1y"
	f.mu.Unlock()
	if _, err := vmrun.CollectEnv(ctx, f.client(), envOpts); !errors.Is(err, vmengine.ErrFlagsDiffer) {
		t.Errorf("another value: %v", err)
	}
	f.mu.Lock()
	f.flags["retentionPeriod"] = "100y"
	f.flags["dedup.minScrapeInterval"] = "30s"
	f.mu.Unlock()
	if _, err := vmrun.CollectEnv(ctx, f.client(), envOpts); !errors.Is(err, vmengine.ErrFlagsDiffer) {
		t.Errorf("deduplication on: %v", err)
	}
	f.mu.Lock()
	f.flags["dedup.minScrapeInterval"] = "0s"
	f.flags["search.maxQueryDuration"] = "1s"
	f.mu.Unlock()
	if _, err := vmrun.CollectEnv(ctx, f.client(), envOpts); !errors.Is(err, vmengine.ErrFlagsDiffer) {
		t.Errorf("an extra flag: %v", err)
	}
	f.mu.Lock()
	delete(f.flags, "search.maxQueryDuration")
	f.version = "v1.152.0"
	f.mu.Unlock()
	if _, err := vmrun.CollectEnv(ctx, f.client(), envOpts); !errors.Is(err, vmrun.ErrVersionDiffers) {
		t.Errorf("another version: %v", err)
	}
	f.mu.Lock()
	f.version = "v1.153.0"
	f.mu.Unlock()

	// The container it runs in must have been started from the registered image.
	script, _ := os.ReadFile("../scripts/vm-server.sh")
	registered, _ := vmrun.ParseImage(script)
	opts := envOpts
	opts.Container = "toposhift-vm"
	opts.Docker = func(_ context.Context, name string) (string, error) {
		if name != "toposhift-vm" {
			t.Errorf("inspected %q", name)
		}
		return registered, nil
	}
	env, err = vmrun.CollectEnv(ctx, f.client(), opts)
	if err != nil || !env.ImageChecked || env.Image != registered {
		t.Fatalf("%+v, %v", env, err)
	}
	for _, other := range []string{
		strings.Replace(registered, "@sha256:", "@sha256:0", 1),
		"victoriametrics/victoria-metrics:v1.153.0",
		"victoriametrics/victoria-metrics:v1.152.0@sha256:" + strings.Split(registered, "@sha256:")[1],
	} {
		opts.Docker = func(context.Context, string) (string, error) { return other, nil }
		if _, err := vmrun.CollectEnv(ctx, f.client(), opts); !errors.Is(err, vmrun.ErrImageDiffers) {
			t.Errorf("image %q: %v", other, err)
		}
	}
	opts.Docker = func(context.Context, string) (string, error) { return "", errors.New("no docker") }
	if _, err := vmrun.CollectEnv(ctx, f.client(), opts); err == nil {
		t.Error("a container that could not be inspected was accepted")
	}
	// A container is checked against the script's image, so a run without the script cannot.
	opts.ScriptPath = ""
	if _, err := vmrun.CollectEnv(ctx, f.client(), opts); err == nil {
		t.Error("a container checked against no image")
	}
	// Without a client nothing about the server is checked, and the flags file is read.
	env, err = vmrun.CollectEnv(ctx, nil, envOpts)
	if err != nil || env.FlagsChecked || env.Version != "" {
		t.Errorf("%+v, %v", env, err)
	}
	if _, err := vmrun.CollectEnv(ctx, nil, vmrun.EnvOptions{FlagsPath: "../scripts/missing.conf"}); err == nil {
		t.Error("a missing flags file")
	}
}

// ---- load, merge and sizes against a stand-in for the server ----

func loadOpts() vmrun.LoadOptions {
	return vmrun.LoadOptions{RequestSamples: 500, Wait: 200 * time.Millisecond, Settle: time.Millisecond, Stable: 2, Env: envOpts}
}

// A load is refused before the first write if the server is not the baseline's, if it
// holds series already, or if the stream crosses a month.
func TestALoadIsRefusedBeforeItWritesAnything(t *testing.T) {
	ctx := context.Background()
	in := planInput(t)

	f := newFakeVM(t)
	f.mu.Lock()
	f.flags["dedup.minScrapeInterval"] = "30s"
	f.mu.Unlock()
	rep, err := vmrun.Load(ctx, in, f.client(), loadOpts())
	if !errors.Is(err, vmengine.ErrFlagsDiffer) || rep == nil || rep.Invalid == "" || f.writes != 0 {
		t.Errorf("a server with other flags: %v, %d writes", err, f.writes)
	}

	f = newFakeVM(t)
	f.hasSeries = true
	rep, err = vmrun.Load(ctx, in, f.client(), loadOpts())
	if !errors.Is(err, vmrun.ErrServerNotEmpty) || rep.Invalid == "" || f.writes != 0 {
		t.Errorf("a server with series: %v, %d writes", err, f.writes)
	}

	// Samples from the 30th of January to the 1st of February are in two months.
	spec := fixture.Spec()
	spec.Workload.Start = time.Date(2026, 1, 30, 0, 0, 0, 0, time.UTC)
	crossing, err := runner.MakePlan(ctx, spec, nil)
	if err != nil {
		t.Fatal(err)
	}
	cin, err := vmrun.PlanInput(crossing, "test")
	if err != nil {
		t.Fatal(err)
	}
	f = newFakeVM(t)
	rep, err = vmrun.Load(ctx, cin, f.client(), loadOpts())
	if !errors.Is(err, vmrun.ErrMonths) || rep.Months != 2 || rep.Invalid == "" || f.writes != 0 {
		t.Errorf("a stream across a month: %v, months %d, %d writes", err, rep.Months, f.writes)
	}
}

// The counters of the server are the converter's, or the load is INVALID and says why.
func TestALoadIsInvalidUnlessTheServersCountersAreTheConverters(t *testing.T) {
	ctx := context.Background()
	in := planInput(t)
	cases := []struct {
		name string
		set  func(*fakeVM)
	}{
		{"counters that never move", func(f *fakeVM) { f.stuck = true }},
		{"a row received and not added", func(f *fakeVM) { f.lose = 1 }},
		{"a row ignored", func(f *fakeVM) { f.ignore = 1 }},
	}
	if testing.Short() { // the race run keeps the cases that differ in kind
		cases = cases[:1]
	}
	for _, c := range cases {
		f := newFakeVM(t)
		c.set(f)
		rep, err := vmrun.Load(ctx, in, f.client(), loadOpts())
		if !errors.Is(err, vmrun.ErrNotLoaded) || !errors.Is(err, vmengine.ErrNotIngested) || rep.Invalid == "" {
			t.Errorf("%s: %v, %+v", c.name, err, rep)
		}
		if f.writes == 0 {
			t.Errorf("%s: nothing was written", c.name)
		}
	}
	// A server that made more series than the converter has (a label set it keeps twice).
	f := newFakeVM(t)
	f.extraSeries = 1
	if rep, err := vmrun.Load(ctx, in, f.client(), loadOpts()); !errors.Is(err, vmrun.ErrNotLoaded) || rep.Invalid == "" {
		t.Errorf("an extra series: %v", err)
	}
	// A stream that is not the one that was counted.
	f = newFakeVM(t)
	changed := planInput(t)
	real := planInput(t).Every
	calls := 0
	changed.SetEvery(func(ctx context.Context, sinks ...runner.Sink) (vmrun.Stream, error) {
		calls++
		st, err := real(ctx, sinks...)
		if calls == 2 {
			st.Digest = "another"
		}
		return st, err
	})
	if _, err := vmrun.Load(ctx, changed, f.client(), loadOpts()); !errors.Is(err, vmrun.ErrNotDeterministic) {
		t.Errorf("a stream that changed between counting and writing: %v", err)
	}
}

func TestLoadMergeAndSizes(t *testing.T) {
	ctx := context.Background()
	in := planInput(t)
	f := newFakeVM(t)
	c := f.client()

	load, err := vmrun.Load(ctx, in, c, loadOpts())
	if err != nil {
		t.Fatal(err)
	}
	rows := load.Counts.Samples + load.Counts.Markers
	if load.Invalid != "" || load.Samples != int(rows) || load.Requests != (int(rows)+499)/500 || uint64(f.writes) != uint64(load.Requests) {
		t.Errorf("%+v", load)
	}
	if load.Server.Received != rows || load.Server.Added != rows || load.Server.Ignored != 0 || load.Server.NewSeries != uint64(load.Counts.Series) {
		t.Errorf("server counters %+v for %d rows in %d series", load.Server, rows, load.Counts.Series)
	}
	if load.Months != 1 || !load.Env.FlagsChecked || load.Env.Version != "v1.153.0" || load.Input.Kind != vmrun.KindPlan {
		t.Errorf("%+v", load)
	}
	// The second load onto the same server is refused: it holds series now.
	if _, err := vmrun.Load(ctx, in, c, loadOpts()); !errors.Is(err, vmrun.ErrServerNotEmpty) {
		t.Errorf("a second load: %v", err)
	}

	mo := vmrun.MergeOptions{Settle: time.Millisecond, Stable: 2, Env: envOpts}
	merge, err := vmrun.Merge(ctx, c, load, mo)
	if err != nil {
		t.Fatal(err)
	}
	if merge.Invalid != "" || merge.Stored != rows || merge.Rows != rows || merge.Series != uint64(load.Counts.Series) || merge.After.BigParts != 1 || merge.After.SmallParts != 0 || f.forceMerges != 1 {
		t.Errorf("%+v (%d forced merges)", merge, f.forceMerges)
	}

	dir := t.TempDir()
	if err := os.MkdirAll(filepath.Join(dir, "data"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "data", "p"), make([]byte, 1000), 0o644); err != nil {
		t.Fatal(err)
	}
	sizes, err := vmrun.Sizes(ctx, c, load, merge, dir, mo)
	if err != nil {
		t.Fatal(err)
	}
	if sizes.Dir.Total != 1000 || sizes.Rows != rows || sizes.DataBytes != rows*3 || sizes.Note != vmrun.SizesNote ||
		sizes.PerRow != 1000/float64(rows) || sizes.PerSeries != 1000/float64(load.Counts.Series) {
		t.Errorf("%+v", sizes)
	}
	if _, err := vmrun.Sizes(ctx, c, load, merge, filepath.Join(dir, "missing"), mo); err == nil {
		t.Error("a missing data directory")
	}

	// A merge after a load that is INVALID, or on a server that holds fewer rows than were
	// written, is INVALID; no size is read of either.
	bad := *load
	bad.Invalid = "x"
	if _, err := vmrun.Merge(ctx, c, &bad, mo); !errors.Is(err, vmrun.ErrNotMerged) {
		t.Errorf("a merge after an INVALID load: %v", err)
	}
	f.mu.Lock()
	f.added--
	f.mu.Unlock()
	short, err := vmrun.Merge(ctx, c, load, mo)
	if !errors.Is(err, vmrun.ErrNotMerged) || short.Invalid == "" {
		t.Errorf("a merge that left a row out: %v, %+v", err, short)
	}
	if _, err := vmrun.Sizes(ctx, c, load, short, dir, mo); err == nil {
		t.Error("a size was read of an INVALID merge")
	}
	if _, err := vmrun.Sizes(ctx, c, &bad, merge, dir, mo); err == nil {
		t.Error("a size was read after an INVALID load")
	}
}

func TestAServerThatIsNotTheBaselinesIsNotMerged(t *testing.T) {
	ctx := context.Background()
	f := newFakeVM(t)
	f.mu.Lock()
	f.flags["retentionPeriod"] = "1y"
	f.mu.Unlock()
	load := &vmrun.LoadReport{}
	load.Counts.Samples = 1
	if _, err := vmrun.Merge(ctx, f.client(), load, vmrun.MergeOptions{Settle: time.Millisecond, Stable: 2, Env: envOpts}); !errors.Is(err, vmengine.ErrFlagsDiffer) {
		t.Errorf("%v", err)
	}
	if f.forceMerges != 0 {
		t.Error("a merge was forced on a server with other flags")
	}
}
