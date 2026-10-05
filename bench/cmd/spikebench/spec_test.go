package main

import (
	"flag"
	"os"
	"path/filepath"
	"slices"
	"testing"
	"time"

	"github.com/lotannauo/toposhift/bench/spike/runner"
	"github.com/lotannauo/toposhift/bench/spike/workload"
)

func parse(t *testing.T, args ...string) planFlags {
	t.Helper()
	var p planFlags
	fs := flag.NewFlagSet("plan", flag.ContinueOnError)
	p.flags(fs)
	if err := fs.Parse(args); err != nil {
		t.Fatal(err)
	}
	return p
}

func TestFlagsChooseTheSpec(t *testing.T) {
	t.Parallel()

	for preset, want := range map[string]workload.Config{
		"tiny": workload.Tiny(), "small": workload.Small(), "ci": workload.CI(), "week": workload.Week(), "month": workload.Month(),
	} {
		s, err := parse(t, "-preset", preset).spec()
		if err != nil {
			t.Fatalf("%s: %v", preset, err)
		}
		if s.Workload.Duration != want.Duration || s.Workload.Seed != want.Seed || s.Workload.Pods != want.Pods {
			t.Errorf("%s: workload %+v, want the preset's", preset, s.Workload)
		}
	}
	// With no flag, the spec is the default one.
	def, err := parse(t).spec()
	if err != nil {
		t.Fatal(err)
	}
	want := runner.DefaultSpec(workload.CI())
	if d1, _ := def.Digest(); d1 == "" {
		t.Fatal("no digest")
	} else if d2, _ := want.Digest(); d1 != d2 {
		t.Errorf("no flags gave a spec other than the default of the ci preset")
	}

	s, err := parse(t, "-preset", "ci", "-days", "5", "-seed", "9", "-batch", "77", "-hot", "3", "-median", "2",
		"-cache-fraction", "0.5", "-min-non-empty", "0", "-retain", "48h/24h,96h/48h").spec()
	if err != nil {
		t.Fatal(err)
	}
	if s.Workload.Duration != 120*time.Hour || s.Workload.Seed != 9 || s.BatchSize != 77 || s.Hot != 3 || s.Median != 2 ||
		s.CacheFraction != 0.5 || s.MinNonEmpty != 0 {
		t.Errorf("overrides not applied: %+v", s)
	}
	if want := []runner.Retention{{At: 48 * time.Hour, Keep: 24 * time.Hour}, {At: 96 * time.Hour, Keep: 48 * time.Hour}}; !slices.Equal(s.Retentions, want) {
		t.Errorf("retentions %v, want %v", s.Retentions, want)
	}
	if s, err := parse(t, "-retain", "none").spec(); err != nil || len(s.Retentions) != 0 {
		t.Errorf("-retain none: %v, %v", s.Retentions, err)
	}
	if s, err := parse(t, "-days", "10", "-seed", "1").spec(); err != nil || s.Workload.Seed != 1 || len(s.Retentions) != 1 {
		t.Errorf("a longer period keeps the default retention: %v, %v", s.Retentions, err)
	}

	for name, args := range map[string][]string{
		"unknown preset":          {"-preset", "huge"},
		"days of tiny":            {"-preset", "tiny", "-days", "2"},
		"days of small":           {"-preset", "small", "-days", "2"},
		"negative share":          {"-min-non-empty", "-2"},
		"slightly negative share": {"-min-non-empty", "-0.5"},
		"negative cache":          {"-cache-fraction", "-0.5"},
		"negative cache size":     {"-cache-mb", "-5"},
		"two ways to size cache":  {"-cache-mb", "8", "-cache-fraction", "0.3"},
		"retain nonsense":         {"-retain", "soon"},
		"retain at":               {"-retain", "x/24h"},
		"retain keep":             {"-retain", "48h/y"},
		"retain past the end":     {"-retain", "9999h/1h"},
	} {
		if _, err := parse(t, args...).spec(); err == nil {
			t.Errorf("%s: accepted", name)
		}
	}
	// The cache is an absolute size unless a share is asked for, and then it is only that.
	if s, err := parse(t, "-cache-mb", "8").spec(); err != nil || s.CacheBytes != 8<<20 || s.CacheFraction != 0 {
		t.Errorf("-cache-mb 8: %+v, %v", s, err)
	}
	if s, err := parse(t, "-cache-fraction", "0.3").spec(); err != nil || s.CacheBytes != 0 || s.CacheFraction != 0.3 {
		t.Errorf("-cache-fraction 0.3: %+v, %v", s, err)
	}
	if def, _ := parse(t).spec(); def.CacheBytes != runner.DefaultCacheBytes || def.CacheFraction != 0 {
		t.Errorf("the default cache is %d bytes and a share of %v", def.CacheBytes, def.CacheFraction)
	}
	// A flag left out, or at zero, leaves the spec's own value; below zero it is an error.
	if s, err := parse(t, "-batch", "0", "-hot", "0", "-days", "0").spec(); err != nil || s.BatchSize != want.BatchSize || s.Hot != want.Hot {
		t.Errorf("zero values: %+v, %v", s, err)
	}
	for _, flag := range []string{"-days", "-batch", "-hot", "-median"} {
		if _, err := parse(t, flag, "-1").spec(); err == nil {
			t.Errorf("%s -1 was accepted", flag)
		}
	}
}

func TestFlagsPassedOnToTheProcessesOfARun(t *testing.T) {
	t.Parallel()

	p := parse(t, "-preset", "week", "-days", "4", "-seed", "5", "-batch", "9", "-retain", "48h/24h", "-hot", "2", "-median", "3",
		"-cache-fraction", "0.1", "-min-non-empty", "0.5")
	q := parse(t, "-cache-mb", "32")
	if got := q.args(); !slices.Equal(got, []string{"-preset", "ci", "-cache-mb", "32"}) {
		t.Errorf("args = %v", got)
	}
	got := p.args()
	want := []string{"-preset", "week", "-days", "4", "-seed", "5", "-batch", "9", "-retain", "48h/24h", "-hot", "2", "-median", "3", "-cache-fraction", "0.1", "-min-non-empty", "0.5"}
	if !slices.Equal(got, want) {
		t.Errorf("args = %v, want %v", got, want)
	}
	// What is passed on makes the same spec.
	again := parse(t, got...)
	a, _ := p.spec()
	b, _ := again.spec()
	da, _ := a.Digest()
	db, _ := b.Digest()
	if da != db {
		t.Error("the flags passed on make another spec")
	}
	if got := parse(t).args(); !slices.Equal(got, []string{"-preset", "ci"}) {
		t.Errorf("no flags passed on %v", got)
	}
}

func TestScenarioFlagsSetTheWorkload(t *testing.T) {
	t.Parallel()

	ci := workload.CI()
	s, err := parse(t, "-preset", "ci").spec()
	if err != nil || s.Workload.ExtendTTLFraction != ci.ExtendTTLFraction || s.Workload.PodHeartbeatInterval != ci.PodHeartbeatInterval {
		t.Fatalf("the preset's own values were changed with no flag: %+v, %v", s.Workload, err)
	}
	for name, c := range map[string]struct {
		args  []string
		check func(workload.Config) bool
	}{
		"extend every":      {[]string{"-extend", "every"}, func(w workload.Config) bool { return w.CoalesceRuns && w.ExtendTTLFraction == 0 && w.ExtendEvery == 0 }},
		"extend a share":    {[]string{"-extend", "0.75"}, func(w workload.Config) bool { return w.CoalesceRuns && w.ExtendTTLFraction == 0.75 }},
		"pod heartbeat off": {[]string{"-pod-heartbeat", "off"}, func(w workload.Config) bool { return w.PodHeartbeatInterval == 0 }},
		"pod heartbeat 5m":  {[]string{"-pod-heartbeat", "5m"}, func(w workload.Config) bool { return w.PodHeartbeatInterval == 5*time.Minute }},
		"a rate":            {[]string{"-events-per-second", "0.25"}, func(w workload.Config) bool { return w.EventsPerSecond == 0.25 }},
		"a run age":         {[]string{"-run-max-age", "2h"}, func(w workload.Config) bool { return w.CoalesceRuns && w.RunMaxAge == 2*time.Hour }},
		"no run age":        {[]string{"-run-max-age", "off"}, func(w workload.Config) bool { return w.RunMaxAge == 0 }},
	} {
		s, err := parse(t, append([]string{"-preset", "ci"}, c.args...)...).spec()
		if err != nil || !c.check(s.Workload) {
			t.Errorf("%s: %+v, %v", name, s.Workload, err)
		}
	}
	for name, args := range map[string][]string{
		"extend nonsense":     {"-extend", "often"},
		"extend zero":         {"-extend", "0"},
		"extend over one":     {"-extend", "1.5"},
		"heartbeat nonsense":  {"-pod-heartbeat", "soon"},
		"heartbeat negative":  {"-pod-heartbeat", "-5m"},
		"negative rate":       {"-events-per-second", "-1"},
		"run age nonsense":    {"-run-max-age", "long"},
		"run age too short":   {"-run-max-age", "10s"},
		"window with days":    {"-window", "2", "-days", "5"},
		"window with retain":  {"-window", "2", "-retain", "48h/24h"},
		"pins with no window": {"-pins", pinsFile(t)},
		"negative window":     {"-window", "-2"},
	} {
		if _, err := parse(t, append([]string{"-preset", "ci"}, args...)...).spec(); err == nil {
			t.Errorf("%s: accepted", name)
		}
	}
	// The flags are passed on to the processes of a run.
	p := parse(t, "-extend", "every", "-pod-heartbeat", "5m", "-events-per-second", "0.25", "-run-max-age", "2h", "-window", "7")
	want := []string{"-preset", "ci", "-events-per-second", "0.25", "-extend", "every", "-pod-heartbeat", "5m", "-run-max-age", "2h", "-window", "7"}
	if got := p.args(); !slices.Equal(got, want) {
		t.Errorf("args = %v, want %v", got, want)
	}
}

// A window is a stream of 2R + 1.5 days with a daily retention that keeps R.
func TestAWindowFlagMakesTheWindowSpec(t *testing.T) {
	t.Parallel()

	s, err := parse(t, "-preset", "ci", "-window", "7").spec()
	if err != nil {
		t.Fatal(err)
	}
	want := runner.WindowSpec(runner.DefaultSpec(workload.CI()), 7)
	d1, _ := s.Digest()
	d2, _ := want.Digest()
	if d1 != d2 {
		t.Errorf("-window 7 made another spec than runner.WindowSpec: %s, %s", s.Workload.Duration, want.Workload.Duration)
	}
	if s.Workload.Duration != (2*7*24+36)*time.Hour {
		t.Errorf("a window of 7 days is %s long", s.Workload.Duration)
	}
	if len(s.Retentions) != 8 || s.Retentions[0] != (runner.Retention{At: 8 * 24 * time.Hour, Keep: 7 * 24 * time.Hour}) {
		t.Errorf("retentions %v, want eight daily ones that keep 7 days from the 8th day", s.Retentions)
	}
}

func TestWindowsListsAreIncreasingAndLongEnoughForG1(t *testing.T) {
	t.Parallel()

	if got, err := parseWindows("2,7,14,30"); err != nil || !slices.Equal(got, []int{2, 7, 14, 30}) {
		t.Errorf("2,7,14,30: %v, %v", got, err)
	}
	if got, err := parseWindows(" 1, 2 ,3 "); err != nil || !slices.Equal(got, []int{1, 2, 3}) {
		t.Errorf("with spaces: %v, %v", got, err)
	}
	for _, bad := range []string{"", "2,7", "7,2,14", "2,2,14", "2,x,14", "0,1,2", "-1,2,3", "2,7,14,14"} {
		if got, err := parseWindows(bad); err == nil {
			t.Errorf("%q was accepted: %v", bad, got)
		}
	}
}

// pinsFile is a file that holds pins that verify (no classes, with the digest of that).
func pinsFile(t *testing.T) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "pins.json")
	if err := os.WriteFile(path, []byte(`{"Classes":null,"Digest":"74234e98afe7498fb5daf1f36ac2d78acc339464f950703b8c019892f982b90b"}`), 0o644); err != nil {
		t.Fatal(err)
	}
	return path
}
