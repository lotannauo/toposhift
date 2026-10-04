package main

import (
	"flag"
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
		"retain nonsense":         {"-retain", "soon"},
		"retain at":               {"-retain", "x/24h"},
		"retain keep":             {"-retain", "48h/y"},
		"retain past the end":     {"-retain", "9999h/1h"},
	} {
		if _, err := parse(t, args...).spec(); err == nil {
			t.Errorf("%s: accepted", name)
		}
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
