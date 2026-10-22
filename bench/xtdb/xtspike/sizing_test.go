package xtspike

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/lotannauo/toposhift/bench/xtdb/xtnode"
)

// sized is a rig on CI with a data directory that holds some files, and a disk with room.
func sized(t *testing.T) (*rig, []string, string) {
	t.Helper()
	r := newRig(t)
	r.env["GITHUB_ACTIONS"] = "true"
	data := t.TempDir()
	for _, f := range []string{"log/a", "storage/b"} {
		if err := os.MkdirAll(filepath.Dir(filepath.Join(data, f)), 0o700); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(data, f), make([]byte, 1000), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	r.app.Free = func(string) (uint64, error) { return 1 << 40, nil }
	plan, sum := planFile(t, tinySpec())
	return r, []string{"sizing", "-script", script, "-dsn", "x", "-container", "node", "-data", data, "-plan", plan, "-expect-plan", sum}, data
}

func TestSizingIsRefusedOffCIBeforeTheNodeIsReached(t *testing.T) {
	r, args, _ := sized(t)
	delete(r.env, "GITHUB_ACTIONS")
	if err := r.run(args...); err == nil || !strings.Contains(err.Error(), "CI hardware only") {
		t.Fatalf("%v", err)
	}
	if r.node.opens != 0 || r.out.Len() != 0 {
		t.Errorf("the node was opened %d times, output %q", r.node.opens, r.out)
	}
}

func TestSizingPrintsTheRuleAndOnlyAVerdict(t *testing.T) {
	r, args, _ := sized(t)
	summary := filepath.Join(t.TempDir(), "summary.md")
	r.env["GITHUB_STEP_SUMMARY"] = summary
	r.env["RUNNER_ARCH"] = "ARM64"
	// The fake clock does not move while the stream is written, so the load is within the limit.
	if err := r.run(args...); err != nil {
		t.Fatalf("%v\n%s", err, r.out)
	}
	got := r.out.String()
	want := SizingRule(RegisteredMaxLoad, RegisteredMaxDiskPct) + "\nsizing: pass\n"
	if got != want {
		t.Fatalf("output:\n%s\nwant:\n%s", got, want)
	}
	noDuration(t, "sizing", got+r.errOut.String())
	if !strings.Contains(got, WriteLabel) || !strings.Contains(got, "within 75 minutes") || !strings.Contains(got, "30%") {
		t.Errorf("the rule does not carry the label or the limits:\n%s", got)
	}
	b, err := os.ReadFile(summary)
	if err != nil || !strings.Contains(string(b), "sizing: pass") || !strings.Contains(string(b), "ARM64") {
		t.Errorf("summary %q, %v", b, err)
	}
	if r.node.Transactions() == 0 {
		t.Error("nothing was loaded")
	}
}

func TestSizingFailsByTheLimitItBrokeAndNamesNoFigure(t *testing.T) {
	cases := map[string]struct {
		setup func(r *rig)
		extra []string
		why   string
	}{
		"a load over the time limit": {func(r *rig) {
			r.node.onWrite = func() { r.clock = r.clock.Add(10 * time.Minute) }
		}, nil, "the load exceeded the time limit"},
		"a node over the disk limit": {func(r *rig) {
			r.app.Free = func(string) (uint64, error) { return 6000, nil } // 2000 bytes are 33%
		}, nil, "over 30% of the free space"},
		"a node that does not settle": {func(r *rig) {
			r.probe = func() xtnode.Sample { return xtnode.Sample{JobsAvailable: 3} }
		}, nil, "the node did not settle (3 compaction jobs available)"},
		"a node that does not finish its block": {func(r *rig) { r.stuck = true }, nil, "the node did not finish its block"},
		"a stricter limit from a flag": {func(r *rig) {
			r.node.onWrite = func() { r.clock = r.clock.Add(time.Second) }
		}, []string{"-max-load", "1m"}, "the load exceeded the time limit"},
	}
	for name, c := range cases {
		t.Run(name, func(t *testing.T) {
			r, args, _ := sized(t)
			c.setup(r)
			if err := r.run(append(args, c.extra...)...); err != nil {
				t.Fatalf("a failed rule is a verdict, not an error: %v", err)
			}
			if !strings.Contains(r.out.String(), "sizing: fail ("+c.why) && !strings.Contains(r.out.String(), c.why) {
				t.Errorf("want %q:\n%s", c.why, r.out)
			}
			if strings.Contains(r.out.String(), "sizing: pass") {
				t.Errorf("a failed rule passed:\n%s", r.out)
			}
			noDuration(t, "sizing", r.out.String())
		})
	}
}

func TestSizingVerdictAtTheLimits(t *testing.T) {
	ok := sizing{load: 75 * time.Minute, storage: 300, free: 1000, finished: true, settled: true}
	if why := ok.verdict(75*time.Minute, 30); len(why) != 0 {
		t.Errorf("at the limits: %v", why)
	}
	for name, z := range map[string]sizing{
		"a nanosecond over":  {load: 75*time.Minute + 1, storage: 1, free: 1000, finished: true, settled: true},
		"a byte over":        {load: time.Minute, storage: 301, free: 1000, finished: true, settled: true},
		"unsettled":          {load: time.Minute, storage: 1, free: 1000, finished: true, settled: false, settleBy: "x"},
		"no free space":      {load: time.Minute, storage: 1, free: 0, finished: true, settled: true},
		"block not finished": {load: time.Minute, storage: 1, free: 1000, finished: false, settled: true, settleBy: "x"},
	} {
		if why := z.verdict(75*time.Minute, 30); len(why) != 1 {
			t.Errorf("%s: %v", name, why)
		}
	}
	// Several limits broken are all named.
	if why := (sizing{load: 2 * time.Hour, storage: 900, free: 1000, finished: true}).verdict(75*time.Minute, 30); len(why) != 3 {
		t.Errorf("%v", why)
	}
}

func TestSizingRefusesWhatItCannotMeasure(t *testing.T) {
	r, args, _ := sized(t)
	for name, mod := range map[string]func([]string) []string{
		"no data directory": func(a []string) []string { return without(a, "-data") },
		"a limit of zero":   func(a []string) []string { return append(a, "-max-load", "0s") },
		"a share over 100":  func(a []string) []string { return append(a, "-max-disk-percent", "101") },
		"questions":         func(a []string) []string { return append(a, "-questions", "q.json") },
		"no plan":           func(a []string) []string { return without(without(a, "-plan"), "-expect-plan") },
	} {
		r.node = newModelNode()
		if err := r.run(mod(append([]string{}, args...))...); err == nil {
			t.Errorf("%s was accepted", name)
		}
	}
	// A node that already holds a stream.
	r.node = newModelNode()
	if err := r.run(args...); err != nil {
		t.Fatal(err)
	}
	if err := r.run(args...); err == nil || !strings.Contains(err.Error(), "already holds a stream") {
		t.Errorf("a second sizing on the same node: %v", err)
	}
}

// without removes a flag and its value.
func without(args []string, flag string) []string {
	var out []string
	for i := 0; i < len(args); i++ {
		if args[i] == flag {
			i++
			continue
		}
		out = append(out, args[i])
	}
	return out
}
