package runner

import (
	"runtime/debug"
	"testing"
)

// What a binary was built with is read from its build settings.
func TestBuildSettingsAreRead(t *testing.T) {
	t.Parallel()

	set := func(kv ...string) []debug.BuildSetting {
		var out []debug.BuildSetting
		for i := 0; i < len(kv); i += 2 {
			out = append(out, debug.BuildSetting{Key: kv[i], Value: kv[i+1]})
		}
		return out
	}
	for name, c := range map[string]struct {
		settings []debug.BuildSetting
		want     BuildInfo
	}{
		"nothing":               {set(), BuildInfo{}},
		"cgo off":               {set("CGO_ENABLED", "0"), BuildInfo{}},
		"race":                  {set("-race", "true"), BuildInfo{Race: true}},
		"race off":              {set("-race", "false"), BuildInfo{}},
		"invariants":            {set("-tags", "x,invariants"), BuildInfo{Invariants: true, Tags: "x,invariants"}},
		"another tag":           {set("-tags", "invariant"), BuildInfo{Tags: "invariant"}},
		"stamp":                 {set("vcs.revision", "abc", "vcs.modified", "true"), BuildInfo{Revision: "abc", Modified: true}},
		"clean stamp":           {set("vcs.revision", "abc", "vcs.modified", "false"), BuildInfo{Revision: "abc"}},
		"no optimization":       {set("-gcflags", "-N -l"), BuildInfo{Unoptimized: true}},
		"no inlining":           {set("-gcflags", "all=-l"), BuildInfo{Unoptimized: true}},
		"no optimization alone": {set("-gcflags", "-N"), BuildInfo{Unoptimized: true}},
		"a package's flag":      {set("-gcflags", "-m"), BuildInfo{}},
	} {
		got := BuildInfo{CGO: true}
		applySettings(&got, c.settings)
		want := c.want
		want.CGO = name != "cgo off"
		if got != want {
			t.Errorf("%s: %+v, want %+v", name, got, want)
		}
	}
}
