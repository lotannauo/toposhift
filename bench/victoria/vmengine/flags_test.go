package vmengine_test

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"os/exec"
	"strings"
	"testing"

	"github.com/lotannauo/toposhift/bench/victoria/vmengine"
)

func TestParseFlags(t *testing.T) {
	got, err := vmengine.ParseFlags(strings.NewReader("# a comment\n\nretentionPeriod = 100y  # trailing\nsearch.maxStalenessInterval=1h\n"))
	if err != nil || len(got) != 2 || got[0] != (vmengine.Flag{Name: "retentionPeriod", Value: "100y"}) || got[1].Value != "1h" {
		t.Fatalf("%v, %v", got, err)
	}
	for name, in := range map[string]string{
		"not name = value":     "retentionPeriod\n",
		"an empty value":       "retentionPeriod =\n",
		"an empty name":        "= 1\n",
		"a name with a dash":   "-retentionPeriod = 1\n",
		"a name with a space":  "retention Period = 1\n",
		"a name twice":         "a = 1\na = 2\n",
		"no flag at all":       "# nothing\n",
		"a value after a hash": "# a = 1\n",
	} {
		if _, err := vmengine.ParseFlags(strings.NewReader(in)); err == nil {
			t.Errorf("%s: accepted", name)
		}
	}
}

// The file in scripts is the one source: it parses, every flag the registered table names
// is in it, and the script gives the server exactly its lines.
func TestFlagsConfIsTheOneSource(t *testing.T) {
	flags, sum, err := vmengine.LoadFlags("../scripts/flags.conf")
	if err != nil {
		t.Fatal(err)
	}
	if len(sum) != 64 {
		t.Errorf("sha256 %q", sum)
	}
	have := map[string]string{}
	for _, f := range flags {
		have[f.Name] = f.Value
	}
	for _, name := range []string{"retentionPeriod", "search.maxStalenessInterval", "dedup.minScrapeInterval", "search.latencyOffset", "search.maxUniqueTimeseries", "search.maxSamplesPerQuery", "search.maxPointsPerTimeseries"} {
		if have[name] == "" {
			t.Errorf("flag %s is not in flags.conf", name)
		}
	}
	for _, owned := range vmengine.ServerOwned {
		if _, ok := have[owned]; ok {
			t.Errorf("%s is set by the script and must not be in flags.conf", owned)
		}
	}
	out, err := exec.CommandContext(context.Background(), "bash", "../scripts/vm-server.sh", "flags").Output()
	if err != nil {
		t.Skipf("the script cannot be run here: %v", err)
	}
	var want []string
	for _, f := range flags {
		want = append(want, "-"+f.Name+"="+f.Value)
	}
	if got := strings.TrimSpace(string(out)); got != strings.Join(want, "\n") {
		t.Errorf("the script passes\n%s\nthe file says\n%s", got, strings.Join(want, "\n"))
	}
	if _, _, err := vmengine.LoadFlags("../scripts/missing.conf"); err == nil {
		t.Error("a missing file")
	}
}

func TestCompareFlags(t *testing.T) {
	want := []vmengine.Flag{{Name: "retentionPeriod", Value: "100y"}, {Name: "search.maxStalenessInterval", Value: "1h"}, {Name: "dedup.minScrapeInterval", Value: "0s"}}
	good := map[string]string{"retentionPeriod": "100y", "search.maxStalenessInterval": "1h0m0s", "dedup.minScrapeInterval": "0s", "storageDataPath": "/d", "httpListenAddr": ":8428"}
	if err := vmengine.CompareFlags(want, good); err != nil {
		t.Errorf("the server's flags: %v", err)
	}
	with := func(f func(map[string]string)) map[string]string {
		m := map[string]string{}
		for k, v := range good {
			m[k] = v
		}
		f(m)
		return m
	}
	for name, got := range map[string]map[string]string{
		"a different value":              with(func(m map[string]string) { m["retentionPeriod"] = "1y" }),
		"a different duration":           with(func(m map[string]string) { m["search.maxStalenessInterval"] = "30m0s" }),
		"a flag left at its default":     with(func(m map[string]string) { delete(m, "dedup.minScrapeInterval") }),
		"a flag that is not in the file": with(func(m map[string]string) { m["search.maxQueryDuration"] = "1h" }),
	} {
		err := vmengine.CompareFlags(want, got)
		var fe *vmengine.FlagsError
		if !errors.Is(err, vmengine.ErrFlagsDiffer) || !errors.As(err, &fe) || len(fe.Problems) != 1 {
			t.Errorf("%s: %v", name, err)
		}
	}
	// A value written as text that is not a duration is compared as text.
	if err := vmengine.CompareFlags([]vmengine.Flag{{Name: "retentionPeriod", Value: "100y"}}, map[string]string{"retentionPeriod": "100y"}); err != nil {
		t.Error(err)
	}
}

func TestCheckFlagsReadsTheServer(t *testing.T) {
	flags := []vmengine.Flag{{Name: "retentionPeriod", Value: "100y"}}
	for body, wantErr := range map[string]bool{
		"-retentionPeriod=\"100y\"\n-httpListenAddr=\":8428\"\n": false,
		"-retentionPeriod=\"1y\"\n":                              true,
		"":                                                       true,
	} {
		_, c := newFake(t, func(w http.ResponseWriter, _ *http.Request) { fmt.Fprint(w, body) })
		err := vmengine.CheckFlags(context.Background(), c, flags)
		if (err != nil) != wantErr {
			t.Errorf("%q: %v", body, err)
		}
	}
}
