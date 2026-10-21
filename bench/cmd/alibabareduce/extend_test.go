package main

import (
	"encoding/json"
	"math"
	"path/filepath"
	"reflect"
	"slices"
	"testing"

	"github.com/lotannauo/toposhift/internal/store"
)

// c60Result is the C60 stream's result of a reduction of the filler alone.
func c60Result(t *testing.T, res *results) sinkResult {
	t.Helper()
	for _, s := range res.sinks {
		if s.Name == "placement_c60" {
			return s
		}
	}
	t.Fatal("no placement_c60 stream")
	return sinkResult{}
}

// TestExtendFractionSetsTheRunsReassertion shows that -extend-fraction moves how often
// a steady heartbeat is stored, and the lag -recency counts with it: at a fraction a
// third of the TTL or less every sample is stored, at the product's half (and from a third
// to two thirds) every second one is, and past two thirds two of every three are, because
// of Flush. The lag never passes the extension interval. The streams are checked
// against their samples (VerifyStreams) at every fraction.
func TestExtendFractionSetsTheRunsReassertion(t *testing.T) {
	var def sinkResult
	for _, tc := range []struct {
		fraction float64
		records  int64 // of the three subjects of the filler over twelve minutes; 0: not pinned
		maxLag   int64
	}{
		{0.25, 36, 0},
		{0.4, 21, 1},
		{0.5, 21, 1},
		{0.66, 21, 1},
		// Past two thirds of the TTL the run is not extended at its second refresh, but
		// Flush writes that refresh before the stored copy lapses (the lead is a minute,
		// the sampling interval), so a run costs two records for every three samples,
		// more than the one for every two of the product's half.
		{0.75, 24, 2},
		{0.8, 24, 2},
		{1, 24, 2},
	} {
		res, _ := run(t, dataset{}, func(c *Config) { c.ExtendFraction, c.Recency = tc.fraction, true })
		got := c60Result(t, res)
		if tc.records != 0 && got.Records != tc.records {
			t.Errorf("fraction %g: %d records, want %d", tc.fraction, got.Records, tc.records)
		}
		t.Logf("fraction %g: %d records, %d at finish, lags %v", tc.fraction, got.Records, got.FinishRecords, got.Recency)
		if got.Recency["max"] != tc.maxLag {
			t.Errorf("fraction %g: the largest lag is %d sampling intervals, want %d (%v)", tc.fraction, got.Recency["max"], tc.maxLag, got.Recency)
		}
		var samples int64
		for k, n := range got.Recency {
			if k != "max" {
				samples += n
			}
		}
		if samples != got.Assertions {
			t.Errorf("fraction %g: the lags count %d samples, the stream took %d", tc.fraction, samples, got.Assertions)
		}
		if tc.fraction == 0.5 {
			def = got
		}
	}
	// The default is the product's half, and counting the lag changes nothing stored.
	res, cfg := run(t, dataset{}, nil)
	if cfg.ExtendFraction != 0.5 || c60Result(t, res).Records != def.Records || c60Result(t, res).Recency != nil {
		t.Errorf("default: fraction %g, %d records, lags %v", cfg.ExtendFraction, c60Result(t, res).Records, c60Result(t, res).Recency)
	}
}

func TestExtendFractionIsInZeroToOne(t *testing.T) {
	for _, f := range []float64{0, -0.5, 1.01, math.NaN(), math.Inf(1)} {
		cfg := defaultConfig()
		cfg.InDir, cfg.OutDir = "in", "out"
		cfg.ExtendFraction = f
		if err := cfg.validate(); err == nil {
			t.Errorf("fraction %g was accepted", f)
		}
	}
}

// TestWholeCoalescerWritesTheSplitOnesStream shows that, at the product's fraction, the
// unsplit coalescer writes the records the split one does, in the same order.
func TestWholeCoalescerWritesTheSplitOnesStream(t *testing.T) {
	d := dataset{placement: gapPods()}
	_, split := run(t, d, nil)
	_, whole := run(t, d, func(c *Config) { c.WholeCoalescer = true })
	for _, name := range []string{"placement_c60.parquet", "placement_c300.parquet"} {
		a, b := records(t, outPath(split, name)), records(t, outPath(whole, name))
		if len(a) == 0 || !slices.EqualFunc(a, b, func(x, y store.Record) bool { return reflect.DeepEqual(x, y) }) {
			t.Errorf("%s: the split coalescer wrote %d records, the whole one %d, or they differ", name, len(a), len(b))
		}
	}
}

// readManifest is the manifest a reduction wrote.
func readManifest(t *testing.T, cfg Config) manifestFile {
	t.Helper()
	var m manifestFile
	if err := json.Unmarshal(readFile(t, filepath.Join(cfg.OutDir, "manifest.json")), &m); err != nil {
		t.Fatal(err)
	}
	return m
}

// outputNamed is a manifest's output by name.
func outputNamed(t *testing.T, m manifestFile, name string) sinkResult {
	t.Helper()
	for _, o := range m.Outputs {
		if o.Name == name {
			return o
		}
	}
	t.Fatalf("the manifest has no output %s", name)
	return sinkResult{}
}

// frozenContentDigests are the content digests of the files a reduction of the dataset
// below wrote before -extend-fraction, -whole-coalescer and -recency existed. They
// are never regenerated to make a test pass: a change of one is a change of what a
// default reduction writes.
var frozenContentDigests = map[string]string{
	"calls.parquet":                 "sha256:a0359f66ab4acbe77c3a6f8de536c76163f51c5e5d3f2eb28210ce39a2a8ef40",
	"fixture_4min_200nodes.parquet": "sha256:ea871b31933f3a7b79f157dce1a89dee9997f174af4dd5e2d87852bdeebecda1",
	"placement_c300.parquet":        "sha256:66cf5cdec75578b61d477ea6bb5a4cf3260f7e64468c3d6bf4a1d4a4c38402de",
	"placement_c60.parquet":         "sha256:f53717feb50f6934211ea3d278ace03487278fb03884c8655c65112d07302e2a",
}

// TestTheDefaultsAreTheProductsAndWriteTheFilesTheyDid shows that a reduction that sets
// none of the new flags uses the product's coalescer, counts nothing it did not, and
// writes files of the content they had before the flags.
func TestTheDefaultsAreTheProductsAndWriteTheFilesTheyDid(t *testing.T) {
	cfg := defaultConfig()
	if cfg.ExtendFraction != 0.5 || cfg.Recency || cfg.WholeCoalescer {
		t.Errorf("the defaults are fraction %g, recency %v, whole %v", cfg.ExtendFraction, cfg.Recency, cfg.WholeCoalescer)
	}
	if cfg.placementCoalesce() != coalesceConfig() {
		t.Errorf("the default placement coalescer %+v is not the product's %+v", cfg.placementCoalesce(), coalesceConfig())
	}

	_, got := run(t, dataset{placement: join(gapPods(), boundaryPod()), calls: callsData().calls}, nil)
	m := readManifest(t, got)
	for file, want := range frozenContentDigests {
		info, err := readInfo(outPath(got, file))
		if err != nil {
			t.Fatal(err)
		}
		if info.Digest != want {
			t.Errorf("%s: content digest %s, want %s", file, info.Digest, want)
		}
	}
	for _, o := range m.Outputs {
		if want := frozenContentDigests[filepath.Base(o.File)]; want == "" || o.ContentDigest != want {
			t.Errorf("%s: the manifest's content digest is %s, want %s", o.Name, o.ContentDigest, want)
		}
		if o.Recency != nil || o.FinishRecords != 0 {
			t.Errorf("%s: counted a recency nobody asked for", o.Name)
		}
	}
	if !reflect.DeepEqual(m.Coalescer, m.ProductCoalescer) || m.Coalescer["extend_ttl_fraction"] != "0.5" {
		t.Errorf("the default manifest's coalescers: %v and the product's %v", m.Coalescer, m.ProductCoalescer)
	}
	for _, name := range []string{"placement_c60", "placement_c300", "calls", "fixture"} {
		if f := outputNamed(t, m, name).CoalescerFlush; f != "split" {
			t.Errorf("%s: the coalescer flushes %q by default, want split", name, f)
		}
	}
}

// TestTheManifestDescribesTheCoalescerThePlacementStreamsUsed shows that the manifest's
// coalescer is the one C60 and C300 ran with, the product's is kept under its own key,
// and each output says how its coalescer flushed.
func TestTheManifestDescribesTheCoalescerThePlacementStreamsUsed(t *testing.T) {
	_, cfg := run(t, dataset{}, func(c *Config) { c.ExtendFraction = 0.75 })
	m := readManifest(t, cfg)
	if got := m.Coalescer["extend_ttl_fraction"]; got != "0.75" {
		t.Errorf("the manifest's coalescer has fraction %q, want 0.75", got)
	}
	if got := m.ProductCoalescer["extend_ttl_fraction"]; got != "0.5" {
		t.Errorf("the product's coalescer has fraction %q, want 0.5", got)
	}
	if got := m.Flags["extend_fraction"]; got != 0.75 {
		t.Errorf("the flags record the fraction %v", got)
	}
	// A TTL of three minutes leaves 45 seconds after the extension interval, less than a
	// flush lead and a second, so C60 is flushed whole; C300's fifteen minutes are split.
	// The calls and the fixture keep the product's coalescer.
	for name, want := range map[string]string{"placement_c60": "whole", "placement_c300": "split", "calls": "split", "fixture": "split"} {
		if got := outputNamed(t, m, name).CoalescerFlush; got != want {
			t.Errorf("%s: the coalescer flushes %q, want %q", name, got, want)
		}
	}

	_, cfg = run(t, dataset{}, func(c *Config) { c.WholeCoalescer = true })
	m = readManifest(t, cfg)
	for name, want := range map[string]string{"placement_c60": "whole", "placement_c300": "whole", "calls": "split"} {
		if got := outputNamed(t, m, name).CoalescerFlush; got != want {
			t.Errorf("-whole-coalescer: %s flushes %q, want %q", name, got, want)
		}
	}
	if m.Flags["whole_coalescer"] != true {
		t.Errorf("the flags do not record -whole-coalescer: %v", m.Flags)
	}
}

// TestTheManifestHoldsEachFilesContentDigest shows that every written output names the
// digest in its file's footer, that it is the same for the same records, and that it moves
// with the records and not with the streams that were left alone.
func TestTheManifestHoldsEachFilesContentDigest(t *testing.T) {
	d := dataset{placement: join(gapPods(), boundaryPod()), calls: callsData().calls}
	_, a := run(t, d, arms(true, true))
	_, b := run(t, d, arms(true, true))
	_, c := run(t, d, func(c *Config) { c.ExtendFraction = 0.75 })
	ma, mb, mc := readManifest(t, a), readManifest(t, b), readManifest(t, c)
	if len(ma.Outputs) != 8 {
		t.Fatalf("%d outputs, want C60, C300, the calls, the four arms and the fixture", len(ma.Outputs))
	}
	for i, o := range ma.Outputs {
		if !o.Written {
			t.Errorf("%s: not written", o.Name)
			continue
		}
		info, err := readInfo(o.File)
		if err != nil {
			t.Fatal(err)
		}
		if o.ContentDigest == "" || o.ContentDigest != info.Digest {
			t.Errorf("%s: the manifest's content digest %q, the file's %q", o.Name, o.ContentDigest, info.Digest)
		}
		if mb.Outputs[i].ContentDigest != o.ContentDigest {
			t.Errorf("%s: two reductions of the same input have digests %s and %s", o.Name, o.ContentDigest, mb.Outputs[i].ContentDigest)
		}
	}
	for _, name := range []string{"placement_c60", "calls", "fixture"} {
		same := outputNamed(t, ma, name).ContentDigest == outputNamed(t, mc, name).ContentDigest
		if want := name != "placement_c60"; same != want {
			t.Errorf("%s: with a fraction of 0.75 the content digest is the default's: %v, want %v", name, same, want)
		}
	}
}
