package pebblekv

import (
	"bytes"
	"context"
	"math"
	"os"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/cockroachdb/pebble/v2"
	"github.com/cockroachdb/pebble/v2/vfs"
)

// effective is the options as Pebble holds them once it has filled in its defaults,
// as text, without the two lines that depend on the machine or on the test: the table
// cache's shard count (the number of CPUs) and, when dropCache is set, the cache's
// size.
func effective(o *pebble.Options, dropCache bool) string {
	o = o.Clone()
	o.EnsureDefaults()
	var keep []string
	for _, line := range strings.Split(o.String(), "\n") {
		if strings.HasPrefix(line, "  table_cache_shards=") || (dropCache && strings.HasPrefix(line, "  cache_size=")) {
			continue
		}
		keep = append(keep, line)
	}
	return strings.Join(keep, "\n")
}

// changes is the lines that differ between two texts of options, as the lines of the
// first with a "-" and of the second with a "+", in order; the texts are of one
// shape, line for line.
func changes(t *testing.T, was, now string) []string {
	t.Helper()
	a, b := strings.Split(was, "\n"), strings.Split(now, "\n")
	if len(a) != len(b) {
		// Only a private setting adds a line, and it is the same place in both.
		var out []string
		have := map[string]bool{}
		for _, l := range a {
			have[l] = true
		}
		for _, l := range b {
			if !have[l] {
				out = append(out, "+"+strings.TrimSpace(l))
			}
		}
		return out
	}
	var out []string
	for i := range a {
		if a[i] != b[i] {
			out = append(out, "-"+strings.TrimSpace(a[i]), "+"+strings.TrimSpace(b[i]))
		}
	}
	return out
}

// With the tunings the product and the tests use, none of the options this package
// gained fields for is touched: the options Pebble is given are the ones it was given
// before them, which is the text kept in testdata, written from the code before the
// fields existed. A Pebble upgrade that moved a default shows here too.
func TestDefaultTuningsLeaveTheOptionsAsTheyWere(t *testing.T) {
	t.Parallel()
	cache := pebble.NewCache(1 << 20)
	defer cache.Unref()

	// The product's tuning comes to the benchmark tuning's options, which are those of
	// before: see also TestProductTuningComesToTheOptionsOfBefore.
	tunings := map[string]Tuning{"bench": BenchTuning(), "tiny": TinyTuning(), "product": ProductTuning()}
	golden := func(name string) string {
		if name == "product" {
			return "bench"
		}
		return name
	}
	for name, tune := range tunings {
		want, err := os.ReadFile("testdata/default_options_" + golden(name) + ".txt")
		if err != nil {
			t.Fatal(err)
		}
		o, err := buildOptions(BytewiseLayout, Config{Schema: SchemaDefault, Tuning: tune}, cache)
		if err != nil {
			t.Fatal(err)
		}
		if got := effective(o, false); got != string(want) {
			t.Errorf("%s: the options are not those of before:\n%s\nwant\n%s", name, got, want)
		}
		// The fields the tuning does not set are left unset, not set to what Pebble
		// would fill in, so that Pebble's own rule applies at whatever the version. (The
		// product's tuning writes out the three signals of compaction concurrency, and
		// nothing else.)
		if name != "product" && (o.Experimental.L0CompactionConcurrency != 0 || o.Experimental.CompactionDebtConcurrency != 0 || o.Experimental.CompactionGarbageFractionForMaxConcurrency != nil) {
			t.Errorf("%s: a signal of compaction concurrency is set", name)
		}
		if o.CompactionConcurrencyRange != nil || o.L0StopWritesThreshold != 0 {
			t.Errorf("%s: concurrency set %v, stop threshold %d", name, o.CompactionConcurrencyRange != nil, o.L0StopWritesThreshold)
		}
		for i := range o.Levels {
			if o.Levels[i].Compression != nil {
				t.Errorf("%s: level %d has a compression set", name, i)
			}
			if i > 0 && o.TargetFileSizes[i] != 0 {
				t.Errorf("%s: level %d has a target size set", name, i)
			}
		}
		if strings.Contains(effective(o, false), "disable_delete_only") {
			t.Errorf("%s: delete-only compactions are named in the options", name)
		}
	}

	// And what an open database reports, which is the path the store takes (Open fills
	// in the cache's size, so that line is left out).
	for name, tune := range tunings {
		want, err := os.ReadFile("testdata/default_options_" + golden(name) + ".txt")
		if err != nil {
			t.Fatal(err)
		}
		kv := bytewise(t, vfs.NewMem(), "db", Config{Tuning: tune})
		got := effective(kv.Options(), true)
		_ = kv.Close()
		var wantLines []string
		for _, l := range strings.Split(string(want), "\n") {
			if !strings.HasPrefix(l, "  cache_size=") {
				wantLines = append(wantLines, l)
			}
		}
		if got != strings.Join(wantLines, "\n") {
			t.Errorf("%s: the open database's options differ from those of before:\n%s", name, strings.Join(changes(t, strings.Join(wantLines, "\n"), got), "\n"))
		}
	}
}

// The defaults of Pebble that the fields fall back on, written in the documentation, in
// Validate and in ProductTuning, are the pinned version's: a Pebble upgrade that moves
// one fails here, and the product's options are then to be decided again.
func TestPebbleDefaultsAreAsAssumed(t *testing.T) {
	t.Parallel()
	var o pebble.Options
	o.EnsureDefaults()
	if o.L0CompactionThreshold != pebbleL0CompactionThreshold || o.L0StopWritesThreshold != pebbleL0StopWritesThreshold {
		t.Errorf("Pebble's level 0 thresholds are %d and %d, the code assumes %d and %d",
			o.L0CompactionThreshold, o.L0StopWritesThreshold, pebbleL0CompactionThreshold, pebbleL0StopWritesThreshold)
	}
	if lower, upper := o.CompactionConcurrencyRange(); lower != 1 || upper != 1 {
		t.Errorf("Pebble's compaction concurrency is (%d, %d), the documentation says (1, 1)", lower, upper)
	}
	if o.Experimental.L0CompactionConcurrency != pebbleL0CompactionConcurrency || o.Experimental.CompactionDebtConcurrency != pebbleCompactionDebtConcurrency ||
		o.Experimental.CompactionGarbageFractionForMaxConcurrency() != pebbleCompactionGarbageFraction {
		t.Errorf("the signals that raise compaction concurrency are %d sublevels, %d bytes and %v of garbage; ProductTuning writes out %d, %d and %v",
			o.Experimental.L0CompactionConcurrency, o.Experimental.CompactionDebtConcurrency, o.Experimental.CompactionGarbageFractionForMaxConcurrency(),
			pebbleL0CompactionConcurrency, uint64(pebbleCompactionDebtConcurrency), pebbleCompactionGarbageFraction)
	}
	for i := range o.Levels {
		if got := o.Levels[i].Compression().Name; got != "Snappy" {
			t.Errorf("level %d compresses with %s by default", i, got)
		}
	}
}

// Each field reaches the Pebble options as its documentation says, and changes nothing
// else in them: the text of the options differs from the default's in exactly the lines
// the field names.
func TestEachTuningFieldReachesPebble(t *testing.T) {
	t.Parallel()
	cache := pebble.NewCache(1 << 20)
	defer cache.Unref()
	build := func(tune Tuning) *pebble.Options {
		t.Helper()
		o, err := buildOptions(BytewiseLayout, Config{Schema: SchemaDefault, Tuning: tune}, cache)
		if err != nil {
			t.Fatal(err)
		}
		return o
	}
	def := effective(build(BenchTuning()), false)

	const mib = 1 << 20
	for _, tc := range []struct {
		name string
		set  func(*Tuning)
		// want is the lines that change, "-" before and "+" after, in the order of the
		// options text.
		want []string
		// check looks at the options themselves.
		check func(t *testing.T, o *pebble.Options)
	}{
		{
			name: "base-level target",
			set:  func(x *Tuning) { x.LBaseTargetFileSize = 4 * mib },
			want: []string{
				"-target_file_size=134217728", "+target_file_size=4194304",
				"-target_file_size=268435456", "+target_file_size=8388608",
				"-target_file_size=536870912", "+target_file_size=16777216",
				"-target_file_size=1073741824", "+target_file_size=33554432",
				"-target_file_size=2147483648", "+target_file_size=67108864",
				"-target_file_size=4294967296", "+target_file_size=134217728",
			},
			check: func(t *testing.T, o *pebble.Options) {
				if o.TargetFileSizes[0] != 64*mib || o.TargetFileSizes[1] != 4*mib {
					t.Errorf("targets %v", o.TargetFileSizes)
				}
			},
		},
		{
			name: "target sizes of every level",
			set: func(x *Tuning) {
				x.TargetFileSizes = [numTargetLevels]int64{64 * mib, 4 * mib, 8 * mib, 64 * mib, 128 * mib, 256 * mib, 512 * mib}
			},
			want: []string{
				"-target_file_size=134217728", "+target_file_size=4194304",
				"-target_file_size=268435456", "+target_file_size=8388608",
				"-target_file_size=536870912", "+target_file_size=67108864",
				"-target_file_size=1073741824", "+target_file_size=134217728",
				"-target_file_size=2147483648", "+target_file_size=268435456",
				"-target_file_size=4294967296", "+target_file_size=536870912",
			},
			check: func(t *testing.T, o *pebble.Options) {
				if want := [numTargetLevels]int64{64 * mib, 4 * mib, 8 * mib, 64 * mib, 128 * mib, 256 * mib, 512 * mib}; o.TargetFileSizes != want {
					t.Errorf("targets %v, want %v", o.TargetFileSizes, want)
				}
			},
		},
		{
			name: "target sizes of the first levels only",
			set:  func(x *Tuning) { x.TargetFileSizes = [numTargetLevels]int64{0, 4 * mib, 8 * mib} },
			want: []string{
				"-target_file_size=134217728", "+target_file_size=4194304",
				"-target_file_size=268435456", "+target_file_size=8388608",
				"-target_file_size=536870912", "+target_file_size=16777216",
				"-target_file_size=1073741824", "+target_file_size=33554432",
				"-target_file_size=2147483648", "+target_file_size=67108864",
				"-target_file_size=4294967296", "+target_file_size=134217728",
			},
		},
		{
			name: "compaction concurrency",
			set:  func(x *Tuning) { x.CompactionConcurrencyLower, x.CompactionConcurrencyUpper = 2, 4 },
			want: []string{"-concurrent_compactions=1", "+concurrent_compactions=2", "-max_concurrent_compactions=1", "+max_concurrent_compactions=4"},
			check: func(t *testing.T, o *pebble.Options) {
				if lower, upper := o.CompactionConcurrencyRange(); lower != 2 || upper != 4 {
					t.Errorf("range (%d, %d)", lower, upper)
				}
			},
		},
		{
			name: "compaction concurrency, an upper bound only",
			set:  func(x *Tuning) { x.CompactionConcurrencyUpper = 3 },
			want: []string{"-max_concurrent_compactions=1", "+max_concurrent_compactions=3"},
		},
		{
			name: "compaction concurrency, a lower bound only",
			set:  func(x *Tuning) { x.CompactionConcurrencyLower = 3 },
			want: []string{"-concurrent_compactions=1", "+concurrent_compactions=3", "-max_concurrent_compactions=1", "+max_concurrent_compactions=3"},
		},
		{
			name: "level 0 stop threshold",
			set:  func(x *Tuning) { x.L0StopWritesThreshold = 20 },
			want: []string{"-l0_stop_writes_threshold=12", "+l0_stop_writes_threshold=20"},
		},
		{
			name: "level 0 stop threshold with the compaction threshold",
			set:  func(x *Tuning) { x.L0StopWritesThreshold, x.L0CompactionThreshold = 11, 8 },
			want: []string{"-l0_compaction_threshold=4", "+l0_compaction_threshold=8", "-l0_stop_writes_threshold=12", "+l0_stop_writes_threshold=11"},
		},
		{
			name: "l0 compaction concurrency",
			set:  func(x *Tuning) { x.L0CompactionConcurrency = 6 },
			want: []string{"-l0_compaction_concurrency=10", "+l0_compaction_concurrency=6"},
		},
		{
			name: "compaction debt concurrency",
			set:  func(x *Tuning) { x.CompactionDebtConcurrency = 256 << 20 },
			want: []string{"-compaction_debt_concurrency=1073741824", "+compaction_debt_concurrency=268435456"},
		},
		{
			name: "compaction garbage fraction",
			set:  func(x *Tuning) { x.CompactionGarbageFraction = 0.2 },
			want: []string{"-compaction_garbage_fraction_for_max_concurrency=0.40", "+compaction_garbage_fraction_for_max_concurrency=0.20"},
		},
		{
			name: "the product's signals written out",
			set:  func(x *Tuning) { *x = ProductTuning() },
			want: nil,
		},
		{
			name: "delete-only compactions off",
			set:  func(x *Tuning) { x.DisableDeleteOnlyCompactions = true },
			want: []string{"+disable_delete_only_compactions=true"},
		},
		{
			name: "snappy, written out",
			set:  func(x *Tuning) { x.Compression = CompressionSnappy },
			want: nil, // the same text: Snappy is the default
			check: func(t *testing.T, o *pebble.Options) {
				for i := range o.Levels {
					if o.Levels[i].Compression == nil || o.Levels[i].Compression().Name != "Snappy" {
						t.Errorf("level %d is not set to Snappy", i)
					}
				}
			},
		},
		{
			name: "zstd",
			set:  func(x *Tuning) { x.Compression = CompressionZstd },
			want: levelsCompressed("ZSTD", "ZSTD", "ZSTD", "ZSTD", "ZSTD", "ZSTD", "ZSTD"),
		},
		{
			name: "balanced",
			set:  func(x *Tuning) { x.Compression = CompressionBalanced },
			want: levelsCompressed("Fastest", "Fastest", "Fastest", "Fastest", "Fastest", "Fast", "Balanced"),
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			tune := BenchTuning()
			tc.set(&tune)
			o := build(tune)
			got := changes(t, def, effective(o, false))
			if strings.Join(got, "\n") != strings.Join(tc.want, "\n") {
				t.Errorf("the options change by\n%s\nwant\n%s", strings.Join(got, "\n"), strings.Join(tc.want, "\n"))
			}
			if tc.check != nil {
				tc.check(t, o)
			}
		})
	}
}

// levelsCompressed is the change of the options text that sets the compression of the
// levels to names, in level order, for the levels whose name is not Snappy.
func levelsCompressed(names ...string) []string {
	var out []string
	for _, n := range names {
		if n != "Snappy" {
			out = append(out, "-compression=Snappy", "+compression="+n)
		}
	}
	return out
}

// A tuning that cannot be honoured is refused by Validate, by buildOptions and by Open
// (which then leaves nothing open), with a message that names the field; a limit case
// that can be honoured is not.
func TestInvalidTuningsAreRefused(t *testing.T) {
	t.Parallel()
	tiny := TinyTuning()
	for _, tc := range []struct {
		name string
		set  func(*Tuning)
		want string // in the message
	}{
		{"negative base target", func(x *Tuning) { x.LBaseTargetFileSize = -1 }, "base-level target file size"},
		{"negative level target", func(x *Tuning) { x.TargetFileSizes[3] = -1 }, "target file size -1 at index 3"},
		{"base target with level targets", func(x *Tuning) { x.LBaseTargetFileSize = 1 << 20; x.TargetFileSizes[2] = 1 << 20 }, "give one"},
		{"level 0 target not the canonical size", func(x *Tuning) { x.TargetFileSizes[0] = x.TargetFileSize + 1 }, "differs from TargetFileSize"},
		{"negative lower concurrency", func(x *Tuning) { x.CompactionConcurrencyLower = -1 }, "compaction concurrency"},
		{"negative upper concurrency", func(x *Tuning) { x.CompactionConcurrencyLower = 1; x.CompactionConcurrencyUpper = -2 }, "compaction concurrency"},
		{"lower above upper", func(x *Tuning) { x.CompactionConcurrencyLower, x.CompactionConcurrencyUpper = 4, 2 }, "above the upper bound"},
		{"negative stop threshold", func(x *Tuning) { x.L0StopWritesThreshold = -1 }, "stop threshold"},
		{"stop threshold below the compaction threshold", func(x *Tuning) { x.L0CompactionThreshold = 6; x.L0StopWritesThreshold = 5 }, "below the compaction threshold 6"},
		{"stop threshold below Pebble's compaction threshold", func(x *Tuning) { x.L0CompactionThreshold = 0; x.L0StopWritesThreshold = 3 }, "below the compaction threshold 4"},
		// The retainer puts its flush off 2 sublevels below the stop: with compactions of
		// level 0 beginning at 4, a stop of 6 puts it off at 4, the depth level 0 has.
		{"stop threshold within the retainer's margin", func(x *Tuning) { x.L0CompactionThreshold = 0; x.L0StopWritesThreshold = 6 }, "within 2 sublevels of the compaction threshold 4"},
		{"stop threshold equal to the compaction threshold", func(x *Tuning) { x.L0StopWritesThreshold = x.L0CompactionThreshold }, "within 2 sublevels"},
		{"a compaction threshold that leaves Pebble's stop within the margin", func(x *Tuning) { x.L0CompactionThreshold = 10 }, "within 2 sublevels of the compaction threshold 10"},
		{"a compaction threshold above Pebble's stop", func(x *Tuning) { x.L0CompactionThreshold = 13 }, "below the compaction threshold 13"},
		{"negative l0 compaction concurrency", func(x *Tuning) { x.L0CompactionConcurrency = -1 }, "L0 compaction concurrency"},
		{"negative garbage fraction", func(x *Tuning) { x.CompactionGarbageFraction = -0.1 }, "garbage fraction"},
		{"garbage fraction above 1", func(x *Tuning) { x.CompactionGarbageFraction = 1.5 }, "garbage fraction"},
		{"garbage fraction not a number", func(x *Tuning) { x.CompactionGarbageFraction = math.NaN() }, "garbage fraction"},
		{"unknown compression", func(x *Tuning) { x.Compression = 99 }, "unknown compression Compression(99)"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			tune := tiny
			tc.set(&tune)
			err := tune.Validate()
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("Validate = %v, want an error naming %q", err, tc.want)
			}
			cache := pebble.NewCache(1 << 20)
			defer cache.Unref()
			if _, err := buildOptions(BytewiseLayout, Config{Schema: SchemaDefault, Tuning: tune}, cache); err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Errorf("buildOptions = %v, want an error naming %q", err, tc.want)
			}
			fs := vfs.NewMem()
			kv, err := Open("db", BytewiseLayout, Config{FS: fs, Schema: SchemaDefault, Tuning: tune, Logger: quietLogger{}})
			if err == nil {
				_ = kv.Close()
				t.Fatal("Open accepted the tuning")
			}
			if !strings.Contains(err.Error(), tc.want) {
				t.Errorf("Open = %v, want an error naming %q", err, tc.want)
			}
			if names, _ := fs.List("db"); len(names) != 0 {
				t.Errorf("a refused open left %v behind", names)
			}
		})
	}

	// What is on the edge of the rules is accepted.
	for name, set := range map[string]func(*Tuning){
		"stop just above the retainer's margin": func(x *Tuning) { x.L0StopWritesThreshold = x.L0CompactionThreshold + RetainFlushL0Margin + 1 },
		"every signal at Pebble's value":        func(x *Tuning) { *x = x.WithShapeOf(ProductTuning()) },
		"a garbage fraction of 1":               func(x *Tuning) { x.CompactionGarbageFraction = 1 },
		"level 0 target equal to the canonical": func(x *Tuning) { x.TargetFileSizes[0] = x.TargetFileSize },
		"equal bounds":                          func(x *Tuning) { x.CompactionConcurrencyLower, x.CompactionConcurrencyUpper = 3, 3 },
		"no choice at all":                      func(x *Tuning) {},
	} {
		tune := tiny
		set(&tune)
		if err := tune.Validate(); err != nil {
			t.Errorf("%s: %v", name, err)
		}
	}
}

// ParseCompression names what String names, and refuses what it does not know.
func TestParseCompression(t *testing.T) {
	t.Parallel()
	for _, c := range []Compression{CompressionDefault, CompressionSnappy, CompressionZstd, CompressionBalanced} {
		got, err := ParseCompression(c.String())
		if err != nil || got != c {
			t.Errorf("ParseCompression(%q) = %v, %v", c, got, err)
		}
	}
	if got, err := ParseCompression(""); err != nil || got != CompressionDefault {
		t.Errorf("ParseCompression(\"\") = %v, %v", got, err)
	}
	for _, bad := range []string{"lz4", "Snappy", "zstd3", " zstd"} {
		if _, err := ParseCompression(bad); err == nil {
			t.Errorf("ParseCompression(%q) was accepted", bad)
		}
	}
}

// The options an open database was given reach its OPTIONS file, and level 0's stop
// threshold is the one the store reads to put off a flush near a stall.
func TestAnOpenDatabaseHasTheTuningItWasGiven(t *testing.T) {
	t.Parallel()
	fs := vfs.NewMem()
	tune := TinyTuning()
	tune.CompactionConcurrencyLower, tune.CompactionConcurrencyUpper = 2, 3
	tune.L0StopWritesThreshold = 7
	tune.DisableDeleteOnlyCompactions = true
	tune.Compression = CompressionZstd
	tune.TargetFileSizes = [numTargetLevels]int64{0, 16 << 10}
	kv := bytewise(t, fs, "db", Config{Tuning: tune})
	defer func() { _ = kv.Close() }()

	if lower, upper := kv.Options().CompactionConcurrencyRange(); lower != 2 || upper != 3 {
		t.Errorf("the open database runs compactions in (%d, %d)", lower, upper)
	}
	if _, stop := kv.L0Sublevels(); stop != 7 {
		t.Errorf("level 0 stops writes at %d, want 7", stop)
	}
	text := optionsFile(t, fs, "db")
	for _, line := range []string{
		"concurrent_compactions=2", "max_concurrent_compactions=3", "l0_stop_writes_threshold=7",
		"disable_delete_only_compactions=true", "compression=ZSTD", "target_file_size=16384",
	} {
		if !strings.Contains(text, line) {
			t.Errorf("the options file lacks %q:\n%s", line, text)
		}
	}
}

// The compression chosen is the one the tables are written with, and a database
// written under one compression reads under another: each block says how it was
// compressed.
func TestTablesAreWrittenWithTheCompressionChosen(t *testing.T) {
	t.Parallel()
	val := bytes.Repeat([]byte("topology "), 40)
	write := func(kv *KV, from, n int) {
		t.Helper()
		for i := range n {
			if err := kv.Set([]byte{'k', byte((from + i) >> 8), byte(from + i)}, val, kv.WriteOptions()); err != nil {
				t.Fatal(err)
			}
		}
		if err := kv.Flush(); err != nil {
			t.Fatal(err)
		}
	}
	names := func(kv *KV) map[string]bool {
		t.Helper()
		props, err := kv.TableProperties()
		if err != nil {
			t.Fatal(err)
		}
		out := map[string]bool{}
		for _, p := range props {
			out[p.CompressionName] = true
		}
		return out
	}

	fs := vfs.NewMem()
	for i, c := range []Compression{CompressionSnappy, CompressionZstd, CompressionBalanced} {
		tune := TinyTuning()
		tune.Compression = c
		tune.CacheBytes = 4 << 20
		kv := bytewise(t, fs, "db", Config{Tuning: tune})
		// Everything written before, under the other compressions, reads.
		for j := range i * 100 {
			if v, closer, err := kv.Get([]byte{'k', byte(j >> 8), byte(j)}); err != nil || !bytes.Equal(v, val) {
				t.Fatalf("%s: key %d: %v", c, j, err)
			} else {
				_ = closer.Close()
			}
		}
		write(kv, i*100, 100)
		got := names(kv)
		want := map[Compression]string{CompressionSnappy: "Snappy", CompressionZstd: "ZSTD", CompressionBalanced: ""}[c]
		if want != "" && !got[want] {
			t.Errorf("%s: table compression names %v, want %q among them", c, got, want)
		}
		if err := kv.Close(); err != nil {
			t.Fatal(err)
		}
	}
}

// Delete-only compactions are off only when the tuning says so, through Pebble's
// private setting, which only Options.Parse reaches at the pinned version: a table a
// range deletion covers wholly is dropped by a delete-only compaction with the default
// and is not with the setting. (That the options text then names the setting and
// differs from the default's in nothing else is TestEachTuningFieldReachesPebble's.) A
// Pebble upgrade that renamed or dropped the setting fails here.
func TestTheDeleteOnlyCompactionsAreOff(t *testing.T) {
	t.Parallel()
	deleteOnly := func(disable bool) int64 {
		t.Helper()
		tune := BenchTuning() // the tables stay where they are put: one at the bottom, one in level 0
		tune.DisableDeleteOnlyCompactions = disable
		kv := bytewise(t, vfs.NewMem(), "db", Config{Tuning: tune})
		defer func() { _ = kv.Close() }()
		for i := range 50 {
			if err := kv.Set([]byte{'k', byte(i)}, bytes.Repeat([]byte{byte(i)}, 100), kv.WriteOptions()); err != nil {
				t.Fatal(err)
			}
		}
		if err := kv.Flush(); err != nil {
			t.Fatal(err)
		}
		if err := kv.Compact(context.Background(), []byte{'k'}, []byte{'l'}, false); err != nil {
			t.Fatal(err)
		}
		if err := kv.DeleteRange([]byte{'k'}, []byte{'l'}, kv.WriteOptions()); err != nil {
			t.Fatal(err)
		}
		if err := kv.Settle(); err != nil {
			t.Fatal(err)
		}
		// The hint comes from the deletion's table statistics, which Pebble loads in the
		// background, and the compaction from the next scheduling after them.
		for deadline := time.Now().Add(30 * time.Second); ; time.Sleep(time.Millisecond) {
			m := kv.Metrics()
			if n := m.Compact.DeleteOnlyCount; n > 0 || time.Now().After(deadline) {
				return n
			}
			// Off, the deletion's statistics are loaded (its estimate of what it reclaims
			// beneath is in), and what Pebble schedules after them has run.
			if disable && m.Table.Garbage.RangeDeletionsBytesEstimate > 0 && m.Table.PendingStatsCollectionCount == 0 {
				if err := kv.Settle(); err != nil {
					t.Fatal(err)
				}
				return kv.Metrics().Compact.DeleteOnlyCount
			}
		}
	}
	if n := deleteOnly(false); n == 0 {
		t.Error("with the default no delete-only compaction ran: the test does not reach one")
	}
	if n := deleteOnly(true); n != 0 {
		t.Errorf("with delete-only compactions off, %d ran", n)
	}
}

// The shape of a tuning is every field but the sizing ones, so a field added to Tuning
// is in WithShapeOf, HasShape and ShapeString unless it is named a sizing field here.
func TestTheShapeOfATuningIsEveryFieldButTheSizingOnes(t *testing.T) {
	t.Parallel()
	sizing := map[string]bool{
		"MemTableSize": true, "BlockSize": true, "TargetFileSize": true, "LBaseMaxBytes": true,
		"L0CompactionThreshold": true, "CacheBytes": true, "FlushDelayDeleteRange": true,
	}
	rt := reflect.TypeOf(Tuning{})
	for i := range rt.NumField() {
		name := rt.Field(i).Name
		var one Tuning
		f := reflect.ValueOf(&one).Elem().Field(i)
		switch f.Kind() {
		case reflect.Int, reflect.Int64:
			f.SetInt(3)
		case reflect.Uint8, reflect.Uint64:
			f.SetUint(3)
		case reflect.Float64:
			f.SetFloat(0.3)
		case reflect.Bool:
			f.SetBool(true)
		case reflect.Array:
			f.Index(0).SetInt(3)
		default:
			// A field of a kind this test does not know how to set is for its author to
			// add here.
			if !sizing[name] {
				t.Errorf("field %s is of kind %s, which this test cannot set", name, f.Kind())
			}
			continue
		}
		if sizing[name] {
			if one.HasShape() || one.ShapeString() != "" {
				t.Errorf("sizing field %s is part of the shape", name)
			}
			continue
		}
		if !one.HasShape() || one.ShapeString() == "" {
			t.Errorf("field %s is not part of the shape (HasShape %v, ShapeString %q)", name, one.HasShape(), one.ShapeString())
		}
		if got := (Tuning{}).WithShapeOf(one); got != one {
			t.Errorf("WithShapeOf did not copy %s: %+v", name, got)
		}
		if got := BenchTuning().WithShapeOf(one); got.MemTableSize != BenchTuning().MemTableSize || got.CacheBytes != BenchTuning().CacheBytes {
			t.Errorf("WithShapeOf changed a sizing field when copying %s", name)
		}
	}
	if (Tuning{}).HasShape() || BenchTuning().HasShape() || TinyTuning().HasShape() || ProductTuning().HasShape() {
		t.Error("a default tuning has a shape")
	}
}

func TestShapeString(t *testing.T) {
	t.Parallel()
	tune := BenchTuning()
	tune.TargetFileSizes = [numTargetLevels]int64{64 << 20, 4 << 20}
	tune.CompactionConcurrencyUpper = 3
	tune.L0StopWritesThreshold = 20
	tune.DisableDeleteOnlyCompactions = true
	tune.L0CompactionConcurrency = 6
	tune.CompactionDebtConcurrency = 256 << 20
	tune.CompactionGarbageFraction = 0.25
	tune.Compression = CompressionBalanced
	const want = "target_file_sizes=67108864,4194304,0,0,0,0,0 compaction_concurrency=1-3 l0_stop_writes_threshold=20 delete_only_compactions=off l0_compaction_concurrency=6 compaction_debt_concurrency=268435456 compaction_garbage_fraction=0.25 compression=balanced"
	if got := tune.ShapeString(); got != want {
		t.Errorf("ShapeString = %q, want %q", got, want)
	}
	tune = BenchTuning()
	tune.LBaseTargetFileSize = 4 << 20
	if got := tune.ShapeString(); got != "lbase_target_file_size=4194304" {
		t.Errorf("ShapeString = %q", got)
	}
}

// The benchmark tuning is frozen: the numbers of the spike and the RocksDB module's mapping
// rest on it. A change to it is a decision, and the product's settings are changed in
// ProductTuning.
func TestBenchTuningIsFrozen(t *testing.T) {
	t.Parallel()
	want := Tuning{
		MemTableSize: 64 << 20, BlockSize: 32 << 10, TargetFileSize: 64 << 20, LBaseMaxBytes: 256 << 20,
		L0CompactionThreshold: 4, CacheBytes: 256 << 20,
	}
	if got := BenchTuning(); got != want {
		t.Errorf("BenchTuning() = %+v, want %+v", got, want)
	}
}

// The product's tuning is the benchmark tuning's sizes, with the signals of compaction
// concurrency written out at Pebble's values, and comes to the options of before, byte for
// byte (TestDefaultTuningsLeaveTheOptionsAsTheyWere compares the text; this is the
// statement of why it can). Writing the signals out is what makes a Pebble upgrade that
// moves them a decision: TestPebbleDefaultsAreAsAssumed fails on it.
func TestProductTuningComesToTheOptionsOfBefore(t *testing.T) {
	t.Parallel()
	p, b := ProductTuning(), BenchTuning()
	if p.WithShapeOf(b) != b {
		t.Errorf("the product tuning %+v differs from the benchmark tuning %+v beyond the signals", p, b)
	}
	if p.L0CompactionConcurrency != 10 || p.CompactionDebtConcurrency != 1<<30 || p.CompactionGarbageFraction != 0.4 {
		t.Errorf("the product writes out %d, %d and %v", p.L0CompactionConcurrency, p.CompactionDebtConcurrency, p.CompactionGarbageFraction)
	}
	if p.ShapeString() != "" || p.HasShape() {
		t.Errorf("the product tuning is shaped: %q", p.ShapeString())
	}
	if err := p.Validate(); err != nil {
		t.Error(err)
	}
}
