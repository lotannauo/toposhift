package backup_test

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/lotannauo/toposhift/internal/backup"
	"github.com/lotannauo/toposhift/internal/lifecycle"
	"github.com/lotannauo/toposhift/internal/segment"
	"github.com/lotannauo/toposhift/internal/store"
)

// goldenManifest has every field of manifest version 1 set, and is the manifest
// testdata/manifest_v1.json holds.
func goldenManifest() backup.Manifest {
	hz := func(at int64, seq uint64) backup.Horizon { return backup.Horizon{TimeNS: at, Seq: seq} }
	return backup.Manifest{
		Format: backup.Format, Version: backup.Version, Generation: 7, Seq: 120,
		Prev: strings.Repeat("ab", 32),
		Store: backup.StoreMeta{
			ClusterName: "prod-eu", ClusterNameSet: true, BootKey: lifecycle.BootID, Skew: 2 * time.Second,
			Offsets: [4]time.Duration{0, 60 * 24 * time.Hour, 0, 0}, Keep: [4]bool{true, false, false, false},
		},
		Margin: 24 * time.Hour, MaxClockAhead: 5 * time.Minute,
		Retentions: []backup.Retention{
			{AskedNS: 1_800_000_000_000_000_000, Seq: 90, Horizons: [4]backup.Horizon{{}, hz(1_794_816_000_000_000_000, 90), hz(1_800_000_000_000_000_000, 90), hz(1_800_000_000_000_000_000, 90)}},
		},
		Retired: [backup.NumClasses]backup.Retired{
			{},
			{ThroughSeq: 40, ThroughCutSeq: 50, Records: 12, HorizonNS: 1_794_816_000_000_000_000},
			{ThroughSeq: 48, ThroughCutSeq: 50, Records: 30, HorizonNS: 1_800_000_000_000_000_000},
			{},
			{},
		},
		NotRetirable: [backup.NumClasses]string{"", "", "", "backup/v1/files/l3/00000000000000000051-00000000000000000060.parquet holds Observes with no TTL (1)", ""},
		Files: []backup.File{{
			Class: "l2", Name: "backup/v1/files/l2/00000000000000000051-00000000000000000120.parquet",
			FirstSeq: 51, LastSeq: 120, AfterSeq: 48, Records: 61, Bytes: 4096,
			SHA256:     strings.Repeat("0f", 32),
			MaxReachNS: 1_800_003_600_000_000_000, MinShadowableObserveEventNS: 1_799_999_000_000_000_000,
			WallFirstNS: 1_800_000_060_000_000_000, WallLastNS: 1_800_003_600_000_000_000, WallClamps: 1,
			MaxObserveTTL: 15 * time.Minute, TTL0Observes: 2,
		}},
		Cuts: []backup.Cut{{FirstSeq: 51, LastSeq: 120, Records: [backup.NumClasses]int64{3, 2, 61, 4, 0}}},
	}
}

// The manifest format, version 1, is frozen by these bytes: a change to any field,
// its name, order or meaning is version 2.
const goldenManifestSHA256 = "6356ed8f6614464f61feeedef3723c684ecf9ae0d1561c31ccad862958cdf28d"

func TestManifestV1IsFrozen(t *testing.T) {
	t.Parallel()
	want, err := os.ReadFile("testdata/manifest_v1.json")
	if err != nil {
		t.Fatal(err)
	}
	if sum := sha256.Sum256(want); hex.EncodeToString(sum[:]) != goldenManifestSHA256 {
		t.Fatalf("testdata/manifest_v1.json has SHA-256 %x, pinned %s", sum, goldenManifestSHA256)
	}
	got, err := backup.Encode(goldenManifest())
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, want) {
		t.Fatalf("the manifest encodes as\n%s\nwant\n%s", got, want)
	}
	m, err := backup.Decode(want)
	if err != nil {
		t.Fatal(err)
	}
	again, err := backup.Encode(m)
	if err != nil || !bytes.Equal(again, want) {
		t.Fatalf("decoding and encoding the golden manifest changes it: %v", err)
	}
	// Every field is in the golden file, with a value other than its zero, so a
	// field added without a new golden file is noticed.
	var top map[string]json.RawMessage
	if err := json.Unmarshal(want, &top); err != nil {
		t.Fatal(err)
	}
	for _, k := range []string{
		"format", "version", "generation", "seq", "prev", "store", "margin_ns", "max_clock_ahead_ns",
		"retentions", "retired", "not_retirable", "files", "cuts",
	} {
		if _, ok := top[k]; !ok {
			t.Errorf("the golden manifest has no %q", k)
		}
	}
	if len(top) != 13 {
		t.Errorf("the golden manifest has %d fields, want 13", len(top))
	}
	for _, k := range []string{
		"after_seq", "max_reach_ns", "min_shadowable_observe_event_ns", "wall_first_ns", "wall_last_ns",
		"wall_clamps", "max_observe_ttl_ns", "ttl0_observes", "through_cut_seq", "cluster_name_set", "offsets_ns", "keep",
	} {
		if !bytes.Contains(want, []byte(`"`+k+`"`)) {
			t.Errorf("the golden manifest has no %q", k)
		}
	}
}

func TestDecodeIsStrict(t *testing.T) {
	t.Parallel()
	good, err := backup.Encode(goldenManifest())
	if err != nil {
		t.Fatal(err)
	}
	for name, b := range map[string][]byte{
		"an unknown field": bytes.Replace(good, []byte(`"seq": 120,`), []byte(`"seq": 120, "extra": 1,`), 1),
		"a trailing value": append(append([]byte{}, good...), []byte("{}")...),
		"version 2":        bytes.Replace(good, []byte(`"version": 1,`), []byte(`"version": 2,`), 1),
		"another format":   bytes.Replace(good, []byte(backup.Format), []byte("other"), 1),
		"not JSON":         []byte("nope"),
	} {
		if _, err := backup.Decode(b); !errors.Is(err, backup.ErrFormat) {
			t.Errorf("%s: Decode = %v, want ErrFormat", name, err)
		}
	}
}

// Names sort by Seq and generation, and are valid segment names at the extremes:
// Seq 1 and 2^64-1 (store.Latest, used for names only).
func TestNamesAtTheExtremes(t *testing.T) {
	t.Parallel()
	for _, seq := range []uint64{1, store.Latest} {
		for cl := range backup.Class(backup.NumClasses) {
			n := backup.FileName(backup.DefaultPrefix, cl, seq, seq)
			if err := segment.ValidName(n); err != nil {
				t.Errorf("%s: %v", n, err)
			}
		}
		if n := backup.ManifestName(backup.DefaultPrefix, seq); segment.ValidName(n) != nil {
			t.Errorf("%s is not a valid name", n)
		}
	}
	if got := backup.FileName("p", backup.ClassHosts, 1, store.Latest); got != "p/v1/files/hosts/00000000000000000001-18446744073709551615.parquet" {
		t.Errorf("FileName = %s", got)
	}
	if a, b := backup.ManifestName("p", 9), backup.ManifestName("p", 10); a >= b {
		t.Errorf("%s sorts after %s", a, b)
	}
}
