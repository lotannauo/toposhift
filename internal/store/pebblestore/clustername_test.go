package pebblestore

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/cockroachdb/pebble/v2"
	"github.com/cockroachdb/pebble/v2/vfs"

	"github.com/lotannauo/toposhift/internal/lifecycle"
	"github.com/lotannauo/toposhift/internal/store"
	"github.com/lotannauo/toposhift/internal/store/pebblekv"
)

// The value at the key 'cluster_name' is a stored format. The vectors in testdata
// were computed independently of the code, and a change to any of them is a change
// of the format, which needs a new tag and a migration, never a regenerated file.
type clusterNameVectorFile struct {
	Key     string `json:"key"`
	Vectors []struct {
		Name    string `json:"name"`
		Set     bool   `json:"set"`
		Cluster string `json:"cluster"`
		Encoded string `json:"encoded"`
	} `json:"vectors"`
	Refused []struct {
		Name    string `json:"name"`
		Encoded string `json:"encoded"`
	} `json:"refused"`
}

func TestClusterNameGoldenVectors(t *testing.T) {
	t.Parallel()
	raw, err := os.ReadFile("testdata/meta_cluster_name_v1.json")
	if err != nil {
		t.Fatal(err)
	}
	var f clusterNameVectorFile
	if err := json.Unmarshal(raw, &f); err != nil {
		t.Fatal(err)
	}
	if got := hex.EncodeToString(metaKey(metaClusterName)); got != f.Key {
		t.Errorf("the key is %s, the vectors say %s", got, f.Key)
	}
	var sawNone, sawLimit, sawFormat bool
	for _, v := range f.Vectors {
		t.Run(v.Name, func(t *testing.T) {
			want, err := hex.DecodeString(v.Encoded)
			if err != nil {
				t.Fatal(err)
			}
			if got := store.AppendClusterName(nil, v.Cluster, v.Set); !bytes.Equal(got, want) {
				t.Fatalf("encoded %x, want %x", got, want)
			}
			name, set, err := store.DecodeClusterName(want)
			if err != nil || set != v.Set || name != v.Cluster {
				t.Fatalf("decoded %q, %v, %v; want %q, %v", name, set, err, v.Cluster, v.Set)
			}
			// Stored under the key, a store reads it back as it was recorded.
			s := openMem(t, Options{})
			if err := s.kv.Apply(applyMeta(t, s, want), pebble.Sync); err != nil {
				t.Fatal(err)
			}
			name, set, present, err := s.ClusterName()
			if err != nil || !present || set != v.Set || name != v.Cluster {
				t.Fatalf("ClusterName = %q, %v, %v, %v", name, set, present, err)
			}
			sawNone = sawNone || !v.Set
			sawLimit = sawLimit || (v.Set && len(v.Cluster) == store.MaxClusterNameLen)
			sawFormat = sawFormat || (v.Set && len(v.Cluster) == store.MaxClusterNameFormat)
		})
	}
	if !sawNone || !sawLimit || !sawFormat {
		t.Errorf("the vectors miss a case: none set %v, a name of the limit's length %v, one of the format's %v", sawNone, sawLimit, sawFormat)
	}
	for _, r := range f.Refused {
		b, err := hex.DecodeString(r.Encoded)
		if err != nil {
			t.Fatal(err)
		}
		if name, set, err := store.DecodeClusterName(b); !errors.Is(err, store.ErrInvalid) {
			t.Errorf("%s: decoded as %q, %v, %v; want a refusal", r.Name, name, set, err)
		}
		// A store that holds such a value says so, and does not guess.
		s := openMem(t, Options{})
		if err := s.kv.Apply(applyMeta(t, s, b), pebble.Sync); err != nil {
			t.Fatal(err)
		}
		if _, _, present, err := s.ClusterName(); !errors.Is(err, pebblekv.ErrValue) || present {
			t.Errorf("%s: ClusterName = present %v, %v; want ErrValue", r.Name, present, err)
		}
	}
	if len(f.Refused) < 10 {
		t.Errorf("only %d refusals are frozen", len(f.Refused))
	}
}

// applyMeta is a batch that sets the cluster name key to raw.
func applyMeta(t *testing.T, s *Store, raw []byte) *pebble.Batch {
	t.Helper()
	b := s.kv.NewBatch()
	t.Cleanup(func() { _ = b.Close() })
	if err := b.Set(metaKey(metaClusterName), raw, nil); err != nil {
		t.Fatal(err)
	}
	return b
}

func wantClusterNameOf(t *testing.T, s *Store, name string, set, present bool) {
	t.Helper()
	gotName, gotSet, gotPresent, err := s.ClusterName()
	if err != nil || gotName != name || gotSet != set || gotPresent != present {
		t.Fatalf("ClusterName = %q, set %v, present %v, %v; want %q, set %v, present %v", gotName, gotSet, gotPresent, err, name, set, present)
	}
}

func TestClusterNameAbsentOnANewStore(t *testing.T) {
	t.Parallel()
	wantClusterNameOf(t, openMem(t, Options{}), "", false, false)
}

func TestClusterNameSurvivesAReopen(t *testing.T) {
	t.Parallel()
	for name, tc := range map[string]struct {
		name string
		set  bool
	}{
		"a name":                      {"prod", true},
		"the longest name written":    {strings.Repeat("a", store.MaxClusterNameLen), true},
		"every kind of byte":          {"aZ09._:/-", true},
		"none set":                    {"", false},
		"none set, with a name given": {"ignored", false},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			fs := vfs.NewMem()
			s := mustOpen(t, chunkOptions(fs, nil))
			if err := s.RecordClusterName(bg, tc.name, tc.set); err != nil {
				t.Fatal(err)
			}
			want := tc.name
			if !tc.set {
				want = ""
			}
			wantClusterNameOf(t, s, want, tc.set, true)
			if err := s.Close(); err != nil {
				t.Fatal(err)
			}
			wantClusterNameOf(t, mustOpen(t, chunkOptions(fs, nil)), want, tc.set, true)
		})
	}
}

func TestClusterNameIsReplacedByTheNextRecording(t *testing.T) {
	t.Parallel()
	fs := vfs.NewMem()
	s := mustOpen(t, chunkOptions(fs, nil))
	for _, step := range []struct {
		name string
		set  bool
	}{{"prod", true}, {"", false}, {"staging", true}, {"prod", true}} {
		if err := s.RecordClusterName(bg, step.name, step.set); err != nil {
			t.Fatal(err)
		}
		wantClusterNameOf(t, s, step.name, step.set, true)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	wantClusterNameOf(t, mustOpen(t, chunkOptions(fs, nil)), "prod", true, true)
}

// A name that breaks a rule is refused with ErrInvalid and nothing is written, so
// what was recorded stands, or the store still records nothing.
func TestRecordClusterNameRefusals(t *testing.T) {
	t.Parallel()
	for name, in := range map[string]string{
		"empty":                  "",
		"one over the limit":     strings.Repeat("a", store.MaxClusterNameLen+1),
		"the format's longest":   strings.Repeat("a", store.MaxClusterNameFormat),
		"over the format":        strings.Repeat("a", store.MaxClusterNameFormat+1),
		"a byte outside the set": "pr d",
		"a NUL":                  "pr\x00d",
		"non-ASCII":              "café",
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			fresh := openMem(t, Options{})
			if err := fresh.RecordClusterName(bg, in, true); !errors.Is(err, store.ErrInvalid) {
				t.Fatalf("RecordClusterName(%q) = %v, want ErrInvalid", in, err)
			}
			wantClusterNameOf(t, fresh, "", false, false)
			if raw, err := fresh.kv.GetMeta(metaKey(metaClusterName)); err != nil || raw != nil {
				t.Errorf("the key holds %x, %v after a refusal", raw, err)
			}

			recorded := openMem(t, Options{})
			if err := recorded.RecordClusterName(bg, "prod", true); err != nil {
				t.Fatal(err)
			}
			if err := recorded.RecordClusterName(bg, in, true); !errors.Is(err, store.ErrInvalid) {
				t.Fatalf("RecordClusterName(%q) = %v, want ErrInvalid", in, err)
			}
			wantClusterNameOf(t, recorded, "prod", true, true)
		})
	}
}

// The limit is on what is written, not on what is read: a name over it that an
// earlier build recorded reads back, and the next recording is held to the limit.
func TestClusterNameOverTheLimitReadsBack(t *testing.T) {
	t.Parallel()
	long := strings.Repeat("l", store.MaxClusterNameLen+1)
	s := openMem(t, Options{})
	if err := s.kv.Apply(applyMeta(t, s, store.AppendClusterName(nil, long, true)), pebble.Sync); err != nil {
		t.Fatal(err)
	}
	wantClusterNameOf(t, s, long, true, true)
	if err := s.RecordClusterName(bg, long, true); !errors.Is(err, store.ErrInvalid) {
		t.Errorf("recording it again = %v, want ErrInvalid", err)
	}
}

func TestClusterNameAfterClose(t *testing.T) {
	t.Parallel()
	s := openMem(t, Options{})
	if err := s.RecordClusterName(bg, "prod", true); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	if err := s.RecordClusterName(bg, "prod", true); !errors.Is(err, store.ErrClosed) {
		t.Errorf("RecordClusterName after Close = %v, want ErrClosed", err)
	}
	if _, _, _, err := s.ClusterName(); !errors.Is(err, store.ErrClosed) {
		t.Errorf("ClusterName after Close = %v, want ErrClosed", err)
	}
}

func TestRecordClusterNameHonoursTheContext(t *testing.T) {
	t.Parallel()
	s := openMem(t, Options{})
	ctx, cancel := context.WithCancel(bg)
	cancel()
	if err := s.RecordClusterName(ctx, "prod", true); !errors.Is(err, context.Canceled) {
		t.Fatalf("RecordClusterName with a cancelled context = %v", err)
	}
	wantClusterNameOf(t, s, "", false, false)
}

// A store whose commit outcome was lost refuses to record, as it refuses a Write.
func TestRecordClusterNameRefusedByAFailedStore(t *testing.T) {
	t.Parallel()
	s := openMem(t, Options{})
	s.mu.Lock()
	s.failed = errors.New("a commit failed: reopen the store")
	s.mu.Unlock()
	if err := s.RecordClusterName(bg, "prod", true); err == nil || !strings.Contains(err.Error(), "reopen the store") {
		t.Fatalf("RecordClusterName on a failed store = %v", err)
	}
	wantClusterNameOf(t, s, "", false, false)
}

// The commit is synced, whatever the store's own setting for the commits of its
// records: a crash that keeps only what was synced keeps the name. A commit that is
// not synced is lost in the same crash, which is what the control shows.
func TestRecordClusterNameCommitIsSynced(t *testing.T) {
	t.Parallel()
	fs := vfs.NewCrashableMem()
	o := chunkOptions(fs, nil)
	if o.Sync {
		t.Fatal("the store under test must not sync its record commits by itself")
	}
	s := mustOpen(t, o)
	// The control: an unsynced commit to another meta key.
	b := s.kv.NewBatch()
	if err := b.Set(metaKey("control"), []byte{1}, nil); err != nil {
		t.Fatal(err)
	}
	if err := s.kv.Apply(b, pebble.NoSync); err != nil {
		t.Fatal(err)
	}
	_ = b.Close()
	lost := fs.CrashClone(vfs.CrashCloneCfg{})
	ctrl := mustOpen(t, chunkOptions(lost, nil))
	if v, err := ctrl.kv.GetMeta(metaKey("control")); err != nil || v != nil {
		t.Fatalf("the control survived a crash (%x, %v): this file system does not lose unsynced commits, so the test below proves nothing", v, err)
	}

	if err := s.RecordClusterName(bg, "prod", true); err != nil {
		t.Fatal(err)
	}
	crashed := fs.CrashClone(vfs.CrashCloneCfg{})
	wantClusterNameOf(t, mustOpen(t, chunkOptions(crashed, nil)), "prod", true, true)

	// None set is synced too.
	if err := s.RecordClusterName(bg, "", false); err != nil {
		t.Fatal(err)
	}
	crashed = fs.CrashClone(vfs.CrashCloneCfg{})
	wantClusterNameOf(t, mustOpen(t, chunkOptions(crashed, nil)), "", false, true)
}

// The key is not in the data keyspace: a store that records a name holds the same
// data keys and values as one that does not, so no digest taken over the data moves.
func TestClusterNameLeavesTheDataKeyspaceAlone(t *testing.T) {
	t.Parallel()
	history := func(record bool) [][2]string {
		s := openMem(t, Options{})
		if record {
			if err := s.RecordClusterName(bg, "prod", true); err != nil {
				t.Fatal(err)
			}
		}
		for i := range 5 {
			r := edgeRecord(uint64(i+1), "p", t0.Add(time.Duration(i)*time.Second), lifecycle.Observe, 0)
			if err := s.Write(bg, []store.Record{r}); err != nil {
				t.Fatal(err)
			}
			if record && i == 2 {
				if err := s.RecordClusterName(bg, "staging", true); err != nil {
					t.Fatal(err)
				}
			}
		}
		if got := s.LastSeq(); got != 5 {
			t.Fatalf("LastSeq = %d, want 5", got)
		}
		return dataBytes(t, s)
	}
	without, with := history(false), history(true)
	if len(without) == 0 {
		t.Fatal("no data was written")
	}
	if len(with) != len(without) {
		t.Fatalf("%d data keys with a recorded name, %d without", len(with), len(without))
	}
	for i := range with {
		if with[i] != without[i] {
			t.Fatalf("data key %d differs: %q with a recorded name, %q without", i, with[i], without[i])
		}
	}
}

// metaKeys is every meta key and value, in key order.
func metaKeys(t *testing.T, s *Store) map[string]string {
	t.Helper()
	// Meta keys start with metaLead, which no data key does.
	it, err := s.kv.NewIter(&pebble.IterOptions{LowerBound: []byte{metaLead}, UpperBound: []byte{metaLead + 1}})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = it.Close() }()
	out := map[string]string{}
	for ok := it.First(); ok; ok = it.Next() {
		out[string(it.Key()[1:])] = string(it.Value())
	}
	if err := it.Error(); err != nil {
		t.Fatal(err)
	}
	return out
}

// An older build ignores the key: it is additive. What opens, writes and retains
// does not read it, write it or change it, the key survives, and the format tag
// stays at meta version 1.
func TestClusterNameKeyIsAdditive(t *testing.T) {
	t.Parallel()
	for name, record := range map[string]bool{"a store that records a name": true, "a store that records none": false} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			fs := vfs.NewMem()
			s := mustOpen(t, chunkOptions(fs, nil))
			if record {
				if err := s.RecordClusterName(bg, "prod", true); err != nil {
					t.Fatal(err)
				}
			}
			wantTag := formatTag
			if !strings.HasSuffix(wantTag, ";meta=1") {
				t.Fatalf("the format tag is %q: the key is additive only under meta version 1", wantTag)
			}
			for round := range 3 {
				if err := s.Close(); err != nil {
					t.Fatal(err)
				}
				s = mustOpen(t, chunkOptions(fs, nil))
				for i := range 4 {
					seq := uint64(round*4 + i + 1)
					r := edgeRecord(seq, "p", t0.Add(time.Duration(seq)*time.Second), lifecycle.Observe, 0)
					if err := s.Write(bg, []store.Record{r}); err != nil {
						t.Fatal(err)
					}
				}
				if err := s.Retain(bg, t0.Add(time.Duration(round*4+2)*time.Second)); err != nil {
					t.Fatal(err)
				}
				if record {
					wantClusterNameOf(t, s, "prod", true, true)
				} else {
					wantClusterNameOf(t, s, "", false, false)
				}
				if tag, err := s.kv.GetMeta(metaKey(pebblekv.MetaFormat)); err != nil || string(tag) != wantTag {
					t.Fatalf("format = %q, %v; want %q", tag, err, wantTag)
				}
			}
			_, present := metaKeys(t, s)[metaClusterName]
			if present != record {
				t.Errorf("the key is present: %v, want %v", present, record)
			}
		})
	}
}
