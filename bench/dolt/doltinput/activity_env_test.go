package doltinput_test

import (
	"os"
	"slices"
	"strconv"
	"testing"
	"time"

	"github.com/lotannauo/toposhift/bench/dolt/doltinput"
	"github.com/lotannauo/toposhift/bench/dolt/doltmodel"
	"github.com/lotannauo/toposhift/bench/spike/engine"
	"github.com/lotannauo/toposhift/bench/spike/oracle"
	"github.com/lotannauo/toposhift/internal/catalog"
	"github.com/lotannauo/toposhift/internal/identity"
)

// TestActivityFileOnDisk loads a real activity file, or a prefix of one, into the model
// of the design and the reference engine, and compares the reads of a sample of entities. It runs only when
// TOPOSHIFT_DOLT_ACTIVITY names the file; TOPOSHIFT_DOLT_ACTIVITY_SKIP skips that many records first
// and TOPOSHIFT_DOLT_ACTIVITY_LIMIT, if set, stops after about that many more. Both engines hold the records in memory.
func TestActivityFileOnDisk(t *testing.T) {
	path := os.Getenv("TOPOSHIFT_DOLT_ACTIVITY")
	if path == "" {
		t.Skip("TOPOSHIFT_DOLT_ACTIVITY is not set")
	}
	var limit int64
	if s := os.Getenv("TOPOSHIFT_DOLT_ACTIVITY_LIMIT"); s != "" {
		var err error
		if limit, err = strconv.ParseInt(s, 10, 64); err != nil {
			t.Fatal(err)
		}
	}
	m, ora := doltmodel.New(), oracle.New()
	var all []engine.Record
	var sample []identity.Fingerprint
	seen := map[identity.Fingerprint]bool{}
	var lo, hi time.Time
	both := tee{m, ora, func(rs []engine.Record) {
		all = append(all, rs...)
		for _, r := range rs {
			if r.Subject.Kind == engine.SubjectEntity && len(sample) < 200 && !seen[r.Subject.A] {
				seen[r.Subject.A] = true
				sample = append(sample, r.Subject.A)
			}
			if lo.IsZero() || r.EventTime.Before(lo) {
				lo = r.EventTime
			}
			if r.EventTime.After(hi) {
				hi = r.EventTime
			}
		}
	}}
	// The records after the first skip, up to limit of them: a file's edges follow its
	// entities, so a prefix alone may hold none.
	var skip, n int64
	if s := os.Getenv("TOPOSHIFT_DOLT_ACTIVITY_SKIP"); s != "" {
		var err error
		if skip, err = strconv.ParseInt(s, 10, 64); err != nil {
			t.Fatal(err)
		}
	}
	var seen64 int64
	info, err := doltinput.ForEach(path, 1000, doltinput.Options{DropBasis: true}, func(rs []engine.Record) error {
		if seen64 += int64(len(rs)); seen64 <= skip {
			return nil
		}
		if err := both.Write(rs); err != nil {
			return err
		}
		if n += int64(len(rs)); limit > 0 && n >= limit {
			return doltinput.ErrStop
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("%d records after the first %d of the %d of the file (content digest %s)", n, skip, info.Records, info.Digest)

	t.Logf("%d records held by the model, %d of the file's read", len(all), n)

	instants := []time.Time{lo.Add(-time.Second), lo, lo.Add(hi.Sub(lo) / 3), lo.Add(hi.Sub(lo) / 2), hi.Add(-time.Nanosecond), hi, hi.Add(time.Hour)}
	for _, layer := range []catalog.Layer{catalog.L1, catalog.L2} {
		sc := engine.Current(layer)
		for _, fp := range sample {
			for _, at := range instants {
				a, err := m.Alive(fp, at, sc)
				if err != nil {
					t.Fatal(err)
				}
				if b, _ := ora.Alive(fp, at, sc); a != b {
					t.Fatalf("Alive(%s, %s, %s) = %v, the reference engine %v", fp, at, layer, a, b)
				}
				for _, dir := range []engine.Direction{engine.Forward, engine.Reverse} {
					na, err := m.Neighbors(fp, dir, at, sc)
					if err != nil {
						t.Fatal(err)
					}
					if nb, _ := ora.Neighbors(fp, dir, at, sc); !slices.Equal(na, nb) {
						t.Fatalf("Neighbors(%s, %s, %s, %s) = %v, the reference engine %v", fp, dir, at, layer, na, nb)
					}
				}
			}
		}
	}
}

// tee writes to two engines and shows the batch to a function; only Write is used.
type tee struct {
	a, b engine.Engine
	see  func([]engine.Record)
}

func (t tee) Write(rs []engine.Record) error {
	t.see(rs)
	if err := t.a.Write(rs); err != nil {
		return err
	}
	return t.b.Write(rs)
}

func (t tee) LastSeq() uint64 { return t.a.LastSeq() }
func (t tee) Neighbors(identity.Fingerprint, engine.Direction, time.Time, engine.Scope) ([]engine.Neighbor, error) {
	panic("unused")
}

func (t tee) NeighborsBatch([]identity.Fingerprint, engine.Direction, time.Time, engine.Scope) ([][]engine.Neighbor, error) {
	panic("unused")
}

func (t tee) Alive(identity.Fingerprint, time.Time, engine.Scope) (bool, error) { panic("unused") }
func (t tee) Window(identity.Fingerprint, engine.Direction, time.Time, time.Time, engine.Scope) ([]engine.Record, error) {
	panic("unused")
}
func (t tee) Retain(h time.Time) error { return nil }
func (t tee) Size() (int64, error)     { return 0, nil }
func (t tee) Close() error             { return nil }
