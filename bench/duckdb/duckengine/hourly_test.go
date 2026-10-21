package duckengine

import (
	"context"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/lotannauo/toposhift/bench/duckdb/duckmodel"
	"github.com/lotannauo/toposhift/bench/spike/engine"
)

func rec(seq uint64, at time.Time) engine.Record {
	return engine.Record{Seq: seq, EventTime: at}
}

// TestSegmentsAreTheHoursOfIngest: a batch belongs to the hour the stream has reached when
// it arrives, so a late event is in the hour it arrives in and the segments hold
// consecutive ranges of Seq; an hour that no batch arrives in has no segment.
func TestSegmentsAreTheHoursOfIngest(t *testing.T) {
	base := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	at := func(h, m int) time.Time { return base.Add(time.Duration(h)*time.Hour + time.Duration(m)*time.Minute) }
	batches := [][]engine.Record{
		{rec(1, at(0, 10)), rec(2, at(0, 20))},
		{rec(3, at(0, 50))},
		{rec(4, at(1, 5)), rec(5, at(0, 30))}, // the second is late: it arrives in hour 1
		{rec(6, at(3, 10))},                   // hour 2 has no batch
	}
	want := []Segment{
		{Index: 0, StartNanos: base.UnixNano(), FirstSeq: 1, LastSeq: 3, Records: 3},
		{Index: 1, StartNanos: at(1, 0).UnixNano(), FirstSeq: 4, LastSeq: 5, Records: 2},
		{Index: 2, StartNanos: at(3, 0).UnixNano(), FirstSeq: 6, LastSeq: 6, Records: 1},
	}
	run := func() []Segment {
		k := &keepSink{passSink: passSink{filter: duckmodel.NewFilter(time.Time{})}, segment: time.Hour}
		for _, b := range batches {
			if err := k.Write(b); err != nil {
				t.Fatal(err)
			}
		}
		return k.segs
	}
	if got := run(); !slices.Equal(got, want) {
		t.Fatalf("the segments are\n%+v\nnot\n%+v", got, want)
	}
	defer func() { segmentByEventTime = false }()
	segmentByEventTime = true
	if got := run(); slices.Equal(got, want) {
		t.Error("segments by the hour an event is about are the same as by the hour it arrived in: the test cannot tell them apart")
	}
}

// TestHourOfRoundsDown for times before 1970 and on the hour.
func TestHourOfRoundsDown(t *testing.T) {
	h := time.Hour
	for _, c := range []struct {
		at   time.Time
		want int64
	}{
		{time.Unix(0, 0), 0},
		{time.Unix(3599, 999), 0},
		{time.Unix(3600, 0), int64(h)},
		{time.Unix(-1, 0), -int64(h)},
		{time.Unix(-3600, 0), -int64(h)},
		{time.Unix(-3601, 0), -2 * int64(h)},
	} {
		if got := hourOf(c.at, h); got != c.want {
			t.Errorf("hourOf(%v) = %d, want %d", c.at.UnixNano(), got, c.want)
		}
	}
}

var hourlyOpts = BuildOptions{Segment: 3 * time.Minute, RowGroups: RowGroupRows{2048, 2048, 2048}}

// TestTheDiagnosticArmHoldsConsecutiveRangesOfSeq: each hour's pair of files holds the
// records of its range of Seq, sorted as A2's two files are, the ranges do not meet, and
// together the files hold every record of the activity file once, in each order.
func TestTheDiagnosticArmHoldsConsecutiveRangesOfSeq(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "build")
	man, err := buildFromFile(t, dir, []Arm{ArmH}, hourlyOpts)
	mustf(t, err, "build")
	if len(man.Segments) < 3 || man.SegmentNanos != int64(3*time.Minute) {
		t.Fatalf("%d segments of %d ns: too few to tell a range from the whole", len(man.Segments), man.SegmentNanos)
	}
	db := openRead(t, false)
	all := seqsOf(t, db, rolePathIn(dir, RoleActivity))
	var union []uint64
	for i, sg := range man.Segments {
		if i > 0 && sg.FirstSeq <= man.Segments[i-1].LastSeq {
			t.Errorf("segment %d starts at seq %d, the one before ends at %d", i, sg.FirstSeq, man.Segments[i-1].LastSeq)
		}
		var want []uint64
		for _, s := range all {
			if s >= sg.FirstSeq && s <= sg.LastSeq {
				want = append(want, s)
			}
		}
		if int64(len(want)) != sg.Records {
			t.Fatalf("segment %d says %d records, the range holds %d", i, sg.Records, len(want))
		}
		for _, role := range []Role{RoleHFwd, RoleHRev} {
			if got := seqsOf(t, db, filepath.Join(dir, filepath.FromSlash(SegmentPath(i, role)))); !slices.Equal(got, want) {
				t.Errorf("segment %d %s holds %d records, its range %d", i, role, len(got), len(want))
			}
		}
		union = append(union, want...)
	}
	if !slices.Equal(union, all) {
		t.Errorf("the segments hold %d records, the activity file %d", len(union), len(all))
	}
	// The order inside a file is A2's: forward by source, reverse by target.
	for _, c := range []struct {
		role  Role
		order string
	}{{RoleHFwd, ArchiveOrder}, {RoleHRev, ReverseOrder}} {
		for i := range man.Segments {
			p := filepath.Join(dir, filepath.FromSlash(SegmentPath(i, c.role)))
			var bad int64
			err := db.QueryRowContext(context.Background(), `SELECT count(*) FROM (SELECT *, row_number() OVER () AS pos FROM read_parquet(`+quote(p)+`)) a
				JOIN (SELECT row_number() OVER (ORDER BY `+c.order+`) AS pos, seq FROM read_parquet(`+quote(p)+`)) b USING (pos) WHERE a.seq <> b.seq`).Scan(&bad)
			mustf(t, err, "checking the order of %s", p)
			if bad != 0 {
				t.Errorf("%s: %d rows are not where %q puts them", p, bad, c.order)
			}
		}
	}
}

// TestTheDiagnosticArmCountsFilesAndRowGroups: a read opens every file of the glob, and
// reads the pages of those that have a row group admitted; the totals are the files'.
func TestTheDiagnosticArmCountsFilesAndRowGroups(t *testing.T) {
	_, qs, _ := sharedSet(t)
	dir := filepath.Join(t.TempDir(), "build")
	man, err := buildFromFile(t, dir, []Arm{ArmH}, hourlyOpts)
	mustf(t, err, "build")
	db := openRead(t, true)
	r, err := OpenReader(context.Background(), db, dir, ArmH, true)
	mustf(t, err, "open")
	defer r.Close()
	qc, err := CollectQueries(context.Background(), r, qs.Queries)
	mustf(t, err, "counters")
	files := len(man.Segments)
	groupsOf := func(role Role) (n int) {
		for _, f := range man.FilesOf(role) {
			n += f.RowGroups
		}
		return
	}
	var someSkipped, someRead bool
	for _, q := range qc {
		for _, s := range q.Scans {
			if s.Files != files || s.FilesRead > s.Files {
				t.Fatalf("%s: %d files opened and %d read of %d", q.Name, s.Files, s.FilesRead, files)
			}
			if s.Total.Groups != groupsOf(s.Role) {
				t.Errorf("%s: %d row groups in all, the files have %d", q.Name, s.Total.Groups, groupsOf(s.Role))
			}
			if s.FilesRead < s.Files {
				someSkipped = true
			}
			if s.FilesRead > 0 {
				someRead = true
			}
			if (s.Admitted.Groups == 0) != (s.FilesRead == 0) {
				t.Errorf("%s: %d row groups admitted in %d files", q.Name, s.Admitted.Groups, s.FilesRead)
			}
		}
	}
	if !someSkipped || !someRead {
		t.Errorf("skipped some: %v, read some: %v: the counting is not shown to tell files apart", someSkipped, someRead)
	}
	var opened int
	for _, g := range Summarize(qc) {
		opened += g.FilesOpened
		if g.FilesRead > g.FilesOpened {
			t.Errorf("%s %s: %d files read of %d opened", g.Group, g.Age, g.FilesRead, g.FilesOpened)
		}
	}
	if want := files * len(qc); opened != want && len(qc) > 0 {
		// Every scan opens every file; a read has one scan.
		scans := 0
		for _, q := range qc {
			scans += len(q.Scans)
		}
		if opened != files*scans {
			t.Errorf("%d files opened over %d scans of %d files", opened, scans, files)
		}
	}
}

// TestTheDiagnosticArmIsAFunctionOfTheStream: two builds are the same files.
func TestTheDiagnosticArmIsAFunctionOfTheStream(t *testing.T) {
	a, err := buildFromFile(t, filepath.Join(t.TempDir(), "a"), []Arm{ArmH}, hourlyOpts)
	mustf(t, err, "a")
	b, err := buildFromFile(t, filepath.Join(t.TempDir(), "b"), []Arm{ArmH}, hourlyOpts)
	mustf(t, err, "b")
	if !slices.Equal(a.Files, b.Files) || !slices.Equal(a.Segments, b.Segments) || a.Stream != b.Stream {
		t.Fatalf("two builds differ:\n%+v\n%+v", a, b)
	}
}

// TestTheDiagnosticArmIsReadOnlyAsItWasBuilt: a directory whose files are not the ones
// the manifest names is refused, and so is a path the glob would read as a pattern.
func TestTheDiagnosticArmIsReadOnlyAsItWasBuilt(t *testing.T) {
	ctx := context.Background()
	dir := filepath.Join(t.TempDir(), "build")
	man, err := buildFromFile(t, dir, []Arm{ArmH}, hourlyOpts)
	mustf(t, err, "build")
	db := openRead(t, false)
	if r, err := OpenReader(ctx, db, dir, ArmH, true); err != nil {
		t.Fatal(err)
	} else {
		r.Close()
	}

	// A file the glob names that the manifest does not.
	src := filepath.Join(dir, filepath.FromSlash(SegmentPath(0, RoleHFwd)))
	stray := filepath.Join(dir, "h", "stray")
	mustf(t, os.MkdirAll(stray, 0o755), "stray dir")
	b, err := os.ReadFile(src)
	mustf(t, err, "read")
	mustf(t, os.WriteFile(filepath.Join(stray, "fwd.parquet"), b, 0o644), "stray")
	if _, err := OpenReader(ctx, db, dir, ArmH, false); err == nil || !strings.Contains(err.Error(), "glob") {
		t.Errorf("a stray file the glob names: %v", err)
	}
	mustf(t, os.RemoveAll(stray), "remove")

	// A file the manifest names that is gone.
	last := filepath.Join(dir, filepath.FromSlash(SegmentPath(len(man.Segments)-1, RoleHRev)))
	saved, err := os.ReadFile(last)
	mustf(t, err, "read last")
	mustf(t, os.Remove(last), "remove last")
	if _, err := OpenReader(ctx, db, dir, ArmH, false); err == nil {
		t.Error("a missing segment file")
	}
	mustf(t, os.WriteFile(last, saved, 0o644), "restore")

	// A file that is not the one built.
	mustf(t, os.WriteFile(last, append(saved[:len(saved):len(saved)], 0), 0o644), "alter")
	if _, err := OpenReader(ctx, db, dir, ArmH, true); err == nil {
		t.Error("a segment file that was changed")
	}
	mustf(t, os.WriteFile(last, saved, 0o644), "restore")

	// A directory whose name is a pattern.
	odd := filepath.Join(t.TempDir(), "b[0-9]")
	mustf(t, os.Rename(dir, odd), "rename")
	if _, err := OpenReader(ctx, db, odd, ArmH, false); err == nil || !strings.Contains(err.Error(), "pattern") {
		t.Errorf("a directory with a pattern in its name: %v", err)
	}
}

// TestAManifestOfSegmentsIsChecked: a manifest whose segments do not follow one another,
// or whose files are not those of its segments, is refused when it is read.
func TestAManifestOfSegmentsIsChecked(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "build")
	man, err := buildFromFile(t, dir, []Arm{ArmH}, hourlyOpts)
	mustf(t, err, "build")
	if _, err := ReadManifest(dir); err != nil {
		t.Fatal(err)
	}
	edit := func(f func(*Manifest)) error {
		m := *man
		m.Segments = slices.Clone(man.Segments)
		m.Files = slices.Clone(man.Files)
		f(&m)
		d := t.TempDir()
		mustf(t, writeManifest(d, &m), "write")
		_, err := ReadManifest(d)
		return err
	}
	for name, f := range map[string]func(*Manifest){
		"a segment that starts where the one before ends": func(m *Manifest) { m.Segments[1].FirstSeq = m.Segments[0].LastSeq },
		"a segment with another index":                    func(m *Manifest) { m.Segments[1].Index = 5 },
		"a segment with no records":                       func(m *Manifest) { m.Segments[1].Records = 0 },
		"a segment with other rows than its files":        func(m *Manifest) { m.Segments[1].Records++ },
		"no segments":          func(m *Manifest) { m.Segments = nil },
		"no length of an hour": func(m *Manifest) { m.SegmentNanos = 0 },
		"a file of a segment that does not exist": func(m *Manifest) {
			m.Files = append(m.Files, FileInfo{Role: RoleHFwd, Path: SegmentPath(len(m.Segments), RoleHFwd)})
		},
		"a segment file at another path": func(m *Manifest) {
			for i := range m.Files {
				if m.Files[i].Role == RoleHRev {
					m.Files[i].Path = "h/000000/fwd.parquet"
					break
				}
			}
		},
		"a segment file of another role": func(m *Manifest) {
			for i := range m.Files {
				if m.Files[i].Role == RoleHRev {
					m.Files[i].Role = RoleHFwd
					break
				}
			}
		},
	} {
		if err := edit(f); err == nil {
			t.Errorf("%s was accepted", name)
		}
	}
}

// TestSegmentsAreRefusedLengthsBelowNothing: an hour of ingest below zero is refused.
func TestSegmentsAreRefusedLengthsBelowNothing(t *testing.T) {
	if _, err := buildFromFile(t, filepath.Join(t.TempDir(), "x"), []Arm{ArmH}, BuildOptions{Segment: -time.Hour}); err == nil {
		t.Error("an hour of ingest of -1h was accepted")
	}
}

// TestTheCountsOfTheDiagnosticArmDoNotDependOnWhatWasReadBefore: the first read of a
// view through a glob is handed an estimate of the rows to scan, until the footers are
// read, where later reads are handed the rows. The reader reads them when it opens, so a
// read costs the same, to the figure, on a connection that has read nothing else and on one
// that has read a great deal.
func TestTheCountsOfTheDiagnosticArmDoNotDependOnWhatWasReadBefore(t *testing.T) {
	_, qs, _ := sharedSet(t)
	dir := filepath.Join(t.TempDir(), "build")
	_, err := buildFromFile(t, dir, []Arm{ArmH}, hourlyOpts)
	mustf(t, err, "build")
	pass := func(queries int) []QueryCounters {
		db := openRead(t, true)
		r, err := OpenReader(context.Background(), db, dir, ArmH, true)
		mustf(t, err, "open")
		defer r.Close()
		// Ask a lot first on one connection, nothing on the other.
		if queries > 0 {
			_, err := CollectQueriesPass(context.Background(), r, qs.Queries[:queries])
			mustf(t, err, "warming")
		}
		out, err := CollectQueriesPass(context.Background(), r, qs.Queries)
		mustf(t, err, "counting")
		return out
	}
	cold, used := pass(0), pass(len(qs.Queries))
	if len(cold) != len(used) {
		t.Fatalf("%d and %d counters", len(cold), len(used))
	}
	for i := range cold {
		if !equalCounters(cold[i], used[i]) {
			t.Fatalf("%s costs differently on a connection that read nothing before it:\n%+v\n%+v", cold[i].Name, cold[i], used[i])
		}
	}
}
