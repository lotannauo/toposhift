package duckengine

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/lotannauo/toposhift/bench/duckdb/filestream"
	"github.com/lotannauo/toposhift/bench/duckdb/g0"
	"github.com/lotannauo/toposhift/internal/replay/activity"
	"github.com/lotannauo/toposhift/internal/store"
)

var fileFixture struct {
	once sync.Once
	act  string
	qs   *filestream.QuestionSet
	sum  string
	err  error
}

// sharedSet is a question set made from the activity file of the shared build: a file of
// records that have been through two retentions, with a period that holds them. It is
// made once and only read.
func sharedSet(t testing.TB) (act string, qs *filestream.QuestionSet, sum string) {
	t.Helper()
	dir, plan, _ := sharedBuild(t)
	fileFixture.once.Do(func() {
		fileFixture.act = rolePathIn(dir, RoleActivity)
		d, err := filestream.FileDigest(fileFixture.act)
		if err != nil {
			fileFixture.err = err
			return
		}
		w := plan.Spec.Workload
		opts := filestream.MakeOptions{Options: filestream.Options{
			Period: filestream.Period{Start: w.Start, Duration: w.Duration}, BatchSize: 64, Digest: d,
		}}
		fileFixture.qs, fileFixture.err = filestream.Make(context.Background(), fileFixture.act, opts, nil)
		if fileFixture.err != nil {
			return
		}
		p := filepath.Join(os.TempDir(), "duckengine-questions-"+d[len("sha256:"):len("sha256:")+16]+".json")
		if fileFixture.sum, fileFixture.err = fileFixture.qs.Save(p); fileFixture.err == nil {
			os.Remove(p)
		}
	})
	if fileFixture.err != nil {
		t.Fatal(fileFixture.err)
	}
	return fileFixture.act, fileFixture.qs, fileFixture.sum
}

func buildFromFile(t testing.TB, dir string, arms []Arm, opts BuildOptions) (*Manifest, error) {
	t.Helper()
	act, qs, sum := sharedSet(t)
	db, err := Open(context.Background(), Config{Mode: ModeBuild, TempDir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	return Build(context.Background(), db, dir, FileSource(act, qs, sum), arms, opts)
}

// TestBuildsFromAFileAnswerItsQuestions: every arm built from an activity file, the
// diagnostic arm among them, answers every question of the set made from the file as the
// reference engine did, and a build keeps the file's own bytes as its activity file.
func TestBuildsFromAFileAnswerItsQuestions(t *testing.T) {
	act, qs, sum := sharedSet(t)
	dir := filepath.Join(t.TempDir(), "build")
	man, err := buildFromFile(t, dir, AllArms, BuildOptions{Segment: 3 * time.Minute, RowGroups: RowGroupRows{2048, 2048, 2048}})
	mustf(t, err, "build")
	if man.Stream.Label != sum || man.Stream.Digest != qs.Stream.Digest || man.Stream.Records != qs.Stream.Records || man.Stream.Kept != qs.Stream.Records || man.Stream.LastSeq != qs.Stream.LastSeq {
		t.Errorf("the manifest's stream %+v is not the set's %+v", man.Stream, qs.Stream)
	}
	want, err := fileSHA256(act)
	mustf(t, err, "hashing the input")
	if f, _ := man.File(RoleActivity); f.SHA256 != want {
		t.Errorf("the activity file of the build is %s, the input's is %s", f.SHA256, want)
	}
	for _, arm := range AllArms {
		db := openRead(t, false)
		r, err := OpenReader(context.Background(), db, dir, arm, true)
		mustf(t, err, "opening %s", arm)
		rep, err := g0.CheckQueries(context.Background(), qs.Queries, r, nil)
		mustf(t, err, "g0 %s", arm)
		if !rep.OK() {
			t.Errorf("%s: %d of %d answers match; first mismatches: %+v", arm, rep.Matched, rep.Queries, rep.Mismatches)
		}
		r.Close()
	}
}

// TestAFileSourceIsRefusedWhenItIsNotTheSetsStream: files of another stream than the one
// the questions were made from are not built.
func TestAFileSourceIsRefusedWhenItIsNotTheSetsStream(t *testing.T) {
	act, qs, sum := sharedSet(t)
	other := *qs
	other.Stream.Records++
	db, err := Open(context.Background(), Config{Mode: ModeBuild, TempDir: t.TempDir()})
	mustf(t, err, "open")
	defer db.Close()
	if _, err := Build(context.Background(), db, filepath.Join(t.TempDir(), "x"), FileSource(act, &other, sum), []Arm{ArmA2}, BuildOptions{}); err == nil {
		t.Error("a stream of another size than the set's was built")
	}
	wrongDigest := *qs
	wrongDigest.Input.Digest = "sha256:" + strings.Repeat("0", 64)
	if _, err := Build(context.Background(), db, filepath.Join(t.TempDir(), "y"), FileSource(act, &wrongDigest, sum), []Arm{ArmA2}, BuildOptions{}); err == nil {
		t.Error("a file of another content digest was built")
	}
	withHorizon := FileSource(act, qs, sum)
	withHorizon.Horizon = time.Unix(1, 0)
	if _, err := Build(context.Background(), db, filepath.Join(t.TempDir(), "z"), withHorizon, []Arm{ArmA2}, BuildOptions{}); err == nil {
		t.Error("a file stream with a horizon was built")
	}
}

// TestAdoptReadsAnArchiveAsItIs: an archive made before, from the activity file, is
// adopted by its SHA-256, holds the file's rows, and answers every question of the set.
func TestAdoptReadsAnArchiveAsItIs(t *testing.T) {
	ctx := context.Background()
	act, qs, sum := sharedSet(t)
	built := filepath.Join(t.TempDir(), "built")
	man, err := buildFromFile(t, built, []Arm{ArmA1}, BuildOptions{})
	mustf(t, err, "build")
	archive := rolePathIn(built, RoleArchive)
	f, _ := man.File(RoleArchive)

	bdb, err := Open(ctx, Config{Mode: ModeBuild, TempDir: t.TempDir()})
	mustf(t, err, "open")
	defer bdb.Close()
	src := FileSource(act, qs, sum)
	dir := filepath.Join(t.TempDir(), "adopted")
	m, err := Adopt(ctx, bdb, dir, src, AdoptOptions{Archive: archive, SHA256: f.SHA256})
	mustf(t, err, "adopt")
	if !slices.Equal(m.Arms, []Arm{ArmA1}) || m.Adopted == nil || m.Adopted.ArchiveSHA256 != f.SHA256 || m.Adopted.Records != int64(qs.Stream.Records) {
		t.Errorf("the manifest: %+v", m)
	}
	if _, ok := m.File(RoleActivity); ok {
		t.Error("an adopted directory holds an activity file")
	}
	if got, err := fileSHA256(rolePathIn(dir, RoleArchive)); err != nil || got != f.SHA256 {
		t.Errorf("the archive in the directory has SHA-256 %s (%v), not %s", got, err, f.SHA256)
	}
	if m.Stream.Label != sum || m.Stream.Digest != qs.Stream.Digest || m.Stream.LastSeq != qs.Stream.LastSeq {
		t.Errorf("the stream: %+v", m.Stream)
	}

	db := openRead(t, true)
	r, err := OpenReader(ctx, db, dir, ArmA1, true)
	mustf(t, err, "reader")
	defer r.Close()
	rep, err := g0.CheckQueries(ctx, qs.Queries, r, nil)
	mustf(t, err, "g0")
	if !rep.OK() {
		t.Fatalf("%d of %d answers match: %+v", rep.Matched, rep.Queries, rep.Mismatches)
	}
	qc, err := CollectQueries(ctx, r, qs.Queries)
	mustf(t, err, "counters")
	if len(qc) != len(qs.Queries) {
		t.Errorf("%d counters for %d questions", len(qc), len(qs.Queries))
	}

	// Adopting again gives the same manifest: it records no time and no path.
	m2, err := Adopt(ctx, bdb, filepath.Join(t.TempDir(), "again"), src, AdoptOptions{Archive: archive, SHA256: f.SHA256})
	mustf(t, err, "adopt again")
	if m2.Stream != m.Stream || !slices.Equal(m2.Files, m.Files) || *m2.Adopted != *m.Adopted {
		t.Errorf("two adoptions differ:\n%+v\n%+v", m, m2)
	}
}

// TestAdoptRefusesAnArchiveThatIsNotTheStreams: an archive with another SHA-256 than the
// one it is named by, one that holds other rows than the activity file (also with as many
// rows), and one the questions were not made for, are not adopted, and leave nothing.
func TestAdoptRefusesAnArchiveThatIsNotTheStreams(t *testing.T) {
	ctx := context.Background()
	act, qs, sum := sharedSet(t)
	built := filepath.Join(t.TempDir(), "built")
	man, err := buildFromFile(t, built, []Arm{ArmA1}, BuildOptions{})
	mustf(t, err, "build")
	archive := rolePathIn(built, RoleArchive)
	good, _ := man.File(RoleArchive)

	bdb, err := Open(ctx, Config{Mode: ModeBuild, TempDir: t.TempDir()})
	mustf(t, err, "open")
	defer bdb.Close()

	// The same number of rows, one of them changed.
	scratch := t.TempDir()
	changed := filepath.Join(scratch, "changed.parquet")
	_, err = bdb.ExecContext(ctx, "COPY (SELECT * REPLACE (CASE WHEN seq = (SELECT min(seq) FROM read_parquet("+quote(archive)+")) THEN ttl_ns + 1 ELSE ttl_ns END AS ttl_ns) FROM read_parquet("+quote(archive)+") ORDER BY "+ArchiveOrder+") TO "+quote(changed)+" (FORMAT parquet, COMPRESSION zstd)")
	mustf(t, err, "making an archive with a changed row")
	changedSum, _ := fileSHA256(changed)
	// One row fewer.
	fewer := filepath.Join(scratch, "fewer.parquet")
	_, err = bdb.ExecContext(ctx, "COPY (SELECT * FROM read_parquet("+quote(archive)+") WHERE seq <> (SELECT min(seq) FROM read_parquet("+quote(archive)+")) ORDER BY "+ArchiveOrder+") TO "+quote(fewer)+" (FORMAT parquet, COMPRESSION zstd)")
	mustf(t, err, "making an archive with a row fewer")
	fewerSum, _ := fileSHA256(fewer)
	// Not a Parquet file.
	junk := filepath.Join(scratch, "junk.parquet")
	mustf(t, os.WriteFile(junk, []byte("not parquet"), 0o644), "junk")
	junkSum := sha256.Sum256([]byte("not parquet"))

	for name, c := range map[string]struct {
		file, sum string
		early     bool // refused before anything is read or made
	}{
		"another SHA-256 than it is named by": {archive, strings.Repeat("0", 64), true},
		"an upper case SHA-256":               {archive, strings.ToUpper(good.SHA256), true},
		"a row changed, as many rows":         {changed, changedSum, false},
		"a row fewer":                         {fewer, fewerSum, false},
		"no such file":                        {filepath.Join(scratch, "none"), good.SHA256, true},
		"a file that is not Parquet":          {junk, hex.EncodeToString(junkSum[:]), false},
	} {
		dir := filepath.Join(t.TempDir(), "adopted")
		if _, err := Adopt(ctx, bdb, dir, FileSource(act, qs, sum), AdoptOptions{Archive: c.file, SHA256: c.sum}); err == nil {
			t.Errorf("%s was adopted", name)
		}
		if _, err := os.Stat(filepath.Join(dir, ManifestName)); err == nil {
			t.Errorf("%s: a refused adoption wrote a manifest", name)
		}
		if _, err := os.Stat(dir); c.early && err == nil {
			t.Errorf("%s: a refusal that needs no read made a directory", name)
		}
	}
	// Questions of another stream.
	other := *qs
	other.Stream.LastSeq++
	if _, err := Adopt(ctx, bdb, filepath.Join(t.TempDir(), "o"), FileSource(act, &other, sum), AdoptOptions{Archive: archive, SHA256: good.SHA256}); err == nil {
		t.Error("an archive was adopted for questions of another stream")
	}
	if _, err := Adopt(ctx, bdb, filepath.Join(t.TempDir(), "n"), Source{Label: "x"}, AdoptOptions{Archive: archive, SHA256: good.SHA256}); err == nil {
		t.Error("an adoption with no stream")
	}
	obs, err := Open(ctx, Config{Mode: ModeObserve, TempDir: t.TempDir()})
	mustf(t, err, "open observe")
	defer obs.Close()
	if _, err := Adopt(ctx, obs, filepath.Join(t.TempDir(), "a"), FileSource(act, qs, sum), AdoptOptions{Archive: archive, SHA256: good.SHA256}); err == nil {
		t.Error("an adoption on an audit's connection")
	}
}

// withBasis writes the records of an activity file again with the event time basis a
// producer stamps (which the engine's records do not hold), and returns the new file.
func withBasis(t testing.TB, in string) string {
	t.Helper()
	f, err := os.Open(in)
	mustf(t, err, "open")
	defer f.Close()
	st, _ := f.Stat()
	rd, err := activity.NewReader(f, st.Size(), activity.ReaderOptions{})
	mustf(t, err, "reader")
	defer rd.Close()
	out := filepath.Join(t.TempDir(), "basis.parquet")
	o, err := os.Create(out)
	mustf(t, err, "create")
	defer o.Close()
	w, err := activity.NewWriter(o, activity.WriterOptions{})
	mustf(t, err, "writer")
	for {
		r, err := rd.Next()
		if errors.Is(err, io.EOF) {
			break
		}
		mustf(t, err, "next")
		r.EventTimeBasis = store.BasisProducerEvent
		mustf(t, w.Write(r), "write")
	}
	mustf(t, w.Close(), "close")
	mustf(t, o.Close(), "close file")
	return out
}

// TestAFileBuildKeepsTheColumnsOfTheInput: a file whose records say which clock stamped
// their times is not rewritten from the engine's records, which do not hold the basis:
// the arms' files have it as the input does, and the archive adopted from them has the
// rows of the input.
func TestAFileBuildKeepsTheColumnsOfTheInput(t *testing.T) {
	ctx := context.Background()
	shared, _, _ := sharedSet(t)
	act := withBasis(t, shared)
	digest, err := filestream.FileDigest(act)
	mustf(t, err, "digest")
	_, plan, _ := sharedBuild(t)
	w := plan.Spec.Workload
	qs, err := filestream.Make(ctx, act, filestream.MakeOptions{Options: filestream.Options{
		Period: filestream.Period{Start: w.Start, Duration: w.Duration}, BatchSize: 64, Digest: digest,
	}}, nil)
	mustf(t, err, "questions")
	src := FileSource(act, qs, "basis")
	dir := filepath.Join(t.TempDir(), "build")
	bdb, err := Open(ctx, Config{Mode: ModeBuild, TempDir: t.TempDir()})
	mustf(t, err, "open")
	defer bdb.Close()
	man, err := Build(ctx, bdb, dir, src, AllArms, BuildOptions{Segment: 3 * time.Minute})
	mustf(t, err, "build")

	want, err := fileSHA256(act)
	mustf(t, err, "hash")
	if f, _ := man.File(RoleActivity); f.SHA256 != want {
		t.Errorf("the activity file of the build is not the input's bytes")
	}
	db := openRead(t, false)
	for _, fi := range man.Files {
		var other int64
		err := db.QueryRowContext(ctx, "SELECT count(*) FROM read_parquet("+quote(filepath.Join(dir, filepath.FromSlash(fi.Path)))+") WHERE event_time_basis <> "+
			strconv.Itoa(int(store.BasisProducerEvent))).Scan(&other)
		if fi.Role == RoleIFwd || fi.Role == RoleIRev || fi.Role == RoleIEnt {
			continue // an interval file has no such column
		}
		mustf(t, err, "counting in %s", fi.Path)
		if other != 0 {
			t.Errorf("%s has %d rows without the input's event time basis", fi.Path, other)
		}
	}
	m, err := Adopt(ctx, bdb, filepath.Join(t.TempDir(), "adopted"), src, AdoptOptions{Archive: rolePathIn(dir, RoleArchive), SHA256: mustSHA(t, rolePathIn(dir, RoleArchive))})
	mustf(t, err, "adopt")
	if m.Adopted.Records != int64(qs.Stream.Records) {
		t.Errorf("%d rows adopted of %d", m.Adopted.Records, qs.Stream.Records)
	}
}

func mustSHA(t testing.TB, p string) string {
	t.Helper()
	s, err := fileSHA256(p)
	mustf(t, err, "hashing %s", p)
	return s
}
