package backup

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/lotannauo/toposhift/internal/catalog"
	"github.com/lotannauo/toposhift/internal/replay/activity"
	"github.com/lotannauo/toposhift/internal/segment"
	"github.com/lotannauo/toposhift/internal/store"
)

// DefaultBatch is the number of records a restore writes per batch when
// RestoreOptions.Batch is zero.
const DefaultBatch = 10_000

// ErrIncomplete is wrapped by the error of a restore that refused a backup
// before writing anything: a file is missing, damaged, or not the one the
// manifest names, or the manifest contradicts itself.
var ErrIncomplete = errors.New("backup: the backup is not whole")

// RestoreOptions configures a restore.
type RestoreOptions struct {
	// Batch is the number of records per Write; zero means DefaultBatch.
	Batch int
	// Store is the meta of the store restored into. It must equal the manifest's.
	Store StoreMeta
	// TempDir is where the files are copied before they are read. Empty means the
	// system's temporary directory.
	TempDir string

	// classify replaces ClassOf, for the tests that show what the hosts class is
	// for.
	classify func(store.Record) Class
}

// Result describes a restore.
type Result struct {
	// Files and Bytes are what was copied.
	Files int
	Bytes int64
	// Records and Batches are what was written, and FirstSeq and LastSeq the first
	// and last Seq written.
	Records, Batches  int64
	FirstSeq, LastSeq uint64
	// Copy is the time taken to copy and check the files, and Write the time taken
	// to read and write their records.
	Copy, Write time.Duration
}

func incomplete(format string, args ...any) error {
	return fmt.Errorf("%w: %s", ErrIncomplete, fmt.Sprintf(format, args...))
}

// checkManifest checks what a manifest says of itself, without reading a file:
// its format, version and generation; that each class's files form a chain from
// what the class retired; and that the cuts account for every file and every
// record a file should hold.
func checkManifest(m Manifest) error {
	if m.Format != Format || m.Version != Version {
		return fmt.Errorf("%w: format %q version %d, want %q version %d", ErrFormat, m.Format, m.Version, Format, Version)
	}
	if m.Generation == 0 {
		return fmt.Errorf("%w: generation 0", ErrFormat)
	}
	// The chain of each class, from what it retired.
	after := [NumClasses]uint64{}
	for cl := range after {
		after[cl] = m.Retired[cl].ThroughSeq
	}
	seen := [NumClasses]bool{}
	prevClass := -1
	for _, f := range m.Files {
		cl, ok := parseClass(f.Class)
		if !ok {
			return incomplete("a file of the unknown class %q", f.Class)
		}
		if int(cl) < prevClass {
			return incomplete("the files are not ordered by class")
		}
		prevClass = int(cl)
		seen[cl] = true
		suffix := "/v1/files/" + cl.String() + "/" + pad(f.FirstSeq) + "-" + pad(f.LastSeq) + ".parquet"
		switch {
		case !strings.HasSuffix(f.Name, suffix) || segment.ValidName(f.Name) != nil:
			return incomplete("the file %q is not named for class %s, seq %d to %d", f.Name, cl, f.FirstSeq, f.LastSeq)
		case f.FirstSeq == 0 || f.FirstSeq > f.LastSeq || f.LastSeq > m.Seq:
			return incomplete("%s: seq %d to %d, with the backup through %d", f.Name, f.FirstSeq, f.LastSeq, m.Seq)
		case f.Records < 1 || uint64(f.Records) > f.LastSeq-f.FirstSeq+1:
			return incomplete("%s: %d records between seq %d and %d", f.Name, f.Records, f.FirstSeq, f.LastSeq)
		case len(f.SHA256) != 64 || !isHex(f.SHA256):
			return incomplete("%s: the digest %q is not 64 hex digits", f.Name, f.SHA256)
		case f.Bytes < 1 || f.Bytes > segment.MaxSize:
			return incomplete("%s: %d bytes", f.Name, f.Bytes)
		}
		if f.AfterSeq != after[cl] || f.FirstSeq <= after[cl] {
			return incomplete("the chain of class %s breaks at %s: it follows seq %d, the file before it ends at %d",
				cl, f.Name, f.AfterSeq, after[cl])
		}
		after[cl] = f.LastSeq
	}
	// The cuts.
	var prevLast uint64
	for i, cut := range m.Cuts {
		if cut.FirstSeq == 0 || cut.FirstSeq > cut.LastSeq || cut.LastSeq > m.Seq || (i > 0 && cut.FirstSeq <= prevLast) {
			return incomplete("cut %d (seq %d to %d) is out of order or outside the backup", i, cut.FirstSeq, cut.LastSeq)
		}
		prevLast = cut.LastSeq
		var sum uint64
		for _, n := range cut.Records {
			if n < 0 {
				return incomplete("cut %d counts %d records of a class", i, n)
			}
			sum += uint64(n)
		}
		if sum != cut.LastSeq-cut.FirstSeq+1 {
			return incomplete("cut %d (seq %d to %d) counts %d records", i, cut.FirstSeq, cut.LastSeq, sum)
		}
	}
	var held [NumClasses][]int64 // per class, per cut, the records of its files
	for cl := range held {
		held[cl] = make([]int64, len(m.Cuts))
	}
	for _, f := range m.Files {
		cl, _ := parseClass(f.Class)
		i := cutOf(m, f)
		if i < 0 {
			return incomplete("%s (seq %d to %d) lies inside no cut", f.Name, f.FirstSeq, f.LastSeq)
		}
		held[cl][i] += f.Records
	}
	for i, cut := range m.Cuts {
		for cl := range Class(NumClasses) {
			if cut.LastSeq <= m.Retired[cl].ThroughCutSeq {
				if held[cl][i] != 0 {
					return incomplete("cut %d (seq %d to %d) is retired for class %s and still lists its files", i, cut.FirstSeq, cut.LastSeq, cl)
				}
				continue
			}
			if held[cl][i] != cut.Records[cl] {
				return incomplete("cut %d (seq %d to %d) counts %d records of class %s, and its files hold %d",
					i, cut.FirstSeq, cut.LastSeq, cut.Records[cl], cl, held[cl][i])
			}
		}
	}
	return nil
}

func isHex(s string) bool {
	_, err := hex.DecodeString(s)
	return err == nil && strings.ToLower(s) == s
}

// storeEmpty reports whether a store holds nothing: no record and no horizon.
func storeEmpty(dst store.Store) bool {
	if dst.LastSeq() != 0 {
		return false
	}
	for l := catalog.L0; l <= catalog.L3; l++ {
		if !dst.LayerHorizon(l).IsZero() {
			return false
		}
	}
	return true
}

// Replay writes a backup's records into an empty store, merged by Seq. Before it
// writes anything it checks, in this order: the manifest's format, version and
// generation; that o.Store equals the manifest's; that dst is empty; that each
// class's files form an unbroken chain from what it retired; and the cut counts,
// which are what notices a missing last file of a class. It then copies every
// file to o.TempDir, hashing it on the way, and checks the digest, and opens it
// and checks its footer against the manifest. Only then does it merge the
// classes into one stream ascending in Seq, checking that every record is of its
// file's class and that every edge keeps one layer, and write it in batches of
// o.Batch. It sets no horizon: see SetHorizons.
//
// An error after the first write says so: the store holds part of the backup,
// and must be discarded.
func Replay(ctx context.Context, src segment.Store, m Manifest, dst store.Store, o RestoreOptions) (Result, error) {
	if err := checkManifest(m); err != nil {
		return Result{}, err
	}
	if o.Store != m.Store {
		return Result{}, fmt.Errorf("backup: the store's meta %+v differs from the backup's %+v", o.Store, m.Store)
	}
	if !storeEmpty(dst) {
		return Result{}, fmt.Errorf("backup: the store is not empty: it holds records through seq %d or a horizon", dst.LastSeq())
	}
	if o.Batch == 0 {
		o.Batch = DefaultBatch
	}
	if o.Batch < 0 {
		return Result{}, fmt.Errorf("backup: a batch of %d records", o.Batch)
	}
	if o.TempDir == "" {
		o.TempDir = os.TempDir()
	}
	dir, err := os.MkdirTemp(o.TempDir, "toposhift-restore-")
	if err != nil {
		return Result{}, err
	}
	defer func() { _ = os.RemoveAll(dir) }()

	var res Result
	start := time.Now()
	paths := make([]string, len(m.Files))
	for i, f := range m.Files {
		if paths[i], err = fetch(ctx, src, f, filepath.Join(dir, fmt.Sprintf("%06d.parquet", i))); err != nil {
			return Result{}, err
		}
		res.Files++
		res.Bytes += f.Bytes
	}
	res.Copy = time.Since(start)

	start = time.Now()
	err = merge(ctx, m, paths, dst, o, &res)
	res.Write = time.Since(start)
	if err != nil {
		if dst.LastSeq() != 0 {
			return res, fmt.Errorf("backup: the restore stopped after writing records through seq %d; the store holds part of the backup and must be discarded: %w", dst.LastSeq(), err)
		}
		return res, fmt.Errorf("backup: the restore stopped before any record was stored: %w", err)
	}
	return res, nil
}

// fetch copies one file to path, hashing it, and checks its size, its digest and
// its footer against the manifest.
func fetch(ctx context.Context, src segment.Store, f File, path string) (string, error) {
	rc, err := src.Open(ctx, f.Name)
	if err != nil {
		return "", incomplete("opening %s: %v", f.Name, err)
	}
	defer func() { _ = rc.Close() }()
	out, err := os.Create(path)
	if err != nil {
		return "", err
	}
	defer func() { _ = out.Close() }()
	sum := sha256.New()
	n, err := io.Copy(io.MultiWriter(out, sum), io.LimitReader(rc, segment.MaxSize+1))
	if err != nil {
		return "", fmt.Errorf("backup: copying %s: %w", f.Name, err)
	}
	if got := hex.EncodeToString(sum.Sum(nil)); n != f.Bytes || got != f.SHA256 {
		return "", incomplete("%s is %d bytes with SHA-256 %s; the manifest says %d bytes with %s", f.Name, n, got, f.Bytes, f.SHA256)
	}
	rd, err := activity.NewReader(out, n, activity.ReaderOptions{})
	if err != nil {
		return "", incomplete("%s: %v", f.Name, err)
	}
	info := rd.Info()
	_ = rd.Close()
	if info.Version != activity.FormatVersion || info.Records != f.Records || info.MinSeq != f.FirstSeq || info.MaxSeq != f.LastSeq {
		return "", incomplete("%s holds %d records, seq %d to %d, at version %d; the manifest says %d, seq %d to %d",
			f.Name, info.Records, info.MinSeq, info.MaxSeq, info.Version, f.Records, f.FirstSeq, f.LastSeq)
	}
	return path, nil
}

// classStream reads one class's files in order.
type classStream struct {
	class Class
	paths []string
	files []File
	i     int
	f     *os.File
	rd    *activity.Reader
	head  store.Record
	ok    bool
}

func (s *classStream) close() {
	if s.rd != nil {
		_ = s.rd.Close()
		s.rd = nil
	}
	if s.f != nil {
		_ = s.f.Close()
		s.f = nil
	}
}

// advance reads the next record of the class into head, or sets ok false at its
// end.
func (s *classStream) advance() error {
	for {
		if s.rd == nil {
			if s.i == len(s.paths) {
				s.ok = false
				return nil
			}
			f, err := os.Open(s.paths[s.i])
			if err != nil {
				return err
			}
			st, err := f.Stat()
			if err != nil {
				_ = f.Close()
				return err
			}
			rd, err := activity.NewReader(f, st.Size(), activity.ReaderOptions{})
			if err != nil {
				_ = f.Close()
				return fmt.Errorf("%s: %w", s.files[s.i].Name, err)
			}
			s.f, s.rd = f, rd
		}
		rec, err := s.rd.Next()
		if errors.Is(err, io.EOF) {
			s.close()
			s.i++
			continue
		}
		if err != nil {
			return fmt.Errorf("%s: %w", s.files[s.i].Name, err)
		}
		s.head, s.ok = rec, true
		return nil
	}
}

// merge writes the classes' records into dst in ascending Seq.
func merge(ctx context.Context, m Manifest, paths []string, dst store.Store, o RestoreOptions, res *Result) error {
	classify := ClassOf
	if o.classify != nil {
		classify = o.classify
	}
	var streams [NumClasses]*classStream
	for cl := range streams {
		streams[cl] = &classStream{class: Class(cl)}
	}
	for i, f := range m.Files {
		cl, _ := parseClass(f.Class)
		streams[cl].paths = append(streams[cl].paths, paths[i])
		streams[cl].files = append(streams[cl].files, f)
	}
	defer func() {
		for _, s := range streams {
			s.close()
		}
	}()
	for _, s := range streams {
		if err := s.advance(); err != nil {
			return err
		}
	}
	edges := map[[16]byte]catalog.Layer{}
	buf := make([]store.Record, 0, min(o.Batch, 1<<16))
	var lastSeq uint64
	flush := func() error {
		if len(buf) == 0 {
			return nil
		}
		if err := dst.Write(ctx, buf); err != nil {
			return fmt.Errorf("writing seq %d to %d: %w", buf[0].Seq, buf[len(buf)-1].Seq, err)
		}
		if res.Records == 0 {
			res.FirstSeq = buf[0].Seq
		}
		res.Records += int64(len(buf))
		res.Batches++
		res.LastSeq = buf[len(buf)-1].Seq
		buf = buf[:0]
		return nil
	}
	for n := 0; ; n++ {
		if n%4096 == 0 {
			if err := ctx.Err(); err != nil {
				return err
			}
		}
		var next *classStream
		for _, s := range streams {
			if s.ok && (next == nil || s.head.Seq < next.head.Seq) {
				next = s
			}
		}
		if next == nil {
			break
		}
		rec := next.head
		if got := classify(rec); got != next.class {
			return incomplete("seq %d, of class %s, is in a file of class %s", rec.Seq, got, next.class)
		}
		if rec.Seq <= lastSeq {
			return incomplete("seq %d follows seq %d", rec.Seq, lastSeq)
		}
		lastSeq = rec.Seq
		if rec.Subject.Kind == store.SubjectEdge {
			k := edgeKey(rec.Subject)
			if was, ok := edges[k]; !ok {
				edges[k] = rec.Layer
			} else if was != rec.Layer {
				return incomplete("the edge %s %s %s is in layer %s at seq %d and was in layer %s before",
					rec.Subject.A, rec.Subject.Relation, rec.Subject.B, rec.Layer, rec.Seq, was)
			}
		}
		buf = append(buf, rec)
		if len(buf) == o.Batch {
			if err := flush(); err != nil {
				return err
			}
		}
		if err := next.advance(); err != nil {
			return err
		}
	}
	return flush()
}

// edgeKey is 16 bytes of the SHA-256 of an edge's subject: its two ends and its
// relation.
func edgeKey(s store.Subject) [16]byte {
	h := sha256.New()
	for _, fp := range []string{s.A.String(), s.B.String(), string(s.Relation)} {
		var n [4]byte
		binary.BigEndian.PutUint32(n[:], uint32(len(fp)))
		h.Write(n[:])
		h.Write([]byte(fp))
	}
	var k [16]byte
	copy(k[:], h.Sum(nil))
	return k
}

// SetHorizons sets a restored store's horizons to the live store's: with no
// logged retention it does nothing; otherwise it retains dst once, at the largest
// instant a retention asked for, and requires every layer's horizon to be the one
// the last logged retention reported. A difference, which mismatched offsets or
// kept layers cause, is an error that names the layer. Call it after Replay and
// before anything reads the store.
func SetHorizons(ctx context.Context, m Manifest, dst store.Store) error {
	last, ok := m.lastRetention()
	if !ok {
		return nil
	}
	asked := m.Retentions[0].AskedNS
	for _, r := range m.Retentions {
		asked = max(asked, r.AskedNS)
	}
	if err := dst.Retain(ctx, time.Unix(0, asked).UTC()); err != nil {
		return fmt.Errorf("backup: retaining the restored store: %w", err)
	}
	for l := catalog.L0; l <= catalog.L3; l++ {
		got, err := horizonOf(dst.LayerHorizon(l))
		want := last.Horizons[l-catalog.L0]
		switch {
		case err != nil:
			return fmt.Errorf("backup: layer %s: %w", l, err)
		case got.IsZero() != want.IsZero() || got.TimeNS != want.TimeNS:
			return fmt.Errorf("backup: the restored store's horizon of layer %s is %s; the live store's was %s",
				l, describeHorizon(got), describeHorizon(want))
		}
	}
	return nil
}

func describeHorizon(h Horizon) string {
	if h.IsZero() {
		return "none"
	}
	return h.Time().Format(time.RFC3339Nano)
}

// Restore is Replay and then SetHorizons.
func Restore(ctx context.Context, src segment.Store, m Manifest, dst store.Store, o RestoreOptions) (Result, error) {
	res, err := Replay(ctx, src, m, dst, o)
	if err != nil {
		return res, err
	}
	return res, SetHorizons(ctx, m, dst)
}
