package backup

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"hash"
	"io"
	"math"
	"os"
	"slices"
	"time"

	"github.com/lotannauo/toposhift/internal/lifecycle"
	"github.com/lotannauo/toposhift/internal/replay/activity"
	"github.com/lotannauo/toposhift/internal/segment"
	"github.com/lotannauo/toposhift/internal/store"
)

// Defaults of Options.
const (
	DefaultMargin        = 24 * time.Hour
	DefaultMaxClockAhead = 5 * time.Minute
	DefaultMaxFileBytes  = 64 << 20
)

// ErrGap is returned by Cutter.Add for a batch whose first Seq is not the one
// after the last Seq added, or whose Seqs are not dense. Nothing of the batch is
// kept.
var ErrGap = errors.New("backup: a gap in the committed sequence")

// ErrClosed is returned by every method of a closed Cutter.
var ErrClosed = errors.New("backup: cutter closed")

// Options configures a Cutter.
type Options struct {
	// Margin is rule R's margin, M: a file is retired only once its records and its
	// commits are at least M behind its layer's horizon. Zero means DefaultMargin.
	// It must exceed MaxClockAhead.
	Margin time.Duration
	// MaxClockAhead is how far an event time may run ahead of the ingest clock.
	// Zero means DefaultMaxClockAhead. A class holding an Observe whose TTL is
	// above Margin less MaxClockAhead is not retired.
	MaxClockAhead time.Duration
	// MaxFileBytes closes a class's file once the records written to it reach this
	// size, by the activity writer's own measure of a record (64 bytes plus its
	// strings and payload, an upper bound of its compressed size in practice), so a
	// class may have several files in one cut. Zero means DefaultMaxFileBytes; it
	// must be below segment.MaxSize.
	MaxFileBytes int64
	// Store is the live store's meta, which a restore must match.
	Store StoreMeta
	// TempDir is where files are written until they are cut. Empty means the
	// system's temporary directory.
	TempDir string
}

func (o Options) withDefaults() (Options, error) {
	if o.Margin == 0 {
		o.Margin = DefaultMargin
	}
	if o.MaxClockAhead == 0 {
		o.MaxClockAhead = DefaultMaxClockAhead
	}
	if o.MaxFileBytes == 0 {
		o.MaxFileBytes = DefaultMaxFileBytes
	}
	switch {
	case o.Margin < 0 || o.MaxClockAhead < 0:
		return o, errors.New("backup: the margin and the clock bound must not be negative")
	case o.Margin <= o.MaxClockAhead:
		return o, fmt.Errorf("backup: the margin %s must exceed the clock bound %s", o.Margin, o.MaxClockAhead)
	case o.MaxFileBytes < 0 || o.MaxFileBytes >= segment.MaxSize:
		return o, fmt.Errorf("backup: MaxFileBytes %d must be above 0 and below %d", o.MaxFileBytes, segment.MaxSize)
	}
	if o.TempDir == "" {
		o.TempDir = os.TempDir()
	}
	return o, nil
}

// hooks change the cutter for the tests that show what a rule or a class is for.
// The zero value is the product's behavior.
type hooks struct {
	// startSeq is the first Seq an empty backup takes; zero means 1.
	startSeq uint64
	// classify replaces ClassOf.
	classify func(store.Record) Class
	// retireHosts retires the hosts class as rule R retires l1.
	retireHosts bool
	// noMinEvent drops rule R's condition (b), per class.
	noMinEvent [NumClasses]bool
	// noCutBeforeRetire has Retire decide on what is cut only.
	noCutBeforeRetire bool
	// zeroMargin and noRefusal, per class, set the margin to zero and drop the
	// refusal of rule R.
	zeroMargin, noRefusal [NumClasses]bool
}

// Cutter writes committed batches into per-class activity files, cuts them into a
// backup under a prefix of a segment store, and retires what rule R allows. It is
// not safe for concurrent use: it has one caller, the writer of the store.
type Cutter struct {
	dst    segment.Store
	prefix string
	o      Options
	h      hooks

	m      Manifest // the last manifest written; generation 0 before the first
	mBytes []byte
	// nextGen is the generation the next manifest is written as. Every attempt takes
	// one, so a write whose outcome is unknown never meets a different manifest
	// under its own name.
	nextGen uint64
	// prevGen is the generation of the manifest written before m, which Retire
	// keeps with m when it deletes the older ones.
	prevGen uint64

	last    uint64 // the last Seq added
	wall    int64  // the last ingest clock, after clamping
	hasWall bool
	batches int64 // batches added, for counting clamps per file
	// The cut being gathered: whether it holds a record, its first Seq and its
	// counts. cutThrough is the last Seq of the last cut taken.
	cutOpen    bool
	cutStart   uint64
	cutCounts  [NumClasses]int64
	cutThrough uint64

	open        [NumClasses]*openFile
	pending     []*closedFile // closed files not yet in a written manifest
	pendingCuts []Cut
	retentions  []Retention
	dirty       bool     // retentions logged since m
	garbage     []string // retired files whose deletion has not succeeded yet

	failed error
}

// openFile is a class's file being written.
type openFile struct {
	class Class
	path  string
	f     *os.File
	w     *activity.Writer
	sum   hash.Hash
	n     int64 // bytes written to f
	est   int64 // the writer's measure of the records written
	meta  File
	// clampBatch is the last batch counted in meta.WallClamps.
	clampBatch int64
}

// closedFile is a file complete on local disk, waiting for a manifest.
type closedFile struct {
	path string
	meta File
	sum  [32]byte
	put  bool // stored under its name
}

// NewCutter starts a cutter on prefix. If the prefix holds a backup, it goes on
// from its latest manifest, and refuses options whose Store, Margin or
// MaxClockAhead differ from it; the first batch added must then follow the
// manifest's Seq. On an empty prefix the first batch must start at Seq 1: a store
// that holds records from before the backup began needs an export of them first.
func NewCutter(ctx context.Context, dst segment.Store, prefix string, o Options) (*Cutter, error) {
	if err := checkPrefix(prefix); err != nil {
		return nil, err
	}
	o, err := o.withDefaults()
	if err != nil {
		return nil, err
	}
	c := &Cutter{dst: dst, prefix: prefix, o: o, nextGen: 1}
	m, b, err := latest(ctx, dst, prefix)
	switch {
	case errors.Is(err, ErrNoBackup):
		return c, nil
	case err != nil:
		return nil, err
	}
	switch {
	case m.Store != o.Store:
		return nil, fmt.Errorf("backup: the store's meta %+v differs from the backup's %+v", o.Store, m.Store)
	case m.Margin != o.Margin || m.MaxClockAhead != o.MaxClockAhead:
		return nil, fmt.Errorf("backup: the margin %s and clock bound %s differ from the backup's %s and %s",
			o.Margin, o.MaxClockAhead, m.Margin, m.MaxClockAhead)
	}
	if err := checkManifest(m); err != nil {
		return nil, err
	}
	c.m, c.mBytes = m, b
	c.nextGen = m.Generation + 1
	c.last, c.cutThrough = m.Seq, m.Seq
	c.retentions = slices.Clone(m.Retentions)
	for _, f := range m.Files {
		if f.WallLastNS > c.wall || !c.hasWall {
			c.wall, c.hasWall = f.WallLastNS, true
		}
	}
	return c, nil
}

func (c *Cutter) classOf(r store.Record) Class {
	if c.h.classify != nil {
		return c.h.classify(r)
	}
	return ClassOf(r)
}

// firstSeq is the Seq the next batch must start at.
func (c *Cutter) firstSeq() uint64 {
	if c.last != 0 || c.m.Generation != 0 {
		return c.last + 1
	}
	if c.h.startSeq != 0 {
		return c.h.startSeq
	}
	return 1
}

// Add takes one committed batch, with the ingest clock of its commit. Its first
// Seq must follow the last one added (on an empty backup, it must be 1) and its
// Seqs must be dense; otherwise it returns an error wrapping ErrGap and keeps
// nothing of it. A clock earlier than the previous batch's is raised to it and
// counted in the files the batch writes to: a clock that stepped back is not a
// reason to stop backing up, and a raised clock only delays retirement.
//
// Any other error leaves part of the batch in the files, and the cutter is broken:
// every later call returns that error, and Close discards what was not cut.
func (c *Cutter) Add(batch []store.Record, wall time.Time) error {
	if c.failed != nil {
		return c.failed
	}
	if len(batch) == 0 {
		return nil
	}
	want := c.firstSeq()
	for i, r := range batch {
		if r.Seq != want+uint64(i) || r.Seq == store.Latest || r.Seq == 0 {
			return fmt.Errorf("%w: record %d of the batch has seq %d, want %d", ErrGap, i, r.Seq, want+uint64(i))
		}
	}
	ns := wall.UnixNano()
	clamped := c.hasWall && ns < c.wall
	if clamped {
		ns = c.wall
	}
	c.wall, c.hasWall = ns, true
	c.batches++
	if !c.cutOpen {
		c.cutOpen, c.cutStart = true, batch[0].Seq
	}
	for _, r := range batch {
		if err := c.write(r, ns, clamped); err != nil {
			c.failed = fmt.Errorf("backup: writing seq %d: %w", r.Seq, err)
			return c.failed
		}
	}
	c.last = batch[len(batch)-1].Seq
	return nil
}

// recordSize is the activity writer's measure of a record.
func recordSize(r store.Record) int64 {
	return int64(64 + len(r.Producer) + len(r.Subject.Relation) + len(r.Boot) + len(r.Payload) +
		len(r.Subject.A.String()) + len(r.Subject.B.String()))
}

func (c *Cutter) write(r store.Record, wall int64, clamped bool) error {
	cl := c.classOf(r)
	if int(cl) >= NumClasses {
		return fmt.Errorf("the record is of no class")
	}
	of := c.open[cl]
	if of != nil && of.est >= c.o.MaxFileBytes {
		if err := c.closeFile(cl); err != nil {
			return err
		}
		of = nil
	}
	if of == nil {
		var err error
		if of, err = c.openFile(cl); err != nil {
			return err
		}
	}
	if err := of.w.Write(r); err != nil {
		return err
	}
	of.est += recordSize(r)
	m := &of.meta
	if m.Records == 0 {
		m.FirstSeq, m.WallFirstNS = r.Seq, wall
		m.MaxReachNS, m.MinShadowableObserveEventNS = math.MinInt64, math.MaxInt64
	}
	m.LastSeq, m.WallLastNS = r.Seq, wall
	m.Records++
	last, _ := store.LastOf(r.EventTime, r.Through)
	m.MaxReachNS = max(m.MaxReachNS, last.UnixNano())
	if r.Kind == lifecycle.Observe {
		if shadowable(r, last, c.o.Margin) {
			m.MinShadowableObserveEventNS = min(m.MinShadowableObserveEventNS, r.EventTime.UnixNano())
		}
		m.MaxObserveTTL = max(m.MaxObserveTTL, r.TTL)
		if r.TTL == 0 {
			m.TTL0Observes++
		}
	}
	if clamped && of.clampBatch != c.batches {
		m.WallClamps++
		of.clampBatch = c.batches
	}
	c.cutCounts[cl]++
	return nil
}

// shadowable reports whether an Observe, whose max(EventTime, Through) is last,
// could be shadowed by a retired record of its reference: its span, last less its
// EventTime, is above the margin less its TTL, or it has no TTL. Any other kept
// Observe whose EventTime is before a retired record's reach has its deadline
// before the horizon (see rule R in retire.go), so it never decides an answer
// there.
func shadowable(r store.Record, last time.Time, margin time.Duration) bool {
	return r.TTL <= 0 || last.Sub(r.EventTime) > margin-r.TTL
}

func (c *Cutter) openFile(cl Class) (*openFile, error) {
	f, err := os.CreateTemp(c.o.TempDir, "toposhift-backup-*.parquet")
	if err != nil {
		return nil, err
	}
	of := &openFile{class: cl, path: f.Name(), f: f, sum: sha256.New(), meta: File{Class: cl.String()}}
	w, err := activity.NewWriter(io.MultiWriter(f, of.sum, countWriter{&of.n}), activity.WriterOptions{
		RowGroupBytes: min(activity.DefaultRowGroupBytes, c.o.MaxFileBytes),
	})
	if err != nil {
		_ = f.Close()
		_ = os.Remove(f.Name())
		return nil, err
	}
	of.w = w
	c.open[cl] = of
	return of, nil
}

type countWriter struct{ n *int64 }

func (w countWriter) Write(p []byte) (int, error) {
	*w.n += int64(len(p))
	return len(p), nil
}

// closeFile finishes a class's open file and queues it for the next cut.
func (c *Cutter) closeFile(cl Class) error {
	of := c.open[cl]
	c.open[cl] = nil
	err := of.w.Close()
	if cerr := of.f.Close(); err == nil {
		err = cerr
	}
	if err != nil {
		_ = os.Remove(of.path)
		return err
	}
	cf := &closedFile{path: of.path, meta: of.meta}
	copy(cf.sum[:], of.sum.Sum(nil))
	cf.meta.Bytes = of.n
	cf.meta.SHA256 = hex.EncodeToString(cf.sum[:])
	cf.meta.Name = FileName(c.prefix, cl, cf.meta.FirstSeq, cf.meta.LastSeq)
	c.pending = append(c.pending, cf)
	return nil
}

// Retained logs a retention of the live store: the instant it was asked for, each
// layer's horizon as the store reported it afterwards, and the store's LastSeq,
// which must be read after Retain returns, since a record committed before the
// retention may have an event time before its horizon. Retire waits until every
// record through that Seq has been added. An ask earlier than the last one logged
// is refused, and so is a horizon that moved backward: the store never moves one
// back. The log goes into the next manifest.
func (c *Cutter) Retained(asked time.Time, h [4]store.Horizon, last uint64) error {
	if c.failed != nil {
		return c.failed
	}
	if asked.Before(time.Unix(0, math.MinInt64)) || asked.After(store.MaxEventTime) {
		return fmt.Errorf("backup: the retention asked for %s cannot be logged in nanoseconds", asked)
	}
	r := Retention{AskedNS: asked.UnixNano(), Seq: last}
	for i := range h {
		var err error
		if r.Horizons[i], err = horizonOf(h[i]); err != nil {
			return err
		}
	}
	if n := len(c.retentions); n > 0 {
		prev := c.retentions[n-1]
		if r.AskedNS < prev.AskedNS {
			return fmt.Errorf("backup: a retention asked for %s after one asked for %s: asks never decrease",
				asked.UTC(), time.Unix(0, prev.AskedNS).UTC())
		}
		if r.Seq < prev.Seq {
			return fmt.Errorf("backup: a retention at seq %d after one at seq %d", r.Seq, prev.Seq)
		}
		for i := range r.Horizons {
			if !prev.Horizons[i].IsZero() && (r.Horizons[i].TimeNS < prev.Horizons[i].TimeNS || r.Horizons[i].IsZero()) {
				return fmt.Errorf("backup: the horizon of layer %d moved backward", i)
			}
		}
	}
	c.retentions = append(c.retentions, r)
	c.dirty = true
	return nil
}

// Cut closes every open file, stores each file not yet stored, and then stores
// the next generation's manifest, which lists them. The files go first and the
// manifest last, so a manifest never names a file that is not there. With nothing
// added or logged since the last manifest it writes nothing and returns that
// manifest. A failed Cut can be called again: what it closed is kept, and is
// stored by the next Cut.
func (c *Cutter) Cut(ctx context.Context) (Manifest, error) {
	if c.failed != nil {
		return Manifest{}, c.failed
	}
	if c.cutOpen {
		cut := Cut{FirstSeq: c.cutStart, LastSeq: c.last, Records: c.cutCounts}
		for cl := range c.open {
			if c.open[cl] == nil {
				continue
			}
			if err := c.closeFile(Class(cl)); err != nil {
				c.failed = fmt.Errorf("backup: closing a file: %w", err)
				return Manifest{}, c.failed
			}
		}
		c.pendingCuts = append(c.pendingCuts, cut)
		c.cutOpen, c.cutCounts, c.cutThrough = false, [NumClasses]int64{}, c.last
	}
	if len(c.pendingCuts) == 0 && !c.dirty && c.m.Generation != 0 {
		return c.m, nil
	}
	for _, cf := range c.pending {
		if err := c.putFile(ctx, cf); err != nil {
			return Manifest{}, err
		}
	}
	next := c.next(c.cutThrough)
	for _, cf := range c.pending {
		next.Files = append(next.Files, cf.meta)
	}
	next.Cuts = append(next.Cuts, c.pendingCuts...)
	sortFiles(next.Files)
	chain(&next)
	if err := c.writeManifest(ctx, next); err != nil {
		return Manifest{}, err
	}
	for _, cf := range c.pending {
		_ = os.Remove(cf.path)
	}
	c.pending, c.pendingCuts = nil, nil
	return c.m, nil
}

// next is the manifest that follows m, covering through seq, with the cutter's
// retention log, before the caller adds or removes files.
func (c *Cutter) next(seq uint64) Manifest {
	n := c.m
	n.Format, n.Version = Format, Version
	n.Seq = seq
	n.Store, n.Margin, n.MaxClockAhead = c.o.Store, c.o.Margin, c.o.MaxClockAhead
	n.Retentions = slices.Clone(c.retentions)
	n.Files = slices.Clone(c.m.Files)
	n.Cuts = slices.Clone(c.m.Cuts)
	n.Prev = ""
	if c.mBytes != nil {
		sum := sha256.Sum256(c.mBytes)
		n.Prev = hex.EncodeToString(sum[:])
	}
	return n
}

// sortFiles orders files by class, then first Seq.
func sortFiles(fs []File) {
	slices.SortStableFunc(fs, func(a, b File) int {
		ca, _ := parseClass(a.Class)
		cb, _ := parseClass(b.Class)
		if ca != cb {
			return int(ca) - int(cb)
		}
		switch {
		case a.FirstSeq < b.FirstSeq:
			return -1
		case a.FirstSeq > b.FirstSeq:
			return 1
		}
		return 0
	})
}

// chain sets each file's AfterSeq: the class's file before it, or the class's
// retired ThroughSeq for its first file.
func chain(m *Manifest) {
	var after [NumClasses]uint64
	for c := range after {
		after[c] = m.Retired[c].ThroughSeq
	}
	for i := range m.Files {
		cl, _ := parseClass(m.Files[i].Class)
		m.Files[i].AfterSeq = after[cl]
		after[cl] = m.Files[i].LastSeq
	}
}

// putFile stores a closed file under its name, once. A name that holds a segment
// already is the same file only if its digest is the same: an earlier Put whose
// outcome was unknown. Any other segment under the name is one a failed cut left
// behind, which no manifest names, and is replaced.
func (c *Cutter) putFile(ctx context.Context, cf *closedFile) error {
	if cf.put {
		return nil
	}
	put := func() error {
		f, err := os.Open(cf.path)
		if err != nil {
			return err
		}
		defer func() { _ = f.Close() }()
		info, err := c.dst.Put(ctx, cf.meta.Name, f, cf.meta.Bytes)
		if err != nil {
			return err
		}
		if info.SHA256 != cf.sum {
			return fmt.Errorf("the store holds %s with SHA-256 %x, the file's is %x", cf.meta.Name, info.SHA256, cf.sum)
		}
		return nil
	}
	err := put()
	if errors.Is(err, segment.ErrExists) {
		info, serr := c.dst.Stat(ctx, cf.meta.Name)
		switch {
		case serr != nil:
			err = serr
		case info.SHA256 == cf.sum:
			err = nil
		case !c.names(cf.meta.Name):
			if err = c.dst.Delete(ctx, cf.meta.Name); err == nil {
				err = put()
			}
		}
	}
	if err != nil {
		return fmt.Errorf("backup: storing %s: %w", cf.meta.Name, err)
	}
	cf.put = true
	return nil
}

// names reports whether the last manifest names a file.
func (c *Cutter) names(name string) bool {
	for _, f := range c.m.Files {
		if f.Name == name {
			return true
		}
	}
	return false
}

// writeManifest stores next as the next generation, and makes it the cutter's
// manifest if that succeeded.
func (c *Cutter) writeManifest(ctx context.Context, next Manifest) error {
	next.Generation = c.nextGen
	c.nextGen++
	b, err := Encode(next)
	if err != nil {
		return err
	}
	name := ManifestName(c.prefix, next.Generation)
	info, err := c.dst.Put(ctx, name, bytes.NewReader(b), int64(len(b)))
	if err == nil && info.SHA256 != sha256.Sum256(b) {
		err = fmt.Errorf("the store holds it with SHA-256 %x", info.SHA256)
	}
	if err != nil {
		return fmt.Errorf("backup: storing the manifest %s: %w", name, err)
	}
	c.prevGen = c.m.Generation
	c.m, c.mBytes = next, b
	c.dirty = false
	return nil
}

// Manifest returns the last manifest written, generation 0 if there is none.
func (c *Cutter) Manifest() Manifest { return c.m }

// Close discards the files not yet cut and ends the cutter. A second Close does
// nothing.
func (c *Cutter) Close() error {
	if errors.Is(c.failed, ErrClosed) {
		return nil
	}
	var errs []error
	for cl, of := range c.open {
		if of == nil {
			continue
		}
		errs = append(errs, of.f.Close(), os.Remove(of.path))
		c.open[cl] = nil
	}
	for _, cf := range c.pending {
		if err := os.Remove(cf.path); err != nil && !errors.Is(err, os.ErrNotExist) {
			errs = append(errs, err)
		}
	}
	c.pending = nil
	c.failed = ErrClosed
	return errors.Join(errs...)
}
