package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"maps"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/lotannauo/toposhift/bench/spike/pebblekv"
	"github.com/lotannauo/toposhift/bench/spike/workload"
	"github.com/lotannauo/toposhift/internal/catalog"
	"github.com/lotannauo/toposhift/internal/coalesce"
	"github.com/lotannauo/toposhift/internal/identity"
	"github.com/lotannauo/toposhift/internal/ingest/k8scluster"
	"github.com/lotannauo/toposhift/internal/ingest/k8sobjects"
	"github.com/lotannauo/toposhift/internal/lifecycle"
	"github.com/lotannauo/toposhift/internal/replay/capture"
	"github.com/lotannauo/toposhift/internal/store"
	"github.com/lotannauo/toposhift/internal/store/pebblestore"
)

// What a longer producer name costs a store: the same capture of a Kubernetes
// cluster is translated, coalesced and written to a store with producer names of
// different lengths, and the tables the store ends in are weighed.

const (
	// defaultProducer is the producer name the objects translator is configured with
	// when nothing else is said. It is 18 bytes.
	defaultProducer = "k8sobjects/default"
	// producerPrefix begins the producer names of the objects translator of other
	// lengths.
	producerPrefix = "k8sobjects/"
	// defaultClusterProducer and clusterProducerPrefix are those of the cluster
	// translator. Their prefixes have the same length, so that for any length the two
	// producers have names of the same length.
	defaultClusterProducer = "k8scluster/default"
	clusterProducerPrefix  = "k8scluster/"

	// entityTTL is the TTL of every pod and node observation when only the objects
	// translator is measured: that of a poller.
	entityTTL = 15 * time.Minute
	// pullInterval is the interval at which the collector of the capture lists pods and
	// nodes, and the receiver's own collection interval: what the translators are
	// configured with when both are measured. The objects translator's TTL is then
	// three of them, and so is the cluster translator's for an event that reports 300
	// seconds or none.
	pullInterval = 5 * time.Minute
)

// The values of the flags when they are not given.
const (
	defaultFiles       = "run1.jsonl,run2.jsonl"
	defaultTranslators = "objects"
)

// mode says which translators the capture goes through.
type mode int

const (
	// objectsOnly is the objects translator alone, as the tool began: the whole capture
	// goes to it, a fixed TTL and the coalescer with a fixed bound on a run's age.
	objectsOnly mode = iota
	// clusterOnly is the cluster translator alone, on the entity events of the capture.
	clusterOnly
	// bothTranslators is the shipped default: the object records go to the objects
	// translator and the entity events to the cluster translator, and both write to
	// the same coalescer and store.
	bothTranslators
)

// parseTranslators reads the value of -translators.
func parseTranslators(s string) (mode, error) {
	switch s {
	case "objects":
		return objectsOnly, nil
	case "cluster":
		return clusterOnly, nil
	case "both":
		return bothTranslators, nil
	}
	return 0, fmt.Errorf("-translators: %q is not objects, cluster or both", s)
}

func (m mode) usesObjects() bool { return m == objectsOnly || m == bothTranslators }
func (m mode) usesCluster() bool { return m == clusterOnly || m == bothTranslators }

// producerOf is the producer of exactly n bytes: the default one for 18, and otherwise
// the prefix of the default one and filler that does not repeat within a few characters (a
// run of one character would compress to nothing and hide the cost), which is how a
// deployment would come to name a source longer.
func producerOf(n int, def, prefix string) (lifecycle.Producer, error) {
	switch {
	case n == len(def):
		return lifecycle.Producer(def), nil
	case n <= len(prefix):
		return "", fmt.Errorf("a producer name of %d bytes does not leave room after %q", n, prefix)
	}
	return lifecycle.Producer(prefix) + lifecycle.Producer(workload.ProducerFill(n-len(prefix))), nil
}

// producerName is the objects translator's producer of exactly n bytes.
func producerName(n int) (lifecycle.Producer, error) {
	return producerOf(n, defaultProducer, producerPrefix)
}

// clusterProducerName is the cluster translator's producer of exactly n bytes.
func clusterProducerName(n int) (lifecycle.Producer, error) {
	return producerOf(n, defaultClusterProducer, clusterProducerPrefix)
}

// coalesceConfig is the coalescer's setting. With the objects translator alone it is
// that of a pull every five minutes: a refresh extends its run at most once per half its
// TTL, and a run continues as a new one at ten minutes of age. With the cluster translator
// it is the shipped one: a run continues as a new one at the age of its own extension
// interval, which each TTL sets.
func coalesceConfig(m mode) coalesce.Config {
	if m == objectsOnly {
		return coalesce.Config{ExtendTTLFraction: 0.5, RunMaxAge: 10 * time.Minute}
	}
	return coalesce.Config{ExtendTTLFraction: 0.5, RunAgeFromTTL: true}
}

// item is one log record of a capture with its resource.
type item struct {
	res capture.Resource
	rec capture.LogRecord
}

// batch is the records of consecutive refs of one line of a capture: what the
// collector delivered in one export.
type batch []item

// loadDir reads the named files of a capture's directory, in the order given.
func loadDir(dir string, files []string) ([]batch, error) {
	paths := make([]string, len(files))
	for i, f := range files {
		paths[i] = filepath.Join(dir, f)
	}
	return loadCapture(paths...)
}

// loadCapture reads the files in order, each in the replay order of its own records
// (by sequence if every record has one, and otherwise by time), and returns the batches
// of all of them. A later file is a later run of the collector.
func loadCapture(paths ...string) ([]batch, error) {
	var out []batch
	for _, path := range paths {
		bs, err := loadFile(path)
		if err != nil {
			return nil, err
		}
		out = append(out, bs...)
	}
	return out, nil
}

func loadFile(path string) (_ []batch, err error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer func() { err = errors.Join(err, f.Close()) }()
	r := capture.NewReader(f, capture.ReaderOptions{})
	var lines []capture.Line
	for {
		l, err := r.Next()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("%s: %w", path, err)
		}
		lines = append(lines, l)
	}
	refs, err := capture.OrderLogs(lines)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", path, err)
	}
	decoded := make(map[int]capture.LogsData)
	var out []batch
	last := -1
	for _, ref := range refs {
		d, ok := decoded[ref.Index]
		if !ok {
			if d, err = capture.DecodeLogs(lines[ref.Index].Raw); err != nil {
				return nil, fmt.Errorf("%s:%d: %w", path, ref.Line, err)
			}
			decoded[ref.Index] = d
		}
		rl := d.ResourceLogs[ref.Resource]
		it := item{res: rl.Resource, rec: rl.ScopeLogs[ref.Scope].LogRecords[ref.Record]}
		if ref.Index != last {
			out = append(out, nil)
			last = ref.Index
		}
		out[len(out)-1] = append(out[len(out)-1], it)
	}
	return out, nil
}

// counts says how many records each step of the pipeline made.
type counts struct {
	// captured is the log records of the capture, translated the store records the
	// translators made of them, and written what the coalescer left to write.
	captured, translated, written int
	// objects and cluster are the part of translated that each translator made. Both
	// are zero for a translator that was not used.
	objects, cluster int
	// clusterSkipped is what the cluster translator set aside, by reason, such as an entity
	// of a type the catalog does not have. It is nil if the translator was not used.
	clusterSkipped map[string]int
}

// captured is how many log records the batches hold.
func captured(in []batch) int {
	n := 0
	for _, b := range in {
		n += len(b)
	}
	return n
}

// isEntityEvent says that a log record is an entity event of the cluster receiver, the
// kind the objects translator sets aside.
func isEntityEvent(r capture.LogRecord) bool { return strings.HasPrefix(r.EventName, "entity.") }

// translate runs the batches through the translators of the mode, each with its own
// producer of n bytes, then the coalescer, and gives back what the store is to be
// written, a slice per batch, with Seq assigned from 1 in order, and how many records each
// step made. With both translators an entity event goes to the cluster translator and every
// other record to the objects translator, and both write to the one coalescer, so that
// both producers are in every batch as in a deployment.
func translate(in []batch, n int, m mode) ([][]store.Record, counts, error) {
	resolver := identity.NewResolver(catalog.Default())
	var objects *k8sobjects.Translator
	var cluster *k8scluster.Translator
	if m.usesObjects() {
		p, err := producerName(n)
		if err != nil {
			return nil, counts{}, err
		}
		cfg := k8sobjects.Config{Producer: p, Resolver: resolver}
		if m == objectsOnly {
			cfg.EntityTTL = entityTTL
		} else {
			cfg.PullInterval = pullInterval // the TTL is the translator's default multiple of it
		}
		if objects, err = k8sobjects.New(cfg); err != nil {
			return nil, counts{}, err
		}
	}
	if m.usesCluster() {
		p, err := clusterProducerName(n)
		if err != nil {
			return nil, counts{}, err
		}
		// The producer's clock is trusted and no receipt time is given, which are the
		// translator's defaults; the TTL of an event is that of the interval it reports.
		if cluster, err = k8scluster.New(k8scluster.Config{Producer: p, Resolver: resolver, CollectionInterval: pullInterval}); err != nil {
			return nil, counts{}, err
		}
	}
	co, err := coalesce.New(coalesceConfig(m))
	if err != nil {
		return nil, counts{}, err
	}
	c := counts{captured: captured(in)}
	if cluster != nil {
		c.clusterSkipped = make(map[string]int)
	}
	var out [][]store.Record
	var seq uint64
	for i, b := range in {
		var recs []store.Record
		for _, it := range b {
			var made []store.Record
			switch {
			case cluster != nil && isEntityEvent(it.rec):
				res, err := cluster.Translate(it.res, it.rec, time.Time{})
				if err != nil {
					return nil, counts{}, fmt.Errorf("batch %d: %w", i, err)
				}
				made = res.Records
				c.cluster += len(made)
				for _, s := range res.Skipped {
					c.clusterSkipped[s.Reason] += s.Count
				}
			case objects != nil:
				res, err := objects.Translate(it.res, it.rec)
				if err != nil {
					return nil, counts{}, fmt.Errorf("batch %d: %w", i, err)
				}
				made = res.Records
				c.objects += len(made)
			}
			c.translated += len(made)
			for _, r := range made {
				recs = co.Add(r, recs)
			}
		}
		if len(recs) == 0 {
			continue
		}
		c.written += len(recs)
		for j := range recs {
			seq++
			recs[j].Seq = seq
		}
		out = append(out, recs)
	}
	return out, c, nil
}

// Row is what a store came to hold.
type Row struct {
	// Label names the case: "empty" or the length of the producer name.
	Label string
	// N is the producer name's length, or zero for the empty store.
	N int
	// Captured is the log records of the capture, Translated the records the translator
	// made of them, and Records how many the coalescer left, which were written. All are
	// zero for the empty store.
	Captured, Translated int
	Records              int
	// ByObjects and ByCluster are the part of Translated that each translator made, and
	// ClusterSkipped what the cluster translator set aside, by reason; they are filled
	// for a measurement with the cluster translator.
	ByObjects, ByCluster int
	ClusterSkipped       map[string]int
	// PayloadBytes is the sum of their payloads' lengths.
	PayloadBytes uint64
	// LiveTableBytes is the bytes of the tables the store holds once everything is
	// flushed and compacted to the bottom, and at rest.
	LiveTableBytes uint64
	// BytesWritten is what Pebble wrote to tables to get there: flushed and compacted.
	BytesWritten uint64
	// TablesPerLevel is how many tables each level holds at the end.
	TablesPerLevel [7]int64
}

// perRecord is the live bytes per record, or zero for a store with none.
func (r Row) perRecord() float64 {
	if r.Records == 0 {
		return 0
	}
	return float64(r.LiveTableBytes) / float64(r.Records)
}

// meanPayload is the mean length of a record's payload, or zero for no record.
func (r Row) meanPayload() float64 {
	if r.Records == 0 {
		return 0
	}
	return float64(r.PayloadBytes) / float64(r.Records)
}

// Cost is what the longer producer name adds to the tables, as a share of what the
// shorter one adds to an empty store: (live64 - live18) / (live18 - liveEmpty). It
// is an error if the shorter one adds nothing to divide by.
func Cost(live64, live18, liveEmpty uint64) (float64, error) {
	base := int64(live18) - int64(liveEmpty)
	if base <= 0 {
		return 0, fmt.Errorf("the store with producer names of 18 bytes (%d live bytes) is no larger than the empty one (%d)", live18, liveEmpty)
	}
	return float64(int64(live64)-int64(live18)) / float64(base), nil
}

// measure writes the records to a store of its own, in a new directory under tmp, brings
// the store to rest in the bottom level, and reads what it holds. The directory is
// removed before it returns.
func measure(ctx context.Context, tmp, label string, n int, batches [][]store.Record) (_ Row, err error) {
	dir, err := os.MkdirTemp(tmp, "producercost-*")
	if err != nil {
		return Row{}, err
	}
	defer func() { err = errors.Join(err, os.RemoveAll(dir)) }()

	s, err := pebblestore.Open(dir, pebblestore.Options{
		Config:    pebblekv.Quiet(pebblekv.Config{Tuning: pebblekv.BenchTuning()}), // no Sync
		Retention: pebblestore.Synchronous,
		// Checkpoints are the store's default.
	})
	if err != nil {
		return Row{}, err
	}
	closed := false
	defer func() {
		if !closed {
			_ = s.Close()
		}
	}()

	row := Row{Label: label, N: n}
	for _, b := range batches {
		if err := s.Write(ctx, b); err != nil {
			return Row{}, err
		}
		row.Records += len(b)
		for _, r := range b {
			row.PayloadBytes += uint64(len(r.Payload))
		}
	}
	kv := pebblekv.Wrap(s.Instrument().KV())
	if err := kv.CompactAll(ctx); err != nil {
		return Row{}, err
	}
	snap := kv.Snapshot()
	row.LiveTableBytes = snap.LiveTableBytes
	row.BytesWritten = snap.BytesFlushed + snap.BytesCompacted
	row.TablesPerLevel = snap.TablesPerLevel
	closed = true
	if err := s.Close(); err != nil {
		return Row{}, err
	}
	return row, nil
}

// baselineLen is the producer length every cost is taken against.
const baselineLen = 18

// Result is the whole measurement: the empty store, the baseline of 18 bytes, and one row
// for each length asked, in the order asked.
type Result struct {
	Empty, Baseline Row
	Lengths         []Row
}

// Rows are the rows of the table, in order.
func (r Result) Rows() []Row {
	return append([]Row{r.Empty, r.Baseline}, r.Lengths...)
}

// CostOf is the cost of the producer names of a row against those of 18 bytes, with the
// empty store taken off: (live_N - live_18) / (live_18 - live_empty).
func (r Result) CostOf(row Row) (float64, error) {
	return Cost(row.LiveTableBytes, r.Baseline.LiveTableBytes, r.Empty.LiveTableBytes)
}

// Producer lengths that may be asked for: those the workload allows and the store
// writes. The store refuses a producer longer than store.MaxProducerLen, so a longer
// length cannot be measured through it; measurements made at longer lengths, before the
// store had that limit, stand as they were recorded.
const (
	minLen = workload.MinProducerLen
	maxLen = store.MaxProducerLen
)

// parseLengths reads a comma-separated list of producer lengths, each from minLen to maxLen,
// none the baseline length and none twice. The order is kept.
func parseLengths(list string) ([]int, error) {
	var out []int
	seen := map[int]bool{}
	for _, f := range strings.Split(list, ",") {
		f = strings.TrimSpace(f)
		n, err := strconv.Atoi(f)
		switch {
		case err != nil:
			return nil, fmt.Errorf("-lengths: %q is not a number", f)
		case n == baselineLen:
			return nil, fmt.Errorf("-lengths: %d is the baseline, which is always measured", baselineLen)
		case n > maxLen:
			return nil, fmt.Errorf("-lengths: %d is above %d, the longest producer name the store writes", n, maxLen)
		case n < minLen:
			return nil, fmt.Errorf("-lengths: %d is not from %d to %d", n, minLen, maxLen)
		case seen[n]:
			return nil, fmt.Errorf("-lengths: %d is given twice", n)
		}
		seen[n] = true
		out = append(out, n)
	}
	return out, nil
}

// measureLength translates the capture with producer names of n bytes and measures the store.
func measureLength(ctx context.Context, tmp string, in []batch, n int, m mode) (Row, error) {
	recs, counted, err := translate(in, n, m)
	if err != nil {
		return Row{}, fmt.Errorf("%d bytes: %w", n, err)
	}
	row, err := measure(ctx, tmp, fmt.Sprint(n), n, recs)
	if err != nil {
		return Row{}, fmt.Errorf("%d bytes: %w", n, err)
	}
	row.Captured, row.Translated = counted.captured, counted.translated
	row.ByObjects, row.ByCluster, row.ClusterSkipped = counted.objects, counted.cluster, counted.clusterSkipped
	if row.Records != counted.written {
		return Row{}, fmt.Errorf("%d bytes: wrote %d records, the coalescer left %d", n, row.Records, counted.written)
	}
	return row, nil
}

// run measures an empty store, the capture with producer names of 18 bytes, and the capture
// with each of the lengths, which are valid per [parseLengths].
func run(ctx context.Context, tmp string, in []batch, lengths []int, m mode) (Result, error) {
	var res Result
	var err error
	if res.Empty, err = measure(ctx, tmp, "empty", 0, nil); err != nil {
		return Result{}, fmt.Errorf("empty: %w", err)
	}
	if res.Baseline, err = measureLength(ctx, tmp, in, baselineLen, m); err != nil {
		return Result{}, err
	}
	for _, n := range lengths {
		row, err := measureLength(ctx, tmp, in, n, m)
		if err != nil {
			return Result{}, err
		}
		res.Lengths = append(res.Lengths, row)
	}
	return res, nil
}

// parseFiles reads a comma-separated list of the names of the files of a capture, in the
// order they are to be read. An empty entry is refused.
func parseFiles(list string) ([]string, error) {
	var out []string
	for _, f := range strings.Split(list, ",") {
		if f = strings.TrimSpace(f); f == "" {
			return nil, fmt.Errorf("-files: %q has an empty entry", list)
		}
		out = append(out, f)
	}
	return out, nil
}

// skippedLine says what the cluster translator set aside, by reason, or is empty if it
// set nothing aside.
func skippedLine(skipped map[string]int) string {
	var parts []string
	for _, reason := range slices.Sorted(maps.Keys(skipped)) {
		parts = append(parts, fmt.Sprintf("%s: %d", reason, skipped[reason]))
	}
	return strings.Join(parts, "; ")
}
