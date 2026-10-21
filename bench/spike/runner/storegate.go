package runner

import (
	"bufio"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"maps"
	"math"
	"os"
	"path/filepath"
	rtmetrics "runtime/metrics"
	"slices"
	"sort"
	"strconv"
	"strings"
	"sync"
	"text/tabwriter"
	"time"

	"github.com/lotannauo/toposhift/bench/spike/engine"
)

// The store gate judges the root store's retention, apart from the spike's gates: it
// never reads [Rules], and the spike's rules version and digest do not depend on it.
// Its limits are constants here, printed with a digest of their text by [WriteStoreGate],
// so that a change of a limit shows in a report and fails the test that pins the digest.

// The limits of the store gate. Q1 to Q5 are fixed before anything is measured.
const (
	// Log (2026-10-10, before any build was judged by the gate): the text was revised
	// before it was first merged and before any measurement. Q2 reads the 99th
	// percentile inside at the top of its histogram bucket and the one outside at the
	// bottom; MB and GB are decimal; a batch that overlaps a settle and no window is
	// counted apart from the inside and the outside; Q4 divides by the time of the
	// inside batches outside the windows too; the Lroot/off baseline must be judgeable
	// and built with the same GOGC, GOMEMLIMIT and metrics interval. No result exists
	// under the earlier text, so the version stays 1.
	//
	// Log (2026-10-10, before any build was judged by the gate): Q1 also uses its
	// control. A build over 250ms is over the limit only when its control, the longest
	// batch outside every window and every settle, is at most 150ms; with a higher
	// control, or none, Q1 is not judged. A control never prevents a pass, and the
	// 250ms limit is unchanged. The version stays 1.

	// Log (2026-10-10, version 2, before any background build was judged by the gate on a
	// measurement): Q3 assumes a retention at most once a day per store. For a build whose
	// retention runs in the background: Q1 to Q4 are judged only on the paced part, from
	// stream day 2W + 1.5 − ⌈W/2⌉, with the writer paced at 4,500 records/s on an open-loop
	// schedule and its commits synced; the rest, including batches in unjudged windows,
	// prints its values for information. Q3 fails when a rewrite, or a lower bound of one
	// (a pass replaced, or unfinished at the end of the stream), exceeds its limit; it is
	// not judged when a judged pass has only a lower bound, and otherwise passes. At 4,500
	// records/s a stream day lasts minutes of wall time, so Q3 can fail only if the writer
	// stalls for about its limit, and a rewrite that outlasts its stream day is caught by
	// the reopen check before dispatch. Q3's load is taken over the paced writing time, a
	// window's time for Q4 ends at the end of the stream, and Q5 also matches the sync
	// setting, pacing rate and pacing start. A build whose retention is synchronous is
	// judged as in version 1.

	// Log (2026-10-13, version 3, before any background build was judged on CI): Q5 and
	// the repetitions of a verdict also match the retention-flush threshold
	// (retain_flush_dels), which a build through the root store records in its manifest; a
	// manifest without it is not recorded, never read as the default. The thresholds
	// 32,768, 24,576 and 16,384 are registered as three distinct configurations, each a
	// 7-day background dispatch of its own from one frozen commit; a dispatch of one rung
	// is not a redispatch of another with a changed configuration. Incomplete builds: A
	// build is complete when it has written its gate trace. A build that stops before that
	// because the build or the store failed (an error returned, a crash, the process
	// killed for memory, a full disk) has failed, and its arm fails. A build that stops
	// before that for another reason (its build step or job reached its time limit, it was
	// cancelled, or its runner was lost with no sign in its logs of memory or disk
	// exhaustion) is incomplete and is not judged; the report lists it with the reason and
	// the evidence. A cancellation counts as incomplete only if its reason is recorded
	// when it is made (for a cancellation the owner did not make, when it is found) and is
	// not drawn from the build's own output; otherwise the arm is not judged and is not
	// redispatched under this rules version. A complete build with a row the report marks
	// over its limit fails its arm, whatever the arm's other builds did. Otherwise an arm
	// with any incomplete pre-registered build is not judged. An arm that is not judged
	// has not passed, and the pre-registered fallback applies: the release ships with
	// retention off by default. The only exception is one redispatch of the whole arm
	// (every build, baselines included, on every architecture), under the same rules
	// version and the same configuration (the same commit, workflow inputs and plans; the
	// runner image is recorded), that is complete and passes. Its verdict replaces the
	// first dispatch's; the first dispatch's complete builds are reported beside it and
	// are not pooled with it. Re-running some of an arm's jobs, in the same workflow run
	// or another, is not a redispatch and is not allowed. Each incomplete build and each
	// redispatch is recorded, with its reason, in the gate's log before the next dispatch.
	// No build or arm is redispatched with a changed configuration under the same rules
	// version. A changed configuration needs a new rules version, whose log lists every
	// earlier dispatch of the arm with its verdict or why it was incomplete; the fallback
	// holds for any release made before an arm under the new version passes. An arm that
	// finished with a criterion not judged has not passed; it may use the same single
	// whole-arm redispatch as an incomplete build, one in total whatever the reason. A
	// reopen on (a) alone leads to one measured change of the settle deadline under a new
	// rules version, logged, and another quiet run; a reopen on (b) or (c) means the
	// fallback holds for this release, and any change goes into a new, logged rules
	// version followed by another quiet run. The retention-chunk criteria, the threshold
	// ladder, the reopen thresholds and the 30-day dispatch condition were written after
	// the void local 30-day reopen build of 2026-10-11/12 and after local observations of
	// the retention's chunks, whose values were seen and are not published. The 25%
	// settle-share criterion is withdrawn because it could not trip at a 2-minute settle
	// deadline. The chunk criteria's bounds come from the chunk clock's caps (chunkTime/2,
	// 1.5 × chunkTime); the local data informed only the rung values.
	// FlushDelayDeleteRange is 0. No threshold was set from a CI measurement. Under
	// version 2 no background arm was dispatched on CI, and the one local 30-day reopen
	// build was void (machine loaded) and was not read.

	// StoreGateRulesVersion is the version of the gate's rules text.
	StoreGateRulesVersion = 3
	// StoreGatePaceRate is the offered load, in records per second, that a build whose
	// retention runs in the background is paced at and is judged at.
	StoreGatePaceRate = 4500
	// StoreGateQ1MaxWait is the most the longest batch inside a retention window may take.
	StoreGateQ1MaxWait = 250 * time.Millisecond
	// StoreGateQ1NoiseCeiling is the most the control, the longest batch outside every
	// window and every settle, may take for a build over StoreGateQ1MaxWait to be over
	// it: above it the machine's own longest batch leaves too little of the limit to tell
	// a retention's wait from its noise, and Q1 is not judged. A control never prevents
	// a pass.
	StoreGateQ1NoiseCeiling = 150 * time.Millisecond
	// StoreGateQ2Factor and StoreGateQ2Floor make the limit of the 99th percentile of a
	// batch's commit inside windows: the factor times the same outside windows, or the
	// floor if that is more.
	StoreGateQ2Factor = 2
	StoreGateQ2Floor  = 50 * time.Millisecond
	// StoreGateQ3Max is the most the longest rewrite may take, with StoreGateQ3KeepDays
	// days of history kept.
	StoreGateQ3Max      = 2 * time.Hour
	StoreGateQ3KeepDays = 30
	// StoreGateRate is the writer's throughput inside windows that Q4 asks for, in
	// records per second, and the load outside windows below which Q3 is not judged, so
	// that a rewrite is not shown to be quick by writing slowly beside it.
	StoreGateRate = 3000
	// StoreGateQ5HeapMargin is how much live heap a build may peak above Lroot/off's,
	// and StoreGateRSSCeiling the peak resident set it may not exceed, both in bytes
	// and both in the decimal units the rules are worded in (512 MB, 14 GB): reading
	// them as MiB and GiB would relax them by 4.9% and 7.4%.
	StoreGateQ5HeapMargin = 512_000_000
	StoreGateRSSCeiling   = 14_000_000_000
)

// The paragraphs of version 2 for a build whose retention runs in the background.
const (
	storeGateCadence = "Q3 assumes the product calls Retain at most once a day per store, each call moving every layer that is due; its bound is one twelfth of that interval. A shorter product cadence needs a new rule."
	storeGatePacing  = "In a build whose retention runs in the background, `-pace R`: records are due on a fixed wall-clock schedule of R per second from the moment pacing starts; when the writer is behind it writes without sleeping until it is back on schedule, and never runs ahead of it. A build of window W is unpaced for its first D = 2W + 1.5 − ⌈W/2⌉ stream days and paced for the rest (7 days: D = 11.5; 30 days: D = 46.5); any other D is refused. A build whose retention is synchronous is not paced."
)

var storeGateBackgroundRules = []string{
	"In a build whose retention runs in the background, Q1, Q2, Q3 and Q4 are judged only on the paced part of a build paced at the pre-registered offered load of 4,500 records/s: its windows that start at or after D (the judged windows) and, for Q1's control, Q2's outside and Q3's load, its batches that start at or after D. Inside, for Q1, Q2 and Q4, means inside a judged window. A batch that overlaps an unjudged window, or its settle, is counted in neither the inside nor the outside, and is reported. Everything else, and every build that is unpaced or paced at another rate, is reported with its values, for information, and not judged.",
	"A window replaced by a later retention, or still open when the stream ended, ends there, for Q1, Q2 and Q4, and does not refuse the build.",
	"Q3's load is taken over the paced writing time, from D to the end of the stream.",
	"Q3 takes the rewrite time of each judged pass whose rewrite ended. A judged pass that a later retention replaced, or whose rewrite had not ended when the stream ended, has as a lower bound of its rewrite the time from its publication to the publication that replaced it, or to the end of the stream. Q3 fails if any rewrite time or any lower bound exceeds its limit, at Q3's load. Otherwise, if any judged pass has only a lower bound, Q3 is not judged. Otherwise it passes. The report gives the number of passes with only a lower bound and their bounds, and prints, for each pass, the rewrite time, the settle time and whether the settle reached its deadline.",
	"A window's time for Q4 ends at the end of the stream.",
	"Q5 compares builds with the same retention mode, sync setting, pacing rate, pacing start and retention-flush threshold.",
	"A control never prevents a pass.",
	"A build whose retention is synchronous is judged as in version 1.",
}

// StoreGateQ3Note is printed with the report when a build in the background mode is in it.
const StoreGateQ3Note = "At 4,500 records/s a stream day lasts minutes of wall time, so in the Background arm Q3 can fail only if no retention follows a publication for its whole limit, that is, if the writer stalls for about that long; otherwise it passes or is not judged. A rewrite that outlasts its stream day is caught by the reopen check before dispatch."

// The retention-chunk criteria registered 2026-10-12 (C1 to C5), which the report prints
// for each retaining build in the background as met or not met. They are not rows of
// the gate, are outside its rules text, and change no verdict. They are derived from the
// root store's chunk: a chunk's clock charges its iterator's opening up to half the chunk
// time, so C1 bounds how often that cap binds and C2 what a chunk holds when it does. A
// change of the chunk time or of the store's chunk bytes reopens them.
const (
	// RetainChunkTime is the root store's chunk time.
	RetainChunkTime = 20 * time.Millisecond
	// RetainChunkC1OpenBound is the opening C1 counts the chunks over (RetainChunkTime/2),
	// and RetainChunkC1MaxPercent the most of the judged chunks that may be over it.
	RetainChunkC1OpenBound  = RetainChunkTime / 2
	RetainChunkC1MaxPercent = 10
	// RetainChunkC2MaxHold is the most the 99th percentile of a judged chunk's hold may be,
	// read as the upper bound of its bucket (1.5 x RetainChunkTime).
	RetainChunkC2MaxHold = RetainChunkTime * 3 / 2
	// RetainChunkC3MinPrefixes is the least the median of a judged chunk's prefixes may
	// be, read as the lower bound of its bucket.
	RetainChunkC3MinPrefixes = 100
)

// StoreGateRulesText is the text of the rules, made from the constants above: a change
// to a limit or to a word changes it, and with it [StoreGateRulesDigest].
func StoreGateRulesText() string {
	v1 := fmt.Sprintf(`store gate rules %d
window: from a retention's publication to the end of its rewrite; a batch that overlaps it, its first and last instants included, is inside; the settle runs from the end of the rewrite to its own end, and a batch that overlaps it and no window is settling: it is counted in neither the inside nor the outside (so not in the control, and not in Q2's baseline) and its longest and 99th percentile are reported
Q1: the longest batch inside a window is at most %s; the longest batch outside every window and every settle is reported as the control. A build over that limit is over it only if its control is at most %s; with a control above %s, or no batch outside every window and every settle, Q1 is not judged for the build, with the control in the reason, because the machine's own longest batch then leaves too little of the limit to tell a retention's wait from its noise. A control never prevents a pass.
Q2: the 99th percentile of a batch's commit inside windows, read as the upper bound of its histogram bucket, is at most max(%dx the same outside windows and settles, read as the lower bound of its bucket, %s)
Q3: with %d days kept, the longest rewrite (from the publication to the end of the rewrite) is at most %s, at a load outside windows of at least %d records/s; a build below that load is not judged
Q4: the records of the batches inside windows over the windows' total time plus the time of those batches outside the windows are at least %d records/s
Q5: the peak live heap is at most that of Lroot/off on the same plan plus %d MB, both built with the same GOGC, GOMEMLIMIT and metrics interval, and with the same retention mode, sync setting, pacing rate, pacing start and retention-flush threshold; the peak resident set of the build process is at most %d GB; MB and GB are 10^6 and 10^9 bytes
`, StoreGateRulesVersion, StoreGateQ1MaxWait, StoreGateQ1NoiseCeiling, StoreGateQ1NoiseCeiling, StoreGateQ2Factor, StoreGateQ2Floor,
		StoreGateQ3KeepDays, StoreGateQ3Max, StoreGateRate, StoreGateRate,
		StoreGateQ5HeapMargin/1_000_000, int64(StoreGateRSSCeiling)/1_000_000_000)
	return v1 + storeGateCadence + "\n" + storeGatePacing + "\n" + strings.Join(storeGateBackgroundRules, "\n") + "\n"
}

// StoreGateRulesDigest is the SHA-256 of [StoreGateRulesText], as hex.
func StoreGateRulesDigest() string {
	sum := sha256.Sum256([]byte(StoreGateRulesText()))
	return hex.EncodeToString(sum[:])
}

const (
	// RetentionModeKey is the key of a manifest's Describe that gives how the root
	// store's retention ran in the build: "sync" or "background". A manifest of a
	// candidate that is not built through the root store does not have it.
	RetentionModeKey = "retention_mode"
	// GoGCKey is the key of a manifest's Describe that gives the Go runtime's GC target
	// the build ran under (GOGC), in percent, or "off". A manifest without it was built
	// before it was recorded.
	GoGCKey = "go_gc"
)

// gateWindowOpen is the end of a window that has not ended.
const gateWindowOpen = math.MaxInt64

// GateWindow is one retention window of a build, in nanoseconds since the build began
// writing: the retention's publication, the end of its rewrite, the end of its settle,
// and the instant the call that made it returned to the writer. In synchronous mode
// the call returns after the settle and the writer is blocked until then; in the mode
// that runs beside the writer it returns at the publication. A retention that a later
// one replaced before it ended ends at the later one's publication.
type GateWindow struct {
	StartNs, RewriteEndNs, SettleEndNs, ReturnNs int64
	// SupersededNs is the instant of the publication of the retention that replaced this
	// one while it was still rewriting, which ended its rewrite there (0 if none did: a
	// retention that replaced one that had finished rewriting cut its settle short, and
	// left the rewrite as it was). A background store never settles a pass it replaced
	// in its rewrite.
	SupersededNs int64 `json:",omitempty"`
	// SettleDeadlineHit says the settle of this pass reached its deadline.
	SettleDeadlineHit bool `json:",omitempty"`
}

// GateBatches counts the batches a build wrote inside, or outside, every window: how
// many, how many records, and how long they took to commit.
type GateBatches struct {
	Batches, Records int64
	Commit           Histogram
	// SpillNs is, for the batches inside windows, the time of their spans that lies
	// outside every window (a batch that begins before a publication or ends after a
	// rewrite): Q4 divides by the windows' time and this.
	SpillNs int64
}

// GateTrace is what a build records for the store gate: the retention windows, the
// batches inside and outside them, the time the builder spent writing and resting,
// the history it kept, the peak resident set, and the largest of the samples a
// retention reports (the recorder adds samples up; these are their maxima).
type GateTrace struct {
	// Mode is the retention mode of a root-store build ("sync"), empty for a candidate
	// that has none.
	Mode    string
	Windows []GateWindow
	// Inside are the batches that overlap a window, Settling those that overlap a
	// settle and no window, and Outside the rest.
	Inside, Settling, Outside GateBatches
	// WriteNs is the time from the first batch to the end of the stream, RestNs the part
	// of it the builder spent waiting for the database to be at rest after a retention,
	// and KeepNs the history the last retention kept.
	WriteNs, RestNs, KeepNs int64
	// PeakRSS is the largest resident set of the build process, in bytes; 0 if not read.
	PeakRSS int64
	// Maxima are the largest values of the samples "retain.max_prefix_records" and
	// "retain.chunk_hold_ns".
	Maxima map[string]int64
	// StreamEndNs is the instant the last batch of the stream was written and the last
	// retention called, before the build waited for the pass of a background store: the
	// end of the stream for the gate. PaceStartNs is the start of the first paced batch
	// of a build that is paced, 0 for one that is not. Unpaced are the batches that
	// start before it; Excluded the paced batches that overlap a window that started
	// before it (an unjudged window) or its settle, which are counted in neither the
	// inside nor the outside. For a build that is not paced, Inside, Settling and Outside
	// mean what they always did.
	StreamEndNs, PaceStartNs int64 `json:",omitempty"`
	Unpaced, Excluded        GateBatches
	// PacedRestNs is the part of RestNs that falls after the pacing started.
	PacedRestNs int64 `json:",omitempty"`
	// Chunks is, for each of the samples in [gateChunkSamples], how its values spread
	// over the chunks of all the passes of the build pooled, each pass's last chunk
	// left out, which is as short as what was left: reported, not judged. (A pass has
	// only tens of chunks, so a quantile of one pass would be little more than its
	// largest value.) A build recorded before it has none.
	Chunks map[string]GateSpread `json:",omitempty"`
	// JudgedChunks and UnjudgedChunks divide Chunks by the part of the build a chunk is
	// in: the judged part holds the chunks of the passes published at or after the start
	// of the pacing (in a build that was not paced, every pass), each pass's last chunk
	// left out as in Chunks (a pass that a later retention replaced keeps it). A build
	// recorded before they were has neither.
	JudgedChunks, UnjudgedChunks map[string]GateSpread `json:",omitempty"`
	// JudgedCounts are the flushes a pass started, put off and could not start
	// ("retain.flushes", "retain.flush_deferred" and "retain.flush_errors") counted from
	// the start of the pacing (of the stream, in a build that was not paced) to the end
	// of the stream, each of the three present, 0 included. A build recorded before they
	// were has none.
	JudgedCounts map[string]int64 `json:",omitempty"`
	// StallsAtPaceStart and StallsAtStreamEnd are the database's count of write stalls
	// (Pebble's WriteStallBegin), from its opening, at the start of the pacing (0 in a
	// build that was not paced) and at the end of the stream; StallsRead says they were
	// read. A build opens its database once while it writes, so the stalls of its judged
	// part are the second less the first: exact up to the first paced batch's wait, since
	// the first count is read just before it.
	StallsAtPaceStart, StallsAtStreamEnd int64 `json:",omitempty"`
	StallsRead                           bool  `json:",omitempty"`
}

// GateSpread is how the values of one sample spread: how many there were, the least
// and the largest, exactly, and the 50th, 90th and 99th percentiles, each read as the
// upper bound of its histogram bucket (exact below 8, and otherwise at most an eighth
// above the value); P50Low is the 50th read as the lower bound of its bucket, Sum the
// exact sum of the values (the mean is Sum/Count), and Over, for "retain.chunk_open_ns"
// only, the exact count of values above [RetainChunkC1OpenBound]. A spread recorded
// before P90, P50Low, Sum and Over were has none of them.
type GateSpread struct {
	Count, Min, P50, P99 int64
	Max                  int64 `json:",omitempty"`
	P90, P50Low, Sum     int64 `json:",omitempty"`
	Over                 int64 `json:",omitempty"`
}

// mean is the mean of the values, 0 for none.
func (s GateSpread) mean() float64 {
	if s.Count == 0 {
		return 0
	}
	return float64(s.Sum) / float64(s.Count)
}

// gateChunkSamples are the samples a retention gives once for each chunk it commits,
// in this order, whose spread a build keeps; "retain.chunks" is sampled once for each
// pass that ends, after the samples of its last chunk.
var gateChunkSamples = []string{"retain.chunk_open_ns", "retain.chunk_prefixes", "retain.chunk_hold_ns"}

// chunkSpread gathers the samples of the chunks: those of a chunk are held until the
// next chunk's first sample shows that it was not the last of its pass, and dropped
// when the pass's "retain.chunks" shows that it was. A pass that a later retention
// replaced in its rewriting ends with no "retain.chunks": its last chunk was cut by
// nothing but the replacement, and is kept.
//
// Each chunk kept is also added to the spread of its part of the build, judged or not,
// which the function given to keep and spread decides from the instant of the chunk's
// first sample (see [gateTracker.chunkJudged]). The part is decided when the chunk is
// kept, not when it is sampled, so that the publication of its pass, which the store
// tells from another goroutine, has been seen.
type chunkSpread struct {
	// seen says a sample of a chunk came, kept or not.
	seen bool
	held map[string]int64
	// heldAt is the instant of the first sample of the chunk held.
	heldAt                int64
	all, judged, unjudged sampleSpread
}

// sampleSpread gathers the values of each sample of the chunks of one part.
type sampleSpread struct {
	hist  map[string]*Histogram
	least map[string]int64
	sums  map[string]int64
	// over counts the openings above [RetainChunkC1OpenBound].
	over int64
}

func (s *sampleSpread) add(held map[string]int64) {
	if s.hist == nil {
		s.hist, s.least, s.sums = map[string]*Histogram{}, map[string]int64{}, map[string]int64{}
	}
	for name, v := range held {
		h := s.hist[name]
		if h == nil {
			h = &Histogram{}
			s.hist[name] = h
		}
		if h.Count == 0 || v < s.least[name] {
			s.least[name] = v
		}
		h.Add(time.Duration(v))
		s.sums[name] += v
		if name == "retain.chunk_open_ns" && v > int64(RetainChunkC1OpenBound) {
			s.over++
		}
	}
}

// spread is the spread of each sample, each of [gateChunkSamples] present (with a
// count of 0 for one that has no value).
func (s *sampleSpread) spread() map[string]GateSpread {
	out := map[string]GateSpread{}
	for _, name := range gateChunkSamples {
		out[name] = GateSpread{}
	}
	for name, h := range s.hist {
		lo, _ := quantileBounds(*h, 0.5)
		sp := GateSpread{
			Count: h.Count, Min: s.least[name], P50: h.Quantile(0.5), P90: h.Quantile(0.9), P99: h.Quantile(0.99), Max: h.MaxNs,
			P50Low: lo, Sum: s.sums[name],
		}
		if name == "retain.chunk_open_ns" {
			sp.Over = s.over
		}
		out[name] = sp
	}
	return out
}

func (c *chunkSpread) sample(name string, v int64, at int64, judged func(at int64) bool) {
	c.seen = true
	if name == gateChunkSamples[0] {
		c.keep(judged)
		c.heldAt = at
	}
	if c.held == nil {
		c.held = map[string]int64{}
	}
	c.held[name] = v
}

// keep adds the chunk held to the spread, and to that of its part.
func (c *chunkSpread) keep(judged func(at int64) bool) {
	if len(c.held) == 0 {
		return
	}
	c.all.add(c.held)
	if judged(c.heldAt) {
		c.judged.add(c.held)
	} else {
		c.unjudged.add(c.held)
	}
	clear(c.held)
}

// passEnded drops the chunk held: it was the last of its pass.
func (c *chunkSpread) passEnded() { clear(c.held) }

// spread is the spread of each sample over every chunk, over those of the judged part
// and over the rest, with the chunk held counted (a build that ends in a pass ends with
// a chunk that was not its last). A build whose store sampled chunks, all of them the
// last of their pass, has each sample with a count of 0; one whose store sampled none
// has nil.
func (c *chunkSpread) spread(judged func(at int64) bool) (all, inJudged, unjudged map[string]GateSpread) {
	c.keep(judged)
	if !c.seen {
		return nil, nil, nil
	}
	return c.all.spread(), c.judged.spread(), c.unjudged.spread()
}

// gateMaxima are the samples whose largest value a build keeps.
var gateMaxima = []string{"retain.max_prefix_records", "retain.chunk_hold_ns"}

// maxRecorder passes everything to the recorder it wraps, and keeps the largest value
// of each sample in [gateMaxima].
type maxRecorder struct {
	engine.Recorder
	mu  sync.Mutex
	max map[string]int64
	// gate, if not nil, is told each count of a settle that reached its deadline.
	gate *gateTracker
}

func newMaxRecorder(r engine.Recorder) *maxRecorder {
	return &maxRecorder{Recorder: r, max: map[string]int64{}}
}

// Count implements [engine.Recorder]. The store counts "retain.settle_deadline_hits" once
// for each settle, 1 if it reached its deadline and 0 if not, just before it tells its
// observer that the settle ended.
func (m *maxRecorder) Count(name string, n int64) {
	switch {
	case name == "retain.settle_deadline_hits":
		m.gate.settleCounted(n)
	case slices.Contains(gateFlushCounts, name):
		m.gate.flushCounted(name, n)
	}
	m.Recorder.Count(name, n)
}

// gateFlushCounts are the counts of the flushes a pass starts, puts off and cannot
// start, which a build counts in its judged part too.
var gateFlushCounts = []string{"retain.flushes", "retain.flush_deferred", "retain.flush_errors"}

// Sample implements [engine.Recorder].
func (m *maxRecorder) Sample(name string, v int64) {
	if slices.Contains(gateMaxima, name) {
		m.mu.Lock()
		m.max[name] = max(m.max[name], v)
		m.mu.Unlock()
	}
	switch {
	case slices.Contains(gateChunkSamples, name):
		m.gate.chunkSample(name, v)
	case name == "retain.chunks":
		m.gate.passEnded()
	}
	m.Recorder.Sample(name, v)
}

func (m *maxRecorder) maxima() map[string]int64 {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := map[string]int64{}
	for k, v := range m.max {
		out[k] = v
	}
	return out
}

// gateTracker builds a [GateTrace] as a build runs. The windows come in order and do
// not overlap; a batch is inside if it overlaps one, its first and last instants
// included. Every method accepts a nil receiver, and is safe to call from the
// goroutine that retains and the one that writes.
type gateTracker struct {
	mu                        sync.Mutex
	epoch                     time.Time
	windows                   []GateWindow
	inside, settling, outside GateBatches
	rest                      time.Duration
	// gens is what the observer of a background store told of each generation, by the
	// number the store's marker carries, whether or not its publication has been told;
	// lastPub is the one published last, whose window the next publication cuts.
	gens    map[uint64]*genEnds
	lastPub *genEnds
	// paced says the build is paced: its batches before paceStartNs (0 until the pacing
	// starts) are unpaced, and those after it that touch a window that started before it
	// are excluded. restPaced is the part of rest after the pacing started.
	paced       bool
	paceStartNs int64
	unpaced     GateBatches
	excluded    GateBatches
	restPaced   time.Duration
	// pendingHit is whether the last settle the store counted reached its deadline, until
	// the observer is told that settle ended (see settleCounted).
	pendingHit bool
	// chunks gathers the samples of the chunks of the retentions.
	chunks chunkSpread
	// now is the clock a chunk's sample is timed with; time.Now unless a test sets it.
	now func() time.Time
	// judgedStarted says the judged part has started: at the start of the pacing, or of
	// the stream in a build that is not paced; streamEnded says it has ended. judgedCounts
	// are the flushes counted between the two.
	judgedStarted, streamEnded bool
	judgedCounts               map[string]int64
	// stallsAtPace and stallsAtEnd are the database's write stalls at the start of the
	// pacing and at the end of the stream, and stallsRead says the second was read (and
	// the first, in a paced build).
	stallsAtPace, stallsAtEnd int64
	stallsRead                bool
}

// chunkSample is one sample of a chunk of a retention (see [gateChunkSamples]).
func (g *gateTracker) chunkSample(name string, v int64) {
	if g == nil {
		return
	}
	at := g.now()
	g.mu.Lock()
	defer g.mu.Unlock()
	g.chunks.sample(name, v, g.off(at), g.chunkJudged)
}

// chunkJudged says whether a chunk whose first sample came at the instant at (since
// the epoch) is in the judged part: its pass was published at or after the start of
// the pacing, its pass being the last window published at or before the chunk. Every
// chunk of a build that is not paced is. The caller holds g.mu.
func (g *gateTracker) chunkJudged(at int64) bool {
	if !g.paced {
		return true
	}
	if g.paceStartNs == 0 || at < g.paceStartNs {
		return false
	}
	for i := len(g.windows) - 1; i >= 0; i-- {
		if g.windows[i].StartNs <= at {
			return g.windows[i].StartNs >= g.paceStartNs
		}
	}
	return false
}

// startJudged starts the judged part of a build that is not paced, at the start of
// its stream.
func (g *gateTracker) startJudged() {
	if g == nil {
		return
	}
	g.mu.Lock()
	g.judgedStarted = true
	g.mu.Unlock()
}

// flushCounted is a count of a pass's flushes (see [gateFlushCounts]): it is counted in
// the judged part if that has started and the stream has not ended.
func (g *gateTracker) flushCounted(name string, n int64) {
	if g == nil {
		return
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.judgedStarted && !g.streamEnded {
		if g.judgedCounts == nil {
			g.judgedCounts = map[string]int64{}
		}
		g.judgedCounts[name] += n
	}
}

// stallsAtPacing records the database's write stalls at the start of the pacing, read
// just before the first paced batch.
func (g *gateTracker) stallsAtPacing(n int64) {
	if g == nil {
		return
	}
	g.mu.Lock()
	g.stallsAtPace = n
	g.mu.Unlock()
}

// endStream ends the judged part at the end of the stream, with the database's write
// stalls then (ok false if the engine does not count them).
func (g *gateTracker) endStream(stalls int64, ok bool) {
	if g == nil {
		return
	}
	g.mu.Lock()
	g.streamEnded = true
	g.stallsAtEnd, g.stallsRead = stalls, ok
	g.mu.Unlock()
}

// passEnded is a retention's "retain.chunks": its pass has ended.
func (g *gateTracker) passEnded() {
	if g == nil {
		return
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	g.chunks.passEnded()
}

func newGateTracker(epoch time.Time) *gateTracker { return &gateTracker{epoch: epoch, now: time.Now} }

func (g *gateTracker) off(t time.Time) int64 { return t.Sub(g.epoch).Nanoseconds() }

// publish opens a window at the publication of a retention. Until the rewrite ends the
// window is open, and a batch that ends after its start is inside.
func (g *gateTracker) publish(at time.Time) {
	if g == nil {
		return
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	s := g.off(at)
	g.windows = append(g.windows, GateWindow{StartNs: s, RewriteEndNs: gateWindowOpen, SettleEndNs: gateWindowOpen, ReturnNs: s})
}

// rewriteEnded closes the open window at the end of its rewrite.
func (g *gateTracker) rewriteEnded(at time.Time) {
	if w := g.open(); w != nil {
		g.mu.Lock()
		w.RewriteEndNs = g.off(at)
		g.mu.Unlock()
	}
}

// settleEnded sets the end of the settle of the last window.
func (g *gateTracker) settleEnded(at time.Time) {
	if w := g.last(); w != nil {
		g.mu.Lock()
		w.SettleEndNs = g.off(at)
		g.mu.Unlock()
	}
}

// returned sets the instant the call that made the last window returned to the writer.
func (g *gateTracker) returned(at time.Time) {
	if w := g.last(); w != nil {
		g.mu.Lock()
		w.ReturnNs = g.off(at)
		g.mu.Unlock()
	}
}

func (g *gateTracker) last() *GateWindow {
	if g == nil {
		return nil
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	if len(g.windows) == 0 {
		return nil
	}
	return &g.windows[len(g.windows)-1]
}

func (g *gateTracker) open() *GateWindow {
	if w := g.last(); w != nil && w.RewriteEndNs == gateWindowOpen {
		return w
	}
	return nil
}

// count is how many windows there are.
func (g *gateTracker) count() int {
	if g == nil {
		return 0
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	return len(g.windows)
}

// returnedSince sets the instant the call that made the window after the first n
// returned to the writer, if it made one: a retention that moved nothing makes none.
func (g *gateTracker) returnedSince(n int, at time.Time) {
	if g == nil {
		return
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	if n < len(g.windows) {
		g.windows[n].ReturnNs = g.off(at)
	}
}

// The three calls that follow feed the windows of a store that retains in the
// background, from its observer. They come from the goroutine that reached the point,
// so any of a generation's calls may arrive before another of its own, or after a later
// one of another goroutine; the instant given, not the order of arrival, says when it
// happened. What each call tells is kept for its generation, with each end at the
// earliest instant it is given, and the window is worked out again from that and from
// the next publication at every call, so the windows depend only on the instants. A
// phase that a later publication cuts short ends there. Publications arrive in
// generation order, because one goroutine calls Retain and the store tells a
// publication before Retain returns; that is not checked. A generation whose
// publication is never told (a pass an earlier process left) has no window, takes no
// batch and moves no other window.

// genEnds is what the observer told of one generation: the earliest instant given for
// the end of its rewrite and of its settle (gateWindowOpen until one is), the deadline
// count taken with its settle, and its window (-1 until its publication is told).
type genEnds struct {
	rewrite, settle int64
	hit             bool
	win             int
}

// ends is the record of generation gen, made on the first call that names it. The
// caller holds g.mu.
func (g *gateTracker) ends(gen uint64) *genEnds {
	if e, ok := g.gens[gen]; ok {
		return e
	}
	if g.gens == nil {
		g.gens = map[uint64]*genEnds{}
	}
	e := &genEnds{rewrite: gateWindowOpen, settle: gateWindowOpen, win: -1}
	g.gens[gen] = e
	return e
}

// place works out the window of e from what was told of it, if its publication was
// told: the publication after it, if there is one, cuts it. A background build makes its
// windows only here, so the window after e's is the next publication. The caller holds
// g.mu.
func (g *gateTracker) place(e *genEnds) {
	if e.win < 0 {
		return
	}
	cut := int64(gateWindowOpen)
	if e.win+1 < len(g.windows) {
		cut = g.windows[e.win+1].StartNs
	}
	w := &g.windows[e.win]
	w.RewriteEndNs = min(e.rewrite, cut)
	w.SettleEndNs = max(min(e.settle, cut), w.RewriteEndNs)
	w.SupersededNs = 0
	if cut != gateWindowOpen && e.rewrite >= cut {
		// The store reports a pass it replaced in its rewrite as ended at the instant of
		// the publication that replaced it, and never settles it.
		w.SupersededNs = cut
	}
	w.SettleDeadlineHit = e.hit
}

// publishedGen opens the window of generation gen at its publication, with whatever
// was told of it before, and cuts the window before it: a retention that has not ended
// ends here, with the rewrite or the settle it was in, because the store replaced it,
// and its settle, if it was settling, is not part of any window from now on. A
// publication that arrives out of generation order, or twice, is not guarded against;
// the store rules both out.
func (g *gateTracker) publishedGen(gen uint64, at time.Time) {
	if g == nil {
		return
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	s := g.off(at)
	prev, e := g.lastPub, g.ends(gen)
	e.win = len(g.windows)
	g.windows = append(g.windows, GateWindow{StartNs: s, RewriteEndNs: gateWindowOpen, SettleEndNs: gateWindowOpen, ReturnNs: s})
	g.lastPub = e
	if prev != nil {
		g.place(prev) // this publication cuts the window before it
	}
	g.place(e) // with whatever was told of gen before its publication
}

// rewriteEndedGen keeps the end of the rewrite of generation gen, whether or not its
// publication has been told; the window, if there is one, is worked out again.
func (g *gateTracker) rewriteEndedGen(gen uint64, at time.Time) {
	if g == nil {
		return
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	// The settle that follows is counted after this and before the pass is told it
	// ended; a count that is still pending is of a pass that was replaced as it settled.
	g.pendingHit = false
	e := g.ends(gen)
	e.rewrite = min(e.rewrite, g.off(at))
	g.place(e)
}

// settleCounted is told the store's count of the settle it has just done: 1 if it
// reached its deadline, 0 if not. The store counts a settle, then tells the observer
// that it ended, from one goroutine, so the count is the one the next settleEndedGen
// takes; a count no settleEndedGen takes (of a pass that was replaced as it settled) is
// dropped when the next pass's rewrite ends. A publication does not drop it: a Retain
// may come between the count of a settle and the report that it ended.
func (g *gateTracker) settleCounted(hits int64) {
	if g == nil {
		return
	}
	g.mu.Lock()
	g.pendingHit = hits > 0
	g.mu.Unlock()
}

// settleEndedGen keeps the end of the settle of generation gen, with the count taken
// for it, whether or not its publication has been told; the window, if there is one, is
// worked out again.
func (g *gateTracker) settleEndedGen(gen uint64, at time.Time) {
	if g == nil {
		return
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	e := g.ends(gen)
	e.settle = min(e.settle, g.off(at))
	e.hit = g.pendingHit
	g.pendingHit = false
	g.place(e)
}

// gateObserver tells a gateTracker what a background store reports of its retentions;
// it is the store's observer (pebblestore.RetainObserver, which it satisfies without
// naming), and is given to the candidate that is built.
type gateObserver struct{ g *gateTracker }

func (o gateObserver) Published(gen uint64, at time.Time)    { o.g.publishedGen(gen, at) }
func (o gateObserver) RewriteEnded(gen uint64, at time.Time) { o.g.rewriteEndedGen(gen, at) }
func (o gateObserver) SettleEnded(gen uint64, at time.Time)  { o.g.settleEndedGen(gen, at) }

// syncWindow records the window of a retention that did all of its work before it
// returned: the rewrite took work from the call at start, then the flush and the
// settle, and the call returned at ret. A retention that rewrote nothing makes none.
func (g *gateTracker) syncWindow(start, ret time.Time, work, flush, settle time.Duration) {
	if g == nil || work <= 0 {
		return
	}
	g.publish(start)
	g.rewriteEnded(start.Add(work))
	g.settleEnded(start.Add(work + flush + settle))
	g.returned(ret)
}

// setPaced says the build is paced: its batches before the pacing starts are unpaced.
func (g *gateTracker) setPaced() {
	if g == nil {
		return
	}
	g.mu.Lock()
	g.paced = true
	g.mu.Unlock()
}

// startPacing records the start of the first paced batch, once.
func (g *gateTracker) startPacing(at time.Time) {
	if g == nil {
		return
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.paceStartNs == 0 {
		g.paceStartNs = g.off(at)
		g.judgedStarted = true
	}
}

// batch records a batch that began at start and took d to commit. In a paced build a
// batch that starts before the pacing did is unpaced, and one that overlaps a window
// that started before it (an unjudged window), or that window's settle, is excluded;
// neither is inside, settling or outside.
func (g *gateTracker) batch(start time.Time, d time.Duration, records int) {
	if g == nil {
		return
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	s := g.off(start)
	e := s + d.Nanoseconds()
	count := func(b *GateBatches) {
		b.Batches++
		b.Records += int64(records)
		b.Commit.Add(d)
	}
	if g.paced && (g.paceStartNs == 0 || s < g.paceStartNs) {
		count(&g.unpaced)
		return
	}
	in, settling, excluded := false, false, false
	var overlap int64 // the time of the batch inside windows
	for i := len(g.windows) - 1; i >= 0; i-- {
		w := g.windows[i]
		if w.SettleEndNs < s { // the windows are in order: the earlier ones end earlier
			break
		}
		unjudged := g.paced && w.StartNs < g.paceStartNs
		switch {
		case w.StartNs <= e && s <= w.RewriteEndNs:
			if unjudged {
				excluded = true
				break
			}
			in = true
			overlap += min(e, w.RewriteEndNs) - max(s, w.StartNs)
		case s <= w.SettleEndNs && e >= w.RewriteEndNs:
			if unjudged {
				excluded = true
				break
			}
			settling = true
		}
	}
	switch {
	case excluded:
		count(&g.excluded)
	case in:
		g.inside.SpillNs += max(e-s-overlap, 0)
		count(&g.inside)
	case settling:
		count(&g.settling)
	default:
		count(&g.outside)
	}
}

// rested adds the time the builder waited for the database to be at rest.
func (g *gateTracker) rested(d time.Duration) {
	if g == nil {
		return
	}
	g.mu.Lock()
	g.rest += d
	if g.paceStartNs > 0 {
		g.restPaced += d
	}
	g.mu.Unlock()
}

// trace is the record of the build: the end of the stream is end, and the windows are
// as they are when it is taken. Windows still open stay open in it; the judge refuses
// them in a synchronous build and ends them at the end of the stream in a background
// one.
func (g *gateTracker) trace(end time.Time, mode string, keep time.Duration, maxima map[string]int64) *GateTrace {
	g.mu.Lock()
	defer g.mu.Unlock()
	all, judged, unjudged := g.chunks.spread(g.chunkJudged)
	t := &GateTrace{
		Mode: mode, Windows: slices.Clone(g.windows), Inside: g.inside, Settling: g.settling, Outside: g.outside,
		WriteNs: g.off(end), RestNs: g.rest.Nanoseconds(), KeepNs: keep.Nanoseconds(), Maxima: maxima,
		StreamEndNs: g.off(end), PaceStartNs: g.paceStartNs, Unpaced: g.unpaced, Excluded: g.excluded,
		PacedRestNs: g.restPaced.Nanoseconds(), Chunks: all, JudgedChunks: judged, UnjudgedChunks: unjudged,
		StallsAtPaceStart: g.stallsAtPace, StallsAtStreamEnd: g.stallsAtEnd, StallsRead: g.stallsRead,
	}
	if g.judgedStarted {
		t.JudgedCounts = map[string]int64{}
		for _, name := range gateFlushCounts {
			t.JudgedCounts[name] = g.judgedCounts[name]
		}
	}
	return t
}

// bucketLower is the lower bound of a histogram bucket, in nanoseconds: its own
// duration for the exact buckets, and the start of its range for the others.
func bucketLower(b int) int64 {
	if b < 8 {
		return int64(b)
	}
	k, sub := (b-8)/8+4, (b-8)%8
	return int64(8+sub) << (k - 4)
}

// quantileBounds are the bounds of the bucket the q-quantile falls in: the quantile is
// at least lo and, for a bucket that is not exact, below hi ([Histogram.Quantile] is hi).
// Both are 0 for no durations.
func quantileBounds(h Histogram, q float64) (lo, hi int64) {
	if h.Count == 0 {
		return 0, 0
	}
	want := int64(float64(h.Count)*q + 0.999999999)
	want = min(max(want, 1), h.Count)
	var seen int64
	for b, n := range h.Buckets {
		if seen += n; seen >= want {
			return bucketLower(b), boundOf(b)
		}
	}
	return h.MaxNs, h.MaxNs
}

// processGC is the GC target the process started with, as a manifest records it ("" if
// the runtime does not say). It is read once, when the package is initialized, from the
// runtime's metrics: reading does not set anything, unlike a read through
// debug.SetGCPercent, which sets the target to read it and so shows -1 to a build that
// runs while another reads (and to code that sets it for a while, as a cold read does).
// A build records this value whatever is done to the target after the program starts.
var processGC = readGCPercent()

// readGCPercent reads the GC target from the runtime's metrics: a very large value, the
// unsigned form of -1, is "off".
func readGCPercent() string {
	s := []rtmetrics.Sample{{Name: "/gc/gogc:percent"}}
	rtmetrics.Read(s)
	if s[0].Value.Kind() != rtmetrics.KindUint64 {
		return ""
	}
	return gcPercentText(int(int64(s[0].Value.Uint64())))
}

// gcPercentText is how a manifest records the GC target: the percent in decimal, or
// "off" for none.
func gcPercentText(n int) string {
	if n < 0 {
		return "off"
	}
	return strconv.Itoa(n)
}

// StoreGateBuild is a build the gate judges: its manifest, its job if the bench
// workflow wrote one, and what its metrics file says of memory.
type StoreGateBuild struct {
	Dir      string
	Manifest *Manifest
	Job      *Job
	Metrics  StoreGateMetrics
}

// StoreGateMetrics is what the gate reads from a build's metrics file.
type StoreGateMetrics struct {
	// Lines is the number of lines, HeapLiveSamples those with a live heap above 0.
	Lines, HeapLiveSamples int
	HeapLivePeak           int64
	// TombstonesPeak is the most tombstones any line counted.
	TombstonesPeak int64
	// L0SublevelsPeak is the most sublevels of level 0 any line counted. WriteStalls
	// is a lower bound of the write stalls of the whole build, summed over the
	// openings of its database: a line counts them from the opening of the database,
	// so a count lower than the line before's starts another opening, and the last
	// count of each opening is added up. It is a lower bound because the stalls after
	// an opening's last line are not counted, and an opening that no line sampled
	// before it overtook the count of the one before is missed. WriteStallsRead says a line counted them (a
	// build recorded before they were has none). Reported, not judged.
	L0SublevelsPeak, WriteStalls int64
	WriteStallsRead              bool
	// JudgedLines is the number of lines written in the judged part of the stream (see
	// metricLine.Judged), and JudgedL0SublevelsPeak the most sublevels of level 0 any of
	// them counted, which misses a peak between two lines. Reported, not judged.
	// UnjudgedL0SublevelsPeak is the same over the other lines.
	JudgedLines                                    int
	JudgedL0SublevelsPeak, UnjudgedL0SublevelsPeak int64
}

// LoadStoreGateBuild reads the build under dir.
func LoadStoreGateBuild(dir string) (*StoreGateBuild, error) {
	m, _, err := readManifest(dir)
	if err != nil {
		return nil, err
	}
	b := &StoreGateBuild{Dir: dir, Manifest: m}
	switch raw, err := os.ReadFile(filepath.Join(dir, JobFile)); {
	case err == nil:
		var j Job
		if err := json.Unmarshal(raw, &j); err != nil {
			return nil, fmt.Errorf("runner: %s: %w", filepath.Join(dir, JobFile), err)
		}
		b.Job = &j
	case !errors.Is(err, os.ErrNotExist):
		return nil, err
	}
	if b.Metrics, err = readStoreGateMetrics(filepath.Join(dir, MetricsFile)); err != nil {
		return nil, err
	}
	return b, nil
}

// readStoreGateMetrics reads the peak live heap, the peak tombstones, the peak
// sublevels of level 0 and the write stalls off a metrics file; a file that is not
// there has none.
func readStoreGateMetrics(path string) (StoreGateMetrics, error) {
	var out StoreGateMetrics
	f, err := os.Open(path)
	if errors.Is(err, os.ErrNotExist) {
		return out, nil
	}
	if err != nil {
		return out, err
	}
	defer f.Close()
	sc := bufio.NewScanner(f)
	sc.Buffer(nil, 1<<22)
	// The write stalls of the openings before the current one, and of the current one.
	var before, opening int64
	for n := 1; sc.Scan(); n++ {
		var l metricLine
		if err := json.Unmarshal(sc.Bytes(), &l); err != nil {
			return out, fmt.Errorf("runner: %s line %d: %w", path, n, err)
		}
		out.Lines++
		if l.Go.HeapLive > 0 {
			out.HeapLiveSamples++
			out.HeapLivePeak = max(out.HeapLivePeak, l.Go.HeapLive)
		}
		out.TombstonesPeak = max(out.TombstonesPeak, l.Stats["tombstones"])
		out.L0SublevelsPeak = max(out.L0SublevelsPeak, l.Stats["l0_sublevels"])
		if l.Judged {
			out.JudgedLines++
			out.JudgedL0SublevelsPeak = max(out.JudgedL0SublevelsPeak, l.Stats["l0_sublevels"])
		} else {
			out.UnjudgedL0SublevelsPeak = max(out.UnjudgedL0SublevelsPeak, l.Stats["l0_sublevels"])
		}
		if n, ok := l.Stats["write_stalls"]; ok {
			if n < opening {
				before += opening // a new opening of the database
			}
			opening, out.WriteStallsRead = n, true
		}
	}
	out.WriteStalls = before + opening
	return out, sc.Err()
}

// LoadStoreGateBuilds loads the builds named by roots: a root with a manifest is one
// build, and one without is a directory of builds, each of its subdirectories that has
// a manifest.
func LoadStoreGateBuilds(roots []string) ([]*StoreGateBuild, error) {
	var out []*StoreGateBuild
	for _, root := range roots {
		if _, err := os.Stat(filepath.Join(root, ManifestFile)); err == nil {
			b, err := LoadStoreGateBuild(root)
			if err != nil {
				return nil, err
			}
			out = append(out, b)
			continue
		}
		entries, err := os.ReadDir(root)
		if err != nil {
			return nil, err
		}
		found := 0
		for _, e := range entries {
			dir := filepath.Join(root, e.Name())
			if !e.IsDir() || e.Name() == DBDir {
				continue
			}
			if _, err := os.Stat(filepath.Join(dir, ManifestFile)); err != nil {
				continue
			}
			b, err := LoadStoreGateBuild(dir)
			if err != nil {
				return nil, err
			}
			out = append(out, b)
			found++
		}
		if found == 0 {
			return nil, fmt.Errorf("runner: %s is not a build and holds none (give a directory with a %s, or one whose subdirectories have)", root, ManifestFile)
		}
	}
	return out, nil
}

// The verdicts of the gate.
const (
	GateOK        = "ok"
	GateOver      = "OVER"
	GateNotJudged = "not judged"
	GateBaseline  = "baseline"
)

// The rows of the gate, in the order they are shown.
const (
	gateQ1 = iota
	gateQ2
	gateQ3
	gateQ4
	gateQ5Heap
	gateQ5RSS
	gateRows
)

var gateRowNames = [gateRows]string{
	"Q1 longest batch inside a window", "Q2 batch commit p99 inside windows", "Q3 longest rewrite",
	"Q4 writer throughput inside windows", "Q5 peak live heap", "Q5 peak resident set",
}

// StoreGateRow is one verdict, of one candidate on one architecture.
type StoreGateRow struct {
	Candidate, Arch, Gate, Value, Limit, State string
	// Partial says some repetitions of the row could not be judged.
	Partial bool
}

// StoreGateReport is the verdicts of the gate and what they stand on.
type StoreGateReport struct {
	RulesText, RulesDigest string
	// Modes are the retention modes of the builds, in order.
	Modes []string
	Rows  []StoreGateRow
	// Info is what the builds report that is not judged.
	Info []string
	// NotJudged says, for each gate a build was not judged on, why.
	NotJudged []string
	// Over is the number of rows over their limit, and Unexpected the number of rows of
	// a build that were not judged though its mode has something to judge in them.
	Over, Unexpected int
	// Refusal, if not empty, is why nothing was judged: the builds are not a set the
	// gate judges (see [CheckStoreGateSettings]). Rows and the counts are then empty.
	Refusal string
}

// gateRow is one build's verdict on one row.
type gateRow struct {
	judged, pass, baseline bool
	// byConstruction says a row that is not judged cannot be: the mode has nothing for
	// it to judge (no batch is written inside a window of a synchronous retention).
	byConstruction bool
	// margin is how much of the limit the build uses: above 1 is over, and the build
	// with the most is the worst of its repetitions.
	margin       float64
	value, limit string
	reason       string
	build        *StoreGateBuild
}

func fmtDur(ns int64) string {
	d := time.Duration(ns)
	if d >= time.Second {
		return d.Round(time.Millisecond).String()
	}
	return d.Round(time.Microsecond).String()
}

func fmtMB(n int64) string { return fmt.Sprintf("%.1f MB", float64(n)/1e6) }

func (b *StoreGateBuild) arch() string { return b.Manifest.Build.GOARCH }

func (b *StoreGateBuild) rep() int {
	if b.Job != nil {
		return b.Job.Rep
	}
	return 0
}

func (b *StoreGateBuild) label() string {
	s := b.Manifest.Candidate + " " + b.arch()
	if r := b.rep(); r > 0 {
		s += " rep " + strconv.Itoa(r)
	}
	return s + " (" + b.Dir + ")"
}

// refusal is why none of the build can be judged, or "".
func (b *StoreGateBuild) refusal() string {
	m := b.Manifest
	switch {
	case !strings.HasPrefix(m.Candidate, "Lroot/"):
		return "not a build through the root store (a candidate named Lroot/...)"
	case m.Untimed:
		return "an untimed build: a result does not come from it"
	case m.Gate == nil:
		return "built without the gate's record (by an earlier binary)"
	case m.Gate.Mode == "":
		return "no retention mode recorded"
	}
	if m.Gate.Mode == "background" {
		if why := b.paceRefusal(); why != "" {
			return why
		}
	}
	for i, w := range m.Gate.Windows {
		switch {
		// A window of a build in the background that is open when the stream ends ends
		// there; a synchronous one that never ended is a build that did not finish.
		case m.Gate.Mode != "background" && (w.RewriteEndNs == gateWindowOpen || w.SettleEndNs == gateWindowOpen):
			return fmt.Sprintf("window %d never ended", i+1)
		case w.RewriteEndNs < w.StartNs || w.SettleEndNs < w.RewriteEndNs || w.ReturnNs < w.StartNs:
			return fmt.Sprintf("window %d ends before it starts", i+1)
		}
	}
	return ""
}

// paceOf is the rate a build was paced at, in records per second (0: not paced), and
// the day it was paced from as the manifest records it ("" if it was not paced).
func (b *StoreGateBuild) paceOf() (rate int, fromDay string) {
	rate, _ = strconv.Atoi(describedOr(b.Manifest, PaceKey, "0"))
	return rate, b.Manifest.Describe[PaceFromDayKey]
}

// window is the days of retained history a build kept, or false if it kept no whole
// number of them.
func (t *GateTrace) window() (days int, ok bool) {
	d := time.Duration(t.KeepNs)
	if d <= 0 || d%(24*time.Hour) != 0 {
		return 0, false
	}
	return int(d / (24 * time.Hour)), true
}

// paceRefusal is why a build in the background that was paced is not one the rules
// know: it kept no whole number of days, or it was paced from another day than the one
// the rules give for its window.
func (b *StoreGateBuild) paceRefusal() string {
	rate, from := b.paceOf()
	if rate <= 0 {
		return ""
	}
	if b.Manifest.Gate.PaceStartNs <= 0 {
		return "paced, but the pacing never started"
	}
	days, ok := b.Manifest.Gate.window()
	if !ok {
		return fmt.Sprintf("paced, and keeping %s, which is not a whole number of days: the rules give the day to pace from for a window of whole days", time.Duration(b.Manifest.Gate.KeepNs))
	}
	want := PaceFromDay(days)
	got, err := strconv.ParseFloat(from, 64)
	if err != nil || got != want {
		if from == "" {
			from = "not recorded"
		}
		return fmt.Sprintf("pace from day %s, the rule gives %s for the window of %d days", from, strconv.FormatFloat(want, 'f', -1, 64), days)
	}
	return ""
}

// load is the builder's records per second outside the windows: the records of the
// batches outside every window (those of a settle included) over the time the stream took, less the rests and the
// time the builder was away in a retention.
func (g *GateTrace) load() (perSecond float64, ok bool) {
	away := int64(0)
	for _, w := range g.Windows {
		away += max(w.RewriteEndNs, w.ReturnNs) - w.StartNs
	}
	ns := g.WriteNs - g.RestNs - away
	// The builder writes beside a settle as it does outside everything, so the batches
	// of the settle are part of its load though they are not part of the control.
	records := g.Outside.Records + g.Settling.Records
	if ns <= 0 || records == 0 {
		return 0, false
	}
	return float64(records) / (float64(ns) / 1e9), true
}

// streamEnd is the end of the stream for the gate: the instant the last batch was
// written and the last retention called. A trace that does not say (a build before the
// field) took it as the time the stream took.
func (t *GateTrace) streamEnd() int64 {
	if t.StreamEndNs > 0 {
		return t.StreamEndNs
	}
	return t.WriteNs
}

// judged says whether a window is one the gate judges in a background build: one that
// starts at or after the pacing did (every window of a build that was not paced).
func (t *GateTrace) judged(w GateWindow) bool { return w.StartNs >= t.PaceStartNs }

// pacedLoad is the builder's records per second over the paced writing time of a
// background build: the records of the paced batches that are inside, settling or
// outside (not those of an unjudged window or its settle) over the time from the start
// of the pacing to the end of the stream, less the rests and the time of unjudged
// windows and their settles that falls in that span. The time of a judged window is not
// removed: the builder writes beside it. A build that was not paced has no unjudged
// window, and its span is the whole stream.
func (t *GateTrace) pacedLoad() (perSecond float64, ok bool) {
	end := t.streamEnd()
	rest := t.PacedRestNs
	if t.PaceStartNs <= 0 {
		rest = t.RestNs
	}
	ns := end - t.PaceStartNs - rest
	for _, w := range t.Windows {
		if t.judged(w) {
			continue
		}
		if hi := min(w.SettleEndNs, end); hi > t.PaceStartNs {
			ns -= hi - t.PaceStartNs
		}
	}
	records := t.Inside.Records + t.Settling.Records + t.Outside.Records
	if ns <= 0 || records == 0 {
		return 0, false
	}
	return float64(records) / (float64(ns) / 1e9), true
}

// gatePass is a window of a background build seen as a pass of the store: how long its
// rewrite took, or the least it took.
type gatePass struct {
	n      int // among the windows of the build, from 1
	w      GateWindow
	judged bool
	// rewrite is the rewrite time; if bound, it is a lower bound of it, because a later
	// retention replaced the pass or the stream ended before its rewrite did.
	rewrite int64
	bound   bool
	why     string
	// settle is the time of the settle, 0 for a pass that was not settled (replaced in
	// its rewrite, or unfinished at the end of the stream); unfinished says the settle
	// had not ended when the stream did.
	settle     int64
	unfinished bool
}

// passes are the windows of a background build as passes.
func (t *GateTrace) passes() []gatePass {
	end := t.streamEnd()
	out := make([]gatePass, 0, len(t.Windows))
	for i, w := range t.Windows {
		p := gatePass{n: i + 1, w: w, judged: t.judged(w)}
		switch {
		case w.SupersededNs > 0:
			p.rewrite, p.bound, p.why = w.SupersededNs-w.StartNs, true, "replaced by the next retention"
		case w.RewriteEndNs > end:
			p.rewrite, p.bound, p.why = end-w.StartNs, true, "not ended when the stream ended"
		default:
			p.rewrite = w.RewriteEndNs - w.StartNs
			p.settle = min(w.SettleEndNs, end) - w.RewriteEndNs
			p.unfinished = w.SettleEndNs > end
		}
		out = append(out, p)
	}
	return out
}

// judgeQ3Background judges Q3 for a build in the background: the rewrite time of each
// judged pass whose rewrite ended, and for a judged pass that was replaced or had not
// ended when the stream did, the lower bound of its rewrite. The row fails if any of
// them is over the limit, at the load the build ran at; otherwise it is not judged if
// some judged pass has only a lower bound; otherwise it passes.
func judgeQ3Background(r *gateRow, t *GateTrace, load float64, loadOK bool) {
	var done, bounded int
	var rewriteMax, boundMax int64
	for _, p := range t.passes() {
		switch {
		case !p.judged:
		case p.bound:
			bounded++
			boundMax = max(boundMax, p.rewrite)
		default:
			done++
			rewriteMax = max(rewriteMax, p.rewrite)
		}
	}
	loadText := "load not determinable"
	if loadOK {
		loadText = fmt.Sprintf("load %.0f records/s over the paced writing time", load)
	}
	value := fmt.Sprintf("longest rewrite %s over %d passes", fmtDur(rewriteMax), done)
	if bounded > 0 {
		value += fmt.Sprintf(", and %d passes with only a lower bound, the longest %s", bounded, fmtDur(boundMax))
	}
	r.value = fmt.Sprintf("%s (%s)", value, loadText)
	r.limit = fmt.Sprintf("%s with %d days kept, at a load of at least %d records/s", StoreGateQ3Max, StoreGateQ3KeepDays, StoreGateRate)
	worst := max(rewriteMax, boundMax)
	over := worst > int64(StoreGateQ3Max)
	r.pass = !over
	r.margin = float64(worst) / float64(StoreGateQ3Max)
	switch keep := time.Duration(t.KeepNs); {
	case !loadOK:
		r.judged, r.reason = false, "the load over the paced writing time cannot be worked out (no paced batch outside an unjudged window)"
	case load < StoreGateRate:
		r.judged, r.reason = false, fmt.Sprintf("the load %.0f records/s is below the %d records/s the rule is judged at: a rewrite is not shown quick by writing slowly beside it", load, StoreGateRate)
	case keep < StoreGateQ3KeepDays*24*time.Hour:
		r.judged, r.reason = false, fmt.Sprintf("the limit is for %d days kept, and this build keeps %.1f", StoreGateQ3KeepDays, keep.Hours()/24)
	case !over && bounded > 0:
		r.judged, r.reason = false, fmt.Sprintf("%d judged passes have only a lower bound of their rewrite (replaced by a later retention, or not ended when the stream ended), the longest %s, within the limit: whether they pass is not known", bounded, fmtDur(boundMax))
	}
}

// judgeBuild gives the build's verdict on each row; base is the Lroot/off build of the
// same plan, architecture and repetition (nil if there is none, with baseWhy saying why).
func judgeBuild(b, base *StoreGateBuild, baseWhy string) (rows [gateRows]gateRow, info string) {
	for i := range rows {
		rows[i].build = b
	}
	if why := b.refusal(); why != "" {
		for i := range rows {
			rows[i].reason = why
		}
		return rows, ""
	}
	m := b.Manifest
	t := m.Gate
	ni := func(i int, reason string) { rows[i].judged, rows[i].reason = false, reason }

	// A build in the background is judged on the paced part when it was paced at the
	// rule's rate; otherwise its values are reported and no row is judged.
	bg := t.Mode == "background"
	rate, fromDay := b.paceOf()
	unjudgedWhy := ""
	switch {
	case !bg:
	case rate == 0:
		unjudgedWhy = fmt.Sprintf("a build in the background is judged only when paced at %d records/s from stream day D; this one was not paced", StoreGatePaceRate)
	case rate != StoreGatePaceRate:
		unjudgedWhy = fmt.Sprintf("paced at %d records/s, not at the pre-registered offered load of %d records/s", rate, StoreGatePaceRate)
	}

	end := t.streamEnd()
	var rewriteMax, settleMax, returnMax, windowsNs, judgedWindows int64
	for _, w := range t.Windows {
		if bg && !t.judged(w) {
			continue
		}
		judgedWindows++
		// A window of a build in the background that is open when the stream ends, or
		// that a later retention replaced, ends there.
		rewriteEnd, settleEnd := w.RewriteEndNs, w.SettleEndNs
		if bg {
			rewriteEnd, settleEnd = min(rewriteEnd, end), min(settleEnd, end)
			if w.SupersededNs > 0 {
				rewriteEnd, settleEnd = min(rewriteEnd, w.SupersededNs), min(settleEnd, w.SupersededNs)
			}
		}
		rewriteMax = max(rewriteMax, rewriteEnd-w.StartNs)
		settleMax = max(settleMax, settleEnd-w.StartNs)
		returnMax = max(returnMax, w.ReturnNs-w.StartNs)
		windowsNs += rewriteEnd - w.StartNs
	}
	noInside := ""
	switch {
	case len(t.Windows) == 0:
		noInside = "no retention window (the build retained nothing, or its engine does not say how a retention spent its time)"
	case bg && judgedWindows == 0:
		noInside = "no judged window: no retention was published at or after the day the pacing started"
	case bg && t.Inside.Batches == 0:
		noInside = "no batch was written inside a judged window"
	case t.Inside.Batches == 0 && t.Mode == "sync":
		noInside = fmt.Sprintf("synchronous retention: no batch is written inside a window, the writer is blocked for all of it (the longest %s)", fmtDur(returnMax))
	case t.Inside.Batches == 0:
		noInside = "no batch was written inside a window"
	}

	byConstruction := len(t.Windows) > 0 && t.Inside.Batches == 0 && t.Mode == "sync"
	defer func() {
		for _, i := range []int{gateQ1, gateQ2, gateQ4} {
			rows[i].byConstruction = !rows[i].judged && byConstruction
		}
	}()

	// Q1: the longest batch inside a window, with the longest outside as the control.
	rows[gateQ1] = gateRow{build: b, judged: noInside == "", reason: noInside}
	if noInside == "" {
		v := t.Inside.Commit.MaxNs
		control, hasControl, ctl := "none", t.Outside.Batches > 0, t.Outside.Commit.MaxNs
		if hasControl {
			control = fmtDur(ctl)
		}
		rows[gateQ1].value = fmt.Sprintf("%s (control, longest outside windows: %s)", fmtDur(v), control)
		rows[gateQ1].limit = StoreGateQ1MaxWait.String()
		rows[gateQ1].pass = v <= int64(StoreGateQ1MaxWait)
		rows[gateQ1].margin = float64(v) / float64(StoreGateQ1MaxWait)
		// Over the limit, a build is over it only if the machine's own longest batch
		// leaves enough of the limit to tell a retention's wait from its noise. The
		// control never prevents a pass and never relaxes the limit.
		if !rows[gateQ1].pass {
			switch {
			case !hasControl:
				ni(gateQ1, "no batch outside every window and settle: without the control a wait over the limit cannot be told from the machine's noise (rerun it)")
			case ctl > int64(StoreGateQ1NoiseCeiling):
				ni(gateQ1, fmt.Sprintf("control %s above %s: the machine's own longest batch leaves too little of the limit to judge by (rerun it)", time.Duration(ctl), StoreGateQ1NoiseCeiling))
			}
		}
	}

	// Q2: the 99th percentile inside against twice the one outside, or the floor.
	rows[gateQ2] = gateRow{build: b, judged: noInside == "", reason: noInside}
	switch {
	case noInside != "":
	case t.Outside.Batches == 0:
		ni(gateQ2, "no batch outside the windows to compare with")
	default:
		// The inside is read at the top of its bucket and the outside at the bottom of
		// its: a bucket is within an eighth of the value it holds, and each reading is
		// the one that cannot let a batch pass that the true values would not.
		_, in := quantileBounds(t.Inside.Commit, 0.99)
		outLo, outHi := quantileBounds(t.Outside.Commit, 0.99)
		lim := max(StoreGateQ2Factor*outLo, int64(StoreGateQ2Floor))
		rows[gateQ2].value = fmt.Sprintf("%s (outside: from %s to %s; over %d inside batches)", fmtDur(in), fmtDur(outLo), fmtDur(outHi), t.Inside.Batches)
		rows[gateQ2].limit = fmt.Sprintf("max(%dx the outside's lower bound, %s) = %s", StoreGateQ2Factor, StoreGateQ2Floor, fmtDur(lim))
		rows[gateQ2].pass = in <= lim
		rows[gateQ2].margin = float64(in) / float64(lim)
	}

	// Q3: the longest rewrite, from the publication to its end, at the load the build ran at.
	load, loadOK := t.load()
	if bg {
		load, loadOK = t.pacedLoad()
	}
	rows[gateQ3] = gateRow{build: b, judged: true}
	switch {
	case len(t.Windows) == 0:
		ni(gateQ3, noInside)
	case bg && judgedWindows == 0:
		ni(gateQ3, noInside)
	case bg:
		judgeQ3Background(&rows[gateQ3], t, load, loadOK)
	default:
		loadText := "load not determinable"
		if loadOK {
			loadText = fmt.Sprintf("load %.0f records/s outside windows", load)
		}
		rows[gateQ3].value = fmt.Sprintf("%s (%s; settle ends %s after the publication)", fmtDur(rewriteMax), loadText, fmtDur(settleMax))
		rows[gateQ3].limit = fmt.Sprintf("%s with %d days kept, at a load of at least %d records/s", StoreGateQ3Max, StoreGateQ3KeepDays, StoreGateRate)
		rows[gateQ3].pass = rewriteMax <= int64(StoreGateQ3Max)
		rows[gateQ3].margin = float64(rewriteMax) / float64(StoreGateQ3Max)
		switch keep := time.Duration(t.KeepNs); {
		case !loadOK:
			ni(gateQ3, "the load outside the windows cannot be worked out (no batch outside them)")
		case load < StoreGateRate:
			ni(gateQ3, fmt.Sprintf("the load %.0f records/s is below the %d records/s the rule is judged at: a rewrite is not shown quick by writing slowly beside it", load, StoreGateRate))
		case keep < StoreGateQ3KeepDays*24*time.Hour:
			ni(gateQ3, fmt.Sprintf("the limit is for %d days kept, and this build keeps %.1f", StoreGateQ3KeepDays, keep.Hours()/24))
		}
	}

	// Q4: the records written inside windows over the windows' total time.
	rows[gateQ4] = gateRow{build: b, judged: noInside == "", reason: noInside}
	switch {
	case noInside != "":
	case windowsNs <= 0:
		ni(gateQ4, "the windows have no length")
	default:
		// A batch that overlaps a window only in part counts all its records, so the
		// time of its part outside the window is divided by too.
		rate := float64(t.Inside.Records) / (float64(windowsNs+t.Inside.SpillNs) / 1e9)
		rows[gateQ4].value = fmt.Sprintf("%.0f records/s", rate)
		rows[gateQ4].limit = fmt.Sprintf("at least %d records/s", StoreGateRate)
		rows[gateQ4].pass = rate >= StoreGateRate
		rows[gateQ4].margin = StoreGateRate / math.Max(rate, 1e-9)
	}

	// Q5: the peak live heap against Lroot/off's plus the margin, and the peak resident set.
	rows[gateQ5Heap] = gateRow{build: b, judged: true}
	switch {
	case b.Metrics.HeapLiveSamples == 0:
		ni(gateQ5Heap, "no live heap samples (build with -metrics-every)")
	case m.Candidate == "Lroot/off":
		rows[gateQ5Heap].baseline, rows[gateQ5Heap].pass = true, true
		rows[gateQ5Heap].value = fmtMB(b.Metrics.HeapLivePeak)
		rows[gateQ5Heap].limit = "the baseline"
	case base == nil:
		ni(gateQ5Heap, baseWhy)
	case base.refusal() != "":
		ni(gateQ5Heap, "the Lroot/off baseline cannot be used: "+base.refusal())
	case base.Metrics.HeapLiveSamples == 0:
		ni(gateQ5Heap, "Lroot/off has no live heap samples (build with -metrics-every)")
	default:
		if why := sameSettings(m, base.Manifest); why != "" {
			ni(gateQ5Heap, why)
			break
		}
		lim := base.Metrics.HeapLivePeak + StoreGateQ5HeapMargin
		rows[gateQ5Heap].value = fmtMB(b.Metrics.HeapLivePeak)
		rows[gateQ5Heap].limit = fmt.Sprintf("%s (Lroot/off's %s + %d MB)", fmtMB(lim), fmtMB(base.Metrics.HeapLivePeak), StoreGateQ5HeapMargin/1_000_000)
		rows[gateQ5Heap].pass = b.Metrics.HeapLivePeak <= lim
		rows[gateQ5Heap].margin = float64(b.Metrics.HeapLivePeak) / float64(lim)
	}
	rows[gateQ5RSS] = gateRow{build: b, judged: t.PeakRSS > 0, reason: "the peak resident set was not read"}
	if t.PeakRSS > 0 {
		rows[gateQ5RSS].reason = ""
		rows[gateQ5RSS].value = fmtMB(t.PeakRSS)
		rows[gateQ5RSS].limit = fmtMB(StoreGateRSSCeiling)
		rows[gateQ5RSS].pass = t.PeakRSS <= StoreGateRSSCeiling
		rows[gateQ5RSS].margin = float64(t.PeakRSS) / float64(int64(StoreGateRSSCeiling))
	}

	if unjudgedWhy != "" {
		for i := range rows {
			rows[i].judged, rows[i].reason, rows[i].byConstruction = false, unjudgedWhy, false
		}
	}

	touch := "not recorded by this store"
	if n, ok := m.Counters["retain.touch_rewrites"]; ok {
		touch = strconv.FormatInt(n, 10)
	}
	maxima := func(k string) string {
		if v, ok := t.Maxima[k]; ok {
			return strconv.FormatInt(v, 10)
		}
		return "not recorded"
	}
	chunk := maxima("retain.chunk_hold_ns")
	if v, ok := t.Maxima["retain.chunk_hold_ns"]; ok {
		chunk = fmtDur(v)
	}
	loadText := "not determinable"
	if loadOK {
		loadText = fmt.Sprintf("%.0f records/s", load)
	}
	settling := "no batch"
	if st := t.Settling; st.Batches > 0 {
		_, p99 := quantileBounds(st.Commit, 0.99)
		settling = fmt.Sprintf("%d batches, longest %s, 99th percentile %s", st.Batches, fmtDur(st.Commit.MaxNs), fmtDur(p99))
	}
	info = fmt.Sprintf("%s: mode %s; %d windows, longest rewrite %s, longest settle end %s, longest call %s; retain.max_prefix_records max %s, retain.chunk_hold_ns max %s, retain.touch_rewrites %s; load outside windows %s; settling (inside no window): %s; tombstones: peak %d sampled, %d at the end of the stream, %d after the compaction (reported, not judged); commits synced: %s",
		b.label(), t.Mode, len(t.Windows), fmtDur(rewriteMax), fmtDur(settleMax), fmtDur(returnMax),
		maxima("retain.max_prefix_records"), chunk, touch, loadText, settling,
		b.Metrics.TombstonesPeak, m.StatsBuilt["tombstones"], m.StatsCompacted["tombstones"], describedOr(m, SyncKey, "false"))
	info += chunkDetail(b)
	info += judgedPartDetail(b, rows)
	if bg {
		info += backgroundDetail(b, rate, fromDay)
	}
	return rows, info
}

// judgedPartDetail is what the report says of a build's retention flush threshold and,
// apart, of the part of the build the gate judges and of the rest: the spread of each
// sample of the chunks (count, least, 50th, 90th and 99th percentiles, largest and mean),
// the exact share of the chunks whose opening took more than [RetainChunkC1OpenBound],
// the flushes, the write stalls and the peak sublevels of level 0. For a build in the
// background other than the Lroot/off baseline it adds the retention-chunk criteria,
// each met or not. All of it is reported and not judged: no verdict depends on it.
func judgedPartDetail(b *StoreGateBuild, rows [gateRows]gateRow) string {
	m := b.Manifest
	t := m.Gate
	var sb strings.Builder
	fmt.Fprintf(&sb, "\n    %s %s", RetainFlushDelsKey, describedOr(m, RetainFlushDelsKey, "not recorded"))
	if t.JudgedChunks == nil && t.JudgedCounts == nil && !t.StallsRead {
		sb.WriteString("; the figures of the judged part not recorded (a build before they were)")
		return sb.String()
	}
	passes := 0
	for _, w := range t.Windows {
		if t.judged(w) {
			passes++
		}
	}
	stalls := "not recorded"
	if t.StallsRead {
		stalls = strconv.FormatInt(t.StallsAtStreamEnd-t.StallsAtPaceStart, 10)
	}
	l0 := "not recorded"
	if b.Metrics.JudgedLines > 0 {
		l0 = strconv.FormatInt(b.Metrics.JudgedL0SublevelsPeak, 10)
	}
	fmt.Fprintf(&sb, "\n    the judged part, the passes published from the start of the pacing (of the stream, unpaced) and the stream's time from then (reported, not judged): %d of %d passes; chunks: %s; flushes: %s; write stalls %s; level 0 sublevels: peak %s over %d metrics lines (a peak between two lines is missed)",
		passes, len(t.Windows), partChunks(t.JudgedChunks), partCounts(t.JudgedCounts, nil), stalls, l0, b.Metrics.JudgedLines)
	restStalls := "not recorded"
	if t.StallsRead {
		restStalls = strconv.FormatInt(t.StallsAtPaceStart, 10)
	}
	restL0 := "not recorded"
	if n := b.Metrics.Lines - b.Metrics.JudgedLines; n > 0 {
		restL0 = fmt.Sprintf("%d over %d metrics lines", b.Metrics.UnjudgedL0SublevelsPeak, n)
	}
	fmt.Fprintf(&sb, "\n    the rest of the build, unpaced days and unjudged windows, and after the stream (reported, not judged): %d passes; chunks: %s; flushes: %s; write stalls before the pacing %s; level 0 sublevels: peak %s",
		len(t.Windows)-passes, partChunks(t.UnjudgedChunks), partCounts(t.JudgedCounts, m.Counters), restStalls, restL0)
	if t.Mode == "background" && m.Candidate != "Lroot/off" {
		met, parts := retainChunkCriteria(t, rows)
		verdict := "the build meets the criteria"
		if !met {
			verdict = "the build does not meet the criteria"
		}
		fmt.Fprintf(&sb, "\n    retention-chunk criteria (registered 2026-10-12; reported, not rows of the gate): %s; %s", strings.Join(parts, "; "), verdict)
	}
	return sb.String()
}

// partChunks is the spread of each sample of the chunks of one part of a build.
func partChunks(spreads map[string]GateSpread) string {
	if spreads == nil {
		return "not recorded"
	}
	var parts []string
	for _, name := range []string{"retain.chunk_open_ns", "retain.chunk_hold_ns", "retain.chunk_prefixes"} {
		sp := spreads[name]
		if sp.Count == 0 {
			parts = append(parts, name+" none")
			continue
		}
		show := func(v int64) string { return strconv.FormatInt(v, 10) }
		mean := fmt.Sprintf("%.1f", sp.mean())
		if strings.HasSuffix(name, "_ns") {
			show, mean = fmtDur, fmtDur(int64(math.Round(sp.mean())))
		}
		s := fmt.Sprintf("%s count %d, min %s, p50 %s, p90 %s, p99 %s, max %s, mean %s", name, sp.Count, show(sp.Min), show(sp.P50), show(sp.P90), show(sp.P99), show(sp.Max), mean)
		if name == "retain.chunk_open_ns" {
			s += fmt.Sprintf(", over %s: %d of %d (%.2f%%)", RetainChunkC1OpenBound, sp.Over, sp.Count, 100*float64(sp.Over)/float64(sp.Count))
		}
		parts = append(parts, s)
	}
	return strings.Join(parts, ", ")
}

// partCounts is the flushes of the judged part, from counts; with total, the build's
// own counts, it is those of the rest of the build instead.
func partCounts(judged, total map[string]int64) string {
	if judged == nil {
		return "not recorded"
	}
	var parts []string
	for _, name := range gateFlushCounts {
		n := judged[name]
		if total != nil {
			n = total[name] - n
		}
		parts = append(parts, fmt.Sprintf("%s %d", name, n))
	}
	return strings.Join(parts, ", ")
}

// retainChunkCriteria says whether a build meets the retention-chunk criteria, and
// how each one went. A build with no judged chunk meets none of C1 to C3, and one that
// did not record its judged stalls or flushes does not meet C4 or C5.
func retainChunkCriteria(t *GateTrace, rows [gateRows]gateRow) (bool, []string) {
	all := true
	say := func(name string, ok bool, format string, args ...any) string {
		all = all && ok
		state := "met"
		if !ok {
			state = "not met"
		}
		return fmt.Sprintf("%s %s (%s)", name, state, fmt.Sprintf(format, args...))
	}
	var parts []string
	open, hold, prefixes := t.JudgedChunks["retain.chunk_open_ns"], t.JudgedChunks["retain.chunk_hold_ns"], t.JudgedChunks["retain.chunk_prefixes"]
	if open.Count == 0 {
		parts = append(parts, say("C1", false, "no judged chunk"))
	} else {
		// An exact count over the bound, not a quantile: at most the share of the chunks.
		parts = append(parts, say("C1", open.Over*100 <= RetainChunkC1MaxPercent*open.Count,
			"%d of %d judged chunks opened in more than %s, at most %d%%", open.Over, open.Count, RetainChunkC1OpenBound, RetainChunkC1MaxPercent))
	}
	if hold.Count == 0 {
		parts = append(parts, say("C2", false, "no judged chunk"))
	} else {
		parts = append(parts, say("C2", hold.P99 <= int64(RetainChunkC2MaxHold),
			"the 99th percentile hold, at the top of its bucket, %s, at most %s", fmtDur(hold.P99), RetainChunkC2MaxHold))
	}
	if prefixes.Count == 0 {
		parts = append(parts, say("C3", false, "no judged chunk"))
	} else {
		parts = append(parts, say("C3", prefixes.P50Low >= RetainChunkC3MinPrefixes,
			"the median prefixes, at the bottom of its bucket, %d, at least %d", prefixes.P50Low, RetainChunkC3MinPrefixes))
	}
	if !t.StallsRead {
		parts = append(parts, say("C4", false, "the judged part's write stalls were not recorded"))
	} else {
		n := t.StallsAtStreamEnd - t.StallsAtPaceStart
		parts = append(parts, say("C4", n == 0, "%d write stalls in the judged part, none allowed", n))
	}
	if t.JudgedCounts == nil {
		parts = append(parts, say("C5", false, "the judged part's flushes were not recorded"))
	} else {
		d, e := t.JudgedCounts["retain.flush_deferred"], t.JudgedCounts["retain.flush_errors"]
		parts = append(parts, say("C5", d == 0 && e == 0, "retain.flush_deferred %d and retain.flush_errors %d in the judged part, none allowed", d, e))
	}
	var failed []string
	for _, i := range []int{gateQ1, gateQ2, gateQ4} {
		if r := rows[i]; r.judged && !r.pass {
			failed = append(failed, gateRowNames[i][:2])
		}
	}
	if len(failed) == 0 {
		parts = append(parts, say("Q1, Q2 and Q4", true, "each passes or is not judged"))
	} else {
		parts = append(parts, say("Q1, Q2 and Q4", false, "over: %s", strings.Join(failed, ", ")))
	}
	return all, parts
}

// chunkDetail is what the report says of the chunks of a build's retentions and of
// its database while it was written: the spread of each sample of a chunk over the
// chunks of all its passes pooled, the last chunk of each pass left out; the least
// prefixes a chunk rewrote, which is 1 if a pass collapsed to one prefix per chunk
// even for a while; the flushes the passes started, put off and could not start; a
// lower bound of the write stalls, summed over the openings of the database; and the
// peak sublevels of level 0. All of it is of the whole build, not only of the part
// the gate judges, and is reported and not judged.
func chunkDetail(b *StoreGateBuild) string {
	t := b.Manifest.Gate
	var parts []string
	for _, name := range []string{"retain.chunk_prefixes", "retain.chunk_open_ns", "retain.chunk_hold_ns"} {
		sp, ok := t.Chunks[name]
		if !ok {
			parts = append(parts, name+" not recorded")
			continue
		}
		if sp.Count == 0 {
			parts = append(parts, name+" none (every chunk was the last of its pass)")
			continue
		}
		show := func(v int64) string { return strconv.FormatInt(v, 10) }
		if strings.HasSuffix(name, "_ns") {
			show = fmtDur
		}
		parts = append(parts, fmt.Sprintf("%s min %s, p50 %s, p99 %s, max %s over %d", name, show(sp.Min), show(sp.P50), show(sp.P99), show(sp.Max), sp.Count))
	}
	collapse := "not recorded"
	if sp, ok := t.Chunks["retain.chunk_prefixes"]; ok && sp.Count == 0 {
		collapse = "none counted"
	} else if ok {
		collapse = strconv.FormatInt(sp.Min, 10)
		if sp.Min <= 1 {
			collapse += " (a chunk that was not its pass's last rewrote one prefix)"
		}
	}
	// A store that samples its chunks counts each flush it starts, puts off or cannot
	// start, so a count it does not report is none; an older one counted none of them.
	flushes := "not recorded"
	if t.Chunks != nil {
		var fs []string
		for _, name := range []string{"retain.flushes", "retain.flush_deferred", "retain.flush_errors"} {
			fs = append(fs, fmt.Sprintf("%s %d", name, b.Manifest.Counters[name]))
		}
		flushes = strings.Join(fs, ", ")
	}
	stalls := "not recorded"
	if b.Metrics.WriteStallsRead {
		stalls = "at least " + strconv.FormatInt(b.Metrics.WriteStalls, 10)
	}
	l0 := "not recorded"
	if b.Metrics.Lines > 0 {
		l0 = strconv.FormatInt(b.Metrics.L0SublevelsPeak, 10)
	}
	return fmt.Sprintf("; the whole build, not only its judged part (reported, not judged): chunks of all passes pooled, each pass's last left out: %s; least prefixes in a chunk %s; flushes: %s; write stalls %s, summed over the openings of the database; level 0 sublevels: peak %s sampled",
		strings.Join(parts, ", "), collapse, flushes, stalls, l0)
}

// backgroundDetail is what the report says of a build in the background besides its
// verdicts: its settings, how its batches were divided, and each pass with the time of
// its rewrite (or the least it took), the time of its settle and whether the settle
// reached its deadline.
func backgroundDetail(b *StoreGateBuild, rate int, fromDay string) string {
	t := b.Manifest.Gate
	if fromDay == "" {
		fromDay = "-"
	}
	var sb strings.Builder
	fmt.Fprintf(&sb, "\n    paced at %d records/s from day %s (pacing started %s into the build, the stream ended at %s); batches: %d unpaced, %d excluded (beside an unjudged window or its settle), %d inside, %d settling, %d outside; Q2 is taken over %d inside batches",
		rate, fromDay, fmtDur(t.PaceStartNs), fmtDur(t.streamEnd()),
		t.Unpaced.Batches, t.Excluded.Batches, t.Inside.Batches, t.Settling.Batches, t.Outside.Batches, t.Inside.Batches)
	var bounds []string
	for _, p := range t.passes() {
		kind := "judged"
		if !p.judged {
			kind = "before the day the pacing started, not judged"
		}
		rewrite := fmtDur(p.rewrite)
		if p.bound {
			rewrite = fmt.Sprintf("at least %s (%s)", rewrite, p.why)
			if p.judged {
				bounds = append(bounds, fmt.Sprintf("pass %d: at least %s", p.n, fmtDur(p.rewrite)))
			}
		}
		settle, hit := "none (the pass was not settled)", "-"
		if !p.bound {
			settle = fmtDur(p.settle)
			if p.unfinished {
				settle = "not ended when the stream ended (" + settle + " so far)"
			}
			hit = "no"
			if p.w.SettleDeadlineHit {
				hit = "yes"
			}
		}
		fmt.Fprintf(&sb, "\n    pass %d (%s): published at %s, rewrite %s, settle %s, settle reached its deadline: %s", p.n, kind, fmtDur(p.w.StartNs), rewrite, settle, hit)
	}
	if len(bounds) > 0 {
		fmt.Fprintf(&sb, "\n    %d judged passes have only a lower bound of their rewrite: %s", len(bounds), strings.Join(bounds, "; "))
	}
	return sb.String()
}

// sameSettings returns why the live heaps of two builds cannot be compared: their GC
// target, their memory limit or their metrics interval differ (the peak of a sample
// depends on how often it is taken) or, for the target, was not recorded; or they differ
// in the retention mode, the sync setting, the pacing rate, the day the pacing started,
// the retention flush threshold or the table shape of Pebble, which change how the store
// ran. A key a manifest lacks is read as its default, except the threshold, which is then
// not recorded.
func sameSettings(a, b *Manifest) string {
	ga, gb := a.Describe[GoGCKey], b.Describe[GoGCKey]
	switch {
	case ga == "" || gb == "":
		return fmt.Sprintf("%s is not recorded in both manifests: the live heaps of builds under settings that are not known to be the same are not compared", GoGCKey)
	case ga != gb:
		return fmt.Sprintf("%s differs (%s and %s): builds under different settings are not compared", GoGCKey, ga, gb)
	}
	if la, lb := describedOr(a, GoMemoryLimitKey, "none"), describedOr(b, GoMemoryLimitKey, "none"); la != lb {
		return fmt.Sprintf("%s differs (%s and %s): builds under different settings are not compared", GoMemoryLimitKey, la, lb)
	}
	if ia, ib := describedOr(a, MetricsKey, "0s"), describedOr(b, MetricsKey, "0s"); ia != ib {
		return fmt.Sprintf("%s differs (%s and %s): the sampled peaks of builds sampled at different intervals are not compared", MetricsKey, ia, ib)
	}
	for _, k := range storeGateSettingKeys {
		if x, y := k.of(a), k.of(b); x != y {
			return fmt.Sprintf("%s differs (%q and %q): builds under different settings are not compared", k.key, x, y)
		}
	}
	return ""
}

// storeGateSettingKeys are the settings of a build that a verdict's repetitions must
// share and that its Q5 baseline must share with it, with the value a manifest that
// lacks the key stands for.
var storeGateSettingKeys = []struct {
	key string
	of  func(*Manifest) string
}{
	{RetentionModeKey, func(m *Manifest) string {
		if v := m.Describe[RetentionModeKey]; v != "" {
			return v
		}
		if m.Gate != nil {
			return m.Gate.Mode
		}
		return ""
	}},
	{SyncKey, func(m *Manifest) string { return describedOr(m, SyncKey, "false") }},
	{PaceKey, func(m *Manifest) string { return describedOr(m, PaceKey, "0") }},
	{PaceFromDayKey, func(m *Manifest) string { return m.Describe[PaceFromDayKey] }},
	// A manifest without the threshold was built before it was recorded: it is not read
	// as the default, so it is neither pooled with nor compared to one that has it.
	{RetainFlushDelsKey, func(m *Manifest) string { return describedOr(m, RetainFlushDelsKey, "not recorded") }},
	// A manifest without the table shape was built with Pebble's own rule, the product's
	// today: builds of different shapes are neither pooled nor compared (Q5).
	{ShapeKey, func(m *Manifest) string { return describedOr(m, ShapeKey, "none") }},
	// A build with serve's options (the boot key, L0 kept, L1 behind) is another store
	// from one without, which every manifest that lacks the key is.
	{ServeOptionsKey, func(m *Manifest) string { return describedOr(m, ServeOptionsKey, "false") }},
}

// JudgeStoreGate judges the builds, each candidate on each architecture, from its
// repetitions: the worst of the repetitions that could be judged is the row.
func JudgeStoreGate(builds []*StoreGateBuild) *StoreGateReport {
	rep := &StoreGateReport{RulesText: StoreGateRulesText(), RulesDigest: StoreGateRulesDigest()}
	if err := CheckStoreGateSettings(builds); err != nil {
		rep.Refusal = err.Error()
		return rep
	}
	bs := slices.Clone(builds)
	sort.SliceStable(bs, func(i, j int) bool {
		a, b := bs[i], bs[j]
		if a.arch() != b.arch() {
			return a.arch() < b.arch()
		}
		if a.Manifest.Candidate != b.Manifest.Candidate {
			return a.Manifest.Candidate < b.Manifest.Candidate
		}
		if a.rep() != b.rep() {
			return a.rep() < b.rep()
		}
		return a.Dir < b.Dir
	})

	type baseKey struct {
		arch, plan string
		rep        int
	}
	bases := map[baseKey][]*StoreGateBuild{}
	for _, b := range bs {
		if b.Manifest.Candidate == "Lroot/off" {
			k := baseKey{b.arch(), b.Manifest.PlanDigest, b.rep()}
			bases[k] = append(bases[k], b)
		}
	}

	type group struct{ cand, arch string }
	var order []group
	groups := map[group][][gateRows]gateRow{}
	for _, b := range bs {
		var base *StoreGateBuild
		baseWhy := "no Lroot/off build of the same plan, architecture and repetition to compare with"
		switch found := bases[baseKey{b.arch(), b.Manifest.PlanDigest, b.rep()}]; len(found) {
		case 1:
			base = found[0]
		case 0:
		default:
			baseWhy = "more than one Lroot/off build of the same plan, architecture and repetition: which is the baseline?"
		}
		rows, info := judgeBuild(b, base, baseWhy)
		if info != "" {
			rep.Info = append(rep.Info, info)
		}
		if mode := b.Manifest.Describe[RetentionModeKey]; mode != "" && !slices.Contains(rep.Modes, mode) {
			rep.Modes = append(rep.Modes, mode)
		}
		g := group{b.Manifest.Candidate, b.arch()}
		if _, ok := groups[g]; !ok {
			order = append(order, g)
		}
		groups[g] = append(groups[g], rows)
		for i, r := range rows {
			if !r.judged {
				if !r.byConstruction {
					rep.Unexpected++
				}
				rep.NotJudged = append(rep.NotJudged, fmt.Sprintf("%s: %s: %s", b.label(), gateRowNames[i], r.reason))
			}
		}
	}

	for _, g := range order {
		reps := groups[g]
		for i := 0; i < gateRows; i++ {
			var worst *gateRow
			judged, n := 0, len(reps)
			for k := range reps {
				r := &reps[k][i]
				if !r.judged {
					continue
				}
				judged++
				if worst == nil || r.margin > worst.margin || (!r.pass && worst.pass) {
					worst = r
				}
			}
			row := StoreGateRow{Candidate: g.cand, Arch: g.arch, Gate: gateRowNames[i], State: GateNotJudged}
			if worst == nil {
				// Not judged: show the value of the first repetition that has one.
				for k := range reps {
					if reps[k][i].value != "" {
						row.Value, row.Limit = reps[k][i].value, reps[k][i].limit
						break
					}
				}
				if row.Value == "" {
					row.Value = "-"
				}
				if row.Limit == "" {
					row.Limit = "-"
				}
				rep.Rows = append(rep.Rows, row)
				continue
			}
			row.Value, row.Limit = worst.value, worst.limit
			switch {
			case worst.baseline:
				row.State = GateBaseline
			case worst.pass:
				row.State = GateOK
			default:
				row.State = GateOver
				rep.Over++
			}
			if n > 1 {
				row.Value += fmt.Sprintf(" [worst of %d judged of %d]", judged, n)
			}
			row.Partial = judged < n
			rep.Rows = append(rep.Rows, row)
		}
	}
	return rep
}

// retentionModeOf is the retention mode a build recorded: the manifest's description,
// or the gate's own record if the description lacks it, or "" for a build that has none
// (a candidate not built through the root store).
func (b *StoreGateBuild) retentionModeOf() string { return storeGateSettingKeys[0].of(b.Manifest) }

// CheckStoreGateSettings returns an error naming the values and the builds if the
// repetitions of one candidate on one architecture (a verdict's pool) were made under
// different retention modes, sync settings, pacing rates, pacing starts, retention
// flush thresholds or table shapes, or did not come from one place (see poolOrigins):
// the gate judges a pool as one set, and the worst of its repetitions is the row. A build of another
// candidate, such as the Lroot/off baseline, is a pool of its own; one that differs from
// the baseline in a setting gets its Q5 not judged instead.
func CheckStoreGateSettings(builds []*StoreGateBuild) error {
	type pool struct{ cand, arch string }
	pools := map[pool][]*StoreGateBuild{}
	var order []pool
	for _, b := range builds {
		if b.retentionModeOf() == "" {
			continue
		}
		k := pool{b.Manifest.Candidate, b.arch()}
		if _, ok := pools[k]; !ok {
			order = append(order, k)
		}
		pools[k] = append(pools[k], b)
	}
	var problems []string
	for _, k := range order {
		for _, setting := range storeGateSettingKeys {
			byValue := map[string][]string{}
			for _, b := range pools[k] {
				v := setting.of(b.Manifest)
				byValue[v] = append(byValue[v], b.label())
			}
			if len(byValue) < 2 {
				continue
			}
			var parts []string
			for _, v := range slices.Sorted(maps.Keys(byValue)) {
				parts = append(parts, fmt.Sprintf("%s=%q: %s", setting.key, v, strings.Join(byValue[v], ", ")))
			}
			problems = append(problems, fmt.Sprintf("the builds of %s on %s were made under different %s (%s)", k.cand, k.arch, setting.key, strings.Join(parts, "; ")))
		}
		problems = append(problems, poolOrigins(k.cand, k.arch, pools[k])...)
	}
	if len(problems) == 0 {
		return nil
	}
	return fmt.Errorf("runner: %s: the store gate judges the repetitions of a candidate as one set, and builds made under different settings, from different plans, or in different workflow runs or attempts are not repetitions of one another; give it the builds of one setting, one plan and one first attempt of one workflow run (or local builds only)", strings.Join(problems, "; "))
}

// poolOrigins is why the builds of one pool did not come from one place: they were
// built from different plans; some were made by the bench workflow (they have its job
// file) and some locally; the workflow's were made in more than one run; or one was
// made by a re-run of its job. A re-run of some of an arm's jobs is not a redispatch,
// and the builds of two dispatches are reported apart, never pooled.
func poolOrigins(cand, arch string, pool []*StoreGateBuild) []string {
	var problems []string
	byPlan, byRun := map[string][]string{}, map[string][]string{}
	var ci, local []string
	for _, b := range pool {
		byPlan[b.Manifest.PlanDigest] = append(byPlan[b.Manifest.PlanDigest], b.label())
		if b.Job == nil {
			local = append(local, b.label())
			continue
		}
		ci = append(ci, b.label())
		byRun[b.Job.RunID] = append(byRun[b.Job.RunID], b.label())
		if b.Job.RunAttempt != "1" {
			problems = append(problems, fmt.Sprintf("%s was made by attempt %q of its workflow run, and only a first attempt is judged: a re-run of some of an arm's jobs is not a redispatch", b.label(), b.Job.RunAttempt))
		}
	}
	listed := func(by map[string][]string, what string) string {
		var parts []string
		for _, v := range slices.Sorted(maps.Keys(by)) {
			parts = append(parts, fmt.Sprintf("%s %q: %s", what, v, strings.Join(by[v], ", ")))
		}
		return strings.Join(parts, "; ")
	}
	if len(byPlan) > 1 {
		problems = append(problems, fmt.Sprintf("the builds of %s on %s were made from different plans (%s)", cand, arch, listed(byPlan, "plan")))
	}
	if len(ci) > 0 && len(local) > 0 {
		problems = append(problems, fmt.Sprintf("the builds of %s on %s mix builds of the bench workflow (%s) and local builds (%s)", cand, arch, strings.Join(ci, ", "), strings.Join(local, ", ")))
	}
	if len(byRun) > 1 {
		problems = append(problems, fmt.Sprintf("the builds of %s on %s were made in different workflow runs (%s)", cand, arch, listed(byRun, "run")))
	}
	return problems
}

// CheckStoreGateCommit refuses builds that are not all of one commit: with commit given
// (40 lowercase hexadecimal digits), a build whose manifest records another revision,
// or whose job file, if it has one, another commit; without it, any build in the
// background mode that was paced, since a series of them is judged only as the builds of
// one frozen commit. A build without a job file (a local build) is checked by its
// manifest's revision alone.
func CheckStoreGateCommit(builds []*StoreGateBuild, commit string) error {
	if commit == "" {
		var paced []string
		for _, b := range builds {
			if rate, _ := b.paceOf(); rate > 0 && b.retentionModeOf() == "background" {
				paced = append(paced, b.label())
			}
		}
		if len(paced) > 0 {
			return fmt.Errorf("runner: paced builds in the background (%s) are judged only with -commit, the frozen commit every one of them must be built from", strings.Join(paced, ", "))
		}
		return nil
	}
	if !hex40.MatchString(commit) {
		return errors.New("runner: -commit: not 40 lowercase hexadecimal digits")
	}
	var problems []string
	for _, b := range builds {
		if r := b.Manifest.Build.Revision; r != commit {
			problems = append(problems, fmt.Sprintf("%s was built from revision %q", b.label(), r))
		}
		if b.Job != nil && b.Job.SHA != commit {
			problems = append(problems, fmt.Sprintf("%s has a job file of commit %q", b.label(), b.Job.SHA))
		}
	}
	if len(problems) == 0 {
		return nil
	}
	return fmt.Errorf("runner: the builds are judged as the builds of commit %s, and %s", commit, strings.Join(problems, "; "))
}

// WriteStoreGate prints the report: the rules and their digest, the retention modes, one
// line per row, what the builds reported that is not judged, and what was not judged.
func WriteStoreGate(w io.Writer, rep *StoreGateReport) {
	fmt.Fprintf(w, "store retention gate; rules digest %s\n%s", rep.RulesDigest, rep.RulesText)
	if rep.Refusal != "" {
		fmt.Fprintf(w, "NOT JUDGED: %s\n", rep.Refusal)
		return
	}
	modes := "none"
	if len(rep.Modes) > 0 {
		modes = strings.Join(rep.Modes, ", ")
	}
	fmt.Fprintf(w, "retention mode: %s\n", modes)
	if slices.Contains(rep.Modes, "sync") {
		fmt.Fprintln(w, "synchronous retention: the writer is blocked for the whole of each window, so no batch is written inside one and Q1, Q2 and Q4 have nothing to judge")
	}
	if slices.Contains(rep.Modes, "background") {
		fmt.Fprintln(w, StoreGateQ3Note)
	}
	fmt.Fprintln(w)
	tw := tabwriter.NewWriter(w, 2, 0, 2, ' ', 0)
	for _, r := range rep.Rows {
		state := r.State
		if r.Partial {
			state += " (partial)"
		}
		fmt.Fprintf(tw, "%s\t%s\t%s\t%s\tlimit %s\t%s\n", r.Candidate, r.Arch, r.Gate, r.Value, r.Limit, state)
	}
	_ = tw.Flush()
	if len(rep.Info) > 0 {
		fmt.Fprintln(w, "\nreported, not judged:")
		for _, s := range rep.Info {
			fmt.Fprintf(w, "  %s\n", s)
		}
	}
	if len(rep.NotJudged) > 0 {
		fmt.Fprintln(w, "\nNOT JUDGED:")
		for _, s := range rep.NotJudged {
			fmt.Fprintf(w, "  - %s\n", s)
		}
	}
}
