package pebblestore

import (
	"bytes"
	"context"
	"fmt"
	"math"
	"time"

	"github.com/lotannauo/toposhift/internal/identity"
	"github.com/lotannauo/toposhift/internal/store"
	"github.com/lotannauo/toposhift/internal/store/pebblekv"
)

// # The seeded window read
//
// WindowSince answers each fingerprint with two walks of its prefix on the one
// iterator of the call: the window, exactly as Window and EntityWindow read it,
// and the seed walk, which is [reader.decide] started where the state just before
// the window's from is decided.
//
// The seed walk starts at stampKey(prefix, from, kindBaseline). In key order
// that is after every record at from (records sort before the stamps of their
// instant, and a stamp's sequence slot sorts after every Seq), after a checkpoint
// at from (kind 1 sorts before kind 2), and at a baseline at from. So it sees the
// records strictly before from, newest first, then the checkpoints and the
// baseline older than them, and a baseline at from itself:
//
//   - a checkpoint at c summarizes the records strictly before c and lists the
//     references alive at c. One at c <= from-1ns holds every reference decided
//     before it that is alive at from-1ns. One at exactly from does not: a
//     reference whose deadline is from itself is alive at from-1ns and not at
//     from, so it is not listed, and the walk steps over such a checkpoint to an
//     older one;
//   - the baseline is keyed at the horizon h and lists the references alive at h.
//     A window from h needs it, and decide("at or before from-1ns"), which seeks
//     past every key at from, would miss it and walk into the history the
//     retention deleted. Starting at the baseline's own key is what finds it. At
//     from == h the state the seeds stand for is the one at h (see
//     [store.Store.WindowSince]), which is what the baseline holds.
//
// Each decided reference is a seed if its value holds at the seed instant
// (from-1ns, or h when from is h). Below the horizon, in a prefix a Background pass
// has not rewritten yet, the walk meets the old records, checkpoints and baselines;
// each is judged the way the baseline the pass will write would judge it, and a
// payload before the horizon is never given, so the answer does not depend on the
// pass. A key of a kind parseKey does not know fails the read, as it fails every
// other read; a kind it knows and the walk does not use is stepped over.
//
// With [store.SinceOptions].Payloads, a seed at or after the horizon that was
// decided by a checkpoint entry, which keeps no payload, has its payload read from
// its own record, which a retention never removes: one seek to the record's key
// (its instant and Seq are in the entry), counted as "read.seed_payload_steps".
//
// # The seed walk's budget (SW1)
//
// Fixed 2026-10-15, before anything was measured, and changed only by a new dated
// paragraph. G is the policy gap of a prefix, the records written between two
// checkpoints under [DefaultCheckpoints]: max(KMin, Alpha x checkpoint bytes / mean
// record bytes). At the reference cell (the reverse prefix of a node with 110 live
// pods, one scheduled_on reference each, written by the k8sobjects translator) the
// stamp codec gives seedWalkReferenceGap (TestTheSeedWalkBudgetIsTheReferenceCellsGap
// computes it from the codec). A seed walk steps over at most seedWalkBudgetRecords
// records (3 x G: the gap, the records the 5-minute lag leaves after the newest
// checkpoint, and the holes a late record makes, about 2.9 x G at worst) and decodes
// at most seedWalkBudgetStamps stamp. It applies only to a token at or above the W of
// the checkpoint the walk uses (an older token walks to an older checkpoint), and to
// a prefix the retention pass has rewritten (below a pending Background pass the walk
// crosses the old records). A host's own prefix under a boot key has no checkpoints,
// so its walk is its history since the horizon: listed, not budgeted. The payload
// seeks are counted apart and not budgeted. Each walk is sampled as
// "read.seed_records_stepped" and "read.seed_stamps_decoded"; the budget is judged on
// those by the composed-query measurement, not by this package.
const (
	// G at the reference cell: a checkpoint of 7,831 bytes over a mean record of
	// 213 (the edge payload is 152 bytes), so 2 x 7831 / 213 = 73, above KMin.
	seedWalkReferenceGap  = 73
	seedWalkBudgetRecords = 3 * seedWalkReferenceGap
	seedWalkBudgetStamps  = 1
)

// WindowSince implements [store.Store]: one iterator for the whole call, a window
// and a seed walk per fingerprint on it.
func (s *Store) WindowSince(ctx context.Context, fps []identity.Fingerprint, dir store.Direction, from, to time.Time, sc store.Scope, o store.SinceOptions) ([]store.Since, error) {
	const op = "WindowSince"
	if s.closed.Load() {
		return nil, closedError(op)
	}
	if err := checkArgs(op, sc, dir, dir != 0, fps...); err != nil {
		return nil, err
	}
	r, h, err := s.beginSince(ctx, op, from, sc)
	if err != nil {
		return nil, err
	}
	defer r.close()
	out := make([]store.Since, len(fps))
	if !from.Before(to) || len(fps) == 0 {
		return out, nil
	}
	w := newSeedWalk(r, from, h, sc, o)
	for i, fp := range fps {
		if out[i].Window, err = r.window(fp, byte(dir), from, to, sc); err != nil {
			return nil, fmt.Errorf("pebblestore: %s: %w", op, err)
		}
		if out[i].Seeds, err = w.seeds(fp, byte(dir)); err != nil {
			return nil, fmt.Errorf("pebblestore: %s: %w", op, err)
		}
	}
	return out, nil
}

// beginSince is [Store.begin] for WindowSince, which also returns the horizon it
// checked: the seed rule depends on it, so it is read once, after the iterator is
// open, and that one value is both checked and used.
func (s *Store) beginSince(ctx context.Context, op string, from time.Time, sc store.Scope) (*reader, store.Horizon, error) {
	if err := ctx.Err(); err != nil {
		return nil, store.Horizon{}, contextError(op, err)
	}
	it, err := s.kv.NewIter(nil)
	if err != nil {
		return nil, store.Horizon{}, fmt.Errorf("pebblestore: %s: %w", op, err)
	}
	h := s.horizonOf(sc.Layer)
	if !h.IsZero() {
		var err error
		switch {
		case from.Before(h.Time):
			err = fmt.Errorf("pebblestore: %s: instant %s is before the horizon %s: %w", op, formatTime(from), formatTime(h.Time), store.ErrBeforeHorizon)
		case sc.AsOf < h.Seq:
			err = fmt.Errorf("pebblestore: %s: token %d is below the horizon's seq %d: %w", op, sc.AsOf, h.Seq, store.ErrBeforeHorizon)
		}
		if err != nil {
			_ = it.Close()
			return nil, store.Horizon{}, err
		}
	}
	return &reader{s: s, op: "since", it: it}, h, nil
}

// seedWalk is the seed rule of one call, in the units of the keys.
type seedWalk struct {
	r  *reader
	sc store.Scope
	// none says nothing can be before from (it is at or before the first instant).
	none bool
	// startNs is the instant whose baseline key the walk starts at, and first says
	// it starts at the first key of the prefix instead (from is after the range).
	startNs int64
	first   bool
	// liveNs is the instant a seed must hold at.
	liveNs int64
	// A seed at ns carries its payload if payloads and ns >= knownNs.
	payloads bool
	knownNs  int64
}

func newSeedWalk(r *reader, from time.Time, h store.Horizon, sc store.Scope, o store.SinceOptions) *seedWalk {
	w := &seedWalk{r: r, sc: sc, payloads: o.Payloads}
	fromNs, where := pebblekv.Locate(from)
	switch where {
	case pebblekv.Before:
		w.none = true
	case pebblekv.After:
		// Every record is before from, and from-1ns (or the horizon, if it is from)
		// is after every deadline: a seed holds there only with no TTL, as a value
		// holds at the last instant.
		w.first, w.liveNs = true, math.MaxInt64
	default:
		w.none = fromNs == 0
		w.startNs, w.liveNs = fromNs, fromNs-1
		if !h.IsZero() && from.Equal(h.Time) {
			w.liveNs = fromNs
		}
	}
	// No horizon, or one before the range: every payload is known. One after the
	// range: none is.
	switch hNs, hWhere := pebblekv.Locate(h.Time); {
	case h.IsZero() || hWhere == pebblekv.Before:
		w.knownNs = 0
	case hWhere == pebblekv.After:
		w.knownNs = math.MaxInt64
		w.payloads = false
	default:
		w.knownNs = hNs
	}
	return w
}

// fetch is a seed whose payload is read from its record after the walk.
type fetch struct {
	seed int    // its index
	ref  []byte // its reference, a copy
	ns   int64
}

// seeds are the seeds of one prefix, sorted.
func (w *seedWalk) seeds(fp identity.Fingerprint, dir byte) ([]store.Seed, error) {
	r := w.r
	if w.none {
		return nil, nil
	}
	prefix, ok := r.s.prefixOf(w.sc.Layer, fp, dir)
	if !ok {
		return nil, nil
	}
	start := stampKey(prefix, w.startNs, kindBaseline)
	if w.first {
		start = seekKey(prefix, math.MaxInt64)
	}
	stepped, skipped := r.stepped, r.skippedW+r.skippedVersion
	var out []store.Seed
	var fetches []fetch
	err := r.decide(prefix, start, w.sc.AsOf, func(ref []byte, ns int64, v pebblekv.Value) (bool, error) {
		if !v.Holds(ns, w.liveNs) {
			return true, nil
		}
		rec, err := w.record(fp, dir, ref, ns, v)
		if err != nil {
			return false, err
		}
		known := w.payloads && ns >= w.knownNs
		switch {
		case !known:
		case r.it.Key()[keyLen-1] != kindRecord:
			// A checkpoint entry keeps no payload: read it from the record.
			fetches = append(fetches, fetch{seed: len(out), ref: bytes.Clone(ref), ns: ns})
		case len(v.Payload) > 0:
			rec.Payload = bytes.Clone(v.Payload)
		}
		out = append(out, store.Seed{Record: rec, PayloadKnown: known})
		return true, nil
	})
	if err != nil {
		return nil, err
	}
	stamps := r.skippedW + r.skippedVersion - skipped
	if r.it.Valid() { // decide returned at the checkpoint or baseline it used
		if k := r.it.Key()[keyLen-1]; k == kindCheckpoint || k == kindBaseline {
			stamps++
		}
	}
	r.s.rec.Sample("read.seed_records_stepped", r.stepped-stepped)
	r.s.rec.Sample("read.seed_stamps_decoded", stamps)
	for _, f := range fetches {
		if err := w.payload(prefix, f, &out[f.seed].Record); err != nil {
			return nil, err
		}
	}
	if len(fetches) > 0 {
		r.s.rec.Count("read.seed_payload_steps", int64(len(fetches)))
	}
	store.SortSeeds(out)
	return out, nil
}

// record is the seed record of a reference of the prefix of fp in direction dir,
// decided by v at ns, without its payload.
func (w *seedWalk) record(fp identity.Fingerprint, dir byte, ref []byte, ns int64, v pebblekv.Value) (store.Record, error) {
	rec := store.Record{
		Layer: w.sc.Layer, EventTime: pebblekv.Time(ns), Seq: v.Seq, Kind: v.Kind, TTL: v.TTL,
		Boot: v.Boot, EventTimeBasis: store.EventTimeBasis(v.Basis), Producer: producerOf(dir, ref),
	}
	if v.HasThrough {
		rec.Through = pebblekv.Time(v.Through)
	}
	if dir == dirEntity {
		rec.Subject = store.EntitySubject(fp)
		return rec, nil
	}
	peer, rel, err := w.r.s.subjectOf(ref)
	if err != nil {
		return store.Record{}, err
	}
	if store.Direction(dir) == store.Forward {
		rec.Subject = store.EdgeSubject(fp, peer, rel)
	} else {
		rec.Subject = store.EdgeSubject(peer, fp, rel)
	}
	return rec, nil
}

// payload reads the payload of a seed a checkpoint entry decided from its record,
// which is at its instant with its Seq: a record at or after the horizon is never
// removed, so a missing one is an error.
func (w *seedWalk) payload(prefix []byte, f fetch, rec *store.Record) error {
	it := w.r.it
	key := recordKey(prefix, f.ns, rec.Seq)
	if !it.SeekGE(key) || !bytes.Equal(it.Key(), key) {
		if err := it.Error(); err != nil {
			return err
		}
		return fmt.Errorf("the record of seq %d a checkpoint names is not stored: %w", rec.Seq, pebblekv.ErrValue)
	}
	ref, v, err := decodeRecordValue(prefix[prefixLen-1], it.Value())
	if err != nil {
		return err
	}
	if !bytes.Equal(ref, f.ref) || v.Seq != rec.Seq {
		return fmt.Errorf("the record of seq %d a checkpoint names is of another reference: %w", rec.Seq, pebblekv.ErrValue)
	}
	if len(v.Payload) > 0 {
		rec.Payload = bytes.Clone(v.Payload)
	}
	return nil
}
