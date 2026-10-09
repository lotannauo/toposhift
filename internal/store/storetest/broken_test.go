package storetest_test

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/lotannauo/toposhift/internal/catalog"
	"github.com/lotannauo/toposhift/internal/identity"
	"github.com/lotannauo/toposhift/internal/lifecycle"
	"github.com/lotannauo/toposhift/internal/store"
	"github.com/lotannauo/toposhift/internal/store/memstore"
	"github.com/lotannauo/toposhift/internal/store/storetest"
)

var bg = context.Background()

// broken is a store with its own honest bookkeeping of the contract (closed,
// arguments, context, horizon refusals, LastSeq, Horizon), which answers reads
// from an inner memstore, possibly rebuilt or overwritten, and which has hooks
// that damage one behavior each. The inner memstore is never retained: the double
// keeps LastSeq and the horizon itself, because a rebuilt memstore reports a lower
// Seq in its horizon, and a mutant meant to lose a baseline must not be caught for
// that instead.
//
// With no damage it is a conforming store, and with a retain hook that compacts
// it is an honest one that discards history. It is safe for concurrent use.
type broken struct {
	mu     sync.RWMutex
	policy lifecycle.Policy

	inner   *memstore.Store
	view    *memstore.Store // if set, reads are answered from it instead of inner
	closed  bool
	lastSeq uint64
	hz      store.Horizon
	written []store.Record
	dropped int // records discarded by retentions so far

	// seqMask, if set, makes the store keep a Seq in fewer bits: the records keep
	// their true order, and a token is compared with the Seq cut to the mask.
	seqMask uint64
	snapMu  sync.Mutex
	snaps   map[snapKey]*memstore.Store

	// Damage to what is written.
	write func([]store.Record) []store.Record
	// partial applies a batch record by record, so a refused batch leaves its
	// valid prefix behind.
	partial bool
	// swallow turns a refusal the store should have returned into success.
	swallow func(error) bool
	// intercept answers a batch itself when it returns true.
	intercept func([]store.Record) (bool, error)
	// overwrite keeps only the newest record of a producer at one instant, as a
	// store that overwrites in place would, and answers reads from what is left.
	overwrite     bool
	overwriteBy   overwriteScope
	overwriteOnly store.SubjectKind // if set, only subjects of this kind are overwritten
	survivors     map[survivorKey]store.Record
	// tokenOnRefusal moves LastSeq even when the batch is refused.
	tokenOnRefusal bool
	// retainMovesToken moves LastSeq when Retain is called.
	retainMovesToken bool
	// acceptMixed accepts a batch that mixes records before the horizon with valid
	// ones, storing the valid ones, though it refuses a record before the horizon
	// on its own.
	acceptMixed bool
	// payloads, for the two damages that share bytes with the caller.
	keepCallerPayload, shareStoredPayload bool
	payloads                              map[uint64][]byte
	emptyWriteErr                         bool
	lastSeqFn                             func(real uint64) uint64

	// Damage to what is read. scope and when change every read's scope and every
	// point read's instant; scopeIn changes the scope before it is validated.
	scope   func(store.Scope) store.Scope
	scopeIn func(store.Scope) store.Scope
	when    func(time.Time) time.Time

	neighbors    func(ctx context.Context, r store.Store, fp identity.Fingerprint, dir store.Direction, t time.Time, sc store.Scope) ([]store.Neighbor, error)
	alive        func(ctx context.Context, r store.Store, fp identity.Fingerprint, t time.Time, sc store.Scope) (bool, error)
	batch        func(ctx context.Context, r store.Store, fps []identity.Fingerprint, dir store.Direction, t time.Time, sc store.Scope) ([][]store.Neighbor, error)
	window       func(ctx context.Context, r store.Store, fp identity.Fingerprint, dir store.Direction, from, to time.Time, sc store.Scope) ([]store.Record, error)
	entityWindow func(ctx context.Context, r store.Store, fp identity.Fingerprint, from, to time.Time, sc store.Scope) ([]store.Record, error)
	retain       func(b *broken, horizon time.Time) error
	// pinnedAtHorizon answers wrongly at exactly the horizon under a pinned token.
	pinnedAtHorizon bool

	// Damage to the horizon.
	noWriteHorizon, noInstantRefusal, noTokenRefusal       bool
	refuseAtHorizonInstant, refuseAtHorizonToken           bool
	refuseBeforeEpoch, retainRaisesSeq, horizonReportsLast bool
	refuseHighToken, horizonBeforeArgs, horizonBackward    bool

	// Damage to closing.
	closedAnswers, closedEmptyWriteOK, closedLastSeqZero  bool
	closedHorizonZero, secondCloseErr, closedInvalidFirst bool

	// Damage to contexts.
	ignoreCtxReads, ctxWriteStores, ctxRetainHalf bool
	// ignoreCtxWrites is not damage: a Write and a Retain that never consult the
	// context, which the contract allows, because it promises a context error only
	// for reads.
	ignoreCtxWrites bool
	// ctxWriteDrops is damage: a Write with a done context raises LastSeq, returns
	// no error, and stores nothing.
	ctxWriteDrops bool

	// Damage to arguments.
	acceptZeroFP, dirZeroIsForward, batchOthersOnZero, batchPartialError, fromGEToErr bool
}

type snapKey struct{ asOf, lastSeq uint64 }

type overwriteScope int

const (
	overwriteSameProducerAndInstant overwriteScope = iota
	overwriteAnyProducerAtInstant
	overwriteAnythingOfTheSubject
)

type survivorKey struct {
	subject  store.Subject
	producer lifecycle.Producer
	at       int64
	seq      uint64 // set for a subject that is not overwritten
}

var _ store.Store = (*broken)(nil)

func newBroken(policy lifecycle.Policy) *broken {
	inner, err := memstore.Open(memstore.Options{Policy: policy})
	if err != nil {
		panic(err)
	}
	return &broken{policy: policy, inner: inner}
}

func (b *broken) reader() store.Store {
	if b.view != nil {
		return b.view
	}
	return b.inner
}

func closedErr(op string) error { return fmt.Errorf("broken: %s: %w", op, store.ErrClosed) }

func ctxErr(op string, err error) error { return fmt.Errorf("broken: %s: %w", op, err) }

func (b *broken) Write(ctx context.Context, batch []store.Record) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.closed {
		if b.closedEmptyWriteOK && len(batch) == 0 {
			return nil
		}
		return closedErr("Write")
	}
	cerr := ctx.Err()
	if cerr != nil && b.ctxWriteDrops && len(batch) > 0 {
		b.lastSeq = batch[len(batch)-1].Seq
		return nil
	}
	if cerr != nil && !b.ctxWriteStores && !b.ignoreCtxWrites {
		return ctxErr("Write", cerr)
	}
	if len(batch) == 0 {
		if b.emptyWriteErr {
			return fmt.Errorf("broken: Write: an empty batch: %w", store.ErrInvalid)
		}
		return nil
	}
	// The store's token follows the batch it was given, whatever it then chose to
	// store of it.
	last := batch[len(batch)-1].Seq
	if b.intercept != nil {
		if handled, err := b.intercept(batch); handled {
			return err
		}
	}
	if !b.noWriteHorizon && !b.hz.IsZero() {
		stale := func(r store.Record) bool { return r.EventTime.Before(b.hz.Time) }
		if slices.ContainsFunc(batch, stale) {
			rest := slices.DeleteFunc(slices.Clone(batch), stale)
			if !b.acceptMixed || len(rest) == 0 {
				if b.tokenOnRefusal && len(rest) > 0 {
					b.lastSeq = max(b.lastSeq, rest[0].Seq)
				}
				return fmt.Errorf("broken: Write: a record is before the horizon %s: %w", b.hz.Time.Format(time.RFC3339), store.ErrBeforeHorizon)
			}
			batch = rest
			last = rest[len(rest)-1].Seq
		}
	}
	if b.payloads == nil {
		b.payloads = map[uint64][]byte{}
	}
	for _, r := range batch {
		switch {
		case b.keepCallerPayload:
			b.payloads[r.Seq] = r.Payload // shared with the caller
		case b.shareStoredPayload:
			b.payloads[r.Seq] = slices.Clone(r.Payload)
		}
	}
	if b.write != nil {
		batch = b.write(batch)
	}
	var err error
	switch {
	case b.partial:
		for _, r := range batch {
			if err = b.inner.Write(bg, []store.Record{r}); err != nil {
				break
			}
			b.written = append(b.written, r)
		}
	default:
		if err = b.inner.Write(bg, cloneRecs(batch)); err == nil {
			b.written = append(b.written, cloneRecs(batch)...)
			if b.overwrite {
				err = b.overwriteView(batch)
			}
		}
	}
	if err == nil {
		b.lastSeq = last
	}
	if err != nil && b.tokenOnRefusal {
		// The valid records processed before the refusal moved the token, and
		// nothing moved it back.
		for _, r := range batch {
			if !r.EventTime.Before(b.hz.Time) {
				b.lastSeq = max(b.lastSeq, r.Seq)
				break
			}
		}
	}
	if err != nil && b.swallow != nil && b.swallow(err) {
		return nil
	}
	if err == nil && cerr != nil && b.ctxWriteStores {
		return ctxErr("Write", cerr)
	}
	return err
}

func cloneRecs(rs []store.Record) []store.Record {
	out := slices.Clone(rs)
	for i := range out {
		out[i].Payload = slices.Clone(out[i].Payload)
	}
	return out
}

// overwriteView keeps what an in-place overwrite leaves and rebuilds the view.
func (b *broken) overwriteView(batch []store.Record) error {
	if b.survivors == nil {
		b.survivors = map[survivorKey]store.Record{}
	}
	for _, r := range batch {
		k := survivorKey{subject: r.Subject, producer: r.Producer, at: r.EventTime.UnixNano()}
		switch {
		case b.overwriteOnly != 0 && r.Subject.Kind != b.overwriteOnly:
			k = survivorKey{seq: r.Seq}
		case b.overwriteBy == overwriteAnyProducerAtInstant:
			k.producer = ""
		case b.overwriteBy == overwriteAnythingOfTheSubject:
			k.producer, k.at = "", 0
		}
		b.survivors[k] = r
	}
	kept := make([]store.Record, 0, len(b.survivors))
	for _, r := range b.survivors {
		kept = append(kept, r)
	}
	slices.SortFunc(kept, func(x, y store.Record) int { return cmpSeq(x.Seq, y.Seq) })
	view, err := memstore.Open(memstore.Options{Policy: b.policy})
	if err != nil {
		return err
	}
	if err := view.Write(bg, cloneRecs(kept)); err != nil {
		return err
	}
	b.view = view
	return nil
}

func (b *broken) LastSeq() uint64 {
	b.mu.RLock()
	defer b.mu.RUnlock()
	if b.closed && b.closedLastSeqZero {
		return 0
	}
	if b.lastSeqFn != nil {
		return b.lastSeqFn(b.lastSeq)
	}
	return b.lastSeq
}

func (b *broken) Horizon() store.Horizon {
	b.mu.RLock()
	defer b.mu.RUnlock()
	if b.closed && b.closedHorizonZero {
		return store.Horizon{}
	}
	hz := b.hz
	if b.horizonReportsLast && !hz.IsZero() {
		hz.Seq = b.lastSeq
	}
	return hz
}

func (b *broken) Close() error {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.closed {
		if b.secondCloseErr {
			return errors.New("broken: already closed")
		}
		return nil
	}
	b.closed = true
	return nil
}

func (b *broken) Retain(ctx context.Context, h time.Time) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.closed {
		return closedErr("Retain")
	}
	if err := ctx.Err(); err != nil && !b.ignoreCtxWrites {
		if b.ctxRetainHalf && h.After(b.hz.Time) {
			b.hz.Time = h.UTC() // the time moved, and the seq did not
		}
		return ctxErr("Retain", err)
	}
	if b.retainMovesToken {
		b.lastSeq++
	}
	moves := h.After(b.hz.Time)
	switch {
	case moves || b.horizonBackward:
		// The horizon is published before anything is discarded.
		b.hz = store.Horizon{Time: h.UTC(), Seq: b.lastSeq}
	case b.retainRaisesSeq:
		b.hz.Seq = b.lastSeq
	}
	if (moves || b.horizonBackward) && b.retain != nil {
		return b.retain(b, h)
	}
	return nil
}

// rebuild replaces the contents of the store with the records keep selects from
// everything written so far.
func (b *broken) rebuild(keep func(store.Record) bool) error {
	return b.replace(slices.DeleteFunc(cloneRecs(b.written), func(r store.Record) bool { return !keep(r) }))
}

// replace gives the store exactly the records kept.
func (b *broken) replace(kept []store.Record) error {
	slices.SortFunc(kept, func(x, y store.Record) int { return cmpSeq(x.Seq, y.Seq) })
	fresh, err := memstore.Open(memstore.Options{Policy: b.policy})
	if err != nil {
		return err
	}
	if err := fresh.Write(bg, cloneRecs(kept)); err != nil {
		return err
	}
	b.inner = fresh
	return nil
}

func cmpSeq(a, b uint64) int {
	switch {
	case a < b:
		return -1
	case a > b:
		return 1
	}
	return 0
}

// request is what a read asks, for the checks every read makes first.
type request struct {
	op     string
	fps    []identity.Fingerprint
	dir    store.Direction
	hasDir bool
	sc     store.Scope
	t      time.Time
}

func (b *broken) checkArgs(q *request) error {
	if err := q.sc.Validate(); err != nil {
		return fmt.Errorf("broken: %s: %w", q.op, err)
	}
	if q.hasDir {
		if q.dir == 0 && b.dirZeroIsForward {
			q.dir = store.Forward
		}
		if q.dir != store.Forward && q.dir != store.Reverse {
			return fmt.Errorf("broken: %s: direction %s: %w", q.op, q.dir, store.ErrInvalid)
		}
	}
	zeroOK := b.acceptZeroFP || (q.op == "NeighborsBatch" && (b.batchOthersOnZero || b.batchPartialError))
	if !zeroOK && slices.ContainsFunc(q.fps, identity.Fingerprint.IsZero) {
		return fmt.Errorf("broken: %s: the zero fingerprint: %w", q.op, store.ErrInvalid)
	}
	return nil
}

func (b *broken) checkHorizon(q request) error {
	if b.refuseBeforeEpoch && b.hz.IsZero() && q.t.Before(store.MinEventTime) {
		return fmt.Errorf("broken: %s: instant before 1970: %w", q.op, store.ErrBeforeHorizon)
	}
	if b.refuseHighToken && q.sc.AsOf != store.Latest && q.sc.AsOf > b.lastSeq {
		return fmt.Errorf("broken: %s: token %d is above LastSeq: %w", q.op, q.sc.AsOf, store.ErrInvalid)
	}
	if b.hz.IsZero() {
		return nil
	}
	instant := q.t.Before(b.hz.Time) || (b.refuseAtHorizonInstant && q.t.Equal(b.hz.Time))
	token := q.sc.AsOf < b.hz.Seq || (b.refuseAtHorizonToken && q.sc.AsOf != store.Latest && q.sc.AsOf == b.hz.Seq)
	if (instant && !b.noInstantRefusal) || (token && !b.noTokenRefusal) {
		return fmt.Errorf("broken: %s: before the horizon: %w", q.op, store.ErrBeforeHorizon)
	}
	return nil
}

// gate makes the checks every read makes before it reads, in the order of the
// reference: closed, arguments, context, horizon. The caller holds the read lock.
func (b *broken) gate(ctx context.Context, q *request) (store.Store, error) {
	if b.scopeIn != nil {
		q.sc = b.scopeIn(q.sc)
	}
	argsErr := b.checkArgs(q)
	if b.closed {
		switch {
		case b.closedInvalidFirst && argsErr != nil:
			return nil, argsErr
		case !b.closedAnswers:
			return nil, closedErr(q.op)
		}
	}
	if b.horizonBeforeArgs {
		if err := b.checkHorizon(*q); err != nil {
			return nil, err
		}
	}
	if argsErr != nil {
		return nil, argsErr
	}
	if err := ctx.Err(); err != nil && !b.ignoreCtxReads {
		return nil, ctxErr(q.op, err)
	}
	if !b.horizonBeforeArgs {
		if err := b.checkHorizon(*q); err != nil {
			return nil, err
		}
	}
	return b.reader(), nil
}

func (b *broken) scoped(sc store.Scope) store.Scope {
	if b.scope != nil {
		return b.scope(sc)
	}
	return sc
}

func (b *broken) at(t time.Time) time.Time {
	if b.when != nil {
		return b.when(t)
	}
	return t
}

// patch puts the payload slices the damages that share bytes keep into records
// a read returns.
func (b *broken) patch(recs []store.Record) []store.Record {
	if !b.keepCallerPayload && !b.shareStoredPayload {
		return recs
	}
	for i := range recs {
		if p, ok := b.payloads[recs[i].Seq]; ok && len(p) > 0 {
			recs[i].Payload = p
		}
	}
	return recs
}

func (b *broken) Neighbors(ctx context.Context, fp identity.Fingerprint, dir store.Direction, t time.Time, sc store.Scope) ([]store.Neighbor, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()
	q := request{op: "Neighbors", fps: []identity.Fingerprint{fp}, dir: dir, hasDir: true, sc: sc, t: t}
	r, err := b.gate(ctx, &q)
	if err != nil {
		return nil, err
	}
	if b.acceptZeroFP && fp.IsZero() {
		return nil, nil
	}
	if b.pinnedAtHorizon && !b.hz.IsZero() && t.Equal(b.hz.Time) && q.sc.AsOf != store.Latest {
		return nil, nil
	}
	sc, t = b.scoped(q.sc), b.at(t)
	r, sc = b.snapshot(r, sc)
	if b.neighbors != nil {
		return b.neighbors(bg, r, fp, q.dir, t, sc)
	}
	return r.Neighbors(bg, fp, q.dir, t, sc)
}

func (b *broken) NeighborsBatch(ctx context.Context, fps []identity.Fingerprint, dir store.Direction, t time.Time, sc store.Scope) ([][]store.Neighbor, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()
	q := request{op: "NeighborsBatch", fps: fps, dir: dir, hasDir: true, sc: sc, t: t}
	r, err := b.gate(ctx, &q)
	if err != nil {
		return nil, err
	}
	sc, t = b.scoped(q.sc), b.at(t)
	r, sc = b.snapshot(r, sc)
	if slices.ContainsFunc(fps, identity.Fingerprint.IsZero) {
		// Answers the fingerprints that are not zero.
		var rest []identity.Fingerprint
		for _, fp := range fps {
			if !fp.IsZero() {
				rest = append(rest, fp)
			}
		}
		got, err := r.NeighborsBatch(bg, rest, q.dir, t, sc)
		if err != nil {
			return nil, err
		}
		out := make([][]store.Neighbor, len(fps))
		for i, fp := range fps {
			if !fp.IsZero() {
				out[i], got = got[0], got[1:]
			}
		}
		if b.batchPartialError {
			return out, fmt.Errorf("broken: NeighborsBatch: the zero fingerprint: %w", store.ErrInvalid)
		}
		return out, nil
	}
	if b.batch != nil {
		return b.batch(bg, r, fps, q.dir, t, sc)
	}
	return r.NeighborsBatch(bg, fps, q.dir, t, sc)
}

func (b *broken) Alive(ctx context.Context, fp identity.Fingerprint, t time.Time, sc store.Scope) (bool, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()
	q := request{op: "Alive", fps: []identity.Fingerprint{fp}, sc: sc, t: t}
	r, err := b.gate(ctx, &q)
	if err != nil {
		return false, err
	}
	if b.acceptZeroFP && fp.IsZero() {
		return false, nil
	}
	sc, t = b.scoped(q.sc), b.at(t)
	r, sc = b.snapshot(r, sc)
	if b.alive != nil {
		return b.alive(bg, r, fp, t, sc)
	}
	return r.Alive(bg, fp, t, sc)
}

func (b *broken) Window(ctx context.Context, fp identity.Fingerprint, dir store.Direction, from, to time.Time, sc store.Scope) ([]store.Record, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()
	q := request{op: "Window", fps: []identity.Fingerprint{fp}, dir: dir, hasDir: true, sc: sc, t: from}
	r, err := b.gate(ctx, &q)
	if err != nil {
		return nil, err
	}
	if b.acceptZeroFP && fp.IsZero() {
		return nil, nil
	}
	if b.fromGEToErr && !from.Before(to) {
		return nil, fmt.Errorf("broken: Window: from is not before to: %w", store.ErrInvalid)
	}
	sc = b.scoped(q.sc)
	r, sc = b.snapshot(r, sc)
	var recs []store.Record
	if b.window != nil {
		recs, err = b.window(bg, r, fp, q.dir, from, to, sc)
	} else {
		recs, err = r.Window(bg, fp, q.dir, from, to, sc)
	}
	return b.patch(recs), err
}

func (b *broken) EntityWindow(ctx context.Context, fp identity.Fingerprint, from, to time.Time, sc store.Scope) ([]store.Record, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()
	q := request{op: "EntityWindow", fps: []identity.Fingerprint{fp}, sc: sc, t: from}
	r, err := b.gate(ctx, &q)
	if err != nil {
		return nil, err
	}
	if b.acceptZeroFP && fp.IsZero() {
		return nil, nil
	}
	if b.fromGEToErr && !from.Before(to) {
		return nil, fmt.Errorf("broken: EntityWindow: from is not before to: %w", store.ErrInvalid)
	}
	sc = b.scoped(q.sc)
	r, sc = b.snapshot(r, sc)
	var recs []store.Record
	if b.entityWindow != nil {
		recs, err = b.entityWindow(bg, r, fp, from, to, sc)
	} else {
		recs, err = r.EntityWindow(bg, fp, from, to, sc)
	}
	return b.patch(recs), err
}

// snapshot answers a read of a token from the records whose Seq, cut to seqMask,
// is at or below it, as a store that keeps a Seq in fewer bits would. A record whose
// Seq wrapped is then seen by a token below its true Seq. The caller holds the read
// lock.
func (b *broken) snapshot(r store.Store, sc store.Scope) (store.Store, store.Scope) {
	if b.seqMask == 0 || sc.AsOf == store.Latest || b.lastSeq <= b.seqMask {
		return r, sc
	}
	b.snapMu.Lock()
	defer b.snapMu.Unlock()
	key := snapKey{sc.AsOf, b.lastSeq}
	m, ok := b.snaps[key]
	if !ok {
		var seen []store.Record
		for _, w := range b.written {
			if w.Seq&b.seqMask <= sc.AsOf {
				seen = append(seen, w)
			}
		}
		slices.SortFunc(seen, func(x, y store.Record) int { return cmpSeq(x.Seq, y.Seq) })
		var err error
		if m, err = memstore.Open(memstore.Options{Policy: b.policy}); err != nil {
			panic(err)
		}
		if err := m.Write(bg, cloneRecs(seen)); err != nil {
			panic(err)
		}
		if len(b.snaps) > 256 {
			clear(b.snaps)
		}
		if b.snaps == nil {
			b.snaps = map[snapKey]*memstore.Store{}
		}
		b.snaps[key] = m
	}
	sc.AsOf = store.Latest
	return m, sc
}

// baseline keeps every record of written at or after h, and for each producer's
// reference to a subject the newest record before it, only if that reference is
// still live at h: the rule a layout is meant to follow. keepAll names records that
// are kept whatever their age.
func baseline(written []store.Record, h time.Time, keepAll func(store.Record) bool) []store.Record {
	type ref struct {
		subject  store.Subject
		producer lifecycle.Producer
	}
	newest := map[ref]store.Record{}
	var kept []store.Record
	for _, r := range written {
		if !r.EventTime.Before(h) || (keepAll != nil && keepAll(r)) {
			kept = append(kept, r)
			continue
		}
		k := ref{r.Subject, r.Producer}
		if cur, ok := newest[k]; !ok || r.EventTime.After(cur.EventTime) || (r.EventTime.Equal(cur.EventTime) && r.Seq > cur.Seq) {
			newest[k] = r
		}
	}
	for _, r := range newest {
		end := r.EventTime
		if r.Through.After(end) {
			end = r.Through
		}
		if r.Kind == lifecycle.Observe && (r.TTL == 0 || h.Before(end.Add(r.TTL))) {
			kept = append(kept, r)
		}
	}
	slices.SortFunc(kept, func(x, y store.Record) int { return cmpSeq(x.Seq, y.Seq) })
	return kept
}

// compact is the retain hook of an honest store that really discards history: it
// keeps what baseline keeps, and when the policy tells boots apart it keeps every
// entity record too, because a baseline must preserve a quarantine and the boot
// history a later collision is judged against.
func compact(b *broken, h time.Time) error {
	var keepAll func(store.Record) bool
	if b.policy.BootKey != "" {
		keepAll = func(r store.Record) bool { return r.Subject.Kind == store.SubjectEntity }
	}
	kept := baseline(b.written, h, keepAll)
	if err := b.replace(cloneRecs(kept)); err != nil {
		return err
	}
	b.dropped += len(b.written) - len(kept)
	b.written = kept
	return nil
}

// quarantined returns the entities that the written records quarantine.
func quarantined(b *broken) (map[store.Subject]bool, error) {
	m, err := memstore.Open(memstore.Options{Policy: b.policy})
	if err != nil {
		return nil, err
	}
	defer func() { _ = m.Close() }()
	all := cloneRecs(b.written)
	slices.SortFunc(all, func(x, y store.Record) int { return cmpSeq(x.Seq, y.Seq) })
	if err := m.Write(bg, all); err != nil {
		return nil, err
	}
	out := map[store.Subject]bool{}
	for _, r := range all {
		if r.Subject.Kind != store.SubjectEntity || out[r.Subject] {
			continue
		}
		if _, err := m.Alive(bg, r.Subject.A, time.Time{}, store.Current(r.Layer)); err != nil {
			var qe *store.QuarantineError
			out[r.Subject] = errors.As(err, &qe)
		}
	}
	return out, nil
}

var everyLayer = []catalog.Layer{catalog.L0, catalog.L1, catalog.L2, catalog.L3}

func withLayer(sc store.Scope, layer catalog.Layer) store.Scope {
	return store.Scope{Layer: layer, AsOf: sc.AsOf}
}

// A mutant is a damaged store. A harness that cannot tell is worthless, so each
// damage must be caught.
type mutant struct {
	make func(policy lifecycle.Policy) *broken
	// by names the scripted checks that must each catch it on their own, because
	// each is the check written for exactly that.
	by []string
	// scriptedOnly says only a scripted check can catch it: the generated
	// workloads never reach what it gets wrong. Every other mutant must be caught
	// by storetest.Check itself, which is what keeps the random rounds honest.
	scriptedOnly bool
	// wrong means the store gives wrong answers to reads, so some check must fail
	// with [storetest.ErrMismatch], not merely with a refused write, a wrong
	// LastSeq or a failed call.
	wrong bool
}

// mut builds a mutant from a damage.
func mut(damage func(b *broken, policy lifecycle.Policy)) func(lifecycle.Policy) *broken {
	return func(policy lifecycle.Policy) *broken {
		b := newBroken(policy)
		damage(b, policy)
		return b
	}
}

// detectors are the scripted checks. Each takes a fresh store, opened with the
// policy it names.
var detectors = map[string]struct {
	check  func(store.Store) error
	policy lifecycle.Policy
}{
	"write contract": {storetest.CheckWriteContract, lifecycle.Policy{}},
	"read contract":  {storetest.CheckReadContract, lifecycle.Policy{}},
	"close":          {storetest.CheckClose, lifecycle.Policy{}},
	"context":        {storetest.CheckContext, lifecycle.Policy{}},
	"horizon":        {storetest.CheckHorizon, lifecycle.Policy{}},
	"instant":        {storetest.CheckInstant, lifecycle.Policy{}},
	"producers":      {storetest.CheckProducers, lifecycle.Policy{}},
	"relations":      {storetest.CheckRelations, lifecycle.Policy{}},
	"extremes":       {storetest.CheckExtremes, lifecycle.Policy{}},
	"entity window":  {storetest.CheckEntityWindow, lifecycle.Policy{}},
	"quarantine":     {storetest.CheckQuarantine, storetest.QuarantinePolicy()},
}

func isQuarantineError(err error) bool {
	var qe *store.QuarantineError
	return errors.As(err, &qe)
}

func mutants() map[string]mutant {
	dropDeletes := func(batch []store.Record) []store.Record {
		return slices.DeleteFunc(slices.Clone(batch), func(r store.Record) bool { return r.Kind == lifecycle.Delete })
	}
	edit := func(f func(*store.Record)) func([]store.Record) []store.Record {
		return func(batch []store.Record) []store.Record {
			out := cloneRecs(batch)
			for i := range out {
				f(&out[i])
			}
			return out
		}
	}
	// Alters, in place, the payload bytes of the batch it was given.
	scribbling := func(batch []store.Record) []store.Record {
		for _, r := range batch {
			for i := range r.Payload {
				r.Payload[i] ^= 0xff
			}
		}
		return batch
	}
	// rebuildKeeping returns a Retain that forgets, by rebuilding from the records
	// keep selects, whatever the real Retain would not.
	rebuildKeeping := func(keep func(r store.Record, h time.Time) bool) func(*broken, time.Time) error {
		return func(b *broken, h time.Time) error {
			return b.rebuild(func(r store.Record) bool { return keep(r, h) })
		}
	}
	atOrAfter := func(r store.Record, h time.Time) bool { return !r.EventTime.Before(h) }
	over := func(f func(*broken)) func(lifecycle.Policy) *broken {
		return mut(func(b *broken, _ lifecycle.Policy) { f(b) })
	}
	stripBoot := func(recs []store.Record, err error) ([]store.Record, error) {
		for i := range recs {
			recs[i].Boot = ""
		}
		return recs, err
	}
	stripBasis := func(recs []store.Record, err error) ([]store.Record, error) {
		for i := range recs {
			recs[i].EventTimeBasis = store.BasisUnknown
		}
		return recs, err
	}

	return map[string]mutant{
		// Boundary errors: an interval is half-open, so an edge that starts at t is
		// not visible at t-1ns, and one that ends at its deadline is gone at the
		// deadline. Reading one nanosecond early or late gets one of those wrong,
		// and only a probe right at a boundary can tell.
		"reads one nanosecond early": {make: over(func(b *broken) {
			b.when = func(t time.Time) time.Time { return t.Add(time.Nanosecond) }
		}), by: []string{"instant"}, wrong: true},
		"reads one nanosecond late": {make: over(func(b *broken) {
			b.when = func(t time.Time) time.Time { return t.Add(-time.Nanosecond) }
		}), by: []string{"instant"}, wrong: true},
		"treats the first instant as before everything": {make: over(func(b *broken) {
			b.when = func(t time.Time) time.Time {
				if t.Equal(store.MinEventTime) {
					return t.Add(-time.Nanosecond)
				}
				return t
			}
		}), by: []string{"extremes"}, wrong: true, scriptedOnly: true},

		"accepts records before the horizon": {make: over(func(b *broken) { b.noWriteHorizon = true }), by: []string{"horizon"}},
		"alters the caller's payload bytes":  {make: over(func(b *broken) { b.write = scribbling }), by: []string{"write contract"}},
		// The first retention is honest. The second forgets what the first kept
		// standing for the history before it: it keeps only the records at or after
		// the first horizon, so whatever was alive across it is lost.
		"a second retention loses the first baseline": {make: over(func(b *broken) {
			var first time.Time
			b.retain = func(b *broken, h time.Time) error {
				if first.IsZero() {
					first = h
					return nil
				}
				return b.rebuild(func(r store.Record) bool { return atOrAfter(r, first) })
			}
		}), wrong: true},
		"drops deletes": {make: over(func(b *broken) { b.write = dropDeletes }), wrong: true},
		"ignores TTL":   {make: over(func(b *broken) { b.write = edit(func(r *store.Record) { r.TTL = 0 }) }), wrong: true},
		"ignores Through": {make: over(func(b *broken) {
			b.write = edit(func(r *store.Record) { r.Through = time.Time{} })
		}), wrong: true},
		"reverse reads forward": {make: over(func(b *broken) {
			b.neighbors = func(ctx context.Context, r store.Store, fp identity.Fingerprint, _ store.Direction, t time.Time, sc store.Scope) ([]store.Neighbor, error) {
				return r.Neighbors(ctx, fp, store.Forward, t, sc)
			}
		}), wrong: true},
		"window end is inclusive": {make: over(func(b *broken) {
			b.window = func(ctx context.Context, r store.Store, fp identity.Fingerprint, dir store.Direction, from, to time.Time, sc store.Scope) ([]store.Record, error) {
				return r.Window(ctx, fp, dir, from, to.Add(time.Nanosecond), sc)
			}
		}), by: []string{"instant"}, wrong: true},
		// The same edge from the other side: a record exactly one nanosecond before
		// the end is missed. Only a window whose end is one nanosecond past a
		// record's instant can tell.
		"window end loses the last nanosecond": {make: over(func(b *broken) {
			b.window = func(ctx context.Context, r store.Store, fp identity.Fingerprint, dir store.Direction, from, to time.Time, sc store.Scope) ([]store.Record, error) {
				return r.Window(ctx, fp, dir, from, to.Add(-time.Nanosecond), sc)
			}
		}), by: []string{"instant"}, wrong: true},
		// A record just before the window's start is let in.
		"window start is one nanosecond early": {make: over(func(b *broken) {
			b.window = func(ctx context.Context, r store.Store, fp identity.Fingerprint, dir store.Direction, from, to time.Time, sc store.Scope) ([]store.Record, error) {
				return r.Window(ctx, fp, dir, from.Add(-time.Nanosecond), to, sc)
			}
		}), by: []string{"instant"}, wrong: true},
		"window start is exclusive": {make: over(func(b *broken) {
			b.window = func(ctx context.Context, r store.Store, fp identity.Fingerprint, dir store.Direction, from, to time.Time, sc store.Scope) ([]store.Record, error) {
				return r.Window(ctx, fp, dir, from.Add(time.Nanosecond), to, sc)
			}
		}), by: []string{"instant"}, wrong: true},
		// Rebuild from only the records at or after the horizon: whatever was alive
		// across the horizon is forgotten.
		"retention loses the baseline": {make: over(func(b *broken) { b.retain = rebuildKeeping(atOrAfter) }), wrong: true},
		// Records at exactly the horizon are not before it, so they must stay.
		"retention drops the records at exactly the horizon": {make: over(func(b *broken) {
			b.retain = rebuildKeeping(func(r store.Record, h time.Time) bool { return !r.EventTime.Equal(h) })
		}), by: []string{"instant"}, wrong: true},

		// The snapshot token and the layer.
		"ignores the snapshot token": {make: over(func(b *broken) {
			b.scope = func(sc store.Scope) store.Scope { sc.AsOf = store.Latest; return sc }
		}), by: []string{"instant"}, wrong: true},
		"treats the snapshot token as exclusive": {make: over(func(b *broken) {
			b.scope = func(sc store.Scope) store.Scope {
				if sc.AsOf != store.Latest && sc.AsOf > 0 {
					sc.AsOf--
				}
				return sc
			}
		}), by: []string{"instant"}, wrong: true},
		// The workload fixes an edge's relation by the types at its ends, so no
		// generated workload gives a pair two relations: only the scripted check can
		// tell a store that identifies an edge by its ends alone.
		"identifies an edge by its two ends, not its relation": {make: over(func(b *broken) {
			b.neighbors = func(ctx context.Context, r store.Store, fp identity.Fingerprint, dir store.Direction, t time.Time, sc store.Scope) ([]store.Neighbor, error) {
				ns, err := r.Neighbors(ctx, fp, dir, t, sc)
				if err != nil {
					return nil, err
				}
				var out []store.Neighbor
				for _, n := range ns {
					if len(out) == 0 || out[len(out)-1].Peer != n.Peer {
						out = append(out, n)
					}
				}
				return out, nil
			}
		}), by: []string{"relations"}, wrong: true, scriptedOnly: true},
		"ignores the layer in Neighbors": {make: over(func(b *broken) {
			b.neighbors = func(ctx context.Context, r store.Store, fp identity.Fingerprint, dir store.Direction, t time.Time, sc store.Scope) ([]store.Neighbor, error) {
				var out []store.Neighbor
				for _, l := range everyLayer {
					ns, err := r.Neighbors(ctx, fp, dir, t, withLayer(sc, l))
					if err != nil {
						return nil, err
					}
					out = append(out, ns...)
				}
				store.SortNeighbors(out)
				return out, nil
			}
		}), by: []string{"instant"}, wrong: true},
		"ignores the layer in Alive": {make: over(func(b *broken) {
			b.alive = func(ctx context.Context, r store.Store, fp identity.Fingerprint, t time.Time, sc store.Scope) (bool, error) {
				for _, l := range everyLayer {
					if ok, err := r.Alive(ctx, fp, t, withLayer(sc, l)); err != nil || ok {
						return ok, err
					}
				}
				return false, nil
			}
		}), wrong: true},
		"mixes layers in Window": {make: over(func(b *broken) {
			b.window = func(ctx context.Context, r store.Store, fp identity.Fingerprint, dir store.Direction, from, to time.Time, sc store.Scope) ([]store.Record, error) {
				var out []store.Record
				for _, l := range everyLayer {
					rs, err := r.Window(ctx, fp, dir, from, to, withLayer(sc, l))
					if err != nil {
						return nil, err
					}
					out = append(out, rs...)
				}
				store.SortRecords(out)
				return out, nil
			}
		}), by: []string{"instant"}, wrong: true},

		// Overwriting in place loses the versions a query pinned to an earlier token
		// needs, and the records Window must still return. A producer that deletes
		// must not hide another producer's record at the same instant, and the
		// newest record of a subject must not decide for all.
		"keeps only the newest record at an instant, whichever producer made it": {make: over(func(b *broken) {
			b.overwrite, b.overwriteBy = true, overwriteAnyProducerAtInstant
		}), by: []string{"producers"}, wrong: true},
		"lets the newest record of a subject decide, whichever producer made it": {make: over(func(b *broken) {
			b.overwrite, b.overwriteBy = true, overwriteAnythingOfTheSubject
		}), by: []string{"producers"}, wrong: true},
		// A long window is cut short: only windows that span many records can show it.
		"a window returns at most three records": {make: over(func(b *broken) {
			b.window = func(ctx context.Context, r store.Store, fp identity.Fingerprint, dir store.Direction, from, to time.Time, sc store.Scope) ([]store.Record, error) {
				rs, err := r.Window(ctx, fp, dir, from, to, sc)
				if len(rs) > 3 {
					rs = rs[:3]
				}
				return rs, err
			}
		}), wrong: true},
		"keeps only the newest record at an instant": {make: over(func(b *broken) { b.overwrite = true }), by: []string{"instant"}, wrong: true},

		"accepts an invalid scope": {make: over(func(b *broken) {
			b.scopeIn = func(sc store.Scope) store.Scope {
				if sc.Layer < catalog.L0 || sc.Layer > catalog.L3 {
					sc.Layer = catalog.L2
				}
				return sc
			}
		}), by: []string{"read contract"}, scriptedOnly: true},

		// The batched read and the token.
		"a batch drops repeated fingerprints": {make: over(func(b *broken) {
			b.batch = func(ctx context.Context, r store.Store, fps []identity.Fingerprint, dir store.Direction, t time.Time, sc store.Scope) ([][]store.Neighbor, error) {
				var unique []identity.Fingerprint
				for _, fp := range fps {
					if !slices.Contains(unique, fp) {
						unique = append(unique, fp)
					}
				}
				return r.NeighborsBatch(ctx, unique, dir, t, sc)
			}
		}), wrong: true},
		"a batch answers every fingerprint like the first": {make: over(func(b *broken) {
			b.batch = func(ctx context.Context, r store.Store, fps []identity.Fingerprint, dir store.Direction, t time.Time, sc store.Scope) ([][]store.Neighbor, error) {
				out := make([][]store.Neighbor, len(fps))
				for i := range fps {
					ns, err := r.Neighbors(ctx, fps[0], dir, t, sc)
					if err != nil {
						return nil, err
					}
					out[i] = ns
				}
				return out, nil
			}
		}), wrong: true},
		"a batch answers an empty batch with something": {make: over(func(b *broken) {
			b.batch = func(ctx context.Context, r store.Store, fps []identity.Fingerprint, dir store.Direction, t time.Time, sc store.Scope) ([][]store.Neighbor, error) {
				if len(fps) == 0 {
					return [][]store.Neighbor{nil}, nil
				}
				return r.NeighborsBatch(ctx, fps, dir, t, sc)
			}
		}), wrong: true},
		// A batch is answered in chunks, and the results after the first chunk are
		// put in the wrong place.
		"a batch puts the results after the first 64 fingerprints in the wrong place": {make: over(func(b *broken) {
			b.batch = func(ctx context.Context, r store.Store, fps []identity.Fingerprint, dir store.Direction, t time.Time, sc store.Scope) ([][]store.Neighbor, error) {
				out, err := r.NeighborsBatch(ctx, fps, dir, t, sc)
				for i := 64; i < len(out); i++ {
					out[i] = out[i%64]
				}
				return out, err
			}
		}), wrong: true},
		"a batch ignores the token": {make: over(func(b *broken) {
			b.batch = func(ctx context.Context, r store.Store, fps []identity.Fingerprint, dir store.Direction, t time.Time, sc store.Scope) ([][]store.Neighbor, error) {
				sc.AsOf = store.Latest
				return r.NeighborsBatch(ctx, fps, dir, t, sc)
			}
		}), by: []string{"instant"}, wrong: true},

		// A reference belongs to the producer that made it.
		"ignores which producer a record is from": {make: over(func(b *broken) {
			b.write = edit(func(r *store.Record) { r.Producer = "anyone" })
		}), by: []string{"producers"}, wrong: true},
		// A sequence number that does not fit 32 bits wraps, so a token below it sees
		// the record.
		"stores Seq in 32 bits":                 {make: over(func(b *broken) { b.seqMask = 0xFFFFFFFF }), by: []string{"instant", "producers"}, wrong: true},
		"LastSeq moves when a batch is refused": {make: over(func(b *broken) { b.tokenOnRefusal = true })},
		"LastSeq moves when Retain is called":   {make: over(func(b *broken) { b.retainMovesToken = true })},
		// A baseline built for the latest token is used for a pinned one.
		"answers wrongly at the horizon instant under a pinned token": {make: over(func(b *broken) { b.pinnedAtHorizon = true }), wrong: true},
		"LastSeq is always zero": {make: over(func(b *broken) { b.lastSeqFn = func(uint64) uint64 { return 0 } }), by: []string{"instant"}},
		"LastSeq runs one ahead": {make: over(func(b *broken) { b.lastSeqFn = func(real uint64) uint64 { return real + 1 } }), by: []string{"instant"}},

		// EntityWindow returns an entity's own records the way Window returns an
		// edge's.
		"EntityWindow end is inclusive": {make: over(func(b *broken) {
			b.entityWindow = func(ctx context.Context, r store.Store, fp identity.Fingerprint, from, to time.Time, sc store.Scope) ([]store.Record, error) {
				return r.EntityWindow(ctx, fp, from, to.Add(time.Nanosecond), sc)
			}
		}), by: []string{"entity window"}, wrong: true},
		"EntityWindow also returns the entity's edge records": {make: over(func(b *broken) {
			b.entityWindow = func(ctx context.Context, r store.Store, fp identity.Fingerprint, from, to time.Time, sc store.Scope) ([]store.Record, error) {
				out, err := r.EntityWindow(ctx, fp, from, to, sc)
				for _, dir := range []store.Direction{store.Forward, store.Reverse} {
					edges, werr := r.Window(ctx, fp, dir, from, to, sc)
					out, err = append(out, edges...), errors.Join(err, werr)
				}
				store.SortRecords(out)
				return out, err
			}
		}), by: []string{"entity window"}, wrong: true},
		"EntityWindow ignores the token": {make: over(func(b *broken) {
			b.entityWindow = func(ctx context.Context, r store.Store, fp identity.Fingerprint, from, to time.Time, sc store.Scope) ([]store.Record, error) {
				sc.AsOf = store.Latest
				return r.EntityWindow(ctx, fp, from, to, sc)
			}
		}), by: []string{"entity window"}, wrong: true},
		"EntityWindow ignores the layer": {make: over(func(b *broken) {
			b.entityWindow = func(ctx context.Context, r store.Store, fp identity.Fingerprint, from, to time.Time, sc store.Scope) ([]store.Record, error) {
				var out []store.Record
				for _, l := range everyLayer {
					rs, err := r.EntityWindow(ctx, fp, from, to, withLayer(sc, l))
					if err != nil {
						return nil, err
					}
					out = append(out, rs...)
				}
				store.SortRecords(out)
				return out, nil
			}
		}), by: []string{"entity window"}, wrong: true},
		"EntityWindow keeps only the newest record at an instant": {make: over(func(b *broken) {
			b.overwrite, b.overwriteOnly = true, store.SubjectEntity
		}), by: []string{"entity window"}, wrong: true},

		// The retention horizon.
		"answers reads before the horizon's instant instead of refusing them": {make: over(func(b *broken) {
			b.noInstantRefusal = true
		}), by: []string{"horizon"}},
		"answers reads below the horizon's token": {make: over(func(b *broken) { b.noTokenRefusal = true }), by: []string{"horizon"}},
		"refuses a read at exactly the horizon's instant": {make: over(func(b *broken) {
			b.refuseAtHorizonInstant = true
		}), by: []string{"horizon"}},
		"refuses a read at exactly the horizon's token": {make: over(func(b *broken) {
			b.refuseAtHorizonToken = true
		}), by: []string{"horizon"}},
		"refuses instants before 1970 while the horizon is zero": {make: over(func(b *broken) {
			b.refuseBeforeEpoch = true
		}), by: []string{"read contract"}, scriptedOnly: true},
		"a Retain that does not move the horizon raises its Seq": {make: over(func(b *broken) {
			b.retainRaisesSeq = true
		}), by: []string{"horizon"}, scriptedOnly: true},
		"reports as the horizon's Seq the LastSeq at the time of the call to Horizon": {make: over(func(b *broken) {
			b.horizonReportsLast = true
		}), by: []string{"horizon"}},
		"refuses a token above LastSeq that is not Latest": {make: over(func(b *broken) {
			b.refuseHighToken = true
		}), by: []string{"read contract", "instant"}},
		"checks the horizon before the arguments": {make: over(func(b *broken) { b.horizonBeforeArgs = true }), by: []string{"horizon"}, scriptedOnly: true},

		// Closing.
		"reads after Close answer from the last state": {make: over(func(b *broken) { b.closedAnswers = true }), by: []string{"close"}, scriptedOnly: true},
		"an empty Write after Close returns nil":       {make: over(func(b *broken) { b.closedEmptyWriteOK = true }), by: []string{"close"}, scriptedOnly: true},
		"LastSeq after Close returns 0":                {make: over(func(b *broken) { b.closedLastSeqZero = true }), by: []string{"close"}, scriptedOnly: true},
		"Horizon after Close is zero":                  {make: over(func(b *broken) { b.closedHorizonZero = true }), by: []string{"close"}, scriptedOnly: true},
		"a second Close returns an error":              {make: over(func(b *broken) { b.secondCloseErr = true }), by: []string{"close"}, scriptedOnly: true},
		"invalid arguments after Close give ErrInvalid instead of ErrClosed": {make: over(func(b *broken) {
			b.closedInvalidFirst = true
		}), by: []string{"close"}, scriptedOnly: true},

		// Contexts.
		"reads ignore a cancelled context": {make: over(func(b *broken) { b.ignoreCtxReads = true }), by: []string{"context"}, scriptedOnly: true},
		"a Write with a cancelled context stores the batch and returns the context's error": {make: over(func(b *broken) {
			b.ctxWriteStores = true
		}), by: []string{"context"}, scriptedOnly: true},
		"a Write with a cancelled context returns nil and stores nothing": {make: over(func(b *broken) {
			b.ctxWriteDrops = true
		}), by: []string{"context"}, scriptedOnly: true},
		"a Retain with a cancelled context moves the horizon's time but not its Seq": {make: over(func(b *broken) {
			b.ctxRetainHalf = true
		}), by: []string{"context"}, scriptedOnly: true},

		// Arguments.
		"accepts the zero fingerprint and answers empty": {make: over(func(b *broken) { b.acceptZeroFP = true }), by: []string{"read contract"}, scriptedOnly: true},
		"treats direction 0 as Forward":                  {make: over(func(b *broken) { b.dirZeroIsForward = true }), by: []string{"read contract"}, scriptedOnly: true},
		"NeighborsBatch answers the others when one fingerprint is zero": {make: over(func(b *broken) {
			b.batchOthersOnZero = true
		}), by: []string{"read contract"}, scriptedOnly: true},
		"NeighborsBatch returns a partial result with its error": {make: over(func(b *broken) {
			b.batchPartialError = true
		}), by: []string{"read contract"}, scriptedOnly: true},
		"from >= to is an error": {make: over(func(b *broken) { b.fromGEToErr = true }), by: []string{"read contract"}, scriptedOnly: true},
		"an empty Write returns an error": {make: over(func(b *broken) {
			b.emptyWriteErr = true
		}), by: []string{"write contract"}, scriptedOnly: true},
		"keeps the caller's payload slices": {make: over(func(b *broken) { b.keepCallerPayload = true }), by: []string{"write contract"}},
		"returns its own stored payload bytes from a read": {make: over(func(b *broken) {
			b.shareStoredPayload = true
		}), by: []string{"write contract"}, scriptedOnly: true},
		// It stores the records before the one it refuses, so the batch is not whole.
		"refuses a Seq equal to Latest only after storing the records before it": {make: over(func(b *broken) {
			b.intercept = func(batch []store.Record) (bool, error) {
				for i, r := range batch {
					if r.Seq == store.Latest && i > 0 {
						_ = b.inner.Write(bg, cloneRecs(batch[:i]))
						return true, fmt.Errorf("broken: Write: seq %d names no record: %w", r.Seq, store.ErrInvalid)
					}
				}
				return false, nil
			}
		}), by: []string{"write contract"}, scriptedOnly: true},
		"refuses a Seq of Latest-1": {make: over(func(b *broken) {
			b.intercept = func(batch []store.Record) (bool, error) {
				return slices.ContainsFunc(batch, func(r store.Record) bool { return r.Seq >= store.Latest-1 }),
					fmt.Errorf("broken: Write: seq near Latest: %w", store.ErrInvalid)
			}
		}), by: []string{"write contract"}, scriptedOnly: true},
		"accepts a Seq equal to Latest": {make: over(func(b *broken) {
			b.intercept = func(batch []store.Record) (bool, error) {
				return slices.ContainsFunc(batch, func(r store.Record) bool { return r.Seq == store.Latest }), nil
			}
		}), by: []string{"write contract"}, scriptedOnly: true},

		// Quarantine, through the boot id a host observation carries.
		"Alive returns false and no error for a quarantined entity": {make: over(func(b *broken) {
			b.alive = func(ctx context.Context, r store.Store, fp identity.Fingerprint, t time.Time, sc store.Scope) (bool, error) {
				ok, err := r.Alive(ctx, fp, t, sc)
				if isQuarantineError(err) {
					return false, nil
				}
				return ok, err
			}
		}), by: []string{"quarantine"}, wrong: true},
		"the QuarantineError has no Collision": {make: over(func(b *broken) {
			b.alive = func(ctx context.Context, r store.Store, fp identity.Fingerprint, t time.Time, sc store.Scope) (bool, error) {
				ok, err := r.Alive(ctx, fp, t, sc)
				var qe *store.QuarantineError
				if errors.As(err, &qe) {
					return false, &store.QuarantineError{Entity: qe.Entity, Layer: qe.Layer}
				}
				return ok, err
			}
		}), by: []string{"quarantine"}, wrong: true},
		// A store can only quarantine in the layer the entity is stored in, which is
		// the scope's, so any other layer is wrong.
		"the QuarantineError names a layer other than the one read": {make: over(func(b *broken) {
			b.alive = func(ctx context.Context, r store.Store, fp identity.Fingerprint, t time.Time, sc store.Scope) (bool, error) {
				ok, err := r.Alive(ctx, fp, t, sc)
				var qe *store.QuarantineError
				if errors.As(err, &qe) {
					other := *qe
					other.Layer = other.Layer%catalog.L3 + 1
					return false, &other
				}
				return ok, err
			}
		}), by: []string{"quarantine"}, wrong: true},
		"decides quarantine from the records up to the instant only": {make: func(policy lifecycle.Policy) *broken {
			b := newBroken(policy)
			b.alive = func(ctx context.Context, r store.Store, fp identity.Fingerprint, t time.Time, sc store.Scope) (bool, error) {
				recs, err := r.EntityWindow(ctx, fp, store.MinEventTime, store.MaxEventTime.Add(time.Nanosecond), sc)
				if err != nil {
					return false, err
				}
				recs = slices.DeleteFunc(recs, func(x store.Record) bool { return x.EventTime.After(t) })
				slices.SortFunc(recs, func(x, y store.Record) int { return cmpSeq(x.Seq, y.Seq) })
				m, err := memstore.Open(memstore.Options{Policy: policy})
				if err != nil {
					return false, err
				}
				defer func() { _ = m.Close() }()
				if err := m.Write(ctx, recs); err != nil {
					return false, err
				}
				return m.Alive(ctx, fp, t, store.Current(sc.Layer))
			}
			return b
		}, by: []string{"quarantine"}, wrong: true},
		"decides quarantine ignoring the token": {make: over(func(b *broken) {
			b.alive = func(ctx context.Context, r store.Store, fp identity.Fingerprint, t time.Time, sc store.Scope) (bool, error) {
				sc.AsOf = store.Latest
				return r.Alive(ctx, fp, t, sc)
			}
		}), by: []string{"quarantine"}, wrong: true},
		"returns true with the QuarantineError": {make: over(func(b *broken) {
			b.alive = func(ctx context.Context, r store.Store, fp identity.Fingerprint, t time.Time, sc store.Scope) (bool, error) {
				ok, err := r.Alive(ctx, fp, t, sc)
				if isQuarantineError(err) {
					return true, err
				}
				return ok, err
			}
		}), by: []string{"quarantine"}, wrong: true},
		"Neighbors returns the QuarantineError of a quarantined endpoint": {make: over(func(b *broken) {
			b.neighbors = func(ctx context.Context, r store.Store, fp identity.Fingerprint, dir store.Direction, t time.Time, sc store.Scope) ([]store.Neighbor, error) {
				ns, err := r.Neighbors(ctx, fp, dir, t, sc)
				if err != nil {
					return nil, err
				}
				for _, end := range append([]identity.Fingerprint{fp}, peers(ns)...) {
					if _, err := r.Alive(ctx, end, t, sc); isQuarantineError(err) {
						return nil, err
					}
				}
				return ns, nil
			}
		}), by: []string{"quarantine"}},
		"retention loses a quarantine": {make: over(func(b *broken) {
			b.retain = rebuildKeeping(atOrAfter)
		}), by: []string{"quarantine"}, wrong: true},
		// The baseline rule applied to hosts: the older boot of a host is not live,
		// so it is dropped, and a later clone that reports it is not a collision.
		"retention loses the boot history": {make: over(func(b *broken) {
			b.retain = func(b *broken, h time.Time) error { return b.replace(baseline(b.written, h, nil)) }
		}), by: []string{"quarantine"}, scriptedOnly: true, wrong: true},
		"retention keeps the quarantines it has but not the boot history of the rest": {make: over(func(b *broken) {
			b.retain = func(b *broken, h time.Time) error {
				q, err := quarantined(b)
				if err != nil {
					return err
				}
				return b.replace(baseline(b.written, h, func(r store.Record) bool { return q[r.Subject] }))
			}
		}), by: []string{"quarantine"}, scriptedOnly: true, wrong: true},
		"accepts a host observation with a blank boot id": {make: over(func(b *broken) {
			b.write = edit(func(r *store.Record) {
				if r.Boot != "" && strings.TrimSpace(r.Boot) == "" {
					r.Boot = ""
				}
			})
		}), by: []string{"write contract", "quarantine"}, scriptedOnly: true},
		"ignores the boot key of the policy": {make: func(lifecycle.Policy) *broken { return newBroken(lifecycle.Policy{}) }, by: []string{"quarantine"}, wrong: true},
		"drops the boot id of a record": {make: over(func(b *broken) {
			b.write = edit(func(r *store.Record) { r.Boot = "" })
		}), by: []string{"quarantine"}, wrong: true},
		"returns records from Window and EntityWindow without their boot id": {make: over(func(b *broken) {
			b.window = func(ctx context.Context, r store.Store, fp identity.Fingerprint, dir store.Direction, from, to time.Time, sc store.Scope) ([]store.Record, error) {
				return stripBoot(r.Window(ctx, fp, dir, from, to, sc))
			}
			b.entityWindow = func(ctx context.Context, r store.Store, fp identity.Fingerprint, from, to time.Time, sc store.Scope) ([]store.Record, error) {
				return stripBoot(r.EntityWindow(ctx, fp, from, to, sc))
			}
		}), by: []string{"quarantine"}, wrong: true},

		// The event time basis a record carries.
		"returns records from Window and EntityWindow without their event time basis": {make: over(func(b *broken) {
			b.window = func(ctx context.Context, r store.Store, fp identity.Fingerprint, dir store.Direction, from, to time.Time, sc store.Scope) ([]store.Record, error) {
				return stripBasis(r.Window(ctx, fp, dir, from, to, sc))
			}
			b.entityWindow = func(ctx context.Context, r store.Store, fp identity.Fingerprint, from, to time.Time, sc store.Scope) ([]store.Record, error) {
				return stripBasis(r.EntityWindow(ctx, fp, from, to, sc))
			}
		}), by: []string{"write contract"}, wrong: true},
		"accepts a record with an undefined event time basis": {make: over(func(b *broken) {
			b.intercept = func(batch []store.Record) (bool, error) {
				return slices.ContainsFunc(batch, func(r store.Record) bool { return !r.EventTimeBasis.Valid() }), nil
			}
		}), by: []string{"write contract"}, scriptedOnly: true},
	}
}

func peers(ns []store.Neighbor) []identity.Fingerprint {
	out := make([]identity.Fingerprint, len(ns))
	for i, n := range ns {
		out[i] = n.Peer
	}
	return out
}

// TestHarnessCatchesBrokenStores runs the scripted checks and then storetest.Check
// over the workloads on every mutant. A mutant must be caught by Check on some
// workload or by a scripted check; if it names scripted checks, each of those must
// catch it on its own.
func TestHarnessCatchesBrokenStores(t *testing.T) {
	t.Parallel()
	for name, m := range mutants() {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			isMismatch := func(err error) bool { return errors.Is(err, storetest.ErrMismatch) }
			for _, d := range m.by {
				if _, ok := detectors[d]; !ok {
					t.Fatalf("the mutant names a check, %q, that does not exist", d)
				}
			}

			// The scripted checks are cheap and deterministic. Each one a mutant
			// names must catch it on its own.
			scripted, scriptedMismatch := false, false
			for dname, d := range detectors {
				err := d.check(m.make(d.policy))
				if err == nil && slices.Contains(m.by, dname) {
					t.Errorf("the %s check did not notice a store that %s", dname, name)
				}
				scripted = scripted || err != nil
				scriptedMismatch = scriptedMismatch || isMismatch(err)
			}
			if m.scriptedOnly && scripted && (!m.wrong || scriptedMismatch) {
				return // nothing the generated workloads do reaches what it gets wrong
			}

			// The generated workloads must catch it too, unless nothing they do can
			// reach what it gets wrong: that is what keeps their probing honest. They
			// stop at the first workload that notices (for a wrong answer, the first
			// that notices it as one).
			random, randomMismatch := false, false
			for _, w := range storetest.Workloads() {
				if random && (!m.wrong || randomMismatch) {
					break
				}
				if err := storetest.Check(m.make(w.Policy), w, storetest.Options{RetainAt: []float64{0.3, 0.6}, CheckEvery: 8}); err != nil {
					random = true
					randomMismatch = randomMismatch || isMismatch(err)
				}
			}
			switch {
			case !scripted && !random:
				t.Fatalf("the harness did not notice a store that %s", name)
			case !m.scriptedOnly && !random:
				t.Errorf("the generated workloads did not notice a store that %s", name)
			}
			if m.wrong && !scriptedMismatch && !randomMismatch {
				t.Errorf("a store that %s was caught, but never for a wrong answer: it was caught by something incidental", name)
			}
			if m.wrong && !m.scriptedOnly && !randomMismatch {
				t.Errorf("the generated workloads caught a store that %s, but never for a wrong answer", name)
			}
		})
	}
}

// TestContractCatchesBrokenStores does the same for the write contract: a store
// that stores part of a refused batch, or accepts records it should refuse, must
// fail it.
func TestContractCatchesBrokenStores(t *testing.T) {
	t.Parallel()

	if err := storetest.CheckWriteContract(newBroken(lifecycle.Policy{})); err != nil {
		t.Fatalf("a store with no damage fails the write contract: %v", err)
	}
	cases := map[string]func(b *broken){
		"applies the valid prefix of a refused batch": func(b *broken) { b.partial = true },
		"lets the horizon move backward":              func(b *broken) { b.horizonBackward = true },
		"moves LastSeq when it refuses a batch":       func(b *broken) { b.tokenOnRefusal = true },
		"accepts a batch that mixes stale and valid records": func(b *broken) {
			b.acceptMixed = true
		},
		"accepts invalid records": func(b *broken) {
			b.swallow = func(err error) bool { return errors.Is(err, store.ErrInvalid) }
		},
	}
	for name, damage := range cases {
		b := newBroken(lifecycle.Policy{})
		damage(b)
		if err := storetest.CheckWriteContract(b); err == nil {
			t.Errorf("the contract did not notice a store that %s", name)
		}
	}
}

func isInvalidNotBeforeHorizon(err error) bool {
	return errors.Is(err, store.ErrInvalid) && !errors.Is(err, store.ErrBeforeHorizon)
}

// A store may commit a Write or a Retain without consulting the context: the
// contract promises a context error only for reads, so the check must not demand it.
func TestCheckContextAllowsAWriteAndARetainThatIgnoreTheContext(t *testing.T) {
	t.Parallel()
	b := newBroken(lifecycle.Policy{})
	b.ignoreCtxWrites = true
	if err := storetest.CheckContext(b); err != nil {
		t.Errorf("a store whose Write and Retain ignore the context fails CheckContext: %v", err)
	}
}

// A Write that returns an error having stored the batch is a real violation.
func TestCheckContextCatchesAWriteThatFailsAfterStoring(t *testing.T) {
	t.Parallel()
	b := newBroken(lifecycle.Policy{})
	b.ctxWriteStores = true
	if err := storetest.CheckContext(b); err == nil {
		t.Error("a Write that returned a context error after storing the batch was not noticed")
	}
}
