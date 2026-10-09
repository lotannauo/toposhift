package pebblestore

import (
	"errors"
	"fmt"
	"math/rand/v2"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cockroachdb/pebble/v2/vfs"
	"github.com/cockroachdb/pebble/v2/vfs/errorfs"

	"github.com/lotannauo/toposhift/internal/store"
)

// A fault is one error injected into one operation on the file system, or into one
// commit, at a place the workload reaches. What the store promises then:
//
//   - a Write that returns an error leaves LastSeq where it was (nothing stored) or
//     at the last Seq of its batch (stored, the commit uncertain), and never between;
//   - what the live store shows is what its LastSeq says, a read that returns an
//     answer returns the right one, and a Retain that fails leaves history that is
//     still true;
//   - Pebble ends the process when it cannot write or sync its log. The tests give
//     it a logger that counts instead, and take the moment as the end of the
//     process: the file system is crashed there, and what the next start finds is a
//     whole number of the operations, from the last acknowledged one on;
//   - whatever happened, the store reopened on the file system the fault no longer
//     touches answers as the reference does when fed the operations it holds, and
//     takes the next batch.

// nthFault injects ErrInjected into the n-th operation, counting from the moment
// it is armed, among those its match selects. Several goroutines of Pebble ask it,
// so it counts atomically.
type nthFault struct {
	match func(errorfs.Op) bool
	at    int64 // the index to fail; negative fails none
	armed atomic.Bool
	seen  atomic.Int64
	fired atomic.Int64
}

var _ errorfs.Injector = (*nthFault)(nil)

func (f *nthFault) String() string { return fmt.Sprintf("operation %d of the matching ones", f.at) }

func (f *nthFault) MaybeError(op errorfs.Op) error {
	if !f.armed.Load() || f.match == nil || !f.match(op) {
		return nil
	}
	if f.seen.Add(1)-1 == f.at {
		f.fired.Add(1)
		return errorfs.ErrInjected
	}
	return nil
}

func isFile(op errorfs.Op, part string) bool { return strings.Contains(op.Path, part) }

func writesTo(part string) func(errorfs.Op) bool {
	return func(op errorfs.Op) bool {
		return isFile(op, part) && (op.Kind == errorfs.OpFileWrite || op.Kind == errorfs.OpFileWriteAt)
	}
}

func syncsOf(part string) func(errorfs.Op) bool {
	return func(op errorfs.Op) bool {
		return isFile(op, part) && (op.Kind == errorfs.OpFileSync || op.Kind == errorfs.OpFileSyncData || op.Kind == errorfs.OpFileSyncTo)
	}
}

func readsOf(part string) func(errorfs.Op) bool {
	return func(op errorfs.Op) bool {
		return isFile(op, part) && (op.Kind == errorfs.OpFileRead || op.Kind == errorfs.OpFileReadAt)
	}
}

func createsOf(part string) func(errorfs.Op) bool {
	return func(op errorfs.Op) bool { return isFile(op, part) && op.Kind == errorfs.OpCreate }
}

// faultKind is a place a fault is injected: either an operation on the file system
// (match) or a commit of the store (hook).
type faultKind struct {
	name string
	// match selects the file system operations that count (a hook counts its own
	// firings); the fault falls on one of the first span of them. The control run,
	// which injects nothing, requires the workload to make at least half as many
	// again, so a fault always has one to fall on. The spans are set below the least
	// the seeds of a run make, as measured over the first eight, with margin for
	// the race detector, which changes the timing and so a few counts; the control
	// run of one seed does not vouch for the others.
	match func(errorfs.Op) bool
	span  int64
	// hook arms a commit of the store to fail at its n-th firing (n counts from 0);
	// span is again how many of them are drawn from.
	hook func(s *Store, n int64, seen, fired *atomic.Int64)
	// log says the fault falls on the log. Pebble ends the process, with a panic that
	// unwinds through code that then unlocks a mutex it no longer holds, when a log
	// cannot be closed or created as it is rotated, and that cannot be recovered in
	// process. The log is rotated when a memtable fills and when a retention commits
	// a range deletion, so these trials run with memtables that do not fill and
	// without retentions. (The tests that flush tables do not fault the log.)
	log bool
	// reads says the kind injects read errors, which a read of the store may
	// return, so the live comparison accepts an error where it would insist on an
	// answer.
	reads bool
	// checkpoints says the kind needs a variant that writes checkpoints.
	checkpoints bool
}

var errCommitFault = errors.New("injected commit fault")

// failNth is a hook that counts its calls and returns an error on the n-th (none
// when n is negative).
func failNth(n int64, seen, fired *atomic.Int64) func() error {
	return func() error {
		if seen.Add(1)-1 == n {
			fired.Add(1)
			return errCommitFault
		}
		return nil
	}
}

// faultKinds are the places a fault falls. There is none for a failed sync of the
// manifest: Pebble then installs a flush's table and runs the flush again, and
// panics in a background goroutine (an invariant violation, the same key twice), so
// a test binary cannot survive it. (A process that panics has ended all the same;
// what it left is what a crash leaves, which the crash tests check.)
var faultKinds = []faultKind{
	{name: "wal-write", match: writesTo(".log"), span: 16, log: true},
	{name: "wal-sync", match: syncsOf(".log"), span: 16, log: true},
	{name: "table-create", match: createsOf(".sst"), span: 6},
	{name: "table-write", match: writesTo(".sst"), span: 16},
	{name: "table-sync", match: syncsOf(".sst"), span: 5},
	{name: "manifest-write", match: writesTo("MANIFEST"), span: 2},
	{name: "table-read", match: readsOf(".sst"), span: 100, reads: true},
	{name: "record-commit-not-stored", span: 12, hook: func(s *Store, n int64, seen, f *atomic.Int64) { s.beforeRecordApply = failNth(n, seen, f) }},
	{name: "record-commit-stored", span: 12, hook: func(s *Store, n int64, seen, f *atomic.Int64) { s.afterRecordApply = failNth(n, seen, f) }},
	{name: "horizon-commit-not-stored", span: 2, hook: func(s *Store, n int64, seen, f *atomic.Int64) { s.beforeHorizonApply = failNth(n, seen, f) }},
	{name: "horizon-commit-stored", span: 2, hook: func(s *Store, n int64, seen, f *atomic.Int64) { s.afterHorizonApply = failNth(n, seen, f) }},
	{name: "checkpoint-commit-not-stored", span: 8, checkpoints: true, hook: func(s *Store, n int64, seen, f *atomic.Int64) { s.beforeCheckpointApply = failNth(n, seen, f) }},
	{name: "checkpoint-commit-stored", span: 8, checkpoints: true, hook: func(s *Store, n int64, seen, f *atomic.Int64) { s.afterCheckpointApply = failNth(n, seen, f) }},
}

// faultResult is how one trial ended.
type faultResult struct {
	seen, fired int64
	// fatal says Pebble reported a condition it cannot continue from, and the
	// process is taken to have ended. failed says an operation returned an error.
	fatal, failed bool
	// inflight says the process ended inside the last operation.
	inflight bool
	// opErr is the error the operation returned, if any.
	opErr error
	// prefix is how many operations the store held when it was reopened.
	prefix, begun, acked int
}

// faultSteps is how many operations a trial runs: enough for the memtable to fill
// and be flushed a few times, which is when tables are written and the manifest is
// extended.
const faultSteps = 40

// runFault runs the workload of the seed with a fault at the at-th operation of the
// kind (none when at is negative) and checks the store afterwards.
func runFault(seed int, k faultKind, at int64) (res faultResult, err error) {
	v := crashVariantOf(seed)
	all, err := v.stream()
	if err != nil {
		return res, err
	}
	base := vfs.NewCrashableMem()
	inj := &nthFault{match: k.match, at: at}
	lg := newFatalLog()
	// The crash of the file system is taken inside the first Fatalf, the moment the
	// process dies, so that no write a dead process would not have made can land in
	// it, whatever the goroutines of Pebble and of the operation go on to do.
	var clone *vfs.MemFS
	percent := []int{0, 25, 60, 100}[rand.New(rand.NewPCG(uint64(seed), 98)).IntN(4)]
	lg.onFatal = func() {
		clone = base.CrashClone(vfs.CrashCloneCfg{UnsyncedDataPercent: percent, RNG: rand.New(rand.NewPCG(uint64(seed), 99))})
	}
	// No compactions run. Pebble panics, in the goroutine of a compaction, when one
	// fails in the way these tests make it (the bookkeeping of the level it was
	// taken from is then out of step), which a test binary cannot survive; and with
	// none the operations on the tables and the manifest are those of the flushes
	// alone, the same at each run of a seed. So that the tables that pile up in
	// level 0 do not stall the writes, a retention commits in one piece. (The crash
	// tests, which fail nothing, run compactions and retentions in pieces.)
	v.retainBytes = 0
	opts := v.options(errorfs.Wrap(base, inj), lg, false)
	if k.log {
		opts.Tuning.MemTableSize = 4 << 20
	}
	s, err := Open("db", opts)
	if err != nil {
		return res, err
	}
	closed := false
	defer func() {
		if !closed {
			_ = s.Close()
		}
	}()
	live, err := memstoreWithPolicy(v.policy)
	if err != nil {
		return res, err
	}
	defer func() { _ = live.Close() }()
	var hookSeen, hookFired atomic.Int64
	if k.hook != nil {
		k.hook(s, at, &hookSeen, &hookFired)
	}
	inj.armed.Store(true)
	rng := rand.New(rand.NewPCG(uint64(seed), 0xfa17))

	var hist opHistory
	var hz, maxEv time.Time
	pos := 0
	for step := 0; step < faultSteps && pos < len(all); step++ {
		var op histOp
		if pos > 0 && !k.log && rng.IntN(10) < 2 {
			h := maxEv.Add(-time.Duration(1+rng.IntN(240)) * time.Second)
			if !h.After(hz) {
				continue
			}
			op = histOp{retain: h, isRetain: true}
		} else {
			end := min(len(all), pos+1+rng.IntN(40))
			batch := notBefore(all[pos:end], hz)
			pos = end
			if len(batch) == 0 {
				continue
			}
			maxEv = maxEvent(maxEv, batch)
			op = histOp{batch: batch}
		}
		if lg.fatal() != 0 {
			// Pebble gave up between two operations (a flush could not be written).
			res.fatal = true
			break
		}
		before := s.LastSeq()
		hist = append(hist, op)
		ended, opErr := applyOrDie(s, op, lg)
		if ended || lg.fatal() != 0 {
			// The log could not be written or synced, or a flush could not be
			// recorded. The process ends here, and the operation, whatever it
			// returned, was not acknowledged to anyone.
			res.fatal, res.inflight = true, true
			break
		}
		if opErr != nil {
			if errors.Is(opErr, errHung) {
				return res, opErr
			}
			res.failed, res.opErr = true, opErr
			after := s.LastSeq()
			switch {
			case op.isRetain && after != before:
				return res, fmt.Errorf("a Retain that failed (%w) moved LastSeq from %d to %d", opErr, before, after)
			case !op.isRetain && after != before && after != op.batch[len(op.batch)-1].Seq:
				return res, fmt.Errorf("a Write of seq %d..%d that failed (%w) left LastSeq at %d, between %d (nothing stored) and %d (stored)",
					op.batch[0].Seq, op.batch[len(op.batch)-1].Seq, opErr, after, before, op.batch[len(op.batch)-1].Seq)
			}
			break
		}
		res.acked = len(hist)
		if op.isRetain {
			hz = op.retain
		}
		if err := op.apply(live); err != nil {
			return res, fmt.Errorf("the reference refused %s: %w", op, err)
		}
		if k.reads && step%4 == 0 {
			if err := liveDiff(s, live, all[:pos], rng, true); err != nil {
				return res, fmt.Errorf("after %s: %w", op, err)
			}
		}
	}
	if !res.fatal && !res.failed && !k.log {
		// Let the flush of what is in the memtable finish, so that the operations
		// the workload makes on the tables and the manifest are all made, however
		// the background work was timed.
		if ended, err := flushOrDie(s, lg); err != nil {
			return res, err
		} else if ended || lg.fatal() != 0 {
			res.fatal = true
		}
	}
	res.begun = len(hist)
	res.acked = len(hist)
	if res.inflight || res.failed {
		res.acked-- // the last operation, in flight or failed, is not acknowledged
	}
	res.seen, res.fired = inj.seen.Load()+hookSeen.Load(), inj.fired.Load()+hookFired.Load()
	if at >= 0 && res.fired == 0 {
		return res, fmt.Errorf("the fault was never injected: only %d operations matched, and it was to fall on the one at index %d", res.seen, at)
	}
	recs := all[:pos]

	if res.fatal {
		closed = true // a store whose log or manifest failed may never close
		// The process ended: what is left is the crash taken at that moment, with
		// some part of what was not synced. (dead is closed after the clone is taken.)
		<-lg.dead
		if err := checkReopened(v, clone, hist, res.acked, res.begun, all, pos, rng, &res); err != nil {
			return res, fmt.Errorf("after the process ended on %q (%d%% of the unsynced data kept): %w", lg.message(), percent, err)
		}
		return res, nil
	}

	// The store is still running: what it shows is what its LastSeq says...
	if res.failed {
		n, ref, err := requireReopened(v.policy, s, hist, res.acked, res.begun, recs, rng)
		if err != nil {
			return res, fmt.Errorf("the live store after %w: %w", res.opErr, err)
		}
		_, _ = n, ref.Close()
	} else if err := liveDiff(s, live, recs, rng, k.reads); err != nil {
		return res, fmt.Errorf("the live store at the end: %w", err)
	}
	// ...and a store reopened on the file system without the fault holds a whole
	// number of the operations and carries on.
	closed = true
	if err := closeOrHang(s); err != nil {
		return res, err
	}
	// (If Pebble gave up meanwhile, in the checks or the close, what is reopened is
	// the crash taken at that moment and not the file system as the close left it.)
	var target vfs.FS = base
	if lg.fatal() != 0 {
		<-lg.dead
		target = clone
	}
	if err := checkReopened(v, target, hist, res.acked, res.begun, all, pos, rng, &res); err != nil {
		return res, fmt.Errorf("after closing the store and reopening it: %w", err)
	}
	return res, nil
}

// applyOrDie does an operation on the store. If Pebble reports a fatal condition
// while it is in progress, the process is taken to have ended: Pebble's own logger
// ends it, and a store whose log or manifest has failed may never return (a write
// waits for a flush that cannot happen), so the operation is left where it is and
// ended says so. An operation that neither returns nor ends the process in a minute
// is a failure of the store.
func applyOrDie(s *Store, op histOp, lg *fatalLog) (ended bool, err error) {
	done := make(chan error, 1)
	go func() {
		// Pebble panics, instead of reporting a fatal condition, when a log it has
		// already failed to write is written again. That too is the end of the process.
		defer func() {
			if r := recover(); r != nil {
				lg.Fatalf("panic: %v", r)
			}
		}()
		done <- op.apply(s)
	}()
	select {
	case err := <-done:
		return false, err
	case <-lg.dead:
		return true, nil
	case <-time.After(time.Minute):
		return false, fmt.Errorf("%s did not return in a minute, and Pebble reported nothing fatal: %w", op, errHung)
	}
}

var errHung = errors.New("hung")

// flushOrDie flushes the memtable, with applyOrDie's rules about a fatal condition
// and a hang.
func flushOrDie(s *Store, lg *fatalLog) (ended bool, err error) {
	done := make(chan struct{})
	go func() {
		defer func() {
			if r := recover(); r != nil {
				lg.Fatalf("panic: %v", r)
			}
		}()
		_ = s.kv.Flush()
		close(done)
	}()
	select {
	case <-done:
		return false, nil
	case <-lg.dead:
		return true, nil
	case <-time.After(time.Minute):
		return false, fmt.Errorf("Flush did not return in a minute, and Pebble reported nothing fatal: %w", errHung)
	}
}

// closeOrHang closes the store, and fails if that takes more than a minute.
func closeOrHang(s *Store) error {
	done := make(chan struct{})
	go func() { _ = s.Close(); close(done) }()
	select {
	case <-done:
		return nil
	case <-time.After(time.Minute):
		return errors.New("Close did not return in a minute")
	}
}

// checkReopened opens the store on fs and requires the properties of a reopened
// store, then that it carries on.
func checkReopened(v crashVariant, fs vfs.FS, hist opHistory, acked, begun int, all []store.Record, pos int, rng *rand.Rand, res *faultResult) error {
	lg := newFatalLog()
	s, err := Open("db", v.options(fs, lg, true))
	if err != nil {
		return fmt.Errorf("reopening failed: %w", err)
	}
	defer func() { _ = s.Close() }()
	n, ref, err := requireReopened(v.policy, s, hist, acked, begun, all[:pos], rng)
	if err != nil {
		return err
	}
	defer func() { _ = ref.Close() }()
	res.prefix = n
	if err := requireCheckpointsTrue(s); err != nil {
		return fmt.Errorf("on reopening: %w", err)
	}
	if err := requireCarriesOn(s, ref, all, pos, rng); err != nil {
		return err
	}
	if lg.fatal() != 0 {
		return fmt.Errorf("Pebble reported a fatal condition on the reopened store: %s", lg.message())
	}
	return nil
}

// liveDiff compares a running store with the reference. With ioErrors, a read that
// returns an error is accepted (the file system is failing it), but one that
// returns an answer must return the right one.
func liveDiff(got, want store.Store, recs []store.Record, rng *rand.Rand, ioErrors bool) error {
	if g, w := got.LastSeq(), want.LastSeq(); g != w {
		return fmt.Errorf("LastSeq = %d, the reference says %d", g, w)
	}
	if g, w := got.Horizon(), want.Horizon(); !sameHorizonOf(g, w) {
		return fmt.Errorf("Horizon() = %v, the reference says %v", g, w)
	}
	p := sampleProbes(rng, recs, want.Horizon(), want.LastSeq())
	if d := answerDiffTolerating(got, want, p, ioErrors); d != "" {
		return errors.New(d)
	}
	return nil
}

// faultTrials is how many positions of each kind a run takes.
func faultTrials() int {
	switch {
	case raceEnabled:
		return 1
	case testing.Short():
		return 3
	}
	return 5
}

// Whatever the fault, at whichever of the first operations of its kind it falls, the
// store keeps the promise written at the top of this file.
func TestIOFaultsLeaveTheStoreWholeOrAbsent(t *testing.T) {
	t.Parallel()
	var mu sync.Mutex
	outcomes := map[string]map[string]int{}
	tally := func(kind, what string) {
		mu.Lock()
		defer mu.Unlock()
		if outcomes[kind] == nil {
			outcomes[kind] = map[string]int{}
		}
		outcomes[kind][what]++
	}
	t.Run("kinds", func(t *testing.T) {
		for ki, k := range faultKinds {
			t.Run(k.name, func(t *testing.T) {
				t.Parallel()
				// The control run: no fault, and the workload makes enough of the operations
				// for every trial to have one to fall on.
				t.Run("control", func(t *testing.T) {
					t.Parallel()
					if raceEnabled {
						t.Skip("the plain builds run the control; the race detector runs the trials")
					}
					res, err := runFault(ki%8, k, -1)
					if err != nil {
						t.Fatalf("%v\nreproduce with: go test ./internal/store/pebblestore -run '^TestIOFaultsLeaveTheStoreWholeOrAbsent$/^kinds$/^%s$/^control$' -v", err, k.name)
					}
					t.Logf("control %s: %d operations of this kind, %d begun", k.name, res.seen, res.begun)
					need := k.span + k.span/2
					if res.seen < need {
						t.Fatalf("the workload made %d operations of this kind, fewer than the %d a fault is drawn from needs", res.seen, need)
					}
				})
				for j := range faultTrials() {
					seed := j
					at := int64((j*5 + ki) % int(k.span))
					t.Run(fmt.Sprintf("seed=%d", seed), func(t *testing.T) {
						t.Parallel()
						if c := crashVariantOf(seed).ckpt; k.checkpoints && (!c.On || c.KMin > 8) {
							t.Skip("this variant writes (almost) no checkpoints")
						}
						res, err := runFault(seed, k, at)
						switch {
						case res.fatal:
							tally(k.name, "the process ended")
						case res.failed:
							tally(k.name, "an operation failed")
						default:
							tally(k.name, "absorbed")
						}
						if err != nil {
							t.Fatalf("seed %d (%s), fault at index %d of %s, operation error %v: %v\nreproduce with: go test ./internal/store/pebblestore -run '^TestIOFaultsLeaveTheStoreWholeOrAbsent$/^kinds$/^%s$/^seed=%d$' -v",
								seed, crashVariantOf(seed).name, at, k.name, res.opErr, err, k.name, seed)
						}
					})
				}
			})
		}
	})
	t.Logf("outcomes by kind: %v", outcomes)
}
