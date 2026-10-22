package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"io/fs"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/lotannauo/toposhift/internal/backup"
	"github.com/lotannauo/toposhift/internal/catalog"
	"github.com/lotannauo/toposhift/internal/datadir"
	"github.com/lotannauo/toposhift/internal/ingest/sequencer"
	"github.com/lotannauo/toposhift/internal/segment"
	"github.com/lotannauo/toposhift/internal/segment/localdir"
	"github.com/lotannauo/toposhift/internal/store"
	"github.com/lotannauo/toposhift/internal/store/pebblestore"
)

// envBackup and envBackupEvery are the environment twins of --backup and
// --backup-every.
const (
	envBackup      = "TOPOSHIFT_BACKUP"
	envBackupEvery = "TOPOSHIFT_BACKUP_EVERY"
)

// The default and the bounds of --backup-every: a whole number of minutes. The
// manifest is written whole at every cut and lists every file the backup holds, so
// a short interval costs more than a long one; an hour is the default.
const (
	defaultBackupEvery = time.Hour
	minBackupEvery     = 5 * time.Minute
	maxBackupEvery     = 24 * time.Hour
)

// Where the backup lives in the data directory: backup/segments is the root of
// the segment store, and backup/tmp is where the cutter writes the files it has
// not cut yet (the data volume's, never the system's temporary directory, which a
// reboot may empty).
const (
	backupDirName      = "backup"
	backupSegmentsName = "segments"
	backupTmpName      = "tmp"
)

// leftoverPattern is the name of a file the cutter leaves in backup/tmp when it
// does not close: the only files serve deletes there at start.
const leftoverPattern = "toposhift-backup-*.parquet"

// The modes of the backup, as GET /status says.
const (
	backupOff      = "off"
	backupOn       = "on"
	backupDegraded = "degraded"
	backupBroken   = "broken"
	backupRefused  = "refused"
)

// maxStatusText bounds the text of an error that /status repeats.
const maxStatusText = 512

// The seams of the backup, for tests: where the segments go, the ingest clock the
// cutter is given, the cadence's timer, how often a backup that is not working is
// said so, and how long a stop waits for the last cut.
var (
	openSegments = func(root string) (segment.Store, error) { return localdir.New(root) }
	backupNow    = time.Now
	backupTicker = func(d time.Duration) (<-chan time.Time, func()) {
		t := time.NewTicker(d)
		return t.C, t.Stop
	}
	backupNagEvery    = time.Hour
	backupStopTimeout = 5 * time.Second
)

// backupConfig is what serve was told about the backup.
type backupConfig struct {
	on    bool
	setBy string // retainByDefault, retainByFlag or retainByEnv
	every time.Duration
}

// backupFlags are the flags of the backup, registered on serve's flag set, and
// what the environment said of them.
type backupFlags struct {
	mode    string
	modeEnv string
	every   time.Duration
	envErr  error
}

// addBackupFlags registers --backup and --backup-every on fs. The environment
// gives their defaults: an empty variable is the same as an unset one.
func addBackupFlags(fs *flag.FlagSet, getenv func(string) string) *backupFlags {
	b := &backupFlags{mode: backupOff, modeEnv: getenv(envBackup), every: defaultBackupEvery}
	if b.modeEnv != "" {
		b.mode = b.modeEnv
	}
	if v := getenv(envBackupEvery); v != "" {
		d, err := time.ParseDuration(v)
		if err != nil {
			b.envErr = fmt.Errorf("%s %q: %w", envBackupEvery, v, err)
		} else {
			b.every = d
		}
	}
	fs.StringVar(&b.mode, "backup", b.mode,
		"on or off: keep a backup of the committed records in the data directory, which a store that holds records cannot start (env "+envBackup+")")
	fs.DurationVar(&b.every, "backup-every", b.every,
		"how often the backup is cut, a whole number of minutes from 5m to 24h (env "+envBackupEvery+")")
	return b
}

// resolve checks the flags once they are parsed. An error is a mistake in how
// serve was called.
func (b *backupFlags) resolve(fs *flag.FlagSet) (backupConfig, error) {
	if b.envErr != nil {
		return backupConfig{}, b.envErr
	}
	c := backupConfig{every: b.every, setBy: retainByDefault}
	switch b.mode {
	case "on":
		c.on = true
	case "off":
	default:
		return backupConfig{}, fmt.Errorf("--backup %q: give on or off", b.mode)
	}
	if err := checkBackupEvery(b.every); err != nil {
		return backupConfig{}, err
	}
	switch {
	case setFlags(fs)["backup"]:
		c.setBy = retainByFlag
	case b.modeEnv != "":
		c.setBy = retainByEnv
	}
	return c, nil
}

// checkBackupEvery refuses an interval outside the bounds, or with a part of a
// minute.
func checkBackupEvery(d time.Duration) error {
	switch {
	case d%time.Minute != 0:
		return fmt.Errorf("--backup-every %s is not a whole number of minutes", d)
	case d < minBackupEvery || d > maxBackupEvery:
		return fmt.Errorf("--backup-every %s is outside %s to %s", d, minBackupEvery, maxBackupEvery)
	}
	return nil
}

// backupReporter is the backup's part of GET /status, which exists whether or not
// the backup runs. A nil one reports the backup off.
type backupReporter struct {
	cfg     backupConfig
	haveDir bool
	run     atomic.Pointer[backupRun]
}

// newBackupReporter makes the reporter of a serve with cfg. A backup asked for
// with no data directory has nowhere to go: that is warned of, once, here, and
// serve boots without it.
func newBackupReporter(cfg backupConfig, haveDir bool, log *slog.Logger) *backupReporter {
	if cfg.on && !haveDir {
		log.Warn("backup is on but there is no data directory; nothing is backed up")
	}
	return &backupReporter{cfg: cfg, haveDir: haveDir}
}

func (r *backupReporter) attach(b *backupRun) {
	if r != nil {
		r.run.Store(b)
	}
}

// backupStatus is the backup's status.
type backupStatus struct {
	Mode   string        `json:"mode"`
	SetBy  string        `json:"set_by,omitempty"`
	Reason string        `json:"reason,omitempty"`
	Every  string        `json:"every,omitempty"`
	Local  *backupLocal  `json:"local,omitempty"`
	Retire *backupRetire `json:"retire,omitempty"`
}

// backupLocal is the local backup: the manifest it stands at, and its last cut.
type backupLocal struct {
	Generation    uint64     `json:"generation"`
	Seq           uint64     `json:"seq"`
	LastCut       *time.Time `json:"last_cut"`
	LastCutError  string     `json:"last_cut_error"`
	Files         int        `json:"files"`
	ManifestBytes int64      `json:"manifest_bytes"`
	// JournalBatches is the number of batches in the journal: those committed since
	// the last cut, and any a cut could not delete yet.
	JournalBatches int `json:"journal_batches"`
}

// backupRetire is the retirement of what the backup holds behind the store's
// horizons.
type backupRetire struct {
	Last         *time.Time        `json:"last"`
	NotRetirable map[string]string `json:"not_retirable"`
	LastError    string            `json:"last_error,omitempty"`
}

func (r *backupReporter) status() backupStatus {
	if r == nil || !r.cfg.on {
		return backupStatus{Mode: backupOff}
	}
	if run := r.run.Load(); run != nil {
		return run.status()
	}
	reason := "ingest is not running: nothing is backed up"
	if !r.haveDir {
		reason = "there is no data directory: nothing is backed up"
	}
	return backupStatus{Mode: backupOff, SetBy: r.cfg.setBy, Reason: reason, Every: r.cfg.every.String()}
}

// measuredSegments is the segment store the cutter writes to, which notes the
// size of the last manifest it was given, since the cutter does not say.
type measuredSegments struct {
	segment.Store
	manifestBytes atomic.Int64
}

func (m *measuredSegments) Put(ctx context.Context, name string, r io.Reader, size int64) (segment.Info, error) {
	info, err := m.Store.Put(ctx, name, r, size)
	if err == nil && strings.Contains(name, "/manifests/") {
		m.manifestBytes.Store(info.Size)
	}
	return info, err
}

// backupRun is a backup serve keeps. One mutex, mu, guards every call of the
// cutter, which is not safe for concurrent use: the commit path adds, the
// retention check logs, and the goroutine of the cadence cuts and retires, so a
// commit waits for a cut in progress.
type backupRun struct {
	cfg     backupConfig
	log     *slog.Logger
	dirPath string
	now     func() time.Time

	// tee is whether the backup takes the store's commits. It is set before the run
	// is used and never changes: a backup that is refused or broken at start has no
	// cutter and takes nothing.
	tee bool

	mu     sync.Mutex
	cutter *backup.Cutter
	broken bool // the cutter stopped taking commits: guarded by mu
	closed bool
	// work is whether a batch or a retention was taken since the last manifest.
	work bool
	segs *measuredSegments

	// jr is the journal of the batches since the last cut: nil when it could not be
	// opened (jerr says why), and then a crash ends the backup. jdead is set when it
	// takes nothing more, because the backup is broken or has stopped.
	jr    *journal
	jerr  string
	jdead atomic.Bool
	// degraded is whether the mode is degraded, for the commit path to look at
	// without the status's lock.
	degraded atomic.Bool
	// dmu makes deciding the mode from the journal and setting it one step, for the
	// commit path and the goroutine of the cadence.
	dmu sync.Mutex

	smu sync.Mutex // guards st, which the status handler reads
	st  backupSnapshot

	retireCh chan struct{}
	cancel   context.CancelFunc
	done     chan struct{}
}

// backupSnapshot is what GET /status says of a backup that runs.
type backupSnapshot struct {
	mode         string
	reason       string
	generation   uint64
	seq          uint64
	files        int
	local        bool
	lastCut      time.Time
	lastCutErr   string
	retireLast   time.Time
	notRetirable map[string]string
	retireErr    string
}

// startBackup starts the backup of the store st that serve has opened and whose
// cluster name is recorded, when it was asked for, and returns it; nil when not.
// A backup that cannot start is refused or broken, said so in the log and in
// GET /status, and ingest and reads go on without it. The returned run is stopped
// by [backupRun.stop].
//
// A new backup starts at Seq 1, so a store that holds records cannot start one:
// it is refused, and so is a store whose records end before its backup's, and one
// whose meta differs from the backup's. A store ahead of its backup (a crash, or a
// stop whose last cut failed) goes on from the journal, which holds the batches
// after the last manifest; when it does not hold every one, the backup is broken:
// it is missing the records between. The same goes for a store that holds records
// and a backup that has no manifest yet, when the journal holds all of them. A
// retention the store applied after the last manifest is logged from the store's
// horizons.
func startBackup(ctx context.Context, dir *datadir.Dir, st *pebblestore.Store, ic ingestConfig, opts pebblestore.Options, status *statusRoute, log *slog.Logger) *backupRun {
	if !ic.backup.on {
		return nil
	}
	b := newBackupRun(ic.backup, log, dir.Path)
	meta := backup.StoreMeta{
		ClusterName: ic.clusterName, ClusterNameSet: ic.set,
		BootKey: opts.Policy.BootKey, Skew: opts.Policy.Skew, Offsets: opts.Offsets, Keep: opts.Keep,
	}
	b.open(ctx, st, meta)
	status.backup.attach(b)
	b.start()
	return b
}

// newBackupRun is a backup run that has not opened anything.
func newBackupRun(cfg backupConfig, log *slog.Logger, dirPath string) *backupRun {
	return &backupRun{
		cfg: cfg, log: log, dirPath: dirPath, now: backupNow,
		retireCh: make(chan struct{}, 1), done: make(chan struct{}),
		st: backupSnapshot{mode: backupOn, notRetirable: map[string]string{}},
	}
}

// start starts the goroutine of the cadence.
func (b *backupRun) start() {
	ctx, cancel := context.WithCancel(context.Background())
	b.cancel = cancel
	go b.loop(ctx)
}

// open decides the start state of the backup: see startBackup.
func (b *backupRun) open(ctx context.Context, st horizonReader, meta backup.StoreMeta) {
	root := filepath.Join(b.dirPath, backupDirName)
	segRoot, tmp, jRoot := filepath.Join(root, backupSegmentsName), filepath.Join(root, backupTmpName), filepath.Join(root, backupJournalName)
	if err := makePrivateDirs(root, segRoot, tmp, jRoot); err != nil {
		b.fail(backupBroken, "the backup directory could not be made: "+err.Error())
		return
	}
	removeLeftovers(tmp, b.log)
	seg, err := openSegments(segRoot)
	if err != nil {
		b.fail(backupBroken, "the backup's segment store could not be opened: "+err.Error())
		return
	}
	b.segs = &measuredSegments{Store: seg}
	m, err := backup.Latest(ctx, b.segs, backup.DefaultPrefix)
	have := err == nil
	if err != nil && !errors.Is(err, backup.ErrNoBackup) {
		b.fail(backupBroken, "the backup cannot be read: "+err.Error())
		return
	}
	last := st.LastSeq()
	// The journal is read before the state is decided: it may hold what the store
	// holds beyond the backup (a crash after the store committed and before the cut).
	var plan journalPlan
	untrusted := untrustedPresent(filepath.Join(root, backupUntrustedName))
	if jseg, err := openJournal(jRoot); err != nil {
		b.jerr = "the journal could not be opened: " + err.Error()
		plan.reason = b.jerr
	} else {
		b.jr = newJournal(jseg, b.log, filepath.Join(root, backupUntrustedName))
		if m.Seq <= last {
			plan = b.jr.plan(ctx, m.Seq, last)
			if last == m.Seq {
				b.jerr = plan.reason // nothing is needed from it, but it cannot be kept clean
			}
		}
	}
	switch {
	case untrusted && m.Seq < last:
		// The journal held an entry that is not the batch the store stored under its
		// name, and nothing says which of its entries are: it is not replayed.
		if have {
			b.setLocal(m)
		}
		b.fail(backupBroken, fmt.Sprintf("the store is ahead of its backup by seqs %d..%d, and %s", m.Seq+1, last, untrustedReason))
		return
	case !have && last > 0 && plan.reason != "" && plan.held == 0:
		b.fail(backupRefused, fmt.Sprintf("the store predates the backup: it holds records through seq %d and the backup holds none; "+
			"a backup can start only on a store with no records, so turn it on from the first boot of a new data directory", last))
		return
	case have && m.Seq > last:
		b.fail(backupRefused, fmt.Sprintf("the store is behind its own backup: the store ends at seq %d and the backup at seq %d, "+
			"and backing up would reuse sequence numbers the backup holds", last, m.Seq))
		return
	}
	cut, err := backup.NewCutter(ctx, b.segs, backup.DefaultPrefix, backup.Options{Store: meta, TempDir: tmp})
	if err != nil {
		b.fail(backupRefused, err.Error())
		return
	}
	// abort ends a start that found the backup unable to go on from here.
	abort := func(reason string) {
		_ = cut.Close()
		if have {
			b.setLocal(m)
		}
		b.fail(backupBroken, b.scrub(reason))
	}
	if m.Seq < last && plan.reason != "" {
		// A journal with entries was written for these records, by a backup that ended
		// before its first manifest or before the next one.
		abort(fmt.Sprintf("the store is ahead of its backup by seqs %d..%d, and the journal does not hold them: %s", m.Seq+1, last, plan.reason))
		return
	}
	// The records the last manifest does not hold are added from the journal, in the
	// batches they were committed in, with the time of this start as their clock: a
	// clock later than the true one only delays a retirement.
	for _, e := range plan.chain {
		batch, err := b.jr.read(ctx, e)
		if err == nil {
			err = cut.Add(batch, b.now())
		}
		if err != nil {
			abort(fmt.Sprintf("the journal's entry %s could not be added to the backup: %v", e.name(), b.scrub(err.Error())))
			return
		}
	}
	// A retention the store applied after the last cut is not in the manifest. It is
	// logged now, after every record through the Seq it was applied at.
	logged, err := logRetention(st, cut)
	if err != nil {
		abort("a retention the store applied after the last cut is not in the backup, and it could not be logged: " + b.scrub(err.Error()))
		return
	}
	b.cutter, b.tee = cut, true
	b.work = len(plan.chain) > 0 || logged
	if b.jr != nil {
		b.jr.adopt(ctx, plan)
	}
	if logged {
		b.retireCh <- struct{}{}
	}
	switch {
	case have:
		b.setLocal(m)
		b.segs.manifestBytes.Store(manifestSize(ctx, b.segs, m.Generation))
		b.log.Info("backup is on: it resumes", "every", b.cfg.every, "set_by", b.cfg.setBy, "generation", m.Generation, "seq", m.Seq)
	default:
		b.smu.Lock()
		b.st.local = true
		b.smu.Unlock()
		b.log.Info("backup is on: it starts with the store's first record", "every", b.cfg.every, "set_by", b.cfg.setBy)
	}
	if n := len(plan.chain); n > 0 {
		b.log.Warn("the backup took from the journal the batches the last cut did not hold",
			"batches", n, "first_seq", plan.chain[0].first, "last_seq", plan.chain[n-1].last)
	}
	if logged {
		b.log.Warn("the backup logged a retention the store applied after the last cut")
	}
	b.syncDegraded()
}

// retentionUnlogged reports whether a horizon of the store is later than the
// one the manifest's last logged retention has for the layer (none is the zero
// time). A retention does not change the store's last Seq, so a crash between a
// Retain and the next cut leaves the store and the manifest at the same Seq with
// the store's horizons ahead.
func retentionUnlogged(st horizonReader, m backup.Manifest) bool {
	var logged [4]backup.Horizon
	if n := len(m.Retentions); n > 0 {
		logged = m.Retentions[n-1].Horizons
	}
	for l := catalog.L0; l <= catalog.L3; l++ {
		if st.LayerHorizon(l).Time.After(logged[l-catalog.L0].Time()) {
			return true
		}
	}
	return false
}

// logRetention logs in the cutter the retention the store applied after the
// manifest's last logged one, and says whether it did. The instant it was asked for
// is the L2 horizon, as serve's offset for the layer is zero, and the Seq is the
// one the store reports for the horizon. Only a retention the L2 horizon shows can
// be logged: a horizon of another layer that is ahead alone is an error.
func logRetention(st horizonReader, cut *backup.Cutter) (bool, error) {
	m := cut.Manifest()
	if !retentionUnlogged(st, m) {
		return false, nil
	}
	var logged time.Time
	if n := len(m.Retentions); n > 0 {
		logged = m.Retentions[n-1].Horizons[catalog.L2-catalog.L0].Time()
	}
	h := st.LayerHorizon(catalog.L2)
	if !h.Time.After(logged) {
		return false, errors.New("the layer that is ahead is not L2")
	}
	if err := cut.Retained(h.Time, horizonsOf(st), h.Seq); err != nil {
		return false, err
	}
	return true, nil
}

// manifestSize is the size of a generation's manifest, 0 when it cannot be read.
func manifestSize(ctx context.Context, s segment.Store, gen uint64) int64 {
	info, err := s.Stat(ctx, backup.ManifestName(backup.DefaultPrefix, gen))
	if err != nil {
		return 0
	}
	return info.Size
}

// makePrivateDirs makes the directories that do not exist, with mode 0700
// whatever the umask, and never changes one that does.
func makePrivateDirs(dirs ...string) error {
	for _, d := range dirs {
		if _, err := os.Stat(d); err == nil {
			continue
		}
		if err := os.Mkdir(d, 0o700); err != nil {
			return err
		}
		if err := os.Chmod(d, 0o700); err != nil {
			return err
		}
	}
	return nil
}

// removeLeftovers deletes the files an earlier run's cutter left in tmp, and
// nothing else.
func removeLeftovers(tmp string, log *slog.Logger) {
	entries, err := os.ReadDir(tmp)
	if err != nil {
		return
	}
	for _, e := range entries {
		if ok, _ := filepath.Match(leftoverPattern, e.Name()); !ok || !e.Type().IsRegular() {
			continue
		}
		if err := os.Remove(filepath.Join(tmp, e.Name())); err != nil && !errors.Is(err, fs.ErrNotExist) {
			log.Warn("a file of an earlier backup run could not be deleted", "name", e.Name(), "err", err)
		}
	}
}

// fail puts the backup in a mode in which it takes nothing, and says so.
func (b *backupRun) fail(mode, reason string) {
	b.smu.Lock()
	b.st.mode, b.st.reason = mode, reason
	b.smu.Unlock()
	b.log.Error("backup is "+mode+": "+reason+"; ingest and reads are not affected", "set_by", b.cfg.setBy)
}

// setLocal records the manifest the backup stands at.
func (b *backupRun) setLocal(m backup.Manifest) {
	b.smu.Lock()
	b.st.local = true
	b.st.generation, b.st.seq, b.st.files = m.Generation, m.Seq, len(m.Files)
	b.smu.Unlock()
}

// writer is the sequencer's writer with the backup's tap: a batch the store
// committed is added to the backup. Without a backup that takes commits it is
// inner.
func (b *backupRun) writer(st interface{ LastSeq() uint64 }, inner sequencer.Writer) sequencer.Writer {
	if b == nil || !b.tee {
		return inner
	}
	return backupWriter{Writer: inner, st: st, run: b}
}

// backupWriter passes every call to the store's writer, and then, for a batch the
// store committed, adds it to the backup. The sequencer is its one caller.
type backupWriter struct {
	sequencer.Writer
	st  interface{ LastSeq() uint64 }
	run *backupRun
}

// Write puts the batch in the journal, synced, writes it to the store, and returns
// the store's answer unchanged. Whether the batch was committed is told by the
// store's last sequence number and not by the error, as the store's contract says:
// a write that failed after its commit is stored, and its Seqs are not reused. A
// stored batch is added to the backup, and its entry stays in the journal until a
// cut holds it. A batch that was not stored is not added, its entry is deleted, and
// its Seqs are the next batch's. A journal that fails never fails the write.
func (w backupWriter) Write(ctx context.Context, batch []store.Record) error {
	if len(batch) == 0 {
		return w.Writer.Write(ctx, batch)
	}
	h := w.run.journalBatch(ctx, batch)
	err := w.Writer.Write(ctx, batch)
	if w.st.LastSeq() >= batch[len(batch)-1].Seq {
		w.run.journalStored(h)
		w.run.add(batch)
	} else {
		w.run.journalNotStored(ctx, h)
	}
	return err
}

// journalBatch is the journal's entry for a batch about to be committed. The
// journal's own file system calls are not cancelled with the sequencer: the entry
// is the promise that a stop or a crash after the commit does not end the backup.
func (b *backupRun) journalBatch(ctx context.Context, batch []store.Record) journalHandle {
	if b.jr == nil || b.jdead.Load() {
		return journalHandle{}
	}
	return b.jr.put(context.WithoutCancel(ctx), batch)
}

// journalStored is called when the store stored the batch of h.
func (b *backupRun) journalStored(h journalHandle) {
	if b.jr == nil || b.jdead.Load() {
		return
	}
	if !h.ok {
		b.jr.missed(h)
	}
	if !h.ok || b.degraded.Load() {
		b.syncDegraded()
	}
}

// journalNotStored is called when the store did not store the batch of h.
func (b *backupRun) journalNotStored(ctx context.Context, h journalHandle) {
	if b.jr == nil || b.jdead.Load() {
		return
	}
	if h.ok {
		b.jr.notStored(context.WithoutCancel(ctx), h)
	}
	if !h.ok || b.degraded.Load() || b.jr.problem() != "" {
		b.syncDegraded()
	}
}

// syncDegraded sets the mode to degraded while a crash would end the backup
// because of the journal, and back to on when it would not.
func (b *backupRun) syncDegraded() {
	b.dmu.Lock()
	defer b.dmu.Unlock()
	reason := b.jerr
	if reason == "" && b.jr != nil {
		reason = b.jr.problem()
	}
	if reason != "" {
		b.degrade(reason)
	} else {
		b.undegrade()
	}
}

// degrade says that a crash now would end the backup. It is said once, when the
// mode changes; a backup that is broken or refused stays so.
func (b *backupRun) degrade(reason string) {
	reason = b.scrub(reason)
	b.smu.Lock()
	switch b.st.mode {
	case backupOn:
		b.st.mode, b.st.reason = backupDegraded, reason
		b.degraded.Store(true)
		b.smu.Unlock()
		b.log.Error("backup is degraded: "+reason+"; a crash before the next cut would end the backup; ingest and reads are not affected", "set_by", b.cfg.setBy)
		return
	case backupDegraded:
		b.st.reason = reason
	}
	b.smu.Unlock()
}

// undegrade puts a degraded backup back to on.
func (b *backupRun) undegrade() {
	b.smu.Lock()
	was := b.st.mode == backupDegraded
	if was {
		b.st.mode, b.st.reason = backupOn, ""
		b.degraded.Store(false)
	}
	b.smu.Unlock()
	if was {
		b.log.Info("backup is on again: the journal holds every batch the last cut does not")
	}
}

// add gives the cutter a batch the store committed. An error breaks the backup,
// never the write.
func (b *backupRun) add(batch []store.Record) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.broken || b.closed || b.cutter == nil {
		return
	}
	if err := b.cutter.Add(batch, b.now()); err != nil {
		b.breakLocked(fmt.Sprintf("a batch ending at seq %d could not be added: %v", batch[len(batch)-1].Seq, err))
		return
	}
	b.work = true
}

// breakLocked ends the taking of commits: the cutter is not used again until a
// restart. mu is held.
func (b *backupRun) breakLocked(reason string) {
	b.broken = true
	b.jdead.Store(true)
	b.fail(backupBroken, b.scrub(reason))
}

// retainer is the retention check's store with the backup's tap: a retention that
// moved a horizon is logged in the backup, and retires what it allows. Without a
// backup that takes commits it is inner.
func (b *backupRun) retainer(inner sequencer.Retainer, st horizonReader) sequencer.Retainer {
	if b == nil || !b.tee {
		return inner
	}
	return backupRetainer{Retainer: inner, st: st, run: b}
}

// horizonReader is what the tap reads of the store.
type horizonReader interface {
	LayerHorizon(catalog.Layer) store.Horizon
	LastSeq() uint64
}

type backupRetainer struct {
	sequencer.Retainer
	st  horizonReader
	run *backupRun
}

// Retain asks the store, and then, if a horizon moved, logs the retention. A
// Retain that returned an error has moved the horizons whole or not at all, so
// it is logged whenever a horizon moved, whatever the error. The last Seq is
// read after the Retain returns, as the cutter requires.
func (r backupRetainer) Retain(ctx context.Context, horizon time.Time) error {
	before := horizonsOf(r.st)
	err := r.Retainer.Retain(ctx, horizon)
	after := horizonsOf(r.st)
	if horizonsMoved(before, after) {
		r.run.retained(horizon, after, r.st.LastSeq())
	}
	return err
}

// horizonsOf is the four layer horizons of a store.
func horizonsOf(st horizonReader) [4]store.Horizon {
	var h [4]store.Horizon
	for l := catalog.L0; l <= catalog.L3; l++ {
		h[l-catalog.L0] = st.LayerHorizon(l)
	}
	return h
}

// horizonsMoved reports whether any layer's horizon differs.
func horizonsMoved(before, after [4]store.Horizon) bool {
	for i := range before {
		if before[i].Seq != after[i].Seq || !before[i].Time.Equal(after[i].Time) {
			return true
		}
	}
	return false
}

// retained logs a retention in the backup and asks the goroutine of the cadence
// to retire.
func (b *backupRun) retained(asked time.Time, h [4]store.Horizon, last uint64) {
	b.mu.Lock()
	if b.broken || b.closed || b.cutter == nil {
		b.mu.Unlock()
		return
	}
	err := b.cutter.Retained(asked, h, last)
	if err != nil {
		// A retention the backup does not hold is one a restore would not apply, and
		// the horizons it logs afterwards would not follow from it: the backup stops.
		b.breakLocked("a retention could not be logged: " + err.Error())
		b.mu.Unlock()
		return
	}
	b.work = true
	b.mu.Unlock()
	select {
	case b.retireCh <- struct{}{}:
	default:
	}
}

// loop cuts on the cadence, retires when a retention asks, and says every
// interval of nagging that a backup that is refused or broken is not working,
// until ctx ends.
func (b *backupRun) loop(ctx context.Context) {
	defer close(b.done)
	var cadence <-chan time.Time
	if b.tee {
		c, stop := backupTicker(b.cfg.every)
		defer stop()
		cadence = c
	}
	nag, stopNag := backupTicker(backupNagEvery)
	defer stopNag()
	for {
		select {
		case <-ctx.Done():
			return
		case <-cadence:
			b.cut(ctx)
		case <-b.retireCh:
			b.retire(ctx)
		case <-nag:
			b.nag()
		}
	}
}

// nag repeats the error of a backup that is not working.
func (b *backupRun) nag() {
	b.smu.Lock()
	mode, reason := b.st.mode, b.st.reason
	b.smu.Unlock()
	if mode == backupBroken || mode == backupRefused || mode == backupDegraded {
		b.log.Error("backup is "+mode+": "+reason+"; ingest and reads are not affected", "set_by", b.cfg.setBy)
	}
}

// cut writes what was added since the last manifest. With nothing added it does
// nothing, so an idle interval writes no manifest. Once the manifest is stored, the
// journal's entries it holds are deleted, outside the lock: a commit does not wait
// for them.
func (b *backupRun) cut(ctx context.Context) {
	if seq, ok := b.cutLocked(ctx); ok {
		b.trimJournal(ctx, seq)
	}
}

// cutLocked is cut under the lock. It returns the Seq of the manifest it stored.
func (b *backupRun) cutLocked(ctx context.Context) (uint64, bool) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.broken || b.closed || b.cutter == nil || !b.work {
		return 0, false
	}
	start := time.Now()
	m, err := b.cutter.Cut(ctx)
	if err != nil && ctx.Err() != nil {
		return 0, false // the stop's own cut goes on from here
	}
	b.cutDone(m, err, time.Since(start), "backup cut")
	return m.Seq, err == nil
}

// trimJournal deletes the journal's entries that a manifest through seq holds, and
// puts the mode back to on if the journal is whole.
func (b *backupRun) trimJournal(ctx context.Context, seq uint64) {
	if b.jr == nil {
		return
	}
	b.jr.through(ctx, seq)
	b.syncDegraded()
}

// cutDone records the outcome of a cut. mu is held.
func (b *backupRun) cutDone(m backup.Manifest, err error, took time.Duration, what string) {
	if err != nil {
		b.log.Error("the backup could not be cut; it is tried again at the next interval", "err", err)
		b.smu.Lock()
		b.st.lastCutErr = b.scrub(err.Error())
		b.smu.Unlock()
		return
	}
	b.work = false
	b.setLocal(m)
	b.smu.Lock()
	b.st.lastCut, b.st.lastCutErr = b.now(), ""
	b.smu.Unlock()
	b.log.Info(what, "generation", m.Generation, "seq", m.Seq, "files", len(m.Files), "took", took)
}

// retire retires what the backup holds behind the store's horizons, as far as
// the margin allows. A class that may not be retired is warned of each time, and
// the backup keeps its files.
func (b *backupRun) retire(ctx context.Context) {
	if seq, ok := b.retireLocked(ctx); ok {
		b.trimJournal(ctx, seq)
	}
}

// retireLocked is retire under the lock. It returns the Seq of the manifest it
// stored, when it stored one.
func (b *backupRun) retireLocked(ctx context.Context) (uint64, bool) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.broken || b.closed || b.cutter == nil {
		return 0, false
	}
	start := time.Now()
	ret, err := b.cutter.Retire(ctx)
	if err != nil && ctx.Err() != nil && ret.Manifest.Generation == 0 {
		return 0, false // stopping: the stop's own cut goes on from here
	}
	var others []error
	if err != nil {
		for _, e := range joined(err) {
			if errors.Is(e, backup.ErrNotRetirable) {
				b.log.Warn("the backup does not retire a class", "err", e)
			} else {
				others = append(others, e)
			}
		}
	}
	if ret.Manifest.Generation != 0 {
		// A manifest was written, with the cut Retire makes first.
		b.work = false
		b.setLocal(ret.Manifest)
		b.log.Info("backup retired", "generation", ret.Manifest.Generation, "files", len(ret.Manifest.Files),
			"retired_files", ret.Files, "took", time.Since(start))
	}
	b.smu.Lock()
	defer b.smu.Unlock()
	if ret.Manifest.Generation != 0 {
		b.st.retireLast = b.now()
		b.st.notRetirable = map[string]string{}
		for cl, reason := range ret.Manifest.NotRetirable {
			if reason != "" {
				b.st.notRetirable[backup.Class(cl).String()] = b.scrub(reason)
			}
		}
	}
	b.st.retireErr = ""
	if len(others) > 0 {
		joinedErr := errors.Join(others...)
		b.log.Error("the backup could not retire; it is tried again at the next retention", "err", joinedErr)
		b.st.retireErr = b.scrub(joinedErr.Error())
	}
	return ret.Manifest.Seq, ret.Manifest.Generation != 0
}

// joined returns the errors a join holds, or err alone.
func joined(err error) []error {
	if j, ok := err.(interface{ Unwrap() []error }); ok {
		return j.Unwrap()
	}
	return []error{err}
}

// scrub removes the data directory's path from text that GET /status repeats, and
// bounds its length.
func (b *backupRun) scrub(s string) string {
	if b.dirPath != "" {
		s = strings.ReplaceAll(s, b.dirPath, "<data-dir>")
	}
	if len(s) > maxStatusText {
		s = strings.ToValidUTF8(s[:maxStatusText], "") + "..."
	}
	return s
}

// status is the backup's status.
func (b *backupRun) status() backupStatus {
	b.smu.Lock()
	defer b.smu.Unlock()
	s := backupStatus{Mode: b.st.mode, SetBy: b.cfg.setBy, Reason: b.st.reason, Every: b.cfg.every.String()}
	if b.st.local {
		l := &backupLocal{
			Generation: b.st.generation, Seq: b.st.seq, LastCutError: b.st.lastCutErr, Files: b.st.files,
			LastCut: orNull(b.st.lastCut), JournalBatches: b.jr.count(),
		}
		if b.segs != nil {
			l.ManifestBytes = b.segs.manifestBytes.Load()
		}
		s.Local = l
		nr := make(map[string]string, len(b.st.notRetirable))
		for k, v := range b.st.notRetirable {
			nr[k] = v
		}
		s.Retire = &backupRetire{Last: orNull(b.st.retireLast), NotRetirable: nr, LastError: b.st.retireErr}
	}
	return s
}

// stop ends the backup, after the sequencer has stopped and the store has been
// closed (or left open): the goroutine of the cadence is stopped, what was added
// since the last manifest is cut once more, within backupStopTimeout, the journal's
// entries that cut holds are deleted in the time that is left, and the cutter is
// closed. A cut that fails is said so: the backup then ends before the
// store, and the next start says by how much. Nothing the backup does reads the
// store, so it runs whether the store was closed or not.
func (b *backupRun) stop() {
	if b == nil {
		return
	}
	if b.cancel != nil { // the loop was started
		b.cancel()
		<-b.done
	}
	b.jdead.Store(true)
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.cutter == nil || b.closed {
		return
	}
	if !b.broken && b.work {
		ctx, cancel := context.WithTimeout(context.Background(), backupStopTimeout)
		start := time.Now()
		m, err := b.cutter.Cut(ctx)
		if err != nil {
			b.log.Error("the last cut of the backup failed; the backup ends before the store, and the next start says by how much",
				"timeout", backupStopTimeout, "err", err)
		} else {
			b.work = false
			b.setLocal(m)
			b.log.Info("backup cut at the stop", "generation", m.Generation, "seq", m.Seq, "files", len(m.Files), "took", time.Since(start))
			// The journal's entries the manifest holds are not needed by the next
			// start, which deletes them if they stay: the time left is the cut's.
			if b.jr != nil {
				b.jr.through(ctx, m.Seq)
			}
		}
		cancel()
	}
	if err := b.cutter.Close(); err != nil {
		b.log.Error("the backup did not close cleanly", "err", err)
	}
	b.closed = true
}
