package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/lotannauo/toposhift/internal/backup"
	"github.com/lotannauo/toposhift/internal/catalog"
	"github.com/lotannauo/toposhift/internal/identity"
	"github.com/lotannauo/toposhift/internal/ingest/sequencer"
	"github.com/lotannauo/toposhift/internal/segment"
	"github.com/lotannauo/toposhift/internal/segment/localdir"
	"github.com/lotannauo/toposhift/internal/store"
	"github.com/lotannauo/toposhift/internal/store/memstore"
	"github.com/lotannauo/toposhift/internal/store/pebblestore"
	"github.com/lotannauo/toposhift/internal/store/storetest"
)

// A zero-config serve is what it was before the backup existed: the log's
// messages in their order, the routes' answers, the keys of GET /status, and the
// data directory's contents are those taken at the commit before the backup was
// added, except that GET /status says the backup is off. The same holds with
// --backup off and with $TOPOSHIFT_BACKUP=off, and nothing is written for a backup.
func TestBackupOffLeavesServeAsItWas(t *testing.T) {
	t.Parallel()
	// The messages of serve with a data directory and nothing else, at the base commit.
	golden := []string{
		"data directory", "listening", "store opened", "recorded the cluster name", "ingest is on",
		"retention is on: once a day, L2 keeps 30 days, L1 90 days, and L0 is kept",
		"stopped", "server stopped", "tools stopped", "sequencer stopped", "store closed",
	}
	routes := []struct {
		method, path string
		want         int
	}{
		{http.MethodGet, "/healthz", http.StatusOK},
		{http.MethodGet, "/status", http.StatusOK},
		{http.MethodGet, "/mcp", http.StatusMethodNotAllowed},
		{http.MethodPost, "/mcp", http.StatusUnsupportedMediaType},
		{http.MethodGet, "/v1/logs", http.StatusMethodNotAllowed},
	}
	for _, tt := range []struct {
		name string
		env  map[string]string
		args []string
	}{
		{"nothing given", nil, nil},
		{"the flag", nil, []string{"--backup", "off"}},
		{"the variable", map[string]string{"TOPOSHIFT_BACKUP": "off"}, nil},
		{"an empty variable", map[string]string{"TOPOSHIFT_BACKUP": ""}, nil},
	} {
		t.Run(tt.name, func(t *testing.T) {
			dir := t.TempDir()
			env := map[string]string{"TOPOSHIFT_DATA_DIR": dir}
			for k, v := range tt.env {
				env[k] = v
			}
			s := startServe(t, testEnv(t, env), append([]string{"--retain", "on"}, tt.args...)...)
			waitIngestOn(t, s.base)
			postOK(t, s.base, podBody("pod-1", time.Now()))
			for _, r := range routes {
				if got := statusOf(t, r.method, s.base+r.path); got != r.want {
					t.Errorf("%s %s = %d, want %d", r.method, r.path, got, r.want)
				}
			}
			code, body := getBody(t, s.base+"/status")
			var st map[string]json.RawMessage
			if err := json.Unmarshal([]byte(body), &st); code != http.StatusOK || err != nil {
				t.Fatalf("GET /status = %d %q (%v)", code, body, err)
			}
			var keys []string
			for k := range st {
				keys = append(keys, k)
			}
			slices.Sort(keys)
			if want := []string{"backup", "metrics", "nodeagent", "resume", "retention"}; !slices.Equal(keys, want) {
				t.Errorf("GET /status has the keys %v, want %v", keys, want)
			}
			if got := string(st["backup"]); got != `{"mode":"off"}` {
				t.Errorf(`"backup" = %s, want {"mode":"off"}`, got)
			}
			stopOK(t, s)
			if got := logMessages(s.stderr.String()); !slices.Equal(got, golden) {
				t.Errorf("the log's messages are\n%q\nwant\n%q", got, golden)
			}
			entries, err := os.ReadDir(dir)
			if err != nil {
				t.Fatal(err)
			}
			var names []string
			for _, e := range entries {
				names = append(names, e.Name())
			}
			if want := []string{"bus", "meta", "store"}; !slices.Equal(names, want) {
				t.Errorf("the data directory holds %v, want %v", names, want)
			}
		})
	}
}

var msgRE = regexp.MustCompile(`\bmsg=("(?:[^"\\]|\\.)*"|\S+)`)

// logMessages are the messages of the log lines of serve, in order, without the
// warning that the data directory is in the temp directory, which depends on where
// the tests run.
func logMessages(log string) []string {
	var out []string
	for line := range strings.SplitSeq(log, "\n") {
		m := msgRE.FindStringSubmatch(line)
		if m == nil {
			continue
		}
		msg := m[1]
		if strings.HasPrefix(msg, `"`) {
			if u, err := strconv.Unquote(msg); err == nil {
				msg = u
			}
		}
		if strings.Contains(msg, "is inside the temp directory") {
			continue
		}
		out = append(out, msg)
	}
	return out
}

func statusOf(t testing.TB, method, url string) int {
	t.Helper()
	req, err := http.NewRequestWithContext(t.Context(), method, url, nil)
	if err != nil {
		t.Fatal(err)
	}
	resp, err := client.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	_ = resp.Body.Close()
	return resp.StatusCode
}

// getBackup is GET /status's backup object.
func getBackup(t testing.TB, base string) backupStatus {
	t.Helper()
	code, body := getBody(t, base+"/status")
	var st struct {
		Backup backupStatus `json:"backup"`
	}
	if err := json.Unmarshal([]byte(body), &st); code != http.StatusOK || err != nil {
		t.Fatalf("GET /status = %d %q (%v)", code, body, err)
	}
	return st.Backup
}

// waitBackup polls GET /status until the backup's status satisfies ok.
func waitBackup(t testing.TB, s *served, what string, ok func(backupStatus) bool) backupStatus {
	t.Helper()
	deadline := time.Now().Add(20 * time.Second)
	for {
		st := getBackup(t, s.base)
		if ok(st) {
			return st
		}
		if time.Now().After(deadline) {
			t.Fatalf("the backup never %s: %+v\n%s", what, st, s.stderr)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// --backup and --backup-every, and their variables: a mistake is exit 2 before
// anything listens; the flag wins over the variable; the status says what set the
// backup and the interval.
func TestBackupFlags(t *testing.T) {
	t.Parallel()
	for _, tt := range []struct {
		name    string
		env     map[string]string
		args    []string
		wantErr string
	}{
		{"a mode that is neither", nil, []string{"--backup", "maybe"}, `--backup "maybe": give on or off`},
		{"a mode in the variable that is neither", map[string]string{"TOPOSHIFT_BACKUP": "yes"}, nil, `--backup "yes"`},
		{"an interval under five minutes", nil, []string{"--backup-every", "4m"}, "--backup-every 4m0s is outside"},
		{"an interval over a day", nil, []string{"--backup-every", "25h"}, "--backup-every 25h0m0s is outside"},
		{"an interval with seconds", nil, []string{"--backup-every", "90s"}, "is not a whole number of minutes"},
		{"an interval of zero", nil, []string{"--backup-every", "0s"}, "--backup-every 0s is outside"},
		{"an interval in the variable that is not a duration", map[string]string{"TOPOSHIFT_BACKUP_EVERY": "soon"}, nil, "TOPOSHIFT_BACKUP_EVERY"},
		{"an interval in the variable out of bounds", map[string]string{"TOPOSHIFT_BACKUP_EVERY": "1m"}, nil, "--backup-every 1m0s is outside"},
		{"an interval out of bounds with the backup off", nil, []string{"--backup", "off", "--backup-every", "1m"}, "--backup-every 1m0s is outside"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			var stderr bytes.Buffer
			code := run(t.Context(), append([]string{"serve", "--listen", "127.0.0.1:0"}, tt.args...), testEnv(t, tt.env), io.Discard, &stderr)
			if code != 2 || !strings.Contains(stderr.String(), tt.wantErr) || strings.Contains(stderr.String(), "listening") {
				t.Errorf("exit %d, stderr %q, want exit 2 and %q", code, &stderr, tt.wantErr)
			}
		})
	}

	for _, tt := range []struct {
		name      string
		env       map[string]string
		args      []string
		wantSetBy string
		wantEvery string
	}{
		{"the flag", nil, []string{"--backup", "on"}, "flag", "1h0m0s"},
		{"the variable", map[string]string{"TOPOSHIFT_BACKUP": "on"}, nil, "environment", "1h0m0s"},
		{"the flag over the variable", map[string]string{"TOPOSHIFT_BACKUP": "off"}, []string{"--backup", "on"}, "flag", "1h0m0s"},
		{"an interval in the flag", nil, []string{"--backup", "on", "--backup-every", "15m"}, "flag", "15m0s"},
		{"an interval in the variable", map[string]string{"TOPOSHIFT_BACKUP": "on", "TOPOSHIFT_BACKUP_EVERY": "90m"}, nil, "environment", "1h30m0s"},
		{"an interval in the flag over the variable", map[string]string{"TOPOSHIFT_BACKUP": "on", "TOPOSHIFT_BACKUP_EVERY": "90m"}, []string{"--backup-every", "24h"}, "environment", "24h0m0s"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			env := map[string]string{"TOPOSHIFT_DATA_DIR": t.TempDir()}
			for k, v := range tt.env {
				env[k] = v
			}
			s := startServe(t, testEnv(t, env), tt.args...)
			waitIngestOn(t, s.base)
			st := getBackup(t, s.base)
			if st.Mode != backupOn || st.SetBy != tt.wantSetBy || st.Every != tt.wantEvery {
				t.Errorf("backup status %+v, want on, set by %s, every %s", st, tt.wantSetBy, tt.wantEvery)
			}
			stopOK(t, s)
		})
	}
}

// serve -h documents the backup.
func TestServeHelpDocumentsTheBackup(t *testing.T) {
	t.Parallel()
	var stderr bytes.Buffer
	if code := run(t.Context(), []string{"serve", "-h"}, noEnv, io.Discard, &stderr); code != 0 {
		t.Fatalf("exit code = %d", code)
	}
	for _, want := range []string{
		"-backup", "TOPOSHIFT_BACKUP", "-backup-every", "TOPOSHIFT_BACKUP_EVERY", "Backup",
		"from the first boot", `"backup"`, "backup/",
	} {
		if !strings.Contains(stderr.String(), want) {
			t.Errorf("serve -h does not mention %q", want)
		}
	}
}

// A backup asked for with no data directory boots without it, warns once, and
// the status says why nothing is backed up.
func TestBackupWithNoDataDirectory(t *testing.T) {
	t.Parallel()
	s := startServe(t, noEnv, "--backup", "on")
	st := getBackup(t, s.base)
	if st.Mode != backupOff || !strings.Contains(st.Reason, "no data directory") || st.SetBy != "flag" {
		t.Errorf("backup status %+v, want off for want of a data directory", st)
	}
	stopOK(t, s)
	if n := strings.Count(s.stderr.String(), "backup is on but there is no data directory; nothing is backed up"); n != 1 {
		t.Errorf("the warning was logged %d times, want once:\n%s", n, s.stderr)
	}
}

// fakeTicks stands for the timers of the backup: a test fires the cadence's, or the
// nag's, itself. The nag's interval is set apart from the cadence's.
type fakeTicks struct {
	mu sync.Mutex
	ch map[time.Duration]chan time.Time
}

const fakeNag = 7 * time.Minute

func installTicks(t *testing.T) *fakeTicks {
	t.Helper()
	f := &fakeTicks{ch: map[time.Duration]chan time.Time{}}
	setSeam(t, &backupNagEvery, fakeNag)
	setSeam(t, &backupTicker, func(d time.Duration) (<-chan time.Time, func()) {
		f.mu.Lock()
		defer f.mu.Unlock()
		c := make(chan time.Time)
		f.ch[d] = c
		return c, func() {}
	})
	return f
}

// fire sends one tick of the timer of interval d, and returns once the backup's
// loop has taken it; what the tick starts may still be running.
func (f *fakeTicks) fire(t testing.TB, d time.Duration) {
	t.Helper()
	deadline := time.Now().Add(20 * time.Second)
	for {
		f.mu.Lock()
		c := f.ch[d]
		f.mu.Unlock()
		if c != nil {
			select {
			case c <- time.Now():
				return
			case <-time.After(20 * time.Second):
				t.Fatalf("the timer of %s was not taken", d)
			}
		}
		if time.Now().After(deadline) {
			t.Fatalf("no timer of %s was made", d)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// recordingWriter keeps the records the store committed, whatever the write
// returned, as the sequencer reads it.
type recordingWriter struct {
	sequencer.Writer
	st      *pebblestore.Store
	mu      sync.Mutex
	written []store.Record
}

func (w *recordingWriter) Write(ctx context.Context, batch []store.Record) error {
	err := w.Writer.Write(ctx, batch)
	if len(batch) > 0 && w.st.LastSeq() >= batch[len(batch)-1].Seq {
		w.mu.Lock()
		for _, r := range batch {
			r.Payload = bytes.Clone(r.Payload)
			w.written = append(w.written, r)
		}
		w.mu.Unlock()
	}
	return err
}

func (w *recordingWriter) records() []store.Record {
	w.mu.Lock()
	defer w.mu.Unlock()
	return slices.Clone(w.written)
}

// storedLastSeq is the last Seq of the store a stopped serve left in dir.
func storedLastSeq(t testing.TB, dir string) uint64 {
	t.Helper()
	st, err := pebblestore.Open(filepath.Join(dir, "store"), serveStoreOptions("", false, nil))
	if err != nil {
		t.Fatal(err)
	}
	last := st.LastSeq()
	if err := st.Close(); err != nil {
		t.Fatal(err)
	}
	return last
}

func stoppedBackup(t testing.TB, dir string) (backup.Manifest, segment.Store) {
	t.Helper()
	seg, err := localdir.New(filepath.Join(dir, backupDirName, backupSegmentsName))
	if err != nil {
		t.Fatal(err)
	}
	m, err := backup.Latest(t.Context(), seg, backup.DefaultPrefix)
	if err != nil {
		t.Fatalf("the stopped serve left no backup: %v", err)
	}
	return m, seg
}

// serve with --backup on, fed a stream and retained, stopped cleanly: the backup
// in backup/segments restores into a memstore and a pebblestore that answer as the
// live store does, at its horizons and after them. The only cut is the stop's, so
// it covers what the sequencer wrote as it stopped (the refresh the coalescer
// absorbed), and the manifest ends at the store's last Seq. Every file of the
// backup is in backup/segments and nothing is left in backup/tmp.
func TestBackupRestoresLikeTheLiveStore(t *testing.T) {
	setSeam(t, &retainEvery, 20*time.Millisecond)
	installTicks(t) // no tick is sent: the stop makes the only cut
	var rec *recordingWriter
	setSeam(t, &writerFor, func(st *pebblestore.Store) sequencer.Writer {
		rec = &recordingWriter{Writer: st, st: st}
		return rec
	})
	dir := t.TempDir()
	s := startServe(t, dirEnv(t, dir), "--backup", "on", "--retain", "on", "--backup-every", "15m")
	waitIngestOn(t, s.base)
	now := time.Now().Truncate(time.Second)
	for i := range 6 {
		postOK(t, s.base, podBody(fmt.Sprintf("pod-%d", i), now.Add(time.Duration(i-40)*time.Minute)))
	}
	// The retention asks the backup to retire once it has logged the retention.
	waitBackup(t, s, "retired", func(b backupStatus) bool { return b.Retire != nil && b.Retire.Last != nil })
	for i := range 4 {
		postOK(t, s.base, podBody(fmt.Sprintf("late-%d", i), now.Add(time.Duration(i-5)*time.Minute)))
	}
	// The second pull of a pod is absorbed, and written at the stop.
	postOK(t, s.base, podBody("steady", now.Add(-20*time.Minute)))
	postOK(t, s.base, podBody("steady", now.Add(-10*time.Minute)))
	stopOK(t, s)

	live := openStopped(t, dir)
	m, seg := stoppedBackup(t, dir)
	if m.Seq != live.LastSeq() {
		t.Fatalf("the backup ends at seq %d, the store at %d", m.Seq, live.LastSeq())
	}
	if len(m.Retentions) == 0 {
		t.Fatal("the backup logged no retention")
	}
	written := rec.records()
	if uint64(len(written)) != live.LastSeq() {
		t.Fatalf("%d records were committed through the writer, the store's last seq is %d", len(written), live.LastSeq())
	}
	if m.Store.Keep != storeOptions().Keep || m.Store.Offsets != storeOptions().Offsets {
		t.Errorf("the backup's meta %+v is not the store's", m.Store)
	}
	log := s.stderr.String()
	if i, j, k := strings.Index(log, "sequencer stopped"), strings.Index(log, "store closed"), strings.Index(log, "backup cut at the stop"); i < 0 || j < i || k < j {
		t.Errorf("the stop's cut is not after the sequencer and the store (%d, %d, %d):\n%s", i, j, k, log)
	}
	if entries, err := os.ReadDir(filepath.Join(dir, backupDirName, backupTmpName)); err != nil || len(entries) != 0 {
		t.Errorf("backup/tmp holds %v (%v), want nothing", entries, err)
	}

	// The live store as the backup saw it: every committed record, retained as the
	// store was.
	meta := m.Store
	opts := storeOptions()
	liveMem, err := memstore.Open(memstore.Options{Policy: opts.Policy, Offsets: opts.Offsets, Keep: opts.Keep})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = liveMem.Close() })
	if err := liveMem.Write(t.Context(), written); err != nil {
		t.Fatal(err)
	}
	h := live.LayerHorizon(catalog.L2)
	if h.IsZero() {
		t.Fatal("the store was not retained")
	}
	if err := liveMem.Retain(t.Context(), h.Time); err != nil {
		t.Fatal(err)
	}
	seen := map[identity.Fingerprint]bool{}
	var entities []identity.Fingerprint
	for _, r := range written {
		for _, fp := range []identity.Fingerprint{r.Subject.A, r.Subject.B} {
			if fp != (identity.Fingerprint{}) && !seen[fp] {
				seen[fp] = true
				entities = append(entities, fp)
			}
		}
	}
	cmpOpts := storetest.RestoredOptions{Entities: entities, Written: written, Span: time.Minute}
	for _, engine := range []string{"memstore", "pebblestore"} {
		var dst store.Store
		if engine == "memstore" {
			dst, err = memstore.Open(memstore.Options{Policy: opts.Policy, Offsets: opts.Offsets, Keep: opts.Keep})
		} else {
			dst, err = pebblestore.Open(filepath.Join(t.TempDir(), "store"), storeOptions())
		}
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = dst.Close() })
		res, err := backup.Restore(t.Context(), seg, m, dst, backup.RestoreOptions{Store: meta, TempDir: t.TempDir()})
		if err != nil {
			t.Fatalf("restore into %s: %v", engine, err)
		}
		if res.Records != int64(len(written)) {
			t.Errorf("restore into %s wrote %d records, want %d", engine, res.Records, len(written))
		}
		if err := storetest.CompareRestored(liveMem, dst, cmpOpts); err != nil {
			t.Errorf("the restore into %s answers differently from the live store: %v", engine, err)
		}
	}
}

// The status of a backup that runs: the manifest it stands at, its last cut, and
// the files and the manifest's size.
func TestBackupStatusAfterACut(t *testing.T) {
	ticks := installTicks(t)
	dir := t.TempDir()
	s := startServe(t, dirEnv(t, dir), "--backup", "on", "--backup-every", "15m")
	waitIngestOn(t, s.base)
	st := getBackup(t, s.base)
	if st.Mode != backupOn || st.Local != nil && st.Local.Generation != 0 || st.Retire != nil && st.Retire.Last != nil {
		t.Fatalf("before anything is added: %+v", st)
	}
	postOK(t, s.base, podBody("pod-1", time.Now()))
	ticks.fire(t, 15*time.Minute)
	st = waitBackup(t, s, "cut", func(b backupStatus) bool { return b.Local != nil && b.Local.Generation == 1 })
	l := st.Local
	if l.Seq == 0 || l.LastCut == nil || l.LastCutError != "" || l.Files == 0 || l.ManifestBytes == 0 {
		t.Errorf("local status %+v after a cut", l)
	}
	if st.Retire == nil || st.Retire.NotRetirable == nil || st.Retire.Last != nil {
		t.Errorf("retire status %+v, want an empty object and no retirement", st.Retire)
	}
	stopOK(t, s)
	if m, _ := stoppedBackup(t, dir); m.Seq != l.Seq {
		t.Errorf("the manifest ends at seq %d, the status said %d", m.Seq, l.Seq)
	}
}

// faultWriter passes every write to the store but the nth one with records, which
// it fails: after the store committed the batch, or without writing it.
type faultWriter struct {
	sequencer.Writer
	n     *atomic.Int64
	at    int64
	after bool
}

func (w faultWriter) Write(ctx context.Context, batch []store.Record) error {
	if len(batch) == 0 || w.n.Add(1) != w.at {
		return w.Writer.Write(ctx, batch)
	}
	if w.after {
		_ = w.Writer.Write(ctx, batch)
		return errors.New("injected failure after the commit")
	}
	return errors.New("injected failure before the commit")
}

// A write that returned an error after the store committed the batch is in the
// backup, since its Seqs are not reused: the next batch follows it, and the backup
// stays on and ends at the store's last Seq. A write that did not store its batch
// is not in the backup, and the retry, which takes the same Seqs, is.
func TestBackupFollowsTheStoresCommitsAndNotTheErrors(t *testing.T) {
	for _, tt := range []struct {
		name  string
		after bool
	}{{"the write failed after the commit", true}, {"the write failed before the commit", false}} {
		t.Run(tt.name, func(t *testing.T) {
			installTicks(t)
			var n atomic.Int64
			setSeam(t, &writerFor, func(st *pebblestore.Store) sequencer.Writer {
				return faultWriter{Writer: st, n: &n, at: 2, after: tt.after}
			})
			dir := t.TempDir()
			s := startServe(t, dirEnv(t, dir), "--backup", "on", "--backup-every", "15m")
			waitIngestOn(t, s.base)
			now := time.Now()
			postOK(t, s.base, podBody("pod-1", now))
			status, _, text, err := post(t.Context(), s.base, podBody("pod-2", now))
			switch {
			case err != nil:
				t.Fatal(err)
			case tt.after && status != http.StatusOK:
				t.Fatalf("the batch was stored and its request answered %d %q", status, text)
			case !tt.after && status == http.StatusOK:
				t.Fatal("the batch was not stored and its request was answered 200")
			}
			if !tt.after {
				// The sequencer asks the sender to retry; the retry takes the same Seqs.
				postOK(t, s.base, podBody("pod-2", now))
			}
			postOK(t, s.base, podBody("pod-3", now))
			if st := getBackup(t, s.base); st.Mode != backupOn {
				t.Fatalf("the backup is %+v after a failed write, want on:\n%s", st, s.stderr)
			}
			stopOK(t, s)
			m, _ := stoppedBackup(t, dir)
			st := openStopped(t, dir)
			if m.Seq != st.LastSeq() {
				t.Errorf("the backup ends at seq %d, the store at %d", m.Seq, st.LastSeq())
			}
			for _, uid := range []string{"pod-1", "pod-2", "pod-3"} {
				if len(podRecords(t, st, uid)) == 0 {
					t.Errorf("%s is not in the store", uid)
				}
			}
		})
	}
}

// failingSegments fails every Put while armed.
type failingSegments struct {
	segment.Store
	armed atomic.Bool
}

func (f *failingSegments) Put(ctx context.Context, name string, r io.Reader, size int64) (segment.Info, error) {
	if f.armed.Load() {
		return segment.Info{}, errors.New("injected put failure")
	}
	return f.Store.Put(ctx, name, r, size)
}

// A segment store that fails does not fail a request or a read: every request is
// answered 200, the status carries the error of the last cut (without the path of
// the data directory), the files are kept, and the next interval's cut succeeds.
func TestBackupFailureNeverReachesIngest(t *testing.T) {
	ticks := installTicks(t)
	fail := &failingSegments{}
	setSeam(t, &openSegments, func(root string) (segment.Store, error) {
		d, err := localdir.New(root)
		fail.Store = d
		return fail, err
	})
	dir := t.TempDir()
	s := startServe(t, dirEnv(t, dir), "--backup", "on", "--backup-every", "15m")
	waitIngestOn(t, s.base)
	postOK(t, s.base, podBody("pod-1", time.Now()))
	fail.armed.Store(true)
	ticks.fire(t, 15*time.Minute)
	st := waitBackup(t, s, "failed a cut", func(b backupStatus) bool { return b.Local != nil && b.Local.LastCutError != "" })
	if !strings.Contains(st.Local.LastCutError, "injected put failure") || strings.Contains(st.Local.LastCutError, dir) {
		t.Errorf("last_cut_error = %q", st.Local.LastCutError)
	}
	if st.Mode != backupOn || st.Local.Generation != 0 {
		t.Errorf("status %+v after a failed cut, want on with no manifest", st)
	}
	postOK(t, s.base, podBody("pod-2", time.Now()))
	if code := get(t, s.base+"/healthz"); code != http.StatusOK {
		t.Errorf("GET /healthz = %d", code)
	}
	fail.armed.Store(false)
	ticks.fire(t, 15*time.Minute)
	st = waitBackup(t, s, "cut again", func(b backupStatus) bool { return b.Local != nil && b.Local.Generation == 1 })
	if st.Local.LastCutError != "" || st.Local.LastCut == nil {
		t.Errorf("local status %+v after the next cut", st.Local)
	}
	stopOK(t, s)
	m, _ := stoppedBackup(t, dir)
	if m.Seq != storedLastSeq(t, dir) {
		t.Errorf("the backup ends at seq %d, not at the store's last", m.Seq)
	}
}

// A cutter that cannot take a batch (here, one that cannot make its file) breaks
// the backup, and only the backup: the request is answered 200, the next ones too,
// the store holds what it was sent, the status says broken, the log says so again
// at each nag, and the stop closes without a cut.
func TestBackupBrokenCutterNeverReachesIngest(t *testing.T) {
	ticks := installTicks(t)
	dir := t.TempDir()
	s := startServe(t, dirEnv(t, dir), "--backup", "on", "--backup-every", "15m")
	waitIngestOn(t, s.base)
	if err := os.RemoveAll(filepath.Join(dir, backupDirName, backupTmpName)); err != nil {
		t.Fatal(err)
	}
	postOK(t, s.base, podBody("pod-1", time.Now()))
	st := waitBackup(t, s, "broke", func(b backupStatus) bool { return b.Mode == backupBroken })
	if !strings.Contains(st.Reason, "could not be added") || strings.Contains(st.Reason, dir) {
		t.Errorf("reason %q", st.Reason)
	}
	postOK(t, s.base, podBody("pod-2", time.Now()))
	if code := get(t, s.base+"/healthz"); code != http.StatusOK {
		t.Errorf("GET /healthz = %d", code)
	}
	ticks.fire(t, fakeNag)
	deadline := time.Now().Add(15 * time.Second)
	for strings.Count(s.stderr.String(), "backup is broken") < 2 {
		if time.Now().After(deadline) {
			t.Fatalf("the nag was not logged:\n%s", s.stderr)
		}
		time.Sleep(10 * time.Millisecond)
	}
	stopOK(t, s)
	if strings.Contains(s.stderr.String(), "backup cut at the stop") {
		t.Errorf("a broken backup was cut at the stop:\n%s", s.stderr)
	}
	held := openStopped(t, dir)
	for _, uid := range []string{"pod-1", "pod-2"} {
		if len(podRecords(t, held, uid)) == 0 {
			t.Errorf("%s is not in the store", uid)
		}
	}
}

// The start states of a backup in serve. A store that holds records and no backup
// is refused, and so is one that ends before its backup; a store ahead of its
// backup is broken. Each is logged and reported, and ingest takes requests all the
// same.
func TestBackupStartStatesInServe(t *testing.T) {
	installTicks(t)
	boot := func(t *testing.T, dir string, args ...string) *served {
		t.Helper()
		s := startServe(t, dirEnv(t, dir), args...)
		waitIngestOn(t, s.base)
		return s
	}
	t.Run("a store that predates the backup", func(t *testing.T) {
		dir := t.TempDir()
		s := boot(t, dir)
		postOK(t, s.base, podBody("pod-1", time.Now()))
		stopOK(t, s)
		s = boot(t, dir, "--backup", "on")
		st := getBackup(t, s.base)
		if st.Mode != backupRefused || !strings.Contains(st.Reason, "predates") || st.Local != nil {
			t.Errorf("status %+v, want refused: the store predates the backup", st)
		}
		postOK(t, s.base, podBody("pod-2", time.Now()))
		stopOK(t, s)
		if _, err := os.Stat(filepath.Join(dir, backupDirName, backupSegmentsName, "backup")); err == nil {
			t.Error("a refused backup wrote segments")
		}
		if !strings.Contains(s.stderr.String(), "backup is refused") {
			t.Errorf("the refusal is not logged:\n%s", s.stderr)
		}
	})
	t.Run("a store behind its backup", func(t *testing.T) {
		dir := t.TempDir()
		s := boot(t, dir, "--backup", "on")
		postOK(t, s.base, podBody("pod-1", time.Now()))
		stopOK(t, s)
		if err := os.RemoveAll(filepath.Join(dir, "store")); err != nil {
			t.Fatal(err)
		}
		s = boot(t, dir, "--backup", "on")
		st := getBackup(t, s.base)
		if st.Mode != backupRefused || !strings.Contains(st.Reason, "behind its own backup") {
			t.Errorf("status %+v, want refused: the store is behind its backup", st)
		}
		postOK(t, s.base, podBody("pod-2", time.Now()))
		stopOK(t, s)
	})
	t.Run("a store ahead of its backup", func(t *testing.T) {
		dir := t.TempDir()
		s := boot(t, dir, "--backup", "on")
		postOK(t, s.base, podBody("pod-1", time.Now()))
		stopOK(t, s)
		m, _ := stoppedBackup(t, dir)
		s = boot(t, dir)
		postOK(t, s.base, podBody("pod-2", time.Now()))
		stopOK(t, s)
		last := storedLastSeq(t, dir)
		s = boot(t, dir, "--backup", "on")
		st := getBackup(t, s.base)
		want := fmt.Sprintf("the store is ahead of its backup by seqs %d..%d", m.Seq+1, last)
		if st.Mode != backupBroken || !strings.Contains(st.Reason, want) {
			t.Errorf("status %+v, want broken: %s", st, want)
		}
		if st.Local == nil || st.Local.Seq != m.Seq {
			t.Errorf("local status %+v, want the manifest at seq %d", st.Local, m.Seq)
		}
		postOK(t, s.base, podBody("pod-3", time.Now()))
		stopOK(t, s)
		if m2, _ := stoppedBackup(t, dir); m2.Seq != m.Seq {
			t.Errorf("a broken backup moved from seq %d to %d", m.Seq, m2.Seq)
		}
	})
	t.Run("a cluster name the backup does not hold", func(t *testing.T) {
		dir := t.TempDir()
		s := boot(t, dir, "--backup", "on", "--cluster-name", "one")
		postOK(t, s.base, podBody("pod-1", time.Now()))
		stopOK(t, s)
		s = boot(t, dir, "--backup", "on", "--cluster-name", "two", "--accept-new-cluster-name=two")
		st := getBackup(t, s.base)
		if st.Mode != backupRefused || !strings.Contains(st.Reason, "differs from the backup's") {
			t.Errorf("status %+v, want refused: the meta differs", st)
		}
		postOK(t, s.base, podBody("pod-2", time.Now()))
		stopOK(t, s)
	})
	t.Run("a store and a backup that agree resume", func(t *testing.T) {
		dir := t.TempDir()
		s := boot(t, dir, "--backup", "on")
		postOK(t, s.base, podBody("pod-1", time.Now()))
		stopOK(t, s)
		first, _ := stoppedBackup(t, dir)
		s = boot(t, dir, "--backup", "on")
		st := getBackup(t, s.base)
		if st.Mode != backupOn || st.Local == nil || st.Local.Seq != first.Seq || st.Local.Generation != first.Generation ||
			st.Local.ManifestBytes == 0 {
			t.Errorf("status %+v, want on at the first run's manifest %d/%d", st, first.Generation, first.Seq)
		}
		postOK(t, s.base, podBody("pod-2", time.Now()))
		stopOK(t, s)
		second, _ := stoppedBackup(t, dir)
		if last := storedLastSeq(t, dir); second.Generation != first.Generation+1 || second.Seq != last || second.Seq <= first.Seq {
			t.Errorf("manifests %d/%d then %d/%d, store at %d", first.Generation, first.Seq, second.Generation, second.Seq, last)
		}
	})
}

// The backup's own goroutine and files, without serve around them: a rig of a
// store of one engine, a backup run on a directory, and the tee on both.
type rig struct {
	dir  string
	run  *backupRun
	st   rigStore
	logs *lockedBuffer
	meta backup.StoreMeta
}

type rigStore interface {
	sequencer.Writer
	sequencer.Retainer
}

var rigMeta = backup.StoreMeta{Offsets: [4]time.Duration{0, 30 * time.Minute, 0, 0}, Keep: [4]bool{true, false, false, false}}

func rigMemstore(t testing.TB, meta backup.StoreMeta) *memstore.Store {
	t.Helper()
	st, err := memstore.Open(memstore.Options{Offsets: meta.Offsets, Keep: meta.Keep})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = st.Close() })
	return st
}

// newRig opens a backup run on dir (a new one when empty) over st, and starts its
// loop when start is true.
func newRig(t *testing.T, dir string, st rigStore, meta backup.StoreMeta, start bool) *rig {
	t.Helper()
	if dir == "" {
		dir = t.TempDir()
	}
	r := &rig{dir: dir, st: st, logs: &lockedBuffer{}, meta: meta}
	log := slog.New(slog.NewTextHandler(r.logs, nil))
	r.run = newBackupRun(backupConfig{on: true, setBy: retainByFlag, every: 15 * time.Minute}, log, dir)
	r.run.open(t.Context(), st, meta)
	if start {
		r.run.start()
	}
	return r
}

func (r *rig) writer() sequencer.Writer { return r.run.writer(r.st, r.st) }

// feed writes n batches of the generator through the tee.
func feed(t *testing.T, w sequencer.Writer, g *storetest.Generator, batches int) {
	t.Helper()
	for range batches {
		b := g.Batch(20)
		if len(b) == 0 {
			t.Fatal("the generator ran out")
		}
		if err := w.Write(t.Context(), b); err != nil {
			t.Fatal(err)
		}
	}
}

func newGen(t *testing.T) *storetest.Generator {
	t.Helper()
	g, err := storetest.NewGenerator(storetest.Tiny())
	if err != nil {
		t.Fatal(err)
	}
	return g
}

// The cadence: a cut for each interval in which something was added, none for an
// interval in which nothing was, and none for a batch.
func TestBackupCutsOnTheCadenceAndNotPerBatch(t *testing.T) {
	ticks := installTicks(t)
	st := rigMemstore(t, rigMeta)
	r := newRig(t, "", st, rigMeta, true)
	w, gn := r.writer(), newGen(t)
	generation := func() uint64 { return r.run.status().Local.Generation }
	// A tick is taken once the cut of the tick before it has ended, so two ticks in a
	// row settle the first.
	interval := func() {
		ticks.fire(t, 15*time.Minute)
		ticks.fire(t, 15*time.Minute)
	}
	// An interval with nothing added writes no manifest, not even a first one.
	interval()
	if _, err := backup.Latest(t.Context(), r.run.segs, backup.DefaultPrefix); !errors.Is(err, backup.ErrNoBackup) {
		t.Fatalf("a manifest was written for an interval with nothing added: %v", err)
	}
	feed(t, w, gn, 3)
	if got := generation(); got != 0 {
		t.Fatalf("generation %d with no interval passed, want none", got)
	}
	interval()
	if got := generation(); got != 1 {
		t.Fatalf("generation %d after an interval with three batches, want 1", got)
	}
	interval() // nothing was added
	interval()
	if got := generation(); got != 1 {
		t.Fatalf("generation %d after idle intervals, want 1", got)
	}
	feed(t, w, gn, 1)
	interval()
	if got := generation(); got != 2 {
		t.Fatalf("generation %d after another interval with a batch, want 2", got)
	}
	r.run.stop()
	m, _ := stoppedBackup(t, r.dir)
	if m.Generation != 2 || m.Seq != st.LastSeq() {
		t.Errorf("manifest %d at seq %d, the store at %d", m.Generation, m.Seq, st.LastSeq())
	}
}

// A Retain that returned an error after it moved the horizons has moved them, and
// the retention is logged in the backup all the same, and retires: the store's
// contract says the horizons are moved whole or not at all.
func TestBackupLogsARetentionThatReturnedAnError(t *testing.T) {
	st := rigMemstore(t, rigMeta)
	r := newRig(t, "", st, rigMeta, false)
	w, g := r.writer(), newGen(t)
	feed(t, w, g, 3)
	rt := r.run.retainer(errAfterRetain{st}, st)
	horizon := storetest.Tiny().Start.Add(10 * time.Minute)
	if err := rt.Retain(t.Context(), horizon); err == nil || err.Error() != "injected failure after the retention" {
		t.Fatalf("Retain returned %v, want the injected error unchanged", err)
	}
	if h := st.LayerHorizon(catalog.L2); h.IsZero() {
		t.Fatal("the injected Retain did not move the horizon")
	}
	select {
	case <-r.run.retireCh:
	default:
		t.Error("the retention did not ask the backup to retire")
	}
	r.run.cut(t.Context())
	m := r.run.cutter.Manifest()
	if len(m.Retentions) != 1 || m.Retentions[0].AskedNS != horizon.UnixNano() || m.Retentions[0].Horizons[catalog.L2-catalog.L0].IsZero() {
		t.Errorf("the backup logged %+v, want one retention asked at %s", m.Retentions, horizon)
	}
	// A Retain that moves nothing is not logged.
	if err := r.run.retainer(st, st).Retain(t.Context(), horizon); err != nil {
		t.Fatal(err)
	}
	r.run.cut(t.Context())
	if got := len(r.run.cutter.Manifest().Retentions); got != 1 {
		t.Errorf("%d retentions logged after a Retain that moved nothing, want 1", got)
	}
	r.run.stop()
}

// A retention the backup cannot log breaks it: a backup that skips a retention
// would carry horizons that do not follow from it. The writes through the tee go
// on succeeding, the next cut writes nothing, and the retirement is not asked for.
func TestBackupBreaksWhenARetentionCannotBeLogged(t *testing.T) {
	st := rigMemstore(t, rigMeta)
	r := newRig(t, "", st, rigMeta, false)
	w, g := r.writer(), newGen(t)
	feed(t, w, g, 2)
	horizon := storetest.Tiny().Start.Add(10 * time.Minute)
	if err := r.run.retainer(st, st).Retain(t.Context(), horizon); err != nil {
		t.Fatal(err)
	}
	<-r.run.retireCh // the first retention asked for a retirement
	// The cutter refuses an ask earlier than the one it logged.
	r.run.retained(horizon.Add(-time.Minute), horizonsOf(st), st.LastSeq())
	if s := r.run.status(); s.Mode != backupBroken || !strings.Contains(s.Reason, "a retention could not be logged") {
		t.Fatalf("status %+v, want broken: a retention could not be logged", s)
	}
	select {
	case <-r.run.retireCh:
		t.Error("a retention that could not be logged asked for a retirement")
	default:
	}
	if err := w.Write(t.Context(), nil); err != nil { // the tee takes nothing, and the write is the store's
		t.Fatal(err)
	}
	r.run.add(g.Batch(20)) // a batch the broken backup is given is ignored, and does not panic
	r.run.cut(t.Context())
	if _, err := backup.Latest(t.Context(), r.run.segs, backup.DefaultPrefix); !errors.Is(err, backup.ErrNoBackup) {
		t.Errorf("a broken backup was cut: %v", err)
	}
	r.run.stop()
}

// A crash between a Retain and the next cut leaves the store and the manifest at
// one Seq (a retention does not change it) with the store's horizons ahead of the
// manifest's last logged retention: the backup is broken at the next start, not
// resumed. A retention that was cut resumes.
func TestBackupDetectsARetentionThatWasNotLogged(t *testing.T) {
	horizon := storetest.Tiny().Start.Add(10 * time.Minute)
	for _, tt := range []struct {
		name     string
		cutAfter bool
		mode     string
	}{{"crashed before the cut", false, backupBroken}, {"cut after the retention", true, backupOn}} {
		t.Run(tt.name, func(t *testing.T) {
			st := rigMemstore(t, rigMeta)
			r := newRig(t, "", st, rigMeta, false)
			feed(t, r.writer(), newGen(t), 3)
			r.run.cut(t.Context())
			if err := r.run.retainer(st, st).Retain(t.Context(), horizon); err != nil {
				t.Fatal(err)
			}
			if tt.cutAfter {
				r.run.cut(t.Context())
			}
			// The process is gone: nothing is stopped or closed.
			r2 := newRig(t, r.dir, st, rigMeta, false)
			s := r2.run.status()
			if s.Mode != tt.mode {
				t.Fatalf("status %+v, want %s", s, tt.mode)
			}
			if tt.mode == backupBroken {
				if !strings.Contains(s.Reason, "a retention the store applied after the last cut is not in the backup") || r2.run.tee {
					t.Errorf("status %+v tee %v", s, r2.run.tee)
				}
				if s.Local == nil || s.Local.Seq != st.LastSeq() {
					t.Errorf("local status %+v, want the manifest at the store's last seq", s.Local)
				}
			} else if !r2.run.tee {
				t.Error("the backup does not take commits")
			}
			r2.run.stop()
		})
	}
}

// errAfterRetain retains the store and then fails.
type errAfterRetain struct{ st rigStore }

func (e errAfterRetain) Retain(ctx context.Context, h time.Time) error {
	if err := e.st.Retain(ctx, h); err != nil {
		return err
	}
	return errors.New("injected failure after the retention")
}

// The start-state table, one case for each row, on a store of memstore: the
// refused and the broken states have no tee, so the sequencer writes to the store
// as it would with no backup.
func TestBackupStartStateTable(t *testing.T) {
	// A store that holds the first n batches of the stream, and the backup of them.
	filled := func(t *testing.T, n int, meta backup.StoreMeta) (*memstore.Store, *rig) {
		t.Helper()
		st := rigMemstore(t, meta)
		r := newRig(t, "", st, meta, false)
		feed(t, r.writer(), newGen(t), n)
		r.run.stop()
		return st, r
	}
	// A second store that holds the first n batches too, without a backup.
	same := func(t *testing.T, n int, meta backup.StoreMeta) *memstore.Store {
		t.Helper()
		st := rigMemstore(t, meta)
		feed(t, st, newGen(t), n)
		return st
	}
	t.Run("a new backup on an empty store", func(t *testing.T) {
		r := newRig(t, "", rigMemstore(t, rigMeta), rigMeta, false)
		if s := r.run.status(); s.Mode != backupOn || !r.run.tee {
			t.Errorf("status %+v tee %v, want on with the tee", s, r.run.tee)
		}
		r.run.stop()
	})
	t.Run("resume", func(t *testing.T) {
		_, first := filled(t, 4, rigMeta)
		st := same(t, 4, rigMeta)
		r := newRig(t, first.dir, st, rigMeta, false)
		if s := r.run.status(); s.Mode != backupOn || !r.run.tee || s.Local.Seq != st.LastSeq() {
			t.Fatalf("status %+v tee %v, want on at seq %d", s, r.run.tee, st.LastSeq())
		}
		// The next batch follows the manifest.
		g := newGen(t)
		g.Batch(80)
		if err := r.writer().Write(t.Context(), g.Batch(20)); err != nil {
			t.Fatal(err)
		}
		if s := r.run.status(); s.Mode != backupOn {
			t.Errorf("status %+v after the next batch, want on", s)
		}
		r.run.stop()
		if m, _ := stoppedBackup(t, first.dir); m.Seq != st.LastSeq() {
			t.Errorf("the manifest ends at seq %d, the store at %d", m.Seq, st.LastSeq())
		}
	})
	for _, tt := range []struct {
		name     string
		backed   int // batches the backup holds; -1 for no backup
		stored   int // batches the store holds
		meta     backup.StoreMeta
		mode     string
		contains string
	}{
		{"the store ends before its backup", 4, 2, rigMeta, backupRefused, "behind its own backup"},
		{"the store has records and there is no backup", -1, 2, rigMeta, backupRefused, "predates the backup"},
		{"the store ends after its backup", 2, 4, rigMeta, backupBroken, "ahead of its backup by seqs 41..80"},
		{"the store's meta differs", 4, 4, backup.StoreMeta{ClusterName: "other", ClusterNameSet: true, Offsets: rigMeta.Offsets, Keep: rigMeta.Keep}, backupRefused, "differs from the backup's"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			dir := ""
			if tt.backed >= 0 {
				_, b := filled(t, tt.backed, rigMeta)
				dir = b.dir
			}
			st := same(t, tt.stored, tt.meta)
			r := newRig(t, dir, st, tt.meta, false)
			if s := r.run.status(); s.Mode != tt.mode || !strings.Contains(s.Reason, tt.contains) {
				t.Errorf("status %+v, want %s: %s", s, tt.mode, tt.contains)
			}
			if r.run.tee || r.run.cutter != nil {
				t.Error("a backup that does not run has a tee or a cutter")
			}
			if w := r.writer(); w != sequencer.Writer(st) {
				t.Error("the sequencer's writer is not the store's own")
			}
			if rt := r.run.retainer(st, st); rt != sequencer.Retainer(st) {
				t.Error("the retention's store is not the store's own")
			}
			r.run.stop()
		})
	}
}

// A stop whose tools did not drain leaves the store open, and the backup is cut
// all the same: the cut reads only the backup's files.
func TestBackupStopCutsWhenTheStoreIsLeftOpen(t *testing.T) {
	f := newStopFixture(t)
	run := newBackupRun(backupConfig{on: true, setBy: retainByFlag, every: 15 * time.Minute}, f.log, t.TempDir())
	run.open(t.Context(), f.st, backupMetaOf(serveStoreOptions("", false, nil), "", false))
	if run.status().Mode != backupOn {
		t.Fatalf("status %+v", run.status())
	}
	f.in.backup = run
	feed(t, run.writer(f.st, f.st), newGen(t), 2)
	f.in.stop(f.log, false)
	if err := probe(f.st); err != nil {
		t.Errorf("the store was closed under a read: %v", err)
	}
	m, _ := stoppedBackup(t, run.dirPath)
	if m.Seq != f.st.LastSeq() || m.Seq == 0 {
		t.Errorf("the backup ends at seq %d, the store at %d", m.Seq, f.st.LastSeq())
	}
	if !strings.Contains(f.logs.String(), "backup cut at the stop") {
		t.Errorf("the log does not say the stop cut:\n%s", f.logs)
	}
}

// backupMetaOf is the meta serve gives the cutter for a store opened with o.
func backupMetaOf(o pebblestore.Options, name string, set bool) backup.StoreMeta {
	return backup.StoreMeta{
		ClusterName: name, ClusterNameSet: set,
		BootKey: o.Policy.BootKey, Skew: o.Policy.Skew, Offsets: o.Offsets, Keep: o.Keep,
	}
}

// The backup's directories are made with mode 0700 and an existing one is left as
// it is; at start only the cutter's leftovers are deleted from backup/tmp.
func TestBackupDirectoriesAndLeftovers(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	root := filepath.Join(dir, backupDirName)
	if err := os.Mkdir(root, 0o750); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(filepath.Join(root, backupTmpName), 0o755); err != nil {
		t.Fatal(err)
	}
	keep := []string{"toposhift-backup-1.txt", "other.parquet", "s3store-put-1.tmp", "xtoposhift-backup-1.parquet"}
	gone := []string{"toposhift-backup-1.parquet", "toposhift-backup-123456.parquet"}
	for _, n := range append(slices.Clone(keep), gone...) {
		if err := os.WriteFile(filepath.Join(root, backupTmpName, n), []byte("x"), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	if err := os.Mkdir(filepath.Join(root, backupTmpName, "toposhift-backup-dir.parquet"), 0o700); err != nil {
		t.Fatal(err)
	}
	run := newBackupRun(backupConfig{on: true, every: time.Hour}, slog.New(slog.DiscardHandler), dir)
	run.open(t.Context(), rigMemstore(t, rigMeta), rigMeta)
	defer run.stop()
	modes := map[string]os.FileMode{backupDirName: 0o750, backupDirName + "/" + backupTmpName: 0o755, backupDirName + "/" + backupSegmentsName: 0o700}
	for rel, want := range modes {
		info, err := os.Stat(filepath.Join(dir, rel))
		if err != nil {
			t.Fatal(err)
		}
		if got := info.Mode().Perm(); got != want {
			t.Errorf("%s has mode %v, want %v", rel, got, want)
		}
	}
	for _, n := range gone {
		if _, err := os.Stat(filepath.Join(root, backupTmpName, n)); err == nil {
			t.Errorf("the leftover %s was not deleted", n)
		}
	}
	for _, n := range append(keep, "toposhift-backup-dir.parquet") {
		if _, err := os.Stat(filepath.Join(root, backupTmpName, n)); err != nil {
			t.Errorf("%s was deleted: %v", n, err)
		}
	}
}

// A backup directory made from nothing is private whatever the process's umask:
// the mode is set after the directory is made.
func TestMakePrivateDirsSetsTheMode(t *testing.T) {
	t.Parallel()
	dir := filepath.Join(t.TempDir(), "backup")
	if err := makePrivateDirs(dir, filepath.Join(dir, "a"), filepath.Join(dir, "b")); err != nil {
		t.Fatal(err)
	}
	for _, p := range []string{dir, filepath.Join(dir, "a"), filepath.Join(dir, "b")} {
		info, err := os.Stat(p)
		if err != nil || !info.IsDir() || info.Mode().Perm() != 0o700 {
			t.Errorf("%s: %v %v", p, info, err)
		}
	}
}

// With no flag and no variable given the backup is off, set by default, and cut
// every hour.
func TestBackupFlagsDefaults(t *testing.T) {
	t.Parallel()
	fs := flag.NewFlagSet("x", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	b := addBackupFlags(fs, func(string) string { return "" })
	if err := fs.Parse(nil); err != nil {
		t.Fatal(err)
	}
	c, err := b.resolve(fs)
	if err != nil || c.on || c.setBy != retainByDefault || c.every != time.Hour {
		t.Errorf("defaults %+v (%v), want off, by default, every hour", c, err)
	}
}
