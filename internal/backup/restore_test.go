package backup_test

import (
	"errors"
	"io"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/lotannauo/toposhift/internal/backup"
	"github.com/lotannauo/toposhift/internal/catalog"
	"github.com/lotannauo/toposhift/internal/identity"
	"github.com/lotannauo/toposhift/internal/segment"
	"github.com/lotannauo/toposhift/internal/segment/localdir"
	"github.com/lotannauo/toposhift/internal/store/storetest"
)

// smallBackup drives the tiny refreshing workload for two hours with two
// retentions, so its backup has retired files.
func smallBackup(t testing.TB, segs segment.Store) *driven {
	t.Helper()
	w := equalWorkloads(t)[0]
	d := drive{cfg: w.cfg, policy: w.policy, retainAt: []float64{0.5, 0.8}, lag: equalLag, segs: segs}
	out := d.run(t)
	if out.retired[backup.ClassL2] == 0 {
		t.Fatal("no l2 file was retired")
	}
	return out
}

func readAll(t testing.TB, segs segment.Store, name string) []byte {
	t.Helper()
	rc, err := segs.Open(ctx, name)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = rc.Close() }()
	b, err := io.ReadAll(rc)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

// classIndexes returns the indexes in m.Files of one class's files.
func classIndexes(m backup.Manifest, cl backup.Class) []int {
	var out []int
	for i, f := range m.Files {
		if f.Class == cl.String() {
			out = append(out, i)
		}
	}
	return out
}

// Each of these is refused before anything is written.
func TestRestoreRefusesABackupThatIsNotWhole(t *testing.T) {
	t.Parallel()
	segs := newMemSegments()
	out := smallBackup(t, segs)
	good, err := backup.Latest(ctx, segs, prefix)
	if err != nil {
		t.Fatal(err)
	}
	l2 := classIndexes(good, backup.ClassL2)
	if len(l2) < 3 {
		t.Fatalf("%d l2 files", len(l2))
	}
	clone := func(m backup.Manifest) backup.Manifest {
		m.Files, m.Cuts = slices.Clone(m.Files), slices.Clone(m.Cuts)
		return m
	}
	tests := []struct {
		name string
		// mod changes the manifest or the store, and restores what it changed in the
		// store with its return.
		mod  func(m *backup.Manifest) func()
		want error // what the error wraps; nil for any error
		says string
	}{
		{"a file with a byte changed", func(m *backup.Manifest) func() {
			// The same size, and a byte in the middle of the data: the reader would
			// find it only when it decodes that row group, after writing the ones before.
			name := m.Files[l2[1]].Name
			was := readAll(t, segs, name)
			changed := slices.Clone(was)
			changed[len(changed)/3] ^= 0xff
			segs.replace(name, changed)
			return func() { segs.replace(name, was) }
		}, backup.ErrIncomplete, "SHA-256"},
		{"a file whose footer disagrees", func(m *backup.Manifest) func() {
			other := m.Files[l2[0]]
			name := m.Files[l2[1]].Name
			was := readAll(t, segs, name)
			segs.replace(name, readAll(t, segs, other.Name))
			m.Files[l2[1]].Bytes, m.Files[l2[1]].SHA256 = other.Bytes, other.SHA256
			return func() { segs.replace(name, was) }
		}, backup.ErrIncomplete, "the manifest says"},
		{"a file missing from the store", func(m *backup.Manifest) func() {
			name := m.Files[l2[1]].Name
			was := readAll(t, segs, name)
			if err := segs.Delete(ctx, name); err != nil {
				t.Fatal(err)
			}
			return func() { segs.replace(name, was) }
		}, backup.ErrIncomplete, "opening"},
		{"a file in the middle missing from the manifest", func(m *backup.Manifest) func() {
			m.Files = slices.Delete(m.Files, l2[1], l2[1]+1)
			return func() {}
		}, backup.ErrIncomplete, "chain"},
		{"the last file of a class missing, with the chain intact", func(m *backup.Manifest) func() {
			last := l2[len(l2)-1]
			m.Files = slices.Delete(m.Files, last, last+1)
			return func() {}
		}, backup.ErrIncomplete, "counts"},
		{"a cut whose class count disagrees with its files", func(m *backup.Manifest) func() {
			c := &m.Cuts[len(m.Cuts)-1]
			c.Records[backup.ClassL2]++
			c.Records[backup.ClassHosts]--
			return func() {}
		}, backup.ErrIncomplete, "counts"},
		{"another store meta", func(m *backup.Manifest) func() {
			m.Store.Offsets[catalog.L1-catalog.L0] = time.Hour
			return func() {}
		}, nil, "meta"},
		{"another version", func(m *backup.Manifest) func() {
			m.Version = 2
			return func() {}
		}, backup.ErrFormat, "version"},
	}
	for _, tt := range tests {
		m := clone(good)
		undo := tt.mod(&m)
		dst := openMem(t, out.meta)
		_, err := backup.Replay(ctx, segs, m, dst, backup.RestoreOptions{Store: out.meta, TempDir: t.TempDir()})
		undo()
		switch {
		case err == nil:
			t.Errorf("%s: restored", tt.name)
		case tt.want != nil && !errors.Is(err, tt.want):
			t.Errorf("%s: %v, want %v", tt.name, err, tt.want)
		case !strings.Contains(err.Error(), tt.says):
			t.Errorf("%s: %v, want it to say %q", tt.name, err, tt.says)
		}
		if dst.LastSeq() != 0 {
			t.Errorf("%s: the store holds records through seq %d", tt.name, dst.LastSeq())
		}
	}
	// A store that is not empty is refused too.
	dst := openMem(t, out.meta)
	if err := dst.Write(ctx, out.written[:1]); err != nil {
		t.Fatal(err)
	}
	if _, err := backup.Replay(ctx, segs, good, dst, backup.RestoreOptions{Store: out.meta}); err == nil || !strings.Contains(err.Error(), "not empty") {
		t.Errorf("a store that is not empty: %v", err)
	}
	// And the backup itself, untouched, restores.
	if _, err := backup.Restore(ctx, segs, good, openMem(t, out.meta), backup.RestoreOptions{Store: out.meta, TempDir: t.TempDir()}); err != nil {
		t.Fatal(err)
	}
}

// A class's last retired file usually ends below the end of its cut, which other
// classes still hold. The cut's count of that class is exempt by the cut's
// end, not by the file's, and the restore succeeds.
func TestRestoreAfterARetirementInsideACut(t *testing.T) {
	t.Parallel()
	segs := newMemSegments()
	out := smallBackup(t, segs)
	m, err := backup.Latest(ctx, segs, prefix)
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for cl, r := range m.Retired {
		if r.ThroughSeq == 0 || r.ThroughSeq >= r.ThroughCutSeq {
			continue
		}
		for _, c := range m.Cuts {
			if c.LastSeq == r.ThroughCutSeq && c.Records[cl] > 0 {
				found = true
			}
		}
	}
	if !found {
		t.Fatal("no class's last retired file ends inside a cut still listed")
	}
	r, _ := restoreInto(t, segs, prefix, engineMem, out.meta, 10_000)
	if r.LastSeq() == 0 {
		t.Fatal("nothing restored")
	}
}

// SetHorizons retains to the largest ask logged, and requires every layer's
// horizon to be the live store's.
func TestSetHorizons(t *testing.T) {
	t.Parallel()
	segs := newMemSegments()
	out := smallBackup(t, segs)
	m, err := backup.Latest(ctx, segs, prefix)
	if err != nil {
		t.Fatal(err)
	}
	if len(m.Retentions) != 2 {
		t.Fatalf("%d retentions", len(m.Retentions))
	}
	// A log whose last ask is not its largest (the cutter never writes one).
	// Retaining at the last ask would leave the horizons short of the ones logged.
	first, second := m.Retentions[0], m.Retentions[1]
	hand := m
	hand.Retentions = []backup.Retention{
		{AskedNS: second.AskedNS, Seq: second.Seq, Horizons: second.Horizons},
		{AskedNS: first.AskedNS, Seq: second.Seq, Horizons: second.Horizons},
	}
	dst := openMem(t, out.meta)
	if err := backup.SetHorizons(ctx, hand, dst); err != nil {
		t.Fatalf("a log whose last ask is not its largest: %v", err)
	}
	if got := dst.LayerHorizon(catalog.L2).Time.UnixNano(); got != second.Horizons[catalog.L2-catalog.L0].TimeNS {
		t.Fatalf("L2 retained to %d, want %d", got, second.Horizons[2].TimeNS)
	}
	// A store opened with other offsets gets other horizons, named by layer.
	other := out.meta
	other.Offsets[catalog.L1-catalog.L0] = time.Hour
	if err := backup.SetHorizons(ctx, m, openMem(t, other)); err == nil || !strings.Contains(err.Error(), "layer L1") {
		t.Fatalf("other offsets: %v", err)
	}
	// With no retention logged nothing is retained.
	none := m
	none.Retentions = nil
	dst = openMem(t, out.meta)
	if err := backup.SetHorizons(ctx, none, dst); err != nil || !dst.LayerHorizon(catalog.L2).IsZero() {
		t.Fatalf("no retention: %v, horizon %v", err, dst.LayerHorizon(catalog.L2))
	}
}

// The backup on a real local directory restores like the one in memory.
func TestBackupOnALocalDirectory(t *testing.T) {
	t.Parallel()
	dir, err := localdir.New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	w := equalWorkloads(t)[0]
	w.cfg.Duration = time.Hour
	d := drive{cfg: w.cfg, policy: w.policy, retainAt: []float64{0.9}, lag: 20 * time.Minute, segs: dir, cutEvery: 100}
	out := d.run(t)
	if out.retired[backup.ClassL2] == 0 {
		t.Fatal("no l2 file was retired")
	}
	r, _ := restoreInto(t, dir, prefix, engineMem, out.meta, 10_000)
	opts := out.restoredOptions(w.span)
	opts.Sample = equalSample()
	compare(t, out.live, r, opts)
}

// faultCase fails one step of a cut or a retirement once.
type faultCase struct {
	name string
	op   string // "put" or "delete"
	file bool   // a file's segment, or else a manifest's
	// atCut arms the fault after that many cuts; otherwise it is armed just before
	// the first Retire.
	atCut int
}

// TestFaultsLeaveTheLatestBackupWhole is T4's fault test: each step of Cut and
// Retire fails once; the call returns an error; the latest manifest still restores
// the live store as it was through that manifest's Seq; and the stream goes on,
// and its backup restores the whole live store.
func TestFaultsLeaveTheLatestBackupWhole(t *testing.T) {
	t.Parallel()
	w := equalWorkloads(t)[0]
	cases := []faultCase{
		{name: "Cut, storing a file", op: "put", file: true, atCut: 3},
		{name: "Cut, storing the manifest", op: "put", atCut: 3},
		{name: "Retire, storing the manifest", op: "put"},
		{name: "Retire, deleting a retired file", op: "delete", file: true},
		{name: "Retire, deleting an old manifest", op: "delete"},
	}
	for _, fc := range cases {
		t.Run(fc.name, func(t *testing.T) {
			t.Parallel()
			armed, fired := false, 0
			segs := &faulty{Store: newMemSegments()}
			segs.fail = func(op, name string) bool {
				if armed && op == fc.op && isFile(name) == fc.file && (isFile(name) || isManifest(name)) {
					armed = false
					fired++
					return true
				}
				return false
			}
			var out *driven
			cuts, retires := 0, 0
			d := drive{cfg: w.cfg, policy: w.policy, retainAt: []float64{0.5, 0.8}, lag: equalLag, segs: segs}
			d.beforeRetire = func() {
				retires++
				if fc.atCut == 0 && retires == 1 {
					armed = true
				}
			}
			d.mid = func(step string, err error) {
				if step == "cut" {
					cuts++
					if fc.atCut != 0 && cuts == fc.atCut {
						armed = true
					}
				}
				if err == nil {
					return
				}
				if !errors.Is(err, errFault) {
					t.Fatalf("%s failed with %v", step, err)
				}
				m, lerr := backup.Latest(ctx, segs, prefix)
				if lerr != nil {
					t.Fatalf("after the fault: %v", lerr)
				}
				r, _ := restoreInto(t, segs, prefix, engineMem, metaFor(w.policy), 10_000)
				written := out.written
				compare(t, liveUpTo(t, metaFor(w.policy), written, m), r, storetest.RestoredOptions{
					Entities: out.entities, Written: written, Span: w.span, Sample: equalSample(),
				})
			}
			d.started = func(o *driven) { out = o }
			d.run(t)
			if fired != 1 {
				t.Fatalf("the fault fired %d times", fired)
			}
			r, _ := restoreInto(t, segs, prefix, engineMem, out.meta, 10_000)
			compare(t, out.live, r, storetest.RestoredOptions{Entities: out.entities, Written: out.written, Span: w.span, Sample: equalSample()})
			// Nothing the latest manifest does not name is left, once a later Retire
			// has retried what failed.
			m, err := backup.Latest(ctx, segs, prefix)
			if err != nil {
				t.Fatal(err)
			}
			named := map[string]bool{}
			for _, f := range m.Files {
				named[f.Name] = true
			}
			for info, err := range segs.List(ctx, prefix+"/v1/files/") {
				if err != nil {
					t.Fatal(err)
				}
				if !named[info.Name] {
					t.Errorf("%s is left and named by no manifest", info.Name)
				}
			}
		})
	}
}

// Retire's own cut fails: nothing is retired, the latest manifest still restores,
// and the next Retire stores what that cut closed.
func TestRetireWhoseCutFails(t *testing.T) {
	t.Parallel()
	pod := fingerprint(t, catalog.K8sPod, catalog.K8sPodUID, "cut-pod")
	c := shadowCases(t, pod)[1] // T6b, which retires r
	s := newScript(t, storetest.QuarantinePolicy(), backup.Hooks{})
	armed := false
	f := &faulty{Store: s.segs, fail: func(op, name string) bool {
		if armed && op == "put" && isFile(name) {
			armed = false
			return true
		}
		return false
	}}
	s.segs = f
	o := testOptions(s.meta)
	o.TempDir = t.TempDir()
	cut, err := backup.NewCutter(ctx, f, "faulty", o)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = cut.Close() }()
	s.cut = cut
	s.commit(reachOf(c.r), c.r)
	s.cutNow()
	s.commit(c.wallD, c.d) // left open
	s.retain(base.Add(2 * time.Hour))
	armed = true
	if _, err := cut.Retire(ctx); !errors.Is(err, errFault) {
		t.Fatalf("Retire with its cut failing: %v", err)
	}
	m, err := backup.Latest(ctx, f, "faulty")
	if err != nil || len(m.Files) != 1 || m.Retired[backup.ClassL2].Records != 0 {
		t.Fatalf("after the failed cut the backup is %+v, %v", m, err)
	}
	ret, err := cut.Retire(ctx)
	if err != nil || ret.Files[backup.ClassL2] != 1 || len(ret.Manifest.Files) != 1 || ret.Manifest.Files[0].FirstSeq != 2 {
		t.Fatalf("the next Retire: %+v, %v", ret, err)
	}
	dst := openMem(t, s.meta)
	if _, err := backup.Restore(ctx, f, ret.Manifest, dst, backup.RestoreOptions{Store: s.meta, TempDir: t.TempDir()}); err != nil {
		t.Fatal(err)
	}
	if err := storetest.CompareRestored(s.live, dst, storetest.RestoredOptions{
		Entities: []identity.Fingerprint{pod}, Written: s.written, Span: 10 * time.Minute,
	}); err != nil {
		t.Fatal(err)
	}
}
