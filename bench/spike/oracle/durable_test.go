package oracle_test

import (
	"errors"
	"os"
	"path/filepath"
	"slices"
	"testing"
	"time"

	"github.com/lotannauo/toposhift/bench/spike/engine"
	"github.com/lotannauo/toposhift/bench/spike/oracle"
	"github.com/lotannauo/toposhift/bench/spike/workload"
	"github.com/lotannauo/toposhift/internal/catalog"
	"github.com/lotannauo/toposhift/internal/identity"
	"github.com/lotannauo/toposhift/internal/lifecycle"
)

func TestDurableComesBackExactlyAfterReopening(t *testing.T) {
	t.Parallel()

	cfg := workload.Tiny()
	cfg.CoalesceRuns, cfg.LateProbability, cfg.ConfirmProbability, cfg.ConfirmTTL = true, 0.2, 0.5, 3*time.Minute
	cfg.FirstSeq = 1<<32 - 100
	g, err := workload.New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	recs := g.All()
	h := cfg.Start.Add(8 * time.Minute)

	dir := t.TempDir()
	d, err := oracle.OpenDurable(dir)
	if err != nil {
		t.Fatal(err)
	}
	mem := oracle.New()
	half := len(recs) / 2
	for _, batch := range [][]engine.Record{recs[:half/2], recs[half/2 : half]} {
		if err := d.Write(slices.Clone(batch)); err != nil {
			t.Fatal(err)
		}
		if err := mem.Write(slices.Clone(batch)); err != nil {
			t.Fatal(err)
		}
	}
	for _, o := range []engine.Engine{d, mem} {
		if err := o.Retain(h); err != nil {
			t.Fatal(err)
		}
	}
	if err := d.Close(); err != nil {
		t.Fatal(err)
	}

	// Reopened twice over, with a write in between: the log is appended to, not
	// rewritten, and replaying it is idempotent.
	for round := range 2 {
		d, err = oracle.OpenDurable(dir)
		if err != nil {
			t.Fatal(err)
		}
		if d.LastSeq() != mem.LastSeq() {
			t.Fatalf("round %d: LastSeq = %d, want %d", round, d.LastSeq(), mem.LastSeq())
		}
		rest := slices.DeleteFunc(slices.Clone(recs[half:]), func(r engine.Record) bool { return r.EventTime.Before(h) })
		if round == 0 {
			rest = rest[:len(rest)/2]
		} else {
			rest = rest[len(rest)/2:]
		}
		if err := d.Write(slices.Clone(rest)); err != nil {
			t.Fatalf("round %d: %v", round, err)
		}
		if err := mem.Write(slices.Clone(rest)); err != nil {
			t.Fatal(err)
		}
		if err := d.Close(); err != nil {
			t.Fatal(err)
		}
	}

	d, err = oracle.OpenDurable(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = d.Close() }()
	if d.LastSeq() != mem.LastSeq() {
		t.Errorf("LastSeq = %d, want %d", d.LastSeq(), mem.LastSeq())
	}
	// Every answer, in every layer, as of several tokens, is the in-memory
	// oracle's.
	ents := g.Entities()
	for i := 0; i < len(ents); i += 3 {
		for _, layer := range []catalog.Layer{catalog.L0, catalog.L1, catalog.L2, catalog.L3} {
			for _, tok := range []uint64{engine.Latest, recs[len(recs)/2].Seq} {
				sc := engine.Scope{Layer: layer, AsOf: tok}
				for _, at := range []time.Time{h, h.Add(5 * time.Minute), cfg.Start.Add(cfg.Duration)} {
					for _, dir := range []engine.Direction{engine.Forward, engine.Reverse} {
						want, _ := mem.Neighbors(ents[i], dir, at, sc)
						got, err := d.Neighbors(ents[i], dir, at, sc)
						if err != nil || !slices.Equal(got, want) {
							t.Fatalf("Neighbors(%s, %s, %s) = %v, %v; want %v", ents[i], dir, layer, got, err, want)
						}
						wantW, _ := mem.Window(ents[i], dir, h, at.Add(time.Hour), sc)
						gotW, err := d.Window(ents[i], dir, h, at.Add(time.Hour), sc)
						if err != nil || len(gotW) != len(wantW) {
							t.Fatalf("Window(%s, %s, %s) has %d records, %v; want %d", ents[i], dir, layer, len(gotW), err, len(wantW))
						}
						for j := range wantW {
							a, b := gotW[j], wantW[j]
							if a.Subject != b.Subject || a.Seq != b.Seq || !a.EventTime.Equal(b.EventTime) || !a.Through.Equal(b.Through) ||
								a.TTL != b.TTL || a.Kind != b.Kind || a.Producer != b.Producer || string(a.Payload) != string(b.Payload) || a.Layer != b.Layer {
								t.Fatalf("Window record %d = %+v; want %+v", j, a, b)
							}
						}
					}
				}
			}
		}
	}

	// What persisted includes the horizon, and the rule that Seq only rises.
	stale := recs[len(recs)-1]
	stale.Seq, stale.EventTime = mem.LastSeq()+1, h.Add(-time.Second)
	if err := d.Write([]engine.Record{stale}); !errors.Is(err, engine.ErrBeforeHorizon) {
		t.Errorf("a record before the horizon after reopening: err = %v, want ErrBeforeHorizon", err)
	}
	repeat := recs[len(recs)-1]
	repeat.Seq = mem.LastSeq()
	if err := d.Write([]engine.Record{repeat}); !errors.Is(err, engine.ErrInvalid) {
		t.Errorf("a repeated Seq after reopening: err = %v, want ErrInvalid", err)
	}
	if size, err := d.Size(); err != nil || size == 0 {
		t.Errorf("Size = %d, %v", size, err)
	}
}

func TestDurableKeepsWhatARecordHoldsAtTheEdges(t *testing.T) {
	t.Parallel()

	pod, err := identity.NewResolver(catalog.Default()).Resolve(catalog.K8sPod, []identity.Attr{{Key: catalog.K8sPodUID, Value: "p"}})
	if err != nil {
		t.Fatal(err)
	}
	fp := pod.Fingerprint()
	recs := []engine.Record{
		// An entity (no second fingerprint), at the first and last instants, with
		// a Through at the last, a huge Seq and a payload of every byte value.
		{
			Layer: catalog.L2, Subject: engine.EntitySubject(fp), Producer: "p", EventTime: engine.MinEventTime, Seq: 1<<63 + 5, Kind: lifecycle.Observe,
			Through: engine.MaxEventTime, TTL: 0, Payload: allBytes(),
		},
		{Layer: catalog.L2, Subject: engine.EntitySubject(fp), Producer: "p", EventTime: engine.MaxEventTime, Seq: 1<<63 + 6, Kind: lifecycle.Delete},
	}
	dir := t.TempDir()
	d, err := oracle.OpenDurable(dir)
	if err != nil {
		t.Fatal(err)
	}
	if err := d.Write(slices.Clone(recs)); err != nil {
		t.Fatal(err)
	}
	if err := d.Close(); err != nil {
		t.Fatal(err)
	}
	d, err = oracle.OpenDurable(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = d.Close() }()
	if d.LastSeq() != 1<<63+6 {
		t.Errorf("LastSeq = %d", d.LastSeq())
	}
	for _, tt := range []struct {
		at   time.Time
		want bool
	}{{engine.MinEventTime.Add(-time.Nanosecond), false}, {engine.MinEventTime, true}, {engine.MaxEventTime.Add(-time.Nanosecond), true}, {engine.MaxEventTime, false}} {
		if got, err := d.Alive(fp, tt.at, engine.Current(catalog.L2)); err != nil || got != tt.want {
			t.Errorf("Alive at %s = %v, %v; want %v", tt.at, got, err, tt.want)
		}
	}
}

func allBytes() []byte {
	b := make([]byte, 256)
	for i := range b {
		b[i] = byte(i)
	}
	return b
}

func TestDurableRefusesWhatItCannotReplay(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "oracle.log"), []byte("{\"write\": [{\"a\": \"not a fingerprint\"}]}\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := oracle.OpenDurable(dir); err == nil {
		t.Error("a log with a bad fingerprint was opened")
	}
	if err := os.WriteFile(filepath.Join(dir, "oracle.log"), []byte("not json\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := oracle.OpenDurable(dir); err == nil {
		t.Error("a log that is not JSON was opened")
	}
	if _, err := oracle.OpenDurable(filepath.Join(dir, "missing", "dir")); err == nil {
		t.Error("a directory that does not exist was opened")
	}
}

func TestDurableDoesNotLogARefusedBatch(t *testing.T) {
	t.Parallel()

	w := newTopology(t)
	dir := t.TempDir()
	d, err := oracle.OpenDurable(dir)
	if err != nil {
		t.Fatal(err)
	}
	bad := placed(w, w.node, 2, "k8s", 0, lifecycle.Observe, 0)
	bad.Producer = ""
	if err := d.Write([]engine.Record{placed(w, w.node, 1, "k8s", 0, lifecycle.Observe, 0), bad}); err == nil {
		t.Fatal("a bad batch was accepted")
	}
	if err := d.Close(); err != nil {
		t.Fatal(err)
	}
	d, err = oracle.OpenDurable(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = d.Close() }()
	if d.LastSeq() != 0 {
		t.Errorf("a refused batch came back: LastSeq = %d", d.LastSeq())
	}
}
