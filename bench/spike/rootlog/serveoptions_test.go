package rootlog_test

import (
	"errors"
	"os"
	"regexp"
	"testing"
	"time"

	"github.com/cockroachdb/pebble/v2/vfs"

	"github.com/lotannauo/toposhift/bench/spike/conformance"
	"github.com/lotannauo/toposhift/bench/spike/engine"
	"github.com/lotannauo/toposhift/bench/spike/rootlog"
	"github.com/lotannauo/toposhift/internal/catalog"
	"github.com/lotannauo/toposhift/internal/identity"
	"github.com/lotannauo/toposhift/internal/lifecycle"
	"github.com/lotannauo/toposhift/internal/store/pebblestore"
)

func serveOptions(c pebblestore.CheckpointOptions) rootlog.Options {
	o := options(c)
	o.ServeOptions = true
	return o
}

// The arm built with serve's options passes the checks of the conformance test that
// do not retain: it reopens with what it wrote, writes checkpoints at instants of its
// own choosing, and tells the truth about the bytes it holds. The retention checks
// are left out because they hold every layer to one horizon, which serve's options
// do not: L0 is kept and L1 is retained behind (see the test of that, below).
func TestServeOptionsPassTheChecksThatDoNotRetain(t *testing.T) {
	t.Parallel()
	conformance.SkipWhenTrimmed(t)
	for name, c := range settings {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			f := inMemory(serveOptions(c))
			dir, recs := t.TempDir(), stream(t)
			e, err := f(dir)
			if err != nil {
				t.Fatal(err)
			}
			if err := e.Write(recs); err != nil {
				t.Fatal(err)
			}
			last := e.LastSeq()
			if err := e.Close(); err != nil {
				t.Fatal(err)
			}
			if e, err = f(dir); err != nil {
				t.Fatalf("reopen: %v", err)
			}
			if got := e.LastSeq(); got != last || last == 0 {
				t.Errorf("reopened at LastSeq %d, wrote up to %d", got, last)
			}
			_ = e.Close()
			if err := conformance.CheckCheckpoint(open(t, f)); err != nil {
				t.Errorf("checkpoint: %v", err)
			}
			if err := conformance.CheckMeasurement(open(t, f)); err != nil {
				t.Errorf("measurement: %v", err)
			}
		})
	}
}

// The lifecycle policy is fixed when a database is created: the default arm creates
// one with no boot key, the serve arm one with it, and each is refused by the other.
// With the option off, the default is the zero policy, as it always was.
func TestServeOptionsAreFixedWhenTheDatabaseIsCreated(t *testing.T) {
	t.Parallel()
	for _, created := range []bool{false, true} {
		dir := t.TempDir()
		fs := vfs.NewMem()
		open := func(serve bool) (*rootlog.Engine, error) {
			o := options(settings["k8a1"])
			o.FS, o.ServeOptions = fs, serve
			return rootlog.Open(dir, o)
		}
		e, err := open(created)
		if err != nil {
			t.Fatal(err)
		}
		if err := e.Close(); err != nil {
			t.Fatal(err)
		}
		if other, err := open(!created); err == nil {
			_ = other.Close()
			t.Errorf("a database created with ServeOptions=%v opens with ServeOptions=%v", created, !created)
		}
		same, err := open(created)
		if err != nil {
			t.Fatalf("a database created with ServeOptions=%v does not reopen with it: %v", created, err)
		}
		_ = same.Close()
	}
}

func fingerprint(t *testing.T, typ catalog.EntityType, key catalog.AttributeKey, id string) identity.Fingerprint {
	t.Helper()
	r, err := identity.NewResolver(catalog.Default()).Resolve(typ, []identity.Attr{{Key: key, Value: id}})
	if err != nil {
		t.Fatal(err)
	}
	return r.Fingerprint()
}

// serve keeps L0 and retains L1 ServeL1Offset behind the others: after a retention
// to h, a record in L0 or in L1 shortly before h is accepted, one in L2 is refused,
// and with the option off, all three are refused.
func TestServeOptionsKeepL0AndRetainL1Behind(t *testing.T) {
	t.Parallel()
	t0 := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	h := t0.Add(100 * 24 * time.Hour)
	late := h.Add(-24 * time.Hour)
	entity := func(seq uint64, layer catalog.Layer, fp identity.Fingerprint, at time.Time) engine.Record {
		return engine.Record{Layer: layer, Subject: engine.EntitySubject(fp), Producer: "p", EventTime: at, Seq: seq, Kind: lifecycle.Observe, Payload: []byte("x")}
	}
	rack := fingerprint(t, catalog.Rack, catalog.RackID, "r")
	host := fingerprint(t, catalog.Host, catalog.HostID, "h")
	node := fingerprint(t, catalog.K8sNode, catalog.K8sNodeUID, "n")
	for _, serve := range []bool{false, true} {
		o := options(settings["k8a1"])
		o.FS, o.ServeOptions = vfs.NewMem(), serve
		e, err := rootlog.Open(t.TempDir(), o)
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = e.Close() }()
		first := []engine.Record{entity(1, catalog.L0, rack, t0), entity(2, catalog.L1, host, t0), entity(3, catalog.L2, node, t0)}
		if err := e.Write(first); err != nil {
			t.Fatal(err)
		}
		if err := e.Retain(h); err != nil {
			t.Fatal(err)
		}
		for i, c := range []struct {
			layer  catalog.Layer
			fp     identity.Fingerprint
			accept bool
		}{
			{catalog.L0, rack, serve},
			{catalog.L1, host, serve},
			{catalog.L2, node, false},
		} {
			seq := uint64(4 + i)
			err := e.Write([]engine.Record{entity(seq, c.layer, c.fp, late)})
			switch {
			case c.accept && err != nil:
				t.Errorf("serve=%v: a record in %s a day before the horizon: %v", serve, c.layer, err)
			case !c.accept && !errors.Is(err, engine.ErrBeforeHorizon):
				t.Errorf("serve=%v: a record in %s a day before the horizon: %v, want a refusal before the horizon", serve, c.layer, err)
			}
		}
	}
}

// ServeL1Offset is the offset in cmd/toposhift/flags.go, which this reads: a change
// to one without the other fails here.
func TestServeL1OffsetIsServes(t *testing.T) {
	t.Parallel()
	src, err := os.ReadFile("../../../cmd/toposhift/flags.go")
	if err != nil {
		t.Fatal(err)
	}
	m := regexp.MustCompile(`(?m)^\s*l1Offset\s*=\s*(\d+) \* 24 \* time\.Hour\s*$`).FindSubmatch(src)
	if m == nil {
		t.Fatal("cmd/toposhift/flags.go has no l1Offset = <days> * 24 * time.Hour: update this test with it")
	}
	if want := string(m[1]); want != "60" || rootlog.ServeL1Offset != 60*24*time.Hour {
		t.Errorf("serve's l1Offset is %s days, and rootlog.ServeL1Offset is %s", want, rootlog.ServeL1Offset)
	}
}
