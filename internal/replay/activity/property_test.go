package activity_test

import (
	"bytes"
	"errors"
	"io"
	"math"
	"strings"
	"testing"
	"time"

	"pgregory.net/rapid"

	"github.com/lotannauo/toposhift/internal/catalog"
	"github.com/lotannauo/toposhift/internal/identity"
	"github.com/lotannauo/toposhift/internal/lifecycle"
	"github.com/lotannauo/toposhift/internal/replay/activity"
	"github.com/lotannauo/toposhift/internal/store"
)

// pool is a small set of fingerprints of several entity types, with the layer
// each type lives in.
type pool struct {
	fps    []identity.Fingerprint
	layers []catalog.Layer
}

func newPool(tb fataler) pool {
	tb.Helper()
	cat := catalog.Default()
	var p pool
	add := func(typ catalog.EntityType, key catalog.AttributeKey, v string) {
		fp := fingerprint(tb, typ, key, v)
		e, ok := cat.Entity(typ)
		if !ok {
			tb.Fatalf("no entity type %s", typ)
		}
		p.fps = append(p.fps, fp)
		p.layers = append(p.layers, e.Layer())
	}
	for _, v := range []string{"a", "b"} {
		add(catalog.Rack, catalog.RackID, v)
		add(catalog.Switch, catalog.SwitchID, v)
		add(catalog.Port, catalog.PortID, v)
		add(catalog.Host, catalog.HostID, v)
		add(catalog.Zone, catalog.ZoneID, v)
		add(catalog.K8sNode, catalog.K8sNodeUID, v)
		add(catalog.K8sPod, catalog.K8sPodUID, v)
		add(catalog.Container, catalog.ContainerID, v)
	}
	return p
}

// storedRelations are the relations of the catalog that are written as facts.
func storedRelations() []catalog.RelationType {
	var out []catalog.RelationType
	for rel := range catalog.Default().Relations() {
		if !rel.Derived() {
			out = append(out, rel.Type())
		}
	}
	return out
}

// genRecords draws up to limit valid records in ascending Seq order.
func genRecords(t *rapid.T, p pool, limit int) []store.Record {
	relations := storedRelations()
	n := rapid.IntRange(0, limit).Draw(t, "count")

	// Sequence numbers rise by random gaps; sometimes the last is the largest
	// possible one.
	gaps := rapid.SliceOfN(rapid.OneOf(rapid.Uint64Range(1, 3), rapid.Uint64Range(1, 1<<40)), n, n).Draw(t, "gaps")
	var span uint64
	for _, g := range gaps[min(1, n):] {
		span += g
	}
	start := rapid.OneOf(rapid.Uint64Range(1, 1<<20), rapid.Just(math.MaxUint64-span)).Draw(t, "start")

	times := rapid.OneOf(
		rapid.Int64Range(0, math.MaxInt64),
		rapid.Int64Range(0, 1<<40),
		rapid.SampledFrom([]int64{0, 1, math.MaxInt64 - 1, math.MaxInt64}),
	)
	producers := []lifecycle.Producer{"collector-a", "collector-b", "k8s"}

	recs := make([]store.Record, n)
	seq := start
	for i := range recs {
		if i > 0 {
			seq += gaps[i]
		}
		r := store.Record{
			Seq: seq, Producer: rapid.SampledFrom(producers).Draw(t, "producer"),
			EventTimeBasis: store.EventTimeBasis(rapid.IntRange(0, int(store.BasisProducerEvent)).Draw(t, "basis")),
		}
		eventNS := times.Draw(t, "event time")
		r.EventTime = time.Unix(0, eventNS).UTC()
		if rapid.Bool().Draw(t, "delete") {
			r.Kind = lifecycle.Delete
		} else {
			r.Kind = lifecycle.Observe
			if rapid.Bool().Draw(t, "has through") {
				through := rapid.Int64Range(eventNS, math.MaxInt64).Draw(t, "through")
				r.Through = time.Unix(0, through).UTC()
			}
			if rapid.Bool().Draw(t, "has ttl") {
				last := eventNS
				if !r.Through.IsZero() {
					last = r.Through.UnixNano()
				}
				r.TTL = time.Duration(rapid.Int64Range(0, min(math.MaxInt64-last, 1<<50)).Draw(t, "ttl"))
			}
			if rapid.Bool().Draw(t, "has payload") {
				r.Payload = rapid.SliceOfN(rapid.Byte(), 0, 64).Draw(t, "payload")
			}
		}
		if rapid.Bool().Draw(t, "is edge") {
			from := rapid.IntRange(0, len(p.fps)-1).Draw(t, "from")
			to := rapid.IntRange(0, len(p.fps)-1).Draw(t, "to")
			r.Subject = store.EdgeSubject(p.fps[from], p.fps[to], rapid.SampledFrom(relations).Draw(t, "relation"))
			r.Layer = rapid.SampledFrom([]catalog.Layer{catalog.L0, catalog.L1, catalog.L2, catalog.L3}).Draw(t, "layer")
		} else {
			e := rapid.IntRange(0, len(p.fps)-1).Draw(t, "entity")
			r.Subject = store.EntitySubject(p.fps[e])
			r.Layer = p.layers[e]
			// A host observation may carry the id of its boot.
			if r.Kind == lifecycle.Observe && p.fps[e].Type() == catalog.Host && rapid.Bool().Draw(t, "has boot") {
				r.Boot = rapid.OneOf(
					rapid.SampledFrom([]string{"b1", "b2", "b3", "ブート", " x ", strings.Repeat("z", store.MaxBootLen)}),
					rapid.StringN(1, 80, store.MaxBootLen).Filter(func(s string) bool { return strings.TrimSpace(s) != "" }),
				).Draw(t, "boot")
			}
		}
		if err := r.Validate(); err != nil {
			t.Fatalf("generated an invalid record: %v\n%+v", err, r)
		}
		recs[i] = r
	}
	return recs
}

func genWriterOptions(t *rapid.T) activity.WriterOptions {
	opts := activity.WriterOptions{RowGroupRows: rapid.IntRange(1, 50).Draw(t, "rows per group")}
	if rapid.Bool().Draw(t, "tiny byte limit") {
		opts.RowGroupBytes = rapid.Int64Range(1, 600).Draw(t, "bytes per group")
	}
	return opts
}

func TestPropertyRoundTrip(t *testing.T) {
	t.Parallel()

	p := newPool(t)
	rapid.Check(t, func(t *rapid.T) {
		recs := genRecords(t, p, 60)
		wopts := genWriterOptions(t)
		ropts := activity.ReaderOptions{BatchRows: rapid.IntRange(1, 200).Draw(t, "batch rows")}

		data := writeFile(t, recs, wopts)
		r, err := openFile(data, ropts)
		if err != nil {
			t.Fatalf("NewReader: %v", err)
		}

		info := r.Info()
		want := activity.Info{Version: 1, Records: int64(len(recs))}
		if len(recs) > 0 {
			want.MinSeq, want.MaxSeq = recs[0].Seq, recs[len(recs)-1].Seq
		}
		minGroups := (len(recs) + wopts.RowGroupRows - 1) / wopts.RowGroupRows
		if wopts.RowGroupBytes == 0 {
			want.RowGroups = minGroups
		} else {
			// The byte limit may close groups early, never late.
			if info.RowGroups < minGroups || info.RowGroups > len(recs) {
				t.Fatalf("%d row groups for %d records in groups of at most %d rows, want from %d to %d",
					info.RowGroups, len(recs), wopts.RowGroupRows, minGroups, len(recs))
			}
			want.RowGroups = info.RowGroups
		}
		if info != want {
			t.Fatalf("Info() = %+v, want %+v", info, want)
		}

		got, err := drain(r)
		if err != nil {
			t.Fatalf("Next after %d of %d records: %v", len(got), len(recs), err)
		}
		requireSame(t, recs, got)
		for range 2 {
			if _, err := r.Next(); !errors.Is(err, io.EOF) {
				t.Fatalf("Next after the end = %v, want io.EOF", err)
			}
		}
	})
}

func TestPropertySameRecordsSameBytes(t *testing.T) {
	t.Parallel()

	p := newPool(t)
	rapid.Check(t, func(t *rapid.T) {
		recs := genRecords(t, p, 30)
		opts := genWriterOptions(t)
		a, b := writeFile(t, recs, opts), writeFile(t, recs, opts)
		if !bytes.Equal(a, b) {
			t.Fatalf("writing the same %d records twice gave files of %d and %d bytes that differ", len(recs), len(a), len(b))
		}
	})
}
