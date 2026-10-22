// Package actfixture builds small activity files shaped like the Alibaba placement
// stream, for the tests of the metrics-store baseline: one producer that samples nodes,
// pods and the placement of each pod on a node at a fixed cadence with a fixed TTL, never
// deletes, and lets a pod that disappears go silent. It is not used outside tests.
package actfixture

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/lotannauo/toposhift/internal/catalog"
	"github.com/lotannauo/toposhift/internal/coalesce"
	"github.com/lotannauo/toposhift/internal/identity"
	"github.com/lotannauo/toposhift/internal/lifecycle"
	"github.com/lotannauo/toposhift/internal/replay/activity"
	"github.com/lotannauo/toposhift/internal/store"
)

// Producer is the producer of every record.
const Producer lifecycle.Producer = "alibaba/placement"

// Options describe a stream.
type Options struct {
	// Start is the first sample's time; Step the cadence and TTL what each record promises.
	Start     time.Time
	Step, TTL time.Duration
	// Samples is how many times the cluster is sampled.
	Samples int
	// Nodes and Pods are the sizes of the cluster. A pod p exists from sample p%4 until
	// Samples-p%3 (so some are born late and some vanish early, and go silent: there is no
	// delete), and sits on node (p + m/MoveEvery) % Nodes at sample m, so pods move.
	Nodes, Pods int
	MoveEvery   int
}

// Default is a cluster of 6 nodes and 40 pods over 24 samples at five-minute steps with a
// fifteen-minute TTL (the Alibaba C300 stream's), whose pods move every 5 samples.
func Default() Options {
	return Options{
		Start: time.Date(2022, 8, 22, 0, 0, 0, 0, time.UTC), Step: 5 * time.Minute, TTL: 15 * time.Minute,
		Samples: 24, Nodes: 6, Pods: 40, MoveEvery: 5,
	}
}

// FP is the fingerprint of the n-th entity of a type.
func FP(typ catalog.EntityType, n int) identity.Fingerprint {
	var h [identity.FingerprintBytes]byte
	h[0], h[1] = byte(n), byte(n>>8)
	f, err := identity.FingerprintFromHash(typ, h)
	if err != nil {
		panic(err)
	}
	return f
}

// Placement returns the every-sample stream: at each sample, every node, then every live
// pod and the edge to its node, in ascending Seq from 1.
func Placement(o Options) []store.Record {
	var out []store.Record
	seq := uint64(0)
	add := func(r store.Record) {
		seq++
		r.Seq = seq
		r.Layer, r.Producer, r.Kind, r.TTL, r.EventTimeBasis = catalog.L2, Producer, lifecycle.Observe, o.TTL, store.BasisProducerEvent
		out = append(out, r)
	}
	for m := 0; m < o.Samples; m++ {
		at := o.Start.Add(time.Duration(m) * o.Step)
		for n := 0; n < o.Nodes; n++ {
			add(store.Record{Subject: store.EntitySubject(FP(catalog.K8sNode, n)), EventTime: at})
		}
		for p := 0; p < o.Pods; p++ {
			if m < p%4 || m >= o.Samples-p%3 {
				continue
			}
			node := (p + m/o.MoveEvery) % o.Nodes
			add(store.Record{Subject: store.EntitySubject(FP(catalog.K8sPod, p)), EventTime: at, Payload: []byte{byte(p)}})
			add(store.Record{Subject: store.EdgeSubject(FP(catalog.K8sPod, p), FP(catalog.K8sNode, node), catalog.ScheduledOn), EventTime: at})
		}
	}
	return out
}

// Coalesce runs the records through the product's coalescer, in order, and numbers what
// it writes: the coalesced stream of the same facts.
func Coalesce(tb testing.TB, rs []store.Record, cfg coalesce.Config) []store.Record {
	tb.Helper()
	c, err := coalesce.New(cfg)
	if err != nil {
		tb.Fatal(err)
	}
	var out []store.Record
	for _, r := range rs {
		out = c.Add(r, out)
	}
	for i := range out {
		out[i].Seq = uint64(i + 1)
	}
	return out
}

// DefaultCoalescer is the coalescer the Alibaba placement streams are written with.
func DefaultCoalescer() coalesce.Config {
	return coalesce.Config{ExtendTTLFraction: 0.5, RunAgeFromTTL: true}
}

// Write writes the records as an activity file in dir and returns its path.
func Write(tb testing.TB, dir, name string, rs []store.Record) string {
	tb.Helper()
	var buf bytes.Buffer
	w, err := activity.NewWriter(&buf, activity.WriterOptions{RowGroupRows: 64})
	if err != nil {
		tb.Fatal(err)
	}
	for _, r := range rs {
		if err := w.Write(r); err != nil {
			tb.Fatal(err)
		}
	}
	if err := w.Close(); err != nil {
		tb.Fatal(err)
	}
	path := filepath.Join(dir, name)
	if err := os.WriteFile(path, buf.Bytes(), 0o600); err != nil {
		tb.Fatal(err)
	}
	return path
}
