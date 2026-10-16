package svcgraph_test

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/lotannauo/toposhift/internal/identity"
	"github.com/lotannauo/toposhift/internal/ingest/otlpmetrics"
	"github.com/lotannauo/toposhift/internal/replay/capture"
	"github.com/lotannauo/toposhift/internal/store"
)

// update rewrites the golden files of this package from the current output. It is
// never on by default.
var update = flag.Bool("update", false, "rewrite the golden files in testdata")

type goldenRecord struct {
	Subject   string `json:"subject"`
	Kind      string `json:"kind"`
	EventTime string `json:"event_time"`
	Basis     string `json:"basis"`
	TTL       string `json:"ttl"`
	// Payload is the exact bytes of the record's payload, as text, so that the
	// golden pins them and not only the JSON they hold.
	Payload string `json:"payload,omitempty"`
}

type goldenStep struct {
	Input    string         `json:"input"`
	Received string         `json:"received"`
	Records  []goldenRecord `json:"records,omitempty"`
	Skipped  map[string]int `json:"skipped,omitempty"`
}

// serviceLabel names a service as "service <namespace>/<name>", or "service <name>"
// when it has no namespace.
func serviceLabel(ns, name string) string {
	if ns == "" {
		return "service " + name
	}
	return "service " + ns + "/" + name
}

// The calls the golden script makes.
var (
	gNS      = "opentelemetry-demo"
	gFront   = edgeID{client: "frontend", server: "checkout", clientNS: gNS, serverNS: gNS}
	gCart    = edgeID{client: "checkout", server: "cart", clientNS: gNS, serverNS: gNS}
	gSelf    = edgeID{client: "cart", server: "cart", clientNS: gNS, serverNS: gNS}
	gLoad    = edge("loadgenerator", "frontend-proxy")
	gCross   = edgeID{client: "frontend", server: "payment", clientNS: gNS}
	gDB      = edgeID{client: "cart", server: "valkey-cart", clientNS: gNS}
	gVirtual = edgeID{client: "frontend", server: "cart", clientNS: gNS, serverNS: gNS}
)

// gStep is one request of the golden script: the points of one flush.
type gStep struct {
	input string
	k     int
	pts   func() []otlpmetrics.SumPoint
}

// typed gives a point the connection type and the virtual_node labels.
func typed(p otlpmetrics.SumPoint, connectionType string, virtualNode ...string) otlpmetrics.SumPoint {
	p.Attributes = append(dropKey(p.Attributes, "connection_type"), kv("connection_type", connectionType))
	return labelled(p, virtualNode...)
}

// Every point carries the empty virtual_node label, as the connector gives it with
// virtual_node_extra_label, unless a step says otherwise.
func gPoint(e edgeID, k int, v float64) otlpmetrics.SumPoint {
	return labelled(counter(e, connStart, at(k), v), "")
}

func goldenScript() []gStep {
	return []gStep{
		{"the first flush: two series, in the warm-up", 0, func() []otlpmetrics.SumPoint {
			return []otlpmetrics.SumPoint{gPoint(gFront, 0, 5), gPoint(gCart, 0, 3)}
		}},
		{"a series new in the second flush, still in the warm-up", 1, func() []otlpmetrics.SumPoint {
			return []otlpmetrics.SumPoint{gPoint(gFront, 1, 5), gPoint(gCart, 1, 3), gPoint(gLoad, 1, 1)}
		}},
		{"the third flush: nothing called", 2, func() []otlpmetrics.SumPoint {
			return []otlpmetrics.SumPoint{gPoint(gFront, 2, 5), gPoint(gCart, 2, 3), gPoint(gLoad, 2, 1)}
		}},
		{"frontend calls checkout again", 3, func() []otlpmetrics.SumPoint {
			return []otlpmetrics.SumPoint{gPoint(gFront, 3, 6), gPoint(gCart, 3, 3), gPoint(gLoad, 3, 1)}
		}},
		{"a self-call and a call with the client's namespace only, both new", 4, func() []otlpmetrics.SumPoint {
			return []otlpmetrics.SumPoint{gPoint(gFront, 4, 6), gPoint(gSelf, 4, 1), gPoint(gCross, 4, 2)}
		}},
		{"a database, two virtual nodes and an unreadable label", 5, func() []otlpmetrics.SumPoint {
			twice := withAttrs(counter(gCart, connStart, at(5), 4), append(gCart.attrs(), kv("virtual_node", "client"), kv("virtual_node", "server"))...)
			notText := withAttrs(counter(gCross, connStart, at(5), 3), append(gCross.attrs(), capture.KeyValue{Key: "virtual_node", Value: iv(1)})...)
			return []otlpmetrics.SumPoint{
				typed(counter(gDB, connStart, at(5), 1), "database", ""),
				typed(counter(gVirtual, connStart, at(5), 1), "", "client"),
				typed(counter(gVirtual, connStart, at(5), 1), "virtual_node", "server"),
				twice, notText,
			}
		}},
		{"checkout calls cart, and loadgenerator calls frontend-proxy", 6, func() []otlpmetrics.SumPoint {
			return []otlpmetrics.SumPoint{gPoint(gFront, 6, 6), gPoint(gCart, 6, 4), gPoint(gLoad, 6, 2)}
		}},
	}
}

// The records of a scripted run of the translator, and what it set aside, are the
// golden file's. A change to a record shows in review as a change to the file.
func TestGolden(t *testing.T) {
	t.Parallel()
	names := map[identity.Fingerprint]string{}
	for _, e := range []edgeID{gFront, gCart, gSelf, gLoad, gCross, gDB, gVirtual} {
		names[serviceFP(t, e.clientNS, e.client)] = serviceLabel(e.clientNS, e.client)
		names[serviceFP(t, e.serverNS, e.server)] = serviceLabel(e.serverNS, e.server)
	}
	label := func(f identity.Fingerprint) string {
		if n, ok := names[f]; ok {
			return n
		}
		return f.String()
	}
	tr := newTranslator(t)
	var out []goldenStep
	for _, s := range goldenScript() {
		received := rcv(at(s.k))
		res := tr.Translate(resource(s.pts()...), received)
		tr.Commit()
		step := goldenStep{Input: s.input, Received: received.Format(time.RFC3339Nano)}
		for _, r := range res.Records {
			g := goldenRecord{
				Kind:      r.Kind.String(),
				EventTime: r.EventTime.Format(time.RFC3339Nano),
				Basis:     r.EventTimeBasis.String(),
				TTL:       r.TTL.String(),
				Payload:   string(r.Payload),
			}
			if r.Subject.Kind == store.SubjectEdge {
				g.Subject = fmt.Sprintf("%s %s -> %s", r.Subject.Relation, label(r.Subject.A), label(r.Subject.B))
			} else {
				g.Subject = label(r.Subject.A)
			}
			step.Records = append(step.Records, g)
		}
		if len(res.Skipped) > 0 {
			step.Skipped = map[string]int{}
			for _, c := range res.Skipped {
				step.Skipped[c.Reason] = c.Count
			}
		}
		out = append(out, step)
	}
	checkGolden(t, "sightings.golden.json", out)
}

// checkGolden compares got with testdata/name, or rewrites the file with -update.
func checkGolden(t *testing.T, name string, got any) {
	t.Helper()
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	enc.SetEscapeHTML(false)
	enc.SetIndent("", " ")
	if err := enc.Encode(got); err != nil {
		t.Fatal(err)
	}
	b := buf.Bytes()
	path := filepath.Join("testdata", name)
	if *update {
		if err := os.MkdirAll("testdata", 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, b, 0o644); err != nil {
			t.Fatal(err)
		}
		t.Logf("rewrote %s", path)
		return
	}
	want, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("%v (run with -update to create it)", err)
	}
	if string(want) != string(b) {
		t.Fatalf("%s differs from the output; run with -update and read the diff through\nfirst difference near: %s", path, firstDiff(string(want), string(b)))
	}
}

func firstDiff(a, b string) string {
	i := 0
	for i < len(a) && i < len(b) && a[i] == b[i] {
		i++
	}
	lo := max(0, i-80)
	return fmt.Sprintf("...%q (want) vs %q (got)", a[lo:min(len(a), i+80)], b[lo:min(len(b), i+80)])
}
