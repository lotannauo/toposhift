package nodeagent_test

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/lotannauo/toposhift/internal/catalog"
	"github.com/lotannauo/toposhift/internal/ingest/nodeagent"
	"github.com/lotannauo/toposhift/internal/lifecycle"
	"github.com/lotannauo/toposhift/internal/store"
)

// gridEvents is every combination of the variations a record of a node collector
// can have that the translator reads: identity, boot, interval, time, cloud keys
// and noise. It is a fixed enumeration, not a random draw.
func gridEvents() []input {
	base := agentEvent("h1", "n1", t0)
	var out []input
	variants := func(x input, vs ...func(input) input) []input {
		var r []input
		for _, v := range vs {
			r = append(r, v(x))
		}
		return r
	}
	id := func(f func(input) input) func(input) input { return f }
	hostIDs := []func(input) input{
		id(func(x input) input { return x.dropRes("host.id") }),
		id(func(x input) input { return x.setResStr("host.id", "") }),
		id(func(x input) input { return x.setResStr("host.id", " ") }),
		id(func(x input) input { return x }),
		id(func(x input) input { return x.setResStr("host.id", " h1 ") }),
	}
	nodeUIDs := []func(input) input{
		id(func(x input) input { return x.dropRes("k8s.node.uid") }),
		id(func(x input) input { return x.setResStr("k8s.node.uid", "") }),
		id(func(x input) input { return x }),
	}
	boots := []func(input) input{
		id(func(x input) input { return x.dropRes("topo.host.boot.id") }),
		id(func(x input) input { return x.setResStr("topo.host.boot.id", "") }),
		id(func(x input) input { return x.setResStr("topo.host.boot.id", " ") }),
		id(func(x input) input { return x }),
		id(func(x input) input { return x.setResStr("topo.host.boot.id", "boot-h1 ") }),
	}
	intervals := []func(input) input{
		id(func(x input) input { return x }),
		id(func(x input) input { return x.setRec("entity.report.interval", iv(600)) }),
		id(func(x input) input { return x.dropRec("entity.report.interval") }),
	}
	times := []func(input) input{
		id(func(x input) input { return x }),
		id(func(x input) input { x = x.clone(); x.Rec.TimeUnixNano = 0; return x }),
		id(func(x input) input { return x.at(t0.Add(3 * time.Hour)) }),
	}
	clouds := []func(input) input{
		id(func(x input) input { return x }),
		id(cloudEvent),
		id(func(x input) input { return cloudEvent(x).dropRes("cloud.region") }),
	}
	noise := []func(input) input{
		id(func(x input) input { return x }),
		id(func(x input) input {
			return x.setResStr("os.version", "6.1").setResStr("host.ip", "10.0.0.1").setResStr("host.cpu.model.name", "").
				setResStr("os.description", "Debian").setResStr("k8s.node.provider_id", "k3s://n1")
		}),
	}
	out = []input{base}
	for _, vs := range [][]func(input) input{hostIDs, nodeUIDs, boots, intervals, times, clouds, noise} {
		var next []input
		for _, x := range out {
			next = append(next, variants(x, vs...)...)
		}
		out = next
	}
	return out
}

// Properties of every record the translator writes, over the grid and the fixtures,
// with the producer's clock trusted and distrusted, zones on and off.
func TestProperties(t *testing.T) {
	t.Parallel()
	events := gridEvents()
	for _, f := range allFixtures {
		events = append(events, loadFixture(t, f)...)
	}
	received := t0.Add(time.Second)
	cat := catalog.Default()
	for _, withZones := range []bool{false, true} {
		for _, untrusted := range []bool{false, true} {
			var opts []func(*nodeagent.Config)
			if withZones {
				opts = append(opts, zones)
			}
			if untrusted {
				opts = append(opts, distrust)
			}
			// One translator for the whole enumeration, as a producer has, and a
			// second, fresh one, which must give the same answer to the same events.
			a, b := newTranslator(t, opts...), newTranslator(t, opts...)
			ttls := map[time.Duration]int{}
			for _, x := range events {
				ra, errA := a.Translate(x.Res, x.Rec, received)
				rb, errB := b.Translate(x.Res, x.Rec, received)
				if (errA == nil) != (errB == nil) {
					t.Fatalf("%s: errors differ: %v and %v", x.Name, errA, errB)
				}
				if errA != nil {
					continue
				}
				if !reflect.DeepEqual(ra, rb) {
					t.Fatalf("%s: two fresh translators gave different results:\n%+v\n%+v", x.Name, ra, rb)
				}
				if len(ra.Backdated) != 0 {
					t.Errorf("%s: Backdated is not empty", x.Name)
				}
				for _, rec := range ra.Records {
					if err := rec.Validate(); err != nil {
						t.Errorf("%s: %v", x.Name, err)
					}
					ttls[rec.TTL]++
					switch {
					case rec.Kind != lifecycle.Observe:
						t.Errorf("%s: a %v record", x.Name, rec.Kind)
					case !rec.Through.IsZero():
						t.Errorf("%s: a record with Through %s", x.Name, rec.Through)
					case rec.Producer != testProducer:
						t.Errorf("%s: producer %s", x.Name, rec.Producer)
					case rec.Seq != 0:
						t.Errorf("%s: Seq %d", x.Name, rec.Seq)
					}
					if rec.Subject.A.Type() == catalog.K8sPod || rec.Subject.B.Type() == catalog.K8sPod {
						t.Errorf("%s: a record about a pod", x.Name)
					}
					if rec.Subject.Kind == store.SubjectEdge {
						rel, ok := cat.Relation(rec.Subject.Relation)
						if !ok || rec.Layer != rel.Layer() || rec.Boot != "" {
							t.Errorf("%s: an edge in %s with boot %q", x.Name, rec.Layer, rec.Boot)
						}
					} else {
						e, ok := cat.Entity(rec.Subject.A.Type())
						if !ok || rec.Layer != e.Layer() {
							t.Errorf("%s: an entity in %s", x.Name, rec.Layer)
						}
						if rec.Subject.A.Type() != catalog.Host && rec.Boot != "" {
							t.Errorf("%s: a boot on a %s", x.Name, rec.Subject.A.Type())
						}
					}
					if bytes.Contains(rec.Payload, []byte("os.version")) || bytes.Contains(rec.Payload, []byte("host.name")) || bytes.Contains(rec.Payload, []byte("host.ip")) {
						t.Errorf("%s: payload %s", x.Name, rec.Payload)
					}
					if rec.EventTimeBasis != store.BasisProducerEvent && rec.EventTimeBasis != store.BasisReceipt {
						t.Errorf("%s: basis %s", x.Name, rec.EventTimeBasis)
					}
				}
			}
			// One TTL at a time: this producer only ever writes one.
			if len(ttls) != 1 || ttls[ttl900] == 0 {
				t.Errorf("zones %v, untrusted %v: TTLs %v, want only %s", withZones, untrusted, ttls, ttl900)
			}
		}
	}
	if len(events) < 4000 {
		t.Errorf("the enumeration has %d events: it was meant to be large", len(events))
	}
}

// The translator never reads a clock: no non-test file of the package names a
// function of package time that reads or waits on one.
func TestTheTranslatorNeverReadsAClock(t *testing.T) {
	t.Parallel()
	banned := map[string]bool{
		"Now": true, "Since": true, "Until": true, "Sleep": true, "After": true, "Tick": true,
		"NewTimer": true, "NewTicker": true, "AfterFunc": true,
	}
	files, err := filepath.Glob("*.go")
	if err != nil || len(files) == 0 {
		t.Fatalf("glob: %v, %d files", err, len(files))
	}
	checked := 0
	for _, name := range files {
		if strings.HasSuffix(name, "_test.go") {
			continue
		}
		checked++
		f, err := parser.ParseFile(token.NewFileSet(), name, nil, 0)
		if err != nil {
			t.Fatal(err)
		}
		ast.Inspect(f, func(n ast.Node) bool {
			sel, ok := n.(*ast.SelectorExpr)
			if !ok {
				return true
			}
			if id, ok := sel.X.(*ast.Ident); ok && id.Name == "time" && banned[sel.Sel.Name] {
				t.Errorf("%s uses time.%s", name, sel.Sel.Name)
			}
			return true
		})
	}
	if checked < 3 {
		t.Errorf("only %d source files were checked", checked)
	}
}

// The fixtures are copies of the capture's lines, byte for byte: a change to any of
// them is a change to the evidence.
func TestFixturesAreUnchanged(t *testing.T) {
	t.Parallel()
	want := map[string]string{
		fixtureReal:      "ad9680a260dc96c1aea2ba2d7e31b42ad10eb578d00de6d7ea28ab9777c84db2",
		fixtureDistinct:  "5fe2474b5d1dc27cf5bec34dfee2d914acc794615e5e57bc4aea8ce2b7327997",
		fixtureShared:    "2a065301a13dffd1f6db7e3b94038cfd6b45648387035228c81d387162d66563",
		fixtureRecreated: "f05945bf953c7a4ea01d7fb28178eb20bf535549e992955ddde8e5e5eacb8561",
		fixtureMarker:    "d5e01400f1872cebd8c00daead02ee794264b5be4a91ffbf07115e4c99beefb4",
	}
	for name, digest := range want {
		b, err := os.ReadFile(filepath.Join("testdata", name))
		if err != nil {
			t.Fatal(err)
		}
		sum := sha256.Sum256(b)
		if got := hex.EncodeToString(sum[:]); got != digest {
			t.Errorf("%s: sha256 %s, want %s", name, got, digest)
		}
	}
}

// What the capture holds, read through the translator: the real fixture has no
// machine id on any node, so nothing is written for it; the distinct fixture has
// three hosts for three nodes; the shared one has two nodes on one host; the
// re-created one gives a node a new uid on a host it had.
func TestFixturesGiveWhatTheCaptureHolds(t *testing.T) {
	t.Parallel()
	type shape struct {
		records, hosts, edges, nodes int
	}
	for name, want := range map[string]shape{
		fixtureReal:      {0, 0, 0, 0},
		fixtureDistinct:  {6, 3, 3, 3},
		fixtureShared:    {6, 2, 3, 3},
		fixtureRecreated: {6, 2, 3, 3},
		fixtureMarker:    {0, 0, 0, 0},
	} {
		recs := records(translateAll(t, newTranslator(t), loadFixture(t, name), false))
		hosts, nodes := map[string]bool{}, map[string]bool{}
		edges := 0
		for _, r := range recs {
			if r.Subject.Kind == store.SubjectEdge {
				edges++
				nodes[r.Subject.A.String()] = true
			} else {
				hosts[r.Subject.A.String()] = true
			}
		}
		if got := (shape{len(recs), len(hosts), edges, len(nodes)}); got != want {
			t.Errorf("%s: %+v, want %+v", name, got, want)
		}
	}
}
