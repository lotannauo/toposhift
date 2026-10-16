package spancount_test

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

// gService names a service, gInstance an instance and gPod a pod, in the form the
// golden file labels them with: the type first.
type (
	gService  struct{ ns, name string }
	gInstance struct{ ns, name, id string }
)

func (s gService) label() string {
	if s.ns == "" {
		return "service " + s.name
	}
	return "service " + s.ns + "/" + s.name
}

func (i gInstance) label() string {
	if i.ns == "" {
		return "instance " + i.name + "/" + i.id
	}
	return "instance " + i.ns + "/" + i.name + "/" + i.id
}

// The resources the golden script sends, one request each.
func goldenScript() []struct {
	input string
	attrs []capture.KeyValue
	at    time.Time
} {
	one := capture.Int64(1)
	return []struct {
		input string
		attrs []capture.KeyValue
		at    time.Time
	}{
		{"an instance with a namespace, a version and a pod", attrs("opentelemetry-demo", "cart", "cart-7d9f-1", "1.4.0", "uid-cart-1"), t0},
		{"an instance with no namespace, no version and no pod", attrs("", "checkout", "checkout-1", "", ""), t0.Add(time.Minute)},
		{"the same instance with a new version", attrs("opentelemetry-demo", "cart", "cart-7d9f-1", "1.5.0", "uid-cart-1"), t0.Add(time.Hour)},
		{"a second instance of the service, in another pod", attrs("opentelemetry-demo", "cart", "cart-7d9f-2", "1.5.0", "uid-cart-2"), t0.Add(time.Hour)},
		{"a name that needs escaping", attrs("", `quote"<&>`, "q-1", "", ""), t0.Add(2 * time.Hour)},
		{"an unusable version and a pod uid given twice", append(attrs("opentelemetry-demo", "ad", "ad-1", "", ""),
			kv("service.version", "1"), kv("service.version", "2"), kv("k8s.pod.uid", "a"), kv("k8s.pod.uid", "b")), t0.Add(2 * time.Hour)},
		{"a service version that is not text", append(attrs("opentelemetry-demo", "ad", "ad-1", "", ""),
			capture.KeyValue{Key: "service.version", Value: capture.AnyValue{IntValue: &one}}), t0.Add(3 * time.Hour)},
		{"no service name", []capture.KeyValue{kv("service.instance.id", "x")}, t0.Add(3 * time.Hour)},
	}
}

// The records of a scripted run of the translator, and what it set aside, are the
// golden file's. A change to a record shows in review as a change to the file.
func TestGolden(t *testing.T) {
	t.Parallel()
	names := map[identity.Fingerprint]string{}
	for _, s := range []gService{
		{"opentelemetry-demo", "cart"}, {"", "checkout"}, {"", `quote"<&>`}, {"opentelemetry-demo", "ad"},
	} {
		names[serviceFP(t, s.ns, s.name)] = s.label()
	}
	for _, i := range []gInstance{
		{"opentelemetry-demo", "cart", "cart-7d9f-1"},
		{"opentelemetry-demo", "cart", "cart-7d9f-2"},
		{"", "checkout", "checkout-1"},
		{"", `quote"<&>`, "q-1"},
		{"opentelemetry-demo", "ad", "ad-1"},
	} {
		names[instanceFP(t, i.ns, i.name, i.id)] = i.label()
	}
	for _, uid := range []string{"uid-cart-1", "uid-cart-2"} {
		names[podFP(t, uid)] = "pod " + uid
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
		received := s.at.Add(time.Second)
		res := tr.Translate(resource(s.attrs, count(s.at.Add(-time.Second), s.at, 3)), received)
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
