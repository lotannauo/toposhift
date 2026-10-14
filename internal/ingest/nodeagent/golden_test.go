package nodeagent_test

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/lotannauo/toposhift/internal/identity"
	"github.com/lotannauo/toposhift/internal/ingest/nodeagent"
	"github.com/lotannauo/toposhift/internal/store"
)

// The golden files hold the translation of the fixtures, one step per log record,
// with the instant the record carries and the records, skips and notes it
// produced. They are made with a node collector that reports every 300 seconds,
// so a TTL is 15 minutes, with the producer's clock trusted and no receipt time.
// They are read through by eye when written. Regenerate them only with
// -update-nodeagent-golden and list the files in the commit.
//
// Provenance of the fixtures. They were captured from a three-node k3s cluster
// run in containers (one server node and two k3s agent nodes), with a node collector on each node:
// the host metrics receiver in a logs pipeline, resource detection with the env,
// k8s_api and system detectors, and the boot id set from the kernel's file by the
// collector's own configuration. Each line is one request of the collector as it
// sent it, with one entity event. The cluster is named l1r1, and every value in
// the files is throwaway: container node uids, the one boot id of the machine
// that hosts the three containers, and host names of the collector's pods.
//
//   - real-nomachineid: the capture as it was. The containers have no machine id
//     on any node, so the events carry no host.id. Two reports of each node.
//   - synthetic-distinct-machineids: the same capture, with a distinct machine id
//     injected into each container's file before the collector started, so that
//     the events carry a host.id: three nodes, three hosts.
//   - synthetic-shared-machineid: as above, with the two k3s agent nodes given one machine
//     id: one host, two nodes.
//   - synthetic-node-recreated-then-repeat: the node of the first k3s agent node deleted and
//     registered again (a new node uid, the same machine id and boot), and a
//     report after it. Its new uid is e81f1010; before it, in
//     synthetic-shared-machineid, it was 72c578b9.
//   - alt-k8sobjects-nodepull-marker: records of another kind, a body and no event
//     name, from an alternative way of marking a node collector's records. They are
//     not entity events, and the translator sets them aside.
//
// The files are copies of the capture's lines, byte for byte. A case the capture
// does not hold is made in the test by editing one decoded line.

// goldenRecord is a record as a golden file shows it.
type goldenRecord struct {
	Subject   string          `json:"subject"`
	Layer     string          `json:"layer"`
	Kind      string          `json:"kind"`
	EventTime string          `json:"event_time"`
	Basis     string          `json:"basis"`
	TTL       string          `json:"ttl"`
	Boot      string          `json:"boot,omitempty"`
	Payload   json.RawMessage `json:"payload,omitempty"`
}

type goldenStep struct {
	Input   string         `json:"input"`
	Instant string         `json:"instant"`
	Records []goldenRecord `json:"records,omitempty"`
	Skipped map[string]int `json:"skipped,omitempty"`
	Notes   []string       `json:"notes,omitempty"`
}

// labels names the nodes and hosts of inputs, so a golden file can say "node
// l1r1-agent-1" and "host ea01b04c" and not a hash.
func labels(t testing.TB, in []input) map[identity.Fingerprint]string {
	t.Helper()
	out := map[identity.Fingerprint]string{}
	short := func(s string) string {
		if len(s) > 8 {
			return s[:8]
		}
		return s
	}
	for _, x := range in {
		attr := func(key string) (string, bool) {
			for _, kv := range x.Res.Attributes {
				if kv.Key == key && kv.Value.StringValue != nil && strings.TrimSpace(*kv.Value.StringValue) != "" {
					return *kv.Value.StringValue, true
				}
			}
			return "", false
		}
		if uid, ok := attr("k8s.node.uid"); ok {
			name, _ := attr("k8s.node.name")
			out[nodeFP(t, uid)] = fmt.Sprintf("node %s %s", name, short(uid))
		}
		if id, ok := attr("host.id"); ok {
			out[hostFP(t, id)] = "host " + short(id)
		}
		p, okP := attr("cloud.provider")
		r, okR := attr("cloud.region")
		z, okZ := attr("cloud.availability_zone")
		if okP && okR && okZ {
			id := p + "/" + r + "/" + z
			out[zoneFP(t, id)] = "zone " + id
		}
	}
	return out
}

func render(names map[identity.Fingerprint]string, in []input, rs []nodeagent.Result) []goldenStep {
	label := func(f identity.Fingerprint) string {
		if n, ok := names[f]; ok {
			return n
		}
		return f.String()
	}
	var out []goldenStep
	for i, r := range rs {
		s := goldenStep{
			Input:   fmt.Sprintf("%s %s", in[i].Name, in[i].Rec.EventName),
			Instant: instantOf(in[i].Rec).Format(time.RFC3339Nano),
		}
		for _, rec := range r.Records {
			g := goldenRecord{
				Layer:     rec.Layer.String(),
				Kind:      rec.Kind.String(),
				EventTime: rec.EventTime.Format(time.RFC3339Nano),
				Basis:     rec.EventTimeBasis.String(),
				TTL:       rec.TTL.String(),
				Boot:      rec.Boot,
			}
			if rec.Subject.Kind == store.SubjectEdge {
				g.Subject = fmt.Sprintf("%s %s to %s", rec.Subject.Relation, label(rec.Subject.A), label(rec.Subject.B))
			} else {
				g.Subject = label(rec.Subject.A)
			}
			if len(rec.Payload) > 0 {
				g.Payload = json.RawMessage(rec.Payload)
			}
			s.Records = append(s.Records, g)
		}
		if len(r.Skipped) > 0 {
			s.Skipped = map[string]int{}
			for _, k := range r.Skipped {
				s.Skipped[k.Reason] = k.Count
			}
		}
		for _, n := range r.Notes {
			line := n.Reason
			if n.Detail != "" {
				line += ": " + n.Detail
			}
			s.Notes = append(s.Notes, fmt.Sprintf("%s (x%d)", line, n.Count))
		}
		out = append(out, s)
	}
	return out
}

// checkGolden compares got with testdata/name, or rewrites the file with
// -update-nodeagent-golden.
func checkGolden(t *testing.T, name string, got any) {
	t.Helper()
	b, err := json.MarshalIndent(got, "", " ")
	if err != nil {
		t.Fatal(err)
	}
	b = append(b, '\n')
	path := filepath.Join("testdata", name)
	if *updateGolden {
		if err := os.WriteFile(path, b, 0o644); err != nil {
			t.Fatal(err)
		}
		t.Logf("rewrote %s", path)
		return
	}
	want, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("%v (run with -update-nodeagent-golden to create it)", err)
	}
	if string(want) != string(b) {
		t.Fatalf("%s differs from the output; run with -update-nodeagent-golden, read the diff through, and list the file in the commit message\nfirst difference near: %s", path, firstDiff(string(want), string(b)))
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

func TestGolden(t *testing.T) {
	for _, name := range allFixtures {
		t.Run(name, func(t *testing.T) {
			in := loadFixture(t, name)
			rs := translateAll(t, newTranslator(t), in, false)
			checkGolden(t, name[:len(name)-len(".logs.jsonl")]+".golden.json", render(labels(t, in), in, rs))
		})
	}
}

// goldenCase is one line of the shared fixture edited into a case the capture does
// not hold.
type goldenCase struct {
	name string
	edit func(input) input
	opts []func(*nodeagent.Config)
}

func cloudKeys(x input) input {
	return x.setResStr("cloud.provider", "aws").setResStr("cloud.platform", "aws_ec2").
		setResStr("cloud.region", "eu-west-1").setResStr("cloud.availability_zone", "eu-west-1b")
}

func editedCases() []goldenCase {
	cases := []goldenCase{
		{name: "as captured", edit: func(x input) input { return x }},
		{name: "host id empty", edit: func(x input) input { return x.setResStr("host.id", "") }},
		{name: "host id white space", edit: func(x input) input { return x.setResStr("host.id", " \t\n") }},
		{name: "host id absent", edit: func(x input) input { return x.dropRes("host.id") }},
		{name: "host id with surrounding space is kept as given", edit: func(x input) input { return x.setResStr("host.id", " ea01b04c38adbb6d2efff691290c1b66 ") }},
		{name: "node uid absent", edit: func(x input) input { return x.dropRes("k8s.node.uid") }},
		{name: "node uid white space", edit: func(x input) input { return x.setResStr("k8s.node.uid", " ") }},
		{name: "time an hour ahead of receipt", edit: func(x input) input { return x.at(instantOf(x.Rec).Add(time.Hour)) }},
		{name: "time zero", edit: func(x input) input { x = x.clone(); x.Rec.TimeUnixNano = 0; return x }},
		{name: "boot blank", edit: func(x input) input { return x.setResStr("topo.host.boot.id", "  ") }},
		{name: "boot absent", edit: func(x input) input { return x.dropRes("topo.host.boot.id") }},
		{name: "boot with a trailing newline is kept as given", edit: func(x input) input {
			return x.setResStr("topo.host.boot.id", "c574baf2-0d0a-4145-991b-575c9e44f405\n")
		}},
		{name: "interval 600", edit: func(x input) input { return x.setRec("entity.report.interval", iv(600)) }},
		{name: "interval absent", edit: func(x input) input { return x.dropRec("entity.report.interval") }},
		{name: "interval a string", edit: func(x input) input { return x.setRec("entity.report.interval", sv("300")) }},
		{name: "delete event", edit: func(x input) input { x = x.clone(); x.Rec.EventName = "entity.delete"; return x }},
		{name: "another entity type", edit: func(x input) input { return x.setRec("entity.type", sv("k8s.node")) }},
		{name: "empty and unwanted description keys", edit: func(x input) input {
			return x.setResStr("host.cpu.model.name", "").setResStr("os.version", "6.1.0").setResStr("os.description", "Debian GNU/Linux").
				setResStr("host.ip", "10.0.0.1").setResStr("host.mac", "aa:bb:cc:dd:ee:ff").setResStr("test.tag", "x")
		}},
		{name: "description keys that are kept", edit: func(x input) input {
			return cloudKeys(x).setResStr("host.type", "m5.large").setResStr("host.cpu.model.name", "Neoverse-N1").setResStr("cloud.resource_id", "i-0abc")
		}},
		{name: "zones off, cloud keys given", edit: cloudKeys},
		{name: "zones on, cloud keys given", edit: cloudKeys, opts: []func(*nodeagent.Config){zones}},
		{name: "zones on, region missing", edit: func(x input) input { return cloudKeys(x).dropRes("cloud.region") }, opts: []func(*nodeagent.Config){zones}},
		{name: "zones on, a part with a slash", edit: func(x input) input { return cloudKeys(x).setResStr("cloud.region", "eu/west-1") }, opts: []func(*nodeagent.Config){zones}},
		{name: "zones on, zone empty", edit: func(x input) input { return cloudKeys(x).setResStr("cloud.availability_zone", "") }, opts: []func(*nodeagent.Config){zones}},
		{name: "the producer's clock distrusted", edit: func(x input) input { return x }, opts: []func(*nodeagent.Config){distrust}},
	}
	return cases
}

func TestGoldenEditedCases(t *testing.T) {
	base := loadFixture(t, fixtureShared)[0]
	var in []input
	var rs []nodeagent.Result
	for _, c := range editedCases() {
		x := c.edit(base.clone()).named(c.name)
		// The receipt time is a second after the time the captured line carries,
		// so that an edited time is measured against it.
		rcv := instantOf(base.Rec).Add(time.Second)
		in = append(in, x)
		rs = append(rs, translate(t, newTranslator(t, c.opts...), x, rcv))
	}
	checkGolden(t, "edited-cases.golden.json", render(labels(t, append([]input{base}, in...)), in, rs))
}
