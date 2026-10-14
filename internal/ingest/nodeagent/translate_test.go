package nodeagent_test

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/lotannauo/toposhift/internal/catalog"
	"github.com/lotannauo/toposhift/internal/identity"
	"github.com/lotannauo/toposhift/internal/ingest/k8scluster"
	"github.com/lotannauo/toposhift/internal/ingest/nodeagent"
	"github.com/lotannauo/toposhift/internal/ingest/resume"
	"github.com/lotannauo/toposhift/internal/lifecycle"
	"github.com/lotannauo/toposhift/internal/replay/capture"
	"github.com/lotannauo/toposhift/internal/store"
)

// The host is identified by host.id alone: not by the pod name the collector
// reports as host.name, nor by the name of the node.
func TestHostIsIdentifiedByHostIDAlone(t *testing.T) {
	t.Parallel()
	a := agentEvent("h1", "n1", t0)
	b := agentEvent("h1", "n2", t0.Add(time.Minute)).setResStr("host.name", "another-pod").setResStr("k8s.node.name", "another-node")
	c := agentEvent("h2", "n1", t0.Add(2*time.Minute)).setResStr("host.name", a.resStr(t, "host.name")).setResStr("k8s.node.name", a.resStr(t, "k8s.node.name"))
	tr := newTranslator(t)
	hosts := func(x input) []identity.Fingerprint {
		var out []identity.Fingerprint
		for _, r := range translate(t, tr, x, time.Time{}).Records {
			if r.Subject.Kind == store.SubjectEntity {
				out = append(out, r.Subject.A)
			}
		}
		return out
	}
	if got := hosts(a); !slices.Equal(got, []identity.Fingerprint{hostFP(t, "h1")}) {
		t.Fatalf("host of a: %v", got)
	}
	if got := hosts(b); !slices.Equal(got, []identity.Fingerprint{hostFP(t, "h1")}) {
		t.Errorf("a new host name and node name gave another host: %v", got)
	}
	if got := hosts(c); !slices.Equal(got, []identity.Fingerprint{hostFP(t, "h2")}) {
		t.Errorf("the same host name and node name as a, with a new host.id, gave %v", got)
	}
}

// host.id is taken exactly as received: nothing is trimmed, so a value with
// surrounding white space is another host than the same value without it.
func TestHostIDIsTakenAsGiven(t *testing.T) {
	t.Parallel()
	for _, id := range []string{" h1", "h1 ", " h1 ", "h1\n", "H1"} {
		r := translate(t, newTranslator(t), agentEvent(id, "n1", t0), time.Time{})
		if len(r.Records) == 0 {
			t.Fatalf("host.id %q: no records", id)
		}
		if got, want := r.Records[0].Subject.A, hostFP(t, id); got != want {
			t.Errorf("host.id %q: the host is %v, want the fingerprint of the value as given", id, got)
		}
		if r.Records[0].Subject.A == hostFP(t, "h1") {
			t.Errorf("host.id %q gave the host of \"h1\"", id)
		}
	}
	// The same holds for the node uid.
	r := translate(t, newTranslator(t), agentEvent("h1", " n1 ", t0), time.Time{})
	if e := edgesOf(r.Records, catalog.RunsOn); len(e) != 1 || e[0].Subject.A != nodeFP(t, " n1 ") {
		t.Errorf("node uid with surrounding space: %v", describeRecords(r.Records))
	}
}

// The node is identified by k8s.node.uid alone, never by a name, and the edge goes
// from that node to the host.
func TestNodeIsIdentifiedByUIDAlone(t *testing.T) {
	t.Parallel()
	tr := newTranslator(t)
	a := agentEvent("h1", "n1", t0)
	r := translate(t, tr, a, time.Time{})
	e := edgesOf(r.Records, catalog.RunsOn)
	if len(e) != 1 || e[0].Subject.A != nodeFP(t, "n1") || e[0].Subject.B != hostFP(t, "h1") {
		t.Fatalf("runs_on: %s", describeRecords(r.Records))
	}
	// The same name with another uid is another node, and another name with the
	// same uid is the same node.
	same := translate(t, tr, a.setResStr("k8s.node.name", "renamed"), time.Time{})
	if e := edgesOf(same.Records, catalog.RunsOn); len(e) != 1 || e[0].Subject.A != nodeFP(t, "n1") {
		t.Errorf("a renamed node: %s", describeRecords(same.Records))
	}
	other := translate(t, tr, a.setResStr("k8s.node.uid", "n2"), time.Time{})
	if e := edgesOf(other.Records, catalog.RunsOn); len(e) != 1 || e[0].Subject.A != nodeFP(t, "n2") {
		t.Errorf("a node with another uid and the same name: %s", describeRecords(other.Records))
	}
	// With no uid the node name is not resolved: no edge.
	none := translate(t, tr, a.dropRes("k8s.node.uid"), time.Time{})
	if len(edgesOf(none.Records, catalog.RunsOn)) != 0 || skipped(none, nodeagent.RefusedNoNodeUID) != 1 {
		t.Errorf("a record with a node name and no uid: %s, skipped %v", describeRecords(none.Records), none.Skipped)
	}
}

// Nothing but host.id and k8s.node.uid is identity. An event that carries every
// other name, address and id a node has, and no host.id, writes nothing.
func TestOnlyHostIDMakesAHost(t *testing.T) {
	t.Parallel()
	x := agentEvent("h1", "n1", t0).dropRes("host.id").
		setResStr("k8s.node.provider_id", "k3s://n1").setResStr("k8s.node.system_uuid", "uuid-1").
		setResStr("k8s.node.machine_id", "machine-1").setResStr("host.ip", "10.0.0.1").
		setResStr("host.mac", "aa:bb:cc:dd:ee:ff").setResStr("system.uuid", "uuid-2").setResStr("topo.zone.id", "z1")
	r := translate(t, newTranslator(t), x, time.Time{})
	if len(r.Records) != 0 || skipped(r, nodeagent.SkipNoHostID) != 1 {
		t.Fatalf("no host.id: %s, skipped %v", describeRecords(r.Records), r.Skipped)
	}
	// The same names change nothing when host.id is there.
	with := x.setResStr("host.id", "h1")
	plain := agentEvent("h1", "n1", t0)
	a, b := translate(t, newTranslator(t), with, time.Time{}), translate(t, newTranslator(t), plain, time.Time{})
	if !equalRecords(a.Records, b.Records) {
		t.Errorf("other identifiers changed the records:\n%s\nvs\n%s", describeRecords(a.Records), describeRecords(b.Records))
	}
}

func equalRecords(a, b []store.Record) bool {
	return slices.EqualFunc(a, b, func(x, y store.Record) bool {
		return x.Layer == y.Layer && x.Subject == y.Subject && x.Producer == y.Producer && x.EventTime.Equal(y.EventTime) &&
			x.Kind == y.Kind && x.TTL == y.TTL && x.Through.Equal(y.Through) && bytes.Equal(x.Payload, y.Payload) &&
			x.Boot == y.Boot && x.EventTimeBasis == y.EventTimeBasis
	})
}

// A required value that is absent, not a string, blank, or given twice with two
// values is unattributed, and a value given twice alike is the value.
func TestAnUnusableIdentityIsUnattributed(t *testing.T) {
	t.Parallel()
	base := agentEvent("h1", "n1", t0)
	cases := []struct {
		name   string
		x      input
		reason string
	}{
		{"host.id absent", base.dropRes("host.id"), nodeagent.SkipNoHostID},
		{"host.id empty", base.setResStr("host.id", ""), nodeagent.SkipNoHostID},
		{"host.id white space", base.setResStr("host.id", " \n\t"), nodeagent.SkipNoHostID},
		{"host.id an integer", base.setRes("host.id", iv(7)), nodeagent.SkipNoHostID},
		{"host.id twice, two values", base.addRes("host.id", sv("h2")), nodeagent.SkipNoHostID},
		{"host.id twice, a string and an integer", base.addRes("host.id", iv(7)), nodeagent.SkipNoHostID},
	}
	for _, c := range cases {
		r := translate(t, newTranslator(t), c.x, time.Time{})
		if len(r.Records) != 0 || skipped(r, c.reason) != 1 {
			t.Errorf("%s: records %s, skipped %v", c.name, describeRecords(r.Records), r.Skipped)
		}
	}
	nodeCases := []struct {
		name string
		x    input
	}{
		{"node uid absent", base.dropRes("k8s.node.uid")},
		{"node uid empty", base.setResStr("k8s.node.uid", "")},
		{"node uid white space", base.setResStr("k8s.node.uid", "  ")},
		{"node uid an integer", base.setRes("k8s.node.uid", iv(7))},
		{"node uid twice, two values", base.addRes("k8s.node.uid", sv("n2"))},
	}
	for _, c := range nodeCases {
		r := translate(t, newTranslator(t), c.x, time.Time{})
		if len(entityOf(r.Records, hostFP(t, "h1"))) != 1 || len(edgesOf(r.Records, catalog.RunsOn)) != 0 || skipped(r, nodeagent.RefusedNoNodeUID) != 1 {
			t.Errorf("%s: records %s, skipped %v", c.name, describeRecords(r.Records), r.Skipped)
		}
	}
	// The same value twice is no conflict.
	r := translate(t, newTranslator(t), base.addRes("host.id", sv("h1")), time.Time{})
	if len(r.Records) != 2 {
		t.Errorf("host.id twice alike: %s", describeRecords(r.Records))
	}
}

// A value the identity package cannot encode is an error, and leaves the
// translator unchanged.
func TestAnUnencodableIdentityIsAnErrorAndChangesNothing(t *testing.T) {
	t.Parallel()
	tr := newTranslator(t)
	bad := agentEvent(strings.Repeat("x", identity.MaxValueLen+1), "n1", t0)
	if _, err := tr.Translate(bad.Res, bad.Rec, time.Time{}); err == nil {
		t.Fatal("a host.id over the limit was translated")
	}
	// Nothing was noted by the failed call: the first good record still notes the
	// dropped host.name, and its interval.
	good := agentEvent("h1", "n1", t0).setRec("entity.report.interval", iv(600))
	r := translate(t, tr, good, time.Time{})
	if noted(r, nodeagent.NoteDescriptionKeyDropped, "host.name") != 1 || noted(r, nodeagent.NoteIntervalMismatch, "10m0s") != 1 {
		t.Errorf("the failed call changed the translator: notes %v", r.Notes)
	}
}

// Only the host asserts anything about itself: the node entity and a pod are never
// asserted, and the only edges are runs_on and located_in.
func TestOnlyHostsZonesAndTheirEdgesAreWritten(t *testing.T) {
	t.Parallel()
	for _, name := range entityFixtures {
		in := loadFixture(t, name)
		for i, r := range translateAll(t, newTranslator(t, zones), in, false) {
			for _, rec := range r.Records {
				switch rec.Subject.Kind {
				case store.SubjectEntity:
					if typ := rec.Subject.A.Type(); typ != catalog.Host && typ != catalog.Zone {
						t.Errorf("%s: a record of a %s entity", in[i].Name, typ)
					}
				case store.SubjectEdge:
					if rel := rec.Subject.Relation; rel != catalog.RunsOn && rel != catalog.LocatedIn {
						t.Errorf("%s: a %s edge", in[i].Name, rel)
					}
					if rec.Subject.A.Type() == catalog.K8sPod || rec.Subject.B.Type() == catalog.K8sPod {
						t.Errorf("%s: an edge of a pod", in[i].Name)
					}
				}
			}
		}
	}
}

// The boot of the host record is the boot id as received, with any white space it
// has, and only the host record carries one.
func TestBootIsKeptAsGiven(t *testing.T) {
	t.Parallel()
	for _, boot := range []string{"b1", "b1 ", " b1", "b1\n", "B1", "c574baf2-0d0a-4145-991b-575c9e44f405\n"} {
		x := agentEvent("h1", "n1", t0).setResStr("topo.host.boot.id", boot)
		r := translate(t, newTranslator(t), x, time.Time{})
		for _, rec := range r.Records {
			isHost := rec.Subject.Kind == store.SubjectEntity
			switch {
			case isHost && rec.Boot != boot:
				t.Errorf("boot %q: the host's boot is %q", boot, rec.Boot)
			case !isHost && rec.Boot != "":
				t.Errorf("boot %q: an edge carries boot %q", boot, rec.Boot)
			}
		}
		if len(r.Skipped) != 0 {
			t.Errorf("boot %q: skipped %v", boot, r.Skipped)
		}
	}
}

// A boot id that is missing or unusable gives a host with no boot, counted.
func TestBootThatIsMissingOrUnusableIsLeftOut(t *testing.T) {
	t.Parallel()
	base := agentEvent("h1", "n1", t0)
	for _, c := range []struct {
		name   string
		x      input
		reason string
	}{
		{"absent", base.dropRes("topo.host.boot.id"), nodeagent.SubstitutedNoBoot},
		{"empty", base.setResStr("topo.host.boot.id", ""), nodeagent.SubstitutedNoBoot},
		{"white space", base.setResStr("topo.host.boot.id", " \n"), nodeagent.SubstitutedNoBoot},
		{"an integer", base.setRes("topo.host.boot.id", iv(3)), nodeagent.SubstitutedBadBoot},
		{"too long", base.setResStr("topo.host.boot.id", strings.Repeat("b", store.MaxBootLen+1)), nodeagent.SubstitutedBadBoot},
		{"not UTF-8", base.setResStr("topo.host.boot.id", "b\xff"), nodeagent.SubstitutedBadBoot},
		{"twice, two values", base.addRes("topo.host.boot.id", sv("other")), nodeagent.SubstitutedBadBoot},
	} {
		r := translate(t, newTranslator(t), c.x, time.Time{})
		if len(r.Records) != 2 || r.Records[0].Boot != "" || skipped(r, c.reason) != 1 {
			t.Errorf("%s: records %s, skipped %v", c.name, describeRecords(r.Records), r.Skipped)
		}
		for _, rec := range r.Records {
			if err := rec.Validate(); err != nil {
				t.Errorf("%s: %v", c.name, err)
			}
		}
	}
	// A boot of exactly the longest length is kept.
	long := strings.Repeat("b", store.MaxBootLen)
	r := translate(t, newTranslator(t), base.setResStr("topo.host.boot.id", long), time.Time{})
	if r.Records[0].Boot != long {
		t.Error("a boot id of the longest length was not kept")
	}
}

// The payload is the allow-list, with empty values left out; host.name, addresses,
// names and the operating system version of the collector's container are not in
// it.
func TestPayloadIsTheAllowList(t *testing.T) {
	t.Parallel()
	x := agentEvent("h1", "n1", t0).
		setResStr("host.type", "m5.large").setResStr("host.cpu.model.name", "Neoverse-N1").
		setResStr("cloud.provider", "aws").setResStr("cloud.platform", "aws_ec2").setResStr("cloud.region", "eu-west-1").
		setResStr("cloud.availability_zone", "eu-west-1b").setResStr("cloud.resource_id", "i-0abc").
		setResStr("os.version", "6.1.0").setResStr("os.description", "Debian GNU/Linux 12").
		setResStr("host.ip", "10.0.0.1").setResStr("host.mac", "aa:bb:cc:dd:ee:ff").setResStr("test.tag", "x")
	r := translate(t, newTranslator(t), x, time.Time{})
	var got map[string]string
	if err := json.Unmarshal(r.Records[0].Payload, &got); err != nil {
		t.Fatal(err)
	}
	want := map[string]string{
		"host.arch": "arm64", "host.type": "m5.large", "os.type": "linux", "host.cpu.model.name": "Neoverse-N1",
		"cloud.provider": "aws", "cloud.platform": "aws_ec2", "cloud.region": "eu-west-1",
		"cloud.availability_zone": "eu-west-1b", "cloud.resource_id": "i-0abc",
	}
	if !mapsEqual(got, want) {
		t.Errorf("payload %s, want %v", r.Records[0].Payload, want)
	}
	for _, k := range []string{"os.version", "os.description", "host.name", "host.ip", "host.mac", "k8s.node.name", "test.tag", "host.id", "k8s.node.uid"} {
		if bytes.Contains(r.Records[0].Payload, []byte(`"`+k+`"`)) {
			t.Errorf("payload has %s: %s", k, r.Records[0].Payload)
		}
	}
	// Canonical: sorted keys, no white space.
	if !bytes.Equal(r.Records[0].Payload, canonicalOf(t, want)) {
		t.Errorf("payload %s is not canonical JSON", r.Records[0].Payload)
	}
	for _, k := range []string{"os.version", "os.description", "host.ip", "host.mac", "test.tag", "host.name", "k8s.node.name"} {
		if noted(r, nodeagent.NoteDescriptionKeyDropped, k) != 1 {
			t.Errorf("the drop of %s is not noted: %v", k, r.Notes)
		}
	}
	// An empty string is left out, silently; a key that is not a string is dropped.
	e := agentEvent("h1", "n1", t0).setResStr("host.cpu.model.name", "").setResStr("host.type", "").setRes("host.arch", iv(1))
	pe := translate(t, newTranslator(t), e, time.Time{})
	if string(pe.Records[0].Payload) != `{"os.type":"linux"}` {
		t.Errorf("payload %s", pe.Records[0].Payload)
	}
	if noted(pe, nodeagent.NoteDescriptionKeyDropped, "host.cpu.model.name") != 0 || noted(pe, nodeagent.NoteDescriptionKeyDropped, "host.arch") != 1 {
		t.Errorf("notes %v", pe.Notes)
	}
}

func mapsEqual(a, b map[string]string) bool {
	if len(a) != len(b) {
		return false
	}
	for k, v := range a {
		if w, ok := b[k]; !ok || w != v {
			return false
		}
	}
	return true
}

func canonicalOf(t testing.TB, m map[string]string) []byte {
	t.Helper()
	b, err := json.Marshal(m) // sorted keys, no white space
	if err != nil {
		t.Fatal(err)
	}
	return b
}

// The first drop of a key name is noted, and a later one is not.
func TestADroppedKeyIsNotedOnce(t *testing.T) {
	t.Parallel()
	tr := newTranslator(t)
	x := agentEvent("h1", "n1", t0).setResStr("test.tag", "x")
	first := translate(t, tr, x, time.Time{})
	second := translate(t, tr, x.at(t0.Add(time.Minute)), time.Time{})
	if noted(first, nodeagent.NoteDescriptionKeyDropped, "test.tag") != 1 || len(second.Notes) != 0 {
		t.Errorf("first notes %v, second notes %v", first.Notes, second.Notes)
	}
	// k8s.cluster.name, host.id, the node uid and the boot are read and not dropped.
	for _, k := range []string{"k8s.cluster.name", "host.id", "k8s.node.uid", "topo.host.boot.id"} {
		if noted(first, nodeagent.NoteDescriptionKeyDropped, k) != 0 {
			t.Errorf("%s is noted as dropped", k)
		}
	}
}

// What the translator remembers is bounded: after 4096 names it notes once that it
// no longer tracks them, and then notes no new name.
func TestTheSetOfDroppedKeysIsBounded(t *testing.T) {
	t.Parallel()
	tr := newTranslator(t)
	base := agentEvent("h1", "n1", t0)
	fulls, names := 0, 0
	for i := range 4096 + 50 {
		r := translate(t, tr, base.setResStr(fmt.Sprintf("tag.%d", i), "x"), time.Time{})
		fulls += noted(r, nodeagent.NoteNotedFull, "")
		for _, n := range r.Notes {
			if n.Reason == nodeagent.NoteDescriptionKeyDropped {
				names++ // among them host.name and k8s.node.name, dropped by every event
			}
		}
	}
	if fulls != 1 || names != 4096 {
		t.Errorf("%d notes that the set is full and %d names noted, want 1 and 4096", fulls, names)
	}
}

// The TTL is three report intervals of the translator, for every record, whatever
// interval the event carries.
func TestTTLIsThreeConfiguredIntervals(t *testing.T) {
	t.Parallel()
	for _, interval := range []time.Duration{time.Minute, 300 * time.Second, time.Hour} {
		tr := newTranslator(t, func(c *nodeagent.Config) { c.Interval = interval })
		if tr.TTL() != 3*interval {
			t.Errorf("interval %s: TTL %s", interval, tr.TTL())
		}
		for _, ev := range []int64{300, 600, 1, 0, 3600} {
			r := translate(t, tr, agentEvent("h1", "n1", t0).setRec("entity.report.interval", iv(ev)), time.Time{})
			for _, rec := range r.Records {
				if rec.TTL != 3*interval {
					t.Errorf("interval %s, event interval %d: a record with TTL %s", interval, ev, rec.TTL)
				}
			}
		}
	}
}

// An event whose interval is not the translator's, or is missing or unusable, is
// translated as usual, and counted; the interval is noted once per distinct value.
func TestAnIntervalThatDiffersIsCountedAndNotUsed(t *testing.T) {
	t.Parallel()
	tr := newTranslator(t)
	base := agentEvent("h1", "n1", t0)
	cases := []struct {
		x      input
		detail string
	}{
		{base.setRec("entity.report.interval", iv(600)), "10m0s"},
		{base.setRec("entity.report.interval", iv(600)), "10m0s"},
		{base.dropRec("entity.report.interval"), "missing"},
		{base.dropRec("entity.report.interval"), "missing"},
		{base.setRec("entity.report.interval", sv("300")), "unusable"},
		{base.setRec("entity.report.interval", iv(-5)), "unusable"},
		{base.setRec("entity.report.interval", iv(1<<40)), "unusable"},
		{base.setRec("entity.report.interval", iv(0)), "0s"},
		{base, ""},
	}
	seen := map[string]bool{}
	for i, c := range cases {
		r := translate(t, tr, c.x.at(t0.Add(time.Duration(i)*time.Minute)), time.Time{})
		if len(r.Records) != 2 {
			t.Fatalf("case %d: %d records", i, len(r.Records))
		}
		want := 1
		if c.detail == "" {
			want = 0
		}
		if got := skipped(r, nodeagent.SubstitutedConfiguredInterval); got != want {
			t.Errorf("case %d: %d mismatches counted, want %d", i, got, want)
		}
		first := c.detail != "" && !seen[c.detail]
		seen[c.detail] = true
		wantNote := 0
		if first {
			wantNote = 1
		}
		if c.detail != "" && noted(r, nodeagent.NoteIntervalMismatch, c.detail) != wantNote {
			t.Errorf("case %d (%s): notes %v, want %d", i, c.detail, r.Notes, wantNote)
		}
		for _, rec := range r.Records {
			if rec.TTL != ttl900 {
				t.Errorf("case %d: TTL %s", i, rec.TTL)
			}
		}
	}
}

// The record of an edge is in the layer the catalog gives the relation, which is
// not the layer of the node it starts at.
func TestEdgeLayerIsTheRelationsNotTheSources(t *testing.T) {
	t.Parallel()
	cat := catalog.Default()
	node, _ := cat.Entity(catalog.K8sNode)
	rel, _ := cat.Relation(catalog.RunsOn)
	loc, _ := cat.Relation(catalog.LocatedIn)
	if node.Layer() == rel.Layer() {
		t.Fatalf("the node and runs_on are both in %s: the check proves nothing", rel.Layer())
	}
	x := cloudEvent(agentEvent("h1", "n1", t0))
	r := translate(t, newTranslator(t, zones), x, time.Time{})
	if len(r.Records) != 4 {
		t.Fatalf("records: %s", describeRecords(r.Records))
	}
	for _, rec := range r.Records {
		if rec.Subject.Kind != store.SubjectEdge {
			continue
		}
		want := rel.Layer()
		if rec.Subject.Relation == catalog.LocatedIn {
			want = loc.Layer()
		}
		if rec.Layer != want {
			t.Errorf("%s edge stored in %s, the catalog says %s", rec.Subject.Relation, rec.Layer, want)
		}
	}
}

func cloudEvent(x input) input {
	return x.setResStr("cloud.provider", "aws").setResStr("cloud.region", "eu-west-1").setResStr("cloud.availability_zone", "eu-west-1b")
}

// The translator writes only Observes and no Through: nothing is deleted and
// nothing is backdated, and a change of host gives a new edge and ends nothing.
func TestNoDeleteAndNoThrough(t *testing.T) {
	t.Parallel()
	tr := newTranslator(t, zones)
	events := []input{
		cloudEvent(agentEvent("h1", "n1", t0)),
		cloudEvent(agentEvent("h1", "n1", t0.Add(5*time.Minute))),
		cloudEvent(agentEvent("h2", "n1", t0.Add(10*time.Minute))), // the node moves to another host
		agentEvent("h2", "n1", t0.Add(15*time.Minute)),             // and loses its zone
		agentEvent("h2", "n1", t0.Add(20*time.Minute)).dropRes("k8s.node.uid"),
	}
	del := agentEvent("h2", "n1", t0.Add(25*time.Minute))
	del.Rec.EventName = "entity.delete"
	events = append(events, del)
	var all []store.Record
	for _, x := range events {
		r := translate(t, tr, x, time.Time{})
		all = append(all, r.Records...)
		if len(r.Backdated) != 0 {
			t.Errorf("%s: backdated marks", x.Name)
		}
	}
	runsOn := map[identity.Fingerprint]bool{}
	for _, rec := range all {
		if rec.Kind != lifecycle.Observe {
			t.Errorf("a %v record: %s", rec.Kind, describeRecords([]store.Record{rec}))
		}
		if !rec.Through.IsZero() {
			t.Errorf("a record with Through %s", rec.Through)
		}
		if rec.Subject.Kind == store.SubjectEdge && rec.Subject.Relation == catalog.RunsOn {
			runsOn[rec.Subject.B] = true
		}
	}
	if len(runsOn) != 2 {
		t.Errorf("runs_on edges to %d hosts, want 2 (the old edge stays and lapses)", len(runsOn))
	}
}

// A delete event, an event of another type, a legacy event and a record that is not
// an entity event write nothing, and are counted.
func TestEventsThatAreNotHostStatesWriteNothing(t *testing.T) {
	t.Parallel()
	base := agentEvent("h1", "n1", t0)
	del := base.clone()
	del.Rec.EventName = "entity.delete"
	other := base.setRec("entity.type", sv("k8s.node"))
	untyped := base.dropRec("entity.type")
	legacy := base.setRec("otel.entity.type", sv("host"))
	logLine := base.clone()
	logLine.Rec.EventName = ""
	for _, c := range []struct {
		name   string
		x      input
		reason string
	}{
		{"delete", del, nodeagent.SkipHostDelete},
		{"another type", other, nodeagent.SkipNotHost},
		{"no type", untyped, nodeagent.SkipNotHost},
		{"legacy form", legacy, nodeagent.SkipLegacyEntityEvent},
		{"not an entity event", logLine, nodeagent.SkipNotEntityEvent},
	} {
		r := translate(t, newTranslator(t), c.x, time.Time{})
		if len(r.Records) != 0 || skipped(r, c.reason) != 1 || len(r.Notes) != 0 {
			t.Errorf("%s: records %s, skipped %v, notes %v", c.name, describeRecords(r.Records), r.Skipped, r.Notes)
		}
	}
}

// The time rules of the package documentation.
func TestTimes(t *testing.T) {
	t.Parallel()
	x := agentEvent("h1", "n1", t0)
	received := t0.Add(2 * time.Second)

	timeOf := func(r nodeagent.Result) (time.Time, store.EventTimeBasis) {
		t.Helper()
		if len(r.Records) == 0 {
			t.Fatalf("no records: %v", r.Skipped)
		}
		for _, rec := range r.Records[1:] {
			if !rec.EventTime.Equal(r.Records[0].EventTime) || rec.EventTimeBasis != r.Records[0].EventTimeBasis {
				t.Errorf("the records of one event differ in time: %s", describeRecords(r.Records))
			}
		}
		return r.Records[0].EventTime, r.Records[0].EventTimeBasis
	}

	// The producer's clock is trusted, with or without a receipt time.
	for _, rc := range []time.Time{{}, received} {
		if got, basis := timeOf(translate(t, newTranslator(t), x, rc)); !got.Equal(t0) || basis != store.BasisProducerEvent {
			t.Errorf("trusted, received %v: %s %s", rc, got, basis)
		}
	}
	// A time up to MaxClockAhead ahead of receipt is kept; one beyond is not.
	ok := x.at(received.Add(nodeagent.MaxClockAhead))
	if got, basis := timeOf(translate(t, newTranslator(t), ok, received)); !got.Equal(received.Add(nodeagent.MaxClockAhead)) || basis != store.BasisProducerEvent {
		t.Errorf("a time MaxClockAhead ahead: %s %s", got, basis)
	}
	far := x.at(received.Add(nodeagent.MaxClockAhead + time.Nanosecond))
	r := translate(t, newTranslator(t), far, received)
	if got, basis := timeOf(r); !got.Equal(received) || basis != store.BasisReceipt || skipped(r, nodeagent.SubstitutedReceiptImplausibleTime) != 1 {
		t.Errorf("a time past MaxClockAhead: %s %s, skipped %v", got, basis, r.Skipped)
	}
	// A zero time takes receipt, counted; without receipt the record is refused.
	zero := x.clone()
	zero.Rec.TimeUnixNano = 0
	r = translate(t, newTranslator(t), zero, received)
	if got, basis := timeOf(r); !got.Equal(received) || basis != store.BasisReceipt || skipped(r, nodeagent.SubstitutedReceiptNoTime) != 1 {
		t.Errorf("a zero time: %s %s, skipped %v", got, basis, r.Skipped)
	}
	if _, err := newTranslator(t).Translate(zero.Res, zero.Rec, time.Time{}); !errors.Is(err, nodeagent.ErrNoTime) {
		t.Errorf("a zero time and no receipt: %v", err)
	}
	// A time too large to be a time, as a uint64.
	huge := x.clone()
	huge.Rec.TimeUnixNano = capture.Uint64(1<<63 + 5)
	r = translate(t, newTranslator(t), huge, received)
	if got, basis := timeOf(r); !got.Equal(received) || basis != store.BasisReceipt || skipped(r, nodeagent.SubstitutedReceiptImplausibleTime) != 1 {
		t.Errorf("a huge time: %s %s, skipped %v", got, basis, r.Skipped)
	}
	// A receipt outside the representable range is as none.
	if _, err := newTranslator(t).Translate(zero.Res, zero.Rec, time.Date(1900, 1, 1, 0, 0, 0, 0, time.UTC)); !errors.Is(err, nodeagent.ErrNoTime) {
		t.Errorf("a zero time and a receipt before 1970: %v", err)
	}
	// Distrust: always receipt, and refused without one.
	r = translate(t, newTranslator(t, distrust), x, received)
	if got, basis := timeOf(r); !got.Equal(received) || basis != store.BasisReceipt || len(r.Skipped) != 0 {
		t.Errorf("distrusted: %s %s, skipped %v", got, basis, r.Skipped)
	}
	if _, err := newTranslator(t, distrust).Translate(x.Res, x.Rec, time.Time{}); !errors.Is(err, nodeagent.ErrNoTime) {
		t.Errorf("distrusted and no receipt: %v", err)
	}
	// A time earlier than receipt minus the TTL is kept and noted.
	behind := x.at(received.Add(-ttl900 - time.Nanosecond))
	r = translate(t, newTranslator(t), behind, received)
	if got, basis := timeOf(r); !got.Equal(instantOf(behind.Rec)) || basis != store.BasisProducerEvent || noted(r, nodeagent.NoteClockBehind, "") != 1 {
		t.Errorf("a time behind receipt by more than the TTL: %s %s, notes %v", got, basis, r.Notes)
	}
	r = translate(t, newTranslator(t), x.at(received.Add(-ttl900)), received)
	if noted(r, nodeagent.NoteClockBehind, "") != 0 {
		t.Errorf("a time behind receipt by exactly the TTL is noted: %v", r.Notes)
	}
}

// A refused record leaves the translator unchanged.
func TestARefusedRecordChangesNothing(t *testing.T) {
	t.Parallel()
	tr := newTranslator(t)
	zero := agentEvent("h1", "n1", t0).setResStr("test.tag", "x").setRec("entity.report.interval", iv(600))
	zero.Rec.TimeUnixNano = 0
	if _, err := tr.Translate(zero.Res, zero.Rec, time.Time{}); !errors.Is(err, nodeagent.ErrNoTime) {
		t.Fatalf("err = %v", err)
	}
	r := translate(t, tr, agentEvent("h1", "n1", t0).setResStr("test.tag", "x").setRec("entity.report.interval", iv(600)), time.Time{})
	if noted(r, nodeagent.NoteDescriptionKeyDropped, "test.tag") != 1 || noted(r, nodeagent.NoteIntervalMismatch, "10m0s") != 1 {
		t.Errorf("the refused record was remembered: %v", r.Notes)
	}
}

// Zones are off by default. With them on, a zone and a located_in edge are written
// only when the three cloud keys are all there and non-empty.
func TestZones(t *testing.T) {
	t.Parallel()
	full := cloudEvent(agentEvent("h1", "n1", t0))
	count := func(r nodeagent.Result, typ catalog.EntityType) int {
		n := 0
		for _, rec := range r.Records {
			if rec.Subject.Kind == store.SubjectEntity && rec.Subject.A.Type() == typ {
				n++
			}
		}
		return n
	}
	off := translate(t, newTranslator(t), full, time.Time{})
	if count(off, catalog.Zone) != 0 || len(edgesOf(off.Records, catalog.LocatedIn)) != 0 || len(off.Records) != 2 {
		t.Errorf("zones off: %s", describeRecords(off.Records))
	}
	on := translate(t, newTranslator(t, zones), full, time.Time{})
	wantZone := zoneFP(t, "aws/eu-west-1/eu-west-1b")
	e := edgesOf(on.Records, catalog.LocatedIn)
	if count(on, catalog.Zone) != 1 || len(e) != 1 || e[0].Subject.A != hostFP(t, "h1") || e[0].Subject.B != wantZone || len(on.Records) != 4 {
		t.Errorf("zones on: %s", describeRecords(on.Records))
	}
	for _, rec := range on.Records {
		if rec.Subject.Kind == store.SubjectEdge && len(rec.Payload) != 0 {
			t.Errorf("an edge with a payload: %s", rec.Payload)
		}
	}
	for _, drop := range []string{"cloud.provider", "cloud.region", "cloud.availability_zone"} {
		for _, how := range []string{"absent", "empty", "not a string"} {
			var x input
			switch how {
			case "absent":
				x = full.dropRes(drop)
			case "empty":
				x = full.setResStr(drop, "")
			default:
				x = full.setRes(drop, iv(1))
			}
			r := translate(t, newTranslator(t, zones), x, time.Time{})
			if count(r, catalog.Zone) != 0 || len(edgesOf(r.Records, catalog.LocatedIn)) != 0 || len(r.Records) != 2 {
				t.Errorf("zones on, %s %s: %s", drop, how, describeRecords(r.Records))
			}
		}
	}
	// A part with a slash would make two zones' ids collide: no zone, counted.
	for _, key := range []string{"cloud.provider", "cloud.region", "cloud.availability_zone"} {
		r := translate(t, newTranslator(t, zones), full.setResStr(key, "a/b"), time.Time{})
		if count(r, catalog.Zone) != 0 || len(edgesOf(r.Records, catalog.LocatedIn)) != 0 || len(r.Records) != 2 || skipped(r, nodeagent.RefusedZoneID) != 1 {
			t.Errorf("a slash in %s: %s, skipped %v", key, describeRecords(r.Records), r.Skipped)
		}
	}
	// "a/b"+"c" and "a"+"b/c" are the case: each is refused, not one zone.
	r1 := translate(t, newTranslator(t, zones), full.setResStr("cloud.provider", "a/b").setResStr("cloud.region", "c"), time.Time{})
	r2 := translate(t, newTranslator(t, zones), full.setResStr("cloud.provider", "a").setResStr("cloud.region", "b/c"), time.Time{})
	if count(r1, catalog.Zone)+count(r2, catalog.Zone) != 0 {
		t.Error("an ambiguous zone id was written")
	}
	// Two hosts of one zone share the zone's fingerprint.
	other := translate(t, newTranslator(t, zones), cloudEvent(agentEvent("h2", "n2", t0)), time.Time{})
	if e2 := edgesOf(other.Records, catalog.LocatedIn); len(e2) != 1 || e2[0].Subject.B != wantZone {
		t.Errorf("a second host of the zone: %s", describeRecords(other.Records))
	}
}

func TestNewRefusesAnIncompleteConfig(t *testing.T) {
	t.Parallel()
	ok := nodeagent.Config{Producer: testProducer, Resolver: resolver(), Interval: interval300}
	for name, mut := range map[string]func(*nodeagent.Config){
		"no producer": func(c *nodeagent.Config) { c.Producer = "" },
		"no resolver": func(c *nodeagent.Config) { c.Resolver = nil },
		"no interval": func(c *nodeagent.Config) { c.Interval = 0 },
		"negative":    func(c *nodeagent.Config) { c.Interval = -time.Second },
		"over a year": func(c *nodeagent.Config) { c.Interval = 367 * 24 * time.Hour },
	} {
		c := ok
		mut(&c)
		if _, err := nodeagent.New(c); err == nil {
			t.Errorf("%s: New succeeded", name)
		}
	}
	if _, err := nodeagent.New(ok); err != nil {
		t.Fatal(err)
	}
	c := ok
	c.Interval = 366 * 24 * time.Hour
	if _, err := nodeagent.New(c); err != nil {
		t.Errorf("an interval of a year: %v", err)
	}
}

// The source name is nine bytes, and a producer of a cluster whose name is the
// longest the store allows still fits.
func TestSourceNameFitsTheProducerBudget(t *testing.T) {
	t.Parallel()
	if nodeagent.Source != "nodeagent" {
		t.Fatalf("Source = %q", nodeagent.Source)
	}
	if err := store.ValidProducer(lifecycle.Producer(nodeagent.Source + "/" + strings.Repeat("c", 22))); err != nil {
		t.Errorf("a producer with a 22-byte cluster name: %v", err)
	}
}

// This package keeps its own bound on how far ahead a clock may be; it is the one
// of the cluster receiver's translator.
func TestMaxClockAheadMatchesTheClusterTranslators(t *testing.T) {
	t.Parallel()
	if nodeagent.MaxClockAhead != k8scluster.MaxClockAhead || nodeagent.DefaultTTLMultiple != k8scluster.DefaultTTLMultiple {
		t.Errorf("MaxClockAhead %s and %d intervals against the cluster translator's %s and %d",
			nodeagent.MaxClockAhead, nodeagent.DefaultTTLMultiple, k8scluster.MaxClockAhead, k8scluster.DefaultTTLMultiple)
	}
}

// Resumed and Comparable exist for the places that ask for them. No record is
// marked, Resumed changes nothing, and a payload is compared as written.
func TestResumeSurfaceIsInert(t *testing.T) {
	t.Parallel()
	tr := newTranslator(t)
	var _ resume.Resumer = tr
	x := agentEvent("h1", "n1", t0)
	before := translate(t, tr, x, time.Time{})
	for _, h := range []any{nil, 42, "handle", &struct{}{}} {
		tr.Resumed(h, t0.Add(time.Hour), store.BasisObserved)
		tr.Resumed(h, t0.Add(-time.Hour), store.BasisReceipt)
	}
	after := translate(t, tr, x, time.Time{})
	if !equalRecords(before.Records, after.Records) || len(after.Backdated) != 0 || len(before.Backdated) != 0 {
		t.Errorf("Resumed changed the records: %s vs %s", describeRecords(before.Records), describeRecords(after.Records))
	}
	for _, rec := range before.Records {
		if got := nodeagent.Comparable(rec.Subject, rec.Payload); !bytes.Equal(got, rec.Payload) {
			t.Errorf("Comparable(%v) = %s, want the payload as written %s", rec.Subject, got, rec.Payload)
		}
	}
	if nodeagent.Comparable(store.EntitySubject(hostFP(t, "h1")), nil) != nil {
		t.Error("Comparable of no payload is not nil")
	}
}
