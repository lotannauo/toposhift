package identity_test

import (
	"bytes"
	"encoding/hex"
	"encoding/json"
	"flag"
	"os"
	"testing"

	"github.com/lotannauo/toposhift/internal/catalog"
	"github.com/lotannauo/toposhift/internal/identity"
)

// The canonical encoding is a stored format. A fingerprint keys every record
// about its entity, so changing the encoding silently orphans everything
// already written, while every property test below would still pass. These
// vectors are therefore a contract with data on disk, not an implementation
// detail: if one changes, the encoding changed, and that needs a new version
// byte, a new vectors file and a migration, never a regenerated golden.
//
// Regenerate only to add a vector:
//
//	go test ./internal/identity -run TestGolden -update
var update = flag.Bool("update", false, "rewrite testdata/golden_v1.json from goldenCases")

const goldenFile = "testdata/golden_v1.json"

type goldenAttr struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

type goldenCase struct {
	Name  string             `json:"name"`
	Type  catalog.EntityType `json:"type"`
	Attrs []goldenAttr       `json:"attrs"`
}

type goldenVector struct {
	goldenCase
	Canonical   string `json:"canonical"`
	Fingerprint string `json:"fingerprint"`
}

type goldenDoc struct {
	EncodingVersion int            `json:"encoding_version"`
	Vectors         []goldenVector `json:"vectors"`
}

// goldenCases are given as strings, which every key kind accepts (integers as
// decimal strings, times as RFC 3339), so the file is plain JSON.
var goldenCases = []goldenCase{
	{"host", catalog.Host, []goldenAttr{{"host.id", "h1"}}},
	{"host, cloud instance id", catalog.Host, []goldenAttr{{"host.id", "i-0abcd1234ef567890"}}},
	{"host, non-ASCII", catalog.Host, []goldenAttr{{"host.id", "ホスト-1"}}},
	{"host, value with surrounding spaces", catalog.Host, []goldenAttr{{"host.id", " h1 "}}},
	{"host, value containing separator bytes", catalog.Host, []goldenAttr{{"host.id", "a\x1fb:c\x1ed\x00e"}}},
	{"pod", catalog.K8sPod, []goldenAttr{{"k8s.pod.uid", "abc"}}},
	{"node, same uid as the pod", catalog.K8sNode, []goldenAttr{{"k8s.node.uid", "abc"}}},
	{"container", catalog.Container, []goldenAttr{{"container.id", "3f4c9a7b"}}},
	{"rack", catalog.Rack, []goldenAttr{{"topo.rack.id", "r1"}}},
	{"switch", catalog.Switch, []goldenAttr{{"topo.switch.id", "sw1"}}},
	{"port", catalog.Port, []goldenAttr{{"topo.port.id", "sw1/eth0"}}},
	{"zone", catalog.Zone, []goldenAttr{{"topo.zone.id", "eu-west-1a"}}},
	{"process", catalog.Process, []goldenAttr{
		{"host.id", "i-0abc"}, {"process.pid", "1234"}, {"process.creation.time", "2023-11-21T09:25:34.853Z"},
	}},
	{"process, same instant written with an offset and keys reordered", catalog.Process, []goldenAttr{
		{"process.creation.time", "2023-11-21T10:25:34.853+01:00"}, {"process.pid", "1234"}, {"host.id", "i-0abc"},
	}},
	{"process, negative pid", catalog.Process, []goldenAttr{
		{"host.id", "i-0abc"}, {"process.pid", "-1"}, {"process.creation.time", "2023-11-21T09:25:34.853Z"},
	}},
	{"process, largest pid", catalog.Process, []goldenAttr{
		{"host.id", "i-0abc"}, {"process.pid", "9223372036854775807"}, {"process.creation.time", "2023-11-21T09:25:34.853Z"},
	}},
	{"process, time before the epoch", catalog.Process, []goldenAttr{
		{"host.id", "i-0abc"}, {"process.pid", "0"}, {"process.creation.time", "1969-12-31T23:59:59.5Z"},
	}},
	{"process, time at the end of year 9999", catalog.Process, []goldenAttr{
		{"host.id", "i-0abc"}, {"process.pid", "1"}, {"process.creation.time", "9999-12-31T23:59:59.999999999Z"},
	}},
	{"process, time in year 1", catalog.Process, []goldenAttr{
		{"host.id", "i-0abc"}, {"process.pid", "1"}, {"process.creation.time", "0001-01-02T00:00:00Z"},
	}},
	{"service, no namespace", catalog.Service, []goldenAttr{{"service.name", "web"}}},
	{"service, empty namespace is the same as none", catalog.Service, []goldenAttr{
		{"service.namespace", ""}, {"service.name", "web"},
	}},
	{"service, with namespace", catalog.Service, []goldenAttr{
		{"service.namespace", "shop"}, {"service.name", "web"},
	}},
	{"service instance", catalog.ServiceInstance, []goldenAttr{
		{"service.namespace", "shop"}, {"service.name", "web"}, {"service.instance.id", "0f8fad5b-d9cb-469f-a165-70867728950e"},
	}},
	{"service instance, no namespace", catalog.ServiceInstance, []goldenAttr{
		{"service.name", "web"}, {"service.instance.id", "0f8fad5b-d9cb-469f-a165-70867728950e"},
	}},
}

func toAttrs(in []goldenAttr) []identity.Attr {
	out := make([]identity.Attr, len(in))
	for i, a := range in {
		out[i] = identity.Attr{Key: catalog.AttributeKey(a.Key), Value: a.Value}
	}
	return out
}

func buildGolden(t *testing.T) []goldenVector {
	t.Helper()
	r := newResolver()
	out := make([]goldenVector, len(goldenCases))
	for i, c := range goldenCases {
		id, err := r.Resolve(c.Type, toAttrs(c.Attrs))
		if err != nil {
			t.Fatalf("%s: %v", c.Name, err)
		}
		out[i] = goldenVector{
			goldenCase:  c,
			Canonical:   hex.EncodeToString([]byte(id.Canonical())),
			Fingerprint: id.Fingerprint().String(),
		}
	}
	return out
}

func TestGoldenVectorsAreFrozen(t *testing.T) {
	doc, err := json.MarshalIndent(goldenDoc{EncodingVersion: identity.EncodingVersion, Vectors: buildGolden(t)}, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	doc = append(doc, '\n')

	if *update {
		if err := os.WriteFile(goldenFile, doc, 0o644); err != nil {
			t.Fatal(err)
		}
		return
	}

	want, err := os.ReadFile(goldenFile)
	if err != nil {
		t.Fatalf("%v (create it with -update)", err)
	}
	if !bytes.Equal(doc, want) {
		t.Fatalf("the canonical encoding or a fingerprint no longer matches %s.\n"+
			"Stored fingerprints would be orphaned. If the change is intended it needs a new "+
			"encoding version and a migration; do not regenerate this file. Diff the output of "+
			"`go test -run TestGolden -update` against git to see what moved.", goldenFile)
	}
}

// TestGoldenFileRoundTrips replays the file itself, so that a vector removed
// from goldenCases but still on disk would also be exercised.
func TestGoldenFileRoundTrips(t *testing.T) {
	t.Parallel()

	raw, err := os.ReadFile(goldenFile)
	if err != nil {
		t.Skip("no golden file yet")
	}
	var doc goldenDoc
	if err := json.Unmarshal(raw, &doc); err != nil {
		t.Fatal(err)
	}
	if doc.EncodingVersion != identity.EncodingVersion {
		t.Fatalf("file is for encoding version %d, code is %d", doc.EncodingVersion, identity.EncodingVersion)
	}

	r := newResolver()
	for _, v := range doc.Vectors {
		t.Run(v.Name, func(t *testing.T) {
			t.Parallel()
			canon, err := hex.DecodeString(v.Canonical)
			if err != nil {
				t.Fatal(err)
			}
			parsed, err := r.Parse(string(canon))
			if err != nil {
				t.Fatalf("Parse: %v", err)
			}
			if got := parsed.Fingerprint().String(); got != v.Fingerprint {
				t.Errorf("fingerprint = %s, want %s", got, v.Fingerprint)
			}
			fp, err := identity.ParseFingerprint(v.Fingerprint)
			if err != nil || fp != parsed.Fingerprint() {
				t.Errorf("ParseFingerprint(%q) = %v, %v", v.Fingerprint, fp, err)
			}
			rebuilt, err := identity.FingerprintFromHash(fp.Type(), fp.Hash())
			if err != nil || rebuilt != fp {
				t.Errorf("FingerprintFromHash(%s, %x) = %v, %v; want %s", fp.Type(), fp.Hash(), rebuilt, err, fp)
			}
			resolved, err := r.Resolve(v.Type, toAttrs(v.Attrs))
			if err != nil || resolved != parsed {
				t.Errorf("Resolve(inputs) = %v, %v; want %s", resolved, err, parsed)
			}
		})
	}
}

// TestGoldenRelationships checks the vectors that exist to show two different
// spellings are one identity, and two different things are not.
func TestGoldenRelationships(t *testing.T) {
	t.Parallel()

	by := map[string]goldenVector{}
	for _, v := range buildGolden(t) {
		by[v.Name] = v
	}
	same := [][2]string{
		{"process", "process, same instant written with an offset and keys reordered"},
		{"service, no namespace", "service, empty namespace is the same as none"},
	}
	for _, p := range same {
		if by[p[0]].Fingerprint != by[p[1]].Fingerprint {
			t.Errorf("%q and %q should be one identity", p[0], p[1])
		}
	}
	if by["pod"].Fingerprint[len("k8s.pod:"):] == by["node, same uid as the pod"].Fingerprint[len("k8s.node:"):] {
		t.Error("a pod and a node with one uid share a hash")
	}
	seen := map[string]string{}
	for _, v := range by {
		if other, dup := seen[v.Canonical]; dup && v.Name != other && !isSamePair(same, v.Name, other) {
			t.Errorf("%q and %q have identical canonical bytes", v.Name, other)
		}
		seen[v.Canonical] = v.Name
	}
}

func isSamePair(pairs [][2]string, a, b string) bool {
	for _, p := range pairs {
		if (p[0] == a && p[1] == b) || (p[0] == b && p[1] == a) {
			return true
		}
	}
	return false
}

// TestHandVerifiedVectors pins the encoding to the specification in the
// package documentation, not to whatever the code currently does. The bytes
// are written out piece by piece from the spec, and the fingerprints were
// computed outside Go: the preimage built by an independent Python script and
// hashed with `shasum -a 256`, taking the first 32 hex digits.
func TestHandVerifiedVectors(t *testing.T) {
	t.Parallel()
	r := newResolver()

	t.Run("host h1", func(t *testing.T) {
		t.Parallel()
		want := "toposhift/identity" + // magic
			"\x01" + // version
			"\x00\x00\x00\x04" + "host" + // entity type
			"\x00\x00\x00\x01" + // one entry
			"\x00\x00\x00\x07" + "host.id" + // name
			"\x01" + // tag: string
			"\x00\x00\x00\x02" + "h1" // payload
		id, err := r.Resolve(catalog.Host, attrs(catalog.HostID, "h1"))
		if err != nil {
			t.Fatal(err)
		}
		if id.Canonical() != want {
			t.Errorf("canonical = %x\n      want %x", id.Canonical(), want)
		}
		if got, wantFP := id.Fingerprint().String(), "host:56445241b9a3fe7e60745db5181df4b3"; got != wantFP {
			t.Errorf("fingerprint = %s, want %s", got, wantFP)
		}
	})

	t.Run("process with every value kind", func(t *testing.T) {
		t.Parallel()
		// 2023-11-21T09:25:34Z is 1700558734 seconds = 0x655c778e.
		want := "toposhift/identity" + "\x01" +
			"\x00\x00\x00\x07" + "process" +
			"\x00\x00\x00\x03" + // three entries, in name order
			"\x00\x00\x00\x07" + "host.id" + "\x01" + "\x00\x00\x00\x06" + "i-0abc" +
			"\x00\x00\x00\x15" + "process.creation.time" + "\x03" +
			"\x00\x00\x00\x00\x65\x5c\x77\x8e" + "\x32\xd7\xbf\x40" + // seconds, then 853000000 ns
			"\x00\x00\x00\x0b" + "process.pid" + "\x02" + "\x00\x00\x00\x00\x00\x00\x04\xd2" // 1234
		id, err := r.Resolve(catalog.Process, attrs(
			catalog.ProcessPID, 1234, catalog.HostID, "i-0abc", catalog.ProcessCreationTime, goodTime))
		if err != nil {
			t.Fatal(err)
		}
		if id.Canonical() != want {
			t.Errorf("canonical = %x\n      want %x", id.Canonical(), want)
		}
		if got, wantFP := id.Fingerprint().String(), "process:39f63099c3a070cea55c43db6138f7cf"; got != wantFP {
			t.Errorf("fingerprint = %s, want %s", got, wantFP)
		}
	})
}
