package activity_test

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"flag"
	"math"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/parquet/file"

	"github.com/lotannauo/toposhift/internal/catalog"
	"github.com/lotannauo/toposhift/internal/identity"
	"github.com/lotannauo/toposhift/internal/lifecycle"
	"github.com/lotannauo/toposhift/internal/replay/activity"
	"github.com/lotannauo/toposhift/internal/store"
)

// testdata/activity_v1.parquet is a format contract, like the identity
// vectors: it is a file of format version 1 as it was written once, and every
// later version of this package must read it. It is never regenerated to make
// a test pass. If reading it breaks, the format changed; that needs a new
// version number and a new file, and the old file stays.
//
// The file is written only to add a record to goldenRecords before the format
// has shipped:
//
//	go test ./internal/replay/activity -run TestGolden -update
var update = flag.Bool("update", false, "rewrite testdata/activity_v1.parquet from goldenRecords")

const goldenFile = "testdata/activity_v1.parquet"

// goldenDigest is the content digest of the file: the digest of the two row
// group digests below. Neither depends on how the Parquet library encodes the
// file, only on the canonical encoding of the records and on how they are
// divided into row groups.
const goldenDigest = "sha256:c4d8bbc4c1eb09d5486d754f598e2db393a13d1911c4876f16dd469186645031"

// goldenGroupDigests are the digests of the two row groups of the file.
const goldenGroupDigests = "9e68e1f713ff3f88ffba574273bc02bc6812a4541a306f265cbc79fb8f56efd8,07dfa921173825a3066eb51ce64c6306051e4bf2f1b3e3fe470c782959ed7d2d"

// goldenRowGroupRows gives the file two row groups of 7 records.
const goldenRowGroupRows = 7

func must(fp identity.Fingerprint, err error) identity.Fingerprint {
	if err != nil {
		panic(err)
	}
	return fp
}

func resolve(typ catalog.EntityType, attrs ...identity.Attr) identity.Fingerprint {
	id, err := identity.NewResolver(catalog.Default()).Resolve(typ, attrs)
	return must(id.Fingerprint(), err)
}

func everyByte() []byte {
	b := make([]byte, 256)
	for i := range b {
		b[i] = byte(i)
	}
	return b
}

// goldenRecords cover: an entity in each layer, an edge of each stored
// relation, a delete of each subject kind, a run with Through, a TTL, an empty
// payload, a payload with every byte value, both ends of the event time range
// and a Seq above 2^63.
var goldenRecords = withBases(func() []store.Record {
	rack := resolve(catalog.Rack, identity.Attr{Key: catalog.RackID, Value: "rack-1"})
	host := resolve(catalog.Host, identity.Attr{Key: catalog.HostID, Value: "host-1"})
	node := resolve(catalog.K8sNode, identity.Attr{Key: catalog.K8sNodeUID, Value: "node-1"})
	pod := resolve(catalog.K8sPod, identity.Attr{Key: catalog.K8sPodUID, Value: "pod-1"})
	ctr := resolve(catalog.Container, identity.Attr{Key: catalog.ContainerID, Value: "ctr-1"})
	instance := resolve(catalog.ServiceInstance,
		identity.Attr{Key: catalog.ServiceName, Value: "checkout"}, identity.Attr{Key: catalog.ServiceInstanceID, Value: "checkout-0"})
	checkout := resolve(catalog.Service, identity.Attr{Key: catalog.ServiceName, Value: "checkout"})
	payments := resolve(catalog.Service, identity.Attr{Key: catalog.ServiceName, Value: "payments"})

	t0 := time.Date(2026, 3, 1, 12, 0, 0, 0, time.UTC)
	obs, del := lifecycle.Observe, lifecycle.Delete
	return []store.Record{
		{
			Layer: catalog.L0, Subject: store.EntitySubject(rack), Producer: "dcim", EventTime: t0, Seq: 1, Kind: obs,
			TTL: 24 * time.Hour, Payload: []byte(`{"row":"a"}`),
		},
		{
			Layer: catalog.L1, Subject: store.EntitySubject(host), Producer: "cloud", EventTime: t0.Add(time.Second), Seq: 2, Kind: obs,
			TTL: 5 * time.Minute, Through: t0.Add(time.Hour), Payload: []byte(`{"arch":"arm64"}`),
			Boot: "6f1c2d3e-0a4b-4c5d-8e9f-0123456789ab",
		},
		{Layer: catalog.L2, Subject: store.EntitySubject(pod), Producer: "k8s", EventTime: t0.Add(2 * time.Second), Seq: 3, Kind: obs},
		{
			Layer: catalog.L3, Subject: store.EntitySubject(checkout), Producer: "mesh", EventTime: t0.Add(3 * time.Second), Seq: 4, Kind: obs,
			Payload: []byte("checkout"),
		},
		{Layer: catalog.L2, Subject: store.EdgeSubject(ctr, pod, catalog.PartOf), Producer: "k8s", EventTime: t0.Add(4 * time.Second), Seq: 5, Kind: obs},
		{
			Layer: catalog.L2, Subject: store.EdgeSubject(pod, node, catalog.ScheduledOn), Producer: "k8s", EventTime: t0.Add(5 * time.Second), Seq: 6, Kind: obs,
			Payload: everyByte(),
		},
		{
			Layer: catalog.L2, Subject: store.EdgeSubject(node, host, catalog.RunsOn), Producer: "k8s", EventTime: t0.Add(6 * time.Second), Seq: 7, Kind: obs,
			TTL: time.Minute,
		},
		{Layer: catalog.L1, Subject: store.EdgeSubject(host, rack, catalog.LocatedIn), Producer: "dcim", EventTime: store.MinEventTime, Seq: 8, Kind: obs},
		{Layer: catalog.L3, Subject: store.EdgeSubject(instance, checkout, catalog.InstanceOf), Producer: "mesh", EventTime: store.MaxEventTime, Seq: 9, Kind: obs},
		{
			Layer: catalog.L3, Subject: store.EdgeSubject(checkout, payments, catalog.DependsOn), Producer: "mesh", EventTime: t0.Add(7 * time.Second), Seq: 10, Kind: obs,
			TTL: 30 * time.Second, Through: t0.Add(90 * time.Second), Payload: []byte("calls/min=120"),
		},
		{Layer: catalog.L2, Subject: store.EntitySubject(pod), Producer: "k8s", EventTime: t0.Add(8 * time.Second), Seq: 11, Kind: del},
		{Layer: catalog.L2, Subject: store.EdgeSubject(pod, node, catalog.ScheduledOn), Producer: "k8s", EventTime: t0.Add(9 * time.Second), Seq: 12, Kind: del},
		{
			Layer: catalog.L1, Subject: store.EntitySubject(host), Producer: "cloud", EventTime: t0.Add(10 * time.Second), Seq: 1<<63 + 5, Kind: obs,
			Payload: []byte("late"), Boot: "ブート-2",
		},
		{Layer: catalog.L1, Subject: store.EdgeSubject(host, rack, catalog.LocatedIn), Producer: "dcim", EventTime: t0.Add(11 * time.Second), Seq: math.MaxUint64, Kind: del},
	}
}())

// withBases gives the records the event time bases 0 to 4 in turn, so that every
// basis is in the file, on observations and deletes alike.
func withBases(recs []store.Record) []store.Record {
	for i := range recs {
		recs[i].EventTimeBasis = store.EventTimeBasis(i % (int(store.BasisProducerEvent) + 1))
	}
	return recs
}

// The golden records must keep covering what the contract says they cover.
func TestGoldenRecordsCover(t *testing.T) {
	t.Parallel()

	layers := map[catalog.Layer]bool{}
	relations := map[catalog.RelationType]bool{}
	bases := map[store.EventTimeBasis]bool{}
	var deleteEntity, deleteEdge, through, ttl, emptyPayload, minTime, maxTime, bigSeq, boot, noBoot bool
	for i, r := range goldenRecords {
		if err := r.Validate(); err != nil {
			t.Errorf("golden record %d is not valid: %v", i, err)
		}
		if r.Subject.Kind == store.SubjectEntity {
			layers[r.Layer] = true
		} else {
			relations[r.Subject.Relation] = true
		}
		if r.Kind == lifecycle.Delete {
			deleteEntity = deleteEntity || r.Subject.Kind == store.SubjectEntity
			deleteEdge = deleteEdge || r.Subject.Kind == store.SubjectEdge
		}
		through = through || !r.Through.IsZero()
		ttl = ttl || r.TTL > 0
		emptyPayload = emptyPayload || (r.Kind == lifecycle.Observe && len(r.Payload) == 0)
		minTime = minTime || r.EventTime.Equal(store.MinEventTime)
		maxTime = maxTime || r.EventTime.Equal(store.MaxEventTime)
		bigSeq = bigSeq || r.Seq > 1<<63
		boot = boot || r.Boot != ""
		bases[r.EventTimeBasis] = true
		noBoot = noBoot || (r.Boot == "" && r.Kind == lifecycle.Observe && r.Subject.Kind == store.SubjectEntity)
	}
	for b := store.BasisUnknown; b <= store.BasisProducerEvent; b++ {
		if !bases[b] {
			t.Errorf("no record with the event time basis %s", b)
		}
	}
	for _, l := range []catalog.Layer{catalog.L0, catalog.L1, catalog.L2, catalog.L3} {
		if !layers[l] {
			t.Errorf("no entity in layer %s", l)
		}
	}
	for rel := range catalog.Default().Relations() {
		if !rel.Derived() && !relations[rel.Type()] {
			t.Errorf("no edge of relation %s", rel.Type())
		}
	}
	for name, ok := range map[string]bool{
		"a delete of an entity": deleteEntity, "a delete of an edge": deleteEdge, "a run with Through": through, "a TTL": ttl,
		"an empty payload": emptyPayload, "the minimum event time": minTime, "the maximum event time": maxTime, "a Seq above 2^63": bigSeq,
		"a boot id": boot, "an observation of an entity without a boot id": noBoot,
	} {
		if !ok {
			t.Errorf("no record with %s", name)
		}
	}
	if !bytes.Contains(goldenRecords[5].Payload, everyByte()) {
		t.Error("no payload with every byte value")
	}
}

func TestGolden(t *testing.T) {
	// Not parallel: -update writes the file.
	if *update {
		data := writeFile(t, goldenRecords, activity.WriterOptions{RowGroupRows: goldenRowGroupRows})
		if err := os.MkdirAll(filepath.Dir(goldenFile), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(goldenFile, data, 0o644); err != nil {
			t.Fatal(err)
		}
	}

	data, err := os.ReadFile(goldenFile)
	if err != nil {
		t.Fatalf("%v: the golden file is part of the repository", err)
	}
	r, err := openFile(data, activity.ReaderOptions{})
	if err != nil {
		t.Fatalf("NewReader on the golden file: %v", err)
	}
	last := goldenRecords[len(goldenRecords)-1]
	want := activity.Info{Version: 1, Records: int64(len(goldenRecords)), MinSeq: goldenRecords[0].Seq, MaxSeq: last.Seq, RowGroups: 2}
	if got := r.Info(); got != want {
		t.Errorf("Info() = %+v, want %+v", got, want)
	}
	got, err := drain(r)
	if err != nil {
		t.Fatalf("reading the golden file: %v", err)
	}
	requireSame(t, goldenRecords, got)

	pf, err := file.NewParquetReader(bytes.NewReader(data))
	if err != nil {
		t.Fatal(err)
	}
	kv := footerMetadata(pf)
	if digest := kv["toposhift.activity.digest"]; digest != goldenDigest {
		t.Errorf("digest in the golden file = %s, want %s", digest, goldenDigest)
	}
	if groups := kv["toposhift.activity.group_digests"]; groups != goldenGroupDigests {
		t.Errorf("group digests in the golden file = %s, want %s", groups, goldenGroupDigests)
	}
}

// Writing the golden records now reads back the same records and has the
// pinned digest. The bytes are not compared: a library upgrade may encode
// differently without changing the format.
func TestGoldenRecordsRoundTrip(t *testing.T) {
	t.Parallel()

	data := writeFile(t, goldenRecords, activity.WriterOptions{RowGroupRows: goldenRowGroupRows})
	requireSame(t, goldenRecords, readFile(t, data, activity.ReaderOptions{}))

	pf, err := file.NewParquetReader(bytes.NewReader(data))
	if err != nil {
		t.Fatal(err)
	}
	kv := footerMetadata(pf)
	if digest := kv["toposhift.activity.digest"]; digest != goldenDigest {
		t.Errorf("digest of a file written now = %s, want the pinned %s", digest, goldenDigest)
	}
	if groups := kv["toposhift.activity.group_digests"]; groups != goldenGroupDigests {
		t.Errorf("group digests of a file written now = %s, want the pinned %s", groups, goldenGroupDigests)
	}
}

// specGroupDigest computes the digest of one row group from its records, as
// the package documentation describes it, independently of the package's own
// code.
func specGroupDigest(recs []store.Record) string {
	h := sha256.New()
	u64 := func(v uint64) { h.Write(binary.BigEndian.AppendUint64(nil, v)) }
	text := func(s string) {
		h.Write(binary.BigEndian.AppendUint32(nil, uint32(len(s))))
		h.Write([]byte(s))
	}
	for _, r := range recs {
		u64(r.Seq)
		u64(uint64(r.EventTime.UnixNano()))
		text(r.Layer.String())
		text(map[store.SubjectKind]string{store.SubjectEntity: "entity", store.SubjectEdge: "edge"}[r.Subject.Kind])
		text(r.Subject.A.String())
		text(r.Subject.B.String()) // empty for an entity
		text(string(r.Subject.Relation))
		text(string(r.Producer))
		text(r.Kind.String())
		u64(uint64(r.TTL))
		if r.Through.IsZero() {
			h.Write([]byte{0})
			u64(0)
		} else {
			h.Write([]byte{1})
			u64(uint64(r.Through.UnixNano()))
		}
		h.Write(binary.BigEndian.AppendUint32(nil, uint32(len(r.Payload))))
		h.Write(r.Payload)
		if r.Boot == "" {
			h.Write([]byte{0})
		} else {
			h.Write([]byte{1})
		}
		text(r.Boot)
		h.Write(binary.BigEndian.AppendUint32(nil, uint32(r.EventTimeBasis)))
	}
	return hex.EncodeToString(h.Sum(nil))
}

// specFileDigest is the digest of a file from the digests of its row groups:
// the SHA-256 of their raw bytes in file order.
func specFileDigest(groups []string) string {
	h := sha256.New()
	for _, g := range groups {
		raw, err := hex.DecodeString(g)
		if err != nil {
			panic(err)
		}
		h.Write(raw)
	}
	return "sha256:" + hex.EncodeToString(h.Sum(nil))
}

// specDigests returns the group digests and the file digest of records written
// in row groups of the given size.
func specDigests(recs []store.Record, rowGroupRows int) (groups []string, file string) {
	for len(recs) > 0 {
		n := min(rowGroupRows, len(recs))
		groups = append(groups, specGroupDigest(recs[:n]))
		recs = recs[n:]
	}
	return groups, specFileDigest(groups)
}

func TestDigestFollowsTheSpecification(t *testing.T) {
	t.Parallel()

	groups, file := specDigests(goldenRecords, goldenRowGroupRows)
	if file != goldenDigest || strings.Join(groups, ",") != goldenGroupDigests {
		t.Errorf("by the written specification goldenRecords have group digests %v and file digest %s, pinned are %s and %s",
			groups, file, goldenGroupDigests, goldenDigest)
	}
	recs := sample(t, 50)
	pf, err := file2reader(writeFile(t, recs, activity.WriterOptions{RowGroupRows: 8}))
	if err != nil {
		t.Fatal(err)
	}
	kv := footerMetadata(pf)
	wantGroups, wantFile := specDigests(recs, 8)
	if got := kv["toposhift.activity.group_digests"]; got != strings.Join(wantGroups, ",") {
		t.Errorf("group digests in the footer = %s, want %s", got, strings.Join(wantGroups, ","))
	}
	if got := kv["toposhift.activity.digest"]; got != wantFile {
		t.Errorf("digest in the footer = %s, want %s", got, wantFile)
	}
}

func file2reader(data []byte) (*file.Reader, error) {
	return file.NewParquetReader(bytes.NewReader(data))
}
