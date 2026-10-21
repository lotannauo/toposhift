package duckmodel

import (
	"slices"
	"strings"
	"testing"
	"time"

	"pgregory.net/rapid"

	"github.com/lotannauo/toposhift/bench/spike/engine"
	"github.com/lotannauo/toposhift/internal/catalog"
	"github.com/lotannauo/toposhift/internal/identity"
	"github.com/lotannauo/toposhift/internal/lifecycle"
)

// testingT is what fp needs of a *testing.T or a *rapid.T.
type testingT interface {
	Helper()
	Fatal(args ...any)
}

// fp is a fingerprint of the type whose 16 hash bytes are all b.
func fp(t testingT, typ catalog.EntityType, b byte) identity.Fingerprint {
	t.Helper()
	var hash [identity.FingerprintBytes]byte
	for i := range hash {
		hash[i] = b
	}
	f, err := identity.FingerprintFromHash(typ, hash)
	if err != nil {
		t.Fatal(err)
	}
	return f
}

func sign(n int) int {
	switch {
	case n < 0:
		return -1
	case n > 0:
		return 1
	}
	return 0
}

// TestTextOrderIsNotTheContractsOrder pins why an answer is ordered in Go: where a type
// is the prefix of another and the byte after it is below the colon, the text of the
// shorter sorts after the longer, and the contract puts it first. Where the next byte
// is above the colon the two orders agree.
func TestTextOrderIsNotTheContractsOrder(t *testing.T) {
	short, dotted, digit, under := fp(t, "k8s", 0x11), fp(t, "k8s.pod", 0x11), fp(t, "k8s0pod", 0x11), fp(t, "k8s_pod", 0x11)
	for _, c := range []struct {
		name     string
		a, b     identity.Fingerprint
		disagree bool
	}{
		{"a dot follows the shorter type", short, dotted, true},
		{"a digit follows it", short, digit, true},
		{"an underscore follows it", short, under, false},
		{"the same type, another hash", fp(t, "k8s", 0x11), fp(t, "k8s", 0x12), false},
		{"unrelated types", fp(t, "host", 0xff), fp(t, "service", 0x00), false},
	} {
		text, contract := sign(CompareText(c.a, c.b)), sign(engine.CompareFingerprints(c.a, c.b))
		if (text != contract) != c.disagree {
			t.Errorf("%s: text order %d, contract order %d", c.name, text, contract)
		}
		if sign(CompareText(c.b, c.a)) != -text {
			t.Errorf("%s: the text order is not antisymmetric", c.name)
		}
	}
}

// textRows are neighbor rows as a statement returns them: for the text of each peer.
func textRows(peers []identity.Fingerprint, rel string) []PeerText {
	rows := make([]PeerText, len(peers))
	for i, p := range peers {
		rows[i] = PeerText{Peer: Text(p), Relation: rel}
	}
	return rows
}

// TestNeighborsFromTextOrdersAsTheContractDoes gives the rows in the order a database
// would (text order, then relation) and requires the contract's order out.
func TestNeighborsFromTextOrdersAsTheContractDoes(t *testing.T) {
	peers := []identity.Fingerprint{
		fp(t, "k8s", 0x22), fp(t, "k8s", 0x11), fp(t, "k8s.pod", 0x33), fp(t, "k8s0pod", 0x44), fp(t, "k8s_pod", 0x55),
		fp(t, "k8s.pod.x", 0x66), fp(t, "k8s.pod", 0x01), fp(t, "service", 0x77),
	}
	rows := textRows(peers, "runs_on")
	rows = append(rows, PeerText{Peer: Text(peers[0]), Relation: "calls"}, PeerText{Peer: Text(peers[0]), Relation: "backs"})
	slices.SortFunc(rows, func(a, b PeerText) int {
		if a.Peer != b.Peer {
			return strings.Compare(a.Peer, b.Peer)
		}
		return strings.Compare(a.Relation, b.Relation)
	})

	got, err := NeighborsFromText(rows)
	if err != nil {
		t.Fatal(err)
	}
	want := make([]engine.Neighbor, 0, len(rows))
	for _, r := range rows {
		p, err := ParseText(r.Peer)
		if err != nil {
			t.Fatal(err)
		}
		want = append(want, engine.Neighbor{Peer: p, Relation: catalog.RelationType(r.Relation)})
	}
	slices.SortFunc(want, func(a, b engine.Neighbor) int {
		if c := engine.CompareFingerprints(a.Peer, b.Peer); c != 0 {
			return c
		}
		return strings.Compare(string(a.Relation), string(b.Relation))
	})
	if !slices.Equal(got, want) {
		t.Fatalf("the rows in text order came out as\n%v\nwant\n%v", got, want)
	}
	// The contract puts k8s before k8s.pod before k8s0pod, which the text does not.
	var types []catalog.EntityType
	for _, n := range got {
		if len(types) == 0 || types[len(types)-1] != n.Peer.Type() {
			types = append(types, n.Peer.Type())
		}
	}
	if !slices.Equal(types, []catalog.EntityType{"k8s", "k8s.pod", "k8s.pod.x", "k8s0pod", "k8s_pod", "service"}) {
		t.Fatalf("the types in order: %v", types)
	}

	// The mutant orders by the text, and so does not.
	bad, err := neighborsFromText(rows, mutOrderByText)
	if err != nil {
		t.Fatal(err)
	}
	if slices.Equal(bad, want) {
		t.Fatal("ordering by the text of the fingerprint gives the contract's order on this input, so it cannot be told apart")
	}
}

// TestNeighborsFromTextIsStrict refuses a peer that is not a fingerprint.
func TestNeighborsFromTextIsStrict(t *testing.T) {
	good := Text(fp(t, "host", 0xab))
	for _, bad := range []string{"", "host", "host:", "host:AB", good + "0", good[:len(good)-1], "Host:" + good[5:], ":" + good[5:], "host " + good[5:]} {
		if _, err := NeighborsFromText([]PeerText{{Peer: good, Relation: "r"}, {Peer: bad, Relation: "r"}}); err == nil {
			t.Errorf("%q was accepted as a peer", bad)
		}
	}
	if ns, err := NeighborsFromText(nil); err != nil || len(ns) != 0 {
		t.Errorf("no rows: %v, %v", ns, err)
	}
}

// TestNeighborsFromTextAnyOrder: whatever order the rows arrive in, the answer is the one
// order the contract has.
func TestNeighborsFromTextAnyOrder(t *testing.T) {
	types := []catalog.EntityType{"k8s", "k8s.pod", "k8s0pod", "k8s_pod", "host", "host.x"}
	rapid.Check(t, func(rt *rapid.T) {
		n := rapid.IntRange(0, 24).Draw(rt, "n")
		var rows []PeerText
		for range n {
			typ := rapid.SampledFrom(types).Draw(rt, "type")
			b := rapid.SampledFrom([]byte{0x00, 0x01, 0x7f, 0xff}).Draw(rt, "hash")
			rel := rapid.SampledFrom([]string{"calls", "runs_on"}).Draw(rt, "relation")
			rows = append(rows, PeerText{Peer: Text(fp(rt, typ, b)), Relation: rel})
		}
		rows = slices.Compact(slices.SortedFunc(slices.Values(rows), func(a, b PeerText) int {
			if a.Peer != b.Peer {
				return strings.Compare(a.Peer, b.Peer)
			}
			return strings.Compare(a.Relation, b.Relation)
		}))
		perm := rapid.Permutation(rows).Draw(rt, "order")
		got, err := NeighborsFromText(perm)
		if err != nil {
			rt.Fatal(err)
		}
		if !slices.IsSortedFunc(got, func(a, b engine.Neighbor) int {
			if c := engine.CompareFingerprints(a.Peer, b.Peer); c != 0 {
				return c
			}
			return strings.Compare(string(a.Relation), string(b.Relation))
		}) || len(got) != len(rows) {
			rt.Fatalf("not in the contract's order: %v", got)
		}
	})
}

// TestModelAnswersInTheContractsOrder reads, through the model, the edges of one source
// whose targets have types like those above, written in an order that is neither.
func TestModelAnswersInTheContractsOrder(t *testing.T) {
	src := fp(t, "host", 0x01)
	targets := []identity.Fingerprint{fp(t, "k8s_pod", 1), fp(t, "k8s0pod", 1), fp(t, "k8s", 2), fp(t, "k8s.pod", 1), fp(t, "k8s", 1)}
	for _, arm := range arms {
		m := New(arm)
		for i, tg := range targets {
			err := m.Write([]engine.Record{{
				Layer: catalog.L2, Subject: engine.EdgeSubject(src, tg, catalog.ScheduledOn), Producer: "p",
				EventTime: time.Unix(10, 0).UTC(), Seq: uint64(i + 1), Kind: lifecycle.Observe,
			}})
			if err != nil {
				t.Fatal(err)
			}
		}
		got, err := m.Neighbors(src, engine.Forward, time.Unix(11, 0).UTC(), engine.Current(catalog.L2))
		if err != nil {
			t.Fatal(err)
		}
		if len(got) != len(targets) {
			t.Fatalf("%s: %d neighbors", arm, len(got))
		}
		for i := 1; i < len(got); i++ {
			if engine.CompareFingerprints(got[i-1].Peer, got[i].Peer) >= 0 {
				t.Fatalf("%s: neighbors %s then %s are out of the contract's order", arm, got[i-1].Peer, got[i].Peer)
			}
		}
	}
}
