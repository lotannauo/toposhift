package k8sobjects_test

import (
	"context"
	"fmt"
	"slices"
	"testing"
	"time"

	"github.com/lotannauo/toposhift/internal/catalog"
	"github.com/lotannauo/toposhift/internal/coalesce"
	"github.com/lotannauo/toposhift/internal/endpoint"
	"github.com/lotannauo/toposhift/internal/identity"
	"github.com/lotannauo/toposhift/internal/ingest/k8sobjects"
	"github.com/lotannauo/toposhift/internal/ingest/resume"
	"github.com/lotannauo/toposhift/internal/lifecycle"
	"github.com/lotannauo/toposhift/internal/store"
	"github.com/lotannauo/toposhift/internal/store/memstore"
)

// The scenarios of this file write out every record they expect, in order: the
// address claims (has_address edges from a pod or a node to an endpoint) and the
// endpoints are asserted only as the scenarios say, and nothing else is written.

// anyPayload stands for the payload of a pod or a node, which other tests check.
const anyPayload = "*"

// wantRec is one record a scenario expects.
type wantRec struct {
	subj    store.Subject
	kind    lifecycle.Kind
	at      time.Time
	basis   store.EventTimeBasis
	ttl     time.Duration
	through time.Time
	payload string
}

// checkRecs compares records with the expected ones, in order, and checks that
// each is stored in the layer the catalog gives its subject and carries the test
// producer.
func checkRecs(t *testing.T, what string, got []store.Record, want ...wantRec) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("%s: %d records, want %d:\n%s", what, len(got), len(want), describe(got))
	}
	c := catalog.Default()
	for i, w := range want {
		g := got[i]
		var layer catalog.Layer
		if g.Subject.Kind == store.SubjectEdge {
			r, _ := c.Relation(g.Subject.Relation)
			layer = r.Layer()
		} else {
			e, _ := c.Entity(g.Subject.A.Type())
			layer = e.Layer()
		}
		payloadOK := w.payload == anyPayload || string(g.Payload) == w.payload
		if g.Subject != w.subj || g.Kind != w.kind || !g.EventTime.Equal(w.at) || g.EventTimeBasis != w.basis ||
			g.TTL != w.ttl || !g.Through.Equal(w.through) || !payloadOK || g.Producer != testProducer || g.Layer != layer {
			t.Errorf("%s: record %d\n got %s\nwant %s %v at %s basis %s ttl %s through %s payload %s, layer %s",
				what, i, describe(got[i:i+1]), w.kind, w.subj, w.at.Format(time.RFC3339Nano), w.basis, w.ttl, w.through.Format(time.RFC3339Nano), w.payload, layer)
		}
	}
}

func describe(rs []store.Record) string {
	s := ""
	for _, r := range rs {
		s += fmt.Sprintf("  %s %v at %s basis %s ttl %s through %s payload %s layer %s\n",
			r.Kind, r.Subject, r.EventTime.Format(time.RFC3339Nano), r.EventTimeBasis, r.TTL, r.Through.Format(time.RFC3339Nano), r.Payload, r.Layer)
	}
	return s
}

func entity(f identity.Fingerprint) store.Subject { return store.EntitySubject(f) }

// endpointOf is the endpoint entity of an address; claimOf is the claim of a
// holder on it.
func endpointOf(t testing.TB, addr string) store.Subject {
	return store.EntitySubject(endpointFP(t, addr))
}

func claimOf(t testing.TB, holder identity.Fingerprint, addr string) store.Subject {
	return store.EdgeSubject(holder, endpointFP(t, addr), catalog.HasAddress)
}

// observed is an Observe timed at its observation: no Through.
func observed(subj store.Subject, at time.Time, ttl time.Duration, payload string) wantRec {
	return wantRec{subj: subj, kind: lifecycle.Observe, at: at, basis: store.BasisObserved, ttl: ttl, payload: payload}
}

func deleted(subj store.Subject, at time.Time, basis store.EventTimeBasis) wantRec {
	return wantRec{subj: subj, kind: lifecycle.Delete, at: at, basis: basis}
}

// claimed is the pair a claim begins or is re-asserted with: its endpoint, then
// the claim itself.
func claimed(t testing.TB, holder identity.Fingerprint, addr string, at time.Time, ttl time.Duration, payload string) []wantRec {
	return []wantRec{observed(endpointOf(t, addr), at, ttl, "{}"), observed(claimOf(t, holder, addr), at, ttl, payload)}
}

// recs joins expectations.
func recs(parts ...any) []wantRec {
	var out []wantRec
	for _, p := range parts {
		switch x := p.(type) {
		case wantRec:
			out = append(out, x)
		case []wantRec:
			out = append(out, x...)
		default:
			panic(fmt.Sprintf("recs: %T", p))
		}
	}
	return out
}

func notes(r k8sobjects.Result) map[string]int {
	out := map[string]int{}
	for _, n := range r.Notes {
		out[n.Reason] += n.Count
	}
	return out
}

// The address appears on a MODIFIED, whose change is timed by a condition before
// it was observed. The pod's record takes that time; the claim begins at the
// observed time of the record that first shows the address, never at the pod's
// creation and never with a Through. A MODIFIED that changes nothing writes
// nothing, the claim included.
func TestClaimBeginsWhenTheAddressIsFirstSeen(t *testing.T) {
	tr := newTranslator(t, 0)
	pod := podFP(t, "p1")
	p := podSpec{UID: "p1", Name: "p", Created: t0, Phase: "Pending"}
	r := translate(t, tr, watchRec(t0.Add(time.Second), "ADDED", p.object()))
	checkRecs(t, "ADDED", r.Records,
		wantRec{subj: entity(pod), kind: lifecycle.Observe, at: t0, basis: store.BasisObjectField, through: t0.Add(time.Second), payload: anyPayload})

	p.Phase, p.IPs = "Running", []string{"10.1.0.5"}
	p.Conds = []map[string]any{cond("Ready", "True", t0.Add(20*time.Second))}
	seen := t0.Add(20*time.Second + 500*time.Millisecond)
	r = translate(t, tr, watchRec(seen, "MODIFIED", p.object()))
	checkRecs(t, "MODIFIED", r.Records, recs(
		wantRec{subj: entity(pod), kind: lifecycle.Observe, at: t0.Add(20 * time.Second), basis: store.BasisObjectField, through: seen, payload: anyPayload},
		claimed(t, pod, "10.1.0.5", seen, 0, "{}"),
	)...)
	if len(r.Backdated) != 0 {
		t.Errorf("a later record marked %d records", len(r.Backdated))
	}

	p.Heartbeat = "2"
	r = translate(t, tr, watchRec(seen.Add(time.Minute), "MODIFIED", p.object()))
	checkRecs(t, "unchanged MODIFIED", r.Records)
}

// With a TTL, the claim and its endpoint are re-asserted with each record of the
// pod, at its observation, and never before the claim's own latest assertion: a
// watch event observed before the latest list is held at the list's time.
func TestClaimIsReassertedWithItsPod(t *testing.T) {
	const ttl = 15 * time.Minute
	tr := newTranslator(t, ttl)
	pod := podFP(t, "p1")
	p := podSpec{UID: "p1", Name: "p", Created: t0, Phase: "Running", IPs: []string{"10.1.0.5"}}

	r := translate(t, tr, pullRec(t0.Add(time.Minute), p.object()))
	checkRecs(t, "first list", r.Records, recs(
		wantRec{subj: entity(pod), kind: lifecycle.Observe, at: t0, basis: store.BasisObjectField, ttl: ttl, through: t0.Add(time.Minute), payload: anyPayload},
		claimed(t, pod, "10.1.0.5", t0.Add(time.Minute), ttl, "{}"),
	)...)

	r = translate(t, tr, pullRec(t0.Add(6*time.Minute), p.object()))
	checkRecs(t, "second list", r.Records, recs(
		observed(entity(pod), t0.Add(6*time.Minute), ttl, anyPayload),
		claimed(t, pod, "10.1.0.5", t0.Add(6*time.Minute), ttl, "{}"),
	)...)

	// Observed a minute before the last list, and unchanged.
	r = translate(t, tr, watchRec(t0.Add(5*time.Minute), "MODIFIED", p.object()))
	checkRecs(t, "older watch event", r.Records, recs(
		observed(entity(pod), t0.Add(6*time.Minute), ttl, anyPayload),
		claimed(t, pod, "10.1.0.5", t0.Add(6*time.Minute), ttl, "{}"),
	)...)

	// A change timed by a condition before its observation: the pod takes that
	// time, the claim its observation.
	p.Conds = []map[string]any{cond("Ready", "False", t0.Add(7*time.Minute))}
	r = translate(t, tr, watchRec(t0.Add(8*time.Minute), "MODIFIED", p.object()))
	checkRecs(t, "changed watch event", r.Records, recs(
		wantRec{subj: entity(pod), kind: lifecycle.Observe, at: t0.Add(7 * time.Minute), basis: store.BasisObjectField, ttl: ttl, through: t0.Add(8 * time.Minute), payload: anyPayload},
		claimed(t, pod, "10.1.0.5", t0.Add(8*time.Minute), ttl, "{}"),
	)...)
}

// A pod in phase Succeeded still lists its address. Its claim ends at the record
// that shows the phase, at that record's time (here the finish of its container),
// and no later record of the pod claims the address again; its DELETED has no
// claim left to end.
func TestTerminalPhaseEndsTheClaim(t *testing.T) {
	const ttl = 15 * time.Minute
	tr := newTranslator(t, ttl)
	pod := podFP(t, "p1")
	p := podSpec{UID: "p1", Name: "p", Created: t0, Phase: "Running", IPs: []string{"10.1.0.5"}}
	translate(t, tr, pullRec(t0.Add(time.Minute), p.object()))

	p.Phase = "Succeeded"
	p.Containers = []map[string]any{{"name": "c", "restartCount": 0, "state": map[string]any{"terminated": map[string]any{
		"startedAt": ts(t0), "finishedAt": ts(t0.Add(10 * time.Minute)),
	}}}}
	seen := t0.Add(10*time.Minute + 300*time.Millisecond)
	r := translate(t, tr, watchRec(seen, "MODIFIED", p.object()))
	checkRecs(t, "Succeeded", r.Records,
		wantRec{subj: entity(pod), kind: lifecycle.Observe, at: t0.Add(10 * time.Minute), basis: store.BasisObjectField, ttl: ttl, through: seen, payload: anyPayload},
		deleted(claimOf(t, pod, "10.1.0.5"), t0.Add(10*time.Minute), store.BasisObjectField),
	)

	r = translate(t, tr, pullRec(t0.Add(15*time.Minute), p.object()))
	checkRecs(t, "a later list of the Succeeded pod", r.Records, observed(entity(pod), t0.Add(15*time.Minute), ttl, anyPayload))

	r = translate(t, tr, watchRec(t0.Add(20*time.Minute), "DELETED", p.object()))
	checkRecs(t, "DELETED", r.Records, deleted(entity(pod), t0.Add(20*time.Minute), store.BasisObserved))

	// Failed is terminal too, and a pod first seen in a terminal phase claims nothing.
	q := podSpec{UID: "p2", Name: "q", Created: t0, Phase: "Failed", IPs: []string{"10.1.0.6"}}
	r = translate(t, tr, pullRec(t0.Add(21*time.Minute), q.object()))
	checkRecs(t, "a Failed pod first seen", r.Records,
		wantRec{subj: entity(podFP(t, "p2")), kind: lifecycle.Observe, at: t0, basis: store.BasisObjectField, ttl: ttl, through: t0.Add(21 * time.Minute), payload: anyPayload})
}

// A claim's end is never timed before the claim's latest assertion. A pod whose
// address appears on a record observed half a second after the condition that
// times it, and whose container finishes within that second, reaches Succeeded at
// a time before its claim began: the pod's record takes that time, and the claim's
// end is held at the claim's start, with its basis.
func TestEndIsNeverBeforeTheClaimsLatestAssertion(t *testing.T) {
	tr := newTranslator(t, 0)
	pod := podFP(t, "p1")
	p := podSpec{UID: "p1", Name: "p", Created: t0, Phase: "Pending"}
	translate(t, tr, watchRec(t0.Add(time.Second), "ADDED", p.object()))
	p.Phase, p.IPs = "Running", []string{"10.1.0.5"}
	p.Conds = []map[string]any{cond("Ready", "True", t0.Add(20*time.Second))}
	seen := t0.Add(20*time.Second + 500*time.Millisecond)
	translate(t, tr, watchRec(seen, "MODIFIED", p.object()))

	p.Phase = "Succeeded"
	p.Containers = []map[string]any{{"name": "c", "restartCount": 0, "state": map[string]any{"terminated": map[string]any{
		"startedAt": ts(t0.Add(20 * time.Second)), "finishedAt": ts(t0.Add(20 * time.Second)),
	}}}}
	done := t0.Add(30 * time.Second)
	r := translate(t, tr, watchRec(done, "MODIFIED", p.object()))
	checkRecs(t, "Succeeded within the second the claim began", r.Records,
		wantRec{subj: entity(pod), kind: lifecycle.Observe, at: t0.Add(20 * time.Second), basis: store.BasisObjectField, through: done, payload: anyPayload},
		deleted(claimOf(t, pod, "10.1.0.5"), seen, store.BasisObserved),
	)
	if hasSkip(r, k8sobjects.SkipClampedTime) {
		t.Errorf("holding a claim's end was counted: %v", r.Skipped)
	}
}

// A pod on its node's network lists the node's address. It claims nothing, and
// each address is counted, at each record.
func TestHostNetworkPodClaimsNothing(t *testing.T) {
	for _, ttl := range ttls {
		tr := newTranslator(t, ttl)
		pod := podFP(t, "p1")
		p := podSpec{UID: "p1", Name: "p", Created: t0, Phase: "Running", IPs: []string{"10.0.0.2"}, HostNetwork: true}
		r := translate(t, tr, pullRec(t0.Add(time.Minute), p.object()))
		checkRecs(t, "host-network pod", r.Records,
			wantRec{subj: entity(pod), kind: lifecycle.Observe, at: t0, basis: store.BasisObjectField, ttl: ttl, through: t0.Add(time.Minute), payload: anyPayload})
		if got := skips(r); len(got) != 1 || got[k8sobjects.SkipHostNetworkAddress] != 1 {
			t.Errorf("ttl %s: skips %v, want one host-network address", ttl, got)
		}
		r = translate(t, tr, watchRec(t0.Add(2*time.Minute), "DELETED", p.object()))
		checkRecs(t, "its DELETED", r.Records, deleted(entity(pod), t0.Add(2*time.Minute), store.BasisObserved))
	}
}

// A pod's DELETED ends its claim at the observed time, before the pod itself. The
// endpoint is never deleted, and the next pod given the address claims it with
// no note: the two claims do not overlap.
func TestDeletedEndsTheClaimAndKeepsTheEndpoint(t *testing.T) {
	tr := newTranslator(t, 0)
	a, b := podFP(t, "pa"), podFP(t, "pb")
	pa := podSpec{UID: "pa", Name: "a", Created: t0, Phase: "Running", IPs: []string{"10.1.0.5"}}
	translate(t, tr, pullRec(t0.Add(time.Minute), pa.object()))

	gone := t0.Add(30 * time.Minute)
	r := translate(t, tr, watchRec(gone, "DELETED", pa.object()))
	checkRecs(t, "DELETED", r.Records,
		deleted(claimOf(t, a, "10.1.0.5"), gone, store.BasisObserved),
		deleted(entity(a), gone, store.BasisObserved),
	)

	pb := podSpec{UID: "pb", Name: "b", Created: gone, Phase: "Running", IPs: []string{"10.1.0.5"}}
	next := gone.Add(time.Second)
	r = translate(t, tr, watchRec(next, "MODIFIED", pb.object()))
	checkRecs(t, "the next holder", r.Records, recs(
		wantRec{subj: entity(b), kind: lifecycle.Observe, at: gone, basis: store.BasisObjectField, through: next, payload: anyPayload},
		claimed(t, b, "10.1.0.5", next, 0, "{}"),
	)...)
	if len(r.Notes) != 0 {
		t.Errorf("a reuse after the DELETED noted %v", r.Notes)
	}
}

// A pod whose DELETED was lost: its claim lapses a TTL after its last record. A
// second pod given the address meanwhile claims it, the note says so, and in the
// store both pods claim the endpoint exactly over the overlap, never before the
// second claim and never after the first lapses.
func TestLostDeleteOverlapsOnlyUntilTheClaimLapses(t *testing.T) {
	const ttl = 15 * time.Minute
	tr := newTranslator(t, ttl)
	a, b := podFP(t, "pa"), podFP(t, "pb")
	pa := podSpec{UID: "pa", Name: "a", Created: t0, Phase: "Running", IPs: []string{"10.1.0.5"}}
	pb := podSpec{UID: "pb", Name: "b", Created: t0.Add(10 * time.Minute), Phase: "Running", IPs: []string{"10.1.0.5"}}
	var batches [][]store.Record
	for _, m := range []int{1, 6} {
		r := translate(t, tr, pullRec(t0.Add(time.Duration(m)*time.Minute), pa.object()))
		batches = append(batches, r.Records)
	}
	for i, m := range []int{11, 16, 21, 26} {
		r := translate(t, tr, pullRec(t0.Add(time.Duration(m)*time.Minute), pb.object()))
		want := map[string]int{}
		if i == 0 {
			want[k8sobjects.NoteAddressClaimedTwice] = 1
		}
		if got := notes(r); !mapsEqual(got, want) {
			t.Errorf("list of b at +%dm: notes %v, want %v", m, got, want)
		}
		for _, rec := range r.Records {
			if rec.Subject.A == a {
				t.Errorf("b's list wrote about a: %s", describe([]store.Record{rec}))
			}
		}
		batches = append(batches, r.Records)
	}
	// Written as the translator gave them: the coalescer absorbs a refresh closer
	// than half a TTL to its run's last extension, which with no Flush would end
	// each claim early by up to that much.
	s := storeRaw(t, batches...)
	ep := endpointFP(t, "10.1.0.5")
	lapse := t0.Add(6*time.Minute + ttl)
	for _, c := range []struct {
		at   time.Time
		want []identity.Fingerprint
	}{
		{t0.Add(time.Minute - time.Nanosecond), nil},
		{t0.Add(time.Minute), []identity.Fingerprint{a}},
		{t0.Add(11*time.Minute - time.Nanosecond), []identity.Fingerprint{a}},
		{t0.Add(11 * time.Minute), []identity.Fingerprint{a, b}},
		{lapse.Add(-time.Nanosecond), []identity.Fingerprint{a, b}},
		{lapse, []identity.Fingerprint{b}},
		{t0.Add(26*time.Minute + ttl - time.Nanosecond), []identity.Fingerprint{b}},
		{t0.Add(26*time.Minute + ttl), nil},
	} {
		got := claimants(t, s, ep, c.at)
		slices.SortFunc(c.want, compareFP)
		if !slices.Equal(got, c.want) {
			t.Errorf("at +%s the endpoint is claimed by %v, want %v", c.at.Sub(t0), got, c.want)
		}
	}
}

// storeRaw writes the batches to a memory store in order, with no coalescer.
func storeRaw(t *testing.T, batches ...[]store.Record) *memstore.Store {
	t.Helper()
	s, err := memstore.Open(memstore.Options{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.Close() })
	seq := uint64(0)
	for _, b := range batches {
		b = slices.Clone(b)
		for i := range b {
			seq++
			b[i].Seq = seq
		}
		if err := s.Write(context.Background(), b); err != nil {
			t.Fatal(err)
		}
	}
	return s
}

// claimants are the holders of the endpoint's claims live in the store at the
// time, sorted.
func claimants(t *testing.T, s store.Store, ep identity.Fingerprint, at time.Time) []identity.Fingerprint {
	t.Helper()
	ns, err := s.Neighbors(context.Background(), ep, store.Reverse, at, store.Current(catalog.L2))
	if err != nil {
		t.Fatal(err)
	}
	var out []identity.Fingerprint
	for _, n := range ns {
		if n.Relation == catalog.HasAddress {
			out = append(out, n.Peer)
		}
	}
	slices.SortFunc(out, compareFP)
	return out
}

func compareFP(a, b identity.Fingerprint) int {
	switch {
	case a.String() < b.String():
		return -1
	case a.String() > b.String():
		return 1
	}
	return 0
}

func mapsEqual(a, b map[string]int) bool {
	if len(a) != len(b) {
		return false
	}
	for k, v := range a {
		if b[k] != v {
			return false
		}
	}
	return true
}

// A dual-stack pod claims both addresses, each in canonical form (lower-case
// IPv6). An address that leaves the list ends its claim at the record's time,
// and the one that stays is not re-asserted without a TTL.
func TestAddressLeavingTheListEndsItsClaim(t *testing.T) {
	tr := newTranslator(t, 0)
	pod := podFP(t, "p1")
	p := podSpec{UID: "p1", Name: "p", Created: t0, Phase: "Running", IPs: []string{"10.1.0.5", "FD00::5"}}
	r := translate(t, tr, pullRec(t0.Add(time.Minute), p.object()))
	checkRecs(t, "dual stack", r.Records, recs(
		wantRec{subj: entity(pod), kind: lifecycle.Observe, at: t0, basis: store.BasisObjectField, through: t0.Add(time.Minute), payload: anyPayload},
		claimed(t, pod, "10.1.0.5", t0.Add(time.Minute), 0, "{}"),
		claimed(t, pod, "fd00::5", t0.Add(time.Minute), 0, "{}"),
	)...)

	p.IPs = []string{"10.1.0.5"}
	at := t0.Add(5*time.Minute + 100*time.Millisecond)
	r = translate(t, tr, watchRec(at, "MODIFIED", p.object()))
	checkRecs(t, "an address gone", r.Records,
		observed(entity(pod), at, 0, anyPayload),
		deleted(claimOf(t, pod, "fd00::5"), at, store.BasisObserved),
	)
}

// An address the canonical form refuses is counted and claims nothing; the
// pod's own record is unchanged (its description lists the addresses as given),
// and its other address is claimed.
func TestRefusedAddressIsCountedAndThePodIsUnchanged(t *testing.T) {
	tr := newTranslator(t, 0)
	pod := podFP(t, "p1")
	given := []string{"10.1.0.9", "not-an-ip", "fe80::1", "fd00::1%eth0", "010.1.0.1"}
	p := podSpec{UID: "p1", Name: "p", Created: t0, Phase: "Running", IPs: given}
	r := translate(t, tr, pullRec(t0.Add(time.Minute), p.object()))
	checkRecs(t, "refused addresses", r.Records, recs(
		wantRec{subj: entity(pod), kind: lifecycle.Observe, at: t0, basis: store.BasisObjectField, through: t0.Add(time.Minute), payload: anyPayload},
		claimed(t, pod, "10.1.0.9", t0.Add(time.Minute), 0, "{}"),
	)...)
	if got := skips(r); len(got) != 1 || got[k8sobjects.SkipBadAddress] != 4 {
		t.Errorf("skips %v, want four refused addresses", got)
	}
	ips, _ := payloadOf(t, r.Records[0])["pod_ips"].([]any)
	if len(ips) != len(given) {
		t.Errorf("the pod's description lists %v, want every address as given: %v", ips, given)
	}
}

// An IPv4-mapped address claims the IPv4 endpoint, and a pod that sets only
// status.podIP claims that address.
func TestMappedAndSingleAddressesClaimTheirCanonicalEndpoint(t *testing.T) {
	tr := newTranslator(t, 0)
	p1, p2 := podFP(t, "p1"), podFP(t, "p2")
	r := translate(t, tr, pullRec(t0.Add(time.Minute), podSpec{UID: "p1", Name: "a", Created: t0, Phase: "Running", IPs: []string{"::ffff:10.1.0.7"}}.object()))
	checkRecs(t, "v4-mapped", r.Records[1:], claimed(t, p1, "10.1.0.7", t0.Add(time.Minute), 0, "{}")...)
	r = translate(t, tr, pullRec(t0.Add(time.Minute), podSpec{UID: "p2", Name: "b", Created: t0, Phase: "Running", PodIP: "10.1.0.8"}.object()))
	checkRecs(t, "podIP alone", r.Records[1:], claimed(t, p2, "10.1.0.8", t0.Add(time.Minute), 0, "{}")...)
}

// A node claims its InternalIP and ExternalIP addresses, each with its type, and
// not its Hostname or DNS names. A public address is global: the same endpoint
// whatever the cluster. An address that leaves the list ends its claim; a type
// that changes re-asserts the claim with the new payload; the node's DELETED ends
// what is left before the node, and no endpoint is deleted.
func TestNodeClaimsItsInternalAndExternalAddresses(t *testing.T) {
	tr := newTranslator(t, 0)
	n := nodeFP(t, "n-1")
	addrs := [][2]string{{"Hostname", "n"}, {"InternalDNS", "n.internal"}, {"InternalIP", "10.0.0.2"}, {"ExternalIP", "34.1.2.3"}}
	r := translate(t, tr, pullRec(t0.Add(time.Minute), nodeWithAddrs("n-1", "n", t0, "1", addrs...)))
	checkRecs(t, "first list", r.Records, recs(
		wantRec{subj: entity(n), kind: lifecycle.Observe, at: t0, basis: store.BasisObjectField, through: t0.Add(time.Minute), payload: anyPayload},
		claimed(t, n, "10.0.0.2", t0.Add(time.Minute), 0, `{"type":"InternalIP"}`),
		claimed(t, n, "34.1.2.3", t0.Add(time.Minute), 0, `{"type":"ExternalIP"}`),
	)...)
	if len(r.Skipped) != 0 {
		t.Errorf("a node's names were counted as addresses: %v", r.Skipped)
	}
	elsewhere, err := endpoint.Identity("another-cluster", "34.1.2.3")
	if err != nil {
		t.Fatal(err)
	}
	if elsewhere.Fingerprint() != endpointFP(t, "34.1.2.3") {
		t.Error("a public address is not one endpoint across clusters")
	}

	r = translate(t, tr, watchRec(t0.Add(5*time.Minute), "MODIFIED", nodeWithAddrs("n-1", "n", t0, "2", addrs[:3]...)))
	checkRecs(t, "ExternalIP gone", r.Records,
		observed(entity(n), t0.Add(5*time.Minute), 0, anyPayload),
		deleted(claimOf(t, n, "34.1.2.3"), t0.Add(5*time.Minute), store.BasisObserved),
	)

	both := append(slices.Clone(addrs[:3]), [2]string{"ExternalIP", "10.0.0.2"})
	r = translate(t, tr, watchRec(t0.Add(6*time.Minute), "MODIFIED", nodeWithAddrs("n-1", "n", t0, "3", both...)))
	checkRecs(t, "one address under both types", r.Records, recs(
		observed(entity(n), t0.Add(6*time.Minute), 0, anyPayload),
		claimed(t, n, "10.0.0.2", t0.Add(6*time.Minute), 0, `{"type":"ExternalIP,InternalIP"}`),
	)...)

	r = translate(t, tr, watchRec(t0.Add(9*time.Minute), "DELETED", nodeWithAddrs("n-1", "n", t0, "4", both...)))
	checkRecs(t, "DELETED", r.Records,
		deleted(claimOf(t, n, "10.0.0.2"), t0.Add(9*time.Minute), store.BasisObserved),
		deleted(entity(n), t0.Add(9*time.Minute), store.BasisObserved),
	)
}

// The DELETED of a pod or a node the translator has not seen (it was seen before
// a restart) ends the claims of the addresses the deleted object lists, at the
// observed time. A host-network pod's lists none.
func TestDeleteOfAnUnseenObjectEndsItsClaims(t *testing.T) {
	tr := newTranslator(t, 0)
	at := t0.Add(time.Hour)
	p := podSpec{UID: "p1", Name: "p", Created: t0, Phase: "Running", IPs: []string{"10.1.0.5"}}
	r := translate(t, tr, watchRec(at, "DELETED", p.object()))
	checkRecs(t, "unseen pod", r.Records,
		deleted(claimOf(t, podFP(t, "p1"), "10.1.0.5"), at, store.BasisObserved),
		deleted(entity(podFP(t, "p1")), at, store.BasisObserved),
	)
	h := podSpec{UID: "p2", Name: "h", Created: t0, Phase: "Running", IPs: []string{"10.0.0.2"}, HostNetwork: true}
	r = translate(t, tr, watchRec(at, "DELETED", h.object()))
	checkRecs(t, "unseen host-network pod", r.Records, deleted(entity(podFP(t, "p2")), at, store.BasisObserved))
	r = translate(t, tr, watchRec(at, "DELETED", nodeWithAddrs("n-1", "n", t0, "1", [2]string{"InternalIP", "10.0.0.2"})))
	checkRecs(t, "unseen node", r.Records,
		deleted(claimOf(t, nodeFP(t, "n-1"), "10.0.0.2"), at, store.BasisObserved),
		deleted(entity(nodeFP(t, "n-1")), at, store.BasisObserved),
	)
}

// An address another holder of this translator still claims: the new claim is
// written all the same and noted, and nothing is written about the first holder,
// whose claim ends only by its own records. A pod that is not on its node's
// network and lists the node's address is noted the same way.
func TestAddressClaimedTwiceIsWrittenAndNoted(t *testing.T) {
	tr := newTranslator(t, 0)
	a, b, c := podFP(t, "pa"), podFP(t, "pb"), podFP(t, "pc")
	spec := func(uid string) podSpec {
		return podSpec{UID: uid, Name: uid, Created: t0, Phase: "Running", IPs: []string{"10.1.0.5"}}
	}
	note := map[string]int{k8sobjects.NoteAddressClaimedTwice: 1}
	r := translate(t, tr, pullRec(t0.Add(time.Minute), spec("pa").object()))
	if len(r.Notes) != 0 {
		t.Errorf("the first claim noted %v", r.Notes)
	}
	r = translate(t, tr, pullRec(t0.Add(2*time.Minute), spec("pb").object()))
	checkRecs(t, "second holder", r.Records, recs(
		wantRec{subj: entity(b), kind: lifecycle.Observe, at: t0, basis: store.BasisObjectField, through: t0.Add(2 * time.Minute), payload: anyPayload},
		claimed(t, b, "10.1.0.5", t0.Add(2*time.Minute), 0, "{}"),
	)...)
	if got := notes(r); !mapsEqual(got, note) {
		t.Errorf("second holder: notes %v, want %v", got, note)
	}
	// The first holder's own DELETED ends its claim; the second holds on.
	r = translate(t, tr, watchRec(t0.Add(4*time.Minute), "DELETED", spec("pa").object()))
	checkRecs(t, "first holder deleted", r.Records,
		deleted(claimOf(t, a, "10.1.0.5"), t0.Add(4*time.Minute), store.BasisObserved),
		deleted(entity(a), t0.Add(4*time.Minute), store.BasisObserved),
	)
	r = translate(t, tr, pullRec(t0.Add(5*time.Minute), spec("pc").object()))
	if got := notes(r); !mapsEqual(got, note) {
		t.Errorf("a third holder while the second holds: notes %v, want %v", got, note)
	}
	if !slices.ContainsFunc(r.Records, func(rec store.Record) bool { return rec.Subject == claimOf(t, c, "10.1.0.5") }) {
		t.Errorf("the third holder's claim is not written:\n%s", describe(r.Records))
	}

	// With a TTL, a holder whose records stopped (its DELETED lost) holds the
	// address only until its claim lapses: a later holder is not noted.
	const ttl = 15 * time.Minute
	tr = newTranslator(t, ttl)
	translate(t, tr, pullRec(t0.Add(time.Minute), spec("pa").object()))
	r = translate(t, tr, pullRec(t0.Add(time.Minute+ttl-time.Second), spec("pb").object()))
	if got := notes(r); !mapsEqual(got, note) {
		t.Errorf("a holder before the first claim lapses: notes %v, want %v", got, note)
	}
	r = translate(t, tr, pullRec(t0.Add(time.Minute+ttl), spec("pc").object()))
	if got := notes(r); !mapsEqual(got, note) {
		t.Errorf("a holder while the second claims: notes %v, want %v", got, note)
	}
	tr = newTranslator(t, ttl)
	translate(t, tr, pullRec(t0.Add(time.Minute), spec("pa").object()))
	r = translate(t, tr, pullRec(t0.Add(time.Minute+ttl), spec("pb").object()))
	if len(r.Notes) != 0 {
		t.Errorf("a holder once the first claim lapsed: notes %v, want none", r.Notes)
	}

	// A node's address listed by a pod that does not use the node's network.
	tr = newTranslator(t, 0)
	translate(t, tr, pullRec(t0.Add(time.Minute), nodeWithAddrs("n-1", "n", t0, "1", [2]string{"InternalIP", "10.0.0.2"})))
	r = translate(t, tr, pullRec(t0.Add(time.Minute), podSpec{UID: "pd", Name: "d", Created: t0, Phase: "Running", IPs: []string{"10.0.0.2"}}.object()))
	if got := notes(r); !mapsEqual(got, note) {
		t.Errorf("a pod listing its node's address: notes %v, want %v", got, note)
	}
}

// After a restart the translator has no state. Its first list backdates the pod
// to its creation and marks it for resume; the claim is asserted at the observed
// time, unmarked, and resume leaves it as it is. In the store the claim is
// continuous across the restart.
func TestRestartAssertsNoBackdatedClaim(t *testing.T) {
	const ttl = 15 * time.Minute
	ctx := context.Background()
	s, err := memstore.Open(memstore.Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	seq := uint64(0)
	write := func(rs []store.Record) {
		t.Helper()
		for i := range rs {
			seq++
			rs[i].Seq = seq
		}
		if err := s.Write(ctx, rs); err != nil {
			t.Fatal(err)
		}
	}
	pod := podFP(t, "p1")
	p := podSpec{UID: "p1", Name: "p", Created: t0, Phase: "Running", IPs: []string{"10.1.0.5"}}
	before := newTranslator(t, ttl)
	write(translate(t, before, pullRec(t0.Add(time.Hour), p.object())).Records)

	after := newTranslator(t, ttl)
	seen := t0.Add(time.Hour + 5*time.Minute)
	r := translate(t, after, pullRec(seen, p.object()))
	checkRecs(t, "first list after the restart", r.Records, recs(
		wantRec{subj: entity(pod), kind: lifecycle.Observe, at: t0, basis: store.BasisObjectField, ttl: ttl, through: seen, payload: anyPayload},
		claimed(t, pod, "10.1.0.5", seen, ttl, "{}"),
	)...)
	if got := markedSubjects(t, r); !sameSubjects(got, []store.Subject{entity(pod)}) {
		t.Errorf("marked %v, want the pod alone", got)
	}

	lk, err := s.NewLookup()
	if err != nil {
		t.Fatal(err)
	}
	res := resume.Result{Records: slices.Clone(r.Records), Marks: r.Backdated}
	if _, err := resume.Apply(ctx, lk, func(catalog.Layer) time.Time { return time.Time{} }, &res, after, k8sobjects.Comparable); err != nil {
		t.Fatal(err)
	}
	if err := lk.Close(); err != nil {
		t.Fatal(err)
	}
	if !res.Records[0].EventTime.Equal(t0.Add(time.Hour)) {
		t.Errorf("the pod was resumed to %s, want the stored last instant", res.Records[0].EventTime)
	}
	checkRecs(t, "the claim after resume", res.Records[1:], claimed(t, pod, "10.1.0.5", seen, ttl, "{}")...)
	write(res.Records)

	ep := endpointFP(t, "10.1.0.5")
	for _, at := range []time.Time{t0.Add(time.Hour), t0.Add(time.Hour + 3*time.Minute), seen, seen.Add(ttl - time.Nanosecond)} {
		if got := claimants(t, s, ep, at); !slices.Equal(got, []identity.Fingerprint{pod}) {
			t.Errorf("at +%s the endpoint is claimed by %v, want the pod", at.Sub(t0), got)
		}
	}
	if got := claimants(t, s, ep, t0.Add(time.Hour-time.Nanosecond)); len(got) != 0 {
		t.Errorf("the claim begins before it was first seen: %v", got)
	}
}

// The records a steady pod costs a day, at a pull every five minutes through the
// shipped coalescer, as predicted before any run: each subject a pod owns keeps
// 144 records a day, and a pod with C containers and A addresses owns 2 + 2C + 2A
// subjects (itself and its scheduled_on edge, each container and its part_of edge,
// each endpoint and its claim). A host-network pod claims nothing. The node keeps
// 144 for itself and 288 for each address it claims.
func TestRecordsPerPodDayArePredicted(t *testing.T) {
	const (
		pull   = 5 * time.Minute
		perDay = 144 // records of one subject a day: one each two pulls
	)
	type podCase struct {
		uid      string
		c        int
		ips      []string
		hostNet  bool
		predicts int
	}
	cases := []podCase{
		{"c1", 1, []string{"10.1.0.1"}, false, (4 + 2*1) * perDay},
		{"c2", 2, []string{"10.1.0.2"}, false, (4 + 2*2) * perDay},
		{"c3", 3, []string{"10.1.0.3"}, false, (4 + 2*3) * perDay},
		{"dual", 1, []string{"10.1.0.4", "fd00::4"}, false, (6 + 2*1) * perDay},
		{"host", 1, []string{"10.0.0.2"}, true, (2 + 2*1) * perDay},
	}
	const nodePredicts = perDay + 2*perDay

	tr, err := k8sobjects.New(k8sobjects.Config{Producer: testProducer, Resolver: resolver(), PullInterval: pull})
	if err != nil {
		t.Fatal(err)
	}
	co, err := coalesce.New(coalesce.Config{ExtendTTLFraction: 0.5, RunAgeFromTTL: true})
	if err != nil {
		t.Fatal(err)
	}
	created := t0.Add(-time.Hour)
	nodeObj := nodeWithAddrs("n-1", node, created.Add(-time.Hour), "1", [2]string{"InternalIP", "10.0.0.2"}, [2]string{"Hostname", node})
	owner := map[identity.Fingerprint]string{nodeFP(t, "n-1"): "node", endpointFP(t, "10.0.0.2"): "node"}
	var pods []map[string]any
	for _, pc := range cases {
		p := podSpec{
			UID: pc.uid, Name: pc.uid, Node: node, Created: created, Phase: "Running", IPs: pc.ips, HostNetwork: pc.hostNet,
			Conds: []map[string]any{cond("PodScheduled", "True", created)},
		}
		for i := range pc.c {
			id := fmt.Sprintf("%s-%d", pc.uid, i)
			p.Containers = append(p.Containers, map[string]any{
				"name": id, "containerID": "containerd://" + id, "restartCount": 0,
				"state": map[string]any{"running": map[string]any{"startedAt": ts(created)}},
			})
			owner[containerFP(t, id)] = pc.uid
		}
		owner[podFP(t, pc.uid)] = pc.uid
		if !pc.hostNet {
			for _, ip := range pc.ips {
				owner[endpointFP(t, ip)] = pc.uid
			}
		}
		pods = append(pods, p.object())
	}

	// A day of pulls after an hour to settle, and an hour after it so that no run is
	// cut at the window's end.
	from, to := t0.Add(time.Hour), t0.Add(25*time.Hour)
	got := map[string]int{}
	for at := t0; at.Before(to.Add(time.Hour)); at = at.Add(pull) {
		var out []store.Record
		for _, o := range append([]map[string]any{nodeObj}, pods...) {
			for _, r := range translate(t, tr, pullRec(at, o)).Records {
				out = co.Add(r, out)
			}
		}
		for _, r := range out {
			if r.EventTime.Before(from) || !r.EventTime.Before(to) {
				continue
			}
			// A record is the pod's or the node's that owns its subject: an edge is
			// owned by its from end, except part_of, owned by its pod.
			f := r.Subject.A
			if r.Subject.Kind == store.SubjectEdge && r.Subject.Relation == catalog.PartOf {
				f = r.Subject.B
			}
			o, ok := owner[f]
			if !ok {
				t.Fatalf("a record of no pod or node: %s", describe([]store.Record{r}))
			}
			got[o]++
		}
	}
	for _, pc := range cases {
		if got[pc.uid] != pc.predicts {
			t.Errorf("pod %s (C=%d, %d addresses, host network %v): %d records a day, predicted %d (ratio %.3f)",
				pc.uid, pc.c, len(pc.ips), pc.hostNet, got[pc.uid], pc.predicts, float64(got[pc.uid])/float64(pc.predicts))
		}
	}
	if got["node"] != nodePredicts {
		t.Errorf("node: %d records a day, predicted %d", got["node"], nodePredicts)
	}
}
