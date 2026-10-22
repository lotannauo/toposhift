package mcp

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/lotannauo/toposhift/internal/catalog"
	"github.com/lotannauo/toposhift/internal/identity"
	"github.com/lotannauo/toposhift/internal/query/coresidency"
	"github.com/lotannauo/toposhift/internal/query/decay"
	"github.com/lotannauo/toposhift/internal/query/extract"
	"github.com/lotannauo/toposhift/internal/query/interval"
	"github.com/lotannauo/toposhift/internal/store"
	"github.com/lotannauo/toposhift/internal/store/memstore"
)

// svcPair finds the pair of a member and a peer by one mechanism, in the pairs or
// in the suppressed list.
func svcPair(list []ServicePair, member, peer identity.Fingerprint, mechanism string) (ServicePair, bool) {
	for _, p := range list {
		if p.Member.FP == member && p.Peer.FP == peer && p.Mechanism == mechanism {
			return p, true
		}
	}
	return ServicePair{}, false
}

// endsOf lists the legs and reasons that end a span of a pair, as "leg:reason".
func endsOf(sp PairSpan) []string {
	var out []string
	for _, e := range sp.Ends {
		out = append(out, e.Leg+":"+str(e.EndReason))
	}
	return out
}

func spanTimes(sp Span) string { return sp.Start[11:16] + "-" + sp.End[11:16] }

func serviceContext(t testing.TB, s *Source, in ServiceContextInput) ServiceContextOutput {
	t.Helper()
	out, err := s.ServiceContext(ctx, in)
	if err != nil {
		t.Fatal(err)
	}
	return out
}

// The facts of the service world, written down before the first run, are in the
// answer for checkout over the window.
func TestServiceContextFacts(t *testing.T) {
	w := newSvcWorld(t, newSvcMem(t))
	s := w.source(t)
	svc := w.svc
	out := serviceContext(t, s, ServiceContextInput{Service: w.id("checkout"), From: w10, To: w1030})

	if out.Service.FP != svc["checkout"] || str(out.Service.Name) != "checkout" || !out.Derived || out.Token != tokenString(w.st.LastSeq()) {
		t.Errorf("service %+v, derived %t, token %s", out.Service, out.Derived, out.Token)
	}
	if want := (Policy{Interval: interval.PolicyVersion, CoResidency: coresidency.PolicyVersion, Extract: extract.PolicyVersion, Rank: out.Policy.Rank, Names: NameTableVersion, Withhold: WithholdVersion, Decay: decay.PolicyVersion}); out.Policy != want {
		t.Errorf("policy %+v", out.Policy)
	}

	// The hop: frontend and legacy-pay call checkout, checkout calls payments, and
	// cart is both.
	hop := func(list []HopService) map[identity.Fingerprint]HopService {
		m := map[identity.Fingerprint]HopService{}
		for _, h := range list {
			m[h.Service.FP] = h
		}
		return m
	}
	callers, callees := hop(out.Callers), hop(out.Callees)
	if len(out.Callers) != 3 || len(out.Callees) != 2 {
		t.Fatalf("%d callers and %d callees", len(out.Callers), len(out.Callees))
	}
	for _, name := range []string{"frontend", "legacy-pay", "cart"} {
		h, ok := callers[svc[name]]
		if !ok {
			t.Errorf("%s is not a caller", name)
			continue
		}
		if h.Edge.From.FP != svc[name] || h.Edge.To.FP != svc["checkout"] || h.Edge.Relation != "depends_on" || h.Subject != SubjectAsserted || h.Basis != BasisDerived || h.Rule != RuleExistence || h.Instances != 1 {
			t.Errorf("caller %s: %+v", name, h)
		}
	}
	for _, name := range []string{"payments", "cart"} {
		h, ok := callees[svc[name]]
		if !ok {
			t.Errorf("%s is not a callee", name)
			continue
		}
		if h.Edge.From.FP != svc["checkout"] || h.Edge.To.FP != svc[name] {
			t.Errorf("callee %s: %+v", name, h.Edge)
		}
	}
	// legacy-pay was last sighted at 10:10, so its edge ends at 10:15 by expiry.
	if l := callers[svc["legacy-pay"]]; len(l.Spans) != 1 || spanTimes(l.Spans[0]) != "10:00-10:15" || str(l.Spans[0].EndReason) != "liveness_expiry" || l.Duration != "15m0s" {
		t.Errorf("legacy-pay: %+v", l)
	}
	if f := callers[svc["frontend"]]; len(f.Spans) != 1 || spanTimes(f.Spans[0]) != "10:00-10:30" || f.Spans[0].EndReason != nil || !f.Spans[0].BeganBeforeWindow {
		t.Errorf("frontend: %+v", f)
	}

	// Every sighting is a record a minute, the last at 10:29, with a TTL of five
	// minutes, so at the window's last instant, 10:29:59.999999999, a series seen
	// to the end has been silent for less than the extension interval, 2m30s, and
	// shows no silence. legacy-pay's edge ended at 10:15, before that instant, so it
	// has no recency.
	for _, h := range append(append([]HopService(nil), out.Callers...), out.Callees...) {
		name := str(h.Service.Name)
		if name == "legacy-pay" {
			if h.Recency != nil {
				t.Errorf("legacy-pay has the recency %+v, and its edge ended at 10:15", *h.Recency)
			}
			continue
		}
		want := Recency{
			Basis: BasisDerived, Subject: SubjectDerived, Rule: RuleRecency, Producer: string(graphProducer),
			LastSeen: "2026-01-01T10:29:00Z", SilentAtLeast: "0s", Confidence: 1,
		}
		if h.Recency == nil || *h.Recency != want {
			t.Errorf("%s: recency %+v, want %+v", name, h.Recency, want)
		}
	}

	// Pairs: envoy shares checkout-0's pod, redis a node for 10:05 to 10:25, search a
	// host, and the pod of weird a node.
	type want struct {
		member, peer string
		mechanism    string
		duration     string
		list         string
	}
	for _, c := range []want{
		{"checkout", "envoy", "shared_pod", "30m0s", "pairs"},
		{"checkout", "redis", "shared_node", "20m0s", "pairs"},
		{"checkout", "search", "shared_host", "30m0s", "pairs"},
		{"checkout", weirdName, "shared_node", "30m0s", "pairs"},
		{"cart", "envoy", "shared_node", "25m0s", "pairs"},
		{"cart", "redis", "shared_node", "20m0s", "pairs"},
		{"cart", "search", "shared_host", "25m0s", "pairs"},
		{"checkout", "node-exporter", "shared_node", "30m0s", "suppressed"},
		{"checkout", "node-exporter", "shared_host", "30m0s", "suppressed"},
		{"frontend", "node-exporter", "shared_node", "30m0s", "suppressed"},
		{"legacy-pay", "node-exporter", "shared_node", "30m0s", "suppressed"},
		{"payments", "node-exporter", "shared_node", "30m0s", "suppressed"},
		{"cart", "node-exporter", "shared_node", "25m0s", "suppressed"},
		{"cart", "node-exporter", "shared_host", "25m0s", "suppressed"},
	} {
		list := out.Pairs
		if c.list == "suppressed" {
			list = out.Suppressed
		}
		p, ok := svcPair(list, svc[c.member], svc[c.peer], c.mechanism)
		if !ok {
			t.Errorf("no %s pair of %s and %s in %s", c.mechanism, c.member, c.peer, c.list)
			continue
		}
		if p.Duration != c.duration || p.Basis != BasisDerived || p.Subject != SubjectDerived || p.Rule != "pair."+c.mechanism {
			t.Errorf("%s %s %s: %+v", c.member, c.peer, c.mechanism, p)
		}
	}
	// The pair of two members is reported once, with the first in id order as the
	// member, and ends where cart's instance expired: the legs named are cart's.
	pairOf := func(a, b string) []ServicePair {
		var out2 []ServicePair
		for _, p := range out.Pairs {
			if (p.Member.FP == svc[a] && p.Peer.FP == svc[b]) || (p.Member.FP == svc[b] && p.Peer.FP == svc[a]) {
				out2 = append(out2, p)
			}
		}
		return out2
	}
	both := pairOf("checkout", "cart")
	if len(both) != 1 {
		t.Fatalf("%d pairs of checkout and cart: %+v", len(both), both)
	}
	cp := both[0]
	wantMember, wantPeer := svc["cart"], svc["checkout"]
	if store.CompareFingerprints(svc["checkout"], svc["cart"]) < 0 {
		wantMember, wantPeer = svc["checkout"], svc["cart"]
	}
	if cp.Member.FP != wantMember || cp.Peer.FP != wantPeer || cp.Mechanism != "shared_node" || cp.Paths != 2 || cp.PeerRole == "none" {
		t.Fatalf("checkout and cart: %+v", cp)
	}
	cartLegs := []string{"member_instance_of:liveness_expiry", "member_part_of:liveness_expiry"}
	if wantMember == svc["checkout"] {
		cartLegs = []string{"peer_part_of:liveness_expiry", "peer_instance_of:liveness_expiry"}
	}
	if len(cp.Overlaps) != 1 || cp.Overlaps[0].End[11:16] != "10:25" || str(cp.Overlaps[0].EndReason) != "reach_ended" || !slices.Equal(endsOf(cp.Overlaps[0]), cartLegs) {
		t.Errorf("the pair of checkout and cart: %+v, want the legs %v", cp.Overlaps, cartLegs)
	}
	// frontend and legacy-pay share node-4, with no host: a pair of two members, once,
	// found from the pod of each.
	if fl := pairOf("frontend", "legacy-pay"); len(fl) != 1 || fl[0].Mechanism != "shared_node" || fl[0].Paths != 2 || fl[0].Duration != "30m0s" || len(fl[0].Overlaps) != 1 || len(fl[0].Overlaps[0].Ends) != 0 || fl[0].PeerRole == "none" {
		t.Errorf("frontend and legacy-pay: %+v", fl)
	}
	// redis ends at 10:25 where its pod's scheduling was deleted.
	rp, _ := svcPair(out.Pairs, svc["checkout"], svc["redis"], "shared_node")
	if len(rp.Overlaps) != 1 || rp.Overlaps[0].Start[11:16] != "10:05" || rp.Overlaps[0].BeganBeforeWindow || !slices.Equal(endsOf(rp.Overlaps[0]), []string{"placement:producer"}) {
		t.Errorf("checkout and redis: %+v", rp.Overlaps)
	}

	// Coverage of the frontier of five nodes.
	for peer, cov := range map[string]Coverage{"node-exporter": {5, 5}, "redis": {1, 5}, "envoy": {1, 5}, "search": {0, 5}} {
		mech := "shared_node"
		if peer == "search" {
			mech = "shared_host"
		}
		list := out.Pairs
		if peer == "node-exporter" {
			list = out.Suppressed
		}
		p, ok := svcPair(list, svc["checkout"], svc[peer], mech)
		if peer == "envoy" {
			p, ok = svcPair(list, svc["cart"], svc[peer], mech)
		}
		if !ok || p.Coverage != cov {
			t.Errorf("%s: coverage %+v (found %t), want %+v", peer, p.Coverage, ok, cov)
		}
		if (peer == "node-exporter") != (p.SuppressedReason == "ubiquitous") {
			t.Errorf("%s: suppressed reason %q", peer, p.SuppressedReason)
		}
	}
	if !out.Ubiquity.Judged || out.Ubiquity.FrontierNodes != 5 {
		t.Errorf("ubiquity %+v", out.Ubiquity)
	}

	// The instance of redis that is part of two pods at once is refused, not guessed,
	// and so is node-4's host, which the two seed pods on it each found missing: one row.
	if len(out.Unattributed) != 2 {
		t.Fatalf("unattributed %+v", out.Unattributed)
	}
	ua, nh := out.Unattributed[0], out.Unattributed[1]
	if ua.Reason != "several_pods" || ua.Rule != "unattributed.several_pods" || ua.Relation != "part_of" || ua.TargetType != "k8s.pod" ||
		ua.Entity.FP != w.inst["redis-2"] || len(ua.Candidates) != 2 || len(ua.Spans) != 1 || spanTimes(ua.Spans[0]) != "10:12-10:17" {
		t.Errorf("unattributed %+v", ua)
	}
	if nh.Reason != "no_host" || nh.Rule != "unattributed.no_host" || nh.Relation != "" || nh.TargetType != "" ||
		nh.Entity.FP != w.node["node-4"] || len(nh.Spans) != 1 || spanTimes(nh.Spans[0]) != "10:00-10:30" {
		t.Errorf("unattributed %+v", nh)
	}
	// Pods with no instance mapped singly are listed: bare-0 throughout, redis-x
	// while its only instance is part of two pods, and cart-0 once cart's
	// instance expired.
	unmapped := map[identity.Fingerprint]string{}
	for _, u := range out.Unmapped {
		if u.Rule != RuleUnmapped || u.Basis != BasisDerived || len(u.Spans) != 1 {
			t.Errorf("unmapped %+v", u)
			continue
		}
		unmapped[u.Pod.FP] = spanTimes(u.Spans[0])
	}
	if wantUnmapped := map[identity.Fingerprint]string{w.pod["bare-0"]: "10:00-10:30", w.pod["redis-x"]: "10:12-10:17", w.pod["cart-0"]: "10:25-10:30"}; len(unmapped) != 3 {
		t.Errorf("unmapped %v, want %v", unmapped, wantUnmapped)
	} else {
		for fp, span := range wantUnmapped {
			if unmapped[fp] != span {
				t.Errorf("unmapped %v, want %v", unmapped, wantUnmapped)
			}
		}
	}

	// The counts are exact, nothing is cut, and the sightings with a TTL are named.
	if want := (ContextCounts{Callers: 3, Callees: 2, Pairs: 9, PairsShown: 9, Suppressed: 7, Unattributed: 2, Unmapped: 3, SeedPods: 7}); out.Counts != want {
		t.Errorf("counts %+v, want %+v", out.Counts, want)
	}
	if out.Truncation != (ContextCut{}) {
		t.Errorf("truncation %+v", out.Truncation)
	}
	for _, note := range []string{
		"A dependency, an instance and its pod mapping are sightings with a TTL: a span that ends by liveness_expiry ends one TTL after the last sighting, so a service that stopped calling, or an instance that stopped reporting, is shown until then.",
		"Suppressed pairs are with a service that shares a node with the seed pods on most of the frontier, such as a DaemonSet's; they are shown, never dropped.",
	} {
		if !contains(out.Notes, note) {
			t.Errorf("no note %q in %q", note, out.Notes)
		}
	}

	// A service whose name holds U+202E has a null name and the fixed note, and the
	// answer is whole.
	weird, ok := svcPair(out.Pairs, svc["checkout"], svc[weirdName], "shared_node")
	if !ok || weird.Peer.Name != nil || weird.Peer.NameNote != nameWithheld {
		t.Errorf("the service with a bidi override in its name: %+v", weird.Peer)
	}
	b, err := json.Marshal(out)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(b), "\u202e") || strings.Contains(string(b), `\u202e`) {
		t.Error("the answer holds the override")
	}
	// Every pod and node of the world has an endpoint, and none is printed.
	if strings.Contains(string(b), "topo.endpoint") {
		t.Error("the answer holds an endpoint id")
	}

	// Node only reads no host: no pair is shared_host.
	nodeOnly := serviceContext(t, s, ServiceContextInput{Service: w.id("checkout"), From: w10, To: w1030, NodeOnly: true})
	for _, list := range [][]ServicePair{nodeOnly.Pairs, nodeOnly.Suppressed} {
		for _, p := range list {
			if p.Mechanism == "shared_host" {
				t.Errorf("node only: %+v", p)
			}
		}
	}
	if len(nodeOnly.Pairs) != 7 || len(nodeOnly.Suppressed) != 5 || !nodeOnly.NodeOnly {
		t.Errorf("node only: %d pairs and %d suppressed", len(nodeOnly.Pairs), len(nodeOnly.Suppressed))
	}
}

// The golden answer for checkout over the window, on the service world. It is its
// own test: goldenAnswers writes the main world's answers.
func TestServiceContextGolden(t *testing.T) {
	w := newSvcWorld(t, newSvcMem(t))
	out := serviceContext(t, w.source(t), ServiceContextInput{Service: w.id("checkout"), From: w10, To: w1030})
	got, err := json.MarshalIndent(out, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	got = append(got, '\n')
	path := filepath.Join("testdata", "service_context.golden.json")
	if *update {
		if err := os.WriteFile(path, got, 0o644); err != nil {
			t.Fatal(err)
		}
		return
	}
	want, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("%v (run with -update to write it)", err)
	}
	if !bytes.Equal(got, want) {
		t.Errorf("the answer differs from %s; run with -update if the shape changed on purpose.\ngot:\n%s", path, got)
	}
}

// Arguments are refused with their own sentence, in the order of the tool and
// before any read.
func TestServiceContextRefusals(t *testing.T) {
	w := newSvcWorld(t, newSvcMem(t))
	spy := &spyStore{Store: w.st}
	s, err := NewSource(Config{Store: spy, Rank: rankObjs, RangeReads: true})
	if err != nil {
		t.Fatal(err)
	}
	good := w.id("checkout")
	endpoint := endpointFP(t, "k8s/c1", "10.9.9.9")
	for _, tc := range []struct {
		name string
		in   ServiceContextInput
		want string
	}{
		{"service missing", ServiceContextInput{From: w10, To: w1030}, `The argument "service" is required.`},
		{"service malformed", ServiceContextInput{Service: "checkout", From: w10, To: w1030}, `The argument "service" must be an entity id such as host:0123456789abcdef0123456789abcdef; got "checkout".`},
		{"service of an unknown type", ServiceContextInput{Service: "nothing:" + strings.Repeat("a", 32), From: w10, To: w1030}, `The argument "service" names the entity type "nothing", which is not in the catalog.`},
		{"a pod", ServiceContextInput{Service: w.pod["checkout-0"].String(), From: w10, To: w1030}, `The argument "service" must be a service id; got a k8s.pod id.`},
		{"a host", ServiceContextInput{Service: w.host["host-a"].String(), From: w10, To: w1030}, `The argument "service" must be a service id; got a host id.`},
		{"an endpoint", ServiceContextInput{Service: endpoint.String(), From: w10, To: w1030}, `The argument "service" must be a service id; got a topo.endpoint id.`},
		{"a service instance", ServiceContextInput{Service: w.inst["checkout-0"].String(), From: w10, To: w1030}, `The argument "service" must be a service id; got a service.instance id.`},
		{"the type is checked before the window", ServiceContextInput{Service: w.pod["checkout-0"].String()}, `The argument "service" must be a service id; got a k8s.pod id.`},
		{"from missing", ServiceContextInput{Service: good, To: w1030}, `The argument "from" is required.`},
		{"to missing", ServiceContextInput{Service: good, From: w10}, `The argument "to" is required.`},
		{"time without a zone", ServiceContextInput{Service: good, From: "2026-01-01T10:00:00", To: w1030}, `The argument "from" must be an RFC 3339 time with a zone`},
		{"from after to", ServiceContextInput{Service: good, From: w1030, To: w10}, `The argument "from" (2026-01-01T10:30:00Z) must be before "to" (2026-01-01T10:00:00Z).`},
		{"empty window", ServiceContextInput{Service: good, From: w10, To: w10}, `must be before "to"`},
		{"limit too big", ServiceContextInput{Service: good, From: w10, To: w1030, Limit: 501}, `The argument "limit" must be between 1 and 500; got 501.`},
		{"limit negative", ServiceContextInput{Service: good, From: w10, To: w1030, Limit: -1}, `The argument "limit" must be between 1 and 500; got -1.`},
		{"asof", ServiceContextInput{Service: good, From: w10, To: w1030, AsOf: "abc"}, `The argument "asof" must be a token as a decimal number`},
	} {
		_, err := s.ServiceContext(ctx, tc.in)
		var te *Error
		if !errors.As(err, &te) || !strings.Contains(te.Message, tc.want) || !errors.Is(err, store.ErrInvalid) {
			t.Errorf("%s: %v, want %q wrapping ErrInvalid", tc.name, err, tc.want)
		}
	}
	if spy.reads != 0 {
		t.Errorf("%d reads for refused arguments", spy.reads)
	}

	// A limit of zero is the default.
	out, err := s.ServiceContext(ctx, ServiceContextInput{Service: good, From: w10, To: w1030, Limit: 0})
	if err != nil || out.Counts.PairsShown != 9 {
		t.Errorf("a limit of 0: %v, %d pairs shown", err, out.Counts.PairsShown)
	}

	// A store without range reads answers that, whatever the arguments.
	noRange, err := NewSource(Config{Store: spy, Rank: rankObjs})
	if err != nil {
		t.Fatal(err)
	}
	reads := spy.reads
	_, err = noRange.ServiceContext(ctx, ServiceContextInput{Service: good, From: w10, To: w1030})
	var te *Error
	if !errors.As(err, &te) || te.Message != "Range answers are not available on this store." || !errors.Is(err, ErrNoRangeReads) {
		t.Errorf("without range reads: %v", err)
	}
	if spy.reads != reads {
		t.Error("a store without range reads was read")
	}
}

// A service that has no record gives a whole, empty answer that says so.
func TestServiceContextOfAnUnknownService(t *testing.T) {
	w := newSvcWorld(t, newSvcMem(t))
	s := w.source(t)
	unknown := fpOf(t, catalog.Service, catalog.ServiceName, "no-such-service")
	out := serviceContext(t, s, ServiceContextInput{Service: unknown.String(), From: w10, To: w1030})
	if len(out.Callers)+len(out.Callees)+len(out.Pairs)+len(out.Suppressed)+len(out.Unattributed)+len(out.Unmapped)+len(out.Quarantined) != 0 {
		t.Errorf("an answer for a service with no record: %+v", out)
	}
	if out.Service.Name != nil || out.Service.NameNote != notPresent || out.Truncation.Incomplete {
		t.Errorf("service %+v, truncation %+v", out.Service, out.Truncation)
	}
	if !contains(out.Notes, "No instance of the service was read in the window, so no pod of it is known and it has no pair.") {
		t.Errorf("notes %q", out.Notes)
	}
}

// The answer is the extraction's: the same members, pairs and counts, and exactly
// the extraction's store reads plus one read of the names, all at the token of the
// answer.
func TestServiceContextIsTheExtraction(t *testing.T) {
	w := newSvcWorld(t, newSvcMem(t))
	names := []string{"checkout", "frontend", "legacy-pay", "payments", "cart", "envoy", "redis", "search", "node-exporter", weirdName}
	windows := []struct{ from, to time.Time }{{hm(10, 0), hm(10, 30)}, {hm(10, 10), hm(10, 11)}, {hm(9, 0), hm(11, 0)}}
	answers, extraReads := 0, 0
	for _, name := range names {
		for _, win := range windows {
			for _, nodeOnly := range []bool{false, true} {
				label := fmt.Sprintf("%q %s-%s node only %t", name, win.from.Format("15:04"), win.to.Format("15:04"), nodeOnly)
				seed := w.svc[name]
				token := w.st.LastSeq()
				tool := &spyStore{Store: w.st}
				s, err := NewSource(Config{Store: tool, Rank: rankObjs, RangeReads: true})
				if err != nil {
					t.Fatal(err)
				}
				out, err := s.ServiceContext(ctx, ServiceContextInput{
					Service: seed.String(), From: fmtTime(win.from), To: fmtTime(win.to), NodeOnly: nodeOnly, Limit: 500,
				})
				if err != nil {
					t.Fatalf("%s: %v", label, err)
				}

				direct := &spyStore{Store: w.st}
				window := interval.Window{From: win.from, To: win.to, AsOf: token}
				res, err := extract.NewExtractor(direct).Extract(ctx, extract.Exploratory{Service: seed}, window, extract.Options{NodeOnly: nodeOnly})
				if err != nil {
					t.Fatalf("%s: %v", label, err)
				}

				// (a) The same members, pairs and counts.
				pairSet := func(ps []extract.Pair) []string {
					var out []string
					for _, p := range ps {
						out = append(out, fmt.Sprintf("%s|%s|%s|%s", p.Member, p.Peer, p.Mechanism, p.Duration))
					}
					return out
				}
				answerSet := func(ps []ServicePair) []string {
					var out []string
					for _, p := range ps {
						dur, err := time.ParseDuration(p.Duration)
						if err != nil {
							t.Fatal(err)
						}
						out = append(out, fmt.Sprintf("%s|%s|%s|%s", p.Member.FP, p.Peer.FP, p.Mechanism, dur))
					}
					return out
				}
				if got, want := answerSet(out.Pairs), pairSet(res.Pairs); !slices.Equal(got, want) {
					t.Errorf("%s: pairs\n got %v\nwant %v", label, got, want)
				}
				if got, want := answerSet(out.Suppressed), pairSet(res.Suppressed); !slices.Equal(got, want) {
					t.Errorf("%s: suppressed\n got %v\nwant %v", label, got, want)
				}
				var callers, callees, gotCallers, gotCallees []identity.Fingerprint
				for _, m := range res.Members {
					if m.Role&extract.RoleCaller != 0 {
						callers = append(callers, m.Service)
					}
					if m.Role&extract.RoleCallee != 0 {
						callees = append(callees, m.Service)
					}
				}
				for _, h := range out.Callers {
					gotCallers = append(gotCallers, h.Service.FP)
				}
				for _, h := range out.Callees {
					gotCallees = append(gotCallees, h.Service.FP)
				}
				if !slices.Equal(gotCallers, callers) || !slices.Equal(gotCallees, callees) {
					t.Errorf("%s: callers %v and callees %v, want %v and %v", label, gotCallers, gotCallees, callers, callees)
				}
				c := out.Counts
				if c.Callers != len(callers) || c.Callees != len(callees) || c.Pairs != len(res.Pairs) || c.PairsShown != len(res.Pairs) ||
					c.Suppressed != len(res.Suppressed) || c.Unmapped != len(res.Unmapped) || c.Quarantined != len(res.Quarantined) ||
					c.SeedPods != len(res.PodResults) {
					t.Errorf("%s: counts %+v", label, c)
				}
				unattributed := 0
				for _, u := range res.Unattributed {
					if u.Mapping != 0 {
						unattributed++
					}
				}
				if len(out.Unattributed) < unattributed {
					t.Errorf("%s: %d unattributed mappings of %d", label, len(out.Unattributed), unattributed)
				}
				if out.Token != tokenString(res.Token) || out.Token != tokenString(token) {
					t.Errorf("%s: token %s, extraction %d", label, out.Token, res.Token)
				}

				// (b) The extraction's reads, and one read of the names, and no other.
				var fps []identity.Fingerprint
				seen := map[identity.Fingerprint]bool{}
				for _, r := range refsOf(&out) {
					if !seen[r.FP] {
						seen[r.FP] = true
						fps = append(fps, r.FP)
					}
				}
				slices.SortFunc(fps, store.CompareFingerprints)
				named := &spyStore{Store: w.st}
				if _, _, err := interval.NewReader(named, interval.Options{Describe: true, Rank: rankObjs}).Entities(ctx, fps, window); err != nil {
					t.Fatal(err)
				}
				// The recency of the hop: the same edges read again with the evidence, one
				// interval read for each side the hop has, at the token of the answer.
				evidence := &spyStore{Store: w.st}
				evReader := interval.NewReader(evidence, interval.Options{Evidence: true})
				byEdge := map[store.Subject]interval.Element{}
				for _, side := range []struct {
					dir  store.Direction
					list []HopService
				}{{store.Reverse, out.Callers}, {store.Forward, out.Callees}} {
					if len(side.list) == 0 {
						continue
					}
					res, err := evReader.Edges(ctx, seed, side.dir, window)
					if err != nil {
						t.Fatal(err)
					}
					for _, el := range res.Elements {
						byEdge[el.Subject] = el
					}
				}
				for _, h := range append(append([]HopService(nil), out.Callers...), out.Callees...) {
					sub := store.Subject{Kind: store.SubjectEdge, A: h.Edge.From.FP, B: h.Edge.To.FP, Relation: catalog.DependsOn}
					el, ok := byEdge[sub]
					if !ok {
						t.Errorf("%s: the edge of %s is not in the read with the evidence", label, h.Service.ID)
						continue
					}
					// The edges are the extraction's, subject and pieces.
					if got, want := h.Spans, pieceSpans(el.Pieces, win.to); !slices.EqualFunc(got, want, func(a, b Span) bool {
						return a.Start == b.Start && a.End == b.End && str(a.EndReason) == str(b.EndReason) && slices.Equal(a.AssertedBy, b.AssertedBy)
					}) {
						t.Errorf("%s: the spans of %s are %v, the read with the evidence has %v", label, h.Service.ID, got, want)
					}
					// The recency is the derivation of the evidence at the window's last instant.
					var want *Recency
					if r, ok := decay.Of(el.Last, win.to.Add(-time.Nanosecond), decay.DefaultPolicy()); ok {
						want = &Recency{
							Basis: BasisDerived, Subject: SubjectDerived, Rule: RuleRecency, Producer: string(r.Producer),
							LastSeen: fmtTime(r.LastSeen), SilentAtLeast: r.SilentAtLeast.String(), Confidence: math.Round(r.Confidence*1e4) / 1e4,
						}
					}
					if (h.Recency == nil) != (want == nil) || (want != nil && *h.Recency != *want) {
						t.Errorf("%s: the recency of %s is %+v, want %+v", label, h.Service.ID, h.Recency, want)
					}
				}
				// The reads are the extraction's, the names', and the evidence's, and no other.
				if tool.reads != direct.reads+named.reads+evidence.reads {
					t.Errorf("%s: the tool made %d reads; the extraction %d, the names %d and the evidence %d", label, tool.reads, direct.reads, named.reads, evidence.reads)
				}
				if (len(out.Callers) > 0 || len(out.Callees) > 0) != (evidence.reads > 0) {
					t.Errorf("%s: %d callers and %d callees, and the evidence made %d reads", label, len(out.Callers), len(out.Callees), evidence.reads)
				}
				// Every interval read begins with a call of LastSeq, and the tool asks for it
				// once more to pin the token: so the tool made the extraction's interval reads,
				// the names', and one for each side of the hop, no more.
				sides := 0
				for _, list := range [][]HopService{out.Callers, out.Callees} {
					if len(list) > 0 {
						sides++
					}
				}
				if evidence.lastSeqs != sides || tool.lastSeqs != direct.lastSeqs+named.lastSeqs+evidence.lastSeqs+1 {
					t.Errorf("%s: LastSeq called %d times by the tool, %d by the extraction, %d by the names and %d by the evidence; the hop has %d sides",
						label, tool.lastSeqs, direct.lastSeqs, named.lastSeqs, evidence.lastSeqs, sides)
				}
				extraReads += evidence.reads
				answers++

				// (c) Every read is at the token of the answer.
				for i, got := range tool.asOf {
					if got != token {
						t.Fatalf("%s: read %d used the token %d, want %d", label, i, got, token)
					}
				}
			}
		}
	}
	t.Logf("%d answers; the recency of their hops made %d store reads in all", answers, extraReads)
}

// numberedWorld writes small services with numbered ids, so that the caps of the
// extraction are reached without hashing.
type numberedWorld struct {
	t    testing.TB
	st   store.Store
	recs []store.Record
}

func newNumberedWorld(t testing.TB) *numberedWorld {
	return &numberedWorld{t: t, st: openMem(t, memstore.Options{Policy: servePolicy(rankObjs)})}
}

func (n *numberedWorld) service(fp identity.Fingerprint, name string) {
	n.recs = append(n.recs, observe(ent(fp), spanProducer, hm(9, 0), 0, fmt.Sprintf(`{"name":%q}`, name)))
}

func (n *numberedWorld) depends(from, to identity.Fingerprint) {
	n.recs = append(n.recs, observe(edge(from, to, catalog.DependsOn), graphProducer, hm(9, 0), 0, `{}`))
}

// instanceOn writes an instance of a service part of a new pod on a new node, and
// returns the pod.
func (n *numberedWorld) instanceOn(svc identity.Fingerprint, k int) identity.Fingerprint {
	pod, node, inst := numbered(catalog.K8sPod, k), numbered(catalog.K8sNode, k), numbered(catalog.ServiceInstance, k)
	n.node(node, k)
	n.podOn(pod, node, k)
	n.instance(svc, inst, pod)
	return pod
}

func (n *numberedWorld) node(node identity.Fingerprint, k int) {
	n.recs = append(n.recs, observe(ent(node), objs, hm(9, 0), 0, fmt.Sprintf(`{"name":"node-%d"}`, k)))
}

func (n *numberedWorld) podOn(pod, node identity.Fingerprint, k int) {
	n.recs = append(n.recs,
		observe(ent(pod), objs, hm(9, 0), 0, fmt.Sprintf(`{"name":"pod-%d"}`, k)),
		observe(edge(pod, node, catalog.ScheduledOn), objs, hm(9, 0), 0, `{}`),
	)
}

func (n *numberedWorld) instance(svc, inst, pod identity.Fingerprint) {
	n.recs = append(n.recs,
		observe(ent(inst), spanProducer, hm(9, 0), 0, `{}`),
		observe(edge(inst, svc, catalog.InstanceOf), spanProducer, hm(9, 0), 0, `{}`),
		observe(edge(inst, pod, catalog.PartOf), spanProducer, hm(9, 0), 0, `{}`),
	)
}

func (n *numberedWorld) commit() *Source {
	n.t.Helper()
	slicesSortByTime(n.recs)
	newFeed(n.t, n.st).write(n.recs...)
	s, err := NewSource(Config{Store: n.st, Rank: rankObjs, RangeReads: true})
	if err != nil {
		n.t.Fatal(err)
	}
	return s
}

func slicesSortByTime(rs []store.Record) {
	slices.SortStableFunc(rs, func(a, b store.Record) int { return a.EventTime.Compare(b.EventTime) })
}

// Every cap of the extraction gives its sentence and marks the answer incomplete;
// the display limit and the hard cap give exact counts and never do.
func TestServiceContextCaps(t *testing.T) {
	t.Run("the hop keeps 16 services", func(t *testing.T) {
		n := newNumberedWorld(t)
		seed := numbered(catalog.Service, 0)
		n.service(seed, "seed")
		for i := 1; i <= 20; i++ {
			c := numbered(catalog.Service, i)
			n.service(c, fmt.Sprintf("caller-%02d", i))
			n.depends(c, seed)
		}
		out := serviceContext(t, n.commit(), ServiceContextInput{Service: seed.String(), From: w10, To: w1030})
		if len(out.Callers) != 15 || out.Counts.Callers != 15 || out.Truncation.MembersDropped != 5 || !out.Truncation.Incomplete {
			t.Errorf("%d callers, counts %+v, truncation %+v", len(out.Callers), out.Counts, out.Truncation)
		}
		if want := "5 neighbours were not read: the hop keeps at most 16 services, the service included, longest dependency first."; !contains(out.Notes, want) {
			t.Errorf("notes %q", out.Notes)
		}
	})

	t.Run("the query searches 64 seed pods", func(t *testing.T) {
		n := newNumberedWorld(t)
		seed, callee := numbered(catalog.Service, 0), numbered(catalog.Service, 1)
		n.service(seed, "seed")
		n.service(callee, "callee")
		n.depends(seed, callee)
		for k := 1; k <= 70; k++ {
			svc := seed
			if k > 40 {
				svc = callee
			}
			n.instanceOn(svc, k)
		}
		out := serviceContext(t, n.commit(), ServiceContextInput{Service: seed.String(), From: w10, To: w1030, NodeOnly: true})
		if out.Counts.SeedPods != 64 || out.Truncation.SeedPodsDropped != 6 || out.Truncation.InstancesDropped != 0 || !out.Truncation.Incomplete {
			t.Errorf("counts %+v, truncation %+v", out.Counts, out.Truncation)
		}
		if want := "6 pods were not searched: the query searches at most 64 seed pods."; !contains(out.Notes, want) {
			t.Errorf("notes %q", out.Notes)
		}
		if !out.Ubiquity.Judged || out.Ubiquity.FrontierNodes != 64 {
			t.Errorf("ubiquity %+v", out.Ubiquity)
		}
	})

	t.Run("the display limit shortens a complete answer", func(t *testing.T) {
		w := newSvcWorld(t, newSvcMem(t))
		out := serviceContext(t, w.source(t), ServiceContextInput{Service: w.id("checkout"), From: w10, To: w1030, Limit: 1})
		if len(out.Pairs) != 1 || len(out.Suppressed) != 1 || len(out.Unmapped) != 1 || len(out.Unattributed) != 1 {
			t.Errorf("%d pairs, %d suppressed, %d unmapped, %d unattributed", len(out.Pairs), len(out.Suppressed), len(out.Unmapped), len(out.Unattributed))
		}
		if out.Counts.Pairs != 9 || out.Counts.PairsShown != 1 || out.Counts.Suppressed != 7 || out.Counts.Unmapped != 3 || out.Counts.Unattributed != 2 || out.Truncation.Incomplete {
			t.Errorf("counts %+v, truncation %+v", out.Counts, out.Truncation)
		}
		if len(out.Callers) != 3 || len(out.Callees) != 2 {
			t.Errorf("the limit cut the hop: %d callers and %d callees", len(out.Callers), len(out.Callees))
		}
		for _, want := range []string{
			"Showing 1 of 9 pairs, longest first.", "Showing 1 of 7 suppressed pairs, longest first.", "Showing 1 of 3 unmapped pods, in the package's order.",
			"Showing 1 of 2 unattributed mappings, in the package's order.",
		} {
			if !contains(out.Notes, want) {
				t.Errorf("no note %q in %q", want, out.Notes)
			}
		}
	})

	t.Run("the hard cap drops unmapped pods, then suppressed pairs, then pairs", func(t *testing.T) {
		n := newNumberedWorld(t)
		seed := numbered(catalog.Service, 0)
		ubiq := numbered(catalog.Service, 1)
		n.service(seed, "seed")
		n.service(ubiq, "ubiquitous")
		const nodes, peersPerNode = 12, 50
		next := 1000
		var nodeFPs []identity.Fingerprint
		for i := range nodes {
			node := numbered(catalog.K8sNode, i)
			nodeFPs = append(nodeFPs, node)
			n.node(node, i)
			seedPod := numbered(catalog.K8sPod, i)
			n.podOn(seedPod, node, i)
			n.instance(seed, numbered(catalog.ServiceInstance, i), seedPod)
			// One instance of the ubiquitous service on every node.
			p := numbered(catalog.K8sPod, 500+i)
			n.podOn(p, node, 500+i)
			n.instance(ubiq, numbered(catalog.ServiceInstance, 500+i), p)
		}
		var peers []identity.Fingerprint
		for i := range nodes * peersPerNode {
			name := padTo(fmt.Sprintf("peer-%03d-", i), 253)
			svc := fpOf(t, catalog.Service, catalog.ServiceName, name)
			peers = append(peers, svc)
			n.service(svc, name)
			p := numbered(catalog.K8sPod, next)
			n.podOn(p, nodeFPs[i%nodes], next)
			n.instance(svc, numbered(catalog.ServiceInstance, next), p)
			next++
		}
		for i := range 3 {
			n.podOn(numbered(catalog.K8sPod, 900+i), nodeFPs[0], 900+i)
		}
		s := n.commit()
		out := serviceContext(t, s, ServiceContextInput{Service: seed.String(), From: w10, To: w1030, Limit: 500})
		b, err := json.Marshal(out)
		if err != nil {
			t.Fatal(err)
		}
		if len(b) > maxAnswerBytes {
			t.Errorf("the answer is %d bytes", len(b))
		}
		if out.Counts.Pairs != len(peers) || out.Counts.Suppressed != 1 || out.Counts.Unmapped != 3 || out.Truncation.Incomplete {
			t.Errorf("counts %+v, truncation %+v", out.Counts, out.Truncation)
		}
		if len(out.Unmapped) != 0 || len(out.Suppressed) != 0 || len(out.Pairs) == 0 || len(out.Pairs) >= 500 || out.Counts.PairsShown != len(out.Pairs) {
			t.Errorf("%d unmapped, %d suppressed, %d pairs shown, counts %+v", len(out.Unmapped), len(out.Suppressed), len(out.Pairs), out.Counts)
		}
		for _, want := range []string{
			"The answer was cut at 256 KiB: the last 3 entries of unmapped pods were left out.",
			"The answer was cut at 256 KiB: the last 1 entries of suppressed pairs were left out.",
			fmt.Sprintf("The answer was cut at 256 KiB: the last %d entries of pairs were left out.", 500-len(out.Pairs)),
		} {
			if !contains(out.Notes, want) {
				t.Errorf("no note %q in %q", want, out.Notes)
			}
		}
	})

	t.Run("the hard cap drops the callers last", func(t *testing.T) {
		n := newNumberedWorld(t)
		seed, flapper, callee := numbered(catalog.Service, 0), numbered(catalog.Service, 1), numbered(catalog.Service, 2)
		n.service(seed, "seed")
		n.service(flapper, "flapper")
		n.service(callee, "callee")
		n.depends(seed, callee)
		from := hm(10, 0)
		const flaps = 2700
		for i := range flaps {
			t0 := from.Add(time.Duration(4*i) * time.Second)
			n.recs = append(n.recs,
				observe(edge(flapper, seed, catalog.DependsOn), graphProducer, t0, 0, `{}`),
				deleted(edge(flapper, seed, catalog.DependsOn), graphProducer, t0.Add(2*time.Second)),
			)
		}
		out := serviceContext(t, n.commit(), ServiceContextInput{Service: seed.String(), From: fmtTime(from), To: fmtTime(from.Add(3 * time.Hour))})
		b, err := json.Marshal(out)
		if err != nil {
			t.Fatal(err)
		}
		if len(b) > maxAnswerBytes {
			t.Errorf("the answer is %d bytes", len(b))
		}
		// The callees go before the callers, which go last; the counts stay exact.
		if len(out.Callers) != 0 || len(out.Callees) != 0 || out.Counts.Callers != 1 || out.Counts.Callees != 1 || out.Truncation.Incomplete {
			t.Errorf("%d callers and %d callees, counts %+v", len(out.Callers), len(out.Callees), out.Counts)
		}
		for _, want := range []string{
			"The answer was cut at 256 KiB: the last 1 entries of callees were left out.",
			"The answer was cut at 256 KiB: the last 1 entries of callers were left out.",
		} {
			if !contains(out.Notes, want) {
				t.Errorf("no note %q in %q", want, out.Notes)
			}
		}
	})
}

// stallStore blocks every read until the call's context ends, while it is set to.
type stallStore struct {
	store.Store
	block atomic.Bool
}

func (b *stallStore) wait(ctx context.Context) error {
	if b.block.Load() {
		<-ctx.Done()
		return ctx.Err()
	}
	return nil
}

func (b *stallStore) Window(ctx context.Context, fp identity.Fingerprint, d store.Direction, from, to time.Time, sc store.Scope) ([]store.Record, error) {
	if err := b.wait(ctx); err != nil {
		return nil, err
	}
	return b.Store.Window(ctx, fp, d, from, to, sc)
}

func (b *stallStore) EntityWindow(ctx context.Context, fp identity.Fingerprint, from, to time.Time, sc store.Scope) ([]store.Record, error) {
	if err := b.wait(ctx); err != nil {
		return nil, err
	}
	return b.Store.EntityWindow(ctx, fp, from, to, sc)
}

func (b *stallStore) WindowSince(ctx context.Context, fps []identity.Fingerprint, d store.Direction, from, to time.Time, sc store.Scope, o store.SinceOptions) ([]store.Since, error) {
	if err := b.wait(ctx); err != nil {
		return nil, err
	}
	return b.Store.WindowSince(ctx, fps, d, from, to, sc, o)
}

func (b *stallStore) Alive(ctx context.Context, fp identity.Fingerprint, t time.Time, sc store.Scope) (bool, error) {
	if err := b.wait(ctx); err != nil {
		return false, err
	}
	return b.Store.Alive(ctx, fp, t, sc)
}

// A call that outlasts its deadline gives the sentence of the deadline and no
// partial answer, and releases its slot.
func TestServiceContextDeadline(t *testing.T) {
	w := newSvcWorld(t, newSvcMem(t))
	blocked := &stallStore{Store: w.st}
	blocked.block.Store(true)
	s, err := NewSource(Config{Store: blocked, Rank: rankObjs, RangeReads: true, CallTimeout: 50 * time.Millisecond, MaxConcurrent: 1})
	if err != nil {
		t.Fatal(err)
	}
	in := ServiceContextInput{Service: w.id("checkout"), From: w10, To: w1030}
	out, err := s.ServiceContext(ctx, in)
	var te *Error
	if !errors.As(err, &te) || te.Message != "The call took longer than 50ms and was stopped." || !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("the blocked call: %v", err)
	}
	if out.Token != "" || len(out.Pairs) != 0 {
		t.Errorf("a partial answer: %+v", out)
	}

	// The slot is released. A call is refused for its arguments only after it has
	// taken a slot, and the source has one: were the first call's still held, this
	// one would wait for it and be refused as busy. It reads nothing, so the answer
	// does not depend on the speed of the machine.
	blocked.block.Store(false)
	_, err = s.ServiceContext(ctx, ServiceContextInput{Service: w.pod["checkout-0"].String(), From: w10, To: w1030})
	if !errors.As(err, &te) || te.Message != `The argument "service" must be a service id; got a k8s.pod id.` || !errors.Is(err, store.ErrInvalid) {
		t.Errorf("the slot was not released: %v", err)
	}
}

// The note about sightings with a TTL is written when a span of the answer ends by
// expiry, whichever it is: for cart only a pair ends so, and for the service with a
// bidi override in its name, which meets neither cart nor legacy-pay, none does.
func TestServiceContextNotesTheTTL(t *testing.T) {
	w := newSvcWorld(t, newSvcMem(t))
	s := w.source(t)
	const note = "A dependency, an instance and its pod mapping are sightings with a TTL: a span that ends by liveness_expiry ends one TTL after the last sighting, so a service that stopped calling, or an instance that stopped reporting, is shown until then."
	cart := serviceContext(t, s, ServiceContextInput{Service: w.id("cart"), From: w10, To: w1030})
	for _, h := range append(append([]HopService{}, cart.Callers...), cart.Callees...) {
		for _, sp := range h.Spans {
			if sp.EndReason != nil {
				t.Fatalf("a dependency of cart ends: %+v", h)
			}
		}
	}
	if !contains(cart.Notes, note) {
		t.Errorf("cart: no note about the TTL in %q", cart.Notes)
	}
	weird := serviceContext(t, s, ServiceContextInput{Service: w.id(weirdName), From: w10, To: w1030})
	if len(weird.Pairs) == 0 || contains(weird.Notes, note) {
		t.Errorf("the note about the TTL, though nothing of the answer expires: %d pairs, notes %q", len(weird.Pairs), weird.Notes)
	}
}
