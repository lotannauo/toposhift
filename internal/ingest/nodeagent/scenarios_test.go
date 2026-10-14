package nodeagent_test

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/lotannauo/toposhift/internal/catalog"
	"github.com/lotannauo/toposhift/internal/identity"
	"github.com/lotannauo/toposhift/internal/ingest/nodeagent"
	"github.com/lotannauo/toposhift/internal/lifecycle"
	"github.com/lotannauo/toposhift/internal/store"
)

// perSubject counts the records written for each subject.
func perSubject(recs []store.Record) map[store.Subject]int {
	out := map[store.Subject]int{}
	for _, r := range recs {
		out[r.Subject]++
	}
	return out
}

// expectedRuns is the records one subject has after n steady reports, the
// coalescer in the sequencer's setting being what it is: a run is continued by a
// new one at the first refresh at least one extension interval after its start, so
// a run takes ceil(extension interval / report interval) reports.
func expectedRuns(n int) int {
	ext := coalescerConfig.ExtensionInterval(ttl900)
	per := int((ext + interval300 - 1) / interval300)
	return (n + per - 1) / per
}

// (a) Twenty-four hours of steady reports of three nodes on three hosts: the
// records stored after the coalescer are the expected runs, host and edge alike,
// and every host and edge is alive at every minute from its first report until its
// TTL after its last, and not before.
func TestScenarioA_SteadyDay(t *testing.T) {
	t.Parallel()
	const reports = 24 * 12
	agents := distinctAgents(t)
	st := newMem(t)
	p := newPipe(t, st)
	start := instantOf(agents[0].Rec)
	stream := steady(agents, reports, start)
	p.feedAll(stream)

	hosts := map[string]identity.Fingerprint{}
	nodes := map[string]identity.Fingerprint{}
	for _, a := range agents {
		hosts[a.Name] = hostFP(t, a.resStr(t, "host.id"))
		nodes[a.Name] = nodeFP(t, a.resStr(t, "k8s.node.uid"))
	}
	counts := perSubject(p.written)
	if len(counts) != 2*len(agents) {
		t.Fatalf("%d subjects written, want %d", len(counts), 2*len(agents))
	}
	want := expectedRuns(reports)
	if want != 144 {
		t.Fatalf("expected runs = %d: the arithmetic of this test is out of date", want)
	}
	for sub, n := range counts {
		if n != want {
			t.Errorf("%v %v: %d records, want %d", sub.Kind, sub.Relation, n, want)
		}
	}
	raw := 0
	for _, r := range p.results {
		raw += len(r.Records)
	}
	t.Logf("%d raw records, %d stored (%.1f%%)", raw, len(p.written), 100*float64(len(p.written))/float64(raw))

	last := instantOf(stream[len(stream)-1].Rec)
	first := start
	for i, a := range agents {
		h, n := hosts[a.Name], nodes[a.Name]
		offset := time.Duration(i) * time.Second // steady staggers the templates by a second
		if alive(t, st, h, first.Add(offset-time.Nanosecond)) {
			t.Errorf("%s: alive before its first report", a.Name)
		}
		for at := first.Add(offset); !at.After(last.Add(-time.Minute)); at = at.Add(time.Minute) {
			if !alive(t, st, h, at) {
				t.Fatalf("%s: host not alive at +%s", a.Name, at.Sub(start))
			}
			if got := runsOn(t, st, n, at); len(got) != 1 || got[0] != h {
				t.Fatalf("%s: node runs on %v at +%s", a.Name, got, at.Sub(start))
			}
			if got := nodesOn(t, st, h, at); len(got) != 1 || got[0] != n {
				t.Fatalf("%s: host carries nodes %v at +%s", a.Name, got, at.Sub(start))
			}
		}
	}
	for _, r := range p.written {
		if !r.Through.IsZero() {
			t.Errorf("a steady stream wrote a record with Through: %s", describeRecords([]store.Record{r}))
		}
	}
}

// (b) An outage longer than the TTL: host and edge both lapse at the last report
// plus the TTL, and nothing is backdated; the first report after it begins a new
// interval at its own time.
func TestScenarioB_Outage(t *testing.T) {
	t.Parallel()
	agents := distinctAgents(t)[:1]
	a := agents[0]
	h, n := hostFP(t, a.resStr(t, "host.id")), nodeFP(t, a.resStr(t, "k8s.node.uid"))
	st := newMem(t)
	p := newPipe(t, st)
	start := instantOf(a.Rec)
	const before, after = 13, 5
	in := steady(agents, before, start)
	last := instantOf(in[len(in)-1].Rec)
	// The outage: nothing for 70 minutes, the flush ticks running.
	resume := last.Add(70 * time.Minute)
	p.feedAll(in)
	p.advanceTo(resume)
	resumed := steady(agents, after, resume)
	p.feedAll(resumed)

	lapse := last.Add(ttl900)
	for _, c := range []struct {
		at   time.Time
		want bool
		what string
	}{
		{start.Add(-time.Nanosecond), false, "before the first report"},
		{start, true, "at the first report"},
		{lapse.Add(-time.Nanosecond), true, "just before the last report plus the TTL"},
		{lapse, false, "at the last report plus the TTL"},
		{lapse.Add(time.Minute), false, "in the outage"},
		{resume.Add(-time.Nanosecond), false, "just before the first report after it"},
		{resume, true, "at the first report after it"},
	} {
		if got := alive(t, st, h, c.at); got != c.want {
			t.Errorf("host %s: alive = %v, want %v", c.what, got, c.want)
		}
		got := runsOn(t, st, n, c.at)
		if (len(got) == 1) != c.want {
			t.Errorf("edge %s: runs on %v, want alive = %v", c.what, got, c.want)
		}
	}
	// Nothing is backdated: every record is at the time of a report, and a run
	// that was extended says so with a Through that is a report's time.
	reports := map[time.Time]bool{}
	for _, x := range append(in, resumed...) {
		reports[instantOf(x.Rec)] = true
	}
	for _, r := range p.written {
		if !reports[r.EventTime] {
			t.Errorf("a record at %s, which is no report's time", r.EventTime)
		}
		if !r.Through.IsZero() && !reports[r.Through] {
			t.Errorf("a record through %s, which is no report's time", r.Through)
		}
	}
	for _, r := range p.results {
		if len(r.Backdated) != 0 {
			t.Error("a result marks records as backdated")
		}
	}
}

// (d) No k8s.node.uid: the host is asserted and there is no edge, counted.
func TestScenarioD_NoNodeUID(t *testing.T) {
	t.Parallel()
	a := distinctAgents(t)[0]
	h := hostFP(t, a.resStr(t, "host.id"))
	st := newMem(t)
	p := newPipe(t, st)
	var in []input
	for k := range 4 {
		in = append(in, a.dropRes("k8s.node.uid").at(instantOf(a.Rec).Add(time.Duration(k)*interval300)))
	}
	p.feedAll(in)
	if !alive(t, st, h, instantOf(a.Rec)) {
		t.Error("the host is not alive")
	}
	if got := nodesOn(t, st, h, instantOf(a.Rec)); len(got) != 0 {
		t.Errorf("nodes on the host: %v", got)
	}
	for _, r := range p.written {
		if r.Subject.Kind == store.SubjectEdge {
			t.Errorf("an edge was written: %s", describeRecords([]store.Record{r}))
		}
	}
	if got := totalSkipped(p.results, nodeagent.RefusedNoNodeUID); got != 4 {
		t.Errorf("%d events counted with no node uid, want 4", got)
	}
}

// (e) No host.id, whether absent (the capture), empty or white space: nothing is
// written, counted, and the placement stops at the node.
func TestScenarioE_NoHostID(t *testing.T) {
	t.Parallel()
	real := loadFixture(t, fixtureReal)
	var in []input
	in = append(in, real...)
	for _, x := range real[:3] {
		in = append(in, x.setResStr("host.id", "").named(x.Name+" empty"), x.setResStr("host.id", " \n\t").named(x.Name+" blank"))
	}
	st := newMem(t)
	p := newPipe(t, st)
	p.feedAll(in)
	if st.LastSeq() != 0 || len(p.written) != 0 {
		t.Fatalf("%d records were written", len(p.written))
	}
	if got := totalSkipped(p.results, nodeagent.SkipNoHostID); got != len(in) {
		t.Errorf("%d events counted with no host id, want %d", got, len(in))
	}
	for _, x := range real[:3] {
		n := nodeFP(t, x.resStr(t, "k8s.node.uid"))
		if got := runsOn(t, st, n, instantOf(x.Rec)); len(got) != 0 {
			t.Errorf("node %s runs on %v", x.resStr(t, "k8s.node.name"), got)
		}
	}
}

// (f) Two nodes on one machine: one host, two nodes, one boot, and no collision.
func TestScenarioF_SharedMachineID(t *testing.T) {
	t.Parallel()
	agents := loadFixture(t, fixtureShared)
	st := newMem(t)
	p := newPipe(t, st)
	start := instantOf(agents[0].Rec)
	stream := steady(agents, 6, start)
	p.feedAll(stream)

	shared := hostFP(t, "ea01b04c38adbb6d2efff691290c1b66")
	at := instantOf(stream[len(stream)-1].Rec)
	want := sorted(nodeFP(t, "72c578b9-1e90-4c76-9319-16197d56e20e"), nodeFP(t, "29ffa0da-cdb4-406b-939a-23bece47625a"))
	if got := nodesOn(t, st, shared, at); !equalFPs(got, want) {
		t.Errorf("nodes on the shared host: %v, want %v", got, want)
	}
	if !alive(t, st, shared, at) {
		t.Error("the shared host is not alive")
	}
	recs, err := st.EntityWindow(context.Background(), shared, start, at.Add(time.Hour), l1)
	if err != nil {
		t.Fatalf("a shared machine id is a collision: %v", err)
	}
	boots := map[string]bool{}
	for _, r := range recs {
		boots[r.Boot] = true
	}
	if len(boots) != 1 || !boots["c574baf2-0d0a-4145-991b-575c9e44f405"] {
		t.Errorf("boots of the shared host: %v", boots)
	}
	// The third node is a host of its own.
	if got := nodesOn(t, st, hostFP(t, "6c48ce4fac22a73e92c7cc06826e54bf"), at); len(got) != 1 {
		t.Errorf("nodes on the third host: %v", got)
	}
}

func equalFPs(a, b []identity.Fingerprint) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// (g) Two collectors that report one host.id with distinct boot ids, interleaved:
// a clone collision, which the store reports by quarantining the host.
func TestScenarioG_CloneCollision(t *testing.T) {
	t.Parallel()
	agents := loadFixture(t, fixtureShared)[:2] // two nodes, one host.id
	a, b := agents[0].setResStr("topo.host.boot.id", "boot-A"), agents[1].setResStr("topo.host.boot.id", "boot-B")
	st := newMem(t)
	p := newPipe(t, st)
	start := instantOf(a.Rec)
	// A at 0, B at 100 s (a new boot appears), A again at 400 s: the older boot is
	// seen 300 s after the newer one first appeared, which is over the skew.
	p.feed(a.at(start))
	p.feed(b.at(start.Add(100 * time.Second)))
	p.feed(a.at(start.Add(400 * time.Second)))

	shared := hostFP(t, a.resStr(t, "host.id"))
	_, err := st.Alive(context.Background(), shared, start.Add(500*time.Second), l1)
	var qe *store.QuarantineError
	if !errors.As(err, &qe) || qe.Entity != shared || qe.Collision == nil {
		t.Fatalf("Alive of the host: %v, want a quarantine of it", err)
	}
	if qe.Collision.StaleBoot != "boot-A" || qe.Collision.NewerBoot != "boot-B" {
		t.Errorf("collision: %+v", qe.Collision)
	}
}

// (h) A node re-created on the same machine has a new uid and a new edge, and the
// old edge lapses by TTL. A node that moves to another host has two live edges
// until the old one lapses, and a pod scheduled on it has no host over the overlap.
func TestScenarioH_NodeRecreatedAndNodeMoved(t *testing.T) {
	t.Parallel()
	t.Run("re-created", func(t *testing.T) {
		before := loadFixture(t, fixtureShared)
		afterIn := loadFixture(t, fixtureRecreated)
		st := newMem(t)
		p := newPipe(t, st)
		// The capture's own events, repeated every five minutes. The capture of the
		// re-created node has the same three machines: its first k3s agent node has the uid
		// that the stream takes from the fourth round on.
		if got := afterIn[0].resStr(t, "k8s.node.uid"); got != "e81f1010-ec47-4561-a900-bcd75b5899c9" {
			t.Fatalf("the re-created node has uid %s", got)
		}
		var stream []input
		for k := range 3 {
			for _, x := range before {
				stream = append(stream, x.at(instantOf(x.Rec).Add(time.Duration(k)*interval300)))
			}
		}
		// The re-created node: from the fourth round the first k3s agent node reports its new
		// uid; the old uid is not reported again.
		for k := 3; k < 6; k++ {
			for i, x := range before {
				y := x.at(instantOf(x.Rec).Add(time.Duration(k) * interval300))
				if i == 0 {
					y = y.setResStr("k8s.node.uid", "e81f1010-ec47-4561-a900-bcd75b5899c9")
				}
				stream = append(stream, y)
			}
		}
		p.feedAll(stream)
		shared := hostFP(t, "ea01b04c38adbb6d2efff691290c1b66")
		oldN, newN := nodeFP(t, "72c578b9-1e90-4c76-9319-16197d56e20e"), nodeFP(t, "e81f1010-ec47-4561-a900-bcd75b5899c9")
		lastOld := instantOf(stream[2*3].Rec)  // the third round's first k3s agent node
		firstNew := instantOf(stream[3*3].Rec) // the fourth round's first k3s agent node
		if got := runsOn(t, st, oldN, firstNew); !equalFPs(got, []identity.Fingerprint{shared}) {
			t.Errorf("old node just after the re-creation: %v", got)
		}
		if got := runsOn(t, st, newN, firstNew); !equalFPs(got, []identity.Fingerprint{shared}) {
			t.Errorf("new node: %v", got)
		}
		if got := runsOn(t, st, oldN, lastOld.Add(ttl900)); len(got) != 0 {
			t.Errorf("old node after its TTL: %v", got)
		}
		if got := runsOn(t, st, oldN, lastOld.Add(ttl900-time.Nanosecond)); len(got) != 1 {
			t.Errorf("old node just before its TTL: %v", got)
		}
		for _, r := range p.written {
			if r.Kind != lifecycle.Observe {
				t.Errorf("a %v record", r.Kind)
			}
		}
	})

	t.Run("moved to another host", func(t *testing.T) {
		a := agentEvent("host-1", "node-1", t0)
		st := newMem(t)
		p := newPipe(t, st)
		pod := podFP(t, "pod-1")
		h1, h2, node := hostFP(t, "host-1"), hostFP(t, "host-2"), nodeFP(t, "node-1")
		p.feed(a)
		schedule(t, st, pod, node, t0, 24*time.Hour)
		p.feed(a.at(t0.Add(5 * time.Minute)))
		// The node is now reported on host-2 (a detector change).
		moved := a.setResStr("host.id", "host-2").setResStr("topo.host.boot.id", "boot-host-2")
		p.feed(moved.at(t0.Add(10 * time.Minute)))
		p.feed(moved.at(t0.Add(15 * time.Minute)))
		p.feed(moved.at(t0.Add(20 * time.Minute)))
		p.advanceTo(t0.Add(3 * time.Hour))

		oldLapse := t0.Add(5 * time.Minute).Add(ttl900)
		for _, c := range []struct {
			at    time.Time
			hosts []identity.Fingerprint
			pod   placement
			what  string
		}{
			{t0.Add(time.Minute), sorted(h1), placement{host: h1}, "before the move"},
			{t0.Add(10 * time.Minute), sorted(h1, h2), placement{why: "two hosts"}, "when the move is first reported"},
			{oldLapse.Add(-time.Nanosecond), sorted(h1, h2), placement{why: "two hosts"}, "just before the old edge lapses"},
			{oldLapse, sorted(h2), placement{host: h2}, "when the old edge lapses"},
		} {
			if got := runsOn(t, st, node, c.at); !equalFPs(got, c.hosts) {
				t.Errorf("%s: the node runs on %v, want %v", c.what, got, c.hosts)
			}
			if got := podHost(t, st, pod, c.at); got != c.pod {
				t.Errorf("%s: the pod's host is %+v, want %+v", c.what, got, c.pod)
			}
		}
		for _, r := range p.written {
			if r.Kind != lifecycle.Observe {
				t.Errorf("a %v record: the old edge must lapse, not be deleted", r.Kind)
			}
		}
	})

	t.Run("no runs_on covers the pod", func(t *testing.T) {
		st := newMem(t)
		p := newPipe(t, st)
		pod, node := podFP(t, "pod-1"), nodeFP(t, "node-1")
		p.feed(agentEvent("host-1", "node-1", t0))
		schedule(t, st, pod, node, t0, 24*time.Hour)
		p.advanceTo(t0.Add(3 * time.Hour))
		if got := podHost(t, st, pod, t0.Add(2*time.Hour)); got != (placement{why: "no runs_on"}) {
			t.Errorf("a pod whose node has no runs_on: %+v", got)
		}
	})
}

// (i) A clock that is far ahead, or no time at all, takes the receipt, counted;
// the stored record says so.
func TestScenarioI_ClockAheadOrZero(t *testing.T) {
	t.Parallel()
	a := distinctAgents(t)[0]
	start := instantOf(a.Rec)
	zero := a.clone()
	zero.Rec.TimeUnixNano = 0
	tr := newTranslator(t)
	st := newMem(t)
	host := hostFP(t, a.resStr(t, "host.id"))
	for i, c := range []struct {
		name   string
		x      input
		reason string
	}{
		{"two hours ahead", a.at(start.Add(2 * time.Hour)), nodeagent.SubstitutedReceiptImplausibleTime},
		{"zero", zero, nodeagent.SubstitutedReceiptNoTime},
	} {
		received := start.Add(time.Duration(i) * time.Hour)
		r := translate(t, tr, c.x, received)
		if skipped(r, c.reason) != 1 || len(r.Records) != 2 {
			t.Fatalf("%s: records %s, skipped %v", c.name, describeRecords(r.Records), r.Skipped)
		}
		for j, rec := range r.Records {
			if !rec.EventTime.Equal(received) || rec.EventTimeBasis != store.BasisReceipt {
				t.Errorf("%s: a record at %s with basis %s, want %s with basis receipt", c.name, rec.EventTime, rec.EventTimeBasis, received)
			}
			rec.Seq = st.LastSeq() + 1 + uint64(j)
			if err := st.Write(context.Background(), []store.Record{rec}); err != nil {
				t.Fatal(err)
			}
		}
		recs, err := st.EntityWindow(context.Background(), host, received, received.Add(time.Minute), l1)
		if err != nil || len(recs) != 1 || recs[0].EventTimeBasis != store.BasisReceipt {
			t.Errorf("%s: stored %v, %v", c.name, describeRecords(recs), err)
		}
	}
}

// (j) Events that report another interval, or none, among those that report 300:
// each is counted and noted, every Observe keeps the TTL of 900 seconds, and the
// runs of refreshes are the same as without them.
func TestScenarioJ_MismatchedIntervals(t *testing.T) {
	t.Parallel()
	agents := distinctAgents(t)
	const reports = 24
	stream := steady(agents, reports, instantOf(agents[0].Rec))
	mixed := make([]input, len(stream))
	mismatched := 0
	for i, x := range stream {
		mixed[i] = x
		switch i % 7 {
		case 2:
			mixed[i] = x.setRec("entity.report.interval", iv(600))
			mismatched++
		case 5:
			mixed[i] = x.dropRec("entity.report.interval")
			mismatched++
		}
	}
	run := func(in []input) *pipe {
		p := newPipe(t, newMem(t))
		p.feedAll(in)
		return p
	}
	control, mix := run(stream), run(mixed)

	if len(mix.written) != len(control.written) {
		t.Errorf("%d records with the mismatched events, %d without: the runs are broken", len(mix.written), len(control.written))
	}
	for _, r := range mix.written {
		if r.TTL != ttl900 {
			t.Errorf("a record with TTL %s", r.TTL)
		}
	}
	if got := totalSkipped(mix.results, nodeagent.SubstitutedConfiguredInterval); got != mismatched {
		t.Errorf("%d events counted as mismatched, want %d", got, mismatched)
	}
	notes := map[string]int{}
	for _, r := range mix.results {
		for _, n := range r.Notes {
			if n.Reason == nodeagent.NoteIntervalMismatch {
				notes[n.Detail] += n.Count
			}
		}
	}
	if len(notes) != 2 || notes["10m0s"] != 1 || notes["missing"] != 1 {
		t.Errorf("notes of mismatches: %v, want each value once", notes)
	}
	if got := totalSkipped(control.results, nodeagent.SubstitutedConfiguredInterval); got != 0 {
		t.Errorf("the control stream counted %d mismatches", got)
	}
}

// (k) Zones off writes no zone; on, a zone and an edge only with all three keys.
func TestScenarioK_Zones(t *testing.T) {
	t.Parallel()
	agents := distinctAgents(t)
	cloud := func(x input) input {
		return x.setResStr("cloud.provider", "aws").setResStr("cloud.region", "eu-west-1").setResStr("cloud.availability_zone", "eu-west-1b")
	}
	// Two hosts in the zone, one with a key missing, one with no cloud keys.
	in := []input{
		cloud(agents[0]),
		cloud(agents[1]),
		cloud(agents[2]).dropRes("cloud.region"),
	}
	zoneID := "aws/eu-west-1/eu-west-1b"
	for _, on := range []bool{false, true} {
		var opts []func(*nodeagent.Config)
		if on {
			opts = append(opts, zones)
		}
		st := newMem(t)
		p := newPipe(t, st, opts...)
		p.feedAll(steady(in, 4, instantOf(agents[0].Rec)))
		at := instantOf(agents[0].Rec).Add(20 * time.Minute)
		zoneHosts, err := st.Neighbors(context.Background(), zoneFP(t, zoneID), store.Reverse, at, l1)
		if err != nil {
			t.Fatal(err)
		}
		switch {
		case !on && len(zoneHosts) != 0:
			t.Errorf("zones off: the zone has %v", zoneHosts)
		case on && len(zoneHosts) != 2:
			t.Errorf("zones on: the zone has %d hosts, want 2", len(zoneHosts))
		}
		zonesWritten := map[identity.Fingerprint]bool{}
		for _, r := range p.written {
			if r.Subject.Kind == store.SubjectEntity && r.Subject.A.Type() == catalog.Zone {
				zonesWritten[r.Subject.A] = true
			}
			if r.Subject.Relation == catalog.LocatedIn && r.Layer != catalog.L1 {
				t.Errorf("located_in in %s", r.Layer)
			}
		}
		if on && (len(zonesWritten) != 1 || !zonesWritten[zoneFP(t, zoneID)]) || !on && len(zonesWritten) != 0 {
			t.Errorf("zones on = %v: zones written %v", on, zonesWritten)
		}
	}
}

// (l) A stream whose host.cpu.model.name is empty (always, or now and then) stores
// as many records as one that has no such key, because empty values are left out;
// os.version never reaches a payload.
func TestScenarioL_EmptyAttributesCoalesce(t *testing.T) {
	t.Parallel()
	agents := distinctAgents(t)
	const reports = 24
	base := steady(agents, reports, instantOf(agents[0].Rec))
	variants := map[string][]input{"none": base, "always empty": nil, "sometimes empty": nil}
	for i, x := range base {
		variants["always empty"] = append(variants["always empty"], x.setResStr("host.cpu.model.name", "").setResStr("os.version", "6.1.0"))
		y := x.setResStr("os.version", "6.1.0")
		if i%3 == 0 {
			y = y.setResStr("host.cpu.model.name", "")
		}
		variants["sometimes empty"] = append(variants["sometimes empty"], y)
	}
	want := -1
	for _, name := range []string{"none", "always empty", "sometimes empty"} {
		p := newPipe(t, newMem(t))
		p.feedAll(variants[name])
		if want < 0 {
			want = len(p.written)
		}
		if len(p.written) != want {
			t.Errorf("%s: %d records, want %d", name, len(p.written), want)
		}
		for _, r := range p.written {
			if strings.Contains(string(r.Payload), "os.version") || strings.Contains(string(r.Payload), "cpu") {
				t.Errorf("%s: payload %s", name, r.Payload)
			}
		}
	}
}

// A changed description starts a new run, as the coalescer is built to do: a new
// cpu model is a record, and the same value again is not.
func TestAChangedDescriptionIsARecordAndARepeatIsNot(t *testing.T) {
	t.Parallel()
	a := distinctAgents(t)[0]
	p := newPipe(t, newMem(t))
	start := instantOf(a.Rec)
	for k, cpu := range []string{"", "N1", "N1", "N1", "N2", "N2"} {
		x := a.at(start.Add(time.Duration(k) * 100 * time.Second))
		if cpu != "" {
			x = x.setResStr("host.cpu.model.name", cpu)
		}
		p.feed(x)
	}
	hosts := 0
	for _, r := range p.written {
		if r.Subject.Kind == store.SubjectEntity {
			hosts++
		}
	}
	if hosts != 3 {
		t.Errorf("%d host records, want 3 (no cpu, N1, N2)", hosts)
	}
}
