package sequencer_test

import (
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"testing"
	"time"

	"github.com/lotannauo/toposhift/internal/catalog"
	"github.com/lotannauo/toposhift/internal/ingest/otlphttp"
	"github.com/lotannauo/toposhift/internal/ingest/sequencer"
	"github.com/lotannauo/toposhift/internal/lifecycle"
	"github.com/lotannauo/toposhift/internal/replay/capture"
	"github.com/lotannauo/toposhift/internal/store"
)

// ---- a node collector's host entity events ----

var naTrue = true

// naScope is the scope the receivers write entity events in, in the current form
// and the legacy one alike.
var naScope = capture.InstrumentationScope{Attributes: []capture.KeyValue{
	{Key: "otel.entity.event_as_log", Value: capture.AnyValue{BoolValue: &naTrue}},
}}

// naResource is the resource of a node collector's event: the host, the node, a
// boot and the description keys the detectors give. An empty host or node leaves
// the attribute out.
func naResource(cluster, hostID, nodeUID string) capture.Resource {
	attrs := []capture.KeyValue{
		{Key: "os.type", Value: sv("linux")},
		{Key: "host.arch", Value: sv("arm64")},
		{Key: "topo.host.boot.id", Value: sv("boot-1")},
	}
	if cluster != "" {
		attrs = append(attrs, capture.KeyValue{Key: "k8s.cluster.name", Value: sv(cluster)})
	}
	if hostID != "" {
		attrs = append(attrs, capture.KeyValue{Key: "host.id", Value: sv(hostID)})
	}
	if nodeUID != "" {
		attrs = append(attrs, capture.KeyValue{Key: "k8s.node.uid", Value: sv(nodeUID)})
	}
	return capture.Resource{Attributes: attrs}
}

// naEvent is a host entity event with the given report interval, in seconds.
func naEvent(tm time.Time, interval int64) capture.LogRecord {
	return capture.LogRecord{
		EventName:    "entity.state",
		TimeUnixNano: capture.Uint64(tm.UnixNano()),
		Attributes: []capture.KeyValue{
			{Key: "entity.type", Value: sv("host")},
			{Key: "entity.report.interval", Value: iv(interval)},
		},
	}
}

func naResourceLogs(res capture.Resource, recs ...capture.LogRecord) capture.ResourceLogs {
	return capture.ResourceLogs{Resource: res, ScopeLogs: []capture.ScopeLogs{{Scope: naScope, LogRecords: recs}}}
}

func naRequest(res capture.Resource, recs ...capture.LogRecord) capture.LogsData {
	return capture.LogsData{ResourceLogs: []capture.ResourceLogs{naResourceLogs(res, recs...)}}
}

func naHarness(t *testing.T, opts ...func(*sequencer.Config)) (*harness, *fakeStore, *logCapture) {
	t.Helper()
	lc := newLogCapture(t.Output())
	st := &fakeStore{inner: newMem(t)}
	h := newHarness(t, st, withConfig(func(c *sequencer.Config) {
		c.Logger = slog.New(lc)
		for _, o := range opts {
			o(c)
		}
	})).start()
	return h, st, lc
}

// ---- the router ----

// A host entity event goes to the node agents' translator, with its own producer,
// and the other entity events and the objects receiver's records go where they
// went; the node's placement is written with the host, and none of it is a record
// of the cluster receiver's translator.
func TestNodeAgentEventsGoToTheirOwnProducer(t *testing.T) {
	h, st, _ := naHarness(t)
	tm := at(time.Minute)
	h.clock.set(tm)
	req := capture.LogsData{ResourceLogs: []capture.ResourceLogs{
		naResourceLogs(naResource("c1", "host-a", n1.uid), naEvent(tm, 300)),
		resource("c1", pullNode(tm, n1), nodeState(n1, tm), podState(p1, tm)),
	}}
	h.sendOK(req)
	h.stop()

	host := fpOf(t, catalog.Host, catalog.HostID, "host-a")
	var agent, withHost []store.Record
	producers := map[lifecycle.Producer]int{}
	for _, r := range st.records() {
		producers[r.Producer]++
		if r.Producer == "nodeagent/c1" {
			agent = append(agent, r)
		}
		if r.Subject.A == host || r.Subject.B == host {
			withHost = append(withHost, r)
		}
	}
	for _, p := range []lifecycle.Producer{"nodeagent/c1", "k8scluster/c1", "k8sobjects/c1"} {
		if producers[p] == 0 {
			t.Errorf("no record of %s: %v", p, producers)
		}
	}
	if len(agent) != 2 || len(withHost) != 2 {
		t.Fatalf("%d records of nodeagent/c1 and %d about the host, want 2 and 2: %v", len(agent), len(withHost), producers)
	}
	for _, r := range agent {
		if r.Layer != catalog.L1 || r.TTL != 15*time.Minute || r.Kind != lifecycle.Observe {
			t.Errorf("a record of the node agents in %s, TTL %s, %v", r.Layer, r.TTL, r.Kind)
		}
	}
	edge := store.EdgeSubject(nodeFP(t, n1.uid), host, catalog.RunsOn)
	if agent[0].Subject != store.EntitySubject(host) || agent[1].Subject != edge {
		t.Errorf("the records are %v and %v, want the host and its runs_on edge", agent[0].Subject, agent[1].Subject)
	}
	// Nothing about the host came from another translator.
	for _, r := range withHost {
		if r.Producer != "nodeagent/c1" {
			t.Errorf("a record about the host from %s", r.Producer)
		}
	}
}

// The TTL of the node agents' records is three node intervals of the sequencer,
// whatever interval an event carries.
func TestNodeAgentTTLIsThreeNodeIntervals(t *testing.T) {
	for _, interval := range []time.Duration{0, 10 * time.Minute} {
		h, st, _ := naHarness(t, func(c *sequencer.Config) { c.NodeInterval = interval })
		tm := at(time.Minute)
		h.clock.set(tm)
		h.sendOK(naRequest(naResource("c1", "host-a", "node-a"), naEvent(tm, 300)))
		h.stop()
		want := 3 * interval
		if interval == 0 {
			want = 3 * sequencer.DefaultNodeInterval
		}
		recs := st.records()
		if len(recs) != 2 {
			t.Fatalf("interval %s: %d records", interval, len(recs))
		}
		for _, r := range recs {
			if r.TTL != want {
				t.Errorf("interval %s: a record with TTL %s, want %s", interval, r.TTL, want)
			}
		}
	}
}

func TestNewRefusesANegativeNodeInterval(t *testing.T) {
	st := &fakeStore{inner: newMem(t)}
	if _, err := sequencer.New(sequencer.Config{Store: st, NodeInterval: -time.Second}); err == nil {
		t.Error("a negative node interval was accepted")
	}
}

// ---- the counters ----

func naCounters(h *harness) map[string]int64 { return h.seq.NodeAgentStatus().Counters }

// What was done with a record is counted under a name that says it, and the
// warnings come at most once a minute for each kind and cluster.
func TestNodeAgentCountersAndWarnings(t *testing.T) {
	h, st, lc := naHarness(t)
	tm := at(time.Minute)
	h.clock.set(tm)
	good := naResource("c1", "host-a", "node-a")
	del := naEvent(tm, 300)
	del.EventName = "entity.delete"
	noTime := naEvent(tm, 300)
	noTime.TimeUnixNano = 0
	noBoot := capture.Resource{Attributes: slices.DeleteFunc(slices.Clone(good.Attributes), func(kv capture.KeyValue) bool {
		return kv.Key == "topo.host.boot.id"
	})}

	h.sendOK(naRequest(good, naEvent(tm, 300)))                                 // clean
	h.sendOK(naRequest(naResource("c1", "", "node-b"), naEvent(tm, 300)))       // no host id
	h.sendOK(naRequest(naResource("c1", "", "node-c"), naEvent(tm, 300)))       // no host id again
	h.sendOK(naRequest(naResource("c1", "host-d", ""), naEvent(tm, 300)))       // no node uid
	h.sendOK(naRequest(good, naEvent(tm, 600)))                                 // another interval
	h.sendOK(naRequest(good, del))                                              // a delete
	h.sendOK(naRequest(noBoot, naEvent(tm, 300)))                               // no boot id
	h.sendOK(naRequest(good, noTime))                                           // no time
	h.sendOK(naRequest(good, naEvent(tm.Add(time.Hour), 300)))                  // a clock an hour ahead
	h.sendOK(naRequest(naResource("c2", "host-e", "node-e"), naEvent(tm, 300))) // another cluster

	want := map[string]int64{
		"nodeagent.host_events":                          10,
		"nodeagent.refused_no_host_id":                   2,
		"nodeagent.refused_no_node_uid":                  1,
		"nodeagent.substituted_configured_interval":      1,
		"nodeagent.skipped_host_delete":                  1,
		"nodeagent.substituted_no_boot":                  1,
		"nodeagent.substituted_receipt_no_time":          1,
		"nodeagent.substituted_receipt_implausible_time": 1,
	}
	got := naCounters(h)
	for k, v := range want {
		if got[k] != v {
			t.Errorf("%s = %d, want %d", k, got[k], v)
		}
	}
	for k, v := range got {
		if _, ok := want[k]; !ok && v != 0 {
			t.Errorf("an unexpected counter %s = %d", k, v)
		}
	}

	lines := lc.take()
	if n := count(lines, slog.LevelWarn, "no usable host.id"); n != 1 {
		t.Errorf("%d warnings of events with no host id in one minute, want 1", n)
	}
	if n := count(lines, slog.LevelWarn, "no k8s.node.uid"); n != 1 {
		t.Errorf("%d warnings of events with no node uid, want 1", n)
	}
	// A minute and a half later, the next is warned about again; another cluster is
	// warned about at once.
	h.clock.set(tm.Add(90 * time.Second))
	h.sendOK(naRequest(naResource("c1", "", "node-b"), naEvent(h.clock.now(), 300)))
	h.sendOK(naRequest(naResource("c2", "", "node-b"), naEvent(h.clock.now(), 300)))
	if n := count(lc.take(), slog.LevelWarn, "no usable host.id"); n != 2 {
		t.Errorf("%d warnings after a minute and a half, want 2 (one for each cluster)", n)
	}
	h.stop()

	// The refused records wrote nothing, and the others wrote what they should:
	// hosts host-a, host-d and host-e, and the edges of node-a and node-e.
	hosts := map[string]bool{}
	for _, r := range st.records() {
		if r.Subject.Kind == store.SubjectEntity && r.Subject.A.Type() == catalog.Host {
			for _, id := range []string{"host-a", "host-b", "host-c", "host-d", "host-e"} {
				if r.Subject.A == fpOf(t, catalog.Host, catalog.HostID, id) {
					hosts[id] = true
				}
			}
		}
	}
	if len(hosts) != 3 || !hosts["host-a"] || !hosts["host-d"] || !hosts["host-e"] {
		t.Errorf("hosts stored: %v", hosts)
	}
}

// A request refused whole counts nothing of the node agents.
func TestARefusedRequestCountsNoNodeAgentEvents(t *testing.T) {
	h, _, _ := naHarness(t, func(c *sequencer.Config) { c.ClusterName, c.ClusterNameSet = "c1", true })
	tm := at(time.Minute)
	h.clock.set(tm)
	_, err := h.send(naRequest(naResource("other", "host-a", "node-a"), naEvent(tm, 300)))
	var refused *otlphttp.RefusedError
	if !errors.As(err, &refused) {
		t.Fatalf("err = %v, want a refusal", err)
	}
	if got := h.seq.NodeAgentStatus(); len(got.Counters) != 0 || len(got.Clusters) != 0 {
		t.Errorf("a refused request counted %v, %v", got.Counters, got.Clusters)
	}
}

// ---- the legacy form ----

// Entity events in the older form, from either receiver, are counted and
// translated by nobody: nothing is stored for them, and the current form, which
// carries the same scope attribute, is not mistaken for it.
func TestLegacyEntityEventsAreCountedAndNotTranslated(t *testing.T) {
	h, st, lc := naHarness(t)
	tm := at(time.Minute)
	h.clock.set(tm)

	// The legacy form of a collector's host event: no event name, the entity in
	// otel.entity.* attributes, the scope attribute.
	legacyHost := capture.LogRecord{
		TimeUnixNano: capture.Uint64(tm.UnixNano()),
		Attributes: []capture.KeyValue{
			{Key: "otel.entity.event.type", Value: sv("entity_state")},
			{Key: "otel.entity.type", Value: sv("host")},
			{Key: "otel.entity.interval", Value: iv(300000)},
		},
	}
	// The cluster receiver's pod state with an otel.entity.* attribute of its own.
	legacyPod := withAttr(podState(p1, tm), "otel.entity.type", sv("k8s.pod"))
	h.sendOK(naRequest(naResource("c1", "host-a", "node-a"), legacyHost))
	h.sendOK(logs("c1", legacyPod))
	if got := naCounters(h)["sequencer.legacy_entity_events"]; got != 2 {
		t.Fatalf("legacy_entity_events = %d, want 2", got)
	}
	if n := count(lc.take(), slog.LevelWarn, "legacy form"); n != 1 {
		t.Errorf("%d warnings of legacy events in one minute, want 1", n)
	}
	if got := st.records(); len(got) != 0 {
		t.Fatalf("%d records were written for legacy events", len(got))
	}

	// The current form, with the same scope attribute, is translated and not counted.
	h.sendOK(capture.LogsData{ResourceLogs: []capture.ResourceLogs{
		naResourceLogs(naResource("c1", "host-a", "node-a"), naEvent(tm, 300)),
		{Resource: naResource("c1", "", ""), ScopeLogs: []capture.ScopeLogs{{Scope: naScope, LogRecords: []capture.LogRecord{podState(p1, tm)}}}},
	}})
	h.sendOK(logs("c1", pullNode(tm, n1)))
	if got := naCounters(h)["sequencer.legacy_entity_events"]; got != 2 {
		t.Errorf("legacy_entity_events = %d after the current form, want 2", got)
	}
	h.stop()
	producers := map[lifecycle.Producer]bool{}
	for _, r := range st.records() {
		producers[r.Producer] = true
	}
	for _, p := range []lifecycle.Producer{"nodeagent/c1", "k8scluster/c1", "k8sobjects/c1"} {
		if !producers[p] {
			t.Errorf("no record of %s after the current form: %v", p, producers)
		}
	}
}

// ---- the latest host event of each cluster ----

// The status holds the instant of each cluster's latest host event. Once three node
// intervals have passed with none, a tick marks the cluster silent and warns once;
// the next event ends the silence, and a later one is warned about again.
func TestNodeAgentSilenceIsMarkedAndWarnedOnce(t *testing.T) {
	h, _, lc := naHarness(t)
	res := naResource("c1", "host-a", "node-a")
	status := func() sequencer.NodeAgentCluster { return h.seq.NodeAgentStatus().Clusters["c1"] }
	silent := func() int {
		return count(lc.take(), slog.LevelWarn, "no host event for more than three node intervals")
	}

	tm := at(time.Minute)
	h.clock.set(tm)
	h.sendOK(naRequest(res, naEvent(tm, 300)))
	if c := status(); !c.LastHostEvent.Equal(tm) || c.Silent {
		t.Fatalf("after the first event: %+v", c)
	}
	// Exactly three intervals is not yet silence.
	h.clock.set(tm.Add(15 * time.Minute))
	h.tick()
	if c := status(); c.Silent || silent() != 0 {
		t.Errorf("silent at three intervals: %+v", c)
	}
	h.clock.set(tm.Add(16 * time.Minute))
	h.tick()
	if c := status(); !c.Silent || !c.LastHostEvent.Equal(tm) {
		t.Errorf("not silent past three intervals: %+v", c)
	}
	if n := silent(); n != 1 {
		t.Errorf("%d warnings when the cluster went silent, want 1", n)
	}
	// It stays silent, with no second warning, however many ticks pass.
	for _, d := range []time.Duration{17 * time.Minute, 30 * time.Minute, 2 * time.Hour} {
		h.clock.set(tm.Add(d))
		h.tick()
	}
	if c := status(); !c.Silent {
		t.Errorf("the status forgot the silent cluster: %+v", c)
	}
	if n := silent(); n != 0 {
		t.Errorf("%d warnings while it stayed silent, want none", n)
	}
	// An event ends the silence at once.
	back := tm.Add(2*time.Hour + time.Minute)
	h.clock.set(back)
	h.sendOK(naRequest(res, naEvent(back, 300)))
	if c := status(); c.Silent || !c.LastHostEvent.Equal(back) {
		t.Errorf("after the agents are back: %+v", c)
	}
	h.clock.set(back.Add(20 * time.Minute))
	h.tick()
	if n := silent(); n != 1 {
		t.Errorf("%d warnings when it went silent again, want 1", n)
	}
	// A day after its latest event, the cluster leaves the status.
	h.clock.set(back.Add(25 * time.Hour))
	h.tick()
	if _, ok := h.seq.NodeAgentStatus().Clusters["c1"]; ok {
		t.Error("the cluster is still in the status a day after its latest event")
	}
}

// ---- scenario (c), through the sequencer ----

const naStreamHours = 4

// naAgents are the nodes of the stream: three nodes on three hosts.
var naAgents = []struct{ host, node string }{{"host-0", "node-0"}, {"host-1", "node-1"}, {"host-2", "node-2"}}

// naReport is one report of every agent at the instant, in one request.
func naReport(tm time.Time) capture.LogsData {
	var d capture.LogsData
	for _, a := range naAgents {
		d.ResourceLogs = append(d.ResourceLogs, naResourceLogs(naResource("", a.host, a.node), naEvent(tm, 300)))
	}
	return d
}

// naOutcome is what a run of the stream leaves to compare.
type naOutcome struct {
	answers      []string
	invalidated  int
	checkpointed int64
}

// naRun runs the stream through a sequencer over a store that writes a checkpoint
// every few records, with the given lag: one report of the agents every five
// minutes for four hours, a flush tick every 30 seconds, and a half hour of ticks
// after the last report. At the report of index cut, which the stream's control
// leaves out as the reset's refused request is, how is: "control" does nothing;
// "dropped" leaves the report out; "reset" sends it and has the store refuse the
// write as invalid, which makes the translators and the coalescer anew;
// "restart" stops the process and starts it again over the same directory.
func naRun(t *testing.T, lag time.Duration, how string) naOutcome {
	t.Helper()
	r := newRS(t, rsOptions{lag: lag, kmin: 4})
	const cut = 2*12 + 1 // the odd report of the second hour, which a run does not start at
	reports := naStreamHours * 12
	end := at(time.Duration(reports) * 5 * time.Minute)
	last := end.Add(30 * time.Minute)
	for tm := t0; !tm.After(last); tm = tm.Add(30 * time.Second) {
		r.clock.set(tm)
		if d := tm.Sub(t0); d%(5*time.Minute) == 0 && !tm.After(end) {
			i := int(d / (5 * time.Minute))
			if i == cut {
				r.arm()
			}
			switch {
			case i != cut || how == "control":
				r.send(naReport(tm))
			case how == "dropped":
				// The report the reset's store refuses is not sent at all.
			case how == "reset":
				var once bool
				r.fs.inject = func(int, []store.Record) (bool, error) {
					if !once {
						once = true
						return false, fmt.Errorf("injected: %w", store.ErrInvalid)
					}
					return true, nil
				}
				r.sendRefused(naReport(tm))
				r.fs.inject = nil
			case how == "restart":
				r.restart()
				r.send(naReport(tm))
			default:
				t.Fatalf("unknown variant %q", how)
			}
		}
		r.tick()
	}

	var out naOutcome
	ctx := t.Context()
	st := r.store()
	for tm := t0; !tm.After(last); tm = tm.Add(7 * time.Minute) {
		for _, a := range naAgents {
			host, node := fpOf(t, catalog.Host, catalog.HostID, a.host), nodeFP(t, a.node)
			up, err := alive(st.Alive(ctx, host, tm, store.Current(catalog.L1)))
			if err != nil {
				t.Fatal(err)
			}
			placed, err := neighbors(st.Neighbors(ctx, node, store.Forward, tm, store.Current(catalog.L1)))
			if err != nil {
				t.Fatal(err)
			}
			carried, err := neighbors(st.Neighbors(ctx, host, store.Reverse, tm, store.Current(catalog.L1)))
			if err != nil {
				t.Fatal(err)
			}
			out.answers = append(out.answers, fmt.Sprintf("%s %s alive=%s runs_on=[%s] carries=[%s]", tm.Sub(t0), a.host, up, placed, carried))
		}
	}
	_, out.invalidated = r.maxPerPrefix()
	out.checkpointed = r.rec.count("checkpoint.written")
	return out
}

// A reset of the sequencer and a restart of the process in the middle of the node
// agents' stream delete no checkpoint of their prefixes afterwards, at both
// checkpoint lags, and the store answers every read as it does in a run without
// them (the reset compared with a run that leaves out the report the store refused).
func TestNodeAgentRecordsAfterAResetInvalidateNoCheckpoint(t *testing.T) {
	for _, lag := range []time.Duration{time.Nanosecond, 5 * time.Minute} {
		t.Run(fmt.Sprint("lag ", lag), func(t *testing.T) {
			control := naRun(t, lag, "control")
			dropped := naRun(t, lag, "dropped")
			if control.checkpointed == 0 || dropped.checkpointed == 0 {
				t.Fatalf("the store wrote no checkpoint (%d, %d): the scenario proves nothing", control.checkpointed, dropped.checkpointed)
			}
			for _, c := range []struct {
				how  string
				want naOutcome
			}{{"restart", control}, {"reset", dropped}} {
				got := naRun(t, lag, c.how)
				t.Logf("lag %s, %s: %d checkpoints written, %d invalidated after the cut (the run it is compared with: %d)",
					lag, c.how, got.checkpointed, got.invalidated, c.want.invalidated)
				if got.invalidated != 0 || c.want.invalidated != 0 {
					t.Errorf("%s: %d checkpoints invalidated after the cut (the control %d), want 0", c.how, got.invalidated, c.want.invalidated)
				}
				if !slices.Equal(got.answers, c.want.answers) {
					t.Errorf("%s: the answers differ from the run without it", c.how)
					for i := range min(len(got.answers), len(c.want.answers)) {
						if got.answers[i] != c.want.answers[i] {
							t.Errorf("  first difference:\n   got  %s\n   want %s", got.answers[i], c.want.answers[i])
							break
						}
					}
				}
			}
		})
	}
}

// The count of invalidated checkpoints is not vacuous at the sequencer: a report of
// a node that the node agents' translator timed before the newest of its prefix
// (a late report, with a changed description so that the coalescer does not take it
// for one it stands for) reaches the store through the sequencer and deletes the
// checkpoints of the node's and the host's edge prefixes after it, at both lags.
func TestALateNodeAgentReportInvalidatesCheckpoints(t *testing.T) {
	for _, lag := range []time.Duration{time.Nanosecond, 5 * time.Minute} {
		t.Run(fmt.Sprint("lag ", lag), func(t *testing.T) {
			r := newRS(t, rsOptions{lag: lag, kmin: 4})
			end := at(3 * time.Hour)
			for tm := t0; !tm.After(end); tm = tm.Add(30 * time.Second) {
				r.clock.set(tm)
				if tm.Sub(t0)%(5*time.Minute) == 0 {
					r.send(naReport(tm))
				}
				r.tick()
			}
			if r.rec.count("checkpoint.written") == 0 {
				t.Fatal("the store wrote no checkpoint")
			}
			r.arm()
			late := naResource("", naAgents[0].host, naAgents[0].node)
			for i, kv := range late.Attributes {
				if kv.Key == "host.arch" {
					late.Attributes[i].Value = sv("amd64")
				}
			}
			r.send(naRequest(late, naEvent(end.Add(-90*time.Minute), 300)))
			if most, total := r.maxPerPrefix(); total == 0 || most == 0 {
				t.Errorf("a late report deleted %d checkpoints (at most %d from one prefix): the count measures nothing", total, most)
			}
		})
	}
}
