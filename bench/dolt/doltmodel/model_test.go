package doltmodel_test

import (
	"errors"
	"fmt"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/lotannauo/toposhift/bench/dolt/doltmodel"
	"github.com/lotannauo/toposhift/bench/spike/engine"
	"github.com/lotannauo/toposhift/internal/catalog"
	"github.com/lotannauo/toposhift/internal/identity"
	"github.com/lotannauo/toposhift/internal/lifecycle"
)

const sec = int64(time.Second)

func fp(typ catalog.EntityType, b byte) identity.Fingerprint {
	var h [identity.FingerprintBytes]byte
	h[0] = b
	f, err := identity.FingerprintFromHash(typ, h)
	if err != nil {
		panic(err)
	}
	return f
}

var (
	pod1, pod2, node1 = fp("k8s.pod", 1), fp("k8s.pod", 2), fp("k8s.node", 3)

	edge1 = engine.EdgeSubject(pod1, node1, catalog.ScheduledOn)
	edge2 = engine.EdgeSubject(pod2, node1, catalog.ScheduledOn)
	edge3 = engine.EdgeSubject(pod1, node1, "part_of")
)

// at is an instant a number of seconds after the epoch.
func at(s int64) time.Time { return time.Unix(s, 0).UTC() }

func observe(s engine.Subject, seq uint64, p lifecycle.Producer, secs, ttl int64) engine.Record {
	return engine.Record{
		Layer: catalog.L2, Subject: s, Producer: p, EventTime: at(secs), Seq: seq,
		Kind: lifecycle.Observe, TTL: time.Duration(ttl) * time.Second, Payload: []byte{byte(seq)},
	}
}

func del(s engine.Subject, seq uint64, p lifecycle.Producer, secs int64) engine.Record {
	return engine.Record{Layer: catalog.L2, Subject: s, Producer: p, EventTime: at(secs), Seq: seq, Kind: lifecycle.Delete}
}

func write(t *testing.T, m *doltmodel.Model, rs ...engine.Record) {
	t.Helper()
	if err := m.Write(rs); err != nil {
		t.Fatal(err)
	}
}

func neighborsOf(t *testing.T, m *doltmodel.Model, fp identity.Fingerprint, dir engine.Direction, secs int64, token uint64) int {
	t.Helper()
	ns, err := m.Neighbors(fp, dir, at(secs), engine.Scope{Layer: catalog.L2, AsOf: token})
	if err != nil {
		t.Fatal(err)
	}
	return len(ns)
}

// forwardState is the forward state rows of the edge subject, one per producer, by
// producer.
func forwardState(m *doltmodel.Model, s engine.Subject) map[string]doltmodel.StateRow {
	out := map[string]doltmodel.StateRow{}
	for _, r := range m.Tables().State {
		if r.Dir == int(engine.Forward) && r.A == string(doltmodel.EncodeFingerprint(s.A)) && r.Peer == string(doltmodel.EncodeFingerprint(s.B)) && r.Rel == string(s.Relation) {
			out[r.Producer] = r
		}
	}
	return out
}

// TestStateKeepsTheNewestRecordOfEachProducer walks one edge through what the state table
// keeps: the deadline of each kind of record, a late record that changes nothing, a tie at
// one instant won by the higher sequence number, a delete that holds nothing, and a record
// with no TTL that holds until the producer's next.
func TestStateKeepsTheNewestRecordOfEachProducer(t *testing.T) {
	m := doltmodel.New()
	write(t, m, observe(edge1, 1, "a", 100, 30))
	row := forwardState(m, edge1)["a"]
	if row.Kind != doltmodel.KindObserve || row.Open || row.DeadlineNs != 130*sec || row.EventNs != 100*sec || row.Seq != doltmodel.EncodeSeq(1) {
		t.Fatalf("an observe with a TTL: %+v", row)
	}
	if got := m.Tables().State; len(got) != 2 || got[0].Dir != int(engine.Forward) || got[1].Dir != int(engine.Reverse) || got[1].A != got[0].Peer {
		t.Fatalf("an edge's state is a row in each direction: %+v", got)
	}

	// A run's Through moves the deadline, as the last observation plus the TTL.
	run := observe(edge1, 2, "a", 100, 30)
	run.Through = at(120)
	write(t, m, run)
	if row := forwardState(m, edge1)["a"]; row.Seq != doltmodel.EncodeSeq(2) || row.DeadlineNs != 150*sec {
		t.Fatalf("a run extended through 120: %+v", row)
	}

	// A late record, older by event time, writes a log row and changes no state.
	before := m.Counts()
	write(t, m, observe(edge1, 3, "a", 90, 30))
	if row := forwardState(m, edge1)["a"]; row.Seq != doltmodel.EncodeSeq(2) {
		t.Fatalf("a late record replaced the state: %+v", row)
	}
	if c := m.Counts(); c.LogRows != before.LogRows+2 || c.StateWrites != before.StateWrites {
		t.Fatalf("a late record: counts %+v after %+v", c, before)
	}

	// At one instant the higher sequence number is the newer.
	write(t, m, observe(edge1, 4, "a", 100, 5))
	if row := forwardState(m, edge1)["a"]; row.Seq != doltmodel.EncodeSeq(4) || row.DeadlineNs != 105*sec {
		t.Fatalf("a tie at one instant: %+v", row)
	}

	// A delete holds nothing from its event time on.
	write(t, m, del(edge1, 5, "a", 110))
	if row := forwardState(m, edge1)["a"]; row.Kind != doltmodel.KindDelete || row.Open || row.DeadlineNs != 110*sec {
		t.Fatalf("a delete: %+v", row)
	}

	// No TTL: open, a NULL deadline; another producer has a row of its own.
	write(t, m, observe(edge1, 6, "b", 120, 0))
	rows := forwardState(m, edge1)
	if len(rows) != 2 || !rows["b"].Open || rows["b"].DeadlineNs != 0 {
		t.Fatalf("an observe with no TTL: %+v", rows)
	}
}

// settled is what one read did to the keys: the keys its rows settled, the keys it sent to
// the log, and so whether the state as of a commit answered it alone.
type settled struct {
	state, log uint64
	alone      bool
}

// counted runs a read and says how the rule settled it, from the change in the counters.
func counted(m *doltmodel.Model, op string, read func()) settled {
	before := m.Paths()
	read()
	after := m.Paths()
	d := func(k string) uint64 { return after[op+"/"+k] - before[op+"/"+k] }
	return settled{state: d("keys-state"), log: d("keys-log"), alone: d("as-of-alone") == 1}
}

// TestACommitIsASnapshotAndEachKeyIsSettledByItsRowOrByTheLog is the heart of the design in
// one scenario. A late batch has an older event time than the batch before it, so its
// commit's frontier is the older batch's, not its own. A read as of a commit takes each
// key's row if the row's event is at or before the instant, and sends the key to the log
// otherwise, so a late record is found and a record after the instant never is.
func TestACommitIsASnapshotAndEachKeyIsSettledByItsRowOrByTheLog(t *testing.T) {
	m := doltmodel.New()
	write(t, m, observe(edge1, 1, "a", 100, 0)) // token 1: placed, until a says otherwise
	write(t, m, del(edge1, 2, "a", 150))        // token 2: a deletes it at 150
	write(t, m, observe(edge1, 3, "a", 50, 0))  // token 3: a late record from 50

	cs := m.Commits()
	if len(cs) != 3 {
		t.Fatalf("%d commits", len(cs))
	}
	wantFrontier := []int64{100 * sec, 150 * sec, 150 * sec} // the late batch's is not 50
	for i, c := range cs {
		if c.Ordinal != i+1 || c.Kind != doltmodel.BatchCommit || c.Token != uint64(i+1) || c.Frontier != wantFrontier[i] || c.ID == "" ||
			c.Message != fmt.Sprintf("seq %d", i+1) || !c.Date.Equal(doltmodel.CommitDate(i+1)) {
			t.Errorf("commit %d: %+v", i+1, c)
		}
	}
	if head, ok := m.Head(); !ok || head != cs[2] {
		t.Fatalf("head %+v", head)
	}
	for token, want := range map[uint64]int64{1: 100 * sec, 2: 150 * sec, 3: 150 * sec, engine.Latest: 150 * sec} {
		if got, ok := m.Frontier(token); !ok || got != want {
			t.Errorf("frontier of token %d = %d, %v; want %d", token, got, ok, want)
		}
	}

	scope := func(token uint64) engine.Scope { return engine.Scope{Layer: catalog.L2, AsOf: token} }
	for _, c := range []struct {
		name  string
		token uint64
		secs  int64
		want  settled
		peers int // neighbors of node1, in reverse
	}{
		{"token 1, after the record: its row settles the key", 1, 200, settled{1, 0, true}, 1},
		{"token 1, at the record's instant: a row at the instant settles it", 1, 100, settled{1, 0, true}, 1},
		{"token 1, just before the record: the row is after the instant, the log finds nothing", 1, 100 - 1, settled{0, 1, false}, 0},
		{"token 2, after the delete", 2, 200, settled{1, 0, true}, 0},
		{"token 2, before the delete: the row is after the instant, the log answers", 2, 120, settled{0, 1, false}, 1},
		{"token 3, before the delete: the late record is found in the log", 3, 60, settled{0, 1, false}, 1},
		{"token 3, before the late record", 3, 40, settled{0, 1, false}, 0},
		{"the newest token, after the delete", engine.Latest, 200, settled{1, 0, true}, 0},
		{"the newest token, before the delete", engine.Latest, 120, settled{0, 1, false}, 1},
	} {
		var got int
		if s := counted(m, "neighbors", func() { got = neighborsOf(t, m, node1, engine.Reverse, c.secs, c.token) }); s != c.want {
			t.Errorf("%s: settled %+v, want %+v", c.name, s, c.want)
		}
		if got != c.peers {
			t.Errorf("%s: %d neighbors, want %d", c.name, got, c.peers)
		}
		if ok, err := m.Alive(pod1, at(c.secs), scope(c.token)); err != nil || ok {
			t.Errorf("%s: the pod has no existence records, Alive = %v, %v", c.name, ok, err)
		}
	}

	// A token that is not a batch boundary names no commit: every key is answered from the
	// log, with the token's filter.
	write(t, m, observe(edge2, 4, "a", 160, 0), observe(edge2, 5, "b", 160, 0))
	if _, ok := m.Frontier(4); ok {
		t.Error("a token inside a batch has a frontier")
	}
	var peers int
	before := m.Paths()["neighbors/no-commit"]
	s := counted(m, "neighbors", func() { peers = neighborsOf(t, m, node1, engine.Reverse, 1000, 4) })
	if s.alone || s.state != 0 || s.log != 0 || m.Paths()["neighbors/no-commit"] != before+1 {
		t.Errorf("a token inside a batch: settled %+v, no-commit %d after %d: it has no commit to read and is counted on its own", s, m.Paths()["neighbors/no-commit"], before)
	}
	if peers != 1 {
		t.Errorf("as of token 4, only the first record of the batch is in: %d neighbors", peers)
	}
	if _, err := m.NeighborsVia(doltmodel.PathCommitTime, node1, engine.Reverse, at(1000), scope(4)); err == nil {
		t.Error("a read was forced onto the diagnostic's path")
	}
}

// TestAReadAtTheHorizonIsUnchangedByRetention: retention deletes the state row of a delete
// whose event is before the horizon and one whose deadline has passed, and a read at the
// horizon, and after it, gives the answer it gave before.
func TestAReadAtTheHorizonIsUnchangedByRetention(t *testing.T) {
	m := doltmodel.New()
	write(t, m, observe(edge1, 1, "a", 10, 0), del(edge1, 2, "a", 12)) // a delete: nothing from 12 on
	write(t, m, observe(edge2, 3, "a", 10, 5))                         // lapsed at 15
	write(t, m, observe(edge3, 4, "a", 10, 0))                         // open
	sc := engine.Current(catalog.L2)
	read := func() (n [3]int) {
		for i, secs := range []int64{20, 21, 1000} {
			ns, err := m.Neighbors(node1, engine.Reverse, at(secs), sc)
			if err != nil {
				t.Fatal(err)
			}
			n[i] = len(ns)
		}
		return n
	}
	before := read()
	if before != [3]int{1, 1, 1} {
		t.Fatalf("before the retention: %v", before)
	}
	rows := len(m.Tables().State)
	if err := m.Retain(at(20)); err != nil {
		t.Fatal(err)
	}
	if after := read(); after != before {
		t.Fatalf("after the retention at 20: %v, before %v", after, before)
	}
	if got := len(m.Tables().State); got != rows-4 { // the delete's row and the lapsed row, in two directions
		t.Fatalf("state rows %d, want %d: the delete and the lapsed row were not deleted", got, rows-4)
	}
}

// TestATokenReadAfterARetentionIsUnchangedAtOrAfterTheHorizon: the token map points a token
// at its newest commit, the retention commit when one follows the batch, and a read as of
// that token at an instant at or after the horizon is what it was before.
func TestATokenReadAfterARetentionIsUnchangedAtOrAfterTheHorizon(t *testing.T) {
	m := doltmodel.New()
	write(t, m, observe(edge1, 1, "a", 10, 5), observe(edge2, 2, "a", 11, 0))
	write(t, m, observe(edge3, 3, "a", 30, 0))
	sc := engine.Scope{Layer: catalog.L2, AsOf: 3}
	read := func() []int {
		var out []int
		for _, secs := range []int64{20, 25, 30, 31, 1000} {
			ns, err := m.Neighbors(node1, engine.Reverse, at(secs), sc)
			if err != nil {
				t.Fatal(err)
			}
			out = append(out, len(ns))
		}
		return out
	}
	before := read()
	if err := m.Retain(at(20)); err != nil {
		t.Fatal(err)
	}
	if after := read(); !slices.Equal(after, before) {
		t.Fatalf("as of token 3 after the retention: %v, before %v", after, before)
	}
	cs := m.Commits()
	last := cs[len(cs)-1]
	if last.Kind != doltmodel.RetentionCommit || last.Token != 3 || last.Ordinal != 3 || last.Message != "retain 20000000000 seq 3" || !last.Date.Equal(doltmodel.CommitDate(3)) {
		t.Fatalf("retention commit %+v", last)
	}
	// The token reads the newest commit with it: the retained state.
	state, _, err := m.StateAt(3)
	if err != nil || len(state) != 4 { // edge2 and edge3, in two directions: edge1 lapsed at 15
		t.Fatalf("state as of the retention commit: %d rows, %v", len(state), err)
	}
}

// TestTheFirstReadsOfAnEmptyModel: with nothing written there is no frontier and the rule
// answers every instant with nothing.
func TestTheFirstReadsOfAnEmptyModel(t *testing.T) {
	m := doltmodel.New()
	if _, ok := m.Head(); ok {
		t.Error("a new model has a head")
	}
	if got, ok := m.Frontier(0); !ok || got != doltmodel.NoFrontier {
		t.Errorf("frontier of token 0 = %d, %v", got, ok)
	}
	sc := engine.Current(catalog.L2)
	if ns, err := m.Neighbors(pod1, engine.Forward, at(5), sc); err != nil || len(ns) != 0 {
		t.Errorf("neighbors of nothing: %v, %v", ns, err)
	}
	if err := m.Write(nil); err != nil || len(m.Commits()) != 0 {
		t.Errorf("an empty batch: %v, %d commits", err, len(m.Commits()))
	}
}

// TestAReadAsOfAnOldTokenReadsThatCommitsState: two commits that change one edge, read as
// of the first at an instant after both.
func TestAReadAsOfAnOldTokenReadsThatCommitsState(t *testing.T) {
	m := doltmodel.New()
	write(t, m, observe(edge1, 1, "a", 10, 0))
	write(t, m, observe(edge2, 2, "a", 11, 0))
	write(t, m, del(edge1, 3, "a", 12))
	for token, want := range map[uint64]int{1: 1, 2: 2, 3: 1, engine.Latest: 1} {
		if got := neighborsOf(t, m, node1, engine.Reverse, 500, token); got != want {
			t.Errorf("as of token %d: %d neighbors, want %d", token, got, want)
		}
	}
	// Every one of those reads is settled by its rows or, for a key whose row is after the
	// instant, never needs the log: here no row is after instant 500.
	if p := m.Paths(); p["neighbors/as-of-alone"] != 4 || p["neighbors/needed-log"] != 0 || p["neighbors/keys-log"] != 0 {
		t.Errorf("paths %v", p)
	}
}

// TestARetentionKeepsBaselinesDeletesWhatExpiredAndCommits covers what retention does to the
// log, the state and the commits.
func TestARetentionKeepsBaselinesDeletesWhatExpiredAndCommits(t *testing.T) {
	m := doltmodel.New()
	write(t, m, observe(edge1, 1, "a", 10, 5), observe(edge1, 2, "a", 12, 5), observe(edge1, 3, "a", 14, 5)) // holds until 19
	write(t, m, observe(edge2, 4, "b", 10, 0))                                                               // open
	write(t, m, observe(edge3, 5, "a", 5, 5))                                                                // lapsed at 10
	before := m.Counts()

	if err := m.Retain(at(18)); err != nil {
		t.Fatal(err)
	}
	c := m.Counts()
	if c.LogDeletes != before.LogDeletes+4 || c.StateDeletes != before.StateDeletes+2 || c.Commits != before.Commits+1 {
		t.Fatalf("counts after the retention %+v, before %+v: want 2 rows of 2 records deleted from the log, the lapsed edge's state, and one commit", c, before)
	}
	last, _ := m.Head()
	if last.Kind != doltmodel.RetentionCommit || last.Token != 5 || last.Ordinal != 4 {
		t.Fatalf("head %+v", last)
	}
	tb := m.Tables()
	if tb.Meta.HorizonNs != 18*sec || tb.Meta.LastSeq != doltmodel.EncodeSeq(5) || tb.Meta.SchemaVersion != doltmodel.SchemaVersion {
		t.Fatalf("meta %+v", tb.Meta)
	}
	if got := forwardState(m, edge3); len(got) != 0 {
		t.Errorf("a lapsed edge's state stays: %+v", got)
	}
	if got := forwardState(m, edge2); !got["b"].Open {
		t.Errorf("an open row was deleted: %+v", got)
	}
	// The baselines stay: the newest record before the horizon of each key.
	fwd := map[string]int64{}
	for _, r := range tb.Log {
		if r.Dir == int(engine.Forward) {
			fwd[r.Rel+"/"+r.Producer] += 1
			if r.Rel == string(catalog.ScheduledOn) && r.Producer == "a" && r.EventNs != 14*sec {
				t.Errorf("edge1's log holds the record at %d, want only the baseline at 14", r.EventNs/sec)
			}
		}
	}
	if len(tb.Log) != 6 || fwd["part_of/a"] != 1 {
		t.Errorf("log rows %d: %v", len(tb.Log), fwd)
	}
	// What held at the horizon still does, and what lapsed does not.
	if got := neighborsOf(t, m, node1, engine.Reverse, 18, engine.Latest); got != 2 {
		t.Errorf("at the horizon: %d neighbors, want edge1 (until 19) and edge2 (open)", got)
	}
	if got := neighborsOf(t, m, node1, engine.Reverse, 19, engine.Latest); got != 1 {
		t.Errorf("after edge1's deadline: %d neighbors", got)
	}

	// A horizon that does not move changes nothing and commits nothing; a write before it
	// is refused whole.
	cs := len(m.Commits())
	if err := m.Retain(at(15)); err != nil || len(m.Commits()) != cs {
		t.Errorf("an earlier retention: %v, %d commits, want %d", err, len(m.Commits()), cs)
	}
	digest := m.Tables().Digest()
	if err := m.Write([]engine.Record{observe(edge2, 6, "a", 20, 0), observe(edge2, 7, "a", 17, 0)}); !errors.Is(err, engine.ErrBeforeHorizon) {
		t.Errorf("a write before the horizon: %v", err)
	}
	if m.Tables().Digest() != digest || m.LastSeq() != 5 || len(m.Commits()) != cs {
		t.Error("a refused batch changed the model")
	}
	// A retention past the last representable instant refuses everything after it.
	if err := m.Retain(engine.MaxEventTime.Add(time.Hour)); err != nil {
		t.Fatal(err)
	}
	if tb := m.Tables(); !tb.Meta.PastEnd {
		t.Errorf("meta after a retention past the end: %+v", tb.Meta)
	}
	if err := m.Write([]engine.Record{observe(edge2, 6, "a", 20, 0)}); !errors.Is(err, engine.ErrBeforeHorizon) {
		t.Errorf("a write after a retention past the end: %v", err)
	}
}

// checkRefusals covers what the contract says a refused batch leaves: nothing, and its
// sequence numbers unused. Each batch has a record that is fine in front of the one that
// is not.
func checkRefusals(m *doltmodel.Model) error {
	if err := m.Write([]engine.Record{observe(edge1, 1, "a", 10, 0)}); err != nil {
		return err
	}
	digest, commits := m.Tables().Digest(), len(m.Commits())

	invalid := observe(edge1, 3, "a", 11, 0)
	invalid.Producer = ""
	otherLayer := observe(edge1, 3, "a", 11, 0)
	otherLayer.Layer = catalog.L3
	long := observe(edge1, 3, "a", 11, 0)
	long.Payload = []byte("12345")
	exact := observe(edge1, 2, "a", 11, 0)
	exact.Payload = []byte("1234")
	longProducer := observe(edge1, 3, lifecycle.Producer(strings.Repeat("p", doltmodel.MaxProducerLen+1)), 11, 0)
	for name, c := range map[string]struct {
		batch []engine.Record
		err   error
	}{
		"an invalid record":                    {[]engine.Record{observe(edge1, 2, "a", 11, 0), invalid}, engine.ErrInvalid},
		"a sequence not above":                 {[]engine.Record{observe(edge1, 2, "a", 11, 0), observe(edge1, 2, "a", 12, 0)}, engine.ErrInvalid},
		"a sequence at the last":               {[]engine.Record{observe(edge1, 1, "a", 12, 0)}, engine.ErrInvalid},
		"a subject in two layers":              {[]engine.Record{observe(edge1, 2, "a", 11, 0), otherLayer}, engine.ErrInvalid},
		"a payload over the column's capacity": {[]engine.Record{exact, long}, engine.ErrInvalid},
		"a producer over the column's size":    {[]engine.Record{exact, longProducer}, engine.ErrInvalid},
	} {
		err := m.Write(c.batch)
		if err == nil {
			return fmt.Errorf("%s: the batch was accepted", name)
		}
		if !errors.Is(err, c.err) {
			return fmt.Errorf("%s: %w", name, err)
		}
		if m.LastSeq() != 1 || m.Tables().Digest() != digest || len(m.Commits()) != commits || m.Counts().Records != 1 {
			return fmt.Errorf("%s: the model moved: seq %d, %d commits, %d records", name, m.LastSeq(), len(m.Commits()), m.Counts().Records)
		}
	}
	// Seq 2 was used by no refused batch, and a payload of exactly the capacity fits.
	if err := m.Write([]engine.Record{exact}); err != nil {
		return fmt.Errorf("seq 2 was used by a refused batch: %w", err)
	}
	return nil
}

func TestWriteRefusesWholeBatches(t *testing.T) {
	if err := checkRefusals(doltmodel.New(doltmodel.WithPayloadCapacity(4))); err != nil {
		t.Fatal(err)
	}
}

// TestTheRowsArmHasNoCommits: the control reads the log as of any older token, and its
// state is the current row alone.
func TestTheRowsArmHasNoCommits(t *testing.T) {
	m := doltmodel.New(doltmodel.WithArm(doltmodel.ArmRows))
	write(t, m, observe(edge1, 1, "a", 10, 0))
	write(t, m, del(edge1, 2, "a", 12))
	if len(m.Commits()) != 0 || m.Counts().Commits != 0 {
		t.Fatalf("commits %d, counted %d", len(m.Commits()), m.Counts().Commits)
	}
	if _, _, err := m.StateAt(1); err == nil {
		t.Error("the rows arm has a state as of a commit")
	}
	if got := counted(m, "neighbors", func() { neighborsOf(t, m, node1, engine.Reverse, 500, 1) }); got != (settled{0, 0, false}) || m.Paths()["neighbors/no-commit"] != 1 {
		t.Errorf("an old token settled %+v, paths %v: it has no commit to read and is counted on its own", got, m.Paths())
	}
	if got := counted(m, "neighbors", func() { neighborsOf(t, m, node1, engine.Reverse, 500, engine.Latest) }); got != (settled{1, 0, true}) {
		t.Errorf("the current state settled %+v", got)
	}
	if got := neighborsOf(t, m, node1, engine.Reverse, 500, 1); got != 1 {
		t.Errorf("as of token 1: %d neighbors", got)
	}
	if got := neighborsOf(t, m, node1, engine.Reverse, 500, engine.Latest); got != 0 {
		t.Errorf("now: %d neighbors", got)
	}
	if rows := forwardState(m, edge1); len(rows) != 1 || rows["a"].Kind != doltmodel.KindDelete {
		t.Errorf("state %+v", rows)
	}
}

// TestStateAtIsTheTablesAsAnAsOfReadSeesThem: each commit's state rows are what the state
// table held when it was made, including rows retention deleted later.
func TestStateAtIsTheTablesAsAnAsOfReadSeesThem(t *testing.T) {
	m := doltmodel.New()
	write(t, m, observe(edge1, 1, "a", 10, 5)) // commit 1
	write(t, m, observe(edge2, 2, "a", 11, 0)) // commit 2
	if err := m.Retain(at(100)); err != nil {  // commit 3 deletes edge1's row
		t.Fatal(err)
	}
	for ord, want := range map[int]int{1: 2, 2: 4, 3: 2} {
		state, ent, err := m.StateAt(ord)
		if err != nil || len(state) != want || len(ent) != 0 {
			t.Errorf("state as of commit %d: %d rows, %d entity rows, %v; want %d", ord, len(state), len(ent), err, want)
		}
	}
	if _, _, err := m.StateAt(0); err == nil {
		t.Error("state as of commit 0")
	}
	if _, _, err := m.StateAt(4); err == nil {
		t.Error("state as of a commit that does not exist")
	}
	// The history is kept: the head is the last commit's state.
	head, _, _ := m.StateAt(3)
	if got := m.Tables().State; !slices.Equal(head, got) {
		t.Errorf("the head state %v is not the last commit's %v", got, head)
	}
}

// TestEntitiesAreStoredByTheirTypeAndAnswerAlive covers the entity tables, which have a row per
// producer and no direction.
func TestEntitiesAreStoredByTheirTypeAndAnswerAlive(t *testing.T) {
	m := doltmodel.New()
	entity := func(seq uint64, p lifecycle.Producer, secs, ttl int64) engine.Record {
		return engine.Record{
			Layer: catalog.L2, Subject: engine.EntitySubject(pod1), Producer: p, EventTime: at(secs), Seq: seq,
			Kind: lifecycle.Observe, TTL: time.Duration(ttl) * time.Second,
		}
	}
	write(t, m, entity(1, "a", 10, 5))
	write(t, m, entity(2, "b", 20, 0))
	tb := m.Tables()
	if len(tb.EntLog) != 2 || len(tb.EntState) != 2 || len(tb.Log) != 0 || len(tb.State) != 0 {
		t.Fatalf("tables %+v", tb)
	}
	if tb.EntLog[0].Layer != int(catalog.L2) || tb.EntState[0].Fp != string(doltmodel.EncodeFingerprint(pod1)) {
		t.Fatalf("entity rows %+v / %+v", tb.EntLog[0], tb.EntState[0])
	}
	sc := engine.Current(catalog.L2)
	for secs, want := range map[int64]bool{9: false, 10: true, 14: true, 15: false, 19: false, 20: true, 1000: true} {
		if got, err := m.Alive(pod1, at(secs), sc); err != nil || got != want {
			t.Errorf("alive at %d = %v, %v; want %v", secs, got, err, want)
		}
	}
	if got, err := m.Alive(pod1, at(1000), engine.Current(catalog.L3)); err != nil || got {
		t.Errorf("an entity is not alive in another layer: %v, %v", got, err)
	}
	if got, err := m.Alive(pod2, at(1000), sc); err != nil || got {
		t.Errorf("a stranger is not alive: %v, %v", got, err)
	}
}

// TestWindowReturnsEveryRecordOfTheTokenInOrderWithItsPayload: a window is the log rows in
// range, in (event time, seq) order, including a record a later one overwrites at its instant.
func TestWindowReturnsEveryRecordOfTheTokenInOrderWithItsPayload(t *testing.T) {
	m := doltmodel.New()
	write(t, m, observe(edge1, 1, "a", 10, 0), observe(edge2, 2, "a", 10, 0))
	write(t, m, observe(edge1, 3, "a", 10, 0), observe(edge1, 4, "b", 5, 0), del(edge1, 5, "a", 20))
	sc := engine.Scope{Layer: catalog.L2, AsOf: engine.Latest}
	got, err := m.Window(node1, engine.Reverse, at(5), at(20), sc)
	if err != nil {
		t.Fatal(err)
	}
	var seqs []uint64
	for _, r := range got {
		seqs = append(seqs, r.Seq)
		if r.Kind == lifecycle.Observe && (len(r.Payload) != 1 || r.Payload[0] != byte(r.Seq)) {
			t.Errorf("payload of seq %d: %v", r.Seq, r.Payload)
		}
	}
	if want := []uint64{4, 1, 2, 3}; !slices.Equal(seqs, want) {
		t.Errorf("window seqs %v, want %v (half open, ordered by event time then seq, overwritten records kept)", seqs, want)
	}
	sc.AsOf = 2
	if got, _ := m.Window(node1, engine.Reverse, at(0), at(100), sc); len(got) != 2 {
		t.Errorf("as of token 2: %d records", len(got))
	}
	if got, _ := m.Window(node1, engine.Reverse, at(20), at(21), engine.Current(catalog.L2)); len(got) != 1 || got[0].Kind != lifecycle.Delete || got[0].Payload != nil {
		t.Errorf("the delete at the edge of the window: %+v", got)
	}
	if got, _ := m.Window(node1, engine.Forward, at(0), at(100), engine.Current(catalog.L2)); len(got) != 0 {
		t.Errorf("the forward window of the target: %d records", len(got))
	}
}

// TestPathsCountTheReadsByKindAndOutcome: the counts are a function of the reads, with reads
// before the range not counted and a batch counted once.
func TestPathsCountTheReadsByKindAndOutcome(t *testing.T) {
	m := doltmodel.New()
	write(t, m, observe(edge1, 1, "a", 100, 0))
	write(t, m, observe(edge2, 2, "a", 200, 0))
	sc := engine.Current(catalog.L2)
	_, _ = m.Neighbors(node1, engine.Reverse, at(500), sc) // both rows settle their keys
	_, _ = m.Neighbors(node1, engine.Reverse, at(150), sc) // edge2's row is after 150: its key goes to the log
	_, _ = m.Alive(node1, at(500), sc)                     // no entity records: nothing to read
	_, _ = m.Alive(node1, at(150), sc)
	_, _ = m.NeighborsBatch([]identity.Fingerprint{node1, pod1}, engine.Reverse, at(500), sc)
	_, _ = m.Window(node1, engine.Reverse, at(0), at(500), sc)
	_, _ = m.Neighbors(node1, engine.Reverse, time.Unix(-5, 0), sc) // before the range: no query
	want := map[string]uint64{
		"neighbors/as-of-alone": 1, "neighbors/needed-log": 1, "neighbors/keys-state": 3, "neighbors/keys-log": 1,
		"alive/as-of-alone": 2, "batch/as-of-alone": 1, "batch/keys-state": 2, "window/log": 1,
	}
	got := m.Paths()
	if len(got) != len(want) {
		t.Fatalf("paths %v, want %v", got, want)
	}
	for k, v := range want {
		if got[k] != v {
			t.Errorf("paths %v, want %v", got, want)
			break
		}
	}
}

// TestRunsAreRepeatable runs a stream twice and gets the same counts, tables and commit
// ids, and a different stream gets different commit ids.
func TestRunsAreRepeatable(t *testing.T) {
	run := func(mod func(*workloadStream)) *doltmodel.Model {
		m := doltmodel.New()
		ws := workloadStream{batch: 40, retain: []float64{0.4}}
		if mod != nil {
			mod(&ws)
		}
		if err := stream(m, pathConfigs()[0], ws.batch, ws.retain, func(written) error { return nil }); err != nil {
			t.Fatal(err)
		}
		return m
	}
	a, b := run(nil), run(nil)
	if a.Counts() != b.Counts() || a.Tables().Digest() != b.Tables().Digest() || !slices.Equal(a.Commits(), b.Commits()) {
		t.Fatal("two runs of one stream differ")
	}
	c := a.Counts()
	if c.Records == 0 || c.LogRows == 0 || c.StateWrites == 0 || c.LogDeletes == 0 || c.StateDeletes == 0 || c.Commits == 0 {
		t.Fatalf("a stream with a retention should write, replace and delete rows: %+v", c)
	}
	if a.Counts().Commits != uint64(len(a.Commits())) {
		t.Fatalf("%d commits counted, %d held", a.Counts().Commits, len(a.Commits()))
	}
	other := run(func(ws *workloadStream) { ws.batch = 41 })
	ha, _ := a.Head()
	ho, _ := other.Head()
	if ha.ID == ho.ID {
		t.Error("a stream batched differently has the same head")
	}
	if a.Tables().Digest() == "" || a.Tables().Len() == 0 {
		t.Error("empty tables")
	}
	// The ids chain: no two commits share one.
	seen := map[string]bool{}
	for _, c := range a.Commits() {
		if seen[c.ID] {
			t.Fatalf("commit %d repeats an id", c.Ordinal)
		}
		seen[c.ID] = true
	}
}

type workloadStream struct {
	batch  int
	retain []float64
}

// TestOpenReturnsWhatWasLeft is the reopen the conformance factory relies on.
func TestOpenReturnsWhatWasLeft(t *testing.T) {
	dir := t.TempDir()
	t.Cleanup(func() { doltmodel.Forget(dir) })
	m := doltmodel.Open(dir, doltmodel.WithArm(doltmodel.ArmRows))
	write(t, m, observe(edge1, 1, "a", 10, 0))
	if err := m.Close(); err != nil {
		t.Fatal(err)
	}
	if again := doltmodel.Open(dir); again.LastSeq() != 1 || again.Arm() != doltmodel.ArmRows {
		t.Fatalf("reopened at seq %d, arm %s", again.LastSeq(), again.Arm())
	}
	doltmodel.Forget(dir)
	if fresh := doltmodel.Open(dir); fresh.LastSeq() != 0 || fresh.Arm() != doltmodel.ArmCommit {
		t.Fatalf("after Forget, seq %d, arm %s", fresh.LastSeq(), fresh.Arm())
	}
}

// TestTablesAreInTheOrderOfTheirKeysByEncoding: a table is ordered by the bytes of the
// encodings, which for fingerprints is the contract's order, and not by the text form.
func TestTablesAreInTheOrderOfTheirKeysByEncoding(t *testing.T) {
	names := []catalog.EntityType{"k8s", "k8s.pod", "k8s.pod.x", "k8s_pod", "k8s0pod", "service"}
	m := doltmodel.New()
	var recs []engine.Record
	for i, n := range names {
		recs = append(recs, observe(engine.EdgeSubject(fp(n, byte(i)), node1, "part_of"), uint64(i+1), "a", 10, 0))
	}
	write(t, m, recs...)
	var fwd []identity.Fingerprint
	for _, r := range m.Tables().State {
		if r.Dir == int(engine.Forward) {
			f, err := doltmodel.DecodeFingerprint([]byte(r.A))
			if err != nil {
				t.Fatal(err)
			}
			fwd = append(fwd, f)
		}
	}
	if len(fwd) != len(names) || !slices.IsSortedFunc(fwd, engine.CompareFingerprints) {
		t.Fatalf("forward state rows are not in the contract's order: %v", fwd)
	}
	byText := slices.Clone(fwd)
	slices.SortFunc(byText, func(a, b identity.Fingerprint) int {
		switch {
		case a.String() < b.String():
			return -1
		case a.String() > b.String():
			return 1
		}
		return 0
	})
	if slices.Equal(byText, fwd) {
		t.Fatal("the text form orders these fingerprints as the contract does, so this test proves nothing")
	}
}

func TestSizeGrowsWithWhatIsStored(t *testing.T) {
	m := doltmodel.New()
	s0, _ := m.Size()
	write(t, m, observe(edge1, 1, "a", 10, 0))
	s1, _ := m.Size()
	if s0 != 0 || s1 <= s0 {
		t.Fatalf("sizes %d, %d", s0, s1)
	}
}

// TestAPayloadOverTheCapacityIsACapabilityRow: a record whose payload does not fit the column is
// refused whole with ErrInvalid and counted, which G0 requires to be zero on a registered input;
// one that fits is not counted, and without a capacity nothing is refused.
func TestAPayloadOverTheCapacityIsACapabilityRow(t *testing.T) {
	m := doltmodel.New(doltmodel.WithPayloadCapacity(doltmodel.RegisteredPayloadCapacity))
	fits, over := observe(edge1, 1, "a", 10, 0), observe(edge1, 2, "a", 11, 0)
	fits.Payload = make([]byte, doltmodel.RegisteredPayloadCapacity)
	over.Payload = make([]byte, doltmodel.RegisteredPayloadCapacity+1)
	write(t, m, fits)
	if c := m.Counts(); c.PayloadRefusals != 0 || c.Records != 1 {
		t.Fatalf("counts %+v", c)
	}
	if err := m.Write([]engine.Record{over}); !errors.Is(err, engine.ErrInvalid) {
		t.Fatalf("a payload over the capacity: %v", err)
	}
	if c := m.Counts(); c.PayloadRefusals != 1 || c.Records != 1 {
		t.Fatalf("counts %+v", c)
	}
	unlimited := doltmodel.New()
	write(t, unlimited, over)
}
