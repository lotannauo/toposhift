package storetest

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"slices"
	"time"

	"github.com/lotannauo/toposhift/internal/catalog"
	"github.com/lotannauo/toposhift/internal/identity"
	"github.com/lotannauo/toposhift/internal/lifecycle"
	"github.com/lotannauo/toposhift/internal/store"
)

// readKinds are the five reads of the contract, in the order the checks ask them.
var readKinds = []string{"Neighbors", "NeighborsBatch", "Alive", "Window", "EntityWindow"}

// readArgs are the arguments of one read, whichever kind it is.
type readArgs struct {
	ctx context.Context
	fp  identity.Fingerprint   // the entity (Neighbors, Alive, Window, EntityWindow)
	fps []identity.Fingerprint // the entities of NeighborsBatch
	dir store.Direction        // Neighbors, NeighborsBatch and Window
	t   time.Time              // the instant, or a window's from
	to  time.Time              // a window's end
	sc  store.Scope
}

// answer is what a read returned, whichever kind it is.
type answer struct {
	neighbors []store.Neighbor
	batch     [][]store.Neighbor
	alive     bool
	records   []store.Record
}

// ask puts one kind of read to a store.
func ask(s store.Store, kind string, a readArgs) (answer, error) {
	var out answer
	var err error
	switch kind {
	case "Neighbors":
		out.neighbors, err = s.Neighbors(a.ctx, a.fp, a.dir, a.t, a.sc)
	case "NeighborsBatch":
		out.batch, err = s.NeighborsBatch(a.ctx, a.fps, a.dir, a.t, a.sc)
	case "Alive":
		out.alive, err = s.Alive(a.ctx, a.fp, a.t, a.sc)
	case "Window":
		out.records, err = s.Window(a.ctx, a.fp, a.dir, a.t, a.to, a.sc)
	case "EntityWindow":
		out.records, err = s.EntityWindow(a.ctx, a.fp, a.t, a.to, a.sc)
	default:
		panic("storetest: unknown kind of read " + kind)
	}
	return out, err
}

func (a answer) empty() bool {
	if len(a.neighbors) > 0 || len(a.records) > 0 || a.alive {
		return false
	}
	for _, ns := range a.batch {
		if len(ns) > 0 {
			return false
		}
	}
	return true
}

// diff says how two answers differ, or returns "".
func (a answer) diff(b answer) string {
	switch {
	case a.alive != b.alive:
		return fmt.Sprintf("alive %v, want %v", a.alive, b.alive)
	case !slices.Equal(a.neighbors, b.neighbors):
		return fmt.Sprintf("neighbors %v, want %v", a.neighbors, b.neighbors)
	case len(a.batch) != len(b.batch):
		return fmt.Sprintf("%d batch answers, want %d", len(a.batch), len(b.batch))
	}
	for i := range a.batch {
		if !slices.Equal(a.batch[i], b.batch[i]) {
			return fmt.Sprintf("batch answer %d is %v, want %v", i, a.batch[i], b.batch[i])
		}
	}
	return diffRecords(a.records, b.records)
}

// fixture is the small store the read, close, context and horizon checks start
// from: a pod placed on a node, and the existence of both.
type fixture struct {
	pod, node identity.Fingerprint
	x         time.Time
	recs      []store.Record // seq 1 to 3
}

func newFixture() (fixture, error) {
	pod, err := entityFP(catalog.K8sPod, catalog.K8sPodUID, "contract-pod")
	if err != nil {
		return fixture{}, err
	}
	node, err := entityFP(catalog.K8sNode, catalog.K8sNodeUID, "contract-node")
	if err != nil {
		return fixture{}, err
	}
	f := fixture{pod: pod, node: node, x: time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)}
	f.recs = []store.Record{
		f.edge(1, f.x, "edge payload"),
		f.entity(2, pod, f.x, "pod payload"),
		f.entity(3, node, f.x.Add(time.Minute), "node payload"),
	}
	return f, nil
}

func (f fixture) edge(seq uint64, at time.Time, payload string) store.Record {
	return store.Record{
		Layer: catalog.L2, Subject: store.EdgeSubject(f.pod, f.node, catalog.ScheduledOn), Producer: "alpha",
		EventTime: at, Seq: seq, Kind: lifecycle.Observe, Payload: []byte(payload),
	}
}

func (f fixture) entity(seq uint64, fp identity.Fingerprint, at time.Time, payload string) store.Record {
	return store.Record{
		Layer: catalog.L2, Subject: store.EntitySubject(fp), Producer: "alpha", EventTime: at, Seq: seq,
		Kind: lifecycle.Observe, Payload: []byte(payload),
	}
}

// args are valid arguments for any read that sees the fixture's records.
func (f fixture) args(ctx context.Context) readArgs {
	return readArgs{
		ctx: ctx, fp: f.pod, fps: []identity.Fingerprint{f.pod, f.node}, dir: store.Forward,
		t: f.x, to: f.x.Add(time.Hour), sc: store.Current(catalog.L2),
	}
}

func wantIs(what string, err, target error) error {
	if errors.Is(err, target) {
		return nil
	}
	return fmt.Errorf("%s: want an error wrapping %w, got %w", what, target, orNil(err))
}

// wantInvalid wants an error that wraps ErrInvalid but not ErrBeforeHorizon (which
// wraps ErrInvalid as well).
func wantInvalid(what string, err error) error {
	if errors.Is(err, store.ErrInvalid) && !errors.Is(err, store.ErrBeforeHorizon) {
		return nil
	}
	return fmt.Errorf("%s: want an error wrapping ErrInvalid but not ErrBeforeHorizon, got %w", what, orNil(err))
}

func describeArgs(kind string, a readArgs) string {
	switch kind {
	case "Window", "EntityWindow":
		return fmt.Sprintf("%s(%s, %s, [%s, %s), %s)", kind, a.fp, a.dir, a.t.Format(time.RFC3339Nano), a.to.Format(time.RFC3339Nano), scopeLabel(a.sc))
	case "NeighborsBatch":
		return fmt.Sprintf("%s(%d fingerprints, %s, %s, %s)", kind, len(a.fps), a.dir, a.t.Format(time.RFC3339Nano), scopeLabel(a.sc))
	}
	return fmt.Sprintf("%s(%s, %s, %s, %s)", kind, a.fp, a.dir, a.t.Format(time.RFC3339Nano), scopeLabel(a.sc))
}

// CheckWriteContract checks that a store refuses records out of sequence and
// records its validation rules say are invalid, that a refused batch is refused
// whole (its valid records are not stored and their sequence numbers are not
// consumed), that an empty batch changes nothing, that the store and the caller
// share no payload bytes, and that the retention horizon only moves forward and
// refuses what is before it. It leaves the store with a retention horizon, so use
// a fresh store opened with the zero policy.
func CheckWriteContract(s store.Store) error {
	g, err := NewGenerator(Tiny())
	if err != nil {
		return err
	}
	recs := g.Batch(12)
	if s.LastSeq() != 0 {
		return fmt.Errorf("a new store has LastSeq %d, want 0", s.LastSeq())
	}
	if h := s.Horizon(); !h.IsZero() {
		return fmt.Errorf("a new store has Horizon %v, want the zero horizon", h)
	}
	if err := s.Write(bg, cloneRecords(recs[:10])); err != nil {
		return fmt.Errorf("a valid batch was refused: %w", err)
	}
	if got, want := s.LastSeq(), recs[9].Seq; got != want {
		return fmt.Errorf("LastSeq = %d after a batch ending at seq %d", got, want)
	}
	if err := wantIs("a repeated seq", s.Write(bg, cloneRecords(recs[:1])), store.ErrInvalid); err != nil {
		return err
	}

	// An empty batch is a no-op.
	for _, empty := range [][]store.Record{nil, {}} {
		if err := s.Write(bg, empty); err != nil {
			return fmt.Errorf("an empty batch must return nil, got %w", err)
		}
	}
	if got, want := s.LastSeq(), recs[9].Seq; got != want {
		return fmt.Errorf("an empty batch moved LastSeq to %d, want %d", got, want)
	}

	good, next := recs[10], recs[11]
	// Sequence numbers: 0 and Latest name no record, and they ascend within a
	// batch.
	for name, batch := range map[string][]store.Record{
		"seq 0":                          {withSeq(good, 0)},
		"seq Latest":                     {withSeq(good, store.Latest)},
		"valid records, then seq Latest": {good, withSeq(next, store.Latest)},
		"descending seqs":                {next, good},
		"a repeated seq in-batch":        {good, good},
	} {
		if err := wantIs("a batch with "+name, s.Write(bg, cloneRecords(batch)), store.ErrInvalid); err != nil {
			return err
		}
	}
	// A batch that is refused is refused whole: its valid records are not stored.
	if got, want := s.LastSeq(), recs[9].Seq; got != want {
		return fmt.Errorf("refused batches moved LastSeq to %d, want %d", got, want)
	}
	held, err := holds(s, good)
	if err != nil {
		return fmt.Errorf("reading back a record: %w", err)
	}
	if held {
		return errors.New("a batch refused for a seq equal to Latest left its valid record readable")
	}

	host, err := entityFP(catalog.Host, catalog.HostID, "write-contract-host")
	if err != nil {
		return err
	}
	rack, err := entityFP(catalog.Rack, catalog.RackID, "write-contract-rack")
	if err != nil {
		return err
	}
	pod, err := entityFP(catalog.K8sPod, catalog.K8sPodUID, "write-contract-pod")
	if err != nil {
		return err
	}
	node, err := entityFP(catalog.K8sNode, catalog.K8sNodeUID, "write-contract-node")
	if err != nil {
		return err
	}
	hostObserve := func(r store.Record) store.Record {
		return store.Record{
			Layer: entityLayer(catalog.Host), Subject: store.EntitySubject(host), Producer: "p", EventTime: r.EventTime, Seq: r.Seq,
			Kind: lifecycle.Observe, Payload: []byte("host"),
		}
	}
	broken := map[string]func(store.Record) store.Record{
		"an event time before 1970": func(r store.Record) store.Record {
			r.EventTime = time.Date(1969, 1, 1, 0, 0, 0, 0, time.UTC)
			return r
		},
		"an empty producer": func(r store.Record) store.Record { r.Producer = ""; return r },
		"an unset layer":    func(r store.Record) store.Record { r.Layer = 0; return r },
		"an unset kind":     func(r store.Record) store.Record { r.Kind = 0; return r },
		"an entity in a layer other than its type's": func(r store.Record) store.Record {
			// Any entity will do: its layer is moved to a different, valid one.
			r.Subject = store.EntitySubject(r.Subject.A)
			r.Layer = entityLayer(r.Subject.A.Type())%catalog.L3 + 1
			r.TTL, r.Through, r.Payload, r.Boot = 0, time.Time{}, nil, ""
			r.Kind = lifecycle.Observe
			return r
		},
		"a delete with a TTL": func(r store.Record) store.Record {
			r.Kind, r.Payload, r.TTL, r.Boot = lifecycle.Delete, nil, time.Minute, ""
			return r
		},
		"a delete with a payload": func(r store.Record) store.Record {
			r.Kind, r.Payload, r.TTL, r.Boot = lifecycle.Delete, []byte("x"), 0, ""
			return r
		},
		"a boot id on a delete": func(r store.Record) store.Record {
			r = hostObserve(r)
			r.Kind, r.Payload, r.Boot = lifecycle.Delete, nil, "boot-a"
			return r
		},
		"a boot id on an edge record": func(r store.Record) store.Record {
			return store.Record{
				Layer: catalog.L1, Subject: store.EdgeSubject(host, rack, catalog.LocatedIn), Producer: "p", EventTime: r.EventTime, Seq: r.Seq,
				Kind: lifecycle.Observe, Payload: []byte("edge"), Boot: "boot-a",
			}
		},
		"a blank boot id": func(r store.Record) store.Record {
			r = hostObserve(r)
			r.Boot = "  \t"
			return r
		},
		// The kind 3 is reserved for an operator purge, whose meaning is not
		// decided: it is refused, as is a basis no layout has a place for.
		"a reserved kind": func(r store.Record) store.Record { r.Kind = 3; return r },
		"an undefined event time basis": func(r store.Record) store.Record {
			r.EventTimeBasis = store.BasisProducerEvent + 1
			return r
		},
		"an event time basis of 255": func(r store.Record) store.Record { r.EventTimeBasis = 255; return r },
	}
	names := make([]string, 0, len(broken))
	for name := range broken {
		names = append(names, name)
	}
	slices.Sort(names)
	for _, name := range names {
		bad := broken[name](next)
		// A valid record followed by an invalid one: the whole batch is refused.
		if err := wantInvalid("a batch with "+name, s.Write(bg, cloneRecords([]store.Record{good, bad}))); err != nil {
			return err
		}
	}
	if got, want := s.LastSeq(), recs[9].Seq; got != want {
		return fmt.Errorf("refused batches moved LastSeq to %d, want %d", got, want)
	}
	// If any refused batch had stored its valid record, that record's sequence
	// number would now be used and this would fail.
	if err := s.Write(bg, cloneRecords([]store.Record{good})); err != nil {
		return fmt.Errorf("a refused batch left its valid record behind: %w", err)
	}

	// The store and the caller share no payload bytes: not the ones Write was
	// given, and not the ones a read returned.
	start := recs[0].EventTime
	edge := store.Record{
		Layer: catalog.L2, Subject: store.EdgeSubject(pod, node, catalog.ScheduledOn), Producer: "alias", EventTime: start.Add(30 * time.Minute),
		Seq: good.Seq + 1, Kind: lifecycle.Observe, Payload: []byte("edge payload 0123456789"),
	}
	own := store.Record{
		Layer: catalog.L2, Subject: store.EntitySubject(pod), Producer: "alias", EventTime: start.Add(30 * time.Minute),
		Seq: good.Seq + 2, Kind: lifecycle.Observe, Payload: []byte("entity payload 0123456789"),
	}
	pristine := []store.Record{edge, own}
	given := cloneRecords(pristine)
	if err := s.Write(bg, given); err != nil {
		return fmt.Errorf("a valid batch was refused: %w", err)
	}
	for i := range given {
		if !bytes.Equal(given[i].Payload, pristine[i].Payload) {
			return fmt.Errorf("the Write altered the bytes of a payload it was given: %q, was %q", given[i].Payload, pristine[i].Payload)
		}
	}
	scribble(given)
	if err := checkAlias(s, pod, pristine, "after the caller overwrote the bytes it had passed to Write"); err != nil {
		return err
	}
	for range 2 {
		for _, kind := range []string{"Window", "EntityWindow"} {
			got, err := ask(s, kind, readArgs{ctx: bg, fp: pod, dir: store.Forward, t: store.MinEventTime, to: store.MaxEventTime, sc: store.Current(catalog.L2)})
			if err != nil {
				return fmt.Errorf("%s: %w", kind, err)
			}
			scribble(got.records)
		}
		if err := checkAlias(s, pod, pristine, "after the caller overwrote the bytes a read returned"); err != nil {
			return err
		}
	}
	last, err := checkBasisKept(s, pod, node, start.Add(35*time.Minute), own.Seq)
	if err != nil {
		return err
	}

	// The horizon only moves forward: a later, earlier-dated Retain must not
	// bring back the window between the two.
	if err := s.Retain(bg, start.Add(2*time.Hour)); err != nil {
		return fmt.Errorf("retaining: %w", err)
	}
	if err := s.Retain(bg, start.Add(time.Hour)); err != nil {
		return fmt.Errorf("an earlier Retain must be accepted and ignored: %w", err)
	}
	record := func(seq uint64, at time.Time) store.Record {
		r := edge
		r.Seq, r.EventTime = seq, at
		return r
	}
	if err := wantIs("after Retain(+2h) then Retain(+1h), a record at +90m", s.Write(bg, []store.Record{record(last+1000, start.Add(90*time.Minute))}), store.ErrBeforeHorizon); err != nil {
		return err
	}
	if got := s.LastSeq(); got != last {
		return fmt.Errorf("a retention or a refused record moved LastSeq to %d, want %d", got, last)
	}

	// A batch that mixes a record before the horizon with valid ones is refused
	// whole: the valid ones are not stored and their sequence numbers stay
	// unused. The horizon is checked against store state, so a store can notice it
	// only after it has already started on the valid records.
	fresh, stale, later := record(last+1, start.Add(3*time.Hour)), record(last+2, start.Add(90*time.Minute)), record(last+3, start.Add(4*time.Hour))
	if err := wantIs("a batch with a record before the horizon", s.Write(bg, cloneRecords([]store.Record{fresh, stale, later})), store.ErrBeforeHorizon); err != nil {
		return err
	}
	if got := s.LastSeq(); got != last {
		return fmt.Errorf("a refused batch moved LastSeq to %d, want %d", got, last)
	}
	if err := s.Write(bg, cloneRecords([]store.Record{fresh, later})); err != nil {
		return fmt.Errorf("a refused batch left some of its records behind: %w", err)
	}
	if got := s.LastSeq(); got != later.Seq {
		return fmt.Errorf("LastSeq = %d after the batch ending at seq %d", got, later.Seq)
	}

	// Latest names no record, but the seq just below it is an ordinary one: it is
	// accepted, and then nothing can be written above it.
	top := record(store.Latest-1, start.Add(5*time.Hour))
	if err := s.Write(bg, cloneRecords([]store.Record{top})); err != nil {
		return fmt.Errorf("a record with seq Latest-1 was refused: %w", err)
	}
	if got := s.LastSeq(); got != top.Seq {
		return fmt.Errorf("LastSeq = %d after a record with seq Latest-1, want %d", got, top.Seq)
	}
	held, err = holds(s, top)
	if err != nil {
		return fmt.Errorf("reading back a record: %w", err)
	}
	if !held {
		return errors.New("a record with seq Latest-1 was accepted but is not readable")
	}
	if err := wantIs("a record with seq Latest, after Latest-1", s.Write(bg, cloneRecords([]store.Record{record(store.Latest, start.Add(6*time.Hour))})), store.ErrInvalid); err != nil {
		return err
	}
	if got := s.LastSeq(); got != top.Seq {
		return fmt.Errorf("a refused record moved LastSeq to %d, want %d", got, top.Seq)
	}
	return nil
}

// checkBasisKept writes, for each valid event time basis, an observation and a
// delete of the pod and of its edge to the node, with seqs after last, and
// requires one read of each (EntityWindow for the pod, Window for the edge) to
// return it with the basis it was written with. It
// returns the last seq it used. Its records are at or after from, and span less
// than an hour.
func checkBasisKept(s store.Store, pod, node identity.Fingerprint, from time.Time, last uint64) (uint64, error) {
	var batch []store.Record
	seq := last
	for i, basis := range []store.EventTimeBasis{store.BasisUnknown, store.BasisObjectField, store.BasisObserved, store.BasisReceipt, store.BasisProducerEvent} {
		at := from.Add(time.Duration(i) * 10 * time.Minute)
		mk := func(step int, sub store.Subject, layer catalog.Layer, kind lifecycle.Kind, payload []byte) store.Record {
			seq++
			return store.Record{
				Layer: layer, Subject: sub, Producer: "basis", EventTime: at.Add(time.Duration(step) * time.Minute),
				Seq: seq, Kind: kind, Payload: payload, EventTimeBasis: basis,
			}
		}
		entity, edge := store.EntitySubject(pod), store.EdgeSubject(pod, node, catalog.ScheduledOn)
		batch = append(batch,
			mk(0, entity, catalog.L2, lifecycle.Observe, []byte("basis entity")),
			mk(1, edge, catalog.L2, lifecycle.Observe, []byte("basis edge")),
			mk(2, entity, catalog.L2, lifecycle.Delete, nil),
			mk(3, edge, catalog.L2, lifecycle.Delete, nil),
		)
	}
	if err := s.Write(bg, cloneRecords(batch)); err != nil {
		return 0, fmt.Errorf("records with each event time basis were refused: %w", err)
	}
	for _, r := range batch {
		a := readArgs{ctx: bg, fp: r.Subject.A, dir: store.Forward, t: r.EventTime, to: r.EventTime.Add(time.Nanosecond), sc: store.Current(r.Layer)}
		kind := "Window"
		if r.Subject.Kind == store.SubjectEntity {
			kind = "EntityWindow"
		}
		got, err := ask(s, kind, a)
		if err != nil {
			return 0, fmt.Errorf("%s: %w", kind, err)
		}
		i := slices.IndexFunc(got.records, func(x store.Record) bool { return x.Seq == r.Seq })
		switch {
		case i < 0:
			return 0, fmt.Errorf("%w: %s did not return the %s of seq %d, written with event time basis %s", ErrMismatch, kind, r.Kind, r.Seq, r.EventTimeBasis)
		case got.records[i].EventTimeBasis != r.EventTimeBasis:
			return 0, fmt.Errorf("%w: %s returned event time basis %s for seq %d, written with %s", ErrMismatch, kind, got.records[i].EventTimeBasis, r.Seq, r.EventTimeBasis)
		}
	}
	return seq, nil
}

// holds reports whether the store returns a record, under the latest token, by the
// read that returns records of its subject.
func holds(s store.Store, r store.Record) (bool, error) {
	a := readArgs{ctx: bg, fp: r.Subject.A, dir: store.Forward, t: r.EventTime, to: r.EventTime.Add(time.Nanosecond), sc: store.Current(r.Layer)}
	kind := "Window"
	if r.Subject.Kind == store.SubjectEntity {
		kind = "EntityWindow"
	}
	got, err := ask(s, kind, a)
	if err != nil {
		return false, err
	}
	return slices.ContainsFunc(got.records, func(x store.Record) bool { return x.Seq == r.Seq }), nil
}

func withSeq(r store.Record, seq uint64) store.Record {
	r.Seq = seq
	return r
}

// checkAlias reads back the pod's own record and the edge record written by
// CheckWriteContract and requires their payloads to be what was written.
func checkAlias(s store.Store, pod identity.Fingerprint, want []store.Record, when string) error {
	for i, kind := range []string{"Window", "EntityWindow"} {
		got, err := ask(s, kind, readArgs{ctx: bg, fp: pod, dir: store.Forward, t: store.MinEventTime, to: store.MaxEventTime, sc: store.Current(catalog.L2)})
		if err != nil {
			return fmt.Errorf("%s: %w", kind, err)
		}
		if len(got.records) != 1 || !bytes.Equal(got.records[0].Payload, want[i].Payload) {
			return fmt.Errorf("%s %s returned %+v; want the one record with payload %q", kind, when, got.records, want[i].Payload)
		}
	}
	return nil
}

// CheckReadContract checks the arguments of a read: a scope that names no layer,
// a direction that is neither forward nor reverse and the zero fingerprint are
// refused with ErrInvalid, by every read; that a batch with a zero fingerprint
// among valid ones fails whole, with a nil result; that empty input has an empty
// answer; that a window with from at or after to is empty and not an error; that a
// token above LastSeq that is not Latest is answered as LastSeq is; and that,
// while nothing has been retained, token 0 and any instant, however far outside
// the range of event times, is answered. Use a fresh store opened with the zero
// policy.
func CheckReadContract(s store.Store) error {
	f, err := newFixture()
	if err != nil {
		return err
	}
	if err := s.Write(bg, cloneRecords(f.recs)); err != nil {
		return fmt.Errorf("a valid batch was refused: %w", err)
	}
	valid := f.args(bg)
	want := map[string]answer{}
	for _, kind := range readKinds {
		a, err := ask(s, kind, valid)
		if err != nil {
			return fmt.Errorf("%s: %w", describeArgs(kind, valid), err)
		}
		if a.empty() {
			return fmt.Errorf("%s answered nothing, though the store holds records it should see", describeArgs(kind, valid))
		}
		want[kind] = a
	}

	// Scopes that name no layer.
	for _, sc := range []store.Scope{{}, {AsOf: store.Latest}, {Layer: catalog.L3 + 1, AsOf: store.Latest}} {
		a := valid
		a.sc = sc
		for _, kind := range readKinds {
			got, err := ask(s, kind, a)
			if err := wantInvalid(fmt.Sprintf("reading %s with scope %+v", kind, sc), err); err != nil {
				return err
			}
			if kind == "NeighborsBatch" && got.batch != nil {
				return fmt.Errorf("NeighborsBatch with scope %+v failed but returned %v; a failed batch returns a nil result", sc, got.batch)
			}
		}
	}
	// Directions that are neither forward nor reverse.
	for _, dir := range []store.Direction{0, 3, 255} {
		a := valid
		a.dir = dir
		for _, kind := range []string{"Neighbors", "NeighborsBatch", "Window"} {
			got, err := ask(s, kind, a)
			if err := wantInvalid(fmt.Sprintf("reading %s with direction %s", kind, dir), err); err != nil {
				return err
			}
			if kind == "NeighborsBatch" && got.batch != nil {
				return fmt.Errorf("NeighborsBatch with direction %s failed but returned %v; a failed batch returns a nil result", dir, got.batch)
			}
		}
	}
	// The zero fingerprint.
	a := valid
	a.fp, a.fps = identity.Fingerprint{}, []identity.Fingerprint{{}}
	for _, kind := range readKinds {
		_, err := ask(s, kind, a)
		if err := wantInvalid(fmt.Sprintf("reading %s of the zero fingerprint", kind), err); err != nil {
			return err
		}
	}
	for _, fps := range [][]identity.Fingerprint{
		{{}, f.pod}, {f.pod, {}}, {f.pod, {}, f.node}, {f.pod, f.node, f.pod, {}},
	} {
		a := valid
		a.fps = fps
		got, err := ask(s, "NeighborsBatch", a)
		if err := wantInvalid(fmt.Sprintf("NeighborsBatch with a zero fingerprint among %d", len(fps)), err); err != nil {
			return err
		}
		if got.batch != nil {
			return fmt.Errorf("NeighborsBatch with a zero fingerprint among %d failed but returned %v; the whole call fails with a nil result", len(fps), got.batch)
		}
	}

	// Empty input has an empty answer.
	for _, fps := range [][]identity.Fingerprint{nil, {}} {
		a := valid
		a.fps = fps
		got, err := ask(s, "NeighborsBatch", a)
		if err != nil {
			return fmt.Errorf("NeighborsBatch of %d fingerprints must have an empty answer, got error %w", len(fps), err)
		}
		if len(got.batch) != 0 {
			return fmt.Errorf("NeighborsBatch of %d fingerprints = %v; want an empty answer", len(fps), got.batch)
		}
	}
	// From at or after to is not an error, and has no records.
	for _, kind := range []string{"Window", "EntityWindow"} {
		for name, iv := range map[string]interval{
			"from == to": {f.x, f.x},
			"from > to":  {f.x.Add(time.Hour), f.x},
			"from far past to": {
				store.MaxEventTime.Add(time.Hour), store.MinEventTime,
			},
		} {
			a := valid
			a.t, a.to = iv.from, iv.to
			got, err := ask(s, kind, a)
			if err != nil {
				return fmt.Errorf("%s with %s must have an empty answer, got error %w", describeArgs(kind, a), name, err)
			}
			if len(got.records) != 0 {
				return fmt.Errorf("%s with %s = %+v; want an empty answer", describeArgs(kind, a), name, got.records)
			}
		}
	}

	// A token above LastSeq that is not Latest reads as LastSeq.
	for _, tok := range []uint64{s.LastSeq() + 1, s.LastSeq() + 1000, 1 << 63, store.Latest - 1} {
		a := valid
		a.sc.AsOf = tok
		for _, kind := range readKinds {
			got, err := ask(s, kind, a)
			if err != nil {
				return fmt.Errorf("%s: a token above LastSeq that is not Latest must be answered: %w", describeArgs(kind, a), err)
			}
			if msg := got.diff(want[kind]); msg != "" {
				return fmt.Errorf("%s: a token above LastSeq must be answered as Latest is: %s", describeArgs(kind, a), msg)
			}
		}
	}

	// While nothing is retained, no token is too low and no instant too early.
	a = valid
	a.sc.AsOf = 0
	for _, kind := range readKinds {
		got, err := ask(s, kind, a)
		if err != nil {
			return fmt.Errorf("%s: with no retention, token 0 must be answered: %w", describeArgs(kind, a), err)
		}
		if !got.empty() {
			return fmt.Errorf("%s: token 0 sees nothing, got %+v", describeArgs(kind, a), got)
		}
	}
	// Any instant may be asked about, however far outside the range of event
	// times.
	for _, t := range []time.Time{{}, time.Date(9999, 12, 31, 0, 0, 0, 0, time.UTC), store.MinEventTime.Add(-time.Nanosecond), store.MaxEventTime.Add(time.Nanosecond)} {
		a := valid
		a.t, a.to = t, t.Add(time.Hour)
		for _, kind := range readKinds {
			if _, err := ask(s, kind, a); err != nil {
				return fmt.Errorf("%s: with no retention, any instant must be answered: %w", describeArgs(kind, a), err)
			}
		}
	}
	return nil
}

// CheckClose checks what Close does: afterwards every method that returns an error
// returns one wrapping store.ErrClosed, even for an empty batch, an invalid scope
// or an instant before the horizon; LastSeq and Horizon keep the values they had;
// a second Close returns nil. It closes the store it is given. Use a fresh store
// opened with the zero policy.
func CheckClose(s store.Store) error {
	f, err := newFixture()
	if err != nil {
		return err
	}
	if err := s.Write(bg, cloneRecords(f.recs)); err != nil {
		return fmt.Errorf("a valid batch was refused: %w", err)
	}
	if err := s.Retain(bg, f.x); err != nil {
		return fmt.Errorf("retaining: %w", err)
	}
	lastSeq, horizon := s.LastSeq(), s.Horizon()
	if lastSeq != 3 || horizon.IsZero() {
		return fmt.Errorf("before Close, LastSeq = %d and Horizon = %v; want 3 and a horizon", lastSeq, horizon)
	}
	if err := s.Close(); err != nil {
		return fmt.Errorf("closing: %w", err)
	}

	closed := func(what string, err error) error { return wantIs(what+" after Close", err, store.ErrClosed) }
	if err := closed("a Write", s.Write(bg, []store.Record{f.edge(4, f.x.Add(time.Hour), "late")})); err != nil {
		return err
	}
	if err := closed("an empty Write", s.Write(bg, nil)); err != nil {
		return err
	}
	if err := closed("an empty batch Write", s.Write(bg, []store.Record{})); err != nil {
		return err
	}
	if err := closed("Retain", s.Retain(bg, f.x.Add(time.Hour))); err != nil {
		return err
	}
	valid := f.args(bg)
	check := func(what string, mod func(*readArgs), kinds ...string) error {
		a := valid
		mod(&a)
		for _, kind := range kinds {
			got, err := ask(s, kind, a)
			if err := closed(fmt.Sprintf("%s with %s", kind, what), err); err != nil {
				return err
			}
			if kind == "NeighborsBatch" && got.batch != nil {
				return fmt.Errorf("NeighborsBatch with %s after Close failed but returned %v; want a nil result", what, got.batch)
			}
		}
		return nil
	}
	for _, c := range []struct {
		what  string
		mod   func(*readArgs)
		kinds []string
	}{
		{"valid arguments", func(*readArgs) {}, readKinds},
		{"an invalid scope", func(a *readArgs) { a.sc = store.Scope{} }, readKinds},
		{"an invalid direction", func(a *readArgs) { a.dir = 0 }, []string{"Neighbors", "NeighborsBatch", "Window"}},
		{"the zero fingerprint", func(a *readArgs) { a.fp, a.fps = identity.Fingerprint{}, []identity.Fingerprint{{}} }, readKinds},
		{"no fingerprints", func(a *readArgs) { a.fps = nil }, []string{"NeighborsBatch"}},
		{"an instant before the horizon", func(a *readArgs) { a.t, a.to = f.x.Add(-time.Hour), f.x }, readKinds},
		{"a token below the horizon", func(a *readArgs) { a.sc.AsOf = 0 }, readKinds},
	} {
		if err := check(c.what, c.mod, c.kinds...); err != nil {
			return err
		}
	}
	if got := s.LastSeq(); got != lastSeq {
		return fmt.Errorf("LastSeq() = %d after Close; it was %d", got, lastSeq)
	}
	if got := s.Horizon(); !sameHorizon(got, horizon) {
		return fmt.Errorf("Horizon() = %v after Close; it was %v", got, horizon)
	}
	for i := range 2 {
		if err := s.Close(); err != nil {
			return fmt.Errorf("the Close number %d after the first must return nil, got %w", i+2, err)
		}
	}
	return nil
}

// CheckContext checks that a cancelled context, and one whose deadline has passed,
// makes every read fail with an error that wraps the context's. A Write or a
// Retain is held to less, because the contract says no more: a Write that returns a
// context error has stored nothing, and a Retain has no context rule, so a backend
// may commit without consulting the context. If either returns an error it must
// wrap the context's, a failed Write must leave LastSeq and the data as they were
// (so the same sequence number is accepted afterwards), and a failed Retain must
// leave the horizon unmoved or moved whole; if it succeeds, the effect must be
// whole. The order of the context check relative to the argument and horizon
// checks is not tested; the contract does not give it. Use a fresh store opened
// with the zero policy.
func CheckContext(s store.Store) error {
	f, err := newFixture()
	if err != nil {
		return err
	}
	if err := s.Write(bg, cloneRecords(f.recs)); err != nil {
		return fmt.Errorf("a valid batch was refused: %w", err)
	}
	cancelled, cancel := context.WithCancel(bg)
	cancel()
	expired, release := context.WithDeadline(bg, time.Now().Add(-time.Minute))
	defer release()
	contexts := []struct {
		name string
		ctx  context.Context
		want error
	}{
		{"cancelled", cancelled, context.Canceled},
		{"expired", expired, context.DeadlineExceeded},
	}
	for _, c := range contexts {
		a := f.args(c.ctx)
		for _, kind := range readKinds {
			if _, err := ask(s, kind, a); !errors.Is(err, c.want) {
				return fmt.Errorf("%s with a %s context: want an error wrapping %w, got %w", describeArgs(kind, a), c.name, c.want, orNil(err))
			}
		}
	}

	// The contract promises a context error only for reads. For Write it says that
	// if a context error is returned nothing was stored, and Retain has no context
	// rule, so a backend may commit without consulting the context at all. What a
	// failure may not do is claim the context while it leaves something behind.
	for _, c := range contexts {
		seq := s.LastSeq() + 1
		next := f.edge(seq, f.x.Add(time.Duration(seq)*time.Minute), "next")
		err := s.Write(c.ctx, cloneRecords([]store.Record{next}))
		switch {
		case err == nil:
			// Committed without noticing the context: the batch must be whole.
			if got := s.LastSeq(); got != seq {
				return fmt.Errorf("a Write with a %s context returned no error, and LastSeq = %d; want %d", c.name, got, seq)
			}
			held, err := holds(s, next)
			if err != nil {
				return fmt.Errorf("reading back a record: %w", err)
			}
			if !held {
				return fmt.Errorf("a Write with a %s context returned no error and raised LastSeq to %d, but the record is not readable", c.name, seq)
			}
		case !errors.Is(err, c.want):
			return fmt.Errorf("a Write with a %s context failed with an error that does not wrap %w: %w", c.name, c.want, err)
		default:
			if got := s.LastSeq(); got != seq-1 {
				return fmt.Errorf("a Write with a %s context returned its error, and LastSeq = %d; want %d: a context error means nothing was stored", c.name, got, seq-1)
			}
			held, err := holds(s, next)
			if err != nil {
				return fmt.Errorf("reading back a record: %w", err)
			}
			if held {
				return fmt.Errorf("a Write with a %s context returned its error, and the record is readable: a context error means nothing was stored", c.name)
			}
		}
	}
	// A sequence number a failed Write left unused is accepted now.
	seq := s.LastSeq() + 1
	if err := s.Write(bg, cloneRecords([]store.Record{f.edge(seq, f.x.Add(time.Duration(seq)*time.Minute), "next")})); err != nil {
		return fmt.Errorf("the batch after the Writes with a done context was refused with a live context: %w", err)
	}
	if got := s.LastSeq(); got != seq {
		return fmt.Errorf("LastSeq = %d after the batch ending at seq %d", got, seq)
	}

	h := f.x.Add(30 * time.Second)
	whole := store.Horizon{Time: h, Seq: s.LastSeq()}
	for _, c := range contexts {
		err := s.Retain(c.ctx, h)
		if err != nil && !errors.Is(err, c.want) {
			return fmt.Errorf("a Retain with a %s context failed with an error that does not wrap %w: %w", c.name, c.want, err)
		}
		// A Retain that fails has moved the horizon whole or not at all, and one
		// that does not has moved it whole.
		got := s.Horizon()
		if (err != nil && !got.IsZero() && !sameHorizon(got, whole)) || (err == nil && !sameHorizon(got, whole)) {
			return fmt.Errorf("after a Retain with a %s context (it failed: %t), Horizon() = %v; want it moved whole, {%s, %d}%s",
				c.name, err != nil, got, h.Format(time.RFC3339Nano), whole.Seq, map[bool]string{true: " or unmoved", false: ""}[err != nil])
		}
	}
	if err := s.Retain(bg, h); err != nil {
		return fmt.Errorf("retaining with a live context: %w", err)
	}
	if got := s.Horizon(); !sameHorizon(got, whole) {
		return fmt.Errorf("after Retain(%s), Horizon() = %v; want {%s, %d}", h.Format(time.RFC3339Nano), got, h.Format(time.RFC3339Nano), whole.Seq)
	}
	if got := s.LastSeq(); got != whole.Seq {
		return fmt.Errorf("the Retain moved LastSeq to %d, want %d", got, whole.Seq)
	}
	return nil
}

// CheckHorizon checks the rules of the retention horizon: nothing is refused until
// the first Retain that moves it; then an instant (or a window's from) one
// nanosecond before it, or a token one below its Seq, is refused with
// store.ErrBeforeHorizon, and the instant and token themselves are answered, as
// are higher ones; arguments are checked before the horizon; a write before it is
// refused and one at it is not; a Retain that does not move the horizon changes
// nothing and does not raise Seq; and a Retain that does publishes the LastSeq at
// the call, whatever zone the instant is given in. Sequence numbers cross 2^32.
// Use a fresh store opened with the zero policy.
func CheckHorizon(s store.Store) error {
	f, err := newFixture()
	if err != nil {
		return err
	}
	x := f.x
	n0 := uint64(1<<32 - 3)
	rec := func(seq uint64, at time.Time, kind string) store.Record {
		switch kind {
		case "edge":
			return f.edge(seq, at, "edge")
		case "edge-beta":
			r := f.edge(seq, at, "edge")
			r.Producer, r.TTL = "beta", 10*time.Minute
			return r
		case "pod":
			return f.entity(seq, f.pod, at, "pod")
		case "pod-beta":
			r := f.entity(seq, f.pod, at, "pod")
			r.Producer = "beta"
			return r
		}
		return f.entity(seq, f.node, at, "node")
	}
	batch := []store.Record{
		rec(n0+1, x, "edge"), rec(n0+2, x.Add(2*time.Minute), "pod"), rec(n0+3, x.Add(4*time.Minute), "node"),
		rec(n0+4, x.Add(6*time.Minute), "edge-beta"), rec(n0+5, x.Add(8*time.Minute), "pod-beta"), rec(n0+6, x.Add(10*time.Minute), "node"),
	}
	if err := s.Write(bg, cloneRecords(batch)); err != nil {
		return fmt.Errorf("a valid batch was refused: %w", err)
	}
	n := n0 + 6

	valid := f.args(bg)
	// Before any Retain nothing is refused.
	for _, kind := range readKinds {
		for _, c := range []struct {
			what string
			mod  func(*readArgs)
		}{
			{"an instant long before the records", func(a *readArgs) { a.t, a.to = x.Add(-24*time.Hour), x }},
			{"token 0", func(a *readArgs) { a.sc.AsOf = 0 }},
		} {
			a := valid
			c.mod(&a)
			if _, err := ask(s, kind, a); err != nil {
				return fmt.Errorf("%s with %s before any Retain: want an answer, got %w", describeArgs(kind, a), c.what, err)
			}
		}
	}

	h := x.Add(5 * time.Minute)
	if err := s.Retain(bg, h); err != nil {
		return fmt.Errorf("retaining: %w", err)
	}
	if got, want := s.Horizon(), (store.Horizon{Time: h, Seq: n}); !sameHorizon(got, want) {
		return fmt.Errorf("Horizon() = %v after Retain(%s); want {%s, %d}", got, h.Format(time.RFC3339Nano), h.Format(time.RFC3339Nano), n)
	}
	early := h.Add(-time.Nanosecond)
	for _, kind := range readKinds {
		windowed := kind == "Window" || kind == "EntityWindow"
		// The instants.
		for _, c := range []struct {
			what    string
			t, to   time.Time
			refused bool
		}{
			{"one nanosecond before the horizon", early, h.Add(10 * time.Minute), true},
			{"the horizon itself", h, h.Add(10 * time.Minute), false},
		} {
			a := valid
			a.t, a.to = c.t, c.to
			if err := horizonOutcome(s, kind, a, c.what, c.refused); err != nil {
				return err
			}
		}
		if windowed {
			// The horizon check applies to from even when the window is empty.
			for _, c := range []struct {
				what    string
				t, to   time.Time
				refused bool
			}{
				{"from before the horizon and to equal to from", early, early, true},
				{"from before the horizon and to before from", early, early.Add(-time.Hour), true},
				{"from after to, both at or after the horizon", x.Add(6 * time.Minute), h, false},
			} {
				a := valid
				a.t, a.to = c.t, c.to
				if err := horizonOutcome(s, kind, a, c.what, c.refused); err != nil {
					return err
				}
			}
			a := valid
			a.t, a.to = x.Add(6*time.Minute), h
			got, err := ask(s, kind, a)
			if err == nil && len(got.records) != 0 {
				return fmt.Errorf("%s with from after to = %+v; want an empty answer", describeArgs(kind, a), got.records)
			}
		}
		// The tokens, read at the horizon's instant.
		for _, c := range []struct {
			tok     uint64
			what    string
			refused bool
		}{
			{n - 1, "the token below the horizon's Seq", true},
			{n, "the horizon's Seq", false},
			{n + 100, "a token above LastSeq", false},
			{store.Latest - 1, "Latest-1", false},
			{store.Latest, "Latest", false},
		} {
			a := valid
			a.t, a.to = h, h.Add(10*time.Minute)
			a.sc.AsOf = c.tok
			if err := horizonOutcome(s, kind, a, c.what, c.refused); err != nil {
				return err
			}
		}
		// Arguments come before the horizon: a read that is invalid and also asks
		// for an instant or a token before it is invalid, not before the horizon.
		for _, c := range []struct {
			what string
			mod  func(*readArgs)
			ok   bool
		}{
			{"an invalid scope", func(a *readArgs) { a.sc.Layer = 0 }, true},
			{"an invalid scope and a token below the horizon", func(a *readArgs) { a.sc = store.Scope{AsOf: n - 1} }, true},
			{"an invalid direction", func(a *readArgs) { a.dir = 0 }, kind != "Alive" && kind != "EntityWindow"},
			{"the zero fingerprint", func(a *readArgs) { a.fp, a.fps = identity.Fingerprint{}, []identity.Fingerprint{{}} }, true},
		} {
			if !c.ok {
				continue
			}
			a := valid
			a.t, a.to = early, early.Add(time.Hour)
			c.mod(&a)
			_, err := ask(s, kind, a)
			if err := wantInvalid(fmt.Sprintf("%s with %s and an instant before the horizon", describeArgs(kind, a), c.what), err); err != nil {
				return err
			}
		}
	}

	// Writes: one before the horizon is refused, and uses no sequence number; one
	// at it is not.
	if err := wantIs("a Write one nanosecond before the horizon", s.Write(bg, []store.Record{rec(n+1, early, "edge")}), store.ErrBeforeHorizon); err != nil {
		return err
	}
	if got := s.LastSeq(); got != n {
		return fmt.Errorf("a Write refused for the horizon moved LastSeq to %d, want %d", got, n)
	}
	if err := s.Write(bg, []store.Record{rec(n+1, h, "edge")}); err != nil {
		return fmt.Errorf("a Write exactly at the horizon was refused: %w", err)
	}
	if err := s.Write(bg, []store.Record{rec(n+2, x.Add(11*time.Minute), "pod")}); err != nil {
		return fmt.Errorf("a Write after the horizon was refused: %w", err)
	}

	// A Retain that does not move the horizon changes nothing, Seq included.
	for _, earlier := range []time.Time{x.Add(4 * time.Minute), h} {
		if err := s.Retain(bg, earlier); err != nil {
			return fmt.Errorf("Retain(%s), which does not move the horizon, must be accepted: %w", earlier.Format(time.RFC3339Nano), err)
		}
		if got, want := s.Horizon(), (store.Horizon{Time: h, Seq: n}); !sameHorizon(got, want) {
			return fmt.Errorf("Horizon() = %v after Retain(%s), which does not move it; want {%s, %d}",
				got, earlier.Format(time.RFC3339Nano), h.Format(time.RFC3339Nano), n)
		}
	}
	a := valid
	a.t, a.to = h, h.Add(10*time.Minute)
	a.sc.AsOf = n
	for _, kind := range readKinds {
		if _, err := ask(s, kind, a); err != nil {
			return fmt.Errorf("%s after a Retain that did not move the horizon: the token %d of the first Retain must still be answered: %w", describeArgs(kind, a), n, err)
		}
	}

	// A Retain that moves it publishes the LastSeq at the call.
	h2 := x.Add(6 * time.Minute)
	if err := s.Retain(bg, h2); err != nil {
		return fmt.Errorf("retaining: %w", err)
	}
	if got, want := s.Horizon(), (store.Horizon{Time: h2, Seq: s.LastSeq()}); !sameHorizon(got, want) || got.Seq != n+2 {
		return fmt.Errorf("Horizon() = %v after Retain(%s); want {%s, %d}", got, h2.Format(time.RFC3339Nano), h2.Format(time.RFC3339Nano), n+2)
	}
	if err := s.Write(bg, []store.Record{rec(n+3, x.Add(12*time.Minute), "node")}); err != nil {
		return fmt.Errorf("a Write after the horizon was refused: %w", err)
	}
	// An instant given in another location is the same instant.
	h3 := x.Add(7 * time.Minute).In(time.FixedZone("far east", 9*3600+1800))
	if err := s.Retain(bg, h3); err != nil {
		return fmt.Errorf("retaining: %w", err)
	}
	if got := s.Horizon(); !got.Time.Equal(x.Add(7*time.Minute)) || got.Seq != n+3 {
		return fmt.Errorf("Horizon() = %v after Retain(%s); want {%s, %d}", got, h3.Format(time.RFC3339Nano), x.Add(7*time.Minute).Format(time.RFC3339Nano), n+3)
	}
	for _, kind := range readKinds {
		a := valid
		a.t, a.to = x.Add(7*time.Minute), x.Add(time.Hour)
		if err := horizonOutcome(s, kind, a, "the horizon given in another location", false); err != nil {
			return err
		}
		a.t = x.Add(7*time.Minute - time.Nanosecond)
		if err := horizonOutcome(s, kind, a, "one nanosecond before the horizon given in another location", true); err != nil {
			return err
		}
	}
	return nil
}

// horizonOutcome asks a read and requires it to be refused with ErrBeforeHorizon, or
// answered.
func horizonOutcome(s store.Store, kind string, a readArgs, what string, refused bool) error {
	_, err := ask(s, kind, a)
	if refused {
		if !errors.Is(err, store.ErrBeforeHorizon) {
			return fmt.Errorf("%s at %s must be refused with ErrBeforeHorizon, got %w", describeArgs(kind, a), what, orNil(err))
		}
		return nil
	}
	if err != nil {
		return fmt.Errorf("%s at %s must be answered: %w", describeArgs(kind, a), what, err)
	}
	return nil
}
