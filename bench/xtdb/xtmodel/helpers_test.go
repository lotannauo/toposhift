package xtmodel

import (
	"cmp"
	"fmt"
	"reflect"
	"slices"
	"strings"
	"time"

	"github.com/lotannauo/toposhift/bench/spike/engine"
	"github.com/lotannauo/toposhift/bench/spike/oracle"
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

// The subjects of the test streams: two entities and four edges between them (two of
// them the same pair in the other direction, and two relations), all in layer L2, so
// that they share endpoints and a reader sees several peers.
var (
	pod1, pod2, node1 = fp("k8s.pod", 1), fp("k8s.pod", 2), fp("k8s.node", 3)

	edgeA = engine.EdgeSubject(pod1, node1, catalog.ScheduledOn)

	subjects = []engine.Subject{
		engine.EntitySubject(pod1),
		engine.EntitySubject(node1),
		edgeA,
		engine.EdgeSubject(pod2, node1, catalog.ScheduledOn),
		engine.EdgeSubject(node1, pod1, catalog.ScheduledOn),
		engine.EdgeSubject(pod1, node1, catalog.PartOf),
	}
	producers = []lifecycle.Producer{"a", "b", "c"}
)

// at is the instant s seconds after the epoch.
func at(s int64) time.Time { return time.Unix(s, 0).UTC() }

func observe(seq uint64, p lifecycle.Producer, s int64, ttl int64) engine.Record {
	return engine.Record{
		Layer: catalog.L2, Subject: edgeA, Producer: p, EventTime: at(s), Seq: seq,
		Kind: lifecycle.Observe, TTL: time.Duration(ttl) * time.Second, Payload: []byte{byte(seq)},
	}
}

func del(seq uint64, p lifecycle.Producer, s int64) engine.Record {
	return engine.Record{Layer: catalog.L2, Subject: edgeA, Producer: p, EventTime: at(s), Seq: seq, Kind: lifecycle.Delete}
}

// rig plans batches and commits them to an image, as a Model does, and lets a test
// see the statements.
type rig struct {
	img *image
	pl  *Planner
	n   uint64
}

func newRig() *rig {
	img := newImage()
	return &rig{img: img, pl: NewPlanner(img)}
}

// write plans and commits a batch and returns its plan.
func (r *rig) write(batch ...engine.Record) *Plan {
	r.n++
	pl, err := r.pl.Plan(r.n, batch)
	if err != nil {
		panic(err)
	}
	pl.Txn.System = SystemTime(r.n)
	if err := r.img.apply(pl.Txn); err != nil {
		panic(err)
	}
	r.pl.Accept(pl)
	return pl
}

// statements lists a plan's statements on one table, in the order they were planned,
// as "+[from,to)" for an insert and "-[from,to)" for a portion delete, in seconds,
// with "inf" for the end of time.
func statements(pl *Plan, t Table) string {
	var out []string
	for _, o := range pl.Txn.Ops {
		if o.Key.Table != t {
			continue
		}
		sign := "+"
		if o.Kind == DeletePortion {
			sign = "-"
		}
		out = append(out, fmt.Sprintf("%s[%s,%s)", sign, secs(o.From), secs(o.To)))
	}
	return strings.Join(out, " ")
}

func secs(i Instant) string {
	if i == Forever {
		return "inf"
	}
	return fmt.Sprint(int64(i) / sec)
}

// span is a stretch of valid time during which a subject exists.
type span struct{ from, to Instant }

func (s span) String() string { return "[" + secs(s.from) + "," + secs(s.to) + ")" }

// unite sorts spans and joins those that overlap or touch, which is what the
// lifecycle specification's maximal intervals are.
func unite(spans []span) []span {
	slices.SortFunc(spans, func(a, b span) int { return cmp.Compare(a.from, b.from) })
	var out []span
	for _, s := range spans {
		if n := len(out); n > 0 && s.from <= out[n-1].to {
			out[n-1].to = max(out[n-1].to, s.to)
			continue
		}
		out = append(out, s)
	}
	return out
}

// existence is the valid time during which the subject is present in the table of
// a direction, as the database knew it at the view: the union of its producers'
// documents.
func (m *Model) existence(t Table, s engine.Subject, v view) []span {
	var all []span
	fp := Key{Table: t, Subject: s}.entity()
	for _, d := range m.img.incident[t][fp] {
		if d.key.Subject != s {
			continue
		}
		for _, x := range d.live {
			if v.latest || x.sFrom <= v.sys {
				all = append(all, span{x.vFrom, x.vTo})
			}
		}
		if v.latest {
			continue
		}
		for _, x := range d.closed {
			if x.sFrom <= v.sys && v.sys < x.sTo {
				all = append(all, span{x.vFrom, x.vTo})
			}
		}
	}
	return unite(all)
}

// foldExistence is the reference: the existence intervals of the subject that
// lifecycle.Fold gives the records of it with Seq at or below the token.
func foldExistence(written []engine.Record, s engine.Subject, token uint64) ([]span, error) {
	var as []lifecycle.Assertion
	for _, r := range written {
		if r.Subject == s && r.Seq <= token {
			as = append(as, r.Assertion())
		}
	}
	tl, err := lifecycle.Fold(as, lifecycle.Policy{})
	if err != nil {
		return nil, err
	}
	var out []span
	for _, iv := range tl.Existence() {
		sp := span{from: instantOf(iv.Start), to: Forever}
		if !iv.Open() {
			sp.to = instantOf(iv.End)
		}
		out = append(out, sp)
	}
	return out, nil
}

// step is one thing a stream does: write a batch, or move the retention horizon.
type step struct {
	batch  []engine.Record
	retain time.Time
}

// checker writes steps to a model and to the reference engine and compares them.
type checker struct {
	m       *Model
	ora     *oracle.Oracle
	written []engine.Record
	// ends are the sequence numbers the committed batches ended at, in order, and
	// inside the tokens that fall within a batch or between two.
	ends  []uint64
	mid   []uint64
	horiz time.Time
	steps int
}

func newChecker(m *Model) *checker { return &checker{m: m, ora: oracle.New()} }

// do applies a step to both engines and compares everything.
func (c *checker) do(s step) error {
	if len(s.batch) > 0 {
		if err := c.m.Write(s.batch); err != nil {
			return fmt.Errorf("the model refused a batch: %w", err)
		}
		if err := c.ora.Write(s.batch); err != nil {
			return fmt.Errorf("the reference engine refused a batch: %w", err)
		}
		c.written = append(c.written, s.batch...)
		last := s.batch[len(s.batch)-1].Seq
		for _, r := range s.batch[:len(s.batch)-1] {
			c.mid = append(c.mid, r.Seq)
		}
		c.mid = append(c.mid, last+1) // between this batch and whatever follows
		c.ends = append(c.ends, last)
	}
	if !s.retain.IsZero() {
		if err := c.m.Retain(s.retain); err != nil {
			return err
		}
		if err := c.ora.Retain(s.retain); err != nil {
			return err
		}
		if s.retain.After(c.horiz) {
			c.horiz = s.retain
		}
	}
	if err := c.checkTables(); err != nil {
		return err
	}
	return c.checkReads()
}

// checkTables compares the segments the planner wrote with the fold of the records:
// for every subject, at the newest system time and at the system time of every batch
// committed so far, the valid time it is present in is exactly the fold's existence
// as of the token the batch ended at.
func (c *checker) checkTables() error {
	for ord, end := range c.ends {
		v := view{sys: SystemTime(uint64(ord) + 1)}
		if ord == len(c.ends)-1 {
			v = view{latest: true}
		}
		for _, s := range subjects {
			want, err := foldExistence(c.written, s, end)
			if err != nil {
				return err
			}
			tables := []Table{Ent}
			if s.Kind == engine.SubjectEdge {
				tables = []Table{SegF, SegR}
			}
			for _, t := range tables {
				if got := c.m.existence(t, s, v); !slices.Equal(got, want) {
					return fmt.Errorf("%s of %s as of batch %d (token %d): the model has %v, the fold %v", t, describeSubject(s), ord+1, end, got, want)
				}
			}
		}
	}
	// And the system time of the newest batch, named, is the same picture as the
	// newest.
	if n := len(c.ends); n > 0 {
		for _, s := range subjects {
			t := Ent
			if s.Kind == engine.SubjectEdge {
				t = SegF
			}
			if a, b := c.m.existence(t, s, view{sys: SystemTime(uint64(n))}), c.m.existence(t, s, view{latest: true}); !slices.Equal(a, b) {
				return fmt.Errorf("%s of %s: the newest system time is %v, the newest picture %v", t, describeSubject(s), a, b)
			}
		}
	}
	return nil
}

// checkReads asks the model and the reference engine the same questions.
func (c *checker) checkReads() error {
	floor := engine.MinEventTime
	if !c.horiz.IsZero() {
		floor = c.horiz
	}
	var instants []time.Time
	for _, r := range c.written {
		for _, x := range []time.Time{r.EventTime, r.Through, r.EventTime.Add(r.TTL), r.Through.Add(r.TTL)} {
			if x.IsZero() {
				continue
			}
			for _, d := range []time.Duration{-1, 0, 1} {
				if t := x.Add(d); !t.Before(floor) {
					instants = append(instants, t)
				}
			}
		}
	}
	instants = append(instants, floor, engine.MaxEventTime, engine.MaxEventTime.Add(time.Hour))
	slices.SortFunc(instants, time.Time.Compare)
	instants = slices.CompactFunc(instants, time.Time.Equal)

	newest := []engine.Scope{engine.Current(catalog.L2), engine.Current(catalog.L1)}
	// Tokens: every batch's end and the ones inside a batch or between two, before any
	// retention (after which earlier tokens are not asked about). They are many, so
	// each step asks about a few of them, a different few each time, at fewer instants.
	var older []engine.Scope
	if c.horiz.IsZero() {
		all := slices.Concat(c.ends, c.mid, []uint64{0})
		for k := 0; k < tokensPerStep && k < len(all); k++ {
			older = append(older, engine.Scope{Layer: catalog.L2, AsOf: all[(c.steps*tokensPerStep+k)%len(all)]})
		}
		older = append(older, engine.Scope{Layer: catalog.L1, AsOf: c.m.LastSeq() / 2})
	}
	c.steps++
	sparse := thin(instants, 24)
	entities := []identity.Fingerprint{pod1, pod2, node1}
	for _, group := range []struct {
		scopes []engine.Scope
		at     []time.Time
	}{{newest, instants}, {older, sparse}} {
		for _, t := range group.at {
			for _, sc := range group.scopes {
				if err := c.askAt(t, sc, entities); err != nil {
					return err
				}
			}
		}
		for _, sc := range group.scopes {
			if err := c.askWindows(group.at, sc, floor); err != nil {
				return err
			}
		}
	}
	return nil
}

// tokensPerStep is how many earlier tokens a step asks about.
const tokensPerStep = 6

// thin returns at most n of the instants, evenly spread, the first and last included.
func thin(ts []time.Time, n int) []time.Time {
	if len(ts) <= n {
		return ts
	}
	out := make([]time.Time, 0, n)
	for i := range n {
		out = append(out, ts[i*(len(ts)-1)/(n-1)])
	}
	return out
}

func (c *checker) askAt(t time.Time, sc engine.Scope, entities []identity.Fingerprint) error {
	for _, e := range entities {
		for _, dir := range []engine.Direction{engine.Forward, engine.Reverse} {
			a, errA := c.m.Neighbors(e, dir, t, sc)
			b, errB := c.ora.Neighbors(e, dir, t, sc)
			if errA != nil || errB != nil || !slices.Equal(a, b) {
				return fmt.Errorf("Neighbors(%s, %s, %s, layer %s as of %d) = %v (%v), the reference engine %v (%v)", e, dir, t.Format(time.RFC3339Nano), sc.Layer, sc.AsOf, a, msg(errA), b, msg(errB))
			}
		}
		a, errA := c.m.Alive(e, t, sc)
		b, errB := c.ora.Alive(e, t, sc)
		if errA != nil || errB != nil || a != b {
			return fmt.Errorf("Alive(%s, %s, layer %s as of %d) = %v (%v), the reference engine %v (%v)", e, t.Format(time.RFC3339Nano), sc.Layer, sc.AsOf, a, msg(errA), b, msg(errB))
		}
	}
	fps := []identity.Fingerprint{pod1, node1, pod2, pod1}
	for _, dir := range []engine.Direction{engine.Forward, engine.Reverse} {
		a, errA := c.m.NeighborsBatch(fps, dir, t, sc)
		b, errB := c.ora.NeighborsBatch(fps, dir, t, sc)
		if errA != nil || errB != nil || !slices.EqualFunc(a, b, slices.Equal[[]engine.Neighbor]) {
			return fmt.Errorf("NeighborsBatch(%s, %s, layer %s as of %d) = %v (%v), the reference engine %v (%v)", dir, t.Format(time.RFC3339Nano), sc.Layer, sc.AsOf, a, msg(errA), b, msg(errB))
		}
	}
	return nil
}

func (c *checker) askWindows(instants []time.Time, sc engine.Scope, floor time.Time) error {
	for i := 0; i+1 < len(instants) && i < 40; i++ {
		for _, e := range []identity.Fingerprint{pod1, node1} {
			for _, dir := range []engine.Direction{engine.Forward, engine.Reverse} {
				a, errA := c.m.Window(e, dir, instants[i], instants[i+1], sc)
				b, errB := c.ora.Window(e, dir, instants[i], instants[i+1], sc)
				if errA != nil || errB != nil || !equalRecords(a, b) {
					return fmt.Errorf("Window(%s, %s, %s, %s, layer %s as of %d) differs: %v (%v) / %v (%v)", e, dir, instants[i].Format(time.RFC3339Nano), instants[i+1].Format(time.RFC3339Nano), sc.Layer, sc.AsOf, a, msg(errA), b, msg(errB))
				}
			}
		}
	}
	// A window to the end of time holds a record at the last representable instant.
	a, _ := c.m.Window(pod1, engine.Forward, floor, engine.MaxEventTime.Add(time.Hour), sc)
	b, _ := c.ora.Window(pod1, engine.Forward, floor, engine.MaxEventTime.Add(time.Hour), sc)
	if !equalRecords(a, b) {
		return fmt.Errorf("Window to the end of time (layer %s as of %d) differs: %v / %v", sc.Layer, sc.AsOf, a, b)
	}
	return nil
}

func equalRecords(a, b []engine.Record) bool {
	return slices.EqualFunc(a, b, func(x, y engine.Record) bool {
		return reflect.DeepEqual(normalize(x), normalize(y))
	})
}

func normalize(r engine.Record) engine.Record {
	if len(r.Payload) == 0 {
		r.Payload = nil
	}
	return r
}

// msg is an error's text, or "<nil>", for a failure message.
func msg(err error) string {
	if err == nil {
		return "<nil>"
	}
	return err.Error()
}
