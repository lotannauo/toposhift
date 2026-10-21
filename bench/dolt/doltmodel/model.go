package doltmodel

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"hash"
	"math"
	"slices"
	"sort"
	"sync"
	"time"

	"github.com/lotannauo/toposhift/bench/spike/engine"
	"github.com/lotannauo/toposhift/internal/catalog"
	"github.com/lotannauo/toposhift/internal/identity"
)

// Arm is which design a model stands for.
type Arm uint8

const (
	// ArmCommit is the registered design: the tables, and one commit per batch that
	// versions the state table, so that a read as of an old snapshot token can read the
	// state as the commit at that token left it.
	ArmCommit Arm = iota + 1
	// ArmRows is the control: the same tables and no commits. A read as of an old
	// token is answered from the log with the token's sequence number as a filter.
	ArmRows
)

func (a Arm) String() string {
	switch a {
	case ArmCommit:
		return "commit"
	case ArmRows:
		return "rows"
	}
	return fmt.Sprintf("Arm(%d)", uint8(a))
}

// Counts is what a Model has written, in the units a database writer is charged in. It is
// a function of the stream, so it is the same on every run and every machine; it counts
// the rows of the model's tables, not anything a database did.
type Counts struct {
	// Records are the records written.
	Records uint64
	// LogRows are the rows inserted into the logs (a record of an edge is a row in each
	// direction), and StateWrites the rows REPLACEd into the state tables: for each key a
	// batch touches, one if the batch's newest record of the key is newer than the stored
	// row.
	LogRows, StateWrites uint64
	// LogDeletes and StateDeletes are the rows retention deleted.
	LogDeletes, StateDeletes uint64
	// Commits are the commits made, one per batch and one per retention that moved the
	// horizon; none in the arm without commits.
	Commits uint64
	// PayloadRefusals are the records refused for a payload over the column's capacity, a
	// capability row (the design cannot hold the record), counted once per refused batch
	// by its first such record. It must be zero on every registered input.
	PayloadRefusals uint64
}

// CommitKind says what a commit is of.
type CommitKind uint8

const (
	// BatchCommit is the commit after a batch is written.
	BatchCommit CommitKind = iota + 1
	// RetentionCommit is the commit after a retention moved the horizon.
	RetentionCommit
)

// NoFrontier is the frontier before any record is written.
const NoFrontier = math.MinInt64

// Commit is a commit as the model keeps it: a snapshot of the state tables, and what is
// needed to know when a read can be answered from it.
type Commit struct {
	// Ordinal counts the commits from 1.
	Ordinal int
	Kind    CommitKind
	// Token is the highest sequence number committed when the commit was made. A
	// retention's commit has the token of the batch before it.
	Token uint64
	// Frontier is the largest event time, in nanoseconds, among every record with a
	// sequence number at or below Token, or [NoFrontier]. The rule that reads the state
	// does not use it (it looks at each key's row); the diagnostic arm reads it as an event
	// time.
	Frontier int64
	// Message is the commit message: "seq <last>" for a batch, "retain <horizon ns> seq
	// <last>" for a retention.
	Message string
	// Date is the synthetic commit date, [CommitDate] of the ordinal.
	Date time.Time
	// ID identifies the commit by what it holds: its parent, message, date, token,
	// frontier and what it wrote. It is the model's, as stable as the stream, and is not a
	// Dolt commit hash.
	ID string
}

// CommitDate is the synthetic date of the n-th commit, counting every commit, batch and
// retention alike, from 1: 2000-01-01T00:00:00Z plus n seconds. A date that is a function of
// the stream keeps the history of two loads of one stream identical.
func CommitDate(ordinal int) time.Time {
	return time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC).Add(time.Duration(ordinal) * time.Second)
}

// Model is an in-memory engine with the tables and the read paths of the Dolt baseline
// (see the package documentation). It implements [engine.Engine]. Reads run concurrently
// with each other; a read sees whole committed batches, because Write and Retain hold the
// lock for their whole effect.
type Model struct {
	mu         sync.RWMutex
	arm        Arm
	payloadCap int // the payload column's size; 0 for no limit
	f          faults

	lastSeq uint64
	horizon int64 // Unix nanoseconds; records before it are refused
	// pastEnd says the horizon was moved after the last representable instant, so
	// every record, that instant's included, is before it. horizon alone cannot say
	// it: it is an int64.
	pastEnd bool
	// frontier is the largest event time of any record written.
	frontier int64
	ops      uint64 // batches and retentions so far, to tell the writer's staging apart

	subjects map[engine.Subject]*subject
	incident [3]map[identity.Fingerprint][]*subject // by direction: entity -> its edges

	commits []Commit
	// byToken maps a token to the ordinal of the last commit made at it.
	byToken map[uint64]int

	counts Counts
	reads  readStats
}

var _ engine.Engine = (*Model)(nil)

// Option configures New.
type Option func(*Model)

// WithArm selects the design; the default is [ArmCommit].
func WithArm(a Arm) Option { return func(m *Model) { m.arm = a } }

// WithPayloadCapacity sets the size of the payload column: a record with a longer payload
// is refused with [engine.ErrInvalid]. The default is no limit; see [RegisteredPayloadCapacity].
func WithPayloadCapacity(n int) Option { return func(m *Model) { m.payloadCap = n } }

// New returns an empty model.
func New(opts ...Option) *Model {
	m := &Model{
		arm: ArmCommit, frontier: NoFrontier,
		subjects: make(map[engine.Subject]*subject), byToken: make(map[uint64]int),
	}
	m.incident[engine.Forward] = make(map[identity.Fingerprint][]*subject)
	m.incident[engine.Reverse] = make(map[identity.Fingerprint][]*subject)
	for _, o := range opts {
		o(m)
	}
	return m
}

// open models are what a directory holds: a model has no files, so a directory is only a
// name, and a model opened again under it finds what the last one left.
var open sync.Map

// Open returns the model that dir names, a new one with the options the first time. It
// is the form a conformance factory needs, which closes an engine and opens it again over
// the same directory and requires everything back. Forget releases it.
func Open(dir string, opts ...Option) *Model {
	m, _ := open.LoadOrStore(dir, New(opts...))
	return m.(*Model)
}

// Forget drops the model Open returned for dir.
func Forget(dir string) { open.Delete(dir) }

// Arm is the design the model stands for.
func (m *Model) Arm() Arm { return m.arm }

// Counts returns what the model has written so far.
func (m *Model) Counts() Counts {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.counts
}

// Commits returns a copy of the commits made, oldest first.
func (m *Model) Commits() []Commit {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return slices.Clone(m.commits)
}

// Head is the newest commit, or false if there is none.
func (m *Model) Head() (Commit, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if len(m.commits) == 0 {
		return Commit{}, false
	}
	return m.commits[len(m.commits)-1], true
}

// Frontier is the frontier a read at the token has: the largest event time among the
// records at or below it. It is false if the token names no commit, so no state to read
// (a token that is not a batch boundary, or any token below the newest in the arm with no
// commits). A token at or above the newest sequence number is the current state, whose
// frontier is that of every record written.
func (m *Model) Frontier(token uint64) (ns int64, ok bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	_, ns, ok = m.snapshotFor(token)
	return ns, ok
}

// headOrd stands for the current state in a read: every version is at or before it.
const headOrd = math.MaxInt

// snapshotFor says which state a read at the token can be answered from, and its
// frontier: the current state if the token is at or above the newest sequence number; the
// state as the last commit at the token left it, in an arm with commits; none otherwise.
func (m *Model) snapshotFor(token uint64) (ord int, frontier int64, ok bool) {
	if token >= m.lastSeq {
		return headOrd, m.frontier, true
	}
	if m.arm != ArmCommit {
		return 0, 0, false
	}
	c, found := m.byToken[token]
	if !found {
		return 0, 0, false
	}
	frontier = m.commits[c-1].Frontier
	if m.f.commitAfter && c < len(m.commits) {
		c++ // the commit after the token
	}
	return c, frontier, true
}

// Write implements [engine.Engine]. The batch is validated whole before any of it is
// applied, so a refused batch changes nothing; then the records go to the logs, the state
// rows they supersede are replaced, and, in the arm with commits, the batch is committed,
// all before the sequence number rises.
func (m *Model) Write(batch []engine.Record) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if len(batch) == 0 {
		return nil
	}
	seq := m.lastSeq
	inBatch := make(map[engine.Subject]catalog.Layer)
	for _, r := range batch {
		if err := r.Validate(); err != nil && !m.f.skipValidate {
			return err
		}
		if len(r.Producer) > MaxProducerLen && !m.f.ignoreProducerLen {
			return fmt.Errorf("record seq %d: producer of %d bytes is over the column's %d: %w", r.Seq, len(r.Producer), MaxProducerLen, engine.ErrInvalid)
		}
		if m.payloadCap > 0 && len(r.Payload) > m.payloadCap && !m.f.ignoreCapacity {
			m.counts.PayloadRefusals++
			return fmt.Errorf("record seq %d: payload of %d bytes is over the column's %d: %w", r.Seq, len(r.Payload), m.payloadCap, engine.ErrInvalid)
		}
		if !m.f.acceptBeforeHorizon && (m.pastEnd || Nanos(r.EventTime) < m.horizon) {
			return fmt.Errorf("record seq %d at %s: %w", r.Seq, r.EventTime.Format(time.RFC3339), engine.ErrBeforeHorizon)
		}
		if r.Seq <= seq && !m.f.acceptStaleSeq {
			return fmt.Errorf("record seq %d is not above the last written seq %d: %w", r.Seq, seq, engine.ErrInvalid)
		}
		seq = r.Seq
		known, ok := inBatch[r.Subject]
		if !ok {
			if st := m.subjects[r.Subject]; st != nil {
				known, ok = st.layer, true
			}
		}
		if ok && known != r.Layer && !m.f.acceptLayerConflict {
			return fmt.Errorf("record seq %d: subject is stored in layer %s, not %s: %w", r.Seq, known, r.Layer, engine.ErrInvalid)
		}
		inBatch[r.Subject] = r.Layer
	}

	m.ops++
	ord := m.nextOrdinal()
	type touch struct {
		st *subject
		p  *producer
	}
	var (
		touched []touch
		digest  = sha256.New()
	)
	for _, r := range batch {
		st := m.subjects[r.Subject]
		if st == nil {
			st = &subject{id: r.Subject, layer: r.Layer}
			m.subjects[r.Subject] = st
			if r.Subject.Kind == engine.SubjectEdge {
				m.incident[engine.Forward][r.Subject.A] = append(m.incident[engine.Forward][r.Subject.A], st)
				m.incident[engine.Reverse][r.Subject.B] = append(m.incident[engine.Reverse][r.Subject.B], st)
			}
		}
		p := st.producer(r.Producer)
		c := keep(r)
		p.insert(c)
		m.counts.LogRows += st.mirror()
		m.frontier = max(m.frontier, c.event)
		if m.arm == ArmCommit {
			hashRecord(digest, r)
		}

		// The writer's in-memory map: the newest record of each key in this batch.
		s := stateOf(c)
		if p.stagedOp != m.ops {
			p.stagedOp, p.hasStaged = m.ops, false
			touched = append(touched, touch{st, p})
		}
		if !p.hasStaged || m.f.stateKeepsLast || m.stagedReplaces(s, p.staged) {
			p.staged, p.hasStaged = s, true
		}
	}
	for _, tc := range touched {
		st, p := tc.st, tc.p
		if stored, ok := p.head(); ok && !m.f.stateKeepsLast && !m.stagedReplaces(p.staged, stored) {
			continue // the stored row is newer: a late record changes no state
		}
		m.putState(p, ord, version{at: ord, present: true, row: p.staged})
		m.counts.StateWrites += st.mirror()
		if m.arm == ArmCommit {
			hashState(digest, st, p)
		}
	}
	m.counts.Records += uint64(len(batch))
	if m.arm == ArmCommit {
		m.commit(BatchCommit, seq, m.frontier, fmt.Sprintf("seq %d", seq), digest.Sum(nil))
	}
	m.lastSeq = seq
	return nil
}

// stagedReplaces reports whether a record's state row replaces another: the newer by
// (event time, seq), or with the fault of ordering by event time alone, a later event
// time only.
func (m *Model) stagedReplaces(s, over stateRow) bool {
	if m.f.tieByEvent {
		return s.event > over.event
	}
	return s.newer(over)
}

// nextOrdinal is the ordinal of the commit the next write makes, or 0 in the arm with none.
func (m *Model) nextOrdinal() int {
	if m.arm != ArmCommit {
		return 0
	}
	return len(m.commits) + 1
}

// putState sets a producer's state row as of commit ordinal ord. Without commits there is
// no history, and the row replaces the one there was.
func (m *Model) putState(p *producer, ord int, v version) {
	if m.arm != ArmCommit {
		p.vers = append(p.vers[:0], v)
		return
	}
	if n := len(p.vers); n > 0 && p.vers[n-1].at == ord {
		p.vers[n-1] = v
		return
	}
	p.vers = append(p.vers, v)
}

// commit appends a commit. Its ID covers the parent, the message, the date, the token, the
// frontier, the horizon and the digest of what it wrote.
func (m *Model) commit(kind CommitKind, token uint64, frontier int64, message string, wrote []byte) {
	parent := ""
	if n := len(m.commits); n > 0 {
		parent = m.commits[n-1].ID
	}
	h := sha256.New()
	h.Write([]byte(parent))
	ordinal := len(m.commits) + 1
	date := CommitDate(ordinal)
	var buf []byte
	buf = appendBytes(buf, []byte(message))
	buf = binary.BigEndian.AppendUint64(buf, uint64(date.Unix()))
	buf = append(buf, byte(kind))
	buf = binary.BigEndian.AppendUint64(buf, token)
	buf = binary.BigEndian.AppendUint64(buf, uint64(frontier))
	buf = binary.BigEndian.AppendUint64(buf, uint64(m.horizon))
	h.Write(buf)
	h.Write(wrote)
	c := Commit{Ordinal: ordinal, Kind: kind, Token: token, Frontier: frontier, Message: message, Date: date, ID: hex.EncodeToString(h.Sum(nil))}
	m.commits = append(m.commits, c)
	m.byToken[token] = c.Ordinal
	m.counts.Commits++
}

// hashRecord adds a record to a commit's digest: the log rows the batch inserted.
func hashRecord(h hash.Hash, r engine.Record) {
	var buf []byte
	buf = append(buf, byte(r.Layer), byte(r.Subject.Kind))
	buf = appendBytes(buf, EncodeFingerprint(r.Subject.A))
	if !r.Subject.B.IsZero() {
		buf = appendBytes(buf, EncodeFingerprint(r.Subject.B))
	} else {
		buf = appendBytes(buf, nil)
	}
	buf = appendBytes(buf, []byte(r.Subject.Relation))
	buf = appendBytes(buf, []byte(r.Producer))
	buf = binary.BigEndian.AppendUint64(buf, uint64(Nanos(r.EventTime)))
	buf = binary.BigEndian.AppendUint64(buf, uint64(EncodeSeq(r.Seq)))
	buf = append(buf, EncodeKind(r.Kind)...)
	buf = binary.BigEndian.AppendUint64(buf, uint64(r.TTL))
	if r.Through.IsZero() {
		buf = append(buf, 0)
	} else {
		buf = append(buf, 1)
		buf = binary.BigEndian.AppendUint64(buf, uint64(Nanos(r.Through)))
	}
	buf = appendBytes(buf, r.Payload)
	_, _ = h.Write(buf)
}

// hashState adds a state row a batch wrote to a commit's digest.
func hashState(h hash.Hash, st *subject, p *producer) {
	var buf []byte
	buf = append(buf, 's', byte(st.layer), byte(st.id.Kind))
	buf = appendBytes(buf, EncodeFingerprint(st.id.A))
	buf = appendBytes(buf, []byte(st.id.Relation))
	buf = appendBytes(buf, []byte(p.name))
	row := p.staged
	buf = binary.BigEndian.AppendUint64(buf, uint64(row.event))
	buf = binary.BigEndian.AppendUint64(buf, uint64(EncodeSeq(row.seq)))
	buf = binary.BigEndian.AppendUint64(buf, uint64(row.deadline))
	if row.open {
		buf = append(buf, 1)
	} else {
		buf = append(buf, 0)
	}
	_, _ = h.Write(buf)
}

func appendBytes(buf, b []byte) []byte {
	buf = binary.BigEndian.AppendUint32(buf, uint32(len(b)))
	return append(buf, b...)
}

// LastSeq implements [engine.Engine].
func (m *Model) LastSeq() uint64 {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.lastSeq
}

// Retain implements [engine.Engine]. In one step it deletes the log rows before the
// horizon but, for each key, the newest (the baseline that stands for what the deleted
// rows left), and the state rows whose deadline is at or before it (not holding at any
// instant from the horizon on); then, with commits, it commits. Earlier commits still
// hold what it deleted. A horizon before the range changes nothing. One after the range
// discards every record but each key's newest, and refuses every later one, as the
// reference engine does.
func (m *Model) Retain(horizon time.Time) error {
	h := int64(0)
	past := horizon.After(engine.MaxEventTime)
	switch {
	case past:
		h = math.MaxInt64
	case horizon.After(engine.MinEventTime):
		h = Nanos(horizon)
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	changed := false
	if past && !m.pastEnd {
		m.pastEnd, changed = true, true
	}
	if h > m.horizon {
		m.horizon, changed = h, true
	}
	if !changed {
		return nil
	}
	m.ops++
	ord := m.nextOrdinal()
	var logDeleted, stateDeleted uint64
	for _, st := range m.subjects {
		for _, p := range st.prods {
			cut := sort.Search(len(p.recs), func(i int) bool { return p.recs[i].event >= h })
			if m.pastEnd {
				cut = len(p.recs)
			}
			// The baseline is the newest record before the horizon: it stays, standing
			// for the rows before it. An earlier retention's baseline is the first row
			// of the log, and is replaced the same way.
			keepFrom := cut - 1
			if m.f.retainDropsHolding {
				keepFrom = cut
			}
			if keepFrom > 0 {
				logDeleted += uint64(keepFrom) * st.mirror()
				p.recs = slices.Clone(p.recs[keepFrom:])
			}
			if row, ok := p.head(); ok && (!row.open && row.deadline <= h || row.open && m.f.retainDropsOpenState) {
				m.putState(p, ord, version{at: ord})
				stateDeleted += st.mirror()
			}
		}
	}
	m.counts.LogDeletes += logDeleted
	m.counts.StateDeletes += stateDeleted
	if m.arm == ArmCommit {
		var buf []byte
		buf = binary.BigEndian.AppendUint64(buf, logDeleted)
		buf = binary.BigEndian.AppendUint64(buf, stateDeleted)
		m.commit(RetentionCommit, m.lastSeq, m.frontier, fmt.Sprintf("retain %d seq %d", m.horizon, m.lastSeq), buf)
	}
	return nil
}

// Size implements [engine.Engine]. A model holds nothing on disk; this is the bytes of
// the rows it holds now at a fixed nominal size per row (state history and earlier commits
// not counted), so that a caller that asks gets a number that grows with what is stored.
// It is not a measurement of Dolt.
func (m *Model) Size() (int64, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	var n int64
	for _, st := range m.subjects {
		for _, p := range st.prods {
			for _, c := range p.recs {
				n += (int64(len(c.payload)) + 96) * int64(st.mirror())
			}
			if _, ok := p.head(); ok {
				n += 64 * int64(st.mirror())
			}
		}
	}
	return n, nil
}

// Close implements [engine.Engine]. The model keeps what it holds, for [Open].
func (m *Model) Close() error { return nil }
