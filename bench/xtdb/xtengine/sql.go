package xtengine

import (
	"encoding/hex"
	"fmt"
	"strconv"
	"strings"

	"github.com/lotannauo/toposhift/bench/xtdb/xtmodel"
)

// stmt is a statement and its arguments.
type stmt struct {
	sql  string
	args []any
}

// sqlText builds a statement whose arguments are numbered as they are written.
type sqlText struct {
	b    strings.Builder
	args []any
}

// s appends text.
func (q *sqlText) s(text string) *sqlText {
	q.b.WriteString(text)
	return q
}

// hexBytes is bytes, passed as the hex string the node's cast to a binary value reads.
type hexBytes string

func hexOf(b []byte) hexBytes { return hexBytes(hex.EncodeToString(b)) }

// p appends a placeholder for the next argument, and the argument.
//
// The driver's simple protocol, which is the one query mode the node accepts for the
// statements the engine sends (see the package documentation), writes every argument into
// the statement as a quoted string, whatever its type. A number must be a number to be
// ordered and compared as one, so a placeholder for an int64 is cast to a bigint, and for
// bytes (as [hexBytes]) to a binary value; a string and a timestamp are what they are, and a
// null is null.
func (q *sqlText) p(arg any) *sqlText {
	var open, closing string
	switch a := arg.(type) {
	case int64:
		open, closing = "CAST(", " AS BIGINT)"
	case hexBytes:
		open, closing = "CAST(", " AS VARBINARY)"
		arg = string(a)
	}
	q.args = append(q.args, arg)
	q.b.WriteString(open + "$" + strconv.Itoa(len(q.args)) + closing)
	return q
}

func (q *sqlText) stmt() stmt { return stmt{sql: q.b.String(), args: q.args} }

// The columns of the tables, which are the document's fields besides its `_id` and
// the valid time of a segment document.
//
//	seg_f, seg_r  layer, fp, peer, rel, producer      (fp: the entity the edge is read
//	                                                    from, peer: the other end)
//	ent           layer, fp, producer
//	log_f, log_r  layer, fp, peer, rel, producer, seq, event_ns, kind, ttl_ns, through_ns, payload
//	log_e         layer, fp, producer, seq, event_ns, kind, ttl_ns, through_ns, payload
//	meta          kind, last_seq, sys_us, horizon_ns, past_end (0 or 1)
//
// A fingerprint is its text form, a sequence number is stored with its top bit
// flipped ([seqToDB]), times in a log document are nanoseconds, and a segment
// document's valid time is the boundaries of the segment.

// Kinds of a meta document: a batch, and a retention that moved the horizon.
const (
	metaBatch  = "batch"
	metaRetain = "retain"
)

// horizonState is the retention horizon a batch's meta document records.
type horizonState struct {
	ns      int64
	pastEnd bool
}

// encode turns one of a plan's statements into the SQL that makes it.
func (e *Engine) encode(o xtmodel.Op, sys xtmodel.Instant, hz horizonState) (stmt, error) {
	tbl := e.names.of(o.Key.Table)
	switch o.Key.Table {
	case xtmodel.SegF, xtmodel.SegR:
		s := o.Key.Subject
		fp, peer := s.A, s.B
		if o.Key.Table == xtmodel.SegR {
			fp, peer = s.B, s.A
		}
		return e.encodeSegment(o, tbl, func(q *sqlText) {
			q.p(int64(o.Layer)).s(", ").p(fp.String()).s(", ").p(peer.String()).s(", ").p(string(s.Relation)).s(", ").p(string(o.Key.Producer))
		}, "_id, layer, fp, peer, rel, producer")
	case xtmodel.Ent:
		return e.encodeSegment(o, tbl, func(q *sqlText) {
			q.p(int64(o.Layer)).s(", ").p(o.Key.Subject.A.String()).s(", ").p(string(o.Key.Producer))
		}, "_id, layer, fp, producer")
	case xtmodel.LogF, xtmodel.LogR, xtmodel.LogE:
		return e.encodeLog(o, tbl)
	case xtmodel.Meta:
		return e.metaStmt(o.Key.Seq, metaBatch, o.LastSeq, sys, hz), nil
	}
	return stmt{}, fmt.Errorf("xtengine: a statement on table %s", o.Key.Table)
}

// encodeSegment is an insert into a segment table over a stretch of valid time, or a
// portion delete. Its `_id` is the first column and the valid time the last two.
func (e *Engine) encodeSegment(o xtmodel.Op, tbl string, fields func(*sqlText), cols string) (stmt, error) {
	id := segID(o.Key)
	from, err := e.timestamp(o.From)
	if err != nil {
		return stmt{}, err
	}
	to, err := e.timestamp(o.To)
	if err != nil {
		return stmt{}, err
	}
	var q sqlText
	switch o.Kind {
	case xtmodel.Insert:
		q.s("INSERT INTO " + tbl + " (" + cols + ", _valid_from, _valid_to) VALUES (").p(id).s(", ")
		fields(&q)
		q.s(", ").p(from).s(", ").p(to).s(")")
	case xtmodel.DeletePortion:
		q.s("DELETE FROM " + tbl + " FOR PORTION OF VALID_TIME FROM ").p(from).s(" TO ").p(to).s(" WHERE _id = ").p(id)
	default:
		return stmt{}, fmt.Errorf("xtengine: a statement of kind %s", o.Kind)
	}
	return q.stmt(), nil
}

// encodeLog is the insert of a record into a log table, over the default valid time:
// a log is only read for all of valid time.
func (e *Engine) encodeLog(o xtmodel.Op, tbl string) (stmt, error) {
	r := o.Rec
	if r == nil || o.Kind != xtmodel.Insert {
		return stmt{}, fmt.Errorf("xtengine: a statement on log table %s that is not the insert of a record", o.Key.Table)
	}
	var through any
	if r.HasThrough {
		through = r.Through
	}
	var payload any = hexOf(r.Payload)
	if e.mut == mutRawPayload {
		payload = r.Payload
	}
	var q sqlText
	switch o.Key.Table {
	case xtmodel.LogF, xtmodel.LogR:
		fp, peer := r.Subject.A, r.Subject.B
		if o.Key.Table == xtmodel.LogR {
			fp, peer = peer, fp
		}
		q.s("INSERT INTO " + tbl + " (_id, layer, fp, peer, rel, producer, seq, event_ns, kind, ttl_ns, through_ns, payload) VALUES (")
		q.p(logID(r.Seq)).s(", ").p(int64(r.Layer)).s(", ").p(fp.String()).s(", ").p(peer.String()).s(", ").p(string(r.Subject.Relation)).s(", ").p(string(r.Producer))
	default:
		q.s("INSERT INTO " + tbl + " (_id, layer, fp, producer, seq, event_ns, kind, ttl_ns, through_ns, payload) VALUES (")
		q.p(logID(r.Seq)).s(", ").p(int64(r.Layer)).s(", ").p(r.Subject.A.String()).s(", ").p(string(r.Producer))
	}
	q.s(", ").p(seqToDB(r.Seq)).s(", ").p(r.Event).s(", ").p(int64(r.Kind)).s(", ").p(int64(r.TTL)).s(", ").p(through).s(", ").p(payload).s(")")
	return q.stmt(), nil
}

// metaStmt is the insert of the meta document of the transaction with ordinal n.
func (e *Engine) metaStmt(n uint64, kind string, last uint64, sys xtmodel.Instant, hz horizonState) stmt {
	var q sqlText
	q.s("INSERT INTO " + e.names.of(xtmodel.Meta) + " (_id, kind, last_seq, sys_us, horizon_ns, past_end) VALUES (")
	q.p(metaID(n)).s(", ").p(kind).s(", ").p(seqToDB(last)).s(", ").p(int64(sys) / nanosPerMicro).s(", ").p(hz.ns).s(", ").p(pastEndFlag(hz.pastEnd)).s(")")
	return q.stmt()
}

// pastEndFlag is the stored form of a bool: the node casts no string to one.
func pastEndFlag(b bool) int64 {
	if b {
		return 1
	}
	return 0
}

// beginWrite is the statement that opens a transaction at a system time.
func beginWrite(sys xtmodel.Instant) string {
	return "BEGIN READ WRITE WITH (SYSTEM_TIME = " + literal(systemTimestamp(sys)) + ")"
}

const (
	beginRead  = "BEGIN READ ONLY"
	commitTx   = "COMMIT"
	rollbackTx = "ROLLBACK"
)
