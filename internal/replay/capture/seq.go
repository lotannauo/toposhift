package capture

import (
	"cmp"
	"encoding/json"
	"slices"
)

// SeqKey is the attribute a capture writer puts on every log record to fix
// the order of replay: an intValue, unique and ascending in the order the
// records were captured.
const SeqKey = "toposhift.capture.seq"

// StampLogs sets [SeqKey] on every record of d, in document order, with the
// value of next, which is called once per record. A record that already has a
// SeqKey attribute gets the new value in its place, and any further SeqKey
// attributes it had are dropped; one without gets the attribute at the end of
// its attributes.
//
// StampLogs works on the typed model, so a capture stamped by decoding its
// lines with [DecodeLogs] and encoding them again with [EncodeLogs] is not the
// capture it was: fields the model does not name are dropped (the OTLP/JSON
// rule for unknown fields), hex IDs come out in lowercase, and the key order
// and spacing are the encoder's. [Line.Raw] round trips are byte exact;
// stamped ones are not. The fields the model names, [Resource.EntityRefs]
// among them, are kept.
func StampLogs(d *LogsData, next func() int64) {
	for i := range d.ResourceLogs {
		scopes := d.ResourceLogs[i].ScopeLogs
		for j := range scopes {
			records := scopes[j].LogRecords
			for k := range records {
				stamp(&records[k], next())
			}
		}
	}
}

func stamp(r *LogRecord, seq int64) {
	value := Int64(seq)
	kv := KeyValue{Key: SeqKey, Value: AnyValue{IntValue: &value}}
	placed := false
	attrs := r.Attributes[:0:0]
	for _, a := range r.Attributes {
		if a.Key != SeqKey {
			attrs = append(attrs, a)
			continue
		}
		if !placed {
			attrs = append(attrs, kv)
			placed = true
		}
	}
	if !placed {
		attrs = append(attrs, kv)
	}
	r.Attributes = attrs
}

// LogRef locates one log record in a capture, and holds what replay order
// depends on. It is small: a caller can stream a capture, keep the refs and
// not the lines, and order them with [SortRefs]. A ref names its line by Index
// only; to read the line again the caller keeps its own table from Index to
// (file, offset), filled from [Line.Offset] as it streams, and reads with a new
// [Reader] from that offset.
type LogRef struct {
	// Index is the position of the record's line in the slice given to
	// [OrderLogs], or the index given to [LogRefs]. Line numbers are not
	// unique across the files of a rotated capture; the index is.
	Index                         int
	Line, Resource, Scope, Record int // line number (1-based) and indexes
	Seq                           int64
	HasSeq                        bool
	Time, Observed                uint64 // the record's TimeUnixNano and ObservedTimeUnixNano
}

// The records of a line, as far as order goes. Decoding this reads the times
// and the keys of the attributes, and decodes the value of the SeqKey
// attribute only; the bodies and the other attribute values, which are most of
// a line, are skipped without being built.
type (
	lightLogs struct {
		ResourceLogs []lightResource `json:"resourceLogs"`
	}
	lightResource struct {
		ScopeLogs []lightScope `json:"scopeLogs"`
	}
	lightScope struct {
		LogRecords []lightRecord `json:"logRecords"`
	}
	lightRecord struct {
		Time       Uint64      `json:"timeUnixNano"`
		Observed   Uint64      `json:"observedTimeUnixNano"`
		Attributes []attrProbe `json:"attributes"`
	}
)

// attrProbe is an attribute of which only the SeqKey one is read.
type attrProbe struct {
	isSeq, isInt bool
	seq          int64
}

func (p *attrProbe) UnmarshalJSON(b []byte) error {
	var k struct {
		Key string `json:"key"`
	}
	if err := json.Unmarshal(b, &k); err != nil {
		return err
	}
	if k.Key != SeqKey {
		return nil
	}
	var kv KeyValue
	if err := json.Unmarshal(b, &kv); err != nil {
		return err
	}
	p.isSeq = true
	if kv.Value.IntValue != nil {
		p.isInt = true
		p.seq = int64(*kv.Value.IntValue)
	}
	return nil
}

// LogRefs returns the refs of the records of one line of a logs capture, in
// document order, with Index set to index. It reads each record's times and
// its [SeqKey] attribute, and nothing else: it does not decode bodies or other
// attributes, so it is cheap on large lines, and it does not validate them
// either ([DecodeLogs] does). Errors wrap [ErrFormat] and carry the line
// number: a line that is not a logs line, a sequence that is not an intValue,
// and two sequences on one record.
func LogRefs(l Line, index int) ([]LogRef, error) {
	sig, err := parseLine(l.Raw)
	if err != nil {
		return nil, withLine(l.Number, err)
	}
	if sig != Logs {
		return nil, formatErrorf(l.Number, nil, "%s line, not logs", sig)
	}
	var d lightLogs
	if err := json.Unmarshal(l.Raw, &d); err != nil {
		return nil, formatErrorf(l.Number, err, "logs: %v", err)
	}
	var refs []LogRef
	for i, rl := range d.ResourceLogs {
		for j, sl := range rl.ScopeLogs {
			for k, rec := range sl.LogRecords {
				ref := LogRef{
					Index: index, Line: l.Number, Resource: i, Scope: j, Record: k,
					Time: uint64(rec.Time), Observed: uint64(rec.Observed),
				}
				seqs := 0
				for _, a := range rec.Attributes {
					if !a.isSeq {
						continue
					}
					if seqs++; seqs > 1 {
						return nil, formatErrorf(l.Number, nil, "resource %d, scope %d, record %d: more than one %s attribute", i, j, k, SeqKey)
					}
					if !a.isInt {
						return nil, formatErrorf(l.Number, nil, "resource %d, scope %d, record %d: %s is not an intValue", i, j, k, SeqKey)
					}
					ref.Seq, ref.HasSeq = a.seq, true
				}
				refs = append(refs, ref)
			}
		}
	}
	return refs, nil
}

// OrderLogs returns the replay order of the records of a capture's lines. It
// is [LogRefs] of each line, with Index set to the line's position in lines,
// followed by [SortRefs]; see SortRefs for the rule and the errors. Like
// LogRefs, it does not validate bodies or other attributes, so a caller that
// needs all-or-nothing should [DecodeLogs] every line in its first pass.
func OrderLogs(lines []Line) ([]LogRef, error) {
	var refs []LogRef
	for i, line := range lines {
		r, err := LogRefs(line, i)
		if err != nil {
			return nil, err
		}
		refs = append(refs, r...)
	}
	if err := SortRefs(refs); err != nil {
		return nil, err
	}
	return refs, nil
}

// SortRefs puts refs in replay order, in place.
//
// If every ref has a sequence ([LogRef.HasSeq]), the order is by [LogRef.Seq],
// and a repeated value is an error. Otherwise, if none has one, the order is by
// (Time, Observed, Index, Resource, Scope, Record). In that order a record
// whose Time is 0, which OTLP defines as unset, is placed by its Observed
// time instead: the collector's own convention is that observed time stands
// in when the event time is unknown. Ties on that time are broken by Observed
// as recorded, then by position.
//
// Refs with distinct positions (Index, Resource, Scope, Record), as those of
// [OrderLogs] and of [LogRefs] called with distinct indexes are, have a total
// order: no two compare equal. The order depends on the order of the input
// only through the last of these tie-breaks: refs whose times are equal are
// ordered by their Index, which is the line's position in the slice the
// caller gave, so lines given in another order tie-break in that order.
// Line numbers play no part, as they repeat across the files of a rotated
// capture.
//
// A mix of refs with and without a sequence is an error that names the first
// ref, in the order given, whose presence differs from the first ref's.
// Errors wrap [ErrFormat].
func SortRefs(refs []LogRef) error {
	if len(refs) == 0 {
		return nil
	}
	first := refs[0]
	for _, r := range refs[1:] {
		if r.HasSeq == first.HasSeq {
			continue
		}
		have, firstHas := "has no", "has one"
		if r.HasSeq {
			have, firstHas = "has", "has none"
		}
		return formatErrorf(r.Line, nil,
			"resource %d, scope %d, record %d (index %d) %s %s, but the first record (line %d, index %d, resource %d, scope %d, record %d) %s: stamp every record or none",
			r.Resource, r.Scope, r.Record, r.Index, have, SeqKey,
			first.Line, first.Index, first.Resource, first.Scope, first.Record, firstHas)
	}
	if !first.HasSeq {
		slices.SortStableFunc(refs, compareByTime)
		return nil
	}
	slices.SortStableFunc(refs, func(a, b LogRef) int { return cmp.Compare(a.Seq, b.Seq) })
	for i := 1; i < len(refs); i++ {
		if refs[i].Seq != refs[i-1].Seq {
			continue
		}
		p, q := refs[i-1], refs[i]
		return formatErrorf(q.Line, nil,
			"%s %d repeats: on line %d, resource %d, scope %d, record %d and on line %d, resource %d, scope %d, record %d",
			SeqKey, q.Seq, p.Line, p.Resource, p.Scope, p.Record, q.Line, q.Resource, q.Scope, q.Record)
	}
	return nil
}

func compareByTime(a, b LogRef) int {
	at, bt := a.Time, b.Time
	if at == 0 {
		at = a.Observed
	}
	if bt == 0 {
		bt = b.Observed
	}
	return cmp.Or(
		cmp.Compare(at, bt),
		cmp.Compare(a.Observed, b.Observed),
		cmp.Compare(a.Index, b.Index),
		cmp.Compare(a.Resource, b.Resource),
		cmp.Compare(a.Scope, b.Scope),
		cmp.Compare(a.Record, b.Record),
	)
}
