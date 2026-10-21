// Package pginput reads the streams the Postgres baseline is loaded from, other than
// the synthetic workload (which runner.Drive regenerates): an activity file, the
// Parquet file of store records the product replays, as engine records in the
// ascending Seq order the file keeps them in.
package pginput

import (
	"errors"
	"fmt"
	"io"
	"os"

	"github.com/lotannauo/toposhift/bench/spike/engine"
	"github.com/lotannauo/toposhift/internal/replay/activity"
	"github.com/lotannauo/toposhift/internal/store"
)

// Options says what to do with the two fields a store record has and an engine record
// does not. Neither changes what exists, so dropping them cannot change an interval:
// the basis says which clock stamped the event time and no fold depends on it, and the
// boot id matters only to a fold under a policy that names it, which the baseline does
// not use. Without the option a record that has the field is an error, so a stream that
// has it is not converted by accident.
type Options struct {
	// DropBasis accepts a record whose event time basis is not unknown, and drops it.
	// The placement streams of the Alibaba trace have one on every record.
	DropBasis bool
	// DropBoot accepts a record that has a boot id, and drops it.
	DropBoot bool
}

// Convert returns the engine record of a store record: the same layer, subject,
// producer, times, sequence number, kind, TTL, through time and payload.
func Convert(r store.Record, o Options) (engine.Record, error) {
	if r.Boot != "" && !o.DropBoot {
		return engine.Record{}, fmt.Errorf("pginput: record seq %d has a boot id, which an engine record does not hold (DropBoot accepts it)", r.Seq)
	}
	if r.EventTimeBasis != store.BasisUnknown && !o.DropBasis {
		return engine.Record{}, fmt.Errorf("pginput: record seq %d has an event time basis (%s), which an engine record does not hold (DropBasis accepts it)", r.Seq, r.EventTimeBasis)
	}
	s := engine.Subject{A: r.Subject.A, B: r.Subject.B, Relation: r.Subject.Relation}
	switch r.Subject.Kind {
	case store.SubjectEntity:
		s.Kind = engine.SubjectEntity
	case store.SubjectEdge:
		s.Kind = engine.SubjectEdge
	default:
		return engine.Record{}, fmt.Errorf("pginput: record seq %d has subject kind %d", r.Seq, r.Subject.Kind)
	}
	return engine.Record{
		Layer: r.Layer, Subject: s, Producer: r.Producer, EventTime: r.EventTime, Seq: r.Seq,
		Kind: r.Kind, TTL: r.TTL, Through: r.Through, Payload: r.Payload,
	}, nil
}

// ErrStop, returned by the function ForEach calls, ends the read without an error.
var ErrStop = errors.New("pginput: stop")

// ForEach reads an activity file and calls fn with its records, in batches of up to
// batch records in the file's order (the last batch may be shorter). The slice is
// not reused. It returns the file's description from its footer; the content digest
// in it has been checked against the rows only if the read reached the end, so a read
// that fn stopped with ErrStop has verified only the row groups it consumed.
func ForEach(path string, batch int, o Options, fn func([]engine.Record) error) (activity.Info, error) {
	f, err := os.Open(path)
	if err != nil {
		return activity.Info{}, err
	}
	defer f.Close()
	st, err := f.Stat()
	if err != nil {
		return activity.Info{}, err
	}
	return ForEachReader(f, st.Size(), batch, o, fn)
}

// ForEachReader is [ForEach] over a reader.
func ForEachReader(r io.ReaderAt, size int64, batch int, o Options, fn func([]engine.Record) error) (activity.Info, error) {
	if batch < 1 {
		return activity.Info{}, fmt.Errorf("pginput: a batch of %d records", batch)
	}
	rd, err := activity.NewReader(r, size, activity.ReaderOptions{})
	if err != nil {
		return activity.Info{}, err
	}
	defer rd.Close()
	info := rd.Info()
	buf := make([]engine.Record, 0, batch)
	flush := func() error {
		if len(buf) == 0 {
			return nil
		}
		err := fn(buf)
		buf = make([]engine.Record, 0, batch)
		return err
	}
	for {
		r, err := rd.Next()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return info, err
		}
		e, err := Convert(r, o)
		if err != nil {
			return info, err
		}
		buf = append(buf, e)
		if len(buf) == batch {
			if err := flush(); err != nil {
				if errors.Is(err, ErrStop) {
					return info, nil
				}
				return info, err
			}
		}
	}
	if err := flush(); err != nil && !errors.Is(err, ErrStop) {
		return info, err
	}
	return info, nil
}

// Load writes an activity file to an engine in batches of batch records, the way a
// store ingests it, and returns the file's description and the records written. limit,
// if positive, stops after about that many records (a whole number of batches).
func Load(e engine.Engine, path string, batch int, limit int64, o Options) (activity.Info, int64, error) {
	var n int64
	info, err := ForEach(path, batch, o, func(rs []engine.Record) error {
		if err := e.Write(rs); err != nil {
			return err
		}
		n += int64(len(rs))
		if limit > 0 && n >= limit {
			return ErrStop
		}
		return nil
	})
	return info, n, err
}
