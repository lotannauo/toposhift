// Package vminput reads the streams the metrics-store baseline is loaded from, other
// than the synthetic workload (which runner.Drive regenerates): an activity file, the
// Parquet file of store records the product replays, as engine records in the ascending
// Seq order the file keeps them in; the registry of the Alibaba trace's placement
// producer; and the question set of such a stream, chosen by the spike's own selector and
// answered by the reference engine fed the same file.
//
// It is this module's own reader. The baselines are independent modules, so the reader
// of another one is not imported.
package vminput

import (
	"errors"
	"fmt"
	"io"
	"os"

	"github.com/lotannauo/toposhift/bench/spike/engine"
	"github.com/lotannauo/toposhift/internal/replay/activity"
	"github.com/lotannauo/toposhift/internal/store"
)

// Convert returns the engine record of a store record: the same layer, subject,
// producer, times, sequence number, kind, TTL, through time and payload. The event time
// basis (which clock stamped the event time; the placement streams of the Alibaba trace
// have one on every record) is dropped, because no fold depends on it. A record with a
// boot id is an error: only an observation of a host has one, and a stream that has it
// is not one the baseline's series model describes.
func Convert(r store.Record) (engine.Record, error) {
	if r.Boot != "" {
		return engine.Record{}, fmt.Errorf("vminput: record seq %d has a boot id, which an engine record does not hold", r.Seq)
	}
	s := engine.Subject{A: r.Subject.A, B: r.Subject.B, Relation: r.Subject.Relation}
	switch r.Subject.Kind {
	case store.SubjectEntity:
		s.Kind = engine.SubjectEntity
	case store.SubjectEdge:
		s.Kind = engine.SubjectEdge
	default:
		return engine.Record{}, fmt.Errorf("vminput: record seq %d has subject kind %d", r.Seq, r.Subject.Kind)
	}
	return engine.Record{
		Layer: r.Layer, Subject: s, Producer: r.Producer, EventTime: r.EventTime, Seq: r.Seq,
		Kind: r.Kind, TTL: r.TTL, Through: r.Through, Payload: r.Payload,
	}, nil
}

// ForEach reads an activity file and calls fn with its records, in batches of up to
// batch records in the file's order (the last batch may be shorter). The slice is not
// reused. It returns the file's description from its footer; the content digest in it has
// been checked against the rows because the read reached the end (an error of fn stops the
// read and is returned as it is).
func ForEach(path string, batch int, fn func([]engine.Record) error) (activity.Info, error) {
	if batch < 1 {
		return activity.Info{}, fmt.Errorf("vminput: a batch of %d records", batch)
	}
	f, err := os.Open(path)
	if err != nil {
		return activity.Info{}, err
	}
	defer f.Close()
	st, err := f.Stat()
	if err != nil {
		return activity.Info{}, err
	}
	rd, err := activity.NewReader(f, st.Size(), activity.ReaderOptions{})
	if err != nil {
		return activity.Info{}, fmt.Errorf("%s: %w", path, err)
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
			return info, fmt.Errorf("%s: %w", path, err)
		}
		e, err := Convert(r)
		if err != nil {
			return info, err
		}
		buf = append(buf, e)
		if len(buf) == batch {
			if err := flush(); err != nil {
				return info, err
			}
		}
	}
	return info, flush()
}

// Info reads the footer of an activity file: its record count, sequence range and content
// digest, without reading a row.
func Info(path string) (activity.Info, error) {
	f, err := os.Open(path)
	if err != nil {
		return activity.Info{}, err
	}
	defer f.Close()
	st, err := f.Stat()
	if err != nil {
		return activity.Info{}, err
	}
	rd, err := activity.NewReader(f, st.Size(), activity.ReaderOptions{})
	if err != nil {
		return activity.Info{}, fmt.Errorf("%s: %w", path, err)
	}
	defer rd.Close()
	return rd.Info(), nil
}
