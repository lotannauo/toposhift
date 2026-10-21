package duckmodel

import (
	"fmt"
	"time"

	"github.com/lotannauo/toposhift/bench/spike/engine"
	"github.com/lotannauo/toposhift/internal/store"
)

// StoreRecord converts an engine record to the store record an activity file holds: the
// same layer, subject, producer, times, sequence number, kind, TTL, through time and
// payload. The two fields a store record has and an engine record does not, the boot id
// and the event time basis, take their zero values (no boot, unknown basis).
func StoreRecord(r engine.Record) (store.Record, error) { return storeRecord(r, mutNone) }

func storeRecord(r engine.Record, mut mutant) (store.Record, error) {
	s := store.Subject{A: r.Subject.A, B: r.Subject.B, Relation: r.Subject.Relation}
	switch r.Subject.Kind {
	case engine.SubjectEntity:
		s.Kind = store.SubjectEntity
	case engine.SubjectEdge:
		s.Kind = store.SubjectEdge
	default:
		return store.Record{}, fmt.Errorf("duckmodel: record seq %d has subject kind %d", r.Seq, r.Subject.Kind)
	}
	out := store.Record{
		Layer: r.Layer, Subject: s, Producer: r.Producer, EventTime: r.EventTime, Seq: r.Seq,
		Kind: r.Kind, TTL: r.TTL, Through: r.Through, Payload: r.Payload,
	}
	if mut == mutConvertDropsThrough {
		out.Through = time.Time{}
	}
	return out, nil
}

// Options says what to do with the two fields a store record has and an engine record
// does not. Neither changes what exists, so dropping them cannot change an answer: the
// basis says which clock stamped the event time and no read depends on it, and the boot
// id matters only to a fold under a policy that names it, which the baseline does not
// use. Without the option a record that has the field is an error, so a stream that has
// it is not converted by accident.
type Options struct {
	// DropBasis accepts a record whose event time basis is not unknown, and drops it.
	DropBasis bool
	// DropBoot accepts a record that has a boot id, and drops it.
	DropBoot bool
}

// EngineRecord is the inverse of [StoreRecord], for the fields an engine record holds.
func EngineRecord(r store.Record, o Options) (engine.Record, error) {
	if r.Boot != "" && !o.DropBoot {
		return engine.Record{}, fmt.Errorf("duckmodel: record seq %d has a boot id, which an engine record does not hold (DropBoot accepts it)", r.Seq)
	}
	if r.EventTimeBasis != store.BasisUnknown && !o.DropBasis {
		return engine.Record{}, fmt.Errorf("duckmodel: record seq %d has an event time basis (%s), which an engine record does not hold (DropBasis accepts it)", r.Seq, r.EventTimeBasis)
	}
	s := engine.Subject{A: r.Subject.A, B: r.Subject.B, Relation: r.Subject.Relation}
	switch r.Subject.Kind {
	case store.SubjectEntity:
		s.Kind = engine.SubjectEntity
	case store.SubjectEdge:
		s.Kind = engine.SubjectEdge
	default:
		return engine.Record{}, fmt.Errorf("duckmodel: record seq %d has subject kind %d", r.Seq, r.Subject.Kind)
	}
	return engine.Record{
		Layer: r.Layer, Subject: s, Producer: r.Producer, EventTime: r.EventTime, Seq: r.Seq,
		Kind: r.Kind, TTL: r.TTL, Through: r.Through, Payload: r.Payload,
	}, nil
}
