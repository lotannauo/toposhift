package nodeagent

import (
	"time"

	"github.com/lotannauo/toposhift/internal/ingest/resume"
	"github.com/lotannauo/toposhift/internal/store"
)

var _ resume.Resumer = (*Translator)(nil)

// Resumed does nothing. It is told the final event time and basis of a record this
// translator marked as backdated, once the store has resumed it (see
// [resume.Apply]), and this translator marks none: it backdates nothing, and keeps
// no state that times a subject's later records. It exists so that the translator
// can be given where a [resume.Resumer] is asked for, and it ignores whatever
// handle it is given.
func (t *Translator) Resumed(any, time.Time, store.EventTimeBasis) {}

// Comparable is the part of a payload this translator wrote for a subject that
// describes it, which [resume.Apply] compares with the stored payload: all of it.
// The payload of a host is a description throughout (the allow-list of the
// resource's attributes), and the payload of every other record is empty.
func Comparable(_ store.Subject, payload []byte) []byte { return payload }
