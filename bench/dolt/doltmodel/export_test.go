package doltmodel

import "fmt"

// Fault names, as the tests switch the deliberate defects of faults.go on.
const (
	FaultDropSeqFilter        = "drops the sequence filter of a read"
	FaultTieByEvent           = "chooses a tie at one instant by event time alone"
	FaultStateKeepsLast       = "replaces the state with an older late record"
	FaultCommitAfter          = "reads the commit after the token"
	FaultWindowDedup          = "deduplicates a window"
	FaultBatchDropsEmpty      = "drops an empty list from a batch"
	FaultRetainDropsHolding   = "deletes the baseline that holds at the horizon"
	FaultRetainDropsOpenState = "deletes open state rows at retention"
	FaultAcceptBeforeHorizon  = "accepts a write before the horizon"
	FaultDeadlineInclusive    = "holds a record at its deadline"
	FaultSkipValidate         = "does not validate a record"
	FaultIgnoreCapacity       = "accepts a payload over the column's capacity"
	FaultIgnoreProducerLen    = "accepts a producer over the column's size"
	FaultAcceptStaleSeq       = "accepts a sequence number that is not above the last"
	FaultAcceptLayerConflict  = "accepts a subject in a second layer"
	FaultEventBefore          = "settles a key by a row at the instant only if it is before it"
	FaultUseRowAfterT         = "uses a state row whose event is after the instant"
	FaultFallbackNoSeq        = "answers a key from the log without the token's filter"
	FaultPathPerQuery         = "chooses the state or the log for the whole read"
	FaultDiagUsesLog          = "answers the diagnostic arm from the log"
)

// Break switches a deliberate defect on, by one of the names above.
func (m *Model) Break(name string) {
	switch name {
	case FaultDropSeqFilter:
		m.f.dropSeqFilter = true
	case FaultTieByEvent:
		m.f.tieByEvent = true
	case FaultStateKeepsLast:
		m.f.stateKeepsLast = true
	case FaultCommitAfter:
		m.f.commitAfter = true
	case FaultWindowDedup:
		m.f.windowDedup = true
	case FaultBatchDropsEmpty:
		m.f.batchDropsEmpty = true
	case FaultRetainDropsHolding:
		m.f.retainDropsHolding = true
	case FaultRetainDropsOpenState:
		m.f.retainDropsOpenState = true
	case FaultAcceptBeforeHorizon:
		m.f.acceptBeforeHorizon = true
	case FaultDeadlineInclusive:
		m.f.deadlineInclusive = true
	case FaultSkipValidate:
		m.f.skipValidate = true
	case FaultIgnoreCapacity:
		m.f.ignoreCapacity = true
	case FaultIgnoreProducerLen:
		m.f.ignoreProducerLen = true
	case FaultAcceptStaleSeq:
		m.f.acceptStaleSeq = true
	case FaultAcceptLayerConflict:
		m.f.acceptLayerConflict = true
	case FaultEventBefore:
		m.f.eventBefore = true
	case FaultUseRowAfterT:
		m.f.useRowAfterT = true
	case FaultFallbackNoSeq:
		m.f.fallbackNoSeq = true
	case FaultPathPerQuery:
		m.f.pathPerQuery = true
	case FaultDiagUsesLog:
		m.f.diagUsesLog = true
	default:
		panic(fmt.Sprintf("doltmodel: no fault %q", name))
	}
}
