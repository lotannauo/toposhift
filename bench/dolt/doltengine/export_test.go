package doltengine

import "fmt"

// Fault names, as the tests switch the deliberate defects of faults.go on.
const (
	FaultDropSeqFilter        = "drops the sequence filter of a read from the log"
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
	FaultIgnoreCapacity       = "sends a payload over the column's capacity"
	FaultIgnoreProducerLen    = "sends a producer over the column's size"
	FaultAcceptStaleSeq       = "accepts a sequence number that is not above the last"
	FaultAcceptLayerConflict  = "accepts a subject in a second layer"
	FaultEventBefore          = "settles a key by a row at the instant only if it is before it"
	FaultUseRowAfterT         = "uses a state row whose event is after the instant"
	FaultNoLogFallback        = "does not answer a key whose row is after the instant from the log"
	FaultLogOnly              = "answers every key from the log"
	FaultHeadForOldToken      = "reads the current state for an old token"
	FaultPublishBeforeCommit  = "raises the sequence number before the commits return"
	FaultWallClockDate        = "commits at the server's time"
	FaultSessionAuthor        = "commits as the session's user"
)

// Break switches a deliberate defect on, by one of the names above, before the engine is used.
func (e *Engine) Break(name string) {
	switch name {
	case FaultDropSeqFilter:
		e.f.dropSeqFilter = true
	case FaultTieByEvent:
		e.f.tieByEvent = true
	case FaultStateKeepsLast:
		e.f.stateKeepsLast = true
	case FaultCommitAfter:
		e.f.commitAfter = true
	case FaultWindowDedup:
		e.f.windowDedup = true
	case FaultBatchDropsEmpty:
		e.f.batchDropsEmpty = true
	case FaultRetainDropsHolding:
		e.f.retainDropsHolding = true
	case FaultRetainDropsOpenState:
		e.f.retainDropsOpenState = true
	case FaultAcceptBeforeHorizon:
		e.f.acceptBeforeHorizon = true
	case FaultDeadlineInclusive:
		e.f.deadlineInclusive = true
	case FaultSkipValidate:
		e.f.skipValidate = true
	case FaultIgnoreCapacity:
		e.f.ignoreCapacity = true
	case FaultIgnoreProducerLen:
		e.f.ignoreProducerLen = true
	case FaultAcceptStaleSeq:
		e.f.acceptStaleSeq = true
	case FaultAcceptLayerConflict:
		e.f.acceptLayerConflict = true
	case FaultEventBefore:
		e.f.eventBefore = true
	case FaultUseRowAfterT:
		e.f.useRowAfterT = true
	case FaultNoLogFallback:
		e.f.noLogFallback = true
	case FaultLogOnly:
		e.f.logOnly = true
	case FaultHeadForOldToken:
		e.f.headForOldToken = true
	case FaultPublishBeforeCommit:
		e.f.publishBeforeCommit = true
	case FaultWallClockDate:
		e.f.wallClockDate = true
	case FaultSessionAuthor:
		e.f.sessionAuthor = true
	default:
		panic(fmt.Sprintf("doltengine: no fault %q", name))
	}
}
