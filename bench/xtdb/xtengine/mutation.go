package xtengine

// mutation names a deliberate fault. The tests switch one on, in an engine they build
// themselves, to prove that the checks catch it: a check that no fault can fail proves
// nothing. Nothing outside the tests ever sets one, and the zero value is the correct
// behaviour, so every switch is dead code in a build of the package that is not under
// test.
type mutation uint8

const (
	_ mutation = iota

	// mutNoSystemTime reads every token as the newest, leaving out the system-time
	// clause.
	mutNoSystemTime
	// mutMidBatchBySystemTime answers a token inside a batch from the batch before it
	// by system time, instead of folding the log.
	mutMidBatchBySystemTime
	// mutNoLayerFilter ignores the scope's layer.
	mutNoLayerFilter
	// mutBatchDropsEmpty leaves an entity with no edges out of a batched read, so the
	// answers no longer line up with the entities.
	mutBatchDropsEmpty
	// mutAcceptBeforeHorizon accepts a write before the retention horizon.
	mutAcceptBeforeHorizon
	// mutFailedBatchTakesOrdinal counts a refused batch among the committed ones.
	mutFailedBatchTakesOrdinal
	// mutRepeatSystemTime gives the next batch the system time of the one before.
	mutRepeatSystemTime
	// mutLastSeqEarly raises LastSeq before the transaction has committed.
	mutLastSeqEarly
	// mutRoundMicros rounds the instant of a read to the nearest microsecond.
	mutRoundMicros
	// mutCeilMicros rounds the instant of a read up to a whole microsecond.
	mutCeilMicros
	// mutWriteOffMicrosecond writes a boundary that is not a whole microsecond,
	// truncated, instead of refusing the batch.
	mutWriteOffMicrosecond
	// mutNoReadRetry does not make a read again whose plan the node found out of date.
	mutNoReadRetry
	// mutNoDeadline gives a write no deadline.
	mutNoDeadline
	// mutNoReadBack does not read back the meta document of a transaction.
	mutNoReadBack
	// mutRawPayload sends a payload as bytes, which the driver quotes as text and the node
	// cannot cast to a binary value.
	mutRawPayload
	// mutRetainNoTransaction keeps a horizon in memory and writes nothing.
	mutRetainNoTransaction
)
