package xtmodel

// mutation names a deliberate fault. The tests switch one on, in a planner or a
// model they build themselves, to prove that the checks catch it: a check that no
// fault can fail proves nothing. Nothing outside the tests ever sets one, and the
// zero value is the correct behaviour, so every switch below is dead code in a
// build of the package that is not under test.
type mutation uint8

const (
	_ mutation = iota

	// Planner faults.

	// mutLateNextIsEnd takes the next assertion of a late record to be the end of
	// time, so its segment and its delete reach past assertions that come after it.
	mutLateNextIsEnd
	// mutKeepLeftover does not delete the portion between a shrinking deadline and
	// the next assertion.
	mutKeepLeftover
	// mutDeleteToEnd deletes from a late delete's instant to the end of time instead
	// of to the next assertion.
	mutDeleteToEnd
	// mutLowerSeqWins lets the earlier record keep an instant that two records of one
	// producer share, instead of the later one.
	mutLowerSeqWins
	// mutMergeOutOfSeqOrder merges a batch's statements for one document in the
	// reverse of their Seq order.
	mutMergeOutOfSeqOrder

	// Model faults.

	// mutNoSystemTime reads every token as the newest, leaving out the system-time
	// clause.
	mutNoSystemTime
	// mutMidBatchBySystemTime answers a token inside a batch from the batch before it
	// by system time, instead of folding the log.
	mutMidBatchBySystemTime
	// mutNoLayerFilter ignores the scope's layer.
	mutNoLayerFilter
	// mutWindowDedup drops a record that a later record of the same producer at the
	// same instant overwrites in effect.
	mutWindowDedup
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
)
