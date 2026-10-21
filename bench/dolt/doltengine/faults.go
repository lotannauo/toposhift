package doltengine

// faults are deliberate defects that tests switch on to prove that each check written
// for one can fail (tests set them through export_test.go before the engine is used).
// Every field is false in an engine anyone else holds, and each stands for one way a
// writer or a reader of these tables goes wrong.
type faults struct {
	// dropSeqFilter: a read from the log ignores the snapshot token's sequence filter.
	dropSeqFilter bool
	// tieByEvent: the newest record of a key is chosen by event time alone, so a tie at
	// one instant is not won by the higher sequence number.
	tieByEvent bool
	// stateKeepsLast: the state row is the last record written, not the newest.
	stateKeepsLast bool
	// commitAfter: a read as of a token reads the commit after it.
	commitAfter bool
	// windowDedup: a window leaves out a record a later one overwrites at its instant.
	windowDedup bool
	// batchDropsEmpty: a batched read leaves out the entities that have no edges.
	batchDropsEmpty bool
	// retainDropsHolding: retention deletes the baseline, the newest row before the
	// horizon, which may hold at it.
	retainDropsHolding bool
	// retainDropsOpenState: retention deletes state rows that have no deadline.
	retainDropsOpenState bool
	// acceptBeforeHorizon: a write before the retention horizon is accepted.
	acceptBeforeHorizon bool
	// deadlineInclusive: a record still holds at the instant of its deadline.
	deadlineInclusive bool
	// skipValidate: a record is not validated.
	skipValidate bool
	// ignoreCapacity: a payload over the column's capacity is sent to the server.
	ignoreCapacity bool
	// ignoreProducerLen: a producer over the column's size is sent to the server.
	ignoreProducerLen bool
	// acceptStaleSeq: a record whose sequence number is not above the last is accepted.
	acceptStaleSeq bool
	// acceptLayerConflict: a subject stored in one layer is accepted in another.
	acceptLayerConflict bool
	// eventBefore: a state row settles a key only if its event time is before the instant,
	// not at or before it.
	eventBefore bool
	// useRowAfterT: a state row settles its key whatever its event time.
	useRowAfterT bool
	// noLogFallback: a key whose row is after the instant is not answered from the log.
	noLogFallback bool
	// logOnly: every key is answered from the log, and the state is not read.
	logOnly bool
	// headForOldToken: a read as of a token below the newest reads the current state.
	headForOldToken bool
	// publishBeforeCommit: the sequence number rises before the batch's commits return.
	publishBeforeCommit bool
	// wallClockDate: a commit is made at the server's time, not the synthetic date.
	wallClockDate bool
	// sessionAuthor: a commit is made by the session's user, not the synthetic author.
	sessionAuthor bool
}
