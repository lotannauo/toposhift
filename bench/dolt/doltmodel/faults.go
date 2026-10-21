package doltmodel

// faults are deliberate defects that tests switch on to prove that each check written for
// one can fail (tests set them through export_test.go). Every field is false in a model
// anyone else holds, and each stands for one way a writer or a reader of these tables goes
// wrong.
type faults struct {
	// dropSeqFilter: a read ignores the snapshot token's sequence filter.
	dropSeqFilter bool
	// tieByEvent: the newest record of a key is chosen by event time alone, so a tie at
	// one instant is not won by the higher sequence number.
	tieByEvent bool
	// stateKeepsLast: the state row is the last record written, not the newest.
	stateKeepsLast bool
	// commitAfter: a read as of a token reads the state of the commit after it.
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
	// ignoreCapacity: a payload over the column's capacity is accepted.
	ignoreCapacity bool
	// ignoreProducerLen: a producer over the column's size is accepted.
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
	// fallbackNoSeq: a key sent to the log is answered without the token's sequence filter.
	fallbackNoSeq bool
	// pathPerQuery: the state is used for the whole read or for none of it, by the token's
	// frontier, instead of key by key.
	pathPerQuery bool
	// diagUsesLog: the diagnostic arm answers from the log, as the real design does.
	diagUsesLog bool
}
