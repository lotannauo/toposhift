package duckmodel

// mutant is a deliberate fault. Only tests set one: each is a fault a test of this
// package must catch, named for what it gets wrong. The zero value is the honest model.
type mutant uint8

const (
	mutNone mutant = iota
	// The read rule.
	mutTieByEventOnly    // of the records at one instant, the lowest Seq decides, not the highest
	mutDeadlineFromEvent // a deadline counts from the event time, ignoring the through time
	mutDeleteHolds       // a delete holds like an observation without a TTL
	mutTTLZeroExpires    // an observation without a TTL has lapsed
	mutSeqIgnored        // the snapshot token is not applied to the rule
	// The reads.
	mutWindowDedups    // Window leaves out a record a later record of its producer overwrites
	mutBatchDropsEmpty // NeighborsBatch leaves out the entities with no neighbor
	mutBatchSorted     // NeighborsBatch answers in the order of the sorted fingerprints
	mutNoClamp         // an instant is converted without clamping it first
	mutWindowInclusive // Window's upper bound is inclusive
	mutOrderByText     // neighbors are ordered by the text of the fingerprint
	// Retention.
	mutBaselinePerSubject // one baseline per subject, not per subject and producer
	mutBaselineBySeq      // the baseline is the record with the highest Seq, not the latest event
	// The interval file.
	mutIntervalsSplit   // the intervals of each producer alone, so intervals that touch stay split
	mutOpenGetsEnd      // an open interval is given an end
	mutEndSourceDropped // an interval's end source is not kept
	// Conversion.
	mutConvertDropsThrough // an engine record's through time is not carried to the store record
)
