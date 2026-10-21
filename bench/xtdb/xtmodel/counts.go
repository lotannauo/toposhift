package xtmodel

// TableCounts is the statements a planner wrote to one table: documents inserted
// (an insert over a valid-time range is one statement, whatever it overwrites) and
// valid-time portions deleted.
type TableCounts struct {
	Inserts, PortionDeletes uint64
}

// Counts is what a planner has planned, in the units an XTDB writer is charged in. It
// is a function of the stream, the batch boundaries and the planner, so it is the
// same on every run and every machine, with no server.
type Counts struct {
	// Batches are the transactions committed, and Records the records they held.
	Batches, Records uint64
	// Tables are the statements per table.
	Tables [NumTables]TableCounts
	// InOrder are the records planned from the planner's memory of the subject and
	// producer, which is every record whose event time is not before the producer's
	// latest on that subject; Late are the records that needed the log to find the
	// producer's next assertion (the slow path).
	InOrder, Late uint64
	// MergedAway are the segment statements the merge of a batch's statements for one
	// document saved: the statements the records asked for, less the statements
	// written.
	MergedAway uint64
	// OffMicrosecond are the records with a boundary (an event time or a finite
	// deadline) that is not a whole microsecond. A server adapter, whose temporal
	// columns hold microseconds, cannot write them; the model, which holds
	// nanoseconds, can.
	OffMicrosecond uint64
}

// Statements is the number of statements: inserts and portion deletes in every table.
func (c Counts) Statements() uint64 {
	var n uint64
	for _, t := range c.Tables {
		n += t.Inserts + t.PortionDeletes
	}
	return n
}

// SegmentInserts is the inserts on the segment tables (seg_f, seg_r and ent), the
// statements the planner decides; the log and meta tables take one insert per record
// and batch whatever the planner does.
func (c Counts) SegmentInserts() (n uint64) {
	for t := range NumTables {
		if Table(t).segment() {
			n += c.Tables[t].Inserts
		}
	}
	return n
}

// SegmentPortionDeletes is the portion deletes on the segment tables.
func (c Counts) SegmentPortionDeletes() (n uint64) {
	for t := range NumTables {
		if Table(t).segment() {
			n += c.Tables[t].PortionDeletes
		}
	}
	return n
}

func (c *Counts) add(o Counts) {
	c.Batches += o.Batches
	c.Records += o.Records
	for i := range c.Tables {
		c.Tables[i].Inserts += o.Tables[i].Inserts
		c.Tables[i].PortionDeletes += o.Tables[i].PortionDeletes
	}
	c.InOrder += o.InOrder
	c.Late += o.Late
	c.MergedAway += o.MergedAway
	c.OffMicrosecond += o.OffMicrosecond
}
