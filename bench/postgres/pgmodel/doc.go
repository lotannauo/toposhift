// Package pgmodel is an in-memory model of the Postgres interval tables the
// Postgres baseline stores a stream in, with no database server and no driver.
//
// A Postgres engine keeps three things: a log of every stored record (what Window
// and a read as of an old snapshot token need), a table of maximal existence
// intervals per edge, and one per entity (what a read at an instant needs). The
// intervals are derived from the log by [lifecycle.Fold], so the rows an engine
// writes are decided by a planner, not by the database. This package is that
// planner and a [Model] that applies its row changes to sorted slices, with the
// semantics the SQL has, so that:
//
//   - the planner is checked against the reference engine by the spike's own
//     conformance test, with no server;
//   - a SQL engine can be checked against the Model, row for row;
//   - the number of rows a stream inserts, updates and deletes is a function of
//     the stream and the planner, and can be counted anywhere.
//
// # Encodings
//
// A fingerprint is its type name, one zero byte and its 16 hash bytes
// ([EncodeFingerprint]); comparing those bytes is [engine.CompareFingerprints], so
// ORDER BY on a bytea column needs no collation. Times are Unix nanoseconds in an
// int64, the range the engine contract allows; a read at an instant outside it
// clamps ([ClampInstant]).
//
// # Rows and the read rule
//
// An interval row is [Start, End) over int64 nanoseconds, with End unbounded for
// an open interval. A row may also carry a Deadline: while a writer keeps a row
// provisional its range stays unbounded and the Deadline is the current end, so
// that extending it does not touch an index. The end of a row is the Deadline if
// it has one and the range's upper bound otherwise ([Row.End]). A read at t finds
// the row with Start <= t < End; at most one row of a subject contains t, because
// the rows of a subject are maximal intervals and so never touch.
//
// # Writing
//
// [Writer] plans the row changes of a batch, and [Model.Write] applies them to sorted
// slices. A subject's rows are always the maximal intervals of its records folded with
// [lifecycle.Fold], each in the form the stream decides: an interval that ends by a
// deadline after the newest event time written is a provisional row (a range left open
// and the deadline in a column of its own, which no index covers), and any other is a
// final row or an open one. A batch reaches them in one of two ways.
//
// A record that can only lengthen the last interval of its subject takes the fast path:
// it needs no read, only what the writer remembers of the last row and of each producer's
// latest assertion, and is one insert or one update of a deadline. That is exact when
// the record is an Observe whose producer has asserted nothing later and nothing longer
// (so that the segments the producer has asserted cover all they covered), and it begins
// at or after the last interval's start: it then ends the interval at the later of the
// interval's end and its own deadline, starts an interval of its own if it begins after
// the end, and changes nothing if its deadline is inside it. Any other record is a
// re-fold: from S, the start of the last stored interval that begins at or before the
// batch's earliest event time for the subject, the subject is re-folded and the rows from
// S on that differ are replaced. That is exact: a maximal interval that starts at S
// contains every segment that overlaps or touches S, so no segment of a record before S
// reaches S, and nothing before S changes. A late event therefore costs a re-fold of the
// subject's history from the interval it lands in, not of all its history. After each
// batch the finalizer closes every provisional row whose deadline the newest event time
// has passed.
//
// # Retention
//
// [Model.Retain] keeps, for each subject and producer, the last record before the
// horizon (a baseline), drops the rest of the log before it (a day partition of the log
// that is wholly before the horizon is dropped whole, and the others have those rows
// deleted), and deletes the interval rows that end at or before it. A row that starts before the horizon and
// ends after it is kept as it is (no engine would rewrite it), so an interval's
// start may precede the horizon; [Tables.Clipped] clips rows to the horizon before
// any two tables are compared. A baseline stands for everything before it: the
// segments of the records it replaces end at or before its event time. A row that
// straddles the horizon keeps its old start until the subject is next re-folded, and
// that fold, which sees only the baselines, restarts the row at the earliest baseline
// in it: the first re-fold after a retention costs such a row one delete and one insert
// (the start is part of the key) where an extension would cost one update.
package pgmodel
