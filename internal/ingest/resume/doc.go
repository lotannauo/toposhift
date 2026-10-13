// Package resume times a record that a source translator backdates from what the
// store already holds, so that a translator that lost its state (a restart, a
// reset after a refused or failed write, a translation panic, a cluster name's
// slot freed while idle) does not re-assert every live object from its creation.
//
// # The rule
//
// A translator marks each record it backdates: the first sighting of a subject,
// timed at the object's creation (or an edge at its PodScheduled time) and carried
// through the time it was observed, and every edge resolved from a pending state.
// For each marked record, [Apply] reads the same producer's last stored assertion
// of the same subject, matched by exact identity ([store.LookupRef]), and:
//
//   - if nothing is stored, the record is a genuine first sighting and is left as
//     it is;
//   - if the stored newest is a Delete at or after the record's start, the record
//     is dropped; a Delete before its start is an earlier life, and the record is
//     left as it is;
//   - if the stored newest is an Observe whose last instant (its Through if set,
//     else its event time) is not after the record's start, the record is left as
//     it is;
//   - if the stored description equals the record's, compared on the described
//     fields only (each translator's Comparable leaves out the fields it derives
//     from its own state), the record's event time is raised to the stored last
//     instant, never above its own Through; the record keeps its own payload and
//     its Through, which is cleared if it is no longer after the event time;
//   - if the description differs, the record takes the observed time (its
//     Through) and no Through: the change happened at some unknown instant
//     between the stored assertion and the observation, and is not guessed;
//   - if the last instant is unknown (the walk reached its bound, only a baseline
//     entry is left, the store could not be read) or below the layer's horizon,
//     the record takes the observed time and no Through too.
//
// A raised record takes the stored record's basis, or the basis of its own
// observation ([Mark.ObservedBasis]) when the stored instant is a Through or the
// raise is held at the record's own Through. A record re-timed to its
// observation takes that basis too.
//
// Later records of the same subject in the same [Result] are never timed before a
// resumed record: each one timed before it takes its time and basis, as the
// translator's own clamp would have given them had it known the stored instant
// (the mirror-raise). The translator is told the final time of each record it
// marked ([Resumer]), and raises, never lowers, its own state with it, so that
// its later records are not timed before it either.
//
// The raised event time is derived from stored facts, and is documented as
// derived; the description and the Through are the producer's. Nothing is written
// that the producer did not assert: no bridge or continuity record.
//
// # Read errors
//
// A lookup that fails because the store or the lookup is closed, or because the
// context is done, aborts: [Apply] returns the error at once, before it changes
// any record, calls [Resumer], or counts anything ([IsAbort]). The caller fails
// the whole group as it would fail a write. Any other error of a lookup (a
// reference that is not valid, a key or value the store cannot decode) is a
// fallback for that record only: it takes the observed time and no Through, is
// counted as unknown with the reason error, and [Stats.FirstError] keeps the
// first such error for the caller to log. The step only spares derived
// checkpoints, so it never decides on its own whether ingest stays up.
//
// # Use
//
// A caller opens one [Lookup] for a group of records after the store's previous
// commit and before the translators build the marks, calls [Apply] for each
// translator's records of each request (one producer per call), and closes the
// Lookup before the group's write: the store's lookups read a snapshot taken
// when they are opened. A record Apply drops is removed from [Result.Records],
// and [Result.Dropped] says which it was, for a caller that keeps records in
// slots of its own.
//
// Apply is deterministic given the lookup's answers and the horizon function: it
// reads no clock and writes no log, and it needs no translator. A caller can build
// the records and marks itself (a re-assertion with its Through set to the
// observed time, marked with a nil handle), pass a nil [Resumer], and give a
// translator's exported Comparable, or nil to compare raw payloads. [Stats] holds
// every counter, and adds up across calls.
package resume
