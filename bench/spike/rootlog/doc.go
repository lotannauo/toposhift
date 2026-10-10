// Package rootlog is the spike's layout-L candidate built through the root
// module's store (internal/store/pebblestore) instead of through spike/pebblelog:
// the same harness, the same plans and the same measurements, run on the code the
// product ships, so that what the spike decided with can be repeated on it and
// compared.
//
// The adapter changes nothing about how the store is used. The store is opened
// with the lifecycle policy that names no boot key (the bench's streams carry no
// boots, and a boot key would make every entity write fold an assertion and every
// existence read fold the whole prefix, which would change both the timings and
// the read counters), with the Pebble configuration the runner chose, unchanged
// except that Pebble's messages are dropped as the spike's own engines drop them,
// and with the checkpoint policy of the variant stated outright, so that "off" is
// off and not the store's default.
//
// What the bench measures of a database it reads through [pebblekv.Wrap]: the
// store opens its own database, and the waits, the snapshot, the description and
// the canonical rewrite are the bench's, applied to it. The counters the store
// reports that the spike's engine did not (the phases of a write, the checkpoint
// build time) are extra names in the recorder and change no other.
//
// Where the store's contract is stricter than the spike's engine, the adapter does
// not hide it: a read before a layer's retention horizon is refused with
// [engine.ErrBeforeHorizon], which the engine's own contract leaves unspecified.
// An error that wraps the store's ErrInvalid or ErrBeforeHorizon also matches the
// engine's, so the harness tells them apart as it does for the other candidates.
//
// A batch is converted record by record on its way in, and a read's records on
// their way out; the conversion is inside the timed call, and is a small part of
// it.
package rootlog
