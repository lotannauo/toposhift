// Package pebblekv is the Pebble plumbing a Pebble-backed store is built on: how
// a database is opened and with which options, the meta keys that let a layout
// survive a reopening, the value codec of one stored version, the numeric forms
// of entity types, relations and layers that keys are built from, and the
// helpers that report what a read or a retention cost.
//
// It holds no layout. A layout (the order of its keys, what it stores under
// them) is the store's own and passes its comparer and key schemas to [Open] as
// a [Layout]; [BytewiseLayout] is the one this package offers.
//
// # Pins
//
// What Pebble writes to disk depends on its version and on settings whose
// defaults it may change. A database must therefore be opened the same way by
// every build that reads it, so this package states the settings that shape the
// stored bytes instead of taking Pebble's defaults:
//
//   - the format major version is [pebble.FormatValueSeparation], not
//     [pebble.FormatNewest]: the format only ratchets forward, and a version
//     bump of Pebble must not move a database by itself;
//   - value separation is off, which is the default today and is written down
//     so that a future default cannot reshape tables;
//   - the comparer, the key schema and every block-property collector are
//     persisted by name in the tables, so a layout's names are part of its
//     stored format;
//   - the Pebble version is pinned in go.mod, and a bump needs a new measurement
//     (the check:pebble-pin task keeps the root and bench modules on one
//     version).
//
// # Failure
//
// Pebble reports some failures, a lost write-ahead log write among them, as a
// fatal condition. [Open] installs a logger that sends the rest to log/slog and
// ends the process, with status 70, on a fatal one: a database Pebble considers
// unusable must not go on serving, and whatever the ingest layer had not
// acknowledged is replayed on the next start.
package pebblekv
