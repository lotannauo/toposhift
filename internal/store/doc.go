// Package store defines the contract every storage backend of toposhift
// satisfies, and the record it stores. It holds no implementation: a backend is
// written against [Store] and checked against the same expectations as every
// other.
//
// # Names
//
// The design plan names four operations; this package spells them as follows.
// PutActivity is [Store.Write]. NeighborsAsOf is [Store.Neighbors], with
// [Store.NeighborsBatch] for many fingerprints at once. ChangesInWindow is
// [Store.Window]. EntityHistory is [Store.EntityWindow].
//
// # Two time axes
//
// Every record carries both axes. Event time is when a statement was true, as
// the producer asserts it. The snapshot token is the store's ingest sequence
// number, the record's Seq: it orders what the store learned, and it is a
// counter rather than a clock because clocks can tie or step backward. Every
// read takes both: an instant in event time, and a [Scope] whose AsOf is the
// token. A read pinned to token S sees exactly the records with Seq at or below
// S.
//
// # Facts only
//
// A store holds what producers asserted, as [Record] values. Liveness expiry,
// cascade and every other derivation are computed at read time from those
// records and never written back.
//
// # Not here yet
//
// This package does not yet cover:
//   - opening or configuring a store;
//   - sizes and statistics;
//   - checkpoints;
//   - the mapping from ingest time to a snapshot token;
//   - cascade and co-residency, which are derived above the store by interval
//     overlap;
//   - an in-memory reference implementation and the conformance suite that
//     every backend must pass, which are a later change.
package store
