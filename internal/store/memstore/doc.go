// Package memstore is the reference implementation of [store.Store]: the store
// that the conformance suite compares every backend against. It keeps every
// record in memory and answers each read by folding the stored assertions with
// the lifecycle specification, with no indexes beyond the list of edges at
// each entity, no cleverness and no persistence. Its only job is to be
// obviously correct by construction.
//
// It is not a performance backend, it is not durable, and it is not the
// in-memory hot window of the design. Retention discards nothing here (see
// Retention below), so memory grows without bound as records are written.
//
// # Policy and the two folds
//
// A store is opened with the lifecycle policy that the existence of entities is
// folded with, and the policy is fixed for the life of the store. Entity
// existence, which is what [Store.Alive] answers, is folded with that policy.
// The liveness of an edge, which is what [Store.Neighbors] answers, is folded
// with the zero policy. The reason is the contract: Neighbors, Window and
// EntityWindow never return a [store.QuarantineError], so boot tracking applies
// to the existence of entities only. (A policy's Rank does not affect
// existence, so the zero policy and a ranked one fold edges alike.)
//
// # Retention
//
// Retain only publishes the horizons, which makes the refusals of the contract
// apply: a read before the horizon of its layer is refused with
// [store.ErrBeforeHorizon], and so is a write before the horizon of its record's
// layer. The store discards nothing. Keeping every record trivially leaves every
// answer for an instant at or after a horizon and a token at or above the
// highest committed Seq unchanged, preserves every quarantine, and keeps the
// boot history that a later clone collision is detected against.
//
// Each layer has its own horizon. [Options].Offsets gives every layer a
// retention offset, and Retain(h) moves the horizon of a layer to h minus its
// offset if that is later than the layer's horizon; [Options].Keep marks layers
// whose horizon never moves. With the zero options all four horizons are equal
// and the store behaves as if it had one. [Store.Horizon] is the horizon of the
// layer retained most recently; when one Retain moved several, the latest of
// them (they share its Seq), and the lower layer when their times tie.
// [Store.LayerHorizon] is the horizon of one layer.
//
// # Order of checks
//
// Every method checks in the same order, and a caller (and a test) may rely on
// it. A read first checks that the store is open: after Close the error wraps
// [store.ErrClosed], even for invalid arguments. Then the arguments: the scope,
// the direction, and every fingerprint, which must not be the zero
// fingerprint; an error wraps [store.ErrInvalid] and never
// [store.ErrBeforeHorizon], whatever instant was asked for. Then the context.
// Then the horizon of the scope's layer, only once one has been published for
// it: an instant (for Window and EntityWindow, from) before the horizon's time,
// or a token below the horizon's Seq, is refused with [store.ErrBeforeHorizon],
// even for a window that would be empty. Then the empty cases: a window with
// from not before to, and a NeighborsBatch with no fingerprints, answer empty.
// Then the answer. The contract does not say whether the horizon applies to a
// NeighborsBatch with no fingerprints; here it does, because the horizon is
// checked before the empty cases.
//
// Write checks that the store is open, then the context, and returns for an
// empty batch. Then it checks every record in order before it stores any:
// [store.Record.Validate]; a Seq above the previous record's and above
// [Store.LastSeq] and not equal to [store.Latest], which names no record; an
// event time not before the horizon of the record's layer; the layer the
// subject was first stored in, in the store or earlier in the batch; and, for an
// entity record in a store whose policy tracks boots, that the record's
// assertion folds under the policy. The last two checks go beyond the contract,
// which makes the layer a precondition the caller keeps and leaves the policy to
// the fold. The reference detects a changed layer so that a bad workload is
// caught here. It refuses a record its policy cannot fold because an entity
// whose stored records the policy cannot fold would be unreadable for ever;
// refusing it at the boundary keeps the batch whole or absent. With the policy
// of a host's boot id, [lifecycle.BootID], that cannot happen, because Validate
// already refuses a blank boot id; the check is for a policy whose boot key is
// another attribute of the assertion, the payload, which it refuses when it is
// blank. A refused batch stores nothing and leaves LastSeq unchanged.
//
// Retain checks that the store is open, then the context, and then moves the
// horizon of each retained layer forward if the instant, less the layer's
// offset, is after it.
//
// # Boot ids
//
// Only the observation of a host carries a boot id, in [store.Record].Boot; no
// edge record, delete or other entity does. A store opened with the policy
// lifecycle.Policy{BootKey: lifecycle.BootID} tells the boots of a host apart
// from it, and an entity in which two boots were live at once is quarantined:
// Alive reports it with a [store.QuarantineError]. The boots a store has seen
// are kept for good, because retention discards nothing, so a stale boot that
// reappears after a Retain still collides with the newer boot that came before
// it. A store with a baseline in place of the records before the horizon has to
// give the same answer. A store opened with the zero policy ignores boot ids and
// never quarantines.
package memstore
