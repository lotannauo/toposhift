// Package storetest is the conformance suite every [store.Store] backend runs, and
// what the tests of backends share. A backend is trusted only if it agrees with the
// reference store, package memstore, everywhere the contract speaks, so the suite
// compares it with the reference on generated workloads, at instants chosen to hit
// every boundary, and adds scripted scenarios for what a workload never reaches,
// checks of the contract's refusals, and checks that a store comes back from being
// closed and reads consistently while it is written.
//
// # What the suite checks
//
// [Run] runs everything as subtests, each on a store of its own opened from a
// [Factory]:
//   - [Check], the differential check, for each of the [Workloads] with no
//     retention, one retention and two. It feeds the store and the reference the same
//     batches, asks both the same questions of every kind of read (Alive, Neighbors,
//     NeighborsBatch, Window, EntityWindow) in every layer an entity is in and one
//     it is not, under the latest token and under earlier ones, compares the errors
//     of Alive, including a [store.QuarantineError], compares the outcome of every
//     Write, LastSeq and Horizon, and offers records before the horizon and asks
//     reads one nanosecond and one token below it, which both must refuse;
//   - the contract checks [CheckWriteContract], [CheckReadContract], [CheckClose],
//     [CheckContext] and [CheckHorizon]: whole-or-absent batches, empty batches,
//     payload bytes shared with the caller, arguments and the order in which they
//     are checked, a token above LastSeq, the behavior after Close, cancelled
//     contexts, and the rules of the retention horizon;
//   - the scripted scenarios [CheckInstant], [CheckEntityWindow], [CheckProducers],
//     [CheckRelations] and [CheckExtremes], which put records of one producer at
//     one instant, several producers on one subject, two relations on one pair, and
//     records at the first and last representable instants, with sequence numbers
//     that cross 2^32, and read them under every token;
//   - [CheckQuarantine], on a store opened with [QuarantinePolicy]: a clone
//     collision quarantines a host whatever the instant, for the tokens that see it,
//     and a Retain keeps both the quarantine and the boot history a later collision
//     is judged against;
//   - [CheckConcurrentReads]: reads that run alongside writes and retentions see
//     one committed world, never a mix of two, and a read pinned to LastSeq sees
//     exactly the records up to it;
//   - [CheckReopen], for a durable store: it comes back from being closed with its
//     records, LastSeq, horizon and quarantines;
//   - random workloads drawn with rapid.
//
// Under go test -short the suite is trimmed (see [Trimmed]): one workload, the one
// that quarantines hosts, the scripted checks, a shorter second phase of the
// concurrency check, and no reopen or random workloads. -short is what a caller
// passes to get that tier, for instance to run the suite under the race detector
// in less time; a plain go test, with or without -race, runs the full one.
//
// # Running it
//
// A backend runs the suite from a test of its own, with a factory that opens its
// store in the directory the suite gives it. The suite owns the directory and
// removes it. The policy is the one entity existence is folded with: a backend
// must support the zero policy and [QuarantinePolicy], which tells the boots of a
// host apart by the boot id its observations carry in [store.Record].Boot.
//
// The random workloads are drawn by rapid, so their seed varies from run to run by
// design; when one fails, rapid prints the -rapid.seed that repeats it. How many
// cases it draws is set by the package's TestMain, which should call
// flag.Set("rapid.checks", storetest.RapidChecks("30")): the environment variable
// TOPOSHIFT_RAPID_CHECKS raises it, and a -rapid.checks on the command line still
// wins.
//
//	func TestConformance(t *testing.T) {
//		storetest.Run(t, storetest.Factory{
//			Open: func(dir string, p lifecycle.Policy) (store.Store, error) {
//				return pebblestore.Open(dir, pebblestore.Options{Policy: p})
//			},
//			Durable: true,
//		})
//	}
//
// # The suite's own tests
//
// A suite that cannot tell a broken store from a good one is worthless, so the
// tests of this package run it against stores that conform and stores that do
// not: a store that discards history on Retain by the rule a layout is meant to
// follow, a store that journals what it accepts and replays it when it is opened,
// and a set of stores each broken in one way, about a hundred of them (a boundary one
// nanosecond off, a lost baseline, a layer ignored, a quarantine that does not
// survive a retention, a Close that answers, and so on). Each must be caught by the
// check written for it.
//
// # Generated records
//
// The generator is a small model of one cluster with a handful of producers: racks
// and host placement, hosts and nodes that heartbeat, pods and their containers that
// come and go, a second opinion on pod placement, a cloned machine that reports an
// old boot, and services that depend on one another. It exists to give the suite a
// stream that exercises every path of the store contract, the same every time for
// one seed, on every architecture: it draws its exponential gaps with the basic
// operations of IEEE arithmetic only. It is not a model of realistic load, and it
// keeps no statistics.
//
// Boot ids belong to host observations only: the records of hosts (and of the
// clone of a host) carry one in store.Record.Boot, and no other record does. They
// are short, plain ASCII strings.
package storetest
