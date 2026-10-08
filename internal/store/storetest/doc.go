// Package storetest holds what the tests of [store.Store] implementations share.
// For now that is a deterministic generator of valid records, [Generator], and
// [RapidChecks]. The conformance suite that every backend must pass is built on
// top of them, and compares each backend with the reference implementation in
// the memstore package.
//
// The generator is a small model of one cluster with a handful of producers:
// racks and host placement, hosts and nodes that heartbeat, pods and their
// containers that come and go, a second opinion on pod placement, a cloned
// machine that reports an old boot, and services that depend on one another. It
// exists to give the suite a stream that exercises every path of the store
// contract, the same every time for one seed, on every architecture: it draws
// its exponential gaps with the basic operations of IEEE arithmetic only. It is
// not a model of realistic load, and it keeps no statistics.
//
// Boot ids belong to host observations only: the records of hosts (and of the
// clone of a host) carry one in store.Record.Boot, and no other record does.
// They are short, plain ASCII strings.
package storetest
