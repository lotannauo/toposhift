// Package demo builds a synthetic Kubernetes cluster with a history of six hours,
// in memory, through the real ingest path, so that the read-only topology tools
// have something to answer from with no cluster, no data directory and no
// configuration.
//
// Everything in it is synthetic. A seed draws a world: two machines, four nodes
// that share them in pairs, five namespaces and forty pods at the anchor, the
// instant the history begins. The anchor is the fixed date 2026-10-01T00:00:00Z,
// in the past, so that no answer depends on the wall clock; the history ends six
// hours later. A pod, a node or a container that exists at the anchor was created
// earlier and exists from its creation timestamp, which the translators read, as
// they do for a real cluster.
//
// # The path
//
// The package writes no record. [World.Requests] lists the requests the shipped
// collector configuration would send: the Kubernetes objects receiver's pulls
// every five minutes and its watch events a second after each change, the cluster
// receiver's entity events every five minutes, and the host events of a collector
// on every node. They are in the shapes the receivers write. [Build] sends them to
// a [sequencer.Sequencer], configured as serve configures it for a cluster name,
// on an ingest clock and a flush tick that the driver sets and sends. The real
// translators, coalescer, detector and resume step take the records into a
// store in memory. Nothing derived is written: no checkpoint, no blast radius, no
// co-residency, no host of a pod.
//
// The build counts what the translators set aside and remarked on, by producer
// and reason ([Built.Skips], [Built.Notes]). The cluster receiver sends the state
// of every namespace in each report, for the detector of cluster identity, and the
// cluster translator sets each one aside, since a namespace is not in the catalog:
// those are the only skips, as many as the world sent ([Built.NamespaceEvents]),
// and there is no note. [Built.UnexplainedSkips] is what those do not account for.
//
// # The script
//
// The story is in script.go. In short: the frontend is rolled out one pod at a
// time at 1h30m; a Job's pod runs for four minutes every hour; at 3h00m node-2
// stops being ready, at 3h05m its pods are deleted and created again on the other
// nodes, and at 4h30m it is ready again. The pods a DaemonSet keeps on node-2
// stay. A simplification: an evicted pod is deleted at once, where the API would
// keep it terminating until its kubelet confirms.
//
// The world is built to grow a service layer: a [Workload] has a Service field,
// empty here, and its pods point at it. A world that sets it adds an emitter and
// script events; the driver and the digest do not change.
//
// # Determinism
//
// One seed gives one store, on any machine and with any GOMAXPROCS: the same
// sequence number, the same record count and the same digest. The seed is read in
// [NewWorld] alone, and the emitters are pure functions of the world and an
// instant. The sequencer reads the driver's clock and nothing else. Its loop is
// one goroutine that nothing outside the package can wait for, so each flush tick
// is followed by a request with no records, the barrier, which the loop claims
// only once the tick has returned and which writes nothing; once it is answered,
// the loop reads no clock before its next select, and the clock may move. No digest
// is frozen in a test: the translators may change what they write, and then the
// digest changes with them.
//
// # What it prints
//
// The banner and the instructions name nodes and pods by id and never an
// endpoint, which the tools withhold unless the request names it.
package demo
