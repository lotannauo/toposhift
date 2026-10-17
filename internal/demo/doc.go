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
// receiver's entity events every five minutes, the host events of a collector on
// every node, and the trace collector's metrics every minute. They are in the
// shapes the receivers write. [Build] sends them to a [sequencer.Sequencer],
// configured as serve configures it for a cluster name, on an ingest clock and a
// flush tick that the driver sets and sends: the logs through AcceptPartial and the
// metrics through AcceptMetrics, which is the same queue, claim, merge wait and
// answers. The real translators, coalescer, detector and resume step take the
// records into a store in memory. Nothing derived is written: no checkpoint, no
// blast radius, no co-residency, no host of a pod.
//
// # The services
//
// Ten of the workloads are services, because their pods send traces; the others
// (the metrics agent, the report Job, the two databases and the cluster's own
// components) send none. The trace collector is modelled in connector.go as a pure
// function of the world and an instant: the service graph connector flushes once a
// minute, at 20 seconds past, a cumulative counter for each call that was made in
// the last fifteen minutes, from its start two hours before the anchor, and the
// count connector sends once a minute, at 50 seconds past, a delta point for each
// instance that runs on a node that is ready. Each request arrives a second after
// its points. The real count connector sends about once for each batch of spans;
// one point a minute writes the same sightings, which the coalescer folds. The
// calls are in services.go: thirteen between services, and two to databases, which
// the translator sets aside and counts, as for a real cluster.
//
// A call or an instance is known from its first sighting and ends one [Expiry] after
// its last. The expiry is 15 minutes here, the dependency expiry and the instance
// TTL the sequencer is given; it is a constant of the demo, and serve's defaults are
// six hours, at which no call would end inside the window. The translator starts
// with no state, so every call that is live at the first flush is set aside once
// and sighted at the second.
//
// No health is stored, and the demo claims none: toposhift stores who calls whom.
// A failure of a service is the premise of a question, never something the demo
// writes.
//
// The build counts what the translators set aside and remarked on, by producer
// and reason ([Built.Skips], [Built.Notes]), and the counters of the metrics path
// ([Built.Metrics]). The cluster receiver sends the state of every namespace in
// each report, for the detector of cluster identity, and the cluster translator
// sets each one aside, since a namespace is not in the catalog; the service graph
// translator sets aside the series that are live at its first flush, while it warms
// up, and every point of a call to a database. Those are the only skips, as many as
// the world sent ([Built.NamespaceEvents], [Built.WarmUpSeries],
// [Built.DatabasePoints]), and there is no note. [Built.UnexplainedSkips] is what
// those do not account for.
//
// # The script
//
// The story is in script.go. In short: the frontend is rolled out one pod at a
// time at 1h30m; a Job's pod runs for four minutes every hour; at 3h00m node-2
// stops being ready, at 3h05m its pods are deleted and created again on the other
// nodes, and at 4h30m it is ready again. The pods a DaemonSet keeps on node-2
// stay. A simplification: an evicted pod is deleted at once, where the API would
// keep it terminating until its kubelet confirms. At 2h00m checkout starts calling
// payments-api, and its last call to legacy-pay is at 2h10m: that call ends 15
// minutes after its last sighting. The question about a service asks what a
// failure of ledger from 4h00m to 4h20m would reach.
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
// endpoint, which the tools withhold unless the request names it. The banner also
// gives the id of ledger, the service of the question about a failure; the
// instructions give no service id, which find_entities finds by name.
package demo
