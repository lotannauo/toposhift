// Package vmmodel is a model of a PromQL-compatible metrics store holding the spike's
// streams as samples, with no server and no network.
//
// A metrics stack that runs kube-state-metrics holds placement as series: one series
// per live object, one sample per scrape, the value 1. This package says what that
// looks like for the spike's streams, so that the questions the spike asks of layout L
// can be put to a metrics store and the answers compared:
//
//   - [Series] is the series model: topo_entity for an entity and topo_edge for an edge,
//     with identity in the labels and nothing else (no description).
//   - [Converter] turns the every-sample stream (the generator with CoalesceRuns off:
//     every refresh is its own record) into samples: one sample, value 1, for each
//     Observe at its event time, and one staleness marker for each Delete. Expiry is
//     never encoded: a producer that goes silent writes nothing, as in a real stack.
//   - [Registry] is the registered table of producers and the refresh window each one is
//     read with. A metrics store has no TTL of its own, so every query names the window
//     of each producer that can hold the thing it asks about.
//   - [World] holds the reference's existence intervals (folded by lifecycle.Fold) and
//     [Boundary] classifies a query as a boundary instant, one a metrics store may
//     answer differently from the reference by construction, and which is counted,
//     never compared.
//   - [Capabilities] is the table of what the store can and cannot answer. A question it
//     cannot express is a row there: never a loss, never timed, never in a ratio.
//   - [CheckSameFacts] is the premise of the comparison: the coalesced stream the
//     plans' expected answers come from and the every-sample stream written to the
//     store describe the same existence.
//
// What a metrics store does with the samples (how a window is read, whether a
// staleness marker is seen by a range function) is behaviour of a server this package
// never runs. Each statement about it is marked [U], unverified, with the letter of the
// assumption it rests on, and is checked against a server before any figure is read.
//
// # Assumptions about the server [U]
//
//	(a) remote write (the Prometheus protobuf, snappy block format) is accepted at /api/v1/write;
//	(b) a Prometheus staleness marker (StaleNaN) ends a series at its timestamp;
//	(c) samples at any past timestamp inside the retention period are accepted, in any order;
//	(d) an instant query accepts time=, and MetricsQL accepts the @ modifier;
//	(e) the lookback of an instant query is bounded by -search.minStalenessInterval and
//	    -search.maxStalenessInterval;
//	(f) nocache=1 disables the rollup result cache for one request;
//	(g) /internal/force_merge merges all partitions;
//	(h) /api/v1/status/tsdb and /metrics report series and sample counts;
//	(i) timestamps are stored in milliseconds.
//
// Two more are about how a window is read, and are the ones the arithmetic of this
// package depends on: (j) last_over_time(sel[W]) at t is the last sample in (t-W, t],
// the left end excluded and the right end included; (k) it does not return a staleness
// marker, so a series ended by a Delete still reads as present until its window ends.
package vmmodel
