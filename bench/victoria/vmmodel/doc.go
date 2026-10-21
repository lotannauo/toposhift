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
//     Observe of a refreshing producer at its event time, and one staleness marker for
//     each Delete. A producer in watch mode (the Kubernetes watch, the fabric) asserts a
//     fact once and ends it with a delete; a metrics stack holds that state because an
//     exporter writes it again at every scrape, so such a fact is written at the
//     registered scrape interval from its assertion until a marker at its delete (or the
//     end of the period). Those samples are counted with the rest: they are what a
//     metrics store pays to hold state. Expiry is never encoded: a producer that goes
//     silent writes nothing, as in a real stack.
//   - [Registry] is the registered table of producers, the window each one is read with
//     and, for a producer in watch mode, its scrape interval. A metrics store has no TTL
//     of its own, so every query names the window of each producer that can hold the
//     thing it asks about.
//   - [World] holds the reference's existence intervals (folded by lifecycle.Fold) and
//     [World.Classify] and [World.ClassifyAnswer] sort a query: a boundary instant (an
//     involved subject starts or ends within a millisecond, or the coalesced stream the
//     plans come from has ended it earlier than the every-sample stream the store holds),
//     a read of a deleted fact (the store returned what a producer deleted less than its
//     window ago), or a query that is compared. Boundary instants and reads of a deleted
//     fact are counted per family, never compared.
//   - [Capabilities] is the table of what the store can and cannot answer. A question it
//     cannot express is a row there: never a loss, never timed, never in a ratio.
//   - [CheckSameFacts] is the premise of the comparison: the coalesced stream the
//     plans' expected answers come from and the every-sample stream written to the
//     store describe the same existence.
//
// What a metrics store does with the samples (how a window is read, whether a staleness
// marker is seen by a range function) is behaviour of a server this package never runs.
// Each statement about it rests on an assumption, listed below with what a local check
// against VictoriaMetrics v1.153.0 found; package vmengine tests them against a server.
//
// # Assumptions about the server
//
//	(a) remote write (the Prometheus protobuf, snappy block format) is accepted at
//	    /api/v1/write: verified (204 with no body);
//	(b) a Prometheus staleness marker (StaleNaN) is accepted and stored at its
//	    timestamp: verified (a row of its own, a value that is not a number);
//	(c) samples at any past timestamp inside the retention period are accepted, in any
//	    order: verified with -retentionPeriod=100y (1971, 2022, 2026; a late sample
//	    written after a later one); two samples of one series in one millisecond are both
//	    kept, so the converter's collapse is what leaves one;
//	(d) an instant query accepts time= to the millisecond, and the modifier inside a
//	    selector (x[60s] @ T) gives what time=T gives: verified. The modifier outside a
//	    call (f(x[60s]) @ T) did not, and is not used;
//	(e) the lookback of a query that names no window is bounded by
//	    -search.maxStalenessInterval: verified (an hour). The planned queries name their
//	    windows and do not depend on it;
//	(f) nocache=1 is accepted: instant queries did not touch the rollup result cache
//	    with or without it, so an effect of the parameter was not observable;
//	(g) /internal/force_merge: it answers at once and merges the parts that are on disk
//	    then, not those still in memory, so a merge is awaited by waiting for the server
//	    to be idle (see vmengine.Client.Compact);
//	(h) the server's counters report the rows and series written: verified
//	    (vm_rows_inserted_total, vm_rows_added_to_storage_total, vm_rows, vm_new_timeseries_created_total,
//	    vm_rows_ignored_total), and they lag a request by up to a second, so a flush and a
//	    wait come before they are read;
//	(i) timestamps are stored in milliseconds: verified;
//	(j) last_over_time(sel[W]) at t is the last sample in (t-W, t], the left end
//	    excluded and the right end included: verified to the millisecond;
//	(k) last_over_time does not return a staleness marker: verified. A series ended by
//	    a Delete still reads as present until its last sample is a window old, and a window
//	    that holds only a marker returns nothing. (The default rollup does see markers,
//	    which is why a query that names no window would answer differently.);
//	(l) new samples are readable after /internal/force_flush, and not before, for up to a
//	    few seconds: verified.
package vmmodel
