# Buffering and recovery

[Collector setup](README.md) · [Node agents](node-agents.md) · [Service traces](service-traces.md)

The cluster collector buffers on its own disk. toposhift acknowledges an export with `200` only after its records are committed to disk; it has no embedded broker. A `200` may still report OTLP partial success, so inspect rejected-record counts too.

## Responses and retries

| Response | What to check |
| --- | --- |
| `503`, `Retry-After: 30` | Ingest is starting or off: store opening, no data directory, open failure, or a changed configured cluster name. The response and server log explain why. |
| `503`, `Retry-After: 1` | Busy: request queue full, merge wait expired, or too many bytes in flight. |
| `400` | Permanent refusal, including an invalid or mismatching cluster name. The collector drops the export. |
| `413` | Request too large; permanent. |

While the store opens, the response starts `ingest is starting: the store is opening`. Off responses include `ingest is off: no data directory; set --data-dir or TOPOSHIFT_DATA_DIR` or `ingest is off: the store could not be opened: …`. See [cluster-name changes](README.md#the-cluster-name) before accepting a new name.

The exporters use OTLP/HTTP protobuf, gzip and a 30-second timeout. They retry 429, 502, 503 and 504; `Retry-After` is honoured on 429 and 503. Other HTTP errors are permanent. Retry starts at 5 seconds, grows to at most 30 seconds, and has no elapsed-time limit (`max_elapsed_time: 0`). A network outage still needs queue capacity; unlimited retries do not mean unlimited storage.

## Durability and storage

The logs exporter uses `num_consumers: 1`, keeping its requests in queue order. Its consumer merges/splits queued receiver requests into exports of at most 16,000,000 bytes. This is below toposhift's 64 MiB decoded-body limit, but is separate from the queue's capacity.

The `file_storage/toposhift` extension uses `/var/lib/otelcol/toposhift`, creates the directory, and requires write access for the collector's user. In Kubernetes, mount a persistent volume with suitable ownership or `fsGroup`. Change `directory` and `compaction.directory` together.

With `fsync: true`, queue writes are synced for power-loss durability rather than only collector-crash recovery. Throughput for this configuration has not been measured. Switching to `fsync: false` trades that guarantee for potentially higher throughput and accepts loss of recent writes on power failure.

Compaction runs at startup and on rebound: stored data grows above `rebound_needed_threshold_mib` (default 100), then falls below `rebound_trigger_threshold_mib` (default 10), checked every 5 seconds.

## A full queue drops

The logs queue holds up to `queue_size: 2000000000` bytes as counted by the collector (`sizer: bytes`). At capacity it drops new data and logs `sending queue is full`; `block_on_overflow` is off. An OTLP receiver can return `503` to its sender, but Kubernetes receivers have no sender to retry: they only log the loss.

toposhift cannot identify records it never received. Later pulls resupply current objects; a change that came and went during the outage is lost. Losing a Delete can leave a reference until its TTL, or until another applicable assertion arrives.

Raise/lower `queue_size` to change capacity. It must meet the batch `min_size` requirement and comfortably exceed the largest receiver request. A whole-kind pull can be tens of megabytes, and a single request larger than the queue is refused outright. The default is about 125 maximum-size exports; queued receiver requests need not have that export size. The request-count default of 1,000 requests would not provide a byte bound.

Estimate outage coverage as queue bytes divided by incoming bytes per hour. For example, 20 MB every 5 minutes is 240 MB/hour, so 2 GB holds roughly 8 hours. These are sizing estimates, not measured retention guarantees.

### The disk it takes

`queue_size` bounds queued data, not disk usage. File overhead and compaction need more space: compaction copies live data before replacing the old file. With the default logs queue, budget about 4 GB plus overhead during compaction; a 5 GB volume leaves headroom.

The optional trace-metrics exporter has a separate 500 MB queue on the same volume. Together they allow 2.5 GB queued data and about 5 GB during compaction; plan about 6 GB of volume space. A 10 GB queue needs comfortably more than 20 GB. These are estimates; scale them with configured capacity and monitor actual disk usage.

The trace collector itself has a separate 100 MB **in-memory** queue. Its restart loses that queued data even though the cluster collector's queues are durable. See [service-trace sizing](service-traces.md#sizing-and-status).

## What a backlog does to toposhift

Backlogged records can arrive earlier in event time than facts already stored. toposhift compares each late record with the state just before it in event time and invalidates later checkpoints that it makes untrue. Checkpoints are derived; invalidating one preserves answers at the cost of more replay work.

The default checkpoint lag is five minutes. A record less than five minutes behind the newest record already stored in its prefix invalidates no checkpoint; older late records can leave gaps. Deleted checkpoints are not immediately rebuilt. Reads across those gaps may exceed the usual checkpoint read bound, and a long outage could leave a gap covering that outage; the deployment impact has not been measured.

Retention eventually removes the affected history. `serve` retains only when retention is on: builds of `main` default on, while releases made before the background-retention gate passes default off. The configured windows are 30 days for pods/nodes and 90 days for hosts, with up to a day of scheduling delay. See [retention](../../README.md#retention).

Keep one cluster collector per cluster, with optional collectors feeding it. Sending directly from additional collectors can introduce lateness and checkpoint invalidation. No setting here rebuilds the gaps; making the queue durable preserves the backlog rather than eliminating that tradeoff.
