# OpenTelemetry collector configuration for toposhift

`toposhift.yaml` is an OpenTelemetry collector configuration that reads a Kubernetes cluster and sends it to toposhift. It is written for the collector contrib distribution, version 0.162.0.

toposhift takes the logs in when it has a data directory (by default under `$HOME/.local/share/toposhift`, or the one named by `--data-dir` or `TOPOSHIFT_DATA_DIR`; see `toposhift serve -h`). It answers an export `200` only once its records are committed to disk. While it starts, and whenever ingest is off, it answers `503` with `Retry-After: 30` and says why; the collector retries, so the data waits in its queue:

- while the store opens at boot: `ingest is starting: the store is opening`;
- with no data directory: `ingest is off: no data directory; set --data-dir or TOPOSHIFT_DATA_DIR`;
- when the store cannot be opened: `ingest is off: the store could not be opened: ` followed by the error;
- when the cluster name toposhift is configured with differs from the one its store recorded (see "The cluster name" below).

When toposhift is merely busy (its queue of requests is full, a request waited too long to be merged into a commit, or too many bytes are in flight), it answers `503` with `Retry-After: 1`, and the collector sends the request again soon.

The collector is the buffer. While toposhift is down or busy, the collector keeps what it has read on its own disk, retries without giving up, and sends it in order when toposhift is back. toposhift needs none of these settings to start: they are a recommendation, not a requirement.

## What to edit

Edit these and nothing else:

| Placeholder | Where | What to put |
| --- | --- | --- |
| `<CLUSTER_NAME>` | `processors.resource` | A name for this cluster. |
| `<TOPOSHIFT_HOST>` | `exporters.otlphttp/toposhift.endpoint` | The host (and so the address) at which the collector reaches toposhift. |
| the storage directory | `extensions.file_storage/toposhift` | Only if `/var/lib/otelcol/toposhift` is not writable by the collector. Change `directory` and `compaction.directory` together. |

**The cluster name.** It is at most 21 bytes, each one of `A-Z a-z 0-9 . _ : / -`. It is part of every producer name toposhift stores, and a store records the name it was started with, so choose it once. To send no name, delete the `resource` processor and its entry in `service.pipelines.logs.processors`: toposhift then uses `default`, or the cluster name it is configured with.

toposhift can be configured with a cluster name too: `--cluster-name`, or `TOPOSHIFT_CLUSTER_NAME` (empty is none). With one, an export whose `k8s.cluster.name` differs from it gets a permanent `400`, and the collector drops it, so set the same name in both places, or send none. Without one, each export's `k8s.cluster.name` names its cluster.

The store records the cluster name toposhift was first started with. If toposhift is later started with another name (or with a name after none), ingest stays off: every export gets `503` with `Retry-After: 30` and a message saying so, and the collector's queue holds the data. Records under the old name stay, and their references lapse by their TTL. To accept the change, start toposhift once with `--accept-new-cluster-name=` followed by the new name (or with nothing after the `=` to accept that no name is set). That flag is for one start and has no environment variable.

**An unedited `<CLUSTER_NAME>` passes the collector's validation, but toposhift refuses it.** `<` is outside the name format (and the placeholder differs from any name toposhift is configured with), so every export gets a permanent 400 (`INVALID_ARGUMENT`) and the collector drops it. The collector starts, looks healthy, and nothing reaches the store. Check the collector's logs for refused exports after the first start.

**Reaching toposhift.** toposhift listens on `127.0.0.1:7070` and nowhere else unless told to. A collector in a cluster cannot reach that. Start toposhift with `TOPOSHIFT_LISTEN` set to an address the cluster can reach (for example `TOPOSHIFT_LISTEN=0.0.0.0:7070`), and put that host in `<TOPOSHIFT_HOST>`. Listening beyond loopback exposes the port to whatever can route to it, so restrict it with the network.

toposhift also needs a data directory to ingest: by default one under `$HOME/.local/share/toposhift`, or the one named by `--data-dir` or `TOPOSHIFT_DATA_DIR`. Without one it answers `503` with `Retry-After: 30`, which the collector retries, so the data waits in the queue.

## Start the collector with the feature gate

The `k8s_cluster` receiver sends entities as entity events only when this feature gate is on:

```sh
otelcol-contrib --config toposhift.yaml \
  --feature-gates=pkg.experimentalmetricmetadata.useEntityEventsSpecification
```

Without the gate the receiver does not send the entity events toposhift reads. In a Helm chart or an operator, pass the same flag in the collector's arguments.

The collector's service account needs permission to `get`, `list` and `watch` pods and nodes (for `k8s_objects`), and the objects `k8s_cluster` reads (namespaces, deployments, replica sets, and so on).

## What each setting is for

- **Receivers.** `k8s_objects` reads pods and nodes twice: in watch mode, which sends each change as it happens, and in pull mode every 5 minutes, which lists the whole kind and repairs what a dropped watch missed. `k8s_cluster` reads cluster entities every 5 minutes (`metadata_collection_interval: 5m`), which makes the entity report interval 300 seconds. Both are on the logs pipeline, the only pipeline there is. `events` objects are not read: toposhift does not translate them yet.
- **Exporter.** OTLP over HTTP, protobuf, gzip, with a 30 second timeout. 429, 502, 503 and 504 are retried, and a `Retry-After` on 429 and 503 is honoured. Every other status is permanent: the request is dropped. `max_elapsed_time: 0` means the collector never gives up on a retryable failure. The retry interval starts at 5 seconds and grows to at most 30.
- **Sending queue.** `num_consumers: 1` means one request is sent at a time, in the order it was queued, which toposhift relies on. The queue is on disk (`storage`), so a restart of the collector keeps it. In the collector's design the queue holds the receivers' own requests, and its consumer merges or splits what it reads off the queue before sending. Each export is merged or split to at most 16,000,000 bytes, by size in bytes whatever the objects weigh, so it stays well under toposhift's limit of 64 MiB for a decoded request body. That bound protects toposhift from a 413 (payload too large); it does not bound the queue's disk; `queue_size` does. toposhift supports a single collector sending to it: more than one collector for one cluster is not supported, because the records of a second one can arrive late and delete checkpoints that are not put back (see "What a backlog does to toposhift").
- **Storage.** The `file_storage/toposhift` extension holds the queue in `directory`. `create_directory: true` makes the collector create it. The directory must be writable by the user the collector runs as, which is usually not root: in a container, mount a volume there and make it writable by that user (for Kubernetes, `fsGroup` in the pod's security context). `compaction` shrinks the file when the collector starts, and again once the stored data has grown past `rebound_needed_threshold_mib` (default 100) and then fallen below `rebound_trigger_threshold_mib` (default 10), which the extension checks every 5 seconds.

## `fsync: true` costs throughput

With `fsync: true` the collector syncs the queue's file to disk on every write. A queued request then survives the host losing power, not only a crash of the collector. The price is collector throughput, which has not been measured for this configuration. If the collector falls behind a busy cluster, set `fsync: false` and accept that a power loss can lose the most recent writes.

## A full queue drops

The queue is sized in bytes (`sizer: bytes`, `queue_size: 2000000000`), so what an outage can hold does not depend on how large the requests are. (A queue sized in requests, as the collector does by default, would hold 1,000 of the receivers' own requests whatever their size; with whole-kind pulls of tens of megabytes that is tens of gigabytes, as an estimate.) The size is the collector's count of the bytes of the queued requests. It bounds the data, not the file: see "The disk it takes" below. **When it is full the collector drops new data.** Each request it cannot queue is refused and logged by the collector (`sending queue is full`), and a receiver that has someone to tell, such as an OTLP one, answers its sender `503`. The Kubernetes receivers here (`k8s_objects` and `k8s_cluster`) have no sender to tell, so they only log it and the data is dropped. `block_on_overflow` stays off, so the receivers are not slowed either. A long outage fills the queue, and whatever the collector reads after that is lost at the collector: toposhift never sees it and cannot say it is missing. The pull-mode lists repair some of this once toposhift is back, but a change that came and went inside the outage is gone.

To keep more, raise `queue_size`, which is a number of bytes; to keep less, lower it. The value must be at least the batch `min_size` (the collector refuses to start otherwise), and it should hold many times the largest request a receiver emits: a pull of a whole kind is one request and can be tens of megabytes on a large cluster, and one request larger than `queue_size` is refused outright. The 2,000,000,000 shipped is about 125 maximum-size exports of 16,000,000 bytes, and dozens of such large pulls. How long that lasts depends on the cluster: the watch changes and the 5 minute pulls together fill it, so an outage lasting about `queue_size` divided by the bytes the cluster produces per hour is what the queue holds. For example, a cluster that produces 20 MB every 5 minutes (240 MB an hour) fills 2 GB in about 8 hours, and a small cluster lasts days; these are estimates, not measurements.

### The disk it takes

The data is `queue_size` at most. The file holds some overhead beyond the data, and the file does not shrink by itself: compaction copies the live data into a new file in `compaction.directory` and then replaces the old one, so for a moment two files exist. With a full 2 GB queue, plan for up to about 4 GB on the volume holding both `directory` and the compaction directory (here the same one), plus overhead; a volume of 5 GB or more is comfortable. This is an estimate, not a measurement. For a different `queue_size`, scale it: for example `queue_size: 10000000000` allows about 10 GB of queued data and needs a volume with comfortably more than 20 GB.

## What a backlog does to toposhift

After an outage the collector sends the backlog it held. Those records are late: each carries an event time earlier than records toposhift already has, and toposhift diffs a late record against the record just before it in event time and deletes the checkpoints newer than it, since they are no longer true. A checkpoint is placed five minutes behind the newest record of its prefix, so a record less than five minutes behind the newest record already stored in its prefix deletes none; the backlog of a longer outage does. Checkpoints are derived, so no answer is wrong, but the ones deleted are not put back at once. Until the next ones are due, reads over the replayed interval replay more events than the usual bound, in the worst case about twice as many on a hot prefix. After a long outage a single gap between checkpoints might cover the whole outage (inferred, not measured). The holes are removed only when retention removes that stretch of history; `serve` retains by default (`--retain off` turns it off), and it keeps 30 days of pods and nodes and 90 days of hosts (up to a day more).

The persistent queue is what keeps the backlog through an outage and replays it, so this follows from the settings above. toposhift does not put the deleted checkpoints back, and no setting in this directory changes that.

## Checking the file

`config_test.go` in this directory reads `toposhift.yaml` and checks the values that matter (the intervals of each pull object, the queue, the retry, the storage and the pipeline) and that this README mentions every placeholder. It does not parse YAML, so it cannot say the collector accepts the file. To check that, validate it with the collector, with the same feature gate:

```sh
otelcol-contrib validate --config toposhift.yaml \
  --feature-gates=pkg.experimentalmetricmetadata.useEntityEventsSpecification
```

An unknown key fails validation. An unedited placeholder does not.

CI runs this check with `mise run check:collector`, in the static shard: it validates `toposhift.yaml` in the collector image pinned by tag and digest in `mise.toml` (`OTELCOL_IMAGE`; change the tag and the digest together when the collector is bumped), then confirms that a copy with one misspelt key is refused. The validation runs with `--network none`; the first local run pulls the image, about 100 MB. Without Docker the task is skipped locally and fails when `CI=true`. If the runner's egress policy ever blocks instead of auditing, it must allow `registry-1.docker.io`, `auth.docker.io` and `production.cloudflare.docker.com`.
