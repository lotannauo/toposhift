# OpenTelemetry collector configuration for toposhift

`toposhift.yaml` is an OpenTelemetry collector configuration that reads a Kubernetes cluster and sends it to toposhift. It is written for the collector contrib distribution, version 0.162.0. `nodeagent.yaml` is an optional second configuration, for a collector on each node, that tells toposhift which host each node runs on: see "Node agents" below. `tracemetrics.yaml` is an optional third, for a collector that the applications send their traces to, so that toposhift learns which services call which: see "Services and their calls" below.

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
| `<TOPOSHIFT_HOST>` | `exporters.otlphttp/toposhift.endpoint`, and `exporters.otlphttp/toposhift_metrics.endpoint` if you enable the trace metrics opt-in | The host (and so the address) at which the collector reaches toposhift. The same in both. |
| the storage directory | `extensions.file_storage/toposhift` | Only if `/var/lib/otelcol/toposhift` is not writable by the collector. Change `directory` and `compaction.directory` together. |

**The cluster name.** It is at most 21 bytes, each one of `A-Z a-z 0-9 . _ : / -`. It is part of every producer name toposhift stores, and a store records the name it was started with, so choose it once. The `resource` processor is in every pipeline that is enabled: the logs pipeline, and the metrics pipeline once the trace metrics opt-in is on. To send no name, delete the processor and its entry in `service.pipelines.logs.processors` (and in `service.pipelines.metrics.processors`, if you enabled that pipeline): toposhift then uses `default`, or the cluster name it is configured with. Deleting it also lets the senders that reach the optional ports choose their own cluster names, as nothing sets the name for them any more, unless toposhift runs with `--cluster-name`, which refuses every other name.

toposhift can be configured with a cluster name too: `--cluster-name`, or `TOPOSHIFT_CLUSTER_NAME` (empty is none). With one, an export whose `k8s.cluster.name` differs from it gets a permanent `400`, and the collector drops it, so set the same name in both places, or send none. Without one, each export's `k8s.cluster.name` names its cluster.

The store records the cluster name toposhift was first started with. If toposhift is later started with another name (or with a name after none), ingest stays off: every export gets `503` with `Retry-After: 30` and a message saying so, and the collector's queue holds the data. Records under the old name stay, and their references lapse by their TTL. To accept the change, start toposhift once with `--accept-new-cluster-name=` followed by the new name (or with nothing after the `=` to accept that no name is set). That flag is for one start and has no environment variable.

**An unedited `<CLUSTER_NAME>` passes the collector's validation, but toposhift refuses it.** `<` is outside the name format (and the placeholder differs from any name toposhift is configured with), so every export gets a permanent 400 (`INVALID_ARGUMENT`) and the collector drops it. The collector starts, looks healthy, and nothing reaches the store. Check the collector's logs for refused exports after the first start.

**Reaching toposhift.** toposhift listens on `127.0.0.1:7070` and nowhere else unless told to. A collector in a cluster cannot reach that. Start toposhift with `TOPOSHIFT_LISTEN` set to an address the cluster can reach (for example `TOPOSHIFT_LISTEN=0.0.0.0:7070`), and put that host in `<TOPOSHIFT_HOST>`. Listening beyond loopback exposes the port to whatever can route to it, so restrict it with the network.

With an address that is not loopback, `serve` does not serve the read-only tools at `/mcp` (its log says so at start). To ingest from a cluster and use the tools from one `serve`, keep it on loopback and bring the collector to it: a collector on the same machine, or a forwarded port (whatever reaches the forward reaches every route).

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

- **Receivers.** `k8s_objects` reads pods and nodes twice: in watch mode, which sends each change as it happens, and in pull mode every 5 minutes, which lists the whole kind and repairs what a dropped watch missed. `k8s_cluster` reads cluster entities every 5 minutes (`metadata_collection_interval: 5m`), which makes the entity report interval 300 seconds. Both are on the logs pipeline, the only pipeline enabled by default. `events` objects are not read: toposhift does not translate them yet.
- **Exporter.** OTLP over HTTP, protobuf, gzip, with a 30 second timeout. 429, 502, 503 and 504 are retried, and a `Retry-After` on 429 and 503 is honoured. Every other status is permanent: the request is dropped. `max_elapsed_time: 0` means the collector never gives up on a retryable failure. The retry interval starts at 5 seconds and grows to at most 30.
- **Sending queue.** `num_consumers: 1` means one request is sent at a time, in the order it was queued, which toposhift relies on. The queue is on disk (`storage`), so a restart of the collector keeps it. In the collector's design the queue holds the receivers' own requests, and its consumer merges or splits what it reads off the queue before sending. Each export is merged or split to at most 16,000,000 bytes, by size in bytes whatever the objects weigh, so it stays well under toposhift's limit of 64 MiB for a decoded request body. That bound protects toposhift from a 413 (payload too large); it does not bound the queue's disk; `queue_size` does. toposhift supports a single collector sending to it: more than one collector for one cluster is not supported, because the records of a second one can arrive late and delete checkpoints that are not put back (see "What a backlog does to toposhift"). The optional node agents and the optional trace collector are no exception: they send to this one collector and never to toposhift (see "Node agents" and "Services and their calls").
- **Storage.** The `file_storage/toposhift` extension holds the queue in `directory`. `create_directory: true` makes the collector create it. The directory must be writable by the user the collector runs as, which is usually not root: in a container, mount a volume there and make it writable by that user (for Kubernetes, `fsGroup` in the pod's security context). `compaction` shrinks the file when the collector starts, and again once the stored data has grown past `rebound_needed_threshold_mib` (default 100) and then fallen below `rebound_trigger_threshold_mib` (default 10), which the extension checks every 5 seconds.

## `fsync: true` costs throughput

With `fsync: true` the collector syncs the queue's file to disk on every write. A queued request then survives the host losing power, not only a crash of the collector. The price is collector throughput, which has not been measured for this configuration. If the collector falls behind a busy cluster, set `fsync: false` and accept that a power loss can lose the most recent writes.

## A full queue drops

The queue is sized in bytes (`sizer: bytes`, `queue_size: 2000000000`), so what an outage can hold does not depend on how large the requests are. (A queue sized in requests, as the collector does by default, would hold 1,000 of the receivers' own requests whatever their size; with whole-kind pulls of tens of megabytes that is tens of gigabytes, as an estimate.) The size is the collector's count of the bytes of the queued requests. It bounds the data, not the file: see "The disk it takes" below. **When it is full the collector drops new data.** Each request it cannot queue is refused and logged by the collector (`sending queue is full`), and a receiver that has someone to tell, such as an OTLP one, answers its sender `503`. The Kubernetes receivers here (`k8s_objects` and `k8s_cluster`) have no sender to tell, so they only log it and the data is dropped. `block_on_overflow` stays off, so the receivers are not slowed either. A long outage fills the queue, and whatever the collector reads after that is lost at the collector: toposhift never sees it and cannot say it is missing. The pull-mode lists repair some of this once toposhift is back, but a change that came and went inside the outage is gone.

To keep more, raise `queue_size`, which is a number of bytes; to keep less, lower it. The value must be at least the batch `min_size` (the collector refuses to start otherwise), and it should hold many times the largest request a receiver emits: a pull of a whole kind is one request and can be tens of megabytes on a large cluster, and one request larger than `queue_size` is refused outright. The 2,000,000,000 shipped is about 125 maximum-size exports of 16,000,000 bytes, and dozens of such large pulls. How long that lasts depends on the cluster: the watch changes and the 5 minute pulls together fill it, so an outage lasting about `queue_size` divided by the bytes the cluster produces per hour is what the queue holds. For example, a cluster that produces 20 MB every 5 minutes (240 MB an hour) fills 2 GB in about 8 hours, and a small cluster lasts days; these are estimates, not measurements.

### The disk it takes

The data is `queue_size` at most. The file holds some overhead beyond the data, and the file does not shrink by itself: compaction copies the live data into a new file in `compaction.directory` and then replaces the old one, so for a moment two files exist. With a full 2 GB queue, plan for up to about 4 GB on the volume holding both `directory` and the compaction directory (here the same one), plus overhead; a volume of 5 GB or more is comfortable. This is an estimate, not a measurement. With the trace metrics opt-in on, its exporter has a second queue of up to 500 MB (`queue_size: 500000000`) in the same directory: up to 2.5 GB of queued data, about 5 GB on the volume while the file compacts, so plan a volume of about 6 GB for both. For a different `queue_size`, scale it: for example `queue_size: 10000000000` allows about 10 GB of queued data and needs a volume with comfortably more than 20 GB.

## What a backlog does to toposhift

After an outage the collector sends the backlog it held. Those records are late: each carries an event time earlier than records toposhift already has, and toposhift diffs a late record against the record just before it in event time and deletes the checkpoints newer than it, since they are no longer true. A checkpoint is placed five minutes behind the newest record of its prefix, so a record less than five minutes behind the newest record already stored in its prefix deletes none; the backlog of a longer outage does. Checkpoints are derived, so no answer is wrong, but the ones deleted are not put back at once. Until the next ones are due, reads over the replayed interval replay more events than the usual bound, in the worst case about twice as many on a hot prefix. After a long outage a single gap between checkpoints might cover the whole outage (inferred, not measured). The holes are removed only when retention removes that stretch of history; `serve` retains when retention is on (the default of builds of `main`; releases made before background retention passes its latency check ship with it off, see the [README](../../README.md#retention)), and it keeps 30 days of pods and nodes and 90 days of hosts (up to a day more).

The persistent queue is what keeps the backlog through an outage and replays it, so this follows from the settings above. toposhift does not put the deleted checkpoints back, and no setting in this directory changes that.

## Node agents

`nodeagent.yaml` is **optional**. Without it, toposhift knows which Kubernetes node a pod is on and nothing about the machine under the node: placement stops at the node, and two pods are said to share a node, never a host. With it, a collector on each node (a DaemonSet) reports the host the node runs on, and toposhift stores the host and the edge from the node to it (`runs_on`). It is not part of the default install, and toposhift needs nothing from it to start.

**The supported topology** is one cluster collector per cluster, as above, and node agents that send only to it. The agents' `otlp_http` exporter points at an `otlp` receiver on the cluster collector, in the same logs pipeline, so their records share the one exporter and the one in-order queue (one pipeline, one exporter, one queue: nothing about the collector's shape changes). An agent that sends straight to toposhift is not supported: its records would not go through the queue in order, and would arrive late. The records are stored under the producer `nodeagent/<cluster name>`, which writes only its own hosts and edges, and the collector adds the cluster name, so the agents do not set it.

**The receiver is off by default, and enabling it opens an unauthenticated port.** `toposhift.yaml` ships the receiver, and its entry in the pipeline, commented out. Anything that can reach the port can send any record the pipeline takes, not only host events: the receiver feeds the same pipeline as `k8s_objects` and `k8s_cluster`, and toposhift routes each record by what it is, so a workload that can reach the pod could forge facts of every kind. Enable it only if you run the agents, and restrict it with a network policy so that only the agents' pods (the DaemonSet) can reach port 4318 of the collector's pod. To enable it, remove the prefix `#NODEAGENTS ` (with the space after it) from the five lines of `toposhift.yaml` that carry it. Enabled, they read, under `receivers:`:

```yaml
  otlp:
    protocols:
      http:
        endpoint: 0.0.0.0:4318
```

and, in the list `service.pipelines.logs.receivers`, after `k8s_cluster`:

```yaml
        - otlp
```

`mise run check:collector` validates the file in this enabled form too, so the step cannot rot, and `config_test.go` checks that the default file has no `otlp` receiver and that the lines above are the ones the file carries.

**Stability.** The `host_metrics` receiver's logs output is Development stability, and its entity events need the same feature gate as `k8s_cluster`:

```sh
otelcol-contrib --config nodeagent.yaml \
  --feature-gates=pkg.experimentalmetricmetadata.useEntityEventsSpecification
```

The file is pinned to 0.162.0. At each collector upgrade, check again that the agents still send entity events in the shape toposhift reads. If they stop, or the gate is missing, the hosts and their `runs_on` edges lapse about 15 minutes later. Three signals say so: `GET /status` has a `nodeagent` object with the latest host event of each cluster, and toposhift warns, once each time it happens, when a cluster that had host events has had none for three intervals; entity events in the older form that a collector writes without the gate are never translated, and are counted in `sequencer.legacy_entity_events` and warned about at most once a minute (for both this receiver and `k8s_cluster`); and the counters below show what was refused.

**What to edit:** `<GATEWAY_HOST>` in `exporters.otlp_http/gateway.endpoint`, the host at which the nodes reach the cluster collector's `otlp` receiver (port 4318). Nothing else.

**What the pod needs**, which the file cannot say:

- the environment variable `K8S_NODE_NAME`, from the downward API (`fieldRef: spec.nodeName`);
- a service account that may `get` nodes. A missing permission is silent: the collector sends events with no `k8s.node.uid`, and toposhift stores the host and no edge (`nodeagent.refused_no_node_uid`, and a warning);
- a read-only `hostPath` mount of the single file `/etc/machine-id`, at the same path in the container: without it, and without a cloud detector, there is no `host.id`, and nothing is stored for that node (`nodeagent.refused_no_host_id`, and a warning). In a cluster run in containers (kind, k3d, k3s in Docker) the file is usually absent, so placement stops at the node there;
- no `hostNetwork`: it would give the agent only a host name and an address, which toposhift does not store, and it makes the collector's own telemetry port clash.

```yaml
env:
  - name: K8S_NODE_NAME
    valueFrom: {fieldRef: {fieldPath: spec.nodeName}}
volumeMounts:
  - {name: machine-id, mountPath: /etc/machine-id, readOnly: true}
volumes:
  - name: machine-id
    hostPath: {path: /etc/machine-id, type: File}
```

**Identity.** The host is identified by `host.id` and the node by `k8s.node.uid`, each exactly as the agent sends it: nothing is trimmed, changed or hashed, here or in toposhift. The detectors are listed so that a cloud provider's instance id (`ec2`, `gcp`, `azure`, `oraclecloud`) comes before the machine id (`system`), which is the host's identity only on machines that are not in a cloud. On a machine outside every cloud the cloud detectors find nothing; detection runs once, when the agent starts, and `timeout: 5s` bounds it. `host.name`, the node's name and addresses are never used to identify anything. Two nodes on one machine report one `host.id`: one host, two nodes. Two machines that report one `host.id` with different boot ids (a cloned image) are a clone collision, and toposhift quarantines the host and says so. Hashing the id in the agent would make the same machine two hosts to any other source that states its raw `host.id`, so nothing hashes it, and toposhift's own fingerprint of it is the only hash.

**Where the machine id is held.** A machine id is meant to be kept confidential. The collector's queue holds it in transit, the traffic between the collectors carries it, and a capture of the exports holds it raw, as `host.id` among the resource attributes. Captures also hold the kubelet's node status fields, which include the node's machine id and system UUID, and the collector's on-disk queue holds the same in transit. toposhift's store keeps only the 16-byte fingerprint of a host's machine id. From this version on, the descriptions of the Kubernetes nodes that `k8s_objects` reads leave out the kubelet's machine id and system UUID, so the store and the activity files do not hold them for new data; the node's boot id stays in its payload. Data written by an earlier version keeps them: the store until retention ages it out (30 days), and activity files for as long as they are kept. The same machine gives the same fingerprint in two installs, a stable pseudonym. Keep captures and queue volumes as private as the machines' ids, and commit only throwaway ids as fixtures.

**What is stored.** The host, with its boot id, its architecture and operating system type, its CPU model and the provider's description when there is one; and the `runs_on` edge. `host.arch` is the architecture of the collector's binary, which is the host's except under emulation (an amd64 image on an arm64 node); it describes and never identifies. The operating system version and description are not stored: the detector reads them from the collector's container, not from the host. Zones are off in this version.

**The interval.** The agents report every 5 minutes (`metadata_collection_interval`; `collection_interval` only paces the scrapers), and toposhift expects that: `--node-interval` (or `TOPOSHIFT_NODE_INTERVAL`), 5 minutes by default, a whole number of seconds from one minute to one hour. Every record the agents cause lives for three times that interval, whatever interval an event carries; an event that reports another is stored all the same and counted (`nodeagent.substituted_configured_interval`). If you change the interval in the file, change it in toposhift too, and the other way round.

**The counters.** `GET /status` has a `nodeagent` object: the interval, the counters, and for each cluster the time of its latest host event and whether three intervals have passed since. A name says what was done with a record, not that it is gone: `refused_*` is a record, or a part of one, that was not stored (`refused_no_host_id`, `refused_no_node_uid`, `refused_zone_id`); `substituted_*` is a record that was stored with something put in place of what it gave (`substituted_receipt_no_time` and `substituted_receipt_implausible_time` take the time of receipt for the agent's clock, `substituted_no_boot` and `substituted_bad_boot` store the host with no boot id, `substituted_configured_interval` is the interval above); `skipped_*` is a record set aside. `host_events` counts every host event, and `sequencer.legacy_entity_events` the entity events in the older form.

## Services and their calls

`tracemetrics.yaml` is **optional**. Without it, toposhift knows pods, nodes and (with the node agents) hosts, and nothing about the applications that run in them. With it, toposhift also stores which services call which, the instances of each service, and the pod each instance runs in.

**What is stored.** Services and the calls between them (layer L3, `depends_on`), service instances, `instance_of` from an instance to its service, and `part_of` from an instance to its pod (the pod bridge, layer L2). The producers are `svcgraph/<cluster name>` for the calls and `spancount/<cluster name>` for the instances; each writes only its own records. A call ends at its last sighting plus `--dependency-expiry`, and an instance at its last span count plus `--instance-ttl`: both are 6 hours by default, with the environment twins `TOPOSHIFT_DEPENDENCY_EXPIRY` and `TOPOSHIFT_INSTANCE_TTL`, and a whole number of seconds from 2 minutes up. A call or an instance that stops being seen is not deleted: it ends at the time it expires, shown as `liveness_expiry`. toposhift stores no count, rate or latency of a call, only that it was seen. `toposhift serve -h` lists the rest.

**The path.** Applications send their traces to `tracemetrics.yaml`, one collector run as a Deployment of **one replica** behind a Service the SDKs export to. It pairs the client and the server span of each call and counts the spans of each instance, and sends only those two metrics, over HTTP, to the cluster collector's `otlp/tracemetrics` receiver on port 4319. The cluster collector adds the cluster name and sends them to toposhift, in order, through a queue of their own, to `POST /v1/metrics`. That path takes protobuf; toposhift answers a JSON body with `415`. Nothing sends metrics straight to toposhift: the cluster collector is the one collector that does, so that its queue keeps the order and the data through an outage.

**To enable it**, two edits.

1. In `tracemetrics.yaml`, put in `<GATEWAY_HOST>` the host at which the trace collector reaches the cluster collector's port 4319. Nothing else.
2. In `toposhift.yaml`, remove the prefix `#TRACEMETRICS ` (with the space after it) from the 37 lines that carry it, for instance with `sed 's/^#TRACEMETRICS //' toposhift.yaml > toposhift.enabled.yaml` and then using the new file. The opt-in is **off by default**. Enabled, they add, under `receivers:`:

```yaml
  otlp/tracemetrics:
    protocols:
      http:
        endpoint: 0.0.0.0:4319
```

a filter under `processors:` (`filter/tracemetrics`), an exporter under `exporters:` (`otlphttp/toposhift_metrics`, with its own queue, `queue_size: 500000000`, in the same storage directory), and a `metrics` pipeline under `service.pipelines`: receiver `otlp/tracemetrics`, processors `filter/tracemetrics` and `resource`, exporter `otlphttp/toposhift_metrics`. The exporter is a separate one so that the queue's bound is stated where it is set: the same exporter in a second pipeline would get a queue of its own for each signal, each as large as the 2 GB above, and double the disk this README promises. `mise run check:collector` validates the file in this enabled form, and with the node agents' opt-in too, so the step cannot rot.

**The receiver is unauthenticated, so who can send to it is a question of the network.** Anything that can reach port 4319 can send metrics, and the sender's restrictions are these, all in the configuration:

- (a) the port is its own, 4319, apart from the node agents' 4318, so a network policy can admit the trace collector's pod to the one and the node agents to the other;
- (b) the receiver feeds the metrics pipeline only, never the logs pipeline, so a sender on 4319 cannot write the Kubernetes objects or the hosts;
- (c) the filter passes the two metrics the trace collector makes, `traces_service_graph_request_total` and `trace.span.count`, and nothing else;
- (d) the `resource` processor upserts `k8s.cluster.name`, so a sender cannot choose the cluster. This holds only while the `resource` processor stays in the pipeline: if you delete it to send no name (see "The cluster name"), then, unless toposhift runs with `--cluster-name`, every sender chooses its own cluster name;
- (e) the trace collector's metrics pipeline takes only what the two connectors make, so no application can send it a metric to pass on; an application can only shape the connectors' output with its spans.

Authentication is not in this version. The control is the network policy, and the trust is this: the facts the traces give are asserted by the workloads themselves, trusted inside one trust domain and attributed to their producer. A sender that can reach the trace collector, or port 4319, can add false calls between any two service names (it needs no uid), false instances in its own pod, and bridges to any pod whose uid it knows. It cannot remove, shorten or read a fact. Restrict the trace collector's receiver to the cluster's application namespaces, and the cluster collector's port 4319 to the trace collector's pod, with policies like these (the labels and namespace names are yours to choose):

```yaml
# The cluster collector admits only the trace collector on 4319. The selector pair
# below (both in one list item) means pods with that label in that namespace; with
# a podSelector alone, only pods in the cluster collector's own namespace match.
apiVersion: networking.k8s.io/v1
kind: NetworkPolicy
metadata: {name: toposhift-collector-trace-metrics}
spec:
  podSelector: {matchLabels: {app: toposhift-collector}}
  policyTypes: [Ingress]
  ingress:
    - from:
        - namespaceSelector: {matchLabels: {kubernetes.io/metadata.name: observability}}
          podSelector: {matchLabels: {app: tracemetrics-collector}}
      ports:
        - {protocol: TCP, port: 4319}
---
# The trace collector admits only the application namespaces on 4317 and 4318.
apiVersion: networking.k8s.io/v1
kind: NetworkPolicy
metadata: {name: tracemetrics-collector-apps}
spec:
  podSelector: {matchLabels: {app: tracemetrics-collector}}
  policyTypes: [Ingress]
  ingress:
    - from:
        - namespaceSelector: {matchLabels: {example.com/apps: "true"}}
      ports:
        - {protocol: TCP, port: 4317}
        - {protocol: TCP, port: 4318}
```

Replace `observability` with the namespace the trace collector runs in; if both collectors run in one namespace, delete the `namespaceSelector` entry and make the `podSelector` line the list item (`- podSelector: …`). A policy that selects a pod makes everything it does not allow refused for that pod: the first one, if it selects the cluster collector, needs the node agents' rule for 4318 beside it.

Network policies do nothing unless the cluster's network plugin (CNI) enforces them. The network policy that restriction (a) makes possible depends on that: on a cluster whose plugin does not enforce policies, any pod that can reach the cluster collector can send to 4319.

**What the applications need.** Each application's SDK states two things about itself, and the deployment, not the collectors, supplies the first:

- `k8s.pod.uid`, from the downward API: a variable `POD_UID` from `fieldRef: metadata.uid`, defined **before** `OTEL_RESOURCE_ATTRIBUTES`, which uses it as `k8s.pod.uid=$(POD_UID)` (Kubernetes expands `$(VAR)` only for a variable defined earlier in the list). toposhift asserts that an instance is part of a pod only from a resource that carries `k8s.pod.uid`, and never derives the pod from an address. So no `k8sattributes` processor, with connection or IP association, may stand in front of toposhift anywhere on the path: an association on `k8s.pod.ip` is address matching too. A `pod_association` on the resource attribute `k8s.pod.uid` itself is fine. A stock demo chart may turn a `kubernetesAttributes` preset on, which associates by the pod's IP first: turn it off;
- `service.name`, and `service.namespace` too, so that two services of one name in two namespaces stay two.

```yaml
env:
  - name: POD_UID
    valueFrom: {fieldRef: {fieldPath: metadata.uid}}
  - name: OTEL_RESOURCE_ATTRIBUTES
    value: service.namespace=shop,k8s.pod.uid=$(POD_UID)
```

**`service.instance.id`.** An instance is the service, the namespace and its `service.instance.id`. An SDK that sets none writes no instance and no service from its span counts: they are counted as `spancount.skipped_no_instance_id` and warned about, and its calls still appear, through the service graph. Many SDKs set none. For those, `service.instance.id=$(POD_UID)` can be added to `OTEL_RESOURCE_ATTRIBUTES`, after `POD_UID`: the instance is then the pod, which is right while the application runs one process per pod, and it is asserted by your deployment, not shipped in any file here. An SDK that sets its own id keeps it.

**The connector settings, and why.** `tracemetrics.yaml` pins them, and toposhift has no flag for them, so the ones shipped are the only supported ones:

- `metrics_flush_interval: 60s` and `cache_loop: 1m`: the connector sends the counter of a call again at every flush until the call has been idle for fifteen minutes, and removes it at its next cache loop. A re-sent counter is not a new call: toposhift tells the two apart, and works out how long it waits after it starts before it believes a counter it has not seen before, from these two numbers. A change of either changes both of those quantities, so **change neither**.
- `dimensions` with `service.namespace`: without it a call's two ends carry the service name only, and a namespaced service would get two identities, one from its calls and one from its instances.
- `virtual_node_peer_attributes: []`: with an empty list the collector converts no expired edge into a virtual node. In 0.162.0 this setting is what turns the conversion off, since this version has no other switch for it, so a span whose other end never arrives is dropped. If a deployment sets peer attributes, an expired edge can become a virtual node again.
- `virtual_node_extra_label: true`: the label is then on every point, and with the shipped setting it is empty on every point (a call between two services). A deployment that sets peer attributes gets points with `connection_type=virtual_node` and a `virtual_node=client` or `virtual_node=server` label; toposhift sets them aside and counts them under `svcgraph.skipped_connection_virtual_node`, and no edge is written for them. With the shipped setting none is expected, so a non-zero count means peer attributes were set.
- `metrics_timestamp_offset: 0s`: points carry the collector's clock, unshifted.
- `store` with `ttl: 2s` and `max_items: 1000`: the window in which the client's and the server's span of one call must meet, and how many half calls wait at once. A call whose two spans arrive further apart is not seen.

**Sizing.** One replica, never autoscaled: a second one splits the pairs. Give the container a memory limit above the `memory_limiter`'s `limit_mib: 600`; the size has not been measured at the scale this supports. The trace collector's own queue is in memory (`queue_size: 100000000`), so a restart of it loses what is queued, and a call that is made again is seen again. Its own telemetry port is off (`service.telemetry.metrics.level: none`); to diagnose pairing, set `level: basic` (or higher), add a `readers` entry with a pull exporter, and look at the connector's counters.

**The metrics queue.** The cluster collector keeps these metrics in their own queue of up to 500 MB on the volume of the other (see "The disk it takes"). An outage that outlasts it loses sightings. A call that is quiet across an outage longer than `--dependency-expiry` shows a gap, and ends as `liveness_expiry`; it is never deleted, and the next sighting starts it again.

**Checking it.** `GET /status` has a `metrics` object with the counters of this path: `svcgraph.sightings` and `spancount.sightings` count what was written, the others (`svcgraph.skipped_*`, `spancount.skipped_*`, `metrics.*`) what was set aside and why, and `silent` marks a cluster that has sent no service graph point for three flushes. toposhift warns about three things: the service graph's series table is full (a sender can do that on purpose), span counts with no usable `service.instance.id` (`spancount.skipped_no_instance_id`), and no service graph point for more than three flushes (the trace collector or its pipeline is down, the opt-in is off, or nothing was called for a while).

## Checking the files

`config_test.go` in this directory reads `toposhift.yaml` (alone and with each opt-in enabled), `nodeagent.yaml` and `tracemetrics.yaml` and checks the values that matter (the intervals of each pull object, the queue, the retry, the storage and the pipeline; for the agent, the feature gate, a metadata interval equal to toposhift's default node interval, the order of the detectors, the boot id and that the agent sends only to the gateway's port; for the trace collector, the connector settings against what toposhift assumes, the pipelines, the filter and that nothing associates a pod with an address) and that this README mentions every placeholder. It does not parse YAML, so it cannot say the collector accepts a file. To check that, validate it with the collector, with the same feature gate:

```sh
otelcol-contrib validate --config toposhift.yaml \
  --feature-gates=pkg.experimentalmetricmetadata.useEntityEventsSpecification
otelcol-contrib validate --config nodeagent.yaml \
  --feature-gates=pkg.experimentalmetricmetadata.useEntityEventsSpecification
otelcol-contrib validate --config tracemetrics.yaml
```

An unknown key fails validation. An unedited placeholder does not.

CI runs this check with `mise run check:collector`, in the static shard: it validates `toposhift.yaml`, copies of it with the node agents' opt-in enabled, with the trace metrics opt-in enabled, and with both (the opt-in steps above), and `nodeagent.yaml` and `tracemetrics.yaml` in the collector image pinned by tag and digest in `mise.toml` (`OTELCOL_IMAGE`; change the tag and the digest together when the collector is bumped), then confirms that a copy with one misspelt key is refused. The validation runs with `--network none`; the first local run pulls the image, about 100 MB. Without Docker the task is skipped locally and fails when `CI=true`. If the runner's egress policy ever blocks instead of auditing, it must allow `registry-1.docker.io`, `auth.docker.io` and `production.cloudflare.docker.com`.
