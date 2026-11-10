# Services and their calls

[Collector setup](README.md) · [Node agents](node-agents.md) · [Queues and recovery](buffering-and-recovery.md)

Use [tracemetrics.yaml](tracemetrics.yaml) when you need application services, their calls and instances. Kubernetes collection alone supplies pods, nodes and containers; node agents add hosts. Neither establishes which services call each other.

## The data path

Run the trace collector as a Deployment with **one replica**, never autoscaled, behind a Service the applications export to. It pairs client/server spans and counts spans by instance. It sends only two metrics to the cluster collector on port 4319; it sends nothing directly to toposhift.

Applications must already emit traces through an SDK or [automatic instrumentation](https://opentelemetry.io/docs/concepts/instrumentation/). To produce call edges, instrument both client and server requests and propagate trace context between them. Resource attributes alone do not emit spans.

Configure each application's trace exporter to reach the trace collector's Service. For an SDK that supports these environment variables, OTLP/HTTP protobuf uses:

```sh
export OTEL_EXPORTER_OTLP_TRACES_PROTOCOL=http/protobuf
export OTEL_EXPORTER_OTLP_TRACES_ENDPOINT="http://<TRACE_COLLECTOR_SERVICE>:4318/v1/traces"
```

Replace `<TRACE_COLLECTOR_SERVICE>` with the Service's reachable DNS name. OTLP/gRPC uses port 4317 and no `/v1/traces` path. SDK configuration support varies; see the [OTLP exporter instructions](https://opentelemetry.io/docs/languages/sdk-configuration/otlp-exporter/) for your language.

The cluster collector adds the cluster name and forwards the metrics through their own persistent queue to `POST /v1/metrics`. toposhift accepts protobuf there; a JSON request body gets `415`. The logs pipeline and queue remain separate.

| Stored fact | Layer and producer |
| --- | --- |
| Services | L3, both `svcgraph/<cluster name>` and `spancount/<cluster name>` |
| `depends_on` calls | L3, `svcgraph/<cluster name>` |
| Instances, `instance_of` and instance → pod `part_of` | L2, `spancount/<cluster name>` |

A service remains live while either producer holds a live reference to it. These are sightings, not stored counts, rates or latency. A call lapses at its last sighting plus `--dependency-expiry`; an instance lapses at its last span-count sighting plus `--instance-ttl`. Both default to 6 hours, with `TOPOSHIFT_DEPENDENCY_EXPIRY` and `TOPOSHIFT_INSTANCE_TTL` equivalents. Flags accept whole seconds from 2 minutes through 366 days. Expiry is derived and labelled `liveness_expiry`, not a producer Delete.

## Enable trace metrics

1. Replace `<GATEWAY_HOST>` in `tracemetrics.yaml` with the host where the trace collector reaches the cluster collector on port 4319.
2. In `toposhift.yaml`, remove the prefix `#TRACEMETRICS `, including its trailing space, from all 37 marked lines:

```sh
sed 's/^#TRACEMETRICS //' toposhift.yaml > toposhift.enabled.yaml
```

Use the generated configuration when starting the cluster collector. This opt-in is **off by default**. If also enabling node agents, remove both prefixes from the same configuration; do not overwrite one enabled copy with the other.

The opt-in adds `otlp/tracemetrics` on `0.0.0.0:4319`, `filter/tracemetrics`, `otlphttp/toposhift_metrics` and a metrics pipeline. Its processors are `filter/tracemetrics` then `resource`; its exporter has one consumer and a separate 500 MB disk queue in the existing storage directory. Reusing the logs exporter for a second signal would allocate another queue of the logs exporter's size, doubling the 2 GB allowance instead. `mise run check:collector` validates each opt-in and both together.

## Restrict who can send

Port 4319 is **unauthenticated**. Restrict it with a **network policy** admitting only the trace collector. The configuration separates inputs as follows:

- Port 4319 feeds metrics only, so it cannot inject objects or host events into the logs pipeline. Node agents use a separate port, 4318.
- The filter passes only `traces_service_graph_request_total` and `trace.span.count`.
- Cluster-name enforcement holds only while the `resource` processor stays in that pipeline. Remove it and senders choose a cluster unless toposhift has `--cluster-name` set.
- The trace collector's forwarded metrics come only from its two connectors. SDK metrics and logs are accepted and discarded by separate `nop` pipelines; applications cannot send a metric straight through this collector.

The facts are workload assertions inside one trust domain, attributed to a producer; their contents are not independently authenticated. A sender reaching the trace collector or port 4319 can forge calls and instances; forging a pod bridge requires a known pod UID. Restrict the trace collector's input to application namespaces as well. These policies illustrate the two boundaries; choose your own labels and namespaces:

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
Apply the first policy in the cluster collector's namespace and the second in the trace collector's namespace: neither example sets `metadata.namespace`. Replace `observability` with the trace collector's namespace; that label selects the source namespace, not where the policy is applied. In the first policy, `namespaceSelector` and `podSelector` in one list item mean both must match. If both collectors share a namespace, remove `namespaceSelector` and make `podSelector` the list item (`- podSelector: …`).

A policy selecting the cluster collector needs a separate allow rule for node agents on 4318 if you run them. Network policies work only where the CNI enforces them; isolation depends on that. Without enforcement, a pod able to reach 4319 can send metrics there.

## Application attributes

The deployment supplies `k8s.pod.uid`: use `POD_UID` from `fieldRef: metadata.uid`, defined **before** `OTEL_RESOURCE_ATTRIBUTES`, which expands `k8s.pod.uid=$(POD_UID)`. Kubernetes expands only variables defined earlier.

```yaml
env:
  - name: POD_UID
    valueFrom: {fieldRef: {fieldPath: metadata.uid}}
  - name: OTEL_RESOURCE_ATTRIBUTES
    value: service.namespace=shop,k8s.pod.uid=$(POD_UID)
```

Also set each application's `service.name` and `service.namespace` so same-named services in different namespaces remain distinct.

toposhift builds the instance → pod bridge only from the exact UID. Do not put a `k8sattributes` processor using connection/IP association anywhere on the path, including `k8s.pod.ip` association. A `pod_association` on the resource's `k8s.pod.uid` itself is allowed. Disable a demo chart's `kubernetesAttributes` preset if it associates by IP first.

An instance is identified by service, namespace and `service.instance.id`. With no usable instance ID, span counts write neither instance nor service and increment `spancount.skipped_no_instance_id`; calls can still appear through the service graph. For an SDK that supplies no ID, add `service.instance.id=$(POD_UID)` to `OTEL_RESOURCE_ATTRIBUTES` after defining `POD_UID`, only when one application process runs per pod. This is a deployment assertion, not a setting supplied by these collector files. Keep an SDK's own ID when it has one.

## Keep the connector settings

The settings in `tracemetrics.yaml` are the supported ones; toposhift has no corresponding connector-timing flags.

| Setting | Why it matters |
| --- | --- |
| `metrics_flush_interval: 60s`, `cache_loop: 1m` | Idle counters are re-exported for fifteen minutes and removed at the next cache loop. Re-export is not evidence of a new call; translation and startup warm-up depend on these intervals. Change neither. |
| `dimensions: [service.namespace]` | Gives both call ends the namespace used by instance identities. |
| `virtual_node_peer_attributes: []` | In 0.162.0 the empty list makes the connector convert no expired edge into a virtual node; this version has no other switch. An unpaired span is dropped. |
| `virtual_node_extra_label: true` | Normal paired-call points have an empty label. With peer attributes set, virtual points carry `connection_type=virtual_node` and `virtual_node=client` or `server`; toposhift skips them under `svcgraph.skipped_connection_virtual_node`. With this configuration, a nonzero count means peer attributes were set. |
| `metrics_timestamp_offset: 0s` | Leaves the collector timestamp unshifted. |
| `store.ttl: 2s`, `store.max_items: 1000` | Bounds the wait for the other span and number of half calls waiting. Spans arriving more than 2 seconds apart are not paired. |

## Sizing and status

Keep one replica: splitting the client/server spans across replicas loses pairs, and multiple connectors behind one producer are counted rather than merged. Give the trace collector a container memory limit above `memory_limiter.limit_mib: 600`. Capacity at supported scale has not been measured.

Its own queue is **in memory**, `queue_size: 100000000` bytes; a restart loses it. The cluster collector's metrics queue holds up to 500 MB on disk alongside the logs queue; see [disk sizing](buffering-and-recovery.md#the-disk-it-takes). An outage longer than capacity loses sightings. Silence beyond the configured TTL can create a gap ending in `liveness_expiry`; the next sighting starts a new interval.

The collector's telemetry is off (`service.telemetry.metrics.level: none`). To diagnose pairing, enable `level: basic` or higher, add a `readers` pull exporter and inspect connector counters.

`GET /status` includes a `metrics` object:

| Signal | Meaning |
| --- | --- |
| `svcgraph.sightings`, `spancount.sightings` | Sightings written. |
| `svcgraph.skipped_*`, `spancount.skipped_*`, `metrics.*` | Inputs set aside, with reasons. |
| `silent` | No service-graph point for three flushes; it does not prove there were no calls. |

Warnings cover a full service-graph series table, unusable instance IDs and missing service-graph points for more than three flushes. Silence may mean the collector/pipeline is down, the opt-in is off, or no calls occurred. A sender can deliberately fill the series table, which is another reason to enforce the input boundary.
