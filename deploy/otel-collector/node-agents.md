# Node agents

[Collector setup](README.md) · [Service traces](service-traces.md) · [Queues and recovery](buffering-and-recovery.md)

Run [nodeagent.yaml](nodeagent.yaml) as a DaemonSet when you need to know which host each Kubernetes node runs on. Without agents, placement stops at the node: two pods may share a node, but toposhift does not infer a shared host. With agents, it stores the host and the node → host `runs_on` edge under `nodeagent/<cluster name>`.

## Enable the receiver

Use one cluster collector per cluster. Agents send only to that collector, **never to toposhift**. The cluster collector sets the cluster name and sends their logs through the same exporter and ordered disk queue as the Kubernetes receivers.

The receiver is **off by default**. Enabling it opens an **unauthenticated** port: anything that reaches it can send records accepted by the logs pipeline, including forged Kubernetes objects and host events. Restrict port 4318 to the agents' pods with a network policy enforced by the cluster's network plugin. Enable it only when running agents.

Remove the prefix `#NODEAGENTS `, including its trailing space, from the five marked lines of `toposhift.yaml`. For example:

```sh
sed 's/^#NODEAGENTS //' toposhift.yaml > toposhift.enabled.yaml
```

This adds the receiver:

```yaml
  otlp:
    protocols:
      http:
        endpoint: 0.0.0.0:4318
```

and this entry after `k8s_cluster` in `service.pipelines.logs.receivers`:

```yaml
        - otlp
```

Use the generated configuration when starting the cluster collector. If also enabling service traces, remove both opt-in prefixes from the same configuration; do not overwrite one enabled copy with the other. `mise run check:collector` validates all combinations.

In `nodeagent.yaml`, replace `<GATEWAY_HOST>` in `exporters.otlp_http/gateway.endpoint` with the host where the agents reach that receiver on port 4318. The agents do not set a cluster name.

## Pod requirements

Each agent needs:

- `K8S_NODE_NAME` from the downward API's `spec.nodeName`;
- a service account allowed to `get` nodes;
- a read-only mount of the single host file `/etc/machine-id` at the same container path;
- no `hostNetwork`: it adds no identity information toposhift uses and can clash with collector telemetry ports.

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

Without node read permission, events lack `k8s.node.uid`: toposhift may store the host, but no placement edge, and warns with `nodeagent.refused_no_node_uid`. Without the machine-id file or a cloud detector supplying `host.id`, it stores no host and warns with `nodeagent.refused_no_host_id`. Container-based clusters may lack that file, leaving placement at the node.

## Start and upgrade

The `host_metrics` logs output has **Development** stability in the pinned Contrib 0.162.0 configuration. Agents need the entity-events feature gate:

```sh
otelcol-contrib --config nodeagent.yaml \
  --feature-gates=pkg.experimentalmetricmetadata.useEntityEventsSpecification
```

On each collector upgrade, check that the host entity-event shape is still accepted. Without usable refreshes, hosts and their placement edges lapse after their TTL, about 15 minutes with the defaults. Missing the gate produces legacy entity events rather than the shape toposhift reads.

## Identity and descriptions

Host identity is the raw `host.id`; node identity is `k8s.node.uid`. Values are matched exactly, without trimming or case changes. The collector does not hash them before sending; toposhift computes their fingerprints.

The detector order is `env`, `k8s_api`, `ec2`, `gcp`, `azure`, `oraclecloud`, `system`. Cloud instance IDs take precedence over a system machine id; detection runs once at startup with `timeout: 5s`. Neither host/node names nor addresses join the node to a host.

Two nodes reporting one `host.id` describe one host. Conflicting boots for that host can trigger clone quarantine. The boot ID comes from `/proc/sys/kernel/random/boot_id`, with its trailing whitespace removed.

The host description includes boot ID, architecture, OS type, CPU model and provider details when present. `host.arch` describes the collector binary's architecture, so it can differ under emulation. OS version/description are omitted because they describe the collector's container. Zones are not written by this version.

## Identity privacy

Treat machine id values as **confidential**. Raw `host.id` travels between collectors and is held in their queues and capture files; captures also contain Kubernetes node status fields such as machine ID and system UUID. Protect those files and volumes accordingly; fixtures should use throwaway IDs.

toposhift stores the host's 16-byte identity fingerprint rather than the raw identifying value. Current Kubernetes node descriptions omit machine ID and system UUID, but keep the boot ID. Earlier records and activity files may still contain those fields: store retention ages old node records out of L2's 30-day window when enabled, while activity files keep them until removed. The fingerprint is a stable pseudonym across installations, not anonymization.

## Intervals and checks

Keep `metadata_collection_interval` aligned with `--node-interval` / `TOPOSHIFT_NODE_INTERVAL`: both default to 5 minutes. The supported flag is a whole number of seconds from one minute to one hour. `collection_interval` only paces the scrapers.

Every agent Observe gets a TTL of three configured intervals. A differing interval in an event is substituted, stored and counted as `nodeagent.substituted_configured_interval`; it does not change the configured TTL.

`GET /status` includes the node interval, counters, latest host event per cluster and whether three intervals have passed without one. The server warns when a previously active cluster goes silent. It also counts legacy entity events in `sequencer.legacy_entity_events` and warns at most once a minute.

| Counter family | Meaning |
| --- | --- |
| `host_events` | Every host event received. |
| `refused_*` | A record or part was not stored: `refused_no_host_id`, `refused_no_node_uid`, `refused_zone_id`. |
| `substituted_*` | Stored with a replacement: `substituted_receipt_no_time`, `substituted_receipt_implausible_time`, `substituted_no_boot`, `substituted_bad_boot`, `substituted_configured_interval`. |
| `skipped_*` | Set aside. |

A refused node binding can still leave a stored host. Read the individual counter, not just its family, when diagnosing missing placement.
