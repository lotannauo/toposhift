# Running toposhift

This page is for whoever runs `toposhift serve` and decides what it keeps. The [README](../README.md) has the short version; this page has the defaults, the rules and the reasons.

`toposhift serve` takes in what an OpenTelemetry collector reads from a Kubernetes cluster (pods, nodes and containers; hosts, through the optional node agents; services and the calls between them, through the optional trace pipeline) and keeps it in an on-disk store, with no configuration. It serves the read-only tools on loopback.

`toposhift demo` serves the same tools from a made-up cluster, with nothing to set up. Interfaces, answers and the store format may change between releases.

## Defaults and routes

`serve` needs no configuration. It listens on `127.0.0.1:7070` and keeps its data under `$HOME/.local/share/toposhift` (`--data-dir` or `TOPOSHIFT_DATA_DIR` names another directory).

It takes OpenTelemetry logs at `POST /v1/logs` and metrics at `POST /v1/metrics`, serves the read-only tools at `POST /mcp`, and reports its state at `GET /status`. `GET /healthz` answers while the process is up. `./bin/toposhift serve -h` lists the flags.

A ready-made collector configuration, with its README, is in [deploy/otel-collector](../deploy/otel-collector).

| Flag | Environment | Sets |
|---|---|---|
| `--listen` | `TOPOSHIFT_LISTEN` | The address to listen on |
| `--data-dir` | `TOPOSHIFT_DATA_DIR` | The data directory |
| `--retain` | `TOPOSHIFT_RETAIN` | Retention, `on` or `off` |
| `--cluster-name` | `TOPOSHIFT_CLUSTER_NAME` | The cluster the logs come from |
| `--node-interval` | `TOPOSHIFT_NODE_INTERVAL` | The report interval of the node agents |
| `--dependency-expiry` | `TOPOSHIFT_DEPENDENCY_EXPIRY` | How long a call outlives its last sighting |
| `--instance-ttl` | `TOPOSHIFT_INSTANCE_TTL` | How long a service instance outlives its last span count |

The data directory is the first of these that is set: `--data-dir`, `$TOPOSHIFT_DATA_DIR`, `$STATE_DIRECTORY` when it is one absolute path, then `$HOME/.local/share/toposhift`. One instance owns a directory at a time.

Without a data directory, `serve` still boots and answers `/healthz`, but ingest is off and a `POST` to `/v1/logs` or `/v1/metrics` is answered 503 and says to set one. A backup that copies the directory while toposhift runs may copy it mid-write: stop the instance, or snapshot the volume.

## Loopback and the tools

The tools are served only when `serve` listens on loopback, because the server has no authentication yet. A collector inside a cluster cannot reach loopback, and the collector README tells it to set `TOPOSHIFT_LISTEN` to an address the cluster can reach.

So today one `serve` either takes ingest from a remote collector or serves the tools, not both, unless the collector reaches the machine's loopback: a collector on the same machine, or a forwarded port (whatever reaches the forward reaches every route). With an address that is not loopback, `serve` says at start that the tools are off.

The ingest routes accept whatever reaches them. Restrict who can reach them with the network.

## Retention

`toposhift serve` can discard old history once a day: L2 (pods, nodes) keeps 30 days, L1 (hosts) 90 days, and L0 is kept. `--retain on` or `TOPOSHIFT_RETAIN=on` turns it on, and `off` turns it off; the flag wins over the variable. L3 (services and their calls) is retained with L2, so it keeps 30 days.

The default depends on the build:

- **Releases made before background retention passes its latency check on CI hardware** ship with retention **off** by default, and v0.1.0 is the first. `go install …@v0.1.0` installs it, and `@latest` installs the newest release, which has retention off while it is one of these releases.

  `serve` then keeps every record it is sent, so its store grows for as long as it runs, and reads of objects with a long history cost more over time.

  Background retention has not yet passed that check, so these releases do not turn it on by default. A checkout of a release branch (`release-*`, such as `release-v0.1.0`) builds with retention off too.
- **Builds of `main`** (`go install …@main`, or `mise run build` in a checkout of main) keep retention **on** by default.

When retention is off by default and `serve` has a data directory, `serve` logs a warning at start that says how to turn it on.

`GET /status` reports where the mode came from, for example `"retention":{"mode":"off","set_by":"default"}`; `set_by` is `default`, `flag` or `environment`. A store that ran without retention loses what is older than each layer's window at its first retention, once the producers' clocks agree with the server's.

This server's clock decides what is old, so the times that producers' records carry must agree with it within a day. While they disagree, nothing is discarded and the log warns. An error from a retention stops retention until `serve` is restarted, and the log says so at every check.

## Services and their calls

The optional trace pipeline sends two metrics to `POST /v1/metrics`: one says that a service called another, and one says that an instance of a service ran. The body is protobuf; a JSON body is answered 415.

From them `serve` stores services, the calls between them (`depends_on`), service instances, and the pod each instance runs in. It stores no count, rate or latency of a call, only that it was seen.

A call ends at its last sighting plus `--dependency-expiry`, and an instance at its last span count plus `--instance-ttl`. Both are 6 hours by default, each is a whole number of seconds from 2m to 8784h, and an end shows as `liveness_expiry`.

The 6-hour defaults are a starting point, not tuned on production traffic. A call seen less often than once per `--dependency-expiry` shows as ended between sightings and starts again at the next one; raise the flag for calls that run rarely, such as nightly jobs. Lower either flag to see a stopped service or instance end sooner.

Setup, the cluster name and the network policies are in the [collector README](../deploy/otel-collector/README.md#services-and-their-calls). The receiver has no authentication: anything that reaches it can add services, instances and calls.

## Builds

The documented install builds without cgo:

```sh
CGO_ENABLED=0 go install github.com/lotannauo/toposhift/cmd/toposhift@latest
```

A cgo build links Pebble's optional C zstd, which the store does not use (block compression is Pebble's default, Snappy).

To build from source, tool versions are pinned in `mise.toml`:

```sh
mise install
mise run build
```

`mise run build` writes `bin/toposhift`, without cgo. If mise cannot install a tool on your platform, build with the `go build` line under "Try it without a cluster" in the [README](../README.md#try-it-without-a-cluster). To contribute, see [CONTRIBUTING.md](../CONTRIBUTING.md).
