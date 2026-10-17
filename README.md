# toposhift

A temporal topology graph for infrastructure. It stores only what changed in each neighborhood, like a git commit graph for hosts, connections and services. You can query any instant or window for blast radius.

Hosts are transient. Events arrive from Kubernetes, integrations and webhooks. Given a service name and a window `t1` to `t2`, toposhift answers: which hosts and services sat one hop upstream and downstream, and which services shared a host, as things changed over that window.

## Status

Early design and scaffolding. Nothing here is usable yet.

## Build

Tool versions are pinned in `mise.toml`.

```sh
mise install
mise run ci
```

Run the server. It needs no configuration and listens on loopback only:

```sh
mise run build
./bin/toposhift serve
curl http://127.0.0.1:7070/healthz
```

If mise cannot install a tool on your platform, install that tool with your package manager at the pinned version.

## Collecting from Kubernetes

A ready-made OpenTelemetry collector configuration, with its README, is in [deploy/otel-collector](deploy/otel-collector).

## Install

```sh
CGO_ENABLED=0 go install github.com/lotannauo/toposhift/cmd/toposhift@latest
```

The documented install builds without cgo. A cgo build links Pebble's optional C zstd, which the store does not use (block compression is Pebble's default, Snappy).

## Retention

`toposhift serve` can discard old history once a day: L2 (pods, nodes) keeps 30 days, L1 (hosts) 90 days, and L0 is kept. `--retain on` or `TOPOSHIFT_RETAIN=on` turns it on, and `off` turns it off; the flag wins over the variable.

The default depends on the build:

- **Releases made before background retention passes its latency check on CI hardware** ship with retention **off** by default, and v0.1.0 is the first. `go install …@v0.1.0` installs it, and once v0.1.0 is tagged `@latest` installs the newest release, which has retention off while it is one of these releases. `serve` then keeps every record it is sent, so its store grows for as long as it runs, and reads of objects with a long history cost more over time. Background retention has not yet passed that check, so these releases do not turn it on by default. A checkout of a release branch (`release-*`), once one exists, builds with retention off too.
- **Builds of `main`** (`go install …@main`, or `mise run build` in a checkout of main) keep retention **on** by default.

When retention is off by default and `serve` has a data directory, `serve` logs a warning at start that says how to turn it on. `GET /status` reports where the mode came from, for example `"retention": {"mode": "off", "set_by": "default"}`. A store that ran without retention loses what is older than each layer's window at its first retention, once the producers' clocks agree with the server's.

## License

Apache-2.0. See [LICENSE](LICENSE).
