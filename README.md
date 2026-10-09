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

## Install

```sh
CGO_ENABLED=0 go install github.com/lotannauo/toposhift/cmd/toposhift@latest
```

The documented install builds without cgo. A cgo build links Pebble's optional C zstd, which the store does not use (block compression is Pebble's default, Snappy).

## License

Apache-2.0. See [LICENSE](LICENSE).
