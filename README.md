# toposhift

A temporal topology graph for infrastructure. It stores only what changed in each neighborhood, like a git commit graph for hosts, connections and services, and lets you query any instant or window for blast radius.

Hosts are transient and events arrive from Kubernetes, integrations and webhooks. Given a service name and a window `t1` to `t2`, toposhift answers: which hosts and services sat one hop upstream and downstream, and which services shared a host, as things changed over that window.

## Status

Early design and scaffolding. Nothing here is usable yet.

## Build

Tool versions are pinned in `mise.toml`.

```
mise install
mise run ci
```

If mise cannot install a tool on your platform, install that tool with your package manager at the pinned version.

## License

Apache-2.0. See [LICENSE](LICENSE).
