# 2. No multi-tenancy

Date: 2026-09-30

## Status

Accepted

## Context

toposhift stores a topology graph whose entities are identified by a fingerprint: a hash of type-tagged identifying attributes. Supporting several mutually distrusting tenants in one deployment would touch every layer: a tenant component in every key and in the fingerprint, per-tenant quotas and retention, authorization on every read path, and isolation in caches and live subscriptions.

The project targets a single operator's infrastructure (and a self-hoster's own). Running on a free tier leaves no budget for the operational burden of isolating tenants.

## Decision

One toposhift deployment is one trust domain. There is no tenant identifier in keys, fingerprints, APIs or caches, and no per-tenant isolation inside a process. Anyone with read access to a deployment can read all of it.

Hosting for several parties means running one instance each.

## Consequences

- Keys, fingerprints and queries stay simple and fast.
- The fingerprint has no tenant component. Adding one later would change the identity encoding, which means new golden vectors and a superseding ADR, and would invalidate stored data.
- Identity collisions across unrelated organizations are not a concern of this design, because two organizations never share a deployment.
- Access control is a deployment-level concern (loopback by default; authentication arrives with the admin UI and API keys).
