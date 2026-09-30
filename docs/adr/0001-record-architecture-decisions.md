# 1. Record architecture decisions

Date: 2026-09-30

## Status

Accepted

## Context

toposhift's design has many decisions that are expensive to reverse later: the identity encoding, the storage key layout, the query semantics. The reasoning behind them lives in a long design plan that is revised as we learn. A decision made months ago needs a stable record of why, separate from a plan that keeps changing.

## Decision

We record each significant decision as an Architecture Decision Record in `docs/adr/`, in the format Michael Nygard described: title, date, status, context, decision, consequences. One decision per file, numbered in order, written in the same pull request as the change it governs.

An ADR is never edited to change its decision. To reverse a decision, a new ADR supersedes the old one, and the old one's status changes to "Superseded by N".

Once an ADR exists, it wins over the design plan. When reality contradicts the plan, write an ADR.

## Consequences

Decisions carry their reasoning, so a future contributor (or agent) can tell a deliberate choice from an accident. Every decision costs a short document, which we accept.
