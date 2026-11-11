# Findings

Start with the [November 2026 update](2026-11-update.md): what the later storage investigations established, how they changed the decision, and what remains unresolved.

Per-entity history with checkpoints has a demonstrated benefit on selected warm historical reads. Native Postgres interval tables also bounded read work in their registered experiment. A report-only background-retention observation remains unfavorable. These studies have different contracts and configurations; they do not rank complete systems or establish serving-path performance.

| Read | Purpose |
|---|---|
| [Current synthesis: November 2026](2026-11-update.md) | Later evidence, decisions it narrowed, remaining questions and reproduction limits |
| [Warm-read figure](2026-11-warm-reads.md) | All primary aarch64 timings, job variation, control and secondary outcomes |
| [Historical report](REPORT.md) | Earlier measurements at their recorded commits, with their original scope and dated corrections |
| [Benchmark guide](../README.md) | Tools, protocols and command references |

## Publication rules

The findings report lives here. It is a showcase of what was measured and compared, not an official benchmark publication.

Every figure in it carries its provenance:

- `CI run <id>`: a number from a GitHub-hosted run, read from the judge's printed output. Timings, rates and memory figures are quoted only with a CI run id.
- `local, deterministic`: a count made on a laptop, never on the Oracle Cloud VM, that is a pure function of the commit, the input hashes and the flags, quoted only after two runs gave the same digest. It is given with the commit, the input sha256 and the command that reproduces it. Block and byte counters qualify only when taken from a canonical store.

No raw output is committed, except the evaluation's results file, kept beside its frozen scorer in `bench/eval/results`: the report quotes figures in its text, and `check:no-results` refuses the file names the bench tools most often write. Anything the report cannot attribute in one of these two ways is not quoted.
