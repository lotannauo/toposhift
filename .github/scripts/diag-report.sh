#!/usr/bin/env bash
# Writes one table for a whole diag dispatch into the step summary, from the job.json
# of every build job's artifact (one directory each under REPORT_DIR). It reads
# nothing but job.json and compares nothing: reading a variant against the spread of
# the control is the analysis's work, on the profiles and metrics, offline.
#
# What it does flag: a runner label whose jobs ran on more than one CPU model, since a
# pair is read only within one, and a job that wrote no manifest.
set -uo pipefail

: "${REPORT_DIR:?}"
summary="${GITHUB_STEP_SUMMARY:-/dev/stdout}"

jobs=()
while IFS= read -r f; do
  jobs+=("$f")
done < <(find "$REPORT_DIR" -name job.json -type f | sort)

{
  echo "### Diagnostic dispatch: what the build jobs ran on"
  echo
  echo "**Diagnostic only.** Not a judged arm, in no pool, never compared with another machine or another runner label, nor with the figures of bench.yml, whose builds run at the default GOMAXPROCS, not 10. A variant is read only where it differs from the control by more than the control's own repetitions differ, and only within one CPU model."
  echo
  if [ "${#jobs[@]}" -eq 0 ]; then
    echo "No build job uploaded a job.json."
    exit 0
  fi
  echo "| runner | configuration | role | rep | CPU model | peak memory | sync p50 / p99 (us) | manifest | profile files | binary sha256 |"
  echo "|---|---|---|---|---|---|---|---|---|---|"
  jq -s -r 'sort_by(.os, .role, .name, .rep)[] |
    [.os, "`" + .name + "`", .role, (.rep | tostring), .cpu_model, ((.peak_rss_mib | tostring) + " MiB"),
     ((.sync_probe | capture("p50_us=(?<a>[0-9]+) p99_us=(?<b>[0-9]+)") // {a: "?", b: "?"}) | .a + " / " + .b),
     .manifest, (.profiles | tostring), "`" + (.binary_sha256[0:12]) + "`"] | "| " + join(" | ") + " |"' \
    "${jobs[@]}"
  echo
  jq -s -r '
    group_by(.os)[] | select((map(.cpu_model) | unique | length) > 1) |
    "- **" + .[0].os + "**: the jobs ran on " + (map(.cpu_model) | unique | join("; ")) + ". Read a pair only within one model."' \
    "${jobs[@]}"
  jq -s -r '.[] | select(.manifest != "yes") | "- **no manifest**: " + .os + " " + .name + " repetition " + (.rep | tostring)' \
    "${jobs[@]}"
  jq -s -r '[.[] | .base_sha + " " + .tree + " " + .patch_sha256] | unique | if length > 1 then "- **the jobs did not build one tree**" else empty end' \
    "${jobs[@]}"
} >>"$summary"
