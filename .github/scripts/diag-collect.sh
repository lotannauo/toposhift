#!/usr/bin/env bash
# Gathers what one diag build job wrote into a directory to upload, writes job.json,
# which says what the job was and what it ran on, and adds the job's lines to the
# step summary. The diag workflow's build job runs it after the build, whatever the
# build did. Never db/: the database is gigabytes.
#
# Reads (all from the environment): WORK (the build's output directory), SLUG (the
# candidate's directory name inside it), OUT, LOGS, SPIKEBENCH (the binary), PLAN
# (the plan.json the build read), and the job's identity: OS, NAME, ROLE, REP,
# BUILD_FLAGS, PLAN_ID, BASE_SHA, PATCHED, PATCH_SHA256, TREE, CANDIDATE, WINDOW,
# FAMILY, PINS_WINDOW, RETENTION_MODE, PACE, RETAIN_FLUSH_DELS, SYNC, METRICS_EVERY,
# GO_MEM_LIMIT, REF, RUN_ID, RUN_ATTEMPT. Linux and macOS. Run from the workspace, where
# the checkout is src.
#
# Every upper-case name is an input from the environment; the lower-case one of the
# same name is its computed form.
# shellcheck disable=SC2153
set -uo pipefail

kind="$(uname -s)"

sha256_of() {
  if command -v sha256sum >/dev/null 2>&1; then
    sha256sum "$1" 2>/dev/null | cut -d' ' -f1
  else
    shasum -a 256 "$1" 2>/dev/null | cut -d' ' -f1
  fi
}

mkdir -p "$OUT"

manifest=no
if [ -f "$WORK/$SLUG/manifest.json" ]; then
  cp "$WORK/$SLUG/manifest.json" "$OUT/manifest.json"
  manifest=yes
fi
if [ -f "$WORK/$SLUG/metrics.jsonl" ]; then
  cp "$WORK/$SLUG/metrics.jsonl" "$OUT/metrics.jsonl"
fi
profiles=0
if [ -d "$WORK/$SLUG/profiles" ]; then
  cp -R "$WORK/$SLUG/profiles" "$OUT/profiles"
  profiles="$(find "$OUT/profiles" -type f | wc -l | tr -d ' ')"
fi
# index.json says what was written for each pass; it sits under profiles/ and is
# copied to the top as well, wherever the program put it.
for idx in "$WORK/$SLUG/index.json" "$WORK/$SLUG/profiles/index.json"; do
  if [ -f "$idx" ]; then
    cp "$idx" "$OUT/index.json"
    break
  fi
done
cp -R "$LOGS" "$OUT/logs"
[ ! -f "$LOGS/sync-probe.txt" ] || cp "$LOGS/sync-probe.txt" "$OUT/sync-probe.txt"

# What the job ran on: hosted runners of one label are not one machine, and a
# timing is read only within one CPU model.
case "$kind" in
  Linux)
    cpu="$(lscpu 2>/dev/null | awk -F': *' '/^Model name/ { print $2; exit }')"
    cpus="$(nproc 2>/dev/null || echo 0)"
    mem_kb="$(awk '/^MemTotal/ { print $2 }' /proc/meminfo 2>/dev/null)"
    rss="$(awk -F': ' '/Maximum resident set size/ { printf "%.0f", $2 / 1024 }' "$LOGS/time.txt" 2>/dev/null)"
    elapsed="$(awk '/Elapsed \(wall clock\)/ { print $NF }' "$LOGS/time.txt" 2>/dev/null)"
    ;;
  Darwin)
    cpu="$(sysctl -n machdep.cpu.brand_string 2>/dev/null)"
    cpus="$(sysctl -n hw.ncpu 2>/dev/null || echo 0)"
    mem_kb="$(($(sysctl -n hw.memsize 2>/dev/null || echo 0) / 1024))"
    # time -l: the peak in bytes; the first line has the wall time in seconds.
    rss="$(awk '/maximum resident set size/ { printf "%.0f", $1 / 1048576 }' "$LOGS/time.txt" 2>/dev/null)"
    elapsed="$(awk '/ real / { print $1; exit }' "$LOGS/time.txt" 2>/dev/null)"
    ;;
  *)
    cpu=unknown
    cpus=0
    mem_kb=0
    rss=""
    elapsed=""
    ;;
esac
cpu="${cpu:-unknown}"
cpus="${cpus:-0}"
mem_kb="${mem_kb:-0}"
rss="${rss:-0}"
elapsed="${elapsed:-unknown}"
binary_sha256="$(sha256_of "$SPIKEBENCH")"
binary_sha256="${binary_sha256:-unknown}"
plan_sha256="$(sha256_of "$PLAN")"
plan_sha256="${plan_sha256:-unknown}"
probe="$(cat "$LOGS/sync-probe.txt" 2>/dev/null || echo "not run")"
tree="$(git -C src rev-parse 'HEAD^{tree}' 2>/dev/null || echo unknown)"

jq -n \
  --arg os "$OS" --arg kernel "$(uname -r)" --arg arch "$(uname -m)" \
  --arg name "$NAME" --arg role "$ROLE" --argjson rep "$REP" \
  --arg build_flags "$BUILD_FLAGS" --arg plan_id "$PLAN_ID" \
  --arg base_sha "$BASE_SHA" --arg tree "$tree" --arg patched "$PATCHED" \
  --arg patch_sha256 "$PATCH_SHA256" --arg expected_tree "$TREE" \
  --arg candidate "$CANDIDATE" --argjson window "$WINDOW" --argjson pins_window "$PINS_WINDOW" \
  --arg family "$FAMILY" --arg retention_mode "$RETENTION_MODE" --argjson pace "$PACE" \
  --arg retain_flush_dels "$RETAIN_FLUSH_DELS" --arg sync "$SYNC" \
  --arg metrics_every "$METRICS_EVERY" --arg go_mem_limit "$GO_MEM_LIMIT" \
  --arg gomaxprocs "${GOMAXPROCS:-unset}" \
  --arg cpu_model "$cpu" --argjson nproc "$cpus" --argjson mem_total_kb "$mem_kb" \
  --arg image_os "${ImageOS:-unknown}" --arg image_version "${ImageVersion:-unknown}" \
  --arg runner_name "${RUNNER_NAME:-unknown}" \
  --arg ref "$REF" --arg run_id "$RUN_ID" --arg run_attempt "$RUN_ATTEMPT" \
  --arg binary_sha256 "$binary_sha256" --arg plan_sha256 "$plan_sha256" \
  --arg sync_probe "$probe" --arg elapsed "$elapsed" --argjson peak_rss_mib "$rss" \
  --argjson profiles "$profiles" --arg manifest "$manifest" \
  '{diagnostic: true, judged: false, os: $os, kernel: $kernel, arch: $arch,
    name: $name, role: $role, rep: $rep, build_flags: $build_flags, plan_id: $plan_id,
    base_sha: $base_sha, tree: $tree, patched: $patched, patch_sha256: $patch_sha256, expected_tree: $expected_tree,
    candidate: $candidate, window: $window, pins_window: $pins_window, family: $family,
    retention_mode: $retention_mode, pace: $pace, retain_flush_dels: $retain_flush_dels,
    sync: $sync, metrics_every: $metrics_every, go_mem_limit: $go_mem_limit, gomaxprocs: $gomaxprocs,
    cpu_model: $cpu_model, nproc: $nproc, mem_total_kb: $mem_total_kb,
    image_os: $image_os, image_version: $image_version, runner_name: $runner_name,
    ref: $ref, run_id: $run_id, run_attempt: $run_attempt,
    binary_sha256: $binary_sha256, plan_sha256: $plan_sha256, sync_probe: $sync_probe,
    elapsed: $elapsed, peak_rss_mib: $peak_rss_mib, profiles: $profiles, manifest: $manifest}' \
  >"$OUT/job.json"

{
  echo "### Diagnostic only: $NAME ($ROLE), $CANDIDATE, $WINDOW days, $OS, repetition $REP"
  echo
  echo "- manifest written: $manifest; profile files: $profiles"
  echo "- peak memory: ${rss} MiB; elapsed: ${elapsed}"
  echo "- hardware: $cpu, $cpus CPUs, $((mem_kb / 1024)) MiB memory, GOMAXPROCS ${GOMAXPROCS:-unset}"
  echo "- $probe"
  echo "- build flags: \`${BUILD_FLAGS:-none}\`; plan: $PLAN_ID (\`$plan_sha256\`)"
  echo "- base commit \`$BASE_SHA\`; tree \`$tree\`; patch: ${PATCHED} \`${PATCH_SHA256:-none}\`"
  echo "- binary sha256: \`$binary_sha256\`"
} >>"${GITHUB_STEP_SUMMARY:-/dev/null}"
