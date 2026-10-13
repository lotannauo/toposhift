#!/usr/bin/env bash
# Checks the inputs of a dispatch of the bench workflow and plans its jobs. The
# workflow's setup job runs it, and a Go test beside it runs it on every pull request
# with each input it must refuse, so that the checks are tested before a dispatch
# depends on them.
#
# Every input arrives in the environment, never in the command line, and no message
# repeats an input: its text is not trusted, and a line that starts with "::" is a
# command to the runner. What it reads:
#
#   REF, SHA                 the ref the run was dispatched from and its commit
#   RUN_ID, RUN_ATTEMPT      the run and its attempt
#   FAMILY WINDOWS CANDIDATES REPS ARCHES PINS_WINDOW SYNC METRICS_EVERY GO_MEM_LIMIT
#   MAX_PARALLEL RETENTION_MODE RETAIN_FLUSH_DELS EXPECTED_SHA
#                            the dispatch's inputs
#   GITHUB_OUTPUT, GITHUB_STEP_SUMMARY, RUNNER_TEMP
#
# A dispatch from a tag needs the history of origin/main, to check that the tag's
# commit is on it. It exits 0 with the outputs written, or 1 with an error.
#
# Every upper-case name is an input from the environment; the lower-case one of the
# same name is its checked form.
# shellcheck disable=SC2153
set -euo pipefail

fail() {
  echo "::error::$1" >&2
  exit 1
}

hex40='^[0-9a-f]{40}$'
[[ $SHA =~ $hex40 ]] || fail "the run's commit is not 40 hexadecimal digits"

# Timed runs come from a merged commit: main, or a tag that freezes a commit of main
# for a series of dispatches. The tag is matched by an anchored expression, not a glob.
# The tag carries this file too, so a dispatch from it runs the frozen workflow. This
# check is a convention, not enforcement: a dispatch with --ref <branch> runs that
# branch's copy of the workflow and of this file, which can skip it. The enforcement is
# offline: spikebench storegate -commit refuses a manifest whose revision is not the
# frozen commit, and the timing judge, given -git, refuses one whose revision is not an
# ancestor of main.
case "$REF" in
  refs/heads/main) ;;
  *)
    re='^refs/tags/gate-freeze-[0-9]{8}$'
    [[ $REF =~ $re ]] ||
      fail "the ref: timed runs are dispatched from main or from a tag gate-freeze-YYYYMMDD"
    git merge-base --is-ancestor "$SHA" origin/main 2>/dev/null ||
      fail "the ref: the tag's commit is not on origin/main"
    ;;
esac

# The scenario flags of each family are fixed here and never taken from free text:
# the plans job splits them into words.
case "$FAMILY" in
  rmae) family_flags="-run-max-age 30m" ;;
  ph5m-rmae) family_flags="-pod-heartbeat 5m -run-max-age 10m" ;;
  *) fail "family: not one of rmae, ph5m-rmae" ;;
esac

case "$ARCHES" in
  both) oses=(ubuntu-24.04 ubuntu-24.04-arm) ;;
  x64) oses=(ubuntu-24.04) ;;
  arm64) oses=(ubuntu-24.04-arm) ;;
  *) fail "arches: not one of both, x64, arm64" ;;
esac

case "$SYNC" in
  true | false) ;;
  *) fail "sync: not true or false" ;;
esac

# The pace is not an input: the gate admits no other rate for a background build.
case "$RETENTION_MODE" in
  sync) pace=0 ;;
  background) pace=4500 ;;
  *) fail "retention_mode: not sync or background" ;;
esac

# A Go duration: one or more numbers, each with a unit.
metrics_every="${METRICS_EVERY//[[:space:]]/}"
if [ -n "$metrics_every" ]; then
  re='^([0-9]+(\.[0-9]+)?(ns|us|ms|s|m|h))+$'
  [[ $metrics_every =~ $re ]] ||
    fail "metrics_every: empty or a Go duration such as 30s"
fi

# A whole number of MiB or GiB, the form the judge parses back.
go_mem_limit="${GO_MEM_LIMIT//[[:space:]]/}"
if [ -n "$go_mem_limit" ]; then
  re='^[1-9][0-9]{0,5}(MiB|GiB)$'
  [[ $go_mem_limit =~ $re ]] ||
    fail "go_mem_limit: empty or a whole number of MiB or GiB such as 11GiB"
fi

re='^[1-9][0-9]?$'
[[ $PINS_WINDOW =~ $re ]] && [ "$PINS_WINDOW" -le 60 ] ||
  fail "pins_window: an integer from 1 to 60"

re='^([1-9]|10)$'
[[ $REPS =~ $re ]] ||
  fail "reps: an integer from 1 to 10"

re='^([1-9]|1[0-9]|20)$'
[[ $MAX_PARALLEL =~ $re ]] ||
  fail "max_parallel: an integer from 1 to 20"

windows="${WINDOWS//[[:space:]]/}"
re='^[0-9]+(,[0-9]+)*$'
[[ $windows =~ $re ]] ||
  fail "windows: a comma list of integers"
IFS=, read -ra wins <<<"$windows"
[ "${#wins[@]}" -le 4 ] ||
  fail "windows: at most 4 windows in one dispatch"
re='^[1-9][0-9]?$'
previous=0
for w in "${wins[@]}"; do
  [[ $w =~ $re ]] && [ "$w" -le 31 ] ||
    fail "windows: every window is an integer from 1 to 31 days"
  [ "$w" -gt "$previous" ] ||
    fail "windows: the windows must be increasing"
  previous="$w"
done
if [ "$PINS_WINDOW" -gt "${wins[0]}" ]; then
  echo "::warning::pins_window is longer than the shortest window"
fi

candidates="${CANDIDATES//[[:space:]]/}"
re='^[^,]+(,[^,]+)*$'
[[ $candidates =~ $re ]] ||
  fail "candidates: a comma list of names"
IFS=, read -ra cands <<<"$candidates"
re='^[A-Za-z0-9][A-Za-z0-9/+._-]*$'
seen=" "
all_root=true
for c in "${cands[@]}"; do
  [[ $c =~ $re ]] ||
    fail "candidates: every name must match ^[A-Za-z0-9][A-Za-z0-9/+._-]*\$"
  # The slug names an artifact and the candidate's directory, so two names with one
  # slug would write over each other.
  case "$seen" in
    *" ${c//\//_} "*) fail "candidates: two names give the same directory" ;;
  esac
  seen="$seen${c//\//_} "
  case "$c" in
    Lroot/*) ;;
    *) all_root=false ;;
  esac
done

# The retention flush threshold takes one of the registered rungs, or the store's
# default; a rung is measured only on a background build of the root store.
case "$RETAIN_FLUSH_DELS" in
  default | 32768 | 24576 | 16384) ;;
  *) fail "retain_flush_dels: not one of default, 32768, 24576, 16384" ;;
esac
if [ "$RETAIN_FLUSH_DELS" != default ]; then
  [ "$RETENTION_MODE" = background ] ||
    fail "retain_flush_dels: a threshold is measured only with retention_mode background"
  [ "$all_root" = true ] ||
    fail "retain_flush_dels: a threshold is for the candidates built through the root store (Lroot/...) only"
fi

# A background dispatch is one arm of the store gate: one window of 7 or 30 days, its
# commits synced, its metrics sampled (Q5 reads the live heap from them), the root
# store's candidates only (only they are paced), and the commit it expects named.
if [ "$RETENTION_MODE" = background ]; then
  case "${#wins[@]}:${wins[0]}" in
    1:7 | 1:30) ;;
    *) fail "windows: a background dispatch builds exactly one window, of 7 or 30 days" ;;
  esac
  [ "$SYNC" = true ] ||
    fail "sync: a background dispatch syncs every commit"
  [ -n "$metrics_every" ] ||
    fail "metrics_every: a background dispatch samples its metrics"
  [ "$all_root" = true ] ||
    fail "candidates: a background dispatch builds the candidates through the root store (Lroot/...) only"
fi

# The commit every job must be at: the dispatch's own, which the dispatch names
# (required for a background dispatch), so that a moved main or a wrong tag fails here.
expected_sha="${EXPECTED_SHA//[[:space:]]/}"
if [ -n "$expected_sha" ]; then
  [[ $expected_sha =~ $hex40 ]] ||
    fail "expected_sha: 40 lowercase hexadecimal digits"
  [ "$expected_sha" = "$SHA" ] ||
    fail "expected_sha: not the commit this run was dispatched at"
elif [ "$RETENTION_MODE" = background ]; then
  fail "expected_sha: a background dispatch names the commit it expects"
else
  expected_sha="$SHA"
fi

jobs=$((${#oses[@]} * ${#wins[@]} * ${#cands[@]} * REPS))
[ "$jobs" -le 256 ] ||
  fail "the matrix has $jobs jobs and a matrix may have at most 256"

# The matrix, compact and on one line (fromJSON reads it from a job output). The
# names were checked above, so none needs escaping.
tsv="$RUNNER_TEMP/matrix.tsv"
: >"$tsv"
matrix='{"include":['
sep=""
for os in "${oses[@]}"; do
  for w in "${wins[@]}"; do
    for c in "${cands[@]}"; do
      for ((rep = 1; rep <= REPS; rep++)); do
        printf '%s\t%s\t%s\t%s\t%s\n' "$os" "$w" "$c" "${c//\//_}" "$rep" >>"$tsv"
        matrix+="$sep{\"os\":\"$os\",\"window\":$w,\"candidate\":\"$c\",\"slug\":\"${c//\//_}\",\"rep\":$rep}"
        sep=","
      done
    done
  done
done
matrix+=']}'

{
  echo "matrix=$matrix"
  echo "family=$FAMILY"
  echo "family_flags=$family_flags"
  echo "windows=$windows"
  echo "pins_window=$PINS_WINDOW"
  echo "sync=$SYNC"
  echo "metrics_every=$metrics_every"
  echo "go_mem_limit=$go_mem_limit"
  echo "max_parallel=$MAX_PARALLEL"
  echo "retention_mode=$RETENTION_MODE"
  echo "pace=$pace"
  echo "retain_flush_dels=$RETAIN_FLUSH_DELS"
  echo "expected_sha=$expected_sha"
  echo "plans_key=bench-plans-${RUN_ID}-${RUN_ATTEMPT}"
} >>"$GITHUB_OUTPUT"

{
  echo "### Planned jobs: $jobs"
  echo
  echo "- ref: \`$REF\`; commit: \`$expected_sha\`"
  echo "- family: \`$FAMILY\` (scenario flags \`$family_flags\`)"
  echo "- windows: \`$windows\` days; pins chosen from the \`$PINS_WINDOW\`-day window"
  echo "- sync: \`$SYNC\`; metrics every: \`${metrics_every:-off}\`"
  echo "- retention mode: \`$RETENTION_MODE\`; pace: \`$pace\` records/s (0: unpaced); retention flush threshold: \`$RETAIN_FLUSH_DELS\`"
  echo "- Go memory limit: \`${go_mem_limit:-none}\`"
  echo "- at most \`$MAX_PARALLEL\` build jobs at once"
  echo
  echo "| runner | window (days) | candidate | repetition |"
  echo "|---|---|---|---|"
  while IFS=$'\t' read -r os w c _ rep; do
    echo "| $os | $w | \`$c\` | $rep |"
  done <"$tsv"
} >>"$GITHUB_STEP_SUMMARY"
