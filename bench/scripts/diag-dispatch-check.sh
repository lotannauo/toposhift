#!/usr/bin/env bash
# Checks the inputs of a dispatch of the diag workflow and plans its jobs. The
# workflow's setup job runs it, and a Go test beside it runs it on every pull request
# with each input it must refuse, so that the checks are tested before a dispatch
# depends on them.
#
# A diag dispatch builds one candidate in two or more configurations on the runners
# it names, each configuration at least twice, and uploads what the builds wrote. The
# first configuration is the control (A) and the others are variants: a variant is
# read only against the spread of the control's own repetitions, on one runner label.
# Nothing it produces is a judged arm.
#
# Every input arrives in the environment, never in the command line, and no message
# repeats an input: its text is not trusted, and a line that starts with "::" is a
# command to the runner. What it reads:
#
#   REF, SHA                 the ref the run was dispatched from and its commit
#   LABEL BASE_SHA HAS_PATCH_B64 GIST_ID PATCH_SHA256 TREE
#                            what to build: the commit on main, and the optional patch
#                            on it (HAS_PATCH_B64 is true or false: the patch itself is
#                            checked by apply-patch.sh)
#   CONFIGS FAMILY WINDOW PINS_WINDOW CANDIDATE REPS RUNNERS MACOS_LOW_RSS
#   RETENTION_MODE RETAIN_FLUSH_DELS SYNC METRICS_EVERY GO_MEM_LIMIT PACE MAX_PARALLEL
#                            the dispatch's inputs
#   GITHUB_OUTPUT, GITHUB_STEP_SUMMARY, RUNNER_TEMP
#
# It runs in a checkout with the history of origin/main, to check that the base
# commit is on it. It exits 0 with the outputs written, or 1 with an error.
#
# Every upper-case name is an input from the environment; the lower-case one of the
# same name is its checked form. Written for bash 3.2 too, the bash a Mac has.
# shellcheck disable=SC2153
set -euo pipefail

fail() {
  echo "::error::$1" >&2
  exit 1
}

hex40='^[0-9a-f]{40}$'
hex64='^[0-9a-f]{64}$'

[[ $SHA =~ $hex40 ]] || fail "the run's commit is not 40 hexadecimal digits"

re='^[A-Za-z0-9._-]{1,40}$'
[[ $LABEL =~ $re ]] || fail "label: 1 to 40 letters, digits, dots, dashes and underscores"

# The build is at a commit of main. A build of a patch (code that is not on main)
# runs only from the verify branch, whose caches no other workflow reads: a patch is
# code that is not reviewed yet, and what its job saves must not reach the caches of
# main.
# This is a convention, not enforcement: a dispatch with --ref <branch> runs that
# branch's copy of the workflow and of this file. The enforcement is that nothing a
# diag run produces is ever a judged arm.
case "$HAS_PATCH_B64" in
  true | false) ;;
  *) fail "has_patch_b64: not true or false" ;;
esac
patched=false
if [ "$HAS_PATCH_B64" = true ] || [ -n "$GIST_ID" ]; then
  patched=true
fi
case "$REF" in
  refs/heads/verify) ;;
  refs/heads/main)
    [ "$patched" = false ] ||
      fail "the ref: a build of a patch is dispatched from the verify branch, not from main"
    ;;
  *) fail "the ref: diagnostics are dispatched from main, or from the verify branch" ;;
esac

[[ $BASE_SHA =~ $hex40 ]] || fail "base_sha: 40 lowercase hexadecimal digits"
git merge-base --is-ancestor "$BASE_SHA" origin/main 2>/dev/null ||
  fail "base_sha: the commit is not on origin/main"

# The workflow and the scripts of a dispatch come from the head of the ref it was
# dispatched from, so that head must be a commit of main: a verify branch with a commit
# of its own would run whatever it carries.
git merge-base --is-ancestor "$SHA" origin/main 2>/dev/null ||
  fail "the ref: the dispatched commit is not on origin/main (the verify branch must point at a commit of main)"

if [ "$patched" = true ]; then
  [ -z "$GIST_ID" ] || [ "$HAS_PATCH_B64" = false ] ||
    fail "patch_b64 and gist_id: give the patch one way, not both"
  [[ $PATCH_SHA256 =~ $hex64 ]] ||
    fail "patch_sha256: 64 lowercase hexadecimal digits, required with a patch"
  [[ $TREE =~ $hex40 ]] ||
    fail "tree: 40 lowercase hexadecimal digits, required with a patch"
  re='^[0-9a-f]{20,40}$'
  [ -z "$GIST_ID" ] || [[ $GIST_ID =~ $re ]] ||
    fail "gist_id: 20 to 40 lowercase hexadecimal digits"
else
  [ -z "$PATCH_SHA256" ] && [ -z "$TREE" ] ||
    fail "patch_sha256 and tree: given without a patch"
fi

case "$FAMILY" in
  rmae) family_flags="-run-max-age 30m" ;;
  ph5m-rmae) family_flags="-pod-heartbeat 5m -run-max-age 10m" ;;
  *) fail "family: not one of rmae, ph5m-rmae" ;;
esac

# One window, of at most 7 days: a 30-day build may need more than the 6 hours a job
# may run.
case "$WINDOW" in
  1 | 2 | 7) ;;
  *) fail "window: not one of 1, 2, 7 (a 30-day build may not fit the 6 hours of a job)" ;;
esac
re='^[1-9][0-9]?$'
[[ $PINS_WINDOW =~ $re ]] && [ "$PINS_WINDOW" -le "$WINDOW" ] ||
  fail "pins_window: an integer from 1 to the window"

re='^(L|Lroot)/[A-Za-z0-9][A-Za-z0-9._+-]*$'
[[ $CANDIDATE =~ $re ]] ||
  fail "candidate: one name such as Lroot/k64a2l5m"
case "$CANDIDATE" in
  Lroot/*) root=true ;;
  *) root=false ;;
esac
candidate_slug="${CANDIDATE//\//_}"

case "$RETENTION_MODE" in
  sync | background) ;;
  *) fail "retention_mode: not sync or background" ;;
esac
if [ "$RETENTION_MODE" = background ]; then
  [ "$root" = true ] ||
    fail "candidate: a background build is of a root-store candidate (Lroot/...)"
fi

case "$RETAIN_FLUSH_DELS" in
  default | 32768 | 24576 | 16384) ;;
  *) fail "retain_flush_dels: not one of default, 32768, 24576, 16384" ;;
esac
if [ "$RETAIN_FLUSH_DELS" != default ]; then
  [ "$RETENTION_MODE" = background ] ||
    fail "retain_flush_dels: a threshold needs retention_mode background"
  [ "$root" = true ] ||
    fail "retain_flush_dels: a threshold is for a root-store candidate (Lroot/...)"
fi

case "$SYNC" in
  true | false) ;;
  *) fail "sync: not true or false" ;;
esac

metrics_every="${METRICS_EVERY//[[:space:]]/}"
if [ -n "$metrics_every" ]; then
  re='^([0-9]+(\.[0-9]+)?(ns|us|ms|s|m|h))+$'
  [[ $metrics_every =~ $re ]] ||
    fail "metrics_every: empty or a Go duration such as 10s"
fi

go_mem_limit="${GO_MEM_LIMIT//[[:space:]]/}"
if [ -n "$go_mem_limit" ]; then
  re='^[1-9][0-9]{0,5}(MiB|GiB)$'
  [[ $go_mem_limit =~ $re ]] ||
    fail "go_mem_limit: empty or a whole number of MiB or GiB such as 11GiB"
fi

re='^(0|[1-9][0-9]{0,4})$'
[[ $PACE =~ $re ]] && [ "$PACE" -le 20000 ] ||
  fail "pace: records per second, an integer from 0 to 20000"
if [ "$PACE" != 0 ]; then
  [ "$RETENTION_MODE" = background ] ||
    fail "pace: a pace needs retention_mode background"
fi

re='^([2-3])$'
[[ $REPS =~ $re ]] ||
  fail "reps: 2 or 3 (a control repeated at least twice is the noise floor)"

re='^([1-9]|1[0-9]|20)$'
[[ $MAX_PARALLEL =~ $re ]] ||
  fail "max_parallel: an integer from 1 to 20"

# The runners: a comma list of the three labels, each at most once. Every runner gets
# every configuration, so that a variant is read against a control on its own label.
case "$MACOS_LOW_RSS" in
  true | false) ;;
  *) fail "macos_low_rss: not true or false" ;;
esac
runners_in="${RUNNERS//[[:space:]]/}"
re='^[^,]+(,[^,]+)*$'
[[ $runners_in =~ $re ]] ||
  fail "runners: a comma list of ubuntu-24.04-arm, ubuntu-24.04, macos-15"
IFS=, read -ra oses <<<"$runners_in"
seen=" "
for os in "${oses[@]}"; do
  case "$os" in
    ubuntu-24.04-arm | ubuntu-24.04) ;;
    macos-15)
      # The macOS runner has 7 GB of memory.
      [ "$MACOS_LOW_RSS" = true ] ||
        fail "runners: macos-15 has 7 GB of memory; confirm that the configurations stay under about 6 GB with macos_low_rss"
      ;;
    *) fail "runners: every one is ubuntu-24.04-arm, ubuntu-24.04 or macos-15" ;;
  esac
  case "$seen" in
    *" $os "*) fail "runners: a runner is named twice" ;;
  esac
  seen="$seen$os "
done
runners="$runners_in"

# The configurations: a JSON list of 2 to 4 objects {name, build_flags, plan_flags},
# the first the control. The flags are words that each match one pattern below.
command -v jq >/dev/null 2>&1 || fail "jq is not installed"
[ "${#CONFIGS}" -le 4000 ] || fail "configs: longer than 4000 characters"
printf '%s' "$CONFIGS" | jq -e '
  type == "array" and length >= 2 and length <= 4
  and all(.[]; type == "object" and (keys | sort) == ["build_flags", "name", "plan_flags"]
               and all(.[]; type == "string"))' >/dev/null 2>&1 ||
  fail 'configs: a JSON list of 2 to 4 objects {"name", "build_flags", "plan_flags"}, all strings, the first the control'
ncfg="$(printf '%s' "$CONFIGS" | jq 'length')"

cfg_field() { # index field
  printf '%s' "$CONFIGS" | jq -r --argjson i "$1" --arg f "$2" '.[$i][$f]'
}

# One build flag word. Values are numbers, in the bounds the program itself checks;
# a flag the built program does not have fails at once ("flag provided but not
# defined"), which is safe.
check_build_word() {
  local w=$1 v
  case "$w" in
    -profile-windows | -disable-delete-only) ;;
    # The three spellings the program takes and no other, so that nothing but one of
    # these words is ever passed on.
    -compression=snappy | -compression=zstd | -compression=balanced) ;;
    -lbase-target-mb=*)
      v="${w#*=}"
      re='^[1-9][0-9]{0,3}$'
      [[ $v =~ $re ]] && [ "$v" -le 1024 ] ||
        fail "build_flags: -lbase-target-mb takes a size in MiB from 1 to 1024"
      ;;
    -target-sizes-mb=*)
      v="${w#*=}"
      re='^[1-9][0-9]{0,3}(,[1-9][0-9]{0,3}){0,6}$'
      [[ $v =~ $re ]] ||
        fail "build_flags: -target-sizes-mb takes 1 to 7 sizes in MiB, comma-separated"
      local s
      local -a sizes
      IFS=, read -ra sizes <<<"$v"
      for s in "${sizes[@]}"; do
        [ "$s" -le 4096 ] || fail "build_flags: every size of -target-sizes-mb is at most 4096"
      done
      ;;
    -read-ahead=*)
      v="${w#*=}"
      re='^[1-9][0-9]?$'
      [[ $v =~ $re ]] && [ "$v" -le 64 ] ||
        fail "build_flags: -read-ahead takes a count from 1 to 64"
      ;;
    *) fail "build_flags: every word is -profile-windows, -disable-delete-only, -lbase-target-mb=N, -target-sizes-mb=N,N,..., -read-ahead=N or -compression=snappy, -compression=zstd or -compression=balanced" ;;
  esac
}

check_plan_word() {
  local w=$1 v
  case "$w" in
    -cache-mb=*)
      v="${w#*=}"
      re='^[1-9][0-9]{0,4}$'
      [[ $v =~ $re ]] && [ "$v" -le 65536 ] ||
        fail "plan_flags: -cache-mb takes a size in MiB from 1 to 65536"
      ;;
    *) fail "plan_flags: every word is -cache-mb=N" ;;
  esac
}

# The words of a flag string, once each flag, joined by single spaces.
normalize() { # kind string
  local kind=$1 s=$2 w name out="" names=" "
  local -a words
  [ "${#s}" -le 300 ] || fail "$kind: longer than 300 characters"
  if [ -n "${s//[[:space:]]/}" ]; then
    read -ra words <<<"$s"
    [ "${#words[@]}" -le 6 ] || fail "$kind: at most 6 words"
    for w in "${words[@]}"; do
      if [ "$kind" = build_flags ]; then check_build_word "$w"; else check_plan_word "$w"; fi
      name="${w%%=*}"
      case "$names" in
        *" $name "*) fail "$kind: a flag is given twice" ;;
      esac
      names="$names$name "
      out="${out:+$out }$w"
    done
  fi
  printf '%s' "$out"
}

name_re='^[a-z0-9][a-z0-9_-]{0,23}$'
names=" "
bflags=()
pflags=()
cnames=()
cplan=()
plan_flags_list=()
plan_ids=()
i=0
while [ "$i" -lt "$ncfg" ]; do
  n="$(cfg_field "$i" name)"
  [[ $n =~ $name_re ]] ||
    fail "configs: every name matches ^[a-z0-9][a-z0-9_-]{0,23}\$"
  case "$names" in
    *" $n "*) fail "configs: two configurations have one name" ;;
  esac
  names="$names$n "
  b="$(normalize build_flags "$(cfg_field "$i" build_flags)")"
  p="$(normalize plan_flags "$(cfg_field "$i" plan_flags)")"

  # The program refuses these too; here a mistake fails before the plans are made.
  case " $b " in
    *" -profile-windows "*)
      [ "$RETENTION_MODE" = background ] && [ "$root" = true ] ||
        fail "build_flags: -profile-windows is for a background build of a root-store candidate"
      ;;
  esac
  case "$b" in
    *-read-ahead=*)
      [ "$root" = true ] || fail "build_flags: -read-ahead is for a root-store candidate (Lroot/...)"
      ;;
  esac
  case "$b" in
    *-lbase-target-mb=*)
      case "$b" in
        *-target-sizes-mb=*) fail "build_flags: -lbase-target-mb and -target-sizes-mb: give one" ;;
      esac
      ;;
  esac

  # A variant that is the control again would be a mislabelled A/A.
  if [ "$i" -gt 0 ] && [ "$b" = "${bflags[0]}" ] && [ "$p" = "${pflags[0]}" ]; then
    fail "configs: a variant has the control's flags; the repetitions of the control are the A/A"
  fi

  # One plan per distinct plan_flags, named by the order they first appear in.
  id=""
  k=0
  while [ "$k" -lt "${#plan_flags_list[@]}" ]; do
    if [ "${plan_flags_list[$k]}" = "$p" ]; then
      id="${plan_ids[$k]}"
    fi
    k=$((k + 1))
  done
  if [ -z "$id" ]; then
    id="p$((${#plan_ids[@]} + 1))"
    plan_flags_list+=("$p")
    plan_ids+=("$id")
  fi
  cnames+=("$n")
  bflags+=("$b")
  pflags+=("$p")
  cplan+=("$id")
  i=$((i + 1))
done

# At most 3 runners x 4 configurations x 3 repetitions: 36 jobs.
jobs=$((${#oses[@]} * ncfg * REPS))

# The matrix, compact and on one line (fromJSON reads it from a job output); rep is
# the outer loop so that, with max_parallel, the control and the variants of one
# repetition run together. Every word was checked above, so none needs escaping.
tsv="$RUNNER_TEMP/diag-matrix.tsv"
: >"$tsv"
matrix='{"include":['
sep=""
rep=1
while [ "$rep" -le "$REPS" ]; do
  for os in "${oses[@]}"; do
    i=0
    while [ "$i" -lt "$ncfg" ]; do
      role=B
      [ "$i" -gt 0 ] || role=A
      printf '%s\t%s\t%s\t%s\t%s\t%s\n' "$os" "${cnames[$i]}" "$role" "${bflags[$i]:-none}" "${cplan[$i]}" "$rep" >>"$tsv"
      matrix+="$sep{\"os\":\"$os\",\"name\":\"${cnames[$i]}\",\"role\":\"$role\",\"build_flags\":\"${bflags[$i]}\",\"plan\":\"${cplan[$i]}\",\"rep\":$rep}"
      sep=","
      i=$((i + 1))
    done
  done
  rep=$((rep + 1))
done
matrix+=']}'

plans='['
sep=""
k=0
while [ "$k" -lt "${#plan_ids[@]}" ]; do
  plans+="$sep{\"id\":\"${plan_ids[$k]}\",\"flags\":\"${plan_flags_list[$k]}\"}"
  sep=","
  k=$((k + 1))
done
plans+=']'

{
  echo "matrix=$matrix"
  echo "plans=$plans"
  echo "patched=$patched"
  echo "label=$LABEL"
  echo "base_sha=$BASE_SHA"
  echo "tree=$TREE"
  echo "patch_sha256=$PATCH_SHA256"
  echo "family=$FAMILY"
  echo "family_flags=$family_flags"
  echo "window=$WINDOW"
  echo "pins_window=$PINS_WINDOW"
  echo "candidate=$CANDIDATE"
  echo "candidate_slug=$candidate_slug"
  echo "retention_mode=$RETENTION_MODE"
  echo "retain_flush_dels=$RETAIN_FLUSH_DELS"
  echo "sync=$SYNC"
  echo "metrics_every=$metrics_every"
  echo "go_mem_limit=$go_mem_limit"
  echo "pace=$PACE"
  echo "max_parallel=$MAX_PARALLEL"
  echo "reps=$REPS"
  echo "runners=$runners"
} >>"$GITHUB_OUTPUT"

{
  echo "### Diagnostic dispatch: $jobs build jobs"
  echo
  echo "**Diagnostic only.** Nothing from this run is a judged arm, goes into a pool, or is compared with a figure from another machine or another runner label. The builds run with GOMAXPROCS=10, set explicitly, which bench.yml's do not: their figures are not comparable with this run's."
  echo
  echo "- ref: \`$REF\`; base commit: \`$BASE_SHA\`"
  if [ "$patched" = true ]; then
    echo "- patch: sha256 \`$PATCH_SHA256\`, expected tree \`$TREE\`"
  else
    echo "- patch: none"
  fi
  echo "- candidate: \`$CANDIDATE\`; window: \`$WINDOW\` days; pins chosen from the \`$PINS_WINDOW\`-day window; family: \`$FAMILY\`"
  echo "- retention mode: \`$RETENTION_MODE\`; pace: \`$PACE\` records/s; retention flush threshold: \`$RETAIN_FLUSH_DELS\`; sync: \`$SYNC\`; metrics every: \`${metrics_every:-off}\`; Go memory limit: \`${go_mem_limit:-none}\`"
  echo "- \`GOMAXPROCS\` 10 on every build; at most \`$MAX_PARALLEL\` build jobs at once"
  echo
  echo "| runner | configuration | role | repetition | build flags | plan |"
  echo "|---|---|---|---|---|---|"
  while IFS=$'\t' read -r os n role b plan rep; do
    echo "| $os | \`$n\` | $role | $rep | \`$b\` | $plan |"
  done <"$tsv"
} >>"$GITHUB_STEP_SUMMARY"
