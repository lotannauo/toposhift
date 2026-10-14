#!/usr/bin/env bash
# Gets a patch into a checkout for the verify and diag workflows, and proves that the
# checkout then holds exactly the tree the dispatch named. Two steps:
#
#   apply-patch.sh fetch   Turn the dispatch's inline patch or its gist into a plain
#                          patch file, and check its sha256.
#                          Reads PATCH_B64 or GIST_ID, PATCH_SHA256 and OUT, and
#                          GITHUB_TOKEN if set (to read a gist without the limit of
#                          an unauthenticated caller).
#   apply-patch.sh apply   In a checkout of BASE_SHA, apply the patch, make one local
#                          commit, and require its tree to be TREE.
#                          Reads BASE_SHA, PATCH_FILE, PATCH_SHA256 and TREE, and
#                          REQUIRE_ON_MAIN=true to also require BASE_SHA to be on
#                          origin/main (the checkout then needs that history).
#
# The patch is the output of `git diff --binary --full-index <base> <shipped-commit>`,
# which covers added, deleted, renamed and binary files and modes. It travels as
# `gzip -9 | base64 -w0` inline, or as that text in a file named patch.b64 of a gist.
# Its sha256 is the one of the plain diff, not of the transport form.
#
# Every input arrives in the environment, never in the command line, and no message
# repeats an input: its text is not trusted, and a line that starts with "::" is a
# command to the runner. Standard tools only; it runs on Linux and macOS, and in bash
# 3.2. The commit stays inside the checkout: nothing here pushes, and the workflows
# that call it give the job no credential to push with.
set -euo pipefail

fail() {
  echo "::error::$1" >&2
  exit 1
}

# The largest plain patch taken, in bytes.
max_patch=20000000

sha256_of() {
  if command -v sha256sum >/dev/null 2>&1; then
    sha256sum "$1" | cut -d' ' -f1
  else
    shasum -a 256 "$1" | cut -d' ' -f1
  fi
}

hex40='^[0-9a-f]{40}$'
hex64='^[0-9a-f]{64}$'

# Reads a gzip + base64 stream on stdin, writes the plain patch to $OUT. A stream
# that decodes to more than max_patch bytes is cut there and refused by its size; one
# that is damaged is refused by its sha256 afterwards.
decode() {
  (
    set +o pipefail
    base64 -d | gzip -dc 2>/dev/null | head -c "$((max_patch + 1))" >"$OUT"
  )
  size="$(wc -c <"$OUT" | tr -d ' ')"
  [ "$size" -gt 0 ] || fail "the patch is empty or is not gzip and base64"
  [ "$size" -le "$max_patch" ] || fail "the patch is larger than $max_patch bytes"
}

fetch() {
  : "${OUT:?}" "${PATCH_SHA256:?}"
  [[ $PATCH_SHA256 =~ $hex64 ]] || fail "patch_sha256: 64 lowercase hexadecimal digits"
  if [ -n "${PATCH_B64:-}" ] && [ -n "${GIST_ID:-}" ]; then
    fail "give the patch inline or as a gist, not both"
  fi
  if [ -n "${PATCH_B64:-}" ]; then
    printf '%s' "$PATCH_B64" | decode
  elif [ -n "${GIST_ID:-}" ]; then
    re='^[0-9a-f]{20,40}$'
    [[ $GIST_ID =~ $re ]] || fail "gist_id: 20 to 40 lowercase hexadecimal digits"
    # A secret gist is readable by its id. GITHUB_TOKEN goes to api.github.com and to
    # nowhere else: the raw address is taken from the gist's own record and must be on
    # the gist host.
    # The call to the API carries GITHUB_TOKEN when there is one (the unauthenticated
    # limit is 60 an hour per address, and runners share addresses), in a config read
    # from standard input so that it is not in a command line. The raw file is fetched
    # without it, from the gist host.
    meta="$(
      {
        echo 'header = "Accept: application/vnd.github+json"'
        [ -z "${GITHUB_TOKEN:-}" ] || echo "header = \"Authorization: Bearer ${GITHUB_TOKEN}\""
      } | curl --fail --silent --show-error --location --proto '=https' --tlsv1.2 \
        --retry 3 --max-time 60 -K - "https://api.github.com/gists/${GIST_ID}"
    )" || fail "the gist could not be read"
    url="$(printf '%s' "$meta" | jq -r '.files["patch.b64"].raw_url // empty')"
    case "$url" in
      https://gist.githubusercontent.com/*) ;;
      *) fail "the gist has no file patch.b64 on the gist host" ;;
    esac
    curl --fail --silent --show-error --location --proto '=https' --tlsv1.2 \
      --retry 3 --max-time 120 --max-filesize "$((max_patch * 2))" "$url" | decode
  else
    fail "give the patch inline or as a gist"
  fi
  [ "$(sha256_of "$OUT")" = "$PATCH_SHA256" ] || fail "the patch does not have the sha256 the dispatch names"
  echo "patch: $(wc -c <"$OUT" | tr -d ' ') bytes, sha256 $PATCH_SHA256"
}

apply() {
  : "${BASE_SHA:?}" "${PATCH_FILE:?}" "${PATCH_SHA256:?}" "${TREE:?}"
  [[ $BASE_SHA =~ $hex40 ]] || fail "base_sha: 40 lowercase hexadecimal digits"
  [[ $TREE =~ $hex40 ]] || fail "tree: 40 lowercase hexadecimal digits"
  [[ $PATCH_SHA256 =~ $hex64 ]] || fail "patch_sha256: 64 lowercase hexadecimal digits"

  [ "$(git rev-parse HEAD)" = "$BASE_SHA" ] || fail "the checkout is not the base commit"
  [ -z "$(git status --porcelain)" ] || fail "the checkout is not clean"
  if [ "${REQUIRE_ON_MAIN:-false}" = true ]; then
    git merge-base --is-ancestor "$BASE_SHA" origin/main 2>/dev/null ||
      fail "base_sha: the base commit is not on origin/main"
  fi
  [ "$(sha256_of "$PATCH_FILE")" = "$PATCH_SHA256" ] || fail "the patch does not have the sha256 the dispatch names"

  # --index puts the change in the index as well, so that the tree can be read before
  # anything is committed. git apply refuses paths outside the checkout and under .git.
  git apply --index --binary --whitespace=nowarn "$PATCH_FILE" || fail "the patch does not apply to the base commit"
  got="$(git write-tree)"
  if [ "$got" != "$TREE" ]; then
    fail "the patched tree is $got, not the tree the dispatch names"
  fi
  [ -z "$(git status --porcelain | grep -v '^[ADMRC]  ' || true)" ] ||
    fail "the checkout has changes that are not in the index after the patch"

  # One local commit, never pushed: gitleaks and check:no-results read the history and
  # the index, so the change has to be committed for them to see it. No hook runs, and
  # nothing is signed.
  GIT_AUTHOR_NAME=patch GIT_AUTHOR_EMAIL=patch@localhost \
    GIT_COMMITTER_NAME=patch GIT_COMMITTER_EMAIL=patch@localhost \
    git -c commit.gpgsign=false -c core.hooksPath=/dev/null \
    commit --quiet --no-verify --message "Apply the patch to ${BASE_SHA}" ||
    fail "the local commit failed"
  [ "$(git rev-parse 'HEAD^{tree}')" = "$TREE" ] || fail "the committed tree is not the tree the dispatch named"
  echo "applied: base ${BASE_SHA}, tree ${TREE}, local commit $(git rev-parse HEAD) (not pushed)"
}

case "${1:-}" in
  fetch) fetch ;;
  apply) apply ;;
  *) fail "usage: apply-patch.sh fetch|apply" ;;
esac
