#!/usr/bin/env bash
# Deletes every Actions cache entry saved under the verify branch's ref. The verify
# workflow never saves a cache, so an entry in that scope was not saved by it: code in
# a patched job can save one by tampering with a later action of its own job, and a
# later verify run would restore it. Run before the shards start and again after they
# end. Reads REPO (owner/name) and GH_TOKEN, which needs actions: write.
set -euo pipefail

: "${REPO:?}"
re='^[A-Za-z0-9._-]+/[A-Za-z0-9._-]+$'
[[ $REPO =~ $re ]] || { echo "::error::REPO is not owner/name" >&2; exit 1; }

ids="$(gh api --paginate "repos/${REPO}/actions/caches?ref=refs/heads/verify&per_page=100" --jq '.actions_caches[].id')"
n=0
for id in $ids; do
  re='^[0-9]+$'
  [[ $id =~ $re ]] || { echo "::error::a cache id is not a number" >&2; exit 1; }
  gh api -X DELETE "repos/${REPO}/actions/caches/${id}" >/dev/null
  n=$((n + 1))
done
echo "deleted $n cache entries of refs/heads/verify"
