#!/usr/bin/env python3
"""Give every tracked file and directory a modification time derived from its content.

go test keys a cached result by the size and modification time of every file and
directory a test opened. A checkout writes every file at the time of the checkout,
so no result from an earlier run can match. This script sets, after checkout:

  - for a file, the time from its git blob id;
  - for a directory (and the checkout root), the time from its git tree id.

Equal content therefore gets an equal time on every runner, and a result is
reused; different content gets a different time, and the package reruns.

Two things are refused, because each would make a stale result look current:

  - A fixed date. One date for all files would let an edit that keeps a file's
    size reuse the result of the old content.
  - A future date. go test does not cache a result that read a file modified less
    than two seconds ago, and a time in the future counts as that. Every time here
    lies between 2000-01-01 and 2019-12-31.

Run from the checkout root: python3 .github/scripts/stable-mtimes.py
Standard library only. A missing path is skipped; any git error exits non-zero.
"""

import datetime
import os
import subprocess
import sys

# 2000-01-01T00:00:00Z, and 20 years of 365.25 days (631 152 000 seconds).
EPOCH = 946684800
SPAN = 631152000


def stamp(object_id: str) -> int:
    """The time for an object id: the first 8 hex digits, folded into the span."""
    return EPOCH + int(object_id[:8], 16) % SPAN


def git(*args: str) -> bytes:
    try:
        return subprocess.run(
            ["git", *args], check=True, stdout=subprocess.PIPE
        ).stdout
    except (subprocess.CalledProcessError, OSError) as err:
        sys.exit(f"stable-mtimes: git {' '.join(args)} failed: {err}")


def records(out: bytes):
    """Yield (metadata fields, path) for each NUL-terminated '<fields>\\t<path>' record."""
    for rec in out.split(b"\0"):
        if not rec:
            continue
        meta, _, path = rec.partition(b"\t")
        yield meta.decode().split(" "), os.fsdecode(path)


def set_time(path: str, t: int) -> bool:
    try:
        os.utime(path, (t, t), follow_symlinks=False)
    except FileNotFoundError:
        return False
    return True


def main() -> None:
    os.chdir(git("rev-parse", "--show-toplevel").decode().strip())

    times = []
    files = 0
    # Files first: setting a file's time does not touch its directory's, but this
    # keeps the order the rule states.
    for (mode, blob, _stage), path in records(git("ls-files", "-s", "-z")):
        if mode == "160000":  # a gitlink: the directory belongs to another repository
            continue
        t = stamp(blob)
        if set_time(path, t):
            files += 1
            times.append(t)

    dirs = 0
    entries = [
        (tree, path)
        for (_mode, _kind, tree), path in records(git("ls-tree", "-r", "-d", "-z", "HEAD"))
    ]
    entries.append((git("rev-parse", "HEAD^{tree}").decode().strip(), "."))
    for tree, path in entries:
        t = stamp(tree)
        if set_time(path, t):
            dirs += 1
            times.append(t)

    if not times:
        sys.exit("stable-mtimes: no tracked paths found")

    def day(t: int) -> str:
        return datetime.datetime.fromtimestamp(t, datetime.timezone.utc).strftime("%Y-%m-%d")

    print(
        f"stable-mtimes: set {files} files and {dirs} directories, "
        f"times from {day(min(times))} to {day(max(times))}"
    )


if __name__ == "__main__":
    main()
