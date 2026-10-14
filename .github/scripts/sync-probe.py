#!/usr/bin/env python3
"""Time small durable writes on the disk a build will write to.

Writes 4 KiB and syncs it, 1000 times, to a file in the directory given, and prints
the 50th and 99th percentile and the largest time. A sync is what a store commit
costs, and it differs between runner images, between machines of one image, and
between Linux and macOS, so a diagnostic run records it beside its CPU model.

On macOS a sync is F_FULLFSYNC, as Go's File.Sync does there; elsewhere fsync.

Usage: sync-probe.py <directory>
Standard library only.
"""

import os
import sys
import time

COUNT = 1000
BLOCK = b"\x5a" * 4096


def durable_sync(fd: int) -> None:
    if sys.platform == "darwin":
        import fcntl

        fcntl.fcntl(fd, fcntl.F_FULLFSYNC)
    else:
        os.fsync(fd)


def micros(ns: int) -> int:
    return ns // 1000


def main() -> None:
    if len(sys.argv) != 2:
        sys.exit("usage: sync-probe.py <directory>")
    path = os.path.join(sys.argv[1], "sync-probe.tmp")
    times = []
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
    try:
        for _ in range(COUNT):
            start = time.perf_counter_ns()
            os.write(fd, BLOCK)
            durable_sync(fd)
            times.append(time.perf_counter_ns() - start)
    finally:
        os.close(fd)
        os.unlink(path)
    times.sort()
    print(
        f"sync_probe n={COUNT} block=4096 "
        f"p50_us={micros(times[COUNT // 2])} p99_us={micros(times[COUNT * 99 // 100])} "
        f"max_us={micros(times[-1])}"
    )


if __name__ == "__main__":
    main()
