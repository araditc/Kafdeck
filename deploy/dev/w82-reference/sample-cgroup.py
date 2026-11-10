import csv
import sys
import time
from pathlib import Path


def read_counter(root, name):
    return int((root / name).read_text())


def main():
    root = Path("/sys/fs/cgroup")
    columns = (
        "unixSeconds", "cpuUsageUsec", "cpuThrottledUsec",
        "memoryCurrentBytes", "memoryPeakBytes", "hostBusyTicks", "hostTotalTicks",
    )
    interval = time.monotonic()
    with Path(sys.argv[1]).open("w", newline="", buffering=1) as output:
        writer = csv.writer(output)
        writer.writerow(columns)
        while True:
            cpu = dict(line.split() for line in (root / "cpu.stat").read_text().splitlines())
            host = list(map(int, Path("/proc/stat").read_text().splitlines()[0].split()[1:9]))
            total = sum(host)
            writer.writerow((
                time.time(), int(cpu["usage_usec"]), int(cpu["throttled_usec"]),
                read_counter(root, "memory.current"), read_counter(root, "memory.peak"),
                total - host[3] - host[4], total,
            ))
            interval += 1
            time.sleep(max(0, interval - time.monotonic()))


if __name__ == "__main__":
    main()
