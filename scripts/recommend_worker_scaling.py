#!/usr/bin/env python3
"""
Given a moppy batch report JSON, work out whether each job's Dask worker
count was limited by cpus_per_node or by mem, and recommend new
cpus_per_node/mem values to add more workers.

moppy sizes workers as:
    n_workers = max(1, min(requested_cpus, effective_mem_gb // per_worker_floor_gb))
    threads_per_worker = 1  (always -- HDF5/netCDF4 reads serialize on a lock,
                             so parallelism only comes from more worker processes)

So a job is:
  - "cpu"-bound if requested_cpus <= n_workers  -> more mem won't add workers,
    only more cpus_per_node will.
  - "mem"-bound if requested_cpus  > n_workers  -> more cpus_per_node won't add
    workers, only more mem will.

Sizing is based on the *actual measured* peak_rss_mb per worker (not on
memory_limit_per_worker, which just reflects whatever the tier heuristic
happened to hand out and is often far more generous than needed). Each
worker is sized as peak_rss_gb * --safety-factor, and mem/cpus are set so
that --target-workers (or n_workers + --add-workers) workers fit at that
footprint.

Usage:
    python scripts/recommend_worker_scaling.py report.json
    python scripts/recommend_worker_scaling.py report.json --add-workers 3
    python scripts/recommend_worker_scaling.py report.json --target-workers 8
    python scripts/recommend_worker_scaling.py report.json --safety-factor 2.0
    python scripts/recommend_worker_scaling.py report.json --pattern ocean
    python scripts/recommend_worker_scaling.py report.json --sort-by peak_rss_gb
"""

import argparse
import json
import re
import sys


def parse_pbs_mem_bytes(mem_str):
    """'68719476736b' -> 68719476736"""
    if not mem_str:
        return None
    m = re.match(r"(\d+)b", mem_str.strip())
    return int(m.group(1)) if m else None


def parse_gb(value_str):
    """'16.00GB' -> 16.0"""
    m = re.match(r"([\d.]+)", value_str.strip())
    return float(m.group(1)) if m else None


def collect_rows(report, pattern=None):
    rows = []
    for task in report.get("tasks", []):
        wm = task.get("worker_memory")
        if not wm:
            continue
        variable = task.get("variable", "")
        if pattern and pattern not in variable:
            continue

        n_workers = wm.get("n_workers")
        mem_per_worker_gb = parse_gb(wm.get("memory_limit_per_worker", ""))
        peak_rss_mb_values = list(wm.get("peak_rss_mb", {}).values())
        peak_rss_gb = max(peak_rss_mb_values) / 1024.0 if peak_rss_mb_values else None

        req = (task.get("pbs") or {}).get("resources_requested", {})
        req_cpus = req.get("ncpus")
        req_mem_gb = None
        mem_bytes = parse_pbs_mem_bytes(req.get("mem"))
        if mem_bytes:
            req_mem_gb = mem_bytes / (1024**3)

        if n_workers is None or mem_per_worker_gb is None or req_cpus is None:
            continue

        binding = "cpu" if req_cpus <= n_workers else "mem"

        rows.append(
            dict(
                variable=variable,
                req_cpus=req_cpus,
                req_mem_gb=req_mem_gb,
                n_workers=n_workers,
                mem_per_worker_gb=mem_per_worker_gb,
                peak_rss_gb=peak_rss_gb,
                headroom_x=(mem_per_worker_gb / peak_rss_gb) if peak_rss_gb else None,
                binding=binding,
            )
        )
    return rows


def recommend(row, add_workers, target_workers_override, safety_factor):
    if row["peak_rss_gb"] is None:
        return None, None, None

    target_workers = target_workers_override or (row["n_workers"] + add_workers)
    # Size each worker off *measured* peak RSS, not off memory_limit_per_worker
    # (which just reflects whatever the tier heuristic granted, and is often
    # much bigger than what the job actually used).
    per_worker_gb = row["peak_rss_gb"] * safety_factor
    target_mem_gb = target_workers * per_worker_gb
    target_cpus = max(row["req_cpus"], target_workers)
    return target_cpus, round(target_mem_gb, 1), round(per_worker_gb, 2)


def main():
    ap = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    ap.add_argument("report", help="Path to a moppy batch report JSON")
    ap.add_argument(
        "--add-workers",
        type=int,
        default=2,
        help="Extra workers to target per job, on top of current "
        "n_workers (default: 2). Ignored if --target-workers is set.",
    )
    ap.add_argument(
        "--target-workers",
        type=int,
        default=None,
        help="Absolute worker count to target per job (overrides --add-workers)",
    )
    ap.add_argument(
        "--safety-factor",
        type=float,
        default=1.5,
        help="Multiply measured peak_rss_gb by this to get the "
        "per-worker memory target (default: 1.5x)",
    )
    ap.add_argument(
        "--pattern",
        default=None,
        help="Only show variables whose name contains this substring",
    )
    ap.add_argument(
        "--sort-by",
        default="variable",
        choices=["variable", "peak_rss_gb", "headroom_x", "n_workers"],
        help="Sort rows by this field (default: variable)",
    )
    ap.add_argument(
        "--only-binding",
        choices=["cpu", "mem"],
        default=None,
        help="Only show jobs currently bound by this resource",
    )
    args = ap.parse_args()

    with open(args.report) as f:
        report = json.load(f)

    rows = collect_rows(report, pattern=args.pattern)
    if not rows:
        print(
            "No tasks with worker_memory + resources_requested found.", file=sys.stderr
        )
        return 1

    if args.only_binding:
        rows = [r for r in rows if r["binding"] == args.only_binding]

    rows.sort(key=lambda r: (r[args.sort_by] is None, r[args.sort_by]))

    header = [
        "variable",
        "cpus",
        "mem_GB",
        "n_workers",
        "mem/worker_GB",
        "peak_rss_GB",
        "binding",
        "->cpus",
        "->mem_GB",
        "->GB/worker",
    ]
    widths = [46, 5, 7, 9, 13, 11, 7, 6, 8, 11]
    print("  ".join(h.ljust(w) for h, w in zip(header, widths)))

    for r in rows:
        target_cpus, target_mem_gb, per_worker_gb = recommend(
            r, args.add_workers, args.target_workers, args.safety_factor
        )
        values = [
            r["variable"],
            r["req_cpus"],
            f'{r["req_mem_gb"]:.1f}' if r["req_mem_gb"] is not None else "-",
            r["n_workers"],
            f'{r["mem_per_worker_gb"]:.2f}',
            f'{r["peak_rss_gb"]:.2f}' if r["peak_rss_gb"] is not None else "-",
            r["binding"],
            target_cpus if target_cpus is not None else "-",
            target_mem_gb if target_mem_gb is not None else "-",
            per_worker_gb if per_worker_gb is not None else "-",
        ]
        print("  ".join(str(v).ljust(w) for v, w in zip(values, widths)))

    n_cpu_bound = sum(1 for r in rows if r["binding"] == "cpu")
    n_mem_bound = sum(1 for r in rows if r["binding"] == "mem")
    print(
        f"\n{n_cpu_bound} job(s) cpu-bound (raising cpus_per_node alone adds workers)"
        f" / {n_mem_bound} job(s) mem-bound (need more mem to add workers).",
        file=sys.stderr,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
