"""New checker reports for freshly generated outputs, optionally proposed plugin."""

import argparse
import concurrent.futures
import hashlib
import json
import os
import subprocess
from pathlib import Path

ap = argparse.ArgumentParser()
ap.add_argument("run", type=Path)
ap.add_argument("destination", type=Path)
ap.add_argument("--plugin", type=Path)
a = ap.parse_args()
a.destination.mkdir(parents=True, exist_ok=False)
for name in ("raw", "logs", "results"):
    (a.destination / name).mkdir()


def digest(p):
    with p.open("rb") as f:
        return hashlib.file_digest(f, "sha256").hexdigest()


env = os.environ.copy()
for key in ("PYTHONPATH", "GITHUB_TOKEN", "GH_TOKEN"):
    env.pop(key, None)
env.update(
    ESGVOC_HOME="/home/romain/PROJECTS/ACCESS-MOPPy/reports/issue_767_rerun/checker_2_4/esgvoc",
    ESGVOC_OFFLINE="true",
    PYTHONNOUSERSITE="1",
    XDG_CACHE_HOME="/tmp/issue767-checker-cache",
    HDF5_PLUGIN_PATH="/tmp/issue767-checker-2.4-venv/lib/python3.13/site-packages/hdf5plugin/plugins",
    OMP_NUM_THREADS="1",
    OPENBLAS_NUM_THREADS="1",
)
if a.plugin:
    env["PYTHONPATH"] = str(a.plugin.resolve())
source = {
    "run": str(a.run.resolve()),
    "plugin": str(a.plugin.resolve()) if a.plugin else None,
    "plugin_commit": subprocess.check_output(
        ["git", "-C", str(a.plugin), "rev-parse", "HEAD"], text=True
    ).strip()
    if a.plugin
    else None,
    "checker": "2.4.0b2",
    "esgvoc": "7.2.0",
    "universe": "3.4.3",
    "cmip7": "2.5.4",
}
(a.destination / "provenance.json").write_text(json.dumps(source, indent=2) + "\n")
rows = [json.loads(p.read_text()) for p in (a.run / "results").glob("*.json")]


def check(row):
    files = row.get("files", [])
    if len(files) != 1:
        return {
            "product": row["product"],
            "legacy": row["legacy"],
            "status": "processing_failure",
            "error": row.get("error"),
        }
    path = a.run / files[0]["path"]
    before = digest(path)
    raw = a.destination / "raw" / f"{row['product']}.json"
    cmd = [
        "/tmp/issue767-checker-2.4-venv/bin/compliance-checker",
        "-t",
        "wcrp_cmip7:1.0",
        "-c",
        "strict",
        "-f",
        "json",
        "-o",
        str(raw),
        str(path),
    ]
    call = subprocess.run(cmd, env=env, capture_output=True, text=True, timeout=300)
    (a.destination / "logs" / f"{row['product']}.log").write_text(
        call.stdout + call.stderr
    )
    result = {
        "product": row["product"],
        "legacy": row["legacy"],
        "path": str(path),
        "sha256_before": before,
        "sha256_after": digest(path),
        "returncode": call.returncode,
        "command": cmd,
    }
    assert result["sha256_before"] == result["sha256_after"]
    if raw.exists():
        payload = json.loads(raw.read_text())["wcrp_cmip7:1.0"]
        result["errors"] = payload.get("errors", {})
        result["failed_checks"] = [
            dict(c, priority=level)
            for level in ("high_priorities", "medium_priorities", "low_priorities")
            for c in payload.get(level, [])
            if c["value"][0] < c["value"][1]
        ]
        result["counts"] = {
            str(w): sum(c["weight"] == w for c in result["failed_checks"])
            for w in (3, 2, 1)
        }
        result["status"] = (
            "error"
            if call.returncode not in (0, 1) or result["errors"]
            else "fail"
            if result["counts"]["3"]
            else "findings"
            if result["failed_checks"]
            else "pass"
        )
    else:
        result["status"] = "error"
    (a.destination / "results" / f"{row['product']}.json").write_text(
        json.dumps(result, indent=2) + "\n"
    )
    return result


results = []
with concurrent.futures.ThreadPoolExecutor(max_workers=2) as pool:
    for i, f in enumerate(
        concurrent.futures.as_completed([pool.submit(check, r) for r in rows]), 1
    ):
        r = f.result()
        results.append(r)
        print(
            f"{i}/{len(rows)} {r['legacy']}: {r['status']} {r.get('counts')}",
            flush=True,
        )
(a.destination / "results.json").write_text(json.dumps(results, indent=2) + "\n")
summary = {
    "outputs": len(results),
    "strict_pass": sum(r["status"] == "pass" for r in results),
    "checker_errors": sum(r["status"] == "error" for r in results),
    "high_failure_outputs": sum(bool(r.get("counts", {}).get("3")) for r in results),
    "medium_outputs": sum(bool(r.get("counts", {}).get("2")) for r in results),
    "optional_outputs": sum(bool(r.get("counts", {}).get("1")) for r in results),
}
(a.destination / "summary.json").write_text(json.dumps(summary, indent=2) + "\n")
print(json.dumps(summary))
