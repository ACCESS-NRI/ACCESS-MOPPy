"""Fresh isolated per-product integration run; never reuse old baseline reports."""

import concurrent.futures
import json
import os
import subprocess
from pathlib import Path

ROOT = Path(__file__).parent
HERE = Path(os.environ["ISSUE767_RUN_ROOT"])
HERE.mkdir(parents=True, exist_ok=True)
BASE = Path("/home/romain/PROJECTS/ACCESS-MOPPy/reports/issue_767_rerun")
ROWS = json.loads((BASE / "selection.json").read_text())["variables"]
for folder in ("logs", "results", "outputs"):
    (HERE / folder).mkdir(exist_ok=True)


def run(row):
    key = row["product"]
    result = HERE / "results" / f"{key}.json"
    if result.exists():
        raise RuntimeError(f"Refusing cached result {result}")
    with (HERE / "logs" / f"{key}.log").open("w") as log:
        call = subprocess.run(
            [
                "/home/romain/PROJECTS/ACCESS-MOPPy/.pixi/envs/dev/bin/python",
                str(ROOT / "run_fresh.py"),
                key,
            ],
            env=os.environ,
            stdout=log,
            stderr=subprocess.STDOUT,
            timeout=900,
        )
    if not result.exists():
        result.write_text(
            json.dumps(dict(row, processing="crashed", returncode=call.returncode))
            + "\n"
        )
    return json.loads(result.read_text())


with concurrent.futures.ThreadPoolExecutor(max_workers=2) as pool:
    for i, future in enumerate(
        concurrent.futures.as_completed([pool.submit(run, row) for row in ROWS]), 1
    ):
        row = future.result()
        counts = {
            w: sum(
                c["weight"] == w
                for f in row.get("files", [])
                for c in f.get("wcrp", {}).get("failed_checks", [])
            )
            for w in (3, 2, 1)
        }
        print(
            f"{i}/{len(ROWS)} {row['legacy']}: processing={row['processing']}; high/medium/low={counts[3]}/{counts[2]}/{counts[1]}",
            flush=True,
        )
rows = [
    json.loads((HERE / "results" / f"{r['product']}.json").read_text()) for r in ROWS
]
(HERE / "results.json").write_text(json.dumps(rows, indent=2) + "\n")
