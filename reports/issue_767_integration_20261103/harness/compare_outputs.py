"""Compare target variable data/masks and fresh processing/checker results."""

import argparse
import json
from pathlib import Path

import netCDF4
import numpy as np


def compare(a, b):
    with netCDF4.Dataset(a) as da, netCDF4.Dataset(b) as db:
        va, vb = da[da.variable_id], db[db.variable_id]
        result = {
            "baseline_shape": list(va.shape),
            "new_shape": list(vb.shape),
            "units_unchanged": getattr(va, "units", None) == getattr(vb, "units", None),
        }
        if va.shape != vb.shape:
            return dict(
                result,
                values_identical=False,
                masks_identical=False,
                error="shape differs",
            )
        masks = True
        values = True
        maxerr = 0.0
        changed = 0
        valid = 0
        for i in range(va.shape[0]):
            x, y = np.ma.asarray(va[i]), np.ma.asarray(vb[i])
            mx, my = np.ma.getmaskarray(x), np.ma.getmaskarray(y)
            masks &= bool(np.array_equal(mx, my))
            keep = ~(mx | my)
            xd, yd = x.data[keep], y.data[keep]
            same = (xd == yd) | (np.isnan(xd) & np.isnan(yd))
            changed += int((~same).sum())
            valid += int(keep.sum())
            values &= bool(same.all())
            if xd.size:
                maxerr = max(
                    maxerr,
                    float(
                        np.nanmax(np.abs(xd.astype("float64") - yd.astype("float64")))
                    ),
                )
        return dict(
            result,
            values_identical=values,
            masks_identical=masks,
            max_absolute_error=maxerr,
            changed_values=changed,
            compared_valid_values=valid,
        )


def rows(root):
    return {
        r["product"]: r
        for p in (root / "results").glob("*.json")
        if (r := json.loads(p.read_text())).get("product")
    }


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("baseline", type=Path)
    ap.add_argument("new", type=Path)
    ap.add_argument("--output", type=Path, required=True)
    args = ap.parse_args()
    old, new = rows(args.baseline), rows(args.new)
    res = []
    for product, r in new.items():
        item = {
            "product": product,
            "legacy": r["legacy"],
            "processing": r["processing"],
            "error": r.get("error"),
            "files": [],
        }
        for f in r.get("files", []):
            c = f.get("wcrp", {})
            counts = {
                str(n): sum(x["weight"] == n for x in c.get("failed_checks", []))
                for n in (3, 2, 1)
            }
            info = {
                "path": f["path"],
                "structural": f.get("structural"),
                "range_qc": f.get("range_qc", {}).get("passed"),
                "checker_returncode": c.get("returncode"),
                "checker_errors": c.get("errors", {}),
                "finding_counts": counts,
                "failed_checks": c.get("failed_checks", []),
            }
            prev = old.get(product, {}).get("files", [])
            if len(prev) == 1:
                info["numerical_comparison"] = compare(
                    args.baseline / prev[0]["path"], args.new / f["path"]
                )
            item["files"].append(info)
        res.append(item)
    args.output.write_text(json.dumps(res, indent=2) + "\n")
    print(
        json.dumps(
            {
                "variables": len(res),
                "completed": sum(r["processing"] == "passed" for r in res),
                "failures": [
                    (r["legacy"], r["error"])
                    for r in res
                    if r["processing"] != "passed"
                ],
                "high_failure_outputs": sum(
                    any(f["finding_counts"]["3"] for f in r["files"]) for r in res
                ),
                "medium_outputs": sum(
                    any(f["finding_counts"]["2"] for f in r["files"]) for r in res
                ),
                "optional_outputs": sum(
                    any(f["finding_counts"]["1"] for f in r["files"]) for r in res
                ),
                "numerical_changes": [
                    r["legacy"]
                    for r in res
                    if any(
                        not f.get("numerical_comparison", {}).get(
                            "values_identical", True
                        )
                        or not f.get("numerical_comparison", {}).get(
                            "masks_identical", True
                        )
                        for f in r["files"]
                    )
                ],
            },
            indent=2,
        )
    )


if __name__ == "__main__":
    main()
