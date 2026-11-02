"""Audit fresh reduced-grid outputs against a separate clean-main run.

Usage: python audit.py BASELINE_RUN NEW_RUN UNIVERSE_DB
Requires netCDF4 and numpy. Outputs evidence.json beside this script.
"""

import hashlib
import json
import sqlite3
import sys
from pathlib import Path

import netCDF4
import numpy as np

EXPECTED = {
    "SImon.siarean": ("g011", "nh"),
    "SImon.siareas": ("g012", "sh"),
    "SImon.siextentn": ("g011", "nh"),
    "SImon.siextents": ("g012", "sh"),
    "SImon.siareaacrossline": ("g014", "glb"),
    # This unchanged product records the unresolved native-row grid defect.
    "Omon.msftmrho": ("g116", "glb"),
}


def sha256(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def audit(baseline, current):
    records = []
    for result_path in sorted((current / "results").glob("*.json")):
        result = json.loads(result_path.read_text())
        if result["legacy"] not in EXPECTED:
            continue
        previous = json.loads((baseline / "results" / result_path.name).read_text())
        assert result["processing"] == previous["processing"] == "passed"
        assert len(result["files"]) == len(previous["files"]) == 1
        new_info, old_info = result["files"][0], previous["files"][0]
        new_path, old_path = current / new_info["path"], baseline / old_info["path"]
        label, region = EXPECTED[result["legacy"]]
        record = {
            "legacy": result["legacy"],
            "baseline_path": str(old_path),
            "new_path": str(new_path),
            "baseline_sha256": sha256(old_path),
            "new_sha256": sha256(new_path),
            "variables": {},
        }
        with netCDF4.Dataset(old_path) as old, netCDF4.Dataset(new_path) as new:
            assert set(new.dimensions) == set(old.dimensions)
            assert set(new.variables) == set(old.variables)
            assert new.region == old.region == region
            assert new.grid_label == label
            assert new_path.parent.parent.name == label
            assert f"_{region}_{label}_" in new_path.name
            assert region in new_path.parts
            for name in old.variables:
                a, b = old[name], new[name]
                assert a.dimensions == b.dimensions and a.dtype == b.dtype
                x, y = np.ma.asarray(a[:]), np.ma.asarray(b[:])
                assert np.array_equal(np.ma.getmaskarray(x), np.ma.getmaskarray(y))
                assert np.array_equal(
                    x.compressed(),
                    y.compressed(),
                    equal_nan=np.issubdtype(x.dtype, np.number),
                )
                record["variables"][name] = {
                    "dimensions": list(b.dimensions),
                    "shape": list(b.shape),
                    "dtype": str(b.dtype),
                    "values_and_masks_identical": True,
                }
            record.update(
                old_grid_label=old.grid_label,
                grid_label=label,
                region=region,
                horizontal_label=new.horizontal_label,
                drs_and_filename_consistent=True,
                dimensions={name: len(value) for name, value in new.dimensions.items()},
            )
        assert sha256(new_path) == record["new_sha256"]
        assert sha256(old_path) == record["baseline_sha256"]
        records.append(record)
    assert len(records) == len(EXPECTED)
    return records


if __name__ == "__main__":
    baseline, current, universe_db = map(Path, sys.argv[1:])
    with sqlite3.connect(f"file:{universe_db}?mode=ro", uri=True) as connection:
        grids = {
            name: json.loads(raw)
            for name, raw in connection.execute(
                "SELECT t.id,t.specs FROM uterms t JOIN udata_descriptors d "
                "ON t.data_descriptor_pk=d.pk WHERE d.id='grid'"
            )
        }
    evidence = {
        "baseline_commit": "99e671d978ae38a102ddbcce36cacf45c4102c41",
        "universe_db_sha256": sha256(universe_db),
        "registered_grids": {
            key: grids[key] for key in ["g011", "g012", "g014", "g262"]
        },
        "registered_grids_with_300_cells": [
            key for key, value in grids.items() if value.get("n_cells") in (300, "300")
        ],
        "fresh_outputs": audit(baseline, current),
    }
    Path(__file__).with_name("evidence.json").write_text(
        json.dumps(evidence, indent=2) + "\n"
    )
    print(
        "PASS: all six fresh files preserve every variable, value and mask; DRS agrees."
    )
