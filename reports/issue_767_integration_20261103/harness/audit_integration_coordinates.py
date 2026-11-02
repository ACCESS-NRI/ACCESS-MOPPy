"""Check interactions among coordinate, label, grid and metadata changes."""

import json
from pathlib import Path

import netCDF4
import numpy as np

ROOT = Path(__file__).parent
BASE = ROOT / "validation/baseline"
RUN = ROOT / "validation/integration"
soil = {"Emon.mrsfl", "Emon.mrsll", "Emon.mrsol", "day.mrsos"}
grids = {
    "SImon.siarean": ("g011", "nh"),
    "SImon.siareas": ("g012", "sh"),
    "SImon.siextentn": ("g011", "nh"),
    "SImon.siextents": ("g012", "sh"),
    "SImon.siareaacrossline": ("g014", "glb"),
    "Omon.msftmrho": ("g116", "glb"),
}
records = []
for p in sorted((RUN / "results").glob("*.json")):
    row = json.loads(p.read_text())
    old = json.loads((BASE / "results" / p.name).read_text())
    if not old.get("files"):
        continue
    path = RUN / row["files"][0]["path"]
    record = {
        "product": row["product"],
        "legacy": row["legacy"],
        "unchanged_auxiliary_arrays": [],
        "labels": [],
    }
    with (
        netCDF4.Dataset(BASE / old["files"][0]["path"]) as a,
        netCDF4.Dataset(path) as b,
    ):
        added = set(b.variables) - set(a.variables)
        removed = set(a.variables) - set(b.variables)
        assert not removed, (row["legacy"], removed)
        assert added <= ({"depth_bnds"} if row["legacy"] in soil else set()), (
            row["legacy"],
            added,
        )
        record["added_variables"] = sorted(added)
        for name in set(a.variables) & set(b.variables) - {b.variable_id}:
            av, bv = a[name], b[name]
            if row["legacy"] in soil and name in {"depth", "depth_bnds"}:
                continue
            x, y = np.ma.asarray(av[:]), np.ma.asarray(bv[:])
            if av.dtype.kind == "S" and bv.dtype.kind == "S":
                xs = netCDF4.chartostring(np.asarray(x.filled(b"\x00")))
                ys = netCDF4.chartostring(np.asarray(y.filled(b"\x00")))
                np.testing.assert_array_equal(xs, ys)
                assert bv.dimensions[-1] == "strlen", (
                    row["legacy"],
                    name,
                    bv.dimensions,
                )
                assert name in getattr(b[b.variable_id], "coordinates", "").split(), (
                    row["legacy"],
                    name,
                )
                record["labels"].append(
                    {
                        "name": name,
                        "dimensions": list(bv.dimensions),
                        "decoded_values_unchanged": True,
                        "associated": True,
                    }
                )
            else:
                assert av.dimensions == bv.dimensions and av.dtype == bv.dtype, (
                    row["legacy"],
                    name,
                )
                np.testing.assert_array_equal(
                    np.ma.getmaskarray(x), np.ma.getmaskarray(y)
                )
                np.testing.assert_array_equal(x.compressed(), y.compressed())
                record["unchanged_auxiliary_arrays"].append(name)
        if row["legacy"] in grids:
            grid, region = grids[row["legacy"]]
            assert b.grid_label == grid and b.region == a.region == region
            assert path.parent.parent.name == grid and f"_{region}_{grid}_" in path.name
            record["grid"] = {
                "label": grid,
                "region": region,
                "drs_and_filename_consistent": True,
                "resolved": row["legacy"] != "Omon.msftmrho",
            }
        if row["legacy"] == "AERmon.phalf":
            assert b["lev"].formula == "z = a + b*orog"
            assert b["lev"].computed_standard_name == "altitude"
            record["hybrid_formula_and_computed_standard_name"] = True
        if row["legacy"] == "OPmon.rsntds":
            assert b["rsntds"].cell_measures == "area: areacello"
            assert "areacello" in b.external_variables.split()
            record["cell_measure_external_variable_consistent"] = True
    records.append(record)
assert len(records) == 113
(RUN / "coordinate-interactions.json").write_text(json.dumps(records, indent=2) + "\n")
print(
    json.dumps(
        {
            "products": len(records),
            "unchanged_auxiliary_arrays": sum(
                len(r["unchanged_auxiliary_arrays"]) for r in records
            ),
            "preserved_associated_string_coordinates": sum(
                len(r["labels"]) for r in records
            ),
            "grid_drs_checks": sum("grid" in r for r in records),
        },
        indent=2,
    )
)
