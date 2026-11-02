"""Independent native-input comparisons; no MOPPy derivation imports."""

import hashlib
import json
from pathlib import Path

import netCDF4
import numpy as np

HERE = Path(__file__).resolve().parent
BASE = Path("/home/romain/PROJECTS/ACCESS-MOPPy/reports/issue_767_rerun")
selection = json.loads((BASE / "selection.json").read_text())["variables"]
products = [
    "land.mrsfl.tavg-sl-hxy-lnd.mon.glb",
    "land.mrsll.tavg-sl-hxy-lnd.mon.glb",
    "land.mrsol.tavg-sl-hxy-lnd.mon.glb",
    "land.mrsol.tavg-d10cm-hxy-lnd.day.glb",
]
results = []
for product in products:
    row = next(r for r in selection if r["product"] == product)
    output = next((HERE / "outputs" / product).rglob("*.nc"))
    baseline = next((BASE / "outputs" / product).rglob("*.nc"))
    sources = {}
    input_hashes = {}
    for relative in row["inputs"]:
        source = BASE / "native" / relative
        with netCDF4.Dataset(source) as ds:
            name = str(ds.variable_id)
            sources[name] = np.ma.asarray(ds[name][:])
        sources[name] = np.ma.array(
            sources[name].filled(0), mask=np.ma.getmaskarray(sources[name])
        )
        input_hashes[relative] = hashlib.sha256(source.read_bytes()).hexdigest()
    water = sources["fld_s08i223"]
    daily = ".day." in product
    if daily:
        expected = (
            water[:, 0].astype("float64")
            + water[:, 1].astype("float64")
            + water[:, 2].astype("float64") * (0.020 / 0.154)
        )
    elif ".mrsfl." in product:
        expected = water * sources["fld_s08i230"]
    elif ".mrsll." in product:
        expected = water * sources["fld_s08i229"]
    else:
        expected = water
    with netCDF4.Dataset(output) as ds, netCDF4.Dataset(baseline) as previous:
        actual = np.ma.asarray(ds[ds.variable_id][:])
        old = np.ma.asarray(previous[previous.variable_id][:])
        masks_match = np.array_equal(
            np.ma.getmaskarray(actual), np.ma.getmaskarray(expected)
        )
        assert masks_match
        native_max_error = float(np.ma.max(np.ma.abs(actual - expected)))
        np.testing.assert_allclose(
            actual.compressed(), expected.compressed(), rtol=1e-6, atol=1e-5
        )
        valid = ~np.ma.getmaskarray(actual)
        comparable = valid & ~np.ma.getmaskarray(old)
        info = dict(
            product=product,
            legacy=row["legacy"],
            output=str(output),
            output_sha256=hashlib.sha256(output.read_bytes()).hexdigest(),
            input_sha256=input_hashes,
            shape=list(actual.shape),
            valid_count=int(valid.sum()),
            missing_count=int((~valid).sum()),
            native_masks_match=masks_match,
            native_max_absolute_error=native_max_error,
            baseline_masks_match=bool(
                np.array_equal(np.ma.getmaskarray(actual), np.ma.getmaskarray(old))
            ),
            baseline_max_absolute_change=float(
                np.abs(
                    actual.data[comparable].astype("float64")
                    - old.data[comparable].astype("float64")
                ).max()
            ),
            newly_missing_count=int(
                (np.ma.getmaskarray(actual) & ~np.ma.getmaskarray(old)).sum()
            ),
            newly_valid_count=int(
                (~np.ma.getmaskarray(actual) & np.ma.getmaskarray(old)).sum()
            ),
            baseline_mean_change=float(
                (
                    actual.data[comparable].astype("float64")
                    - old.data[comparable].astype("float64")
                ).mean()
            ),
            depth=ds["depth"][:].tolist(),
            depth_dimensions=list(ds["depth"].dimensions),
            depth_units=ds["depth"].units,
            depth_bounds=ds["depth_bnds"][:].tolist(),
            coordinates=getattr(ds[ds.variable_id], "coordinates", ""),
        )
        if not daily:
            assert info["baseline_masks_match"]
            np.testing.assert_array_equal(actual.compressed(), old.compressed())
        fresh_root = HERE.parent / "baseline" / "outputs" / product
        fresh_paths = list(fresh_root.rglob("*.nc"))
        if fresh_paths:
            with netCDF4.Dataset(fresh_paths[0]) as clean:
                clean_values = np.ma.asarray(clean[clean.variable_id][:])
                info["clean_main_baseline"] = str(fresh_paths[0])
                info["clean_main_masks_match"] = bool(
                    np.array_equal(
                        np.ma.getmaskarray(actual), np.ma.getmaskarray(clean_values)
                    )
                )
                common = valid & ~np.ma.getmaskarray(clean_values)
                info["clean_main_max_absolute_change"] = float(
                    np.abs(
                        actual.data[common].astype("float64")
                        - clean_values.data[common].astype("float64")
                    ).max()
                )
                if not daily:
                    assert info["clean_main_masks_match"]
                    np.testing.assert_array_equal(
                        actual.compressed(), clean_values.compressed()
                    )
        results.append(info)
(HERE / "numerical.json").write_text(json.dumps(results, indent=2) + "\n")
for r in results:
    print(
        r["legacy"],
        "native error",
        r["native_max_absolute_error"],
        "baseline change",
        r["baseline_max_absolute_change"],
        "masks",
        r["baseline_masks_match"],
    )
