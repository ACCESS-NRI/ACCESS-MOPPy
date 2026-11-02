"""Reproduce the additional 19 land-normalization defects with native data.

This is an evidence harness, not a production mapping change. Candidate recipes
replace precisely one landfrac argument with unity and then mask pure ocean.
References use explicit native-field sums, independent of the mapping helpers.
Existing field choices and unit constants are retained, including the separately
incorrect cRoot leaf input. See README.md for the scientific scope and limits.
"""

import argparse
import copy
import hashlib
import importlib.metadata
import json
import subprocess
from pathlib import Path
from unittest.mock import patch

import dask.array as da
import numpy as np
import xarray as xr

import access_moppy
from access_moppy.derivations import evaluate_expression
from access_moppy.driver import ACCESS_ESM_CMORiser
from access_moppy.utilities import load_model_mappings

YEAR_SECONDS = 365.2422 * 86400
# Tile-density inputs, already land-area-weighted product inputs, conversion.
CASES = {
    "cLand": (list(range(851, 861)), [898, 899, 900], 1 / 1000),
    "cLeaf": ([852], [], 1 / 1000),
    "cLitter": ([855, 856, 857], [], 1 / 1000),
    "cProduct": ([], [898, 899, 900], 1 / 1000),
    "cRoot": ([852], [], 1 / 1000),  # Area-only check; root diagnostic is 854.
    "nLand": (list(range(861, 871)), [901, 902, 903], 1 / 1000),
    "nLitter": ([864, 865, 866], [], 1 / 1000),
    "nMineral": ([870], [], 1 / 1000),
    "nProduct": ([], [901, 902, 903], 1 / 1000),
    "nSoil": ([867, 868, 869, 870], [], 1 / 1000),
    "nVeg": ([861, 862, 863], [], 1 / 1000),
    "fNdep": ([884], [], 1 / (1000 * 86400)),
    "fNgas": ([920], [], 1 / (1000 * 86400)),
    "fNleach": ([918], [], 1 / (1000 * 86400)),
    "fNloss": ([918, 920], [], 1 / (1000 * 86400)),
    "fNnetmin": ([917], [], 1 / (1000 * 86400)),
    "fNup": ([919], [], 1 / (1000 * 86400)),
    "fDeforestToProduct": ([], [895], 1 / (1000 * YEAR_SECONDS)),
    "fProductDecomp": ([], [907, 908, 909], 1 / (1000 * YEAR_SECONDS)),
}
MODELS = ("ACCESS-ESM1-5", "ACCESS-ESM1-6")
FRACTION = "fld_s03i395"
TILES = "fld_s03i317"


def sha256(path):
    with Path(path).open("rb") as stream:
        return hashlib.file_digest(stream, "sha256").hexdigest()


def candidate(expression):
    """Change only the one land-area-to-grid-area scaling argument."""
    result = copy.deepcopy(expression)
    changed = []

    def visit(node):
        if isinstance(node, dict):
            kwargs = node.get("kwargs", {})
            if kwargs.get("landfrac") == FRACTION:
                kwargs["landfrac"] = 1.0
                changed.append(node["operation"])
            for value in node.values():
                visit(value)
        elif isinstance(node, list):
            for value in node:
                visit(value)

    visit(result)
    assert len(changed) == 1, changed
    return {"type": "formula", "operation": "mask_land", "args": [result, FRACTION]}


def reference(name, context):
    weighted, products, conversion = CASES[name]
    tiles = context[TILES]
    dim = next(d for d in tiles.dims if "pseudo_level" in d)
    total = 0
    if weighted:
        total = (sum(context[f"fld_s03i{n}"] for n in weighted) * tiles).sum(dim)
    if products:
        total += sum(context[f"fld_s03i{n}"] for n in products).sum(dim)
    return (total * conversion).where(context[FRACTION] > 0)


def close_with_roundoff(actual, expected, tolerance, roundoff):
    if roundoff is None:
        np.testing.assert_allclose(
            actual, expected, rtol=tolerance, atol=0, equal_nan=True
        )
        return
    np.testing.assert_array_equal(np.isnan(actual), np.isnan(expected))
    allowed = tolerance * abs(expected) + roundoff
    error = abs(actual - expected)
    assert not (error > allowed).any(), float((error / allowed).max())


def check_values(old, fixed, expected, fraction, area, tolerance, roundoff=None):
    coast = (fraction > 0) & (fraction < 1)
    pure_land = fraction == 1
    ocean = fraction == 0
    assert coast.any() and pure_land.any() and ocean.any()
    close_with_roundoff(
        old.where(fraction > 0),
        expected * fraction,
        tolerance,
        None if roundoff is None else roundoff * fraction,
    )
    close_with_roundoff(fixed, expected, tolerance, roundoff)
    np.testing.assert_allclose(
        old.where(pure_land),
        fixed.where(pure_land),
        rtol=tolerance,
        atol=0,
        equal_nan=True,
    )
    assert np.isnan(fixed.values[ocean.values]).all()
    active = coast & (abs(expected) > 0)
    assert active.any()
    # Ensure the uncorrected recipe actually fails the intended land-mean
    # requirement, rather than merely agreeing with an algebraic identity.
    assert not np.allclose(
        old.values[active.values],
        expected.values[active.values],
        rtol=tolerance,
        atol=0,
    )
    land_area = area * fraction
    old_total = (old * land_area).sum(["lat", "lon"])
    fixed_total = (fixed * land_area).sum(["lat", "lon"])
    expected_total = (expected * land_area).sum(["lat", "lon"])
    np.testing.assert_allclose(fixed_total, expected_total, rtol=tolerance, atol=0)
    return {
        "nonzero_coastal_cells": int(active.sum()),
        "max_coastal_ratio_error": float(
            abs((old / expected - fraction).where(active)).max()
        ),
        "baseline_land_area_integral": old_total.values.tolist(),
        "candidate_land_area_integral": fixed_total.values.tolist(),
        "baseline_fails_land_mean": True,
        "candidate_passes_land_mean": True,
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--native-root", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    args.output.mkdir(parents=True, exist_ok=True)
    package = Path(access_moppy.__file__).parent
    repo = package.parents[1]
    paths = sorted(
        args.native_root.glob("output*/atmosphere/netCDF/*pa-195[123]??_mon.nc")
    )
    assert len(paths) == 36, f"Expected all 36 months of 1951-1953; found {len(paths)}"
    mappings = {
        model: {
            name: load_model_mappings(f"land.{name}.tavg-u-hxy-lnd.mon.glb", model)[
                name
            ]
            for name in CASES
        }
        for model in MODELS
    }
    for name in CASES:
        assert (
            mappings[MODELS[0]][name]["calculation"]
            == mappings[MODELS[1]][name]["calculation"]
        )
    candidates = {
        name: candidate(mappings[MODELS[1]][name]["calculation"]) for name in CASES
    }
    report = {
        "code_commit": subprocess.check_output(
            ["git", "rev-parse", "HEAD"], cwd=repo, text=True
        ).strip(),
        "script_sha256": sha256(__file__),
        "native_model": MODELS[1],
        "models_with_identical_expressions": list(MODELS),
        "versions": {
            p: importlib.metadata.version(p)
            for p in ["numpy", "xarray", "dask", "netCDF4", "cmip7-repack"]
        },
        "scope": "Area normalization only; existing field choices and conversions retained. cRoot still uses leaf field 852; true root field 854 is checked separately. No matching published piControl or ESM1-5 native validation. No production reprocessing or erratum resolution.",
        "integral_method": "areacella from native latitude/longitude bounds, radius 6371000 m; land area = areacella * sftlf/100 = areacella * native fld_s03i395",
        "reference_cases": CASES,
        "candidate_calculations": candidates,
        "native_months": [],
        "backend_axis_model_checks": [],
        "pipeline": {},
    }
    expected_1951 = {name: [] for name in CASES}
    fractions_1951 = []
    netmin_roundoff_1951 = []
    netmin_float32_1951 = []
    wanted = sorted(
        {n for weighted, products, _ in CASES.values() for n in weighted + products}
        | {317, 395, 854}
    )
    for index, path in enumerate(paths):
        with xr.open_dataset(path, engine="netcdf4") as raw:
            context = {}
            for n in wanted:
                field = raw[f"fld_s03i{n}"]
                # Match the native monthly sample, including CASA's time_0.
                for dim in [d for d in field.dims if d.startswith("time")]:
                    assert field.sizes[dim] == 1
                    assert raw[dim].dt.year.item() == raw.time.dt.year.item()
                    assert raw[dim].dt.month.item() == raw.time.dt.month.item()
                    field = field.isel({dim: 0}, drop=True)
                context[f"fld_s03i{n}"] = (
                    field.reset_coords(drop=True).astype("float64").load()
                )
            latb, lonb = np.deg2rad(raw.lat_bnds), np.deg2rad(raw.lon_bnds)
            area = (
                6371000.0**2
                * abs(np.sin(latb.isel(bnds=1)) - np.sin(latb.isel(bnds=0)))
                * abs(lonb.isel(bnds=1) - lonb.isel(bnds=0))
            ).load()
            time = raw.time.values
            first_year = raw.time.dt.year.item() == 1951
        f, tiles = context[FRACTION], context[TILES]
        tile_dim = next(d for d in tiles.dims if "pseudo_level" in d)
        np.testing.assert_allclose(
            tiles.sum(tile_dim).values[f.values > 0], 1, atol=2e-7, rtol=0
        )
        entry = {
            "file": str(path),
            "sha256": sha256(path),
            "month": str(time[0]),
            "coastal_cells": int(((f > 0) & (f < 1)).sum()),
            "variables": {},
        }
        for name in CASES:
            expression = mappings[MODELS[1]][name]["calculation"]
            expected = reference(name, context)
            old = evaluate_expression(expression, context)
            fixed = evaluate_expression(candidates[name], context)
            entry["variables"][name] = check_values(
                old, fixed, expected, f, area, 1e-12
            )
            if first_year:
                expected_1951[name].append(expected.expand_dims(time=time))
            if index == 0:
                for model in MODELS:
                    for axis in ("pseudo_level_0", "pseudo_level"):
                        for backend in ("eager", "dask"):
                            inputs = {
                                k: v.rename({tile_dim: axis})
                                if tile_dim in v.dims
                                else v
                                for k, v in context.items()
                            }
                            if backend == "dask":
                                inputs = {
                                    k: v.chunk({"lat": 50, "lon": 64})
                                    for k, v in inputs.items()
                                }
                            before = evaluate_expression(
                                mappings[model][name]["calculation"], inputs
                            )
                            after = evaluate_expression(candidates[name], inputs)
                            if backend == "dask":
                                assert isinstance(before.data, da.Array) and isinstance(
                                    after.data, da.Array
                                )
                            check_values(
                                before.compute(),
                                after.compute(),
                                expected,
                                f,
                                area,
                                1e-12,
                            )
                            report["backend_axis_model_checks"].append(
                                [name, model, axis, backend]
                            )
        entry["cRoot_separate_wrong_input_max_abs_kg_m2"] = float(
            abs(
                ((context["fld_s03i852"] - context["fld_s03i854"]) * tiles).sum(
                    tile_dim
                )
                / 1000
            )
            .where(f > 0)
            .max()
        )
        report["native_months"].append(entry)
        if first_year:
            fractions_1951.append(f.expand_dims(time=time))
            # Signed net-mineralisation contributions can nearly cancel.
            # Bound float32 product/sum/conversion rounding by gamma_(n+4)
            # times the sum of absolute contributions, not the tiny net flux.
            terms = context["fld_s03i917"] * tiles
            eps = np.finfo("float32").eps
            gamma = (tiles.sizes[tile_dim] + 4) * eps
            gamma /= 1 - gamma
            budget = gamma * abs(terms).sum(tile_dim) / (1000 * 86400)
            netmin_roundoff_1951.append(budget.expand_dims(time=time))
            direct32 = (
                (
                    context["fld_s03i917"].astype("float32") * tiles.astype("float32")
                ).sum(tile_dim)
                * np.float32(1 / (1000 * 86400))
            ).where(f > 0)
            netmin_float32_1951.append(direct32.expand_dims(time=time))
        print(
            f"Native month {index + 1}/36: {path.name}; all 19 normalization checks pass",
            flush=True,
        )
    fraction = xr.concat(fractions_1951, dim="time")
    paths_1951 = [p for p in paths if "-1951" in p.name]
    parent = {
        "parent_experiment_id": "piControl",
        "parent_activity_id": "CMIP",
        "parent_source_id": MODELS[1],
        "parent_variant_label": "r1i1p1f1",
        "parent_time_units": "days since 0001-01-01 00:00:00",
        "parent_mip_era": "CMIP7",
        "branch_time_in_child": 0.0,
        "branch_time_in_parent": 54786.0,
        "branch_method": "standard",
    }
    for name in CASES:
        written = {}
        output_info = {}
        for mode in ("baseline", "candidate"):

            def loader(*loader_args, **loader_kwargs):
                mapping = load_model_mappings(*loader_args, **loader_kwargs)
                if mode == "candidate" and name in mapping:
                    mapping[name]["calculation"] = copy.deepcopy(candidates[name])
                return mapping

            output = args.output / "netcdf" / name / mode
            with patch("access_moppy.driver.load_model_mappings", loader):
                with ACCESS_ESM_CMORiser(
                    input_data=paths_1951,
                    compound_name=f"land.{name}.tavg-u-hxy-lnd.mon.glb",
                    cmip_version="CMIP7",
                    source_id=MODELS[1],
                    experiment_id="historical",
                    variant_label="r1i1p1f1",
                    parent_info=parent,
                    output_path=output,
                    split_years=1,
                ) as cmoriser:
                    cmoriser.run()
                    cmoriser.write()
            files = list(output.rglob("*.nc"))
            assert len(files) == 1, files
            with xr.open_dataset(files[0]) as ds:
                written[mode] = ds[name].load()
                assert "area: mean where land" in ds[name].attrs["cell_methods"]
                assert ds.attrs["area_label"] == "lnd"
                output_info[mode] = {
                    "file": str(files[0]),
                    "sha256": sha256(files[0]),
                    "units": ds[name].attrs["units"],
                    "cell_methods": ds[name].attrs["cell_methods"],
                }
        expected = xr.concat(expected_1951[name], dim="time")
        for actual in written.values():
            assert actual.dims == expected.dims
            assert actual.shape == expected.shape
            for dim in expected.dims:
                np.testing.assert_array_equal(actual[dim].values, expected[dim].values)
        roundoff = (
            xr.concat(netmin_roundoff_1951, dim="time") if name == "fNnetmin" else None
        )
        stats = check_values(
            written["baseline"],
            written["candidate"],
            expected,
            fraction,
            area,
            1e-6,
            roundoff,
        )
        if roundoff is not None:
            np.testing.assert_array_equal(
                written["candidate"], xr.concat(netmin_float32_1951, dim="time")
            )
            stats["candidate_matches_independent_float32_reference_exactly"] = True
            stats["candidate_max_error_over_roundoff_bound"] = float(
                (abs(written["candidate"] - expected) / roundoff).max()
            )
            stats["roundoff_bound"] = (
                "gamma_(17+4) * sum(abs(tile_value * tile_fraction)) / (1000*86400), eps=float32.eps"
            )
        report["pipeline"][name] = {**stats, "outputs": output_info, "months": 12}
        print(
            f"CMOR/repack {name}: baseline defect reproduced; candidate coastal/land/ocean/integral checks pass",
            flush=True,
        )
        (args.output / "results.json").write_text(json.dumps(report, indent=2) + "\n")
    report["summary"] = {
        "variables": 19,
        "native_months": 36,
        "variable_month_checks": 19 * 36,
        "backend_axis_model_checks": len(report["backend_axis_model_checks"]),
        "cmor_repack_runs": 38,
        "candidate_output_variable_months": 19 * 12,
    }
    (args.output / "results.json").write_text(json.dumps(report, indent=2) + "\n")
    print(json.dumps(report["summary"]), flush=True)


if __name__ == "__main__":
    main()
