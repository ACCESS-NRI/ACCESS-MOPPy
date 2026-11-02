"""Independent NumPy validation of fresh #816 full-year nitrogen outputs."""

import argparse
import hashlib
import json
from pathlib import Path

import netCDF4
import numpy as np

parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument(
    "--native", type=Path, required=True, help="Native output000/atmosphere directory"
)
parser.add_argument(
    "--outputs", type=Path, required=True, help="Fresh per-product output directories"
)
parser.add_argument("--report", type=Path, required=True)
args = parser.parse_args()
NATIVE = args.native
OUTPUTS = args.outputs


def read(number):
    path = next(NATIVE.glob(f"*fld_s03i{number}.1mon.mean.0101.nc"))
    with netCDF4.Dataset(path) as ds:
        data = (
            np.ma.asarray(ds[f"fld_s03i{number}"][:]).astype("float64").filled(np.nan)
        )
        bounds = np.asarray(ds["time_bnds"][:])
        metadata = {
            k: str(ds.getncattr(k))
            for k in (
                "model",
                "model_version",
                "base_configuration",
                "experiment_uuid",
                "history",
            )
        }
    return (
        data,
        bounds,
        {
            "path": str(path),
            "sha256": hashlib.sha256(path.read_bytes()).hexdigest(),
            "metadata": metadata,
        },
    )


tiles, calendar_bounds, tiles_info = read(317)
land, _, land_info = read(395)
result = {
    "inputs": [tiles_info, land_info],
    "products": [],
    "method": "Independent NumPy float64 weighted sum over the 17 native tiles; mask landfrac<=0; convert gN/m2/day to kgN/m2/s by dividing by 1000*86400. No output baseline exists because prior processing failed.",
}
result["tile_max_change_over_year"] = float(np.nanmax(abs(tiles - tiles[:1])))
result["land_fraction_max_change_over_year"] = float(np.nanmax(abs(land - land[:1])))
result["tile_fraction_sum_max_error_on_land"] = float(
    np.nanmax(abs(np.nansum(tiles, axis=1)[land > 0] - 1))
)
for name, numbers in [
    ("fNgas", [920]),
    ("fNleach", [918]),
    ("fNloss", [918, 920]),
    ("fNup", [919]),
]:
    fields = []
    for number in numbers:
        values, bounds, info = read(number)
        fields.append(values)
        if info not in result["inputs"]:
            result["inputs"].append(info)
        np.testing.assert_allclose(
            bounds[:, 0] - calendar_bounds[:, 0], 47 / 48, rtol=0, atol=1e-10
        )
        np.testing.assert_array_equal(bounds[:, 1], calendar_bounds[:, 1])
    terms = sum(fields) * tiles
    expected = np.nansum(terms, axis=1) / (1000 * 86400)
    expected[~(land > 0)] = np.nan
    # Bound rounding in float32 multiplication, tile reduction, conversion and
    # addition of two inputs. This is scale-aware near small flux values.
    eps = np.finfo("float32").eps
    gamma = (tiles.shape[1] + 5) * eps / (1 - (tiles.shape[1] + 5) * eps)
    bound = gamma * np.nansum(abs(terms), axis=1) / (1000 * 86400)
    path = next((OUTPUTS / f"land.{name}.tavg-u-hxy-lnd.mon.glb").rglob("*.nc"))
    with netCDF4.Dataset(path) as ds:
        actual = np.ma.asarray(ds[name][:]).astype("float64").filled(np.nan)
        t = ds["time"]
        out_bounds = np.asarray(ds[t.bounds][:])
        dates = netCDF4.num2date(out_bounds.ravel(), t.units, t.calendar)
        native_dates = netCDF4.num2date(
            calendar_bounds.ravel(), "days since 0001-01-01", "proleptic_gregorian"
        )
        assert list(dates) == list(native_dates)
        np.testing.assert_array_equal(np.isnan(actual), np.isnan(expected))
        valid = np.isfinite(expected)
        delta = abs(actual - expected)
        assert np.all(delta[valid] <= bound[valid]), (
            name,
            float(np.nanmax(delta - bound)),
        )
        # Algebraic checks are independent of the expected-value construction.
        assert actual.shape == (12, 145, 192)
        assert not np.isinf(actual).any()
        product = {
            "name": name,
            "path": str(path),
            "sha256": hashlib.sha256(path.read_bytes()).hexdigest(),
            "shape": list(actual.shape),
            "mask_exact": True,
            "valid_values": int(valid.sum()),
            "missing_values": int((~valid).sum()),
            "max_abs_error": float(np.nanmax(delta)),
            "max_relative_error_nonzero": float(
                np.max(
                    delta[valid & (expected != 0)]
                    / abs(expected[valid & (expected != 0)])
                )
            ),
            "max_float32_roundoff_bound": float(np.nanmax(bound)),
            "roundoff_bound_passed": True,
            "calendar_bounds_exact": True,
        }
        result["products"].append(product)
# Confirm originals remained unchanged through the independent reads.
for item in result["inputs"]:
    assert hashlib.sha256(Path(item["path"]).read_bytes()).hexdigest() == item["sha256"]
args.report.write_text(json.dumps(result, indent=2) + "\n")
print(json.dumps({k: v for k, v in result.items() if k != "inputs"}, indent=2))
