"""Compare two time records per case through archived MOM and fresh CMOR output."""

import argparse
import json
import tempfile
from contextlib import ExitStack
from pathlib import Path

import dask
import netCDF4 as nc
import numpy as np
import xarray as xr

from access_moppy.driver import ACCESS_ESM_CMORiser

ARCHIVE = Path("/g/data/p73/archive/CMIP7/ACCESS-ESM1-6/production")
PUBLIC = Path("/g/data/im55/publications/MIP-DRS7/CMIP7")
MODEL = "ACCESS-ESM1-6"
MEMBER = "r1i1p1f1"
BRAND = "tavg-u-hxy-sea"
OFFSET = 273.15
FIELDS = {"tos": "surface_temp", "tossq": "squared_surface_temp"}
CASES = {
    "piControl": (
        "CMIP",
        "Ndep2-PI-CNP-concentrations",
        "mon",
        "010101-010912",
        ("v20261001", "v20261117"),
    ),
    "1pctCO2-bgc": (
        "C4MIP",
        "1pctCO2-bgc-01",
        "day",
        "02010101-02041231",
        ("v20261019", "v20261119"),
    ),
}
PARENT_KEYS = (
    "parent_experiment_id",
    "parent_activity_id",
    "parent_mip_era",
    "parent_source_id",
    "parent_variant_label",
    "parent_time_units",
    "branch_time_in_child",
    "branch_time_in_parent",
    "branch_method",
)
N_RECORDS = 2


def representation(variable):
    return {
        "dtype": str(variable.dtype),
        "units": variable.units,
        "cell_methods": variable.cell_methods,
        "packing": {
            name: variable.getncattr(name)
            for name in ("scale_factor", "add_offset")
            if name in variable.ncattrs()
        },
        "quantization": variable.quantization(),
    }


def comparable_variance(pair):
    return pair["tossq"].astype(np.float64) - pair["tos"].astype(np.float64) ** 2


def rounding_interval(values):
    """Bound the exact value assuming rounding to nearest at the archive dtype."""
    data = values.data
    exact = data.astype(np.float64)
    lower = np.nextafter(data, data.dtype.type(-np.inf))
    upper = np.nextafter(data, data.dtype.type(np.inf))
    return (exact + lower) / 2, (exact + upper) / 2


def compare(scratch, experiment):
    activity, run, frequency, dates, versions = CASES[experiment]
    ocean = ARCHIVE / run / "output000/ocean"
    native_paths = {
        product: ocean / f"ocean-2d-{field}-1{frequency}-mean-y_{dates[:4]}.nc"
        for product, field in FIELDS.items()
    }
    public_paths = {
        product: PUBLIC
        / activity
        / "ACCESS-Consortium"
        / MODEL
        / experiment
        / MEMBER
        / "glb"
        / frequency
        / product
        / BRAND
        / "g118"
        / version
        / f"{product}_{BRAND}_{frequency}_glb_g118_{MODEL}_{experiment}_{MEMBER}_{dates}.nc"
        for product, version in zip(FIELDS, versions, strict=True)
    }
    result = {
        "experiment": experiment,
        "frequency": frequency,
        "native": {},
        "published": {},
        "fresh": {},
    }
    values = {"native": {}, "published": {}, "fresh": {}}
    with ExitStack() as stack:
        native_grid = {}
        for name in ("geolat_t", "geolon_t"):
            path = ocean / f"ocean-2d-{name}-fx.nc"
            source = stack.enter_context(nc.Dataset(path))
            native_grid[name] = source[name][:].squeeze()
        for product, native_field in FIELDS.items():
            native_path = native_paths[product]
            published_path = public_paths[product]
            native = stack.enter_context(nc.Dataset(native_path))
            public = stack.enter_context(nc.Dataset(published_path))
            native_variable = native[native_field]
            public_variable = public[product]
            native_metadata = representation(native_variable)
            assert native_metadata["dtype"] == "float32"
            assert (
                not native_metadata["packing"]
                and native_metadata["quantization"] is None
            )
            assert native_variable.units == {"tos": "K", "tossq": "squared K"}[product]
            assert public_variable.units == {"tos": "degC", "tossq": "degC2"}[product]
            assert native_variable.cell_methods == "time: mean"
            assert public_variable.cell_methods == "area: mean where sea time: mean"
            result["native"][product] = {"path": str(native_path), **native_metadata}
            result["published"][product] = {
                "path": str(published_path),
                "file": str(published_path.resolve()),
                "tracking_id": public.tracking_id,
                **representation(public_variable),
            }
            values["native"][product] = native_variable[:N_RECORDS]
            values["published"][product] = public_variable[:N_RECORDS]
            if product == "tos":
                times = native["time"][:N_RECORDS]
                bounds = native["time_bnds"][:N_RECORDS]
                time_metadata = (native["time"].units, native["time"].calendar)
                axes = {name: native[name][:] for name in ("xt_ocean", "yt_ocean")}
                parent = {key: public.getncattr(key) for key in PARENT_KEYS}
            for data in (native, public):
                assert (data["time"].units, data["time"].calendar) == time_metadata
                np.testing.assert_array_equal(data["time"][:N_RECORDS], times)
                np.testing.assert_array_equal(data["time_bnds"][:N_RECORDS], bounds)
            np.testing.assert_array_equal(
                native["average_T1"][:N_RECORDS], bounds[:, 0]
            )
            np.testing.assert_array_equal(
                native["average_T2"][:N_RECORDS], bounds[:, 1]
            )
            np.testing.assert_array_equal(
                native["average_DT"][:N_RECORDS], np.diff(bounds)[:, 0]
            )
            for name, axis in axes.items():
                np.testing.assert_array_equal(native[name][:], axis)
            np.testing.assert_allclose(
                public["latitude"][:], native_grid["geolat_t"], atol=3e-5, rtol=0
            )
            lon_difference = (
                public["longitude"][:] - native_grid["geolon_t"] + 180
            ) % 360 - 180
            np.testing.assert_allclose(lon_difference, 0, atol=3e-5, rtol=0)

    # Write two-record copies with the original values and time metadata.
    run_dir = Path(tempfile.mkdtemp(prefix=f"{experiment}-", dir=scratch))
    staged = {}
    for product, path in native_paths.items():
        staged[product] = run_dir / path.name
        with xr.open_dataset(path, decode_times=False, engine="netcdf4") as original:
            original.isel(time=slice(0, N_RECORDS)).to_netcdf(staged[product])

    for product in FIELDS:
        output = run_dir / product
        output.mkdir()
        inputs = [staged["tos"]] if product == "tos" else list(staged.values())
        with ACCESS_ESM_CMORiser(
            input_data=inputs,
            compound_name=f"ocean.{product}.{BRAND}.{frequency}.glb",
            experiment_id=experiment,
            source_id=MODEL,
            variant_label=MEMBER,
            cmip_version="CMIP7",
            activity_id=activity,
            output_path=output,
            publication_profile="access-consortium",
            parent_info=parent,
            split_years=1,
            write_prefetch=1,
        ) as cmoriser:
            cmoriser.run(write_output=True)
        (output_file,) = output.rglob("*.nc")
        with (
            nc.Dataset(output_file) as fresh,
            nc.Dataset(public_paths[product]) as public,
        ):
            for coord in ("time", "time_bnds", "i", "j", "latitude", "longitude"):
                expected = (
                    public[coord][:N_RECORDS]
                    if coord.startswith("time")
                    else public[coord][:]
                )
                np.testing.assert_array_equal(fresh[coord][:], expected)
            values["fresh"][product] = fresh[product][:]
            result["fresh"][product] = {
                "path": str(output_file),
                **representation(fresh[product]),
            }

    mean = values["native"]["tos"]
    squared = values["native"]["tossq"]
    mean_c = mean.astype(np.float64) - OFFSET
    squared_c = (
        squared.astype(np.float64) - 2 * OFFSET * mean.astype(np.float64) + OFFSET**2
    )
    mask = np.ma.getmaskarray(mean)
    for dataset in values.values():
        for array in dataset.values():
            np.testing.assert_array_equal(np.ma.getmaskarray(array), mask)
    references = {
        "tos": mean - np.float32(OFFSET),
        "tossq": np.ma.maximum(squared_c, 0),
    }
    for product in FIELDS:
        expected = references[product].astype(values["fresh"][product].dtype)
        np.testing.assert_array_equal(values["fresh"][product][~mask], expected[~mask])

    mean_lower, mean_upper = rounding_interval(mean)
    square_lower, square_upper = rounding_interval(squared)
    # The native means are positive Kelvin values, so squaring is monotone.
    variance_lower = square_lower - mean_upper**2
    variance_upper = square_upper - mean_lower**2
    variances = {name: comparable_variance(pair) for name, pair in values.items()}
    native_variance = variances["native"]
    public_variance = variances["published"]
    fresh_variance = variances["fresh"]
    result["time_units"], result["calendar"] = time_metadata
    result["records"] = []
    for index, bounds_row in enumerate(bounds):
        valid = ~mask[index]
        record = {
            "bounds": bounds_row.tolist(),
            "wet": int(valid.sum()),
            "masked": int((~valid).sum()),
        }
        for name, variance in variances.items():
            samples = variance[index][valid]
            record[f"{name}_negative"] = int((samples < 0).sum())
            record[f"{name}_min"] = float(samples.min())
        record["native_negative_beyond_rounding"] = int(
            (variance_upper[index][valid] < 0).sum()
        )
        record["native_max_rounding_width"] = float(
            (variance_upper[index][valid] - variance_lower[index][valid]).max()
        )
        record["native_max_upward_rounding"] = float(
            (variance_upper[index][valid] - native_variance[index][valid]).max()
        )
        record["max_abs_fresh_minus_native_variance"] = float(
            np.ma.max(np.ma.abs(fresh_variance[index] - native_variance[index]))
        )
        record["max_abs_published_minus_fresh_variance"] = float(
            np.ma.max(np.ma.abs(public_variance[index] - fresh_variance[index]))
        )
        record["max_abs_fresh_tos_minus_ideal_conversion"] = float(
            np.ma.max(np.ma.abs(values["fresh"]["tos"][index] - mean_c[index]))
        )
        record["second_moment_preclip_negative"] = int(
            (squared_c[index][valid] < 0).sum()
        )
        record["max_abs_published_minus_fresh_moment"] = {
            product: float(
                np.ma.max(
                    np.ma.abs(
                        values["published"][product][index]
                        - values["fresh"][product][index]
                    )
                )
            )
            for product in FIELDS
        }
        result["records"].append(record)
    return result


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("scratch", type=Path, help="Existing private scratch directory")
    parser.add_argument("experiment", choices=CASES)
    args = parser.parse_args()
    with dask.config.set(scheduler="synchronous", num_workers=1):
        print(
            json.dumps(compare(args.scratch, args.experiment), sort_keys=True),
            flush=True,
        )
