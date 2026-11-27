"""Write and independently check one native month and day for issue #859."""

import argparse
import json
import logging
import tempfile
from pathlib import Path

import dask
import netCDF4 as nc
import numpy as np
import xarray as xr

from access_moppy.driver import ACCESS_ESM_CMORiser

NATIVE = Path(
    "/g/data/p73/archive/CMIP7/ACCESS-ESM1-6/production/"
    "Ndep2-PI-CNP-concentrations/output000/atmosphere/netCDF"
)
PUBLISHED = Path(
    "/g/data/im55/publications/MIP-DRS7/CMIP7/CMIP/ACCESS-Consortium/"
    "ACCESS-ESM1-6/piControl/r1i1p1f1/glb"
)
WATER = "fld_s08i223"
PRODUCTS = (
    ("mon", "aiihca.pa-010101_mon.nc", "v20261002", "010101-010912"),
    ("day", "aiihca.pe-010101_dai.nc", "v20261117", "01010101-01041231"),
)
PARENT_KEYS = (
    "parent_activity_id",
    "parent_experiment_id",
    "parent_mip_era",
    "parent_source_id",
    "parent_time_units",
    "parent_variant_label",
    "branch_time_in_child",
    "branch_time_in_parent",
    "branch_method",
)


def time_bounds(dataset):
    time = dataset["time"]
    return nc.num2date(dataset[time.bounds][0, :], time.units, time.calendar).tolist()


def read_native(path, frequency):
    with nc.Dataset(path) as source:
        if frequency == "mon":
            assert len(source.dimensions["time"]) == 1
        assert source[WATER].dimensions == (
            "time",
            "soil_model_level_number",
            "lat",
            "lon",
        )
        np.testing.assert_array_equal(source["soil_model_level_number"][:], range(1, 7))
        assert source[WATER].units == "kg m-2"
        layers = source[WATER][0].astype(np.float64).filled(np.nan)
        coordinates = {dim: source[dim][:] for dim in ("lat", "lon")}
        return layers, coordinates, time_bounds(source)


def stage_day(native, workdir):
    """Copy the first real daily mean and its original time and space metadata."""
    path = workdir / native.name
    with xr.open_dataset(native, decode_times=False) as source:
        dependencies = [WATER, source.time.attrs["bounds"]]
        for coordinate in ("lat", "lon"):
            bounds = source[coordinate].attrs.get("bounds")
            if bounds in source:
                dependencies.append(bounds)
        grid_mapping = source[WATER].attrs.get("grid_mapping")
        if grid_mapping in source:
            dependencies.append(grid_mapping)
        source[dependencies].isel(time=slice(0, 1)).to_netcdf(path)
    return path


def check_depth(dataset):
    depth = dataset["depth"]
    assert depth.ndim == 0 and "depth" not in dataset.dimensions
    assert depth.standard_name == "depth" and depth.positive == "down"
    assert "depth" in dataset["mrsol"].coordinates.split()
    cm_per_unit = {"cm": 1, "m": 100}[depth.units]
    np.testing.assert_allclose(depth[()] * cm_per_unit, 5, rtol=0, atol=1e-6)
    np.testing.assert_allclose(
        dataset[depth.bounds][:] * cm_per_unit, [0, 10], rtol=0, atol=1e-6
    )


def check_output(dataset, frequency, coordinates, bounds, expected):
    field = dataset["mrsol"]
    assert field.dimensions == ("time", "lat", "lon") and field.units == "kg m-2"
    assert dataset.frequency == frequency and dataset.area_label == "lnd"
    check_depth(dataset)
    for name, values in coordinates.items():
        np.testing.assert_array_equal(dataset[name][:], values)
    assert time_bounds(dataset) == bounds

    missing = np.isnan(expected)
    actual = field[0]
    np.testing.assert_array_equal(np.ma.getmaskarray(actual), missing)
    # Allow float32 input-precision rounding, even when the writer stores float64.
    np.testing.assert_allclose(actual[~missing], expected[~missing], rtol=2e-7, atol=0)
    field.set_auto_maskandscale(False)
    np.testing.assert_array_equal(field[0][missing], field._FillValue)
    return float(np.max(abs(actual[~missing] - expected[~missing])))


def check_product(workdir, land, frequency, filename, version, dates):
    native = NATIVE / filename
    layers, coordinates, bounds = read_native(native, frequency)
    assert (bounds[1] - bounds[0]).days == {"mon": 31, "day": 1}[frequency]
    np.testing.assert_array_equal(np.isnan(layers[:3]).all(axis=0), ~land)
    assert np.isfinite(layers[:3, land]).all()

    # The first two layers span 0–8 cm and layer 3 spans 8–23.4 cm.
    expected = layers[0] + layers[1] + (0.020 / 0.154) * layers[2]
    assert (layers[3:, land].sum(axis=0) > 0).any()
    publication = (
        PUBLISHED
        / frequency
        / "mrsol/tavg-d10cm-hxy-lnd/g115"
        / version
        / f"mrsol_tavg-d10cm-hxy-lnd_{frequency}_glb_g115_ACCESS-ESM1-6_piControl_r1i1p1f1_{dates}.nc"
    )
    with nc.Dataset(publication) as old:
        parent_info = {key: old.getncattr(key) for key in PARENT_KEYS}
        published_version = old.access_moppy_version
        if frequency == "mon":
            assert time_bounds(old) == bounds
            actual = old["mrsol"][0]
            legacy = (
                layers[0] + layers[1] + ((0.1 - 0.051) / (0.157 - 0.051)) * layers[2]
            )
            np.testing.assert_array_equal(np.ma.getmaskarray(actual), False)
            np.testing.assert_array_equal(actual[~land], 0)
            np.testing.assert_allclose(actual[land], legacy[land], rtol=2e-7, atol=0)
            np.testing.assert_array_equal(old["depth"][:], np.arange(1, 7))
            assert old["depth"].units == "cm"
            assert "depth" not in getattr(old["mrsol"], "coordinates", "").split()
            ratios = actual[land] / expected[land]
            published_metrics = {
                "max_abs_error_vs_legacy_kg_m2": float(
                    np.max(abs(actual[land] - legacy[land]))
                ),
                "published_over_reference_min": float(np.min(ratios)),
                "published_over_reference_mean": float(np.mean(ratios)),
                "published_over_reference_max": float(np.max(ratios)),
                "ocean_zeros": int((~land).sum()),
                "orphan_depth_values": old["depth"][:].tolist(),
            }
        else:
            published_metrics = {
                "max_abs_error_vs_reference_kg_m2": check_output(
                    old, frequency, coordinates, bounds, expected
                ),
            }

    input_path = native if frequency == "mon" else stage_day(native, workdir)
    output_dir = workdir / frequency
    with ACCESS_ESM_CMORiser(
        input_data=input_path,
        compound_name=f"land.mrsol.tavg-d10cm-hxy-lnd.{frequency}.GLB",
        experiment_id="piControl",
        source_id="ACCESS-ESM1-6",
        variant_label="r1i1p1f1",
        cmip_version="CMIP7",
        output_path=output_dir,
        publication_profile="access-consortium",
        parent_info=parent_info,
        split_years=1,
        write_prefetch=1,
    ) as cmoriser:
        cmoriser.run()
        cmoriser.write()

    (fresh,) = output_dir.rglob("*.nc")
    with nc.Dataset(fresh) as dataset:
        assert len(dataset.dimensions["time"]) == 1
        error = check_output(dataset, frequency, coordinates, bounds, expected)
    return {
        "frequency": frequency,
        "time_interval": [str(date) for date in bounds],
        "native": str(native),
        "published": str(publication),
        "published_moppy_version": published_version,
        "published_comparison": published_metrics,
        "fresh": str(fresh),
        "fresh_max_abs_error_vs_reference_kg_m2": error,
        "land_values_checked": int(land.sum()),
        "ocean_fill_values_checked": int((~land).sum()),
        "depth_cm": 5,
        "depth_bounds_cm": [0, 10],
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("scratch", type=Path, help="Existing private scratch directory")
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO)
    workdir = Path(tempfile.mkdtemp(prefix="issue-859-", dir=args.scratch.resolve()))
    # The daily file lacks land fraction. Use the co-located monthly field on
    # the same piControl grid as an independent land/ocean reference for both.
    with nc.Dataset(NATIVE / PRODUCTS[0][1]) as monthly:
        land = monthly["fld_s03i395"][0].filled(0) > 0
        coordinates = {dim: monthly[dim][:] for dim in ("lat", "lon")}
    with nc.Dataset(NATIVE / PRODUCTS[1][1]) as daily:
        for name, values in coordinates.items():
            np.testing.assert_array_equal(daily[name][:], values)
    assert land.any() and (~land).any()
    with dask.config.set(scheduler="synchronous"):
        for product in PRODUCTS:
            print(json.dumps(check_product(workdir, land, *product)), flush=True)


if __name__ == "__main__":
    main()
