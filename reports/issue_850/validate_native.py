"""Check the #850 coordinate repair on one native January 0101 piControl month."""

import argparse
import json
import logging
import tempfile
from pathlib import Path

import dask
import netCDF4 as nc
import numpy as np

from access_moppy.driver import ACCESS_ESM_CMORiser

NATIVE = Path(
    "/g/data/p73/archive/CMIP7/ACCESS-ESM1-6/production/"
    "Ndep2-PI-CNP-concentrations/output000/atmosphere/netCDF/aiihca.pa-010101_mon.nc"
)
PUBLISHED = Path(
    "/g/data/im55/publications/MIP-DRS7/CMIP7/CMIP/ACCESS-Consortium/"
    "ACCESS-ESM1-6/piControl/r1i1p1f1/glb/mon/tsl/tavg-sl-hxy-lnd/g115/v20261002/"
    "tsl_tavg-sl-hxy-lnd_mon_glb_g115_ACCESS-ESM1-6_piControl_r1i1p1f1_010101-010912.nc"
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
CENTRES = [0.011, 0.051, 0.157, 0.4385, 1.1855, 3.164]
INTERFACES = [0.0, 0.022, 0.080, 0.234, 0.643, 1.728, 4.600]
DIMENSIONS = ("time", "depth", "lat", "lon")


def time_bounds(dataset):
    time = dataset["time"]
    return nc.num2date(dataset[time.bounds][0], time.units, time.calendar).tolist()


def write_month(directory, cmip_version, compound_name, parent_info):
    with ACCESS_ESM_CMORiser(
        input_data=NATIVE,
        compound_name=compound_name,
        experiment_id="piControl",
        source_id="ACCESS-ESM1-6",
        variant_label="r1i1p1f1",
        cmip_version=cmip_version,
        output_path=directory,
        publication_profile="access-consortium",
        parent_info=parent_info,
        split_years=1,
        write_prefetch=1,
    ) as cmoriser:
        cmoriser.run()
        cmoriser.write()
    (path,) = directory.rglob("*.nc")
    return path


def check_geometry(dataset, name):
    assert dataset[name].dimensions == DIMENSIONS
    depth = dataset["depth"]
    assert depth.units == "m" and depth.positive == "down"
    assert depth.standard_name == "depth" and depth.bounds == "depth_bnds"
    bounds = dataset[depth.bounds]
    assert bounds.dimensions == ("depth", "bnds")
    centres, edges = depth[:], bounds[:]
    np.testing.assert_array_equal(np.ma.getmaskarray(centres), False)
    np.testing.assert_array_equal(np.ma.getmaskarray(edges), False)
    np.testing.assert_allclose(centres, CENTRES, rtol=0, atol=1e-7)
    np.testing.assert_allclose(
        edges, np.column_stack((INTERFACES[:-1], INTERFACES[1:])), rtol=0, atol=2e-7
    )
    return centres, edges


def check_samples(actual, reference, label):
    """Compare by position because the original depth labels are incorrect."""
    missing = np.ma.getmaskarray(reference)
    valid = ~missing
    np.testing.assert_array_equal(np.ma.getmaskarray(actual), missing, err_msg=label)
    np.testing.assert_array_equal(actual[valid], reference[valid], err_msg=label)
    return float(np.max(np.abs(actual[valid] - reference[valid])))


def validate(workdir):
    with nc.Dataset(NATIVE) as dataset:
        field = dataset["fld_s08i225"]
        assert field.dimensions == ("time", "soil_model_level_number", "lat", "lon")
        assert field.shape == (1, 6, 145, 192) and field.units == "K"
        np.testing.assert_array_equal(
            dataset["soil_model_level_number"][:], range(1, 7)
        )
        native = field[0]
        coordinates = {dim: dataset[dim][:] for dim in ("lat", "lon")}
        interval = time_bounds(dataset)
        assert (interval[1] - interval[0]).days == 31
    missing = np.ma.getmaskarray(native)
    # Require both valid and missing samples in every CABLE layer.
    assert missing.any(axis=(1, 2)).all() and (~missing).any(axis=(1, 2)).all()
    assert np.isfinite(native.compressed()).all()

    with nc.Dataset(PUBLISHED) as dataset:
        assert dataset["tsl"].dimensions == DIMENSIONS and dataset["tsl"].units == "K"
        assert time_bounds(dataset) == interval
        for name, values in coordinates.items():
            np.testing.assert_array_equal(dataset[name][:], values)
        depth = dataset["depth"]
        assert depth.units == "m" and "bounds" not in depth.ncattrs()
        assert "depth_bnds" not in dataset.variables
        np.testing.assert_allclose(depth[:], [*CENTRES[:-1], 2.872], rtol=0, atol=1e-7)
        legacy_depth = depth[:].tolist()
        legacy_version = dataset.access_moppy_version
        published = dataset["tsl"][0]
        parent_info = {key: dataset.getncattr(key) for key in PARENT_KEYS}

    outputs = {}
    for era, compound in (
        ("CMIP7", "land.tsl.tavg-sl-hxy-lnd.mon.GLB"),
        ("CMIP6", "Lmon.tsl"),
    ):
        path = write_month(workdir / era, era, compound, parent_info)
        with nc.Dataset(path) as dataset:
            assert dataset.mip_era == era and len(dataset.dimensions["time"]) == 1
            depth, bounds = check_geometry(dataset, "tsl")
            assert time_bounds(dataset) == interval
            for name, values in coordinates.items():
                np.testing.assert_array_equal(dataset[name][:], values)
            field = dataset["tsl"]
            assert field.units == "K" and field.standard_name == "soil_temperature"
            actual = field[0]
            native_error = check_samples(actual, native, f"{era} versus native")
            published_error = check_samples(
                actual, published, f"{era} versus published"
            )
            field.set_auto_maskandscale(False)
            np.testing.assert_array_equal(field[0][missing], field._FillValue)
            outputs[era] = {
                "fresh": str(path),
                "depth_m": depth.tolist(),
                "bounds_m": bounds.tolist(),
                "max_abs_error_vs_native_K": native_error,
                "max_abs_error_vs_published_K": published_error,
            }

    water_path = write_month(
        workdir / "layered_mrsol",
        "CMIP7",
        "land.mrsol.tavg-sl-hxy-lnd.mon.GLB",
        parent_info,
    )
    with nc.Dataset(water_path) as dataset:
        depth, bounds = check_geometry(dataset, "mrsol")
        np.testing.assert_array_equal(depth, outputs["CMIP7"]["depth_m"])
        np.testing.assert_array_equal(bounds, outputs["CMIP7"]["bounds_m"])

    return {
        "native": str(NATIVE),
        "published": str(PUBLISHED),
        "published_moppy_version": legacy_version,
        "published_depth_m": legacy_depth,
        "time_interval": [str(date) for date in interval],
        "valid_values_per_layer": (~missing).sum(axis=(1, 2)).tolist(),
        "missing_values_per_layer": missing.sum(axis=(1, 2)).tolist(),
        "outputs": outputs,
        "layered_mrsol": str(water_path),
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("scratch", type=Path, help="Existing private scratch directory")
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO)
    workdir = Path(tempfile.mkdtemp(prefix="issue-850-", dir=args.scratch.resolve()))
    with dask.config.set(scheduler="synchronous"):
        print(json.dumps(validate(workdir)), flush=True)


if __name__ == "__main__":
    main()
