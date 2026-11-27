"""Check one native CICE5 month and a fresh CMIP7 snd file on Gadi."""

import argparse
import json
import tempfile
from pathlib import Path

import dask
import netCDF4 as nc
import numpy as np
import xarray as xr

from access_moppy.derivations import evaluate_expression
from access_moppy.driver import ACCESS_ESM_CMORiser
from access_moppy.utilities import load_model_mappings

SOURCE = Path(
    "/g/data/p73/archive/CMIP7/ACCESS-ESM1-6/production/"
    "Ndep2-PI-CNP-concentrations/output000/ice/iceh-1monthly-mean_0101-01.nc"
)
PUBLISHED = Path(
    "/g/data/im55/publications/MIP-DRS7/CMIP7/CMIP/ACCESS-Consortium/"
    "ACCESS-ESM1-6/piControl/r1i1p1f1/glb/mon/snd/tavg-u-hxy-sn/g118/v20261003/"
    "snd_tavg-u-hxy-sn_mon_glb_g118_ACCESS-ESM1-6_piControl_r1i1p1f1_010101-010912.nc"
)
PRODUCT = "seaIce.snd.tavg-u-hxy-sn.mon.glb"
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
# The input and output variables are float32. Calculate the reference in float64.
RELATIVE_TOLERANCE = 3e-7


def assert_snow(actual, expected, missing):
    np.testing.assert_array_equal(np.ma.getmaskarray(actual), missing)
    np.testing.assert_allclose(
        actual.data[~missing], expected[~missing], rtol=RELATIVE_TOLERANCE, atol=0
    )


def validate(scratch):
    with nc.Dataset(SOURCE) as native, nc.Dataset(PUBLISHED) as published:
        assert len(native.dimensions["time"]) == 1
        assert native["siconc"].units == "%"
        assert native["sisnmass"].units == "kg m^-2"
        assert native["aice"].units == "1"
        assert published.access_moppy_version == "1.8.1"

        concentration = native["siconc"][0]
        mass = native["sisnmass"][0]
        land = native["tmask"][:] == 0
        missing = np.ma.getmaskarray(concentration) | np.ma.getmaskarray(mass)
        np.testing.assert_array_equal(missing, land)
        assert 0 <= concentration.min() <= concentration.max() <= 100
        assert mass.min() >= 0
        aice = native["aice"][0]
        np.testing.assert_array_equal(
            np.ma.getmaskarray(concentration), np.ma.getmaskarray(aice)
        )
        np.testing.assert_allclose(
            concentration.compressed(),
            100 * aice.compressed(),
            rtol=RELATIVE_TOLERANCE,
            atol=0,
        )

        percent = concentration.data
        fraction = percent.astype(np.float64) / 100
        mass_kg = mass.data.astype(np.float64)
        positive_ice = (~missing) & (percent > 0)
        # Compare in the native percent dtype at the inclusive boundary.
        threshold = percent.dtype.type(0.01)
        retained = (~missing) & (percent >= threshold)
        residual = positive_ice & ~retained
        expected = np.zeros(percent.shape)
        expected[retained] = mass_kg[retained] / (330 * fraction[retained])

        legacy_reference = np.zeros(percent.shape)
        legacy_reference[positive_ice] = mass_kg[positive_ice] / (
            317 * fraction[positive_ice]
        )
        old = published["snd"][0]
        assert_snow(old, legacy_reference, np.zeros_like(missing))
        extremes = old.data > 10
        assert extremes.any()
        assert retained.any() and residual.any() and missing.any()
        assert np.all(residual[extremes])
        time = native["time"][:]
        bounds = native["time_bounds"][:]
        np.testing.assert_array_equal(published["time"][:1], time)
        np.testing.assert_array_equal(published["time_bnds"][:1], bounds)
        for native_name, public_name in (("TLAT", "latitude"), ("TLON", "longitude")):
            # The bundled grid is float64 and CICE history coordinates are float32.
            np.testing.assert_allclose(
                native[native_name][:], published[public_name][:], rtol=0, atol=2e-5
            )
        parent_info = {key: published.getncattr(key) for key in PARENT_KEYS}
        tracking_id = published.tracking_id

    output_dir = Path(tempfile.mkdtemp(prefix="moppy-snd-", dir=scratch))
    with dask.config.set(scheduler="synchronous"):
        mapping = load_model_mappings(PRODUCT, "ACCESS-ESM1-6")["sisnthick"]
        fields = ["sisnmass", "siconc"]
        with xr.open_dataset(SOURCE, chunks={"time": 1}, decode_times=False) as native:
            calculated = evaluate_expression(mapping["calculation"], native[fields])
            derived = np.ma.masked_invalid(calculated.isel(time=0).compute().values)
        assert_snow(derived, expected, missing)

        with ACCESS_ESM_CMORiser(
            input_data=SOURCE,
            compound_name=PRODUCT,
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
    (output,) = output_dir.rglob("*.nc")

    with nc.Dataset(output) as fresh, nc.Dataset(PUBLISHED) as published:
        assert fresh["snd"].units == "m"
        assert fresh.area_label == "sn"
        assert fresh.grid_label == "g118"
        assert fresh.branding_suffix == "tavg-u-hxy-sn"
        np.testing.assert_array_equal(fresh["time"][:], time)
        np.testing.assert_array_equal(fresh["time_bnds"][:], bounds)
        for coord in ("i", "j", "latitude", "longitude"):
            np.testing.assert_array_equal(fresh[coord][:], published[coord][:])
        snd = fresh["snd"]
        new = snd[0]
        assert_snow(new, expected, missing)
        assert 0 <= new.min() <= new.max() < 10
        np.testing.assert_array_equal(new[residual], 0)
        np.testing.assert_allclose(
            new[retained],
            old[retained].astype(np.float64) * (317 / 330),
            rtol=RELATIVE_TOLERANCE,
            atol=0,
        )
        snd.set_auto_maskandscale(False)
        np.testing.assert_array_equal(snd[0][missing], snd.getncattr("_FillValue"))

    summary = {
        "native": str(SOURCE),
        "published": str(PUBLISHED),
        "published_tracking_id": tracking_id,
        "fresh": str(output),
        "native_masked_land_cells": int(missing.sum()),
        "published_land_zeros": int((old[missing] == 0).sum()),
        "fresh_land_fill_cells": int(np.ma.getmaskarray(new).sum()),
        "positive_residual_ice_cells": int(residual.sum()),
        "positive_residual_ice_cells_with_old_positive_snd": int(
            (old[residual] > 0).sum()
        ),
        "retained_ice_cells": int(retained.sum()),
        "retained_positive_snd_cells": int((new[retained] > 0).sum()),
        "published_snd_max_m": float(old.max()),
        "published_snd_cells_over_10_m": int(extremes.sum()),
        "concentration_percent_range_at_published_extremes": [
            float(percent[extremes].min()),
            float(percent[extremes].max()),
        ],
        "fresh_snd_range_m": [float(new.min()), float(new.max())],
        "fresh_snd_max_abs_reference_difference_m": float(
            np.max(np.abs(new.compressed() - expected[~missing]))
        ),
    }
    print(json.dumps(summary, sort_keys=True), flush=True)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("scratch", type=Path, help="Existing private scratch directory")
    validate(parser.parse_args().scratch)
