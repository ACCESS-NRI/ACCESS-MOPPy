"""Validate #808 against native historical data through CMOR file writing.

Set ACCESS_MOPPY_DATA_ROOT to the ACCESS-ESM1-6 historical archive. This is
not a paired comparison with the published piControl release in the erratum.
"""

import os
from pathlib import Path

import numpy as np
import pytest
import xarray as xr

from access_moppy.driver import ACCESS_ESM_CMORiser
from access_moppy.file_discovery import _extract_year_from_path, discover_files


@pytest.mark.integration
@pytest.mark.parametrize(
    ("name", "fields", "conversion", "version", "branded"),
    [
        (
            "cSoil",
            ["fld_s03i858", "fld_s03i859", "fld_s03i860"],
            1 / 1000,
            "CMIP7",
            True,
        ),
        (
            "cVeg",
            ["fld_s03i851", "fld_s03i852", "fld_s03i853", "fld_s03i854"],
            1 / 1000,
            "CMIP7",
            True,
        ),
        ("lai", ["fld_s03i893"], 1, "CMIP7", True),
        ("fBNF", ["fld_s03i885"], 1 / (1000 * 86400), "CMIP7", True),
        ("snc", ["fld_s08i236"], 100, "CMIP7", True),
        ("snc", ["fld_s08i236"], 100, "CMIP7", False),
        ("snc", ["fld_s08i236"], 100, "CMIP6", False),
    ],
)
def test_native_baseline_land_means(
    name,
    fields,
    conversion,
    version,
    branded,
    tmp_path,
    parent_experiment_config,
    record_property,
):
    root = os.environ.get("ACCESS_MOPPY_DATA_ROOT")
    if not root or not Path(root).is_dir():
        pytest.skip("Set ACCESS_MOPPY_DATA_ROOT to an ACCESS-ESM1-6 historical archive")
    realm = "landIce" if name == "snc" else "land"
    request = f"{realm}.{name}.tavg-u-hxy-lnd.mon.glb"
    table = {"snc": "LImon", "cSoil": "Emon", "fBNF": "Emon"}.get(name, "Lmon")
    product = f"{table}.{name}"
    if not branded:
        request = product
    whole_cell = name == "snc" and version == "CMIP6"
    paths = discover_files(root, product, model_id="ACCESS-ESM1-6")
    assert paths
    year = _extract_year_from_path(paths[0])
    paths = discover_files(
        root, product, model_id="ACCESS-ESM1-6", start_year=year, end_year=year
    )
    wanted = fields + ["fld_s03i317", "fld_s03i395", "lat_bnds", "lon_bnds"]
    with xr.open_mfdataset(
        paths,
        engine="netcdf4",
        preprocess=lambda ds: ds[wanted],
        data_vars="minimal",
        coords="minimal",
        compat="override",
    ) as native:
        raw = native.astype("float64")
        tile_dim = next(d for d in raw.fld_s03i317.dims if "pseudo_level" in d)
        fraction = raw.fld_s03i395.load()
        coast = (fraction > 0) & (fraction < 1)
        pure_land = fraction == 1
        ocean = fraction == 0
        assert coast.any() and pure_land.any() and ocean.any()
        tile_total = raw.fld_s03i317.sum(tile_dim).load()
        np.testing.assert_allclose(tile_total.values[fraction.values > 0], 1, atol=2e-7)
        if name == "snc":
            expected = raw.fld_s03i317.where(raw.fld_s08i236 > 0, 0).sum(tile_dim)
        else:
            expected = (sum(raw[f] for f in fields) * raw.fld_s03i317).sum(tile_dim)
        expected = (expected * conversion).where(fraction > 0).load()
        if whole_cell:
            expected = (expected * fraction).fillna(0)
        # Independently derive areacella from native bounds, using MOPPy's
        # spherical radius. sftlf = 100 * native land fraction.
        lat_bounds = np.deg2rad(raw.lat_bnds)
        lon_bounds = np.deg2rad(raw.lon_bnds)
        area = (
            6371000.0**2
            * abs(np.sin(lat_bounds.isel(bnds=1)) - np.sin(lat_bounds.isel(bnds=0)))
            * abs(lon_bounds.isel(bnds=1) - lon_bounds.isel(bnds=0))
        ).load()
        land_area = area * fraction

    parent = {
        **parent_experiment_config,
        "parent_source_id": "ACCESS-ESM1-6",
        "parent_mip_era": version,
    }
    with ACCESS_ESM_CMORiser(
        input_data=paths,
        compound_name=request,
        cmip_version=version,
        source_id="ACCESS-ESM1-6",
        experiment_id="historical",
        variant_label="r1i1p1f1",
        parent_info=parent,
        output_path=tmp_path,
        split_years=1,
    ) as cmoriser:
        cmoriser.run()
        cmoriser.write()

    outputs = list(tmp_path.rglob("*.nc"))
    assert len(outputs) == 1
    with xr.open_dataset(outputs[0]) as output:
        actual = output[name].load()
        assert actual.dims == expected.dims
        assert actual.shape == expected.shape
        for dim in expected.dims:
            np.testing.assert_array_equal(actual[dim].values, expected[dim].values)
        if whole_cell:
            assert "where land" not in actual.attrs["cell_methods"]
        else:
            assert "area: mean where land" in actual.attrs["cell_methods"]
            assert output.attrs["area_label"] == "lnd"
        np.testing.assert_allclose(actual, expected, rtol=1e-6, atol=0, equal_nan=True)
        for mask in (coast, pure_land):
            np.testing.assert_allclose(
                actual.values[mask.values],
                expected.values[mask.values],
                rtol=1e-6,
                atol=0,
            )
        if whole_cell:
            assert (actual.values[ocean.values] == 0).all()
        else:
            assert np.isnan(actual.values[ocean.values]).all()
        integration_area = area if whole_cell else land_area
        actual_total = (actual * integration_area).sum(["lat", "lon"])
        expected_total = (expected * integration_area).sum(["lat", "lon"])
        np.testing.assert_allclose(actual_total, expected_total, rtol=1e-6, atol=0)

    record_property("native_files", ";".join(map(str, paths)))
    record_property("coastal_cells_per_month", int(coast.isel(time=0).sum()))
    record_property("first_month_area_integral", float(actual_total.isel(time=0)))
