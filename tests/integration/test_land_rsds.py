"""Check written land-only rsds against native ACCESS data, not just metadata."""

import os
from pathlib import Path

import numpy as np
import pytest
import xarray as xr

from access_moppy.driver import ACCESS_ESM_CMORiser
from access_moppy.file_discovery import _extract_year_from_path, discover_files


@pytest.mark.integration
@pytest.mark.parametrize(
    "compound_name", ["Emon.rsds", "land.rsds.tavg-u-hxy-lnd.mon.glb"]
)
def test_land_rsds_written_values(compound_name, tmp_path, parent_experiment_config):
    root = os.environ.get("ACCESS_MOPPY_DATA_ROOT")
    if not root or not Path(root).is_dir():
        pytest.skip("Set ACCESS_MOPPY_DATA_ROOT to an ACCESS-ESM1-6 archive")
    available = discover_files(root, "Emon.rsds", model_id="ACCESS-ESM1-6")
    if not available:
        pytest.skip("No monthly atmosphere inputs available")
    year = _extract_year_from_path(available[0])
    assert year is not None
    paths = discover_files(
        root, "Emon.rsds", model_id="ACCESS-ESM1-6", start_year=year, end_year=year
    )
    native_fields = ("fld_s01i235", "fld_s03i395")
    with xr.open_mfdataset(
        paths,
        engine="netcdf4",
        preprocess=lambda ds: ds[[name for name in native_fields if name in ds]],
    ) as raw:
        flux = raw["fld_s01i235"].load()
        fraction = raw["fld_s03i395"].broadcast_like(flux).load()
    assert flux.dims == ("time", "lat", "lon")
    ocean = fraction.values == 0
    coast = (fraction.values > 0) & (fraction.values < 1)
    land = fraction.values > 0
    assert ocean.any() and coast.any() and (fraction.values == 1).any()
    parent = {
        **parent_experiment_config,
        "parent_source_id": "ACCESS-ESM1-6",
        "parent_mip_era": "CMIP7",
    }
    with ACCESS_ESM_CMORiser(
        input_folder=root,
        start_year=year,
        end_year=year,
        compound_name=compound_name,
        cmip_version="CMIP7",
        source_id="ACCESS-ESM1-6",
        experiment_id="historical",
        variant_label="r1i1p1f1",
        parent_info=parent,
        output_path=tmp_path,
        split_years=1,
    ) as cmoriser:
        cmoriser.run()
        cmoriser.write()
    outputs = sorted(tmp_path.rglob("*.nc"))
    assert len(outputs) == 1
    with xr.open_dataset(outputs[0]) as output:
        result = output["rsds"]
        np.testing.assert_array_equal(result.lat, flux.lat)
        np.testing.assert_array_equal(result.lon, flux.lon)
        assert result.shape == flux.shape
        assert np.isnan(result.values[ocean]).all()
        np.testing.assert_allclose(result.values[land], flux.values[land], rtol=1e-6)
        np.testing.assert_allclose(result.values[coast], flux.values[coast], rtol=1e-6)
        assert output.attrs["realm"] == "land"
        assert output.attrs["area_label"] == "lnd"
        assert "where land" in result.attrs["cell_methods"]
