"""Numerical checks of #803 recipes against native ACCESS data and written files."""

import os
from pathlib import Path

import numpy as np
import pytest
import xarray as xr

from access_moppy.derivations import evaluate_expression
from access_moppy.driver import ACCESS_ESM_CMORiser
from access_moppy.file_discovery import _extract_year_from_path, discover_files
from access_moppy.utilities import load_cmip6_to_cmip7_mapping, load_model_mappings


@pytest.fixture
def native_archive():
    root = os.environ.get("ACCESS_MOPPY_DATA_ROOT")
    if not root or not Path(root).is_dir():
        pytest.skip("Set ACCESS_MOPPY_DATA_ROOT to an ACCESS-ESM1-6 archive")
    return Path(root)


@pytest.mark.integration
@pytest.mark.parametrize("product", ["Eday.clt", "LImon.sbl", "Amon.sbl"])
@pytest.mark.parametrize("version", ["CMIP6", "CMIP7"])
def test_written_land_domains(
    native_archive, product, version, tmp_path, parent_experiment_config
):
    paths = discover_files(native_archive, product, model_id="ACCESS-ESM1-6")
    assert paths
    year = _extract_year_from_path(paths[0])
    paths = discover_files(
        native_archive,
        product,
        model_id="ACCESS-ESM1-6",
        start_year=year,
        end_year=year,
    )
    fields = {
        "Eday.clt": ["fld_s02i204", "fld_s03i317"],
        "LImon.sbl": ["fld_s03i331", "fld_s03i317", "fld_s08i236"],
        "Amon.sbl": ["fld_s03i298"],
    }[product]
    with xr.open_mfdataset(
        paths,
        engine="netcdf4",
        preprocess=lambda ds: ds[[v for v in fields if v in ds]],
    ) as raw:
        if product != "Amon.sbl":
            tiles = raw.fld_s03i317
            tile_dim = next(dim for dim in tiles.dims if "pseudo_level" in dim)
            total = tiles.sum(tile_dim)
        if product == "Amon.sbl":
            expected = raw.fld_s03i298.load()
        elif product == "Eday.clt":
            expected = (raw.fld_s02i204 * 100).where(total > 0).load()
        else:
            snow = (raw.fld_s08i236 * tiles).sum(tile_dim)
            expected = (
                ((raw.fld_s03i331 * tiles).sum(tile_dim) / total)
                .where((total > 0) & (snow > 0))
                .load()
            )
        assert expected.notnull().any()
        if product != "Amon.sbl":
            assert expected.isnull().any()

    name = product.split(".")[1]
    request = product if version == "CMIP6" else load_cmip6_to_cmip7_mapping()[product]
    parent = {
        **parent_experiment_config,
        "parent_source_id": "ACCESS-ESM1-6",
        "parent_mip_era": version,
    }
    with ACCESS_ESM_CMORiser(
        input_folder=native_archive,
        start_year=year,
        end_year=year,
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
        np.testing.assert_allclose(output[name].values, expected.values, rtol=1e-6)
        assert ("where land" in output[name].attrs["cell_methods"]) == (
            product != "Amon.sbl"
        )
        if product == "LImon.sbl":
            assert output.attrs["realm"] == "landIce"
            assert output.attrs["nominal_resolution"] == "250 km"
        if version == "CMIP7":
            assert output.attrs["area_label"] == (
                "u" if product == "Amon.sbl" else "lnd"
            )


@pytest.mark.integration
def test_native_evaporation_tile_budget(native_archive):
    """The archived monthly tiles verify the budget used by the daily recipe.

    This does not pretend monthly inputs can supply daily evaporation.
    On pure land their total must equal native total moisture flux; at coasts
    the land value must differ from the mixed land/ocean diagnostic.
    """
    path = discover_files(native_archive, "Amon.evspsbl", model_id="ACCESS-ESM1-6")[0]
    mapping = load_model_mappings("Eday.evspsbl", "ACCESS-ESM1-6")["evspsbl"]
    with xr.open_dataset(path) as raw:
        result = evaluate_expression(mapping["calculation"], raw)
        fraction = raw.fld_s03i395
        pure_land = fraction == 1
        coast = (fraction > 0) & (fraction < 1)
        assert pure_land.any() and coast.any()
        np.testing.assert_allclose(
            result.where(pure_land),
            raw.fld_s03i223.where(pure_land),
            rtol=2e-5,
            atol=2e-11,
        )
        assert result.where(fraction == 0).isnull().all()
        assert float(abs(result - raw.fld_s03i223).where(coast).max()) > 1e-6


@pytest.mark.integration
@pytest.mark.parametrize("product,step", [("Eday.evspsbl", 1.0), ("3hr.tran", 0.125)])
def test_required_cadence_inputs_write_correct_flux(
    product, step, tmp_path, parent_experiment_config
):
    """Synthetic daily/3-hourly inputs exercise the complete writing path."""
    coords = {
        "time": (
            "time",
            [step / 2, 3 * step / 2],
            {
                "units": "days since 1951-01-01",
                "calendar": "365_day",
                "bounds": "time_bnds",
            },
        ),
        "lat": ("lat", [-30.0, 30.0], {"units": "degrees_north"}),
        "lon": ("lon", [0.0, 90.0, 180.0], {"units": "degrees_east"}),
        "pseudo_level": [1, 2],
    }
    tiles = np.broadcast_to(
        np.array([[0, 0.25, 0.25], [0, 0.75, 0.75]])[None, :, None, :], (2, 2, 2, 3)
    )
    flux = np.broadcast_to(np.array([5e-6, 1e-6])[None, :, None, None], tiles.shape)
    dims = ("time", "pseudo_level", "lat", "lon")
    raw = xr.Dataset(
        {
            "fld_s03i317": (dims, tiles, {"units": "1"}),
            "fld_s03i174": (dims, flux, {"units": "kg m-2 s-1"}),
            "fld_s03i287": (dims, np.full_like(flux, 1e-6), {"units": "kg m-2 s-1"}),
            "fld_s03i288": (dims, flux, {"units": "kg m-2 s-1"}),
            "fld_s03i331": (dims, flux, {"units": "kg m-2 s-1"}),
            "fld_s03i395": (
                ("time", "lat", "lon"),
                np.broadcast_to([0, 0.25, 1], (2, 2, 3)),
                {"units": "1"},
            ),
            "time_bnds": (("time", "bnds"), [[0, step], [step, 2 * step]]),
        },
        coords=coords,
    )
    parent = {
        **parent_experiment_config,
        "parent_source_id": "ACCESS-ESM1-6",
        "parent_mip_era": "CMIP7",
    }
    with ACCESS_ESM_CMORiser(
        input_data=raw,
        compound_name=load_cmip6_to_cmip7_mapping()[product],
        cmip_version="CMIP7",
        source_id="ACCESS-ESM1-6",
        experiment_id="historical",
        variant_label="r1i1p1f1",
        parent_info=parent,
        output_path=tmp_path,
    ) as cmoriser:
        cmoriser.run()
        cmoriser.write()
    outputs = list(tmp_path.rglob("*.nc"))
    assert len(outputs) == 1
    name = product.split(".")[1]
    expected = [np.nan, 5e-6, 5e-6] if name == "evspsbl" else [0, 0.5e-6, 2e-6]
    with xr.open_dataset(outputs[0]) as output:
        np.testing.assert_allclose(
            output[name], np.broadcast_to(expected, (2, 2, 3)), rtol=1e-6
        )
        assert output.attrs["area_label"] == ("lnd" if name == "evspsbl" else "u")
