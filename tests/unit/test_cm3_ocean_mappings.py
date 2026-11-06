"""CM3 archive discovery and physical ocean CMORisation contracts."""

import numpy as np
import pytest

from access_moppy.driver import ACCESS_ESM_CMORiser
from access_moppy.file_discovery import FileDiscoveryError, discover_files
from access_moppy.utilities import load_model_mappings
from tests.unit.test_project_cmor_equivalence import _mom6_3d_inputs, _project_cv


@pytest.mark.parametrize(
    "name", ["thetao", "so", "uo", "vo", "volcello", "tauuo", "hfds"]
)
def test_cm3_archive_discovery_selects_requested_year_and_field(tmp_path, name):
    source = "thkcello" if name == "volcello" else name
    rank, vertical = ("2d", "") if name in {"tauuo", "hfds"} else ("3d", ".z")
    expected = None
    for year in (1985, 1986, 1987):
        directory = tmp_path / "archive" / str(year) / "ocean"
        directory.mkdir(parents=True)
        path = (
            directory / f"access-cm3.mom6.{rank}.{source}{vertical}.1mon.mean.{year}.nc"
        )
        path.touch()
        (directory / f"access-cm3.mom6.2d.unrelated.1mon.mean.{year}.nc").touch()
        if year == 1986:
            expected = path
    assert discover_files(
        tmp_path, f"Omon.{name}", model_id="ACCESS-CM3", start_year=1986, end_year=1986
    ) == [expected]


def test_cm3_new_mapping_products_do_not_invent_bgc_or_frequency_support():
    for product in ("Omon.fgco2", "Omon.phos", "Oday.thetao", "Ofx.volcello"):
        assert load_model_mappings(product, "ACCESS-CM3") == {}


@pytest.mark.parametrize(
    "name,source_units,value,expected",
    [
        ("thetao", "degC", 3.5, 3.5),
        ("so", "psu", 34.6, 34.6),
        ("volcello", "m", 10.0, 20.0),
    ],
)
def test_cm3_tracer_products_keep_identity_geometry_and_values(
    tmp_path, name, source_units, value, expected
):
    """Run the actual driver and compare its lazy Iris cube with written CMOR data."""
    import iris

    native, grid, supergrid = _mom6_3d_inputs()
    source_name = "thkcello" if name == "volcello" else name
    native = native.rename({"thetao": source_name})
    native[source_name] = native[source_name] * value
    native[source_name].attrs["units"] = source_units
    native = native.chunk({"time": 1})
    cv = _project_cv()
    cv["source_id"]["ACCESS-CM3"] = cv["source_id"].pop("ACCESS-OM3")
    cv["source_id"]["ACCESS-CM3"]["label"] = "ACCESS-CM3 development configuration"
    cv["experiment_id"]["cm3-pd-control"] = cv["experiment_id"].pop("om3-real-run")
    cv["experiment_id"]["cm3-pd-control"]["experiment"] = "Present-day control"
    driver = ACCESS_ESM_CMORiser(
        native,
        compound_name=f"Omon.{name}",
        source_id="ACCESS-CM3",
        model_id="ACCESS-CM3",
        experiment_id="cm3-pd-control",
        variant_label="r1i1p1f1",
        activity_id="ACCESS-DEV",
        cmip_version="CMIP6",
        project_cv=cv,
        grid_data=grid,
        supergrid_data=supergrid,
        output_path=tmp_path,
        enable_resampling=False,
        split_years=None,
    )
    try:
        driver.run()
        cube = driver.to_iris()
        assert cube.has_lazy_data()
        assert cube.attributes["source_id"] == "ACCESS-CM3"
        assert cube.attributes["experiment_id"] == "cm3-pd-control"
        assert (
            "not a CMIP publication"
            in cube.attributes["access_moppy_publication_status"]
        )
        np.testing.assert_allclose(cube.data.compressed(), expected)
        assert np.ma.getmaskarray(cube.data)[..., 1, 1].all()
        np.testing.assert_array_equal(cube.coord("depth").bounds, [[0, 1], [1, 2]])
        driver.write()
        assert len(driver.cmoriser.written_files) == 1
        saved = iris.load_cube(str(driver.cmoriser.written_files[0]))
        assert saved.units == cube.units
        np.testing.assert_array_equal(
            np.ma.getmaskarray(saved.data), np.ma.getmaskarray(cube.data)
        )
        np.testing.assert_allclose(saved.data.compressed(), cube.data.compressed())
        for coordinate in ("time", "depth", "latitude", "longitude"):
            np.testing.assert_array_equal(
                saved.coord(coordinate).bounds, cube.coord(coordinate).bounds
            )
    finally:
        driver.close()


def test_cm3_legacy_daily_mapping_does_not_discover_monthly_files(tmp_path):
    # Existing direct-file mappings stay valid; archive discovery may only
    # choose a source frequency explicitly represented in its configuration.
    with pytest.raises(FileDiscoveryError, match="No pattern for frequency 'day'"):
        discover_files(tmp_path, "Oday.so", model_id="ACCESS-CM3")
