"""Native grid time bounds follow the selected CMOR table frequency."""

import numpy as np
import pytest
import xarray as xr

from access_moppy.driver import ACCESS_ESM_CMORiser
from access_moppy.native_time import normalise_explicit_time_bounds
from tests.unit.test_project_cmor_equivalence import (
    _cice_inputs,
    _mom6_3d_inputs,
    _mom6_u_inputs,
)


def _driver(tmp_path, project_cv, native, grid, compound, **options):
    return ACCESS_ESM_CMORiser(
        native,
        compound_name=compound,
        experiment_id="om3-real-run",
        source_id="ACCESS-OM3",
        variant_label="r1i1p1f1",
        activity_id="ACCESS-DEV",
        model_id="ACCESS-OM3",
        cmip_version="CMIP6",
        project_cv=project_cv,
        grid_data=grid,
        output_path=tmp_path,
        enable_resampling=False,
        split_years=None,
        **options,
    )


def _daily_bounds():
    return np.array(
        [["2000-01-01", "2000-01-02"], ["2000-01-02", "2000-01-03"]],
        dtype="datetime64[ns]",
    )


@pytest.mark.unit
def test_explicit_mom6_daily_table_accepts_complete_days(tmp_path, project_cv):
    _, grid, supergrid = _mom6_3d_inputs()
    bounds = _daily_bounds()
    native = xr.Dataset(
        {
            "tos": (("time", "yh", "xh"), np.ones((2, 2, 2)), {"units": "degC"}),
            "time_bnds": (("time", "bnds"), bounds),
        },
        coords={"time": bounds[:, 1], "yh": [-70.0, -60.0], "xh": [0.0, 1.0]},
    )
    native.time.attrs["bounds"] = "time_bnds"
    native.time.encoding.update(units="days since 1850-01-01", calendar="standard")
    with _driver(
        tmp_path, project_cv, native, grid, "Oday.tos", supergrid_data=supergrid
    ) as driver:
        driver.run()
        result = driver.to_dataset()
        assert result.time.dt.hour.values.tolist() == [12, 12]
        np.testing.assert_array_equal(result.time_bnds.values, bounds)


@pytest.mark.unit
def test_explicit_mom6_reversed_months_sort_before_interval_validation(
    tmp_path, project_cv
):
    native, grid, supergrid = _mom6_u_inputs()
    native = native.isel(time=[1, 0])
    with _driver(
        tmp_path, project_cv, native, grid, "Omon.tauuo", supergrid_data=supergrid
    ) as driver:
        driver.run()
        result = driver.to_dataset()
        assert result.time.dt.month.values.tolist() == [1, 2]
        np.testing.assert_allclose(result.tauuo.values[:, 0, 0], [1.0, 2.0])


@pytest.mark.unit
def test_explicit_cice_daily_table_accepts_complete_days(tmp_path, project_cv):
    native, grid = _cice_inputs()
    bounds = _daily_bounds()
    native = native.assign_coords(time=("time", bounds[:, 1]))
    native["time_bounds"] = (("time", "d2"), bounds)
    native.time.attrs["bounds"] = "time_bounds"
    native.time.encoding.update(units="days since 1850-01-01", calendar="standard")
    with _driver(
        tmp_path,
        project_cv,
        native,
        grid,
        "SIday.siconc",
        grid_identity_verified=True,
        grid_reference="static-grid@sha256:test",
    ) as driver:
        driver.run()
        result = driver.to_dataset()
        assert result.time.dt.hour.values.tolist() == [12, 12]
        np.testing.assert_array_equal(result.time_bnds.values, bounds)


@pytest.mark.unit
@pytest.mark.parametrize("calendar", ["noleap", "360_day"])
def test_explicit_daily_bounds_respect_cf_calendars(calendar):
    bounds = xr.DataArray([[0.0, 24.0], [24.0, 48.0]], dims=("time", "bnds"))
    time = xr.DataArray(
        [24.0, 48.0],
        dims="time",
        attrs={"bounds": "time_bnds"},
    )
    time.encoding.update(units="hours since 2001-01-01", calendar=calendar)
    source = xr.Dataset({"time_bnds": bounds}, coords={"time": time})
    result = normalise_explicit_time_bounds(
        source, model="MOM6", frequency="day", fallback_name="time_bnds"
    )
    np.testing.assert_allclose(result.time.values, [12.0, 36.0])
    assert result.time.encoding["calendar"] == calendar


@pytest.mark.unit
@pytest.mark.parametrize(
    "intervals,match",
    [
        (
            [["2000-01-01", "2000-01-03"], ["2000-01-03", "2000-01-04"]],
            "complete calendar days",
        ),
        (
            [["2000-01-01T12", "2000-01-02T12"], ["2000-01-02T12", "2000-01-03T12"]],
            "complete calendar days",
        ),
        (
            [["2000-01-01", "2000-01-02"], ["2000-01-03", "2000-01-04"]],
            "contiguous",
        ),
    ],
)
def test_explicit_daily_bounds_reject_partial_days_and_gaps(intervals, match):
    bounds = np.asarray(intervals, dtype="datetime64[ns]")
    source = xr.Dataset(
        {"time_bnds": (("time", "bnds"), bounds)},
        coords={"time": bounds[:, 1]},
    )
    source.time.attrs["bounds"] = "time_bnds"
    with pytest.raises(ValueError, match=match):
        normalise_explicit_time_bounds(
            source, model="MOM6", frequency="day", fallback_name="time_bnds"
        )


@pytest.mark.unit
def test_fixed_explicit_grid_does_not_require_time_bounds():
    source = xr.Dataset({"areacello": (("j", "i"), [[2.0]])})
    result = normalise_explicit_time_bounds(source, model="MOM6", frequency="fx")
    xr.testing.assert_identical(result, source)
