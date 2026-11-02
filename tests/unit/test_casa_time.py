"""CASA daily sampling represents calendar-month means in either file layout."""

from unittest.mock import Mock

import cftime
import numpy as np
import pytest
import xarray as xr

from access_moppy.base import CMORiser, _preprocess_open_mfdataset
from access_moppy.casa_time import (
    CASA_MONTHLY_CONVENTION,
    CasaTimeConventionError,
    normalise_casa_monthly_time,
)

pytestmark = pytest.mark.unit
REQUIRED = frozenset({"fld_s03i919", "fld_s03i317", "fld_s03i395", "time", "time_bnds"})


def nitrogen_months(
    calendar="proleptic_gregorian", units="days since 0001-01-01", year=101
):
    edges = cftime.date2num(
        [cftime.datetime(year, month, 1, calendar=calendar) for month in (1, 2, 3)],
        units,
        calendar,
    )
    bounds = np.column_stack((edges[:-1], edges[1:])).astype(np.float64)
    offset = (
        cftime.date2num(
            cftime.datetime(year, 1, 1, 23, 30, calendar=calendar), units, calendar
        )
        - edges[0]
    )
    sampled = bounds.copy()
    sampled[:, 0] += offset
    flux = np.array([[[1.0, np.nan], [3.0, 4.0]], [[5.0, 6.0], [7.0, 8.0]]], dtype="f4")
    ds = xr.Dataset(
        {
            "fld_s03i919": (
                ("time", "tile", "lat"),
                flux,
                {"cell_methods": "time: mean"},
            ),
            "time_bnds": (("time", "bnds"), sampled),
        },
        coords={
            "time": (
                "time",
                sampled.mean(axis=1),
                {"units": units, "calendar": calendar, "bounds": "time_bnds"},
            )
        },
    )
    ds.encoding["source"] = "native-nitrogen.nc"
    ds.time.encoding["dtype"] = "f8"
    return ds, bounds


@pytest.mark.parametrize(
    "calendar,units,year",
    [
        ("proleptic_gregorian", "days since 0001-01-01", 101),
        ("proleptic_gregorian", "hours since 2000-01-01", 2000),
        ("360_day", "seconds since 2001-01-01", 2001),
    ],
)
def test_sampling_convention_preserves_values_masks_source_and_encoding(
    calendar, units, year, caplog
):
    ds, bounds = nitrogen_months(calendar, units, year)
    before = ds.copy(deep=True)
    result = normalise_casa_monthly_time(ds, required_vars=REQUIRED)
    np.testing.assert_array_equal(result.time_bnds, bounds)
    np.testing.assert_array_equal(result.time, bounds.mean(axis=1))
    np.testing.assert_array_equal(result.fld_s03i919.values, ds.fld_s03i919.values)
    xr.testing.assert_identical(ds, before)
    assert result.time.encoding == ds.time.encoding
    assert "23.5 h" in caplog.text and "native-nitrogen.nc" in caplog.text
    assert "https://github.com/ACCESS-NRI/UM7/pull/192" in caplog.text


def test_existing_calendar_month_means_remain_identical(caplog):
    ds, bounds = nitrogen_months()
    ds["time_bnds"] = (("time", "bnds"), bounds)
    ds = ds.assign_coords(time=ds.time.copy(data=bounds.mean(axis=1)))
    xr.testing.assert_identical(
        normalise_casa_monthly_time(ds, required_vars=REQUIRED), ds
    )
    assert not caplog.records


@pytest.mark.parametrize(
    "defect,match",
    [
        ("offset", "23.5-hour"),
        ("end", "23.5-hour"),
        ("nonfinite", "finite"),
        ("midpoint", "midpoints"),
        ("no_bounds", "missing declared"),
        ("no_units", "units"),
        ("not_mean", "declare a time mean"),
        ("duplicate_month", "more than one"),
        ("reverse", "strictly increasing"),
        ("mixed", "23.5-hour"),
        ("other_field", "unrecognised selected"),
        ("no_coordinate", "missing time coordinate"),
        ("bounds_shape", "two bounds per time sample"),
        ("empty", "empty time axis"),
        ("invalid_units", "cannot decode monthly interval"),
    ],
)
def test_rejects_unestablished_sampling_conventions(defect, match):
    ds, bounds = nitrogen_months()
    required = REQUIRED
    if defect == "offset":
        ds.time_bnds.values[:, 0] += 1 / 48
    elif defect == "end":
        ds.time_bnds.values[:, 1] -= 1
    elif defect == "nonfinite":
        ds.time_bnds.values[0, 0] = np.nan
    elif defect == "midpoint":
        ds = ds.assign_coords(time=ds.time.copy(data=ds.time.values + 0.1))
    elif defect == "no_bounds":
        ds = ds.drop_vars("time_bnds")
    elif defect == "no_units":
        del ds.time.attrs["units"]
    elif defect == "not_mean":
        ds.fld_s03i919.attrs["cell_methods"] = "time: point"
    elif defect == "duplicate_month":
        ds = ds.isel(time=[0, 0])
    elif defect == "reverse":
        ds = ds.isel(time=[1, 0])
    elif defect == "mixed":
        ds.time_bnds.values[0] = bounds[0]
    elif defect == "other_field":
        ds["other"] = ("time", [1.0, 2.0])
        required = REQUIRED | {"other"}
    elif defect == "no_coordinate":
        ds = ds.drop_vars("time")
    elif defect == "bounds_shape":
        ds = ds.isel(bnds=[0])
    elif defect == "empty":
        ds = ds.isel(time=slice(0, 0))
    elif defect == "invalid_units":
        ds.time.attrs["units"] = "not a CF time unit"
    if defect in {"offset", "end", "mixed"}:
        ds = ds.assign_coords(time=ds.time.copy(data=ds.time_bnds.values.mean(axis=1)))
    before = ds.copy(deep=True)
    with pytest.raises(CasaTimeConventionError, match=match):
        normalise_casa_monthly_time(ds, required_vars=required)
    xr.testing.assert_identical(ds, before)


def test_unrelated_diagnostics_and_non_opted_in_inputs_are_unchanged():
    ds, _ = nitrogen_months()
    unrelated = ds.rename(fld_s03i919="fld_s03i884")
    xr.testing.assert_identical(
        normalise_casa_monthly_time(
            unrelated, required_vars=REQUIRED | {"fld_s03i884"}
        ),
        unrelated,
    )
    xr.testing.assert_identical(
        _preprocess_open_mfdataset(ds, required_vars=REQUIRED), ds
    )


def write_layout(tmp_path, layout):
    flux, bounds = nitrogen_months()
    weights = xr.Dataset(
        {
            "fld_s03i317": (("time", "tile", "lat"), np.full((2, 2, 2), 0.5)),
            "fld_s03i395": (("time", "lat"), np.ones((2, 2))),
            "time_bnds": (("time", "bnds"), bounds),
        },
        coords={"time": ("time", bounds.mean(axis=1), flux.time.attrs)},
    )
    paths = []
    if layout == "packed":
        flux = flux.rename(time="time_0", time_bnds="time_0_bnds")
        flux.time_0.attrs["bounds"] = "time_0_bnds"
        flux.fld_s03i919.attrs["cell_methods"] = "time_0: mean"
        combined = xr.merge([flux, weights])
        datasets = [
            combined.isel(time=slice(i, i + 1), time_0=slice(i, i + 1))
            for i in range(2)
        ]
    else:
        datasets = [
            flux,
            weights[["fld_s03i317", "time_bnds"]],
            weights[["fld_s03i395", "time_bnds"]],
        ]
    for i, ds in enumerate(datasets):
        path = tmp_path / f"{layout}-{i}.nc"
        ds.to_netcdf(path)
        paths.append(str(path))
    return paths, bounds


def test_packed_and_split_files_load_the_same_months_and_masks(tmp_path):
    loaded = []
    for layout in ("packed", "split"):
        paths, bounds = write_layout(tmp_path, layout)
        cmoriser = CMORiser(
            input_data=paths,
            output_path=str(tmp_path),
            vocab=Mock(),
            variable_mapping={
                "fNup": {"input_time_convention": CASA_MONTHLY_CONVENTION}
            },
            compound_name="Emon.fNup",
            enable_chunking=False,
        )
        cmoriser.load_dataset(required_vars=list(REQUIRED))
        loaded.append(cmoriser.ds.load())
        assert dict(cmoriser.ds.sizes) == {"time": 2, "tile": 2, "lat": 2, "bnds": 2}
        np.testing.assert_array_equal(cmoriser.ds.time_bnds, bounds)
        assert np.isnan(cmoriser.ds.fld_s03i919).sum() == 1
        cmoriser.ds.close()
    # The native cell_methods spelling differs with its dimension name.
    for name in REQUIRED - {"time"}:
        np.testing.assert_array_equal(loaded[0][name], loaded[1][name])
    np.testing.assert_array_equal(loaded[0].time, loaded[1].time)


def test_failed_convention_does_not_fall_back_to_nested_concat(tmp_path, caplog):
    paths, _ = write_layout(tmp_path, "split")
    with xr.open_dataset(paths[0], decode_cf=False) as source:
        bad = source.load()
    bad.time_bnds.values[0, 0] += 1
    bad.to_netcdf(paths[0], mode="w")
    cmoriser = CMORiser(
        input_data=paths,
        output_path=str(tmp_path),
        vocab=Mock(),
        variable_mapping={"fNup": {"input_time_convention": CASA_MONTHLY_CONVENTION}},
        compound_name="Emon.fNup",
        enable_chunking=False,
    )
    with pytest.raises(CasaTimeConventionError):
        cmoriser.load_dataset(required_vars=list(REQUIRED))
    assert "falling back" not in caplog.text
