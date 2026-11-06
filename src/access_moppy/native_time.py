"""Time-bound checks shared by native MOM6 and CICE CMORisation."""

from datetime import timedelta

import numpy as np
import xarray as xr


def _validate_interval_structure(values, time, model):
    if values.shape != (time.size, 2) or not np.all(values[:, 0] < values[:, 1]):
        raise ValueError(f"Native {model} time bounds require increasing intervals")
    if len(values) > 1 and not np.all(values[:-1, 1] == values[1:, 0]):
        raise ValueError(f"Native {model} time bounds must be contiguous")


def _decode_bound_dates(values, time, model):
    if np.issubdtype(values.dtype, np.number):
        import cftime

        units = time.attrs.get("units") or time.encoding.get("units")
        calendar = time.attrs.get("calendar") or time.encoding.get(
            "calendar", "standard"
        )
        if not units:
            raise ValueError(f"Numeric {model} time bounds require CF time units")
        dates = cftime.num2date(values, units=units, calendar=calendar)
    else:
        dates = values
    return dates


def _are_midnight_boundaries(starts, ends):
    return all(
        np.all(getattr(axis.dt, part).values == expected)
        for axis in (starts, ends)
        for part, expected in (
            ("hour", 0),
            ("minute", 0),
            ("second", 0),
            ("microsecond", 0),
        )
    )


def validate_calendar_month_bounds(values, time, model):
    """Require contiguous, complete calendar months in chronological order."""
    _validate_interval_structure(values, time, model)
    dates = _decode_bound_dates(values, time, model)
    starts = xr.DataArray(dates[:, 0], dims="time")
    ends = xr.DataArray(dates[:, 1], dims="time")
    month_ids = starts.dt.year * 12 + starts.dt.month
    end_ids = ends.dt.year * 12 + ends.dt.month
    month_starts = (
        np.all(starts.dt.day.values == 1)
        and np.all(ends.dt.day.values == 1)
        and _are_midnight_boundaries(starts, ends)
    )
    if not np.all((end_ids - month_ids).values == 1) or not month_starts:
        raise ValueError(
            f"Native {model} monthly time bounds must span complete calendar months"
        )


def _validate_calendar_day_bounds(values, time, model):
    _validate_interval_structure(values, time, model)
    dates = _decode_bound_dates(values, time, model)
    starts = xr.DataArray(dates[:, 0], dims="time")
    ends = xr.DataArray(dates[:, 1], dims="time")
    durations = (ends - starts).values
    one_day = (
        np.timedelta64(1, "D")
        if np.issubdtype(durations.dtype, np.timedelta64)
        else timedelta(days=1)
    )
    full_day = np.all(durations == one_day)
    if not full_day or not _are_midnight_boundaries(starts, ends):
        raise ValueError(
            f"Native {model} daily time bounds must span complete calendar days"
        )


def normalise_explicit_time_bounds(
    dataset, *, model, frequency, fallback_name=None, rename_to=None
):
    """Place native daily/monthly means at interval midpoints.

    The caller must sort and validate the time axis first. Fixed fields have no
    time axis; other frequencies retain their existing component behaviour.
    """
    if "time" not in dataset.dims or frequency not in {"mon", "day"}:
        return dataset
    time = dataset["time"]
    name = time.attrs.get("bounds") or time.encoding.get("bounds") or fallback_name
    if not name or name not in dataset:
        raise ValueError(
            f"Native {model} {frequency} input requires explicit time bounds"
        )
    bounds = dataset[name]
    extra = [dim for dim in bounds.dims if dim != "time"]
    if bounds.ndim != 2 or len(extra) != 1 or bounds.sizes[extra[0]] != 2:
        raise ValueError(f"Native {model} time bounds require two endpoints")
    bounds = bounds.transpose("time", extra[0])
    values = bounds.values
    if frequency == "mon":
        validate_calendar_month_bounds(values, time, model)
    else:
        _validate_calendar_day_bounds(values, time, model)
    midpoint = (
        bounds.isel({extra[0]: 0}, drop=True)
        + (
            bounds.isel({extra[0]: 1}, drop=True)
            - bounds.isel({extra[0]: 0}, drop=True)
        )
        / 2
    )
    attrs, encoding = dict(time.attrs), dict(time.encoding)
    result = dataset.assign_coords(time=("time", midpoint.values))
    result["time"].attrs = attrs
    result["time"].encoding = encoding
    if rename_to is not None:
        renames = {}
        if name != rename_to:
            renames[name] = rename_to
        if extra[0] != "bnds":
            renames[extra[0]] = "bnds"
        if renames:
            result = result.rename(renames)
    result["time"].attrs["bounds"] = rename_to or name
    return result
