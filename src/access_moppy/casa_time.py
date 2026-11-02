"""The ACCESS-ESM1.6 daily-sampled CASA nitrogen monthly-mean convention.

UM7 PR #192 (merge b5f58fcf8487c177b0d75878bcf015102a90dc7c) samples these
diagnostics once at the end of each day. Its developers verified the monthly
means against averages of daily output. The converted lower bound is the start
of the first sampling timestep (23:30), not the start of the represented month.
See https://github.com/ACCESS-NRI/UM7/pull/192 and ACCESS-MOPPy issue #816.
Configuration adoption (STASHmaster_A dated 2025-12-19):
https://github.com/ACCESS-NRI/access-esm1.6-configs/issues/63#issuecomment-3672831133
"""

import logging
import re
from datetime import timedelta

import cftime
import numpy as np
import xarray as xr

logger = logging.getLogger(__name__)

CASA_MONTHLY_CONVENTION = "access_esm1p6_casa_daily_monthly"
CASA_NITROGEN_FIELDS = frozenset(f"fld_s03i{number}" for number in range(917, 921))


class CasaTimeConventionError(ValueError):
    """The input does not establish the explicitly requested CASA convention."""


def normalise_casa_monthly_time(
    ds: xr.Dataset, *, required_vars: frozenset[str]
) -> xr.Dataset:
    """Express verified daily-sampled monthly means on calendar-month bounds.

    Call only for mappings explicitly opting into the ACCESS-ESM1.6 convention.
    Data values/masks are untouched. Neither missing months nor invalid bounds
    are repaired. The checks run before xarray can outer-join different axes.
    """
    selected = required_vars & CASA_NITROGEN_FIELDS & set(ds.data_vars)
    time_dims = {
        dim for name in selected for dim in ds[name].dims if dim.startswith("time")
    }
    if not selected:
        return ds  # A split file containing only the tile/land fractions.
    result = ds.copy(deep=False)
    for dim in sorted(time_dims):
        fields = sorted(name for name in selected if dim in ds[name].dims)
        context = f"CASA monthly convention for {', '.join(fields)} on {dim!r}"

        def invalid(reason):
            raise CasaTimeConventionError(
                f"{context}: {reason}. No time alignment was applied; "
                "inspect the source sampling convention (ACCESS-MOPPy #816)."
            )

        if dim not in ds.coords:
            invalid("missing time coordinate")
        time = ds[dim]
        bounds_name = time.attrs.get("bounds")
        if bounds_name not in ds:
            invalid("missing declared time bounds")
        bounds = ds[bounds_name]
        if bounds.ndim != 2 or bounds.dims[0] != dim or bounds.shape[1] != 2:
            invalid("expected two bounds per time sample")
        if any(
            not re.search(
                rf"\b{re.escape(dim)}:\s*mean\b", ds[name].attrs.get("cell_methods", "")
            )
            for name in fields
        ):
            invalid("selected nitrogen fields must declare a time mean")
        # Do not silently retime a different selected diagnostic sharing this axis.
        others = {
            name
            for name in required_vars & set(ds.data_vars)
            if dim in ds[name].dims
            and name not in CASA_NITROGEN_FIELDS
            and name != bounds_name
        }
        units = time.attrs.get("units")
        calendar = time.attrs.get("calendar", "standard")
        values = np.asarray(time.values, dtype=np.float64)
        raw = np.asarray(bounds.values, dtype=np.float64)
        if not units or not np.isfinite(values).all() or not np.isfinite(raw).all():
            invalid("time units and finite coordinates/bounds are required")
        if len(values) == 0:
            invalid("empty time axis")
        try:
            dates = cftime.num2date(
                values, units, calendar, only_use_cftime_datetimes=True
            )
            starts = [
                date.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
                for date in dates
            ]
            ends = [
                date.replace(year=date.year + 1, month=1)
                if date.month == 12
                else date.replace(month=date.month + 1)
                for date in starts
            ]
            canonical = np.column_stack(
                (
                    cftime.date2num(starts, units, calendar),
                    cftime.date2num(ends, units, calendar),
                )
            ).astype(np.float64)
            sampled = canonical.copy()
            sampled[:, 0] = cftime.date2num(
                [date + timedelta(hours=23, minutes=30) for date in starts],
                units,
                calendar,
            )
            # A millisecond in the source units, with no date-dependent rtol.
            tolerance = (
                abs(
                    float(
                        cftime.date2num(
                            starts[0] + timedelta(seconds=1), units, calendar
                        )
                    )
                    - canonical[0, 0]
                )
                / 1000
            )
        except (TypeError, ValueError, OverflowError) as exc:
            invalid(f"cannot decode monthly interval: {exc}")
        if len({(date.year, date.month) for date in starts}) != len(values):
            invalid("more than one sample in a calendar month")
        if np.any(np.diff(canonical[:, 0]) <= 0):
            invalid("months are not strictly increasing")
        if not np.allclose(values, raw.mean(axis=1), rtol=0, atol=tolerance):
            invalid("time values are not the midpoints of the declared bounds")
        if np.allclose(raw, canonical, rtol=0, atol=tolerance):
            continue  # Already a calendar-month mean; retain exact metadata.
        if not np.allclose(raw, sampled, rtol=0, atol=tolerance):
            invalid("bounds do not match the documented 23.5-hour sampling offset")
        if others:
            invalid(
                f"unrecognised selected fields share the shifted axis: {sorted(others)}"
            )

        result = result.assign_coords({dim: time.copy(data=canonical.mean(axis=1))})
        # Use the bare Variable: a DataArray still carries the old time labels
        # and would outer-align the bounds back onto the sampling midpoints.
        result[bounds_name] = bounds.variable.copy(data=canonical)
        logger.warning(
            "%s: representing %d daily-sampled monthly means with calendar-month "
            "bounds and midpoints; native lower-bound offset=23.5 h. "
            "Values and masks unchanged. Convention: %s; evidence: "
            "https://github.com/ACCESS-NRI/UM7/pull/192; source: %s",
            context,
            len(values),
            CASA_MONTHLY_CONVENTION,
            ds.encoding.get("source", "in-memory"),
        )
    return result
