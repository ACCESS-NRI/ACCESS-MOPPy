#!/usr/bin/env python

from importlib.resources import as_file

import cftime
import numpy as np
import pandas as pd
import xarray as xr

from access_moppy.utilities import get_bundled_resource_path


def add_axis(var, name, value):
    """Returns the same variable with an extra singleton axis added

    Parameters
    ----------
    var : Xarray DataArray
        Variable to modify
    name : str
        cmor name for axis
    value : float
        value of the new singleton dimension

    Returns
    -------
    var : Xarray DataArray
        Same variable with added axis at start

    """
    var = var.expand_dims(dim={name: [float(value)]})
    return var


def drop_axis(var, dims, errors="raise"):
    """Returns variable with specified dimensions dropped (lazy operation).

    This function performs lazy dimension dropping that preserves dask arrays.
    For time dimensions, it selects the first time step and then drops the coordinate.

    Parameters
    ----------
    var : xarray.DataArray
        Variable to modify (supports both eager and lazy/dask arrays)
    dims : str or list of str
        Dimension name(s) to drop
    errors : str, optional
        How to handle missing dimensions ('raise' or 'ignore'), default 'raise'

    Returns
    -------
    var : xarray.DataArray
        Variable with specified dimensions dropped. Preserves lazy computation
        if input is lazy.

    Notes
    -----
    - Uses lazy xarray operations - no computation until .compute() is called
    - Fully compatible with dask arrays and preserves chunking
    - For time dimensions: selects first time step then drops time coordinate
    - For other dimensions: uses isel(dim=0) then drops the coordinate
    """
    if isinstance(dims, str):
        dims = [dims]

    result = var
    for dim in dims:
        if dim in result.dims:
            # Select first index along this dimension and drop the coordinate
            result = result.isel({dim: 0}, drop=True)

    return result


def drop_time_axis(var):
    """Returns variable with time dimension dropped by selecting first time step (lazy operation).

    Convenience function specifically for dropping time dimensions, which is a common
    operation for time-independent variables like cell thickness, bathymetry, etc.

    Parameters
    ----------
    var : xarray.DataArray
        Variable to modify (supports both eager and lazy/dask arrays)

    Returns
    -------
    var : xarray.DataArray
        Variable with time dimension dropped. Preserves lazy computation if input is lazy.

    Notes
    -----
    - Uses lazy xarray operations - no computation until .compute() is called
    - Selects first time step and drops time coordinate
    - Safe to use even if time dimension doesn't exist
    """
    if "time" in var.dims:
        return var.isel(time=0, drop=True)
    return var


def squeeze_axis(var, dims=None):
    """Returns variable with singleton dimensions removed (lazy operation).

    This function performs lazy dimension squeezing that preserves dask arrays.
    No computation is triggered until .compute() is called.

    Parameters
    ----------
    var : xarray.DataArray
        Variable to modify (supports both eager and lazy/dask arrays)
    dims : str, list of str, or None, optional
        Dimension name(s) to squeeze. If None, squeeze all singleton dims

    Returns
    -------
    var : xarray.DataArray
        Variable with singleton dimensions squeezed. Preserves lazy computation
        if input is lazy.

    Notes
    -----
    - Uses lazy xarray operations - no computation until .compute() is called
    - Fully compatible with dask arrays and preserves chunking
    - When dims=None, automatically detects and squeezes all singleton dimensions
    """
    # squeeze is a lazy operation that preserves dask arrays
    return var.squeeze(dim=dims)


def sum_vars(varlist):
    """Returns sum of all variables in list
    Parameters
    ----------
    varlist : list(xarray.DataArray)
        Variables to sum

    Returns
    -------
    varout : xarray.DataArray
        Sum of input variables

    """
    # first check that dimensions are same for all variables
    varout = varlist[0]
    for v in varlist[1:]:
        varout = varout + v
    return varout


def rename_coord(var1, var2, ndim, override=False):
    """If coordinates in ndim position are different, renames var2
    coordinates as var1.
    """
    coord1 = var1.dims[ndim]
    coord2 = var2.dims[ndim]
    if coord1 != coord2:
        var2 = var2.rename({coord2: coord1})
        if "bounds" in var1[coord1].attrs.keys():
            var2[coord1].attrs["bounds"] = var1[coord1].attrs["bounds"]
        override = True
    return var2, override


def _monthly_midpoint_coord(time_da: xr.DataArray) -> xr.DataArray:
    """Relabel a resampled monthly time coordinate to each month's midpoint.

    ``resample(..., "ME")`` labels every monthly bin at the month-end
    timestamp. CF/CMIP6 require the time coordinate of a time-mean quantity
    to be the midpoint of its cell bounds, which for monthly data is the
    centre of the ``[month start, next month start)`` interval. This recentres
    each label so ``time`` matches ``midpoint(time_bnds)``. Handles both cftime
    and datetime64 coordinates.
    """
    values = time_da.values
    is_cftime = isinstance(values.flat[0], cftime.datetime)
    midpoints = np.empty(len(values), dtype=object)
    for i, t in enumerate(values):
        if is_cftime:
            start = cftime.datetime(t.year, t.month, 1, calendar=t.calendar)
            if t.month == 12:
                nxt = cftime.datetime(t.year + 1, 1, 1, calendar=t.calendar)
            else:
                nxt = cftime.datetime(t.year, t.month + 1, 1, calendar=t.calendar)
        else:
            ts = pd.Timestamp(t)
            start = pd.Timestamp(year=ts.year, month=ts.month, day=1)
            nxt = start + pd.offsets.MonthBegin(1)
        midpoints[i] = start + (nxt - start) / 2
    if not is_cftime:
        midpoints = pd.DatetimeIndex(midpoints).values
    return time_da.copy(data=midpoints)


def _mask_missing_values_for_reduction(da: xr.DataArray) -> xr.DataArray:
    """Mask configured missing-value sentinels so reductions ignore them.

    Input files can carry numeric sentinels (for example ``1e20``) in data while
    storing marker values in attrs/encoding as ``_FillValue`` or ``missing_value``.
    If those sentinels are not masked, temporal maxima can collapse to the marker
    value. This helper applies a lazy mask using xarray operations, preserving
    Dask-backed arrays.
    """

    def _iter_markers(value):
        if value is None:
            return
        if np.isscalar(value):
            yield value
            return
        for v in np.ravel(value):
            yield v

    markers = []
    has_nan_marker = False
    for container in (da.attrs, da.encoding):
        for key in ("missing_value", "_FillValue"):
            for raw in _iter_markers(container.get(key)):
                try:
                    marker = float(raw)
                except (TypeError, ValueError):
                    continue
                if np.isnan(marker):
                    has_nan_marker = True
                else:
                    markers.append(marker)

    mask = None
    if np.issubdtype(da.dtype, np.floating) and has_nan_marker:
        mask = np.isnan(da)

    for marker in set(markers):
        if np.isfinite(marker):
            # Match both exact values and float32-rounded encodings (e.g. 1e20).
            atol = max(1e-12, abs(float(np.spacing(np.float32(marker)))))
            # Use abs(da - marker) <= atol, not np.isclose: np.isclose is not a
            # ufunc and eagerly computes a Dask-backed array to NumPy (loading
            # the whole series into memory). This form is Dask-native/lazy and,
            # with rtol=0, is equivalent to np.isclose(da, marker, atol=atol).
            condition = abs(da - marker) <= atol
        else:
            condition = da == marker
        mask = condition if mask is None else (mask | condition)

    if mask is None:
        return da

    masked = da.where(~mask)
    masked.attrs = da.attrs.copy()
    masked.encoding = da.encoding.copy()
    return masked


def _ensure_submonthly_spacing(time_values: np.ndarray, cf_name: str) -> None:
    """Refuse monthly-or-coarser input to a monthly reduction.

    When every "ME" resample bin holds a single sample, the reduction is an
    identity and silently returns the input unchanged. This is how monthly
    tasmax/tasmin came out bit-identical to tas (#644): the monthly-mean field
    was fed to calculate_monthly_maximum/minimum. Raise instead so a
    mis-wired mapping fails loudly.

    A single-timestep axis cannot reveal its frequency, so it is let through.
    """
    if time_values.size < 2:
        return
    diffs = np.diff(time_values)
    if np.issubdtype(diffs.dtype, np.timedelta64):
        seconds = diffs.astype("timedelta64[s]").astype(np.float64)
    else:  # cftime objects subtract to datetime.timedelta
        seconds = np.array([d.total_seconds() for d in diffs], dtype=np.float64)
    median_days = float(np.median(seconds)) / 86400
    # Monthly spacing is 28-31 days; anything >= 27 days cannot be sub-monthly.
    if median_days >= 27:
        raise ValueError(
            f"Cannot calculate monthly {cf_name}: the input time axis is already "
            f"monthly or coarser (median spacing {median_days:.1f} days), so the "
            "reduction would be an identity and silently copy the input. "
            "Feed sub-monthly (e.g. daily) data instead."
        )


def _reduce_to_monthly(
    da: xr.DataArray,
    time_dim: str,
    preserve_attrs: bool,
    method: str,
    cf_name: str,
) -> xr.DataArray:
    """Shared implementation for the monthly reductions below.

    ``method`` is the xarray resampler method ("min"/"max"/"mean");
    ``cf_name`` the CF cell_methods word ("minimum"/"maximum"/"mean").
    """
    if time_dim not in da.dims:
        raise ValueError(
            f"Time dimension '{time_dim}' not found in data array dimensions: {list(da.dims)}"
        )

    # Check if we have a time coordinate
    if time_dim not in da.coords:
        raise ValueError(
            f"Time coordinate '{time_dim}' not found in data array coordinates"
        )

    # Save units/calendar before decode_cf moves them from attrs to encoding,
    # and before resample creates a new coordinate that loses the encoding.
    _saved_units = da[time_dim].attrs.get("units") or da[time_dim].encoding.get("units")
    _saved_calendar = da[time_dim].attrs.get("calendar") or da[time_dim].encoding.get(
        "calendar"
    )

    if (
        not np.issubdtype(da[time_dim].dtype, np.datetime64)
        and da[time_dim].dtype != object
    ):
        _name = da.name or "__tmp"
        da = xr.decode_cf(da.to_dataset(name=_name))[_name]

    _ensure_submonthly_spacing(da[time_dim].values, cf_name)

    da = _mask_missing_values_for_reduction(da)

    try:
        resampler = da.resample({time_dim: "ME"})
        monthly = getattr(resampler, method)(keep_attrs=preserve_attrs)
        # "ME" labels each bin at month-end; recentre to the cell midpoint
        # so the time coordinate matches midpoint(time_bnds) (CF/CMIP6).
        monthly = monthly.assign_coords(
            {time_dim: _monthly_midpoint_coord(monthly[time_dim])}
        )

        # Restore units/calendar lost through decode_cf + resample
        if _saved_units and not monthly[time_dim].attrs.get("units"):
            monthly[time_dim].attrs["units"] = _saved_units
        if _saved_calendar and not monthly[time_dim].attrs.get("calendar"):
            monthly[time_dim].attrs["calendar"] = _saved_calendar

        if preserve_attrs:
            # Update cell_methods to reflect the temporal aggregation
            cell_methods = da.attrs.get("cell_methods", "")
            new_cell_method = f"{time_dim}: {cf_name}"

            if cell_methods:
                monthly.attrs["cell_methods"] = f"{cell_methods} {new_cell_method}"
            else:
                monthly.attrs["cell_methods"] = new_cell_method

        return monthly

    except Exception as e:
        raise RuntimeError(f"Failed to calculate monthly {cf_name}: {e}")


def calculate_monthly_minimum(
    da: xr.DataArray, time_dim: str = "time", preserve_attrs: bool = True
) -> xr.DataArray:
    """
    Calculate monthly minimum values from higher frequency data (lazy computation).

    Aggregates data with frequency higher than monthly (e.g., daily, 3hr, 6hr)
    to monthly minimum values using lazy xarray operations that preserve Dask
    arrays.

    Parameters
    ----------
    da : xarray.DataArray
        Input data array with a sub-monthly time dimension. Supports both
        eager and lazy (Dask) arrays.
    time_dim : str, default "time"
        Name of the time dimension in the input data array.
    preserve_attrs : bool, default True
        Whether to preserve variable attributes in the output.

    Returns
    -------
    xarray.DataArray
        Monthly minimum values with updated cell_methods attribute and the
        time coordinate set to each month's midpoint (centre of time_bnds).

    Raises
    ------
    ValueError
        If the time dimension/coordinate is missing, or the input time axis is
        already monthly or coarser (the reduction would be an identity, #644).
    """
    return _reduce_to_monthly(da, time_dim, preserve_attrs, "min", "minimum")


def calculate_monthly_maximum(
    da: xr.DataArray, time_dim: str = "time", preserve_attrs: bool = True
) -> xr.DataArray:
    """
    Calculate monthly maximum values from higher frequency data (lazy computation).

    Aggregates data with frequency higher than monthly (e.g., daily, 3hr, 6hr)
    to monthly maximum values using lazy xarray operations that preserve Dask
    arrays.

    Parameters
    ----------
    da : xarray.DataArray
        Input data array with a sub-monthly time dimension. Supports both
        eager and lazy (Dask) arrays.
    time_dim : str, default "time"
        Name of the time dimension in the input data array.
    preserve_attrs : bool, default True
        Whether to preserve variable attributes in the output.

    Returns
    -------
    xarray.DataArray
        Monthly maximum values with updated cell_methods attribute and the
        time coordinate set to each month's midpoint (centre of time_bnds).

    Raises
    ------
    ValueError
        If the time dimension/coordinate is missing, or the input time axis is
        already monthly or coarser (the reduction would be an identity, #644).
    """
    return _reduce_to_monthly(da, time_dim, preserve_attrs, "max", "maximum")


def calculate_monthly_mean(
    da: xr.DataArray, time_dim: str = "time", preserve_attrs: bool = True
) -> xr.DataArray:
    """
    Calculate monthly mean values from higher frequency data (lazy computation).

    Aggregates data with frequency higher than monthly (e.g., daily, 3hr, 6hr)
    to monthly mean values using lazy xarray operations that preserve Dask
    arrays. This is the second stage of two-stage CF reductions such as
    "time: maximum within days time: mean over days" (monthly tasmax/tasmin,
    #644): the model supplies the within-day extremum at daily frequency, and
    this function averages it over each month.

    Parameters
    ----------
    da : xarray.DataArray
        Input data array with a sub-monthly time dimension. Supports both
        eager and lazy (Dask) arrays.
    time_dim : str, default "time"
        Name of the time dimension in the input data array.
    preserve_attrs : bool, default True
        Whether to preserve variable attributes in the output.

    Returns
    -------
    xarray.DataArray
        Monthly mean values with updated cell_methods attribute and the
        time coordinate set to each month's midpoint (centre of time_bnds).

    Raises
    ------
    ValueError
        If the time dimension/coordinate is missing, or the input time axis is
        already monthly or coarser (the reduction would be an identity, #644).
    """
    return _reduce_to_monthly(da, time_dim, preserve_attrs, "mean", "mean")


def load_ressource_data(ressource_file: str, var_name: str) -> xr.DataArray:
    """Load a single variable from a bundled resource file.

    Designed to be used as a nested expression inside a mapping's
    calculation args, so that static/fx variables (e.g. areacello)
    can be injected into any derivation without being listed in
    model_variables or loaded from the main input dataset.
    """
    resource_path = get_bundled_resource_path(ressource_file)
    with as_file(resource_path) as resolved:
        ds = xr.open_dataset(str(resolved))
        if var_name not in ds:
            raise ValueError(
                f"Variable '{var_name}' not found in resource file '{ressource_file}'. "
                f"Available: {list(ds.data_vars)}"
            )
        return ds[var_name]
