#!/usr/bin/env python

import numpy as np
import xarray as xr


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


def calculate_monthly_minimum(
    da: xr.DataArray, time_dim: str = "time", preserve_attrs: bool = True
) -> xr.DataArray:
    """
    Calculate monthly minimum values from higher frequency data (lazy computation).

    This function aggregates data with frequency higher than monthly (e.g., daily, 3hr, 6hr)
    to monthly minimum values using lazy xarray operations that preserve Dask arrays.

    Parameters
    ----------
    da : xarray.DataArray
        Input data array with time dimension. Should have frequency higher than monthly.
        Supports both eager and lazy (Dask) arrays.
    time_dim : str, default "time"
        Name of the time dimension in the input data array.
    preserve_attrs : bool, default True
        Whether to preserve variable attributes in the output.

    Returns
    -------
    xarray.DataArray
        Monthly minimum values with updated cell_methods attribute.
        Preserves lazy computation if input is lazy.

    Raises
    ------
    ValueError
        If the time dimension is not found in the input data array.

    Examples
    --------
    >>> # Calculate monthly minimum from daily temperature data
    >>> daily_tas = xr.DataArray(...)  # Daily temperature data
    >>> monthly_min_tas = calculate_monthly_minimum(daily_tas)

    Notes
    -----
    - Uses lazy xarray/Dask operations - no computation until .compute() is called
    - Input data should have temporal frequency higher than monthly (daily, 3hr, 6hr, etc.)
    - The function uses xarray's resample method with 'M' frequency (end of month)
    - Cell methods attribute is updated to reflect the temporal aggregation
    - Time coordinate is preserved with monthly timestamps
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

    # Perform monthly resampling using minimum (lazy operation)
    if (
        not np.issubdtype(da[time_dim].dtype, np.datetime64)
        and da[time_dim].dtype != object
    ):
        _name = da.name or "__tmp"
        da = xr.decode_cf(da.to_dataset(name=_name))[_name]

    try:
        monthly_min = da.resample({time_dim: "ME"}).min(keep_attrs=preserve_attrs)

        if preserve_attrs:
            # Update cell_methods to reflect the temporal aggregation
            cell_methods = da.attrs.get("cell_methods", "")
            new_cell_method = f"{time_dim}: minimum"

            if cell_methods:
                monthly_min.attrs["cell_methods"] = f"{cell_methods} {new_cell_method}"
            else:
                monthly_min.attrs["cell_methods"] = new_cell_method

        return monthly_min

    except Exception as e:
        raise RuntimeError(f"Failed to calculate monthly minimum: {e}")


def calculate_monthly_maximum(
    da: xr.DataArray, time_dim: str = "time", preserve_attrs: bool = True
) -> xr.DataArray:
    """
    Calculate monthly maximum values from higher frequency data (lazy computation).

    This function aggregates data with frequency higher than monthly (e.g., daily, 3hr, 6hr)
    to monthly maximum values using lazy xarray operations that preserve Dask arrays.

    Parameters
    ----------
    da : xarray.DataArray
        Input data array with time dimension. Should have frequency higher than monthly.
        Supports both eager and lazy (Dask) arrays.
    time_dim : str, default "time"
        Name of the time dimension in the input data array.
    preserve_attrs : bool, default True
        Whether to preserve variable attributes in the output.

    Returns
    -------
    xarray.DataArray
        Monthly maximum values with updated cell_methods attribute.
        Preserves lazy computation if input is lazy.

    Raises
    ------
    ValueError
        If the time dimension is not found in the input data array.

    Examples
    --------
    >>> # Calculate monthly maximum from daily temperature data
    >>> daily_tasmax = xr.DataArray(...)  # Daily maximum temperature data
    >>> monthly_max_tasmax = calculate_monthly_maximum(daily_tasmax)

    Notes
    -----
    - Uses lazy xarray/Dask operations - no computation until .compute() is called
    - Input data should have temporal frequency higher than monthly (daily, 3hr, 6hr, etc.)
    - The function uses xarray's resample method with 'M' frequency (end of month)
    - Cell methods attribute is updated to reflect the temporal aggregation
    - Time coordinate is preserved with monthly timestamps
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

    # Perform monthly resampling using maximum (lazy operation)
    if (
        not np.issubdtype(da[time_dim].dtype, np.datetime64)
        and da[time_dim].dtype != object
    ):
        _name = da.name or "__tmp"
        da = xr.decode_cf(da.to_dataset(name=_name))[_name]

    try:
        monthly_max = da.resample({time_dim: "ME"}).max(keep_attrs=preserve_attrs)

        if preserve_attrs:
            # Update cell_methods to reflect the temporal aggregation
            cell_methods = da.attrs.get("cell_methods", "")
            new_cell_method = f"{time_dim}: maximum"

            if cell_methods:
                monthly_max.attrs["cell_methods"] = f"{cell_methods} {new_cell_method}"
            else:
                monthly_max.attrs["cell_methods"] = new_cell_method

        return monthly_max

    except Exception as e:
        raise RuntimeError(f"Failed to calculate monthly maximum: {e}")
