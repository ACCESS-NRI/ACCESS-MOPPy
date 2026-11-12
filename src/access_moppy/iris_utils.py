"""Lazy conversion of a CMORised xarray dataset to one Iris cube."""

from __future__ import annotations

import numpy as np
import xarray as xr


def _fill_values(variable: xr.DataArray) -> list[float]:
    """Read numeric missing-value markers from both CF metadata locations."""
    values = []
    for key in ("_FillValue", "missing_value"):
        for source in (variable.attrs, variable.encoding):
            marker = source.get(key)
            if marker is None:
                continue
            try:
                marker = float(marker)
            except (TypeError, ValueError):
                continue
            if np.isfinite(marker) and marker not in values:
                values.append(marker)
    return values


def dataset_to_iris(dataset: xr.Dataset, short_name: str):
    """Return the named field as an Iris cube without loading its data.

    The conversion works on a shallow copy. CF coordinates, coordinate bounds,
    and referenced cell measures remain available to ncdata, while fill-value
    and invalid-value masking stays in the Dask graph until data is requested.
    """
    try:
        import dask.array as da
        from ncdata.iris_xarray import cubes_from_xarray
    except ImportError as exc:
        raise ImportError(
            "ncdata and iris are required for to_iris(). Please install ncdata and iris."
        ) from exc

    if short_name not in dataset.data_vars:
        raise ValueError(f"Variable {short_name!r} is absent from the dataset")

    ds = dataset.copy(deep=False)
    # ncdata serializes through xarray's CF datetime encoder. A decoded
    # datetime with explicit units but no dtype cannot be encoded there;
    # choose floating-point offsets on this shallow copy so fractional days
    # and the caller's original encoding are both preserved.
    for name in ds.variables:
        variable = ds[name]
        if (
            np.issubdtype(variable.dtype, np.datetime64)
            and variable.encoding.get("units")
            and variable.encoding.get("dtype") is None
        ):
            variable.encoding["dtype"] = "float64"
    # ncdata's in-memory NetCDF adapter cannot represent scalar Unicode
    # coordinates. Carry them across as Iris auxiliary coordinates instead.
    scalar_strings = []
    for name, coord in ds.coords.items():
        if coord.ndim == 0 and coord.dtype.kind in {"U", "S"}:
            value = coord.values.item()
            if isinstance(value, bytes):
                value = value.decode("utf-8")
            scalar_strings.append((name, value, dict(coord.attrs)))
    if scalar_strings:
        ds = ds.drop_vars([name for name, _, _ in scalar_strings])
    # CF coordinates may be data variables after the CMOR processing steps.
    promote = {
        name
        for name in ("latitude", "longitude", "vertices_latitude", "vertices_longitude")
        if name in ds.data_vars
    }
    for coord in ds.coords.values():
        bounds_name = coord.attrs.get("bounds")
        if isinstance(bounds_name, str) and bounds_name in ds.data_vars:
            promote.add(bounds_name)
    coordinates = ds[short_name].attrs.get("coordinates", "")
    if isinstance(coordinates, str):
        promote.update(name for name in coordinates.split() if name in ds.data_vars)
    if promote:
        ds = ds.set_coords(sorted(promote))

    # The file writer lists scalar auxiliary coordinates on the data variable.
    # ncdata does not infer that link for an in-memory xarray dataset, so make
    # the same CF association here (notably CMOR's depth0m -> scalar depth).
    scalar_aux = [name for name, coord in ds.coords.items() if coord.ndim == 0]
    if scalar_aux:
        attrs = dict(ds[short_name].attrs)
        existing = attrs.get("coordinates", "")
        tokens = existing.split() if isinstance(existing, str) else []
        attrs["coordinates"] = " ".join(dict.fromkeys(tokens + scalar_aux))
        ds[short_name].attrs = attrs

    variable = ds[short_name]
    values = _fill_values(variable)
    if values:
        masked = variable
        for fill in values:
            # The relative allowance handles a sentinel rounded through float32.
            # Zero still uses an exact comparison.
            masked = masked.where(abs(masked - fill) > abs(fill) * 1e-4)
        masked.attrs = {
            key: value
            for key, value in variable.attrs.items()
            if key not in {"_FillValue", "missing_value"}
        }
        masked.encoding = {
            key: value
            for key, value in variable.encoding.items()
            if key not in {"_FillValue", "missing_value"}
        }
        ds[short_name] = masked

    cubes = cubes_from_xarray(ds)
    matches = [cube for cube in cubes if cube.var_name == short_name]
    if len(matches) != 1:
        raise ValueError(
            f"Could not find cube for variable '{short_name}' in converted CubeList. "
            f"Available cubes: {[cube.var_name for cube in cubes]}"
        )
    cube = matches[0]
    core = cube.core_data() if hasattr(cube, "core_data") else cube.data
    cube.data = (
        da.ma.masked_invalid(core)
        if isinstance(core, da.Array)
        else np.ma.masked_invalid(core)
    )
    if scalar_strings:
        from iris.coords import AuxCoord
    for name, value, attrs in scalar_strings:
        coord_attrs = {
            key: val
            for key, val in attrs.items()
            if key not in {"standard_name", "long_name", "units"}
        }
        cube.add_aux_coord(
            AuxCoord(
                value,
                var_name=name,
                standard_name=attrs.get("standard_name"),
                long_name=attrs.get("long_name"),
                units=attrs.get("units"),
                attributes=coord_attrs,
            )
        )
    return cube
