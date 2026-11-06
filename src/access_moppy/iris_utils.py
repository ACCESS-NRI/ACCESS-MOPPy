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
    return cube
