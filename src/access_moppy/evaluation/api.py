"""Normalize explicit native datasets for evaluation without writing files."""

from __future__ import annotations

import json
from copy import deepcopy

import numpy as np
import xarray as xr

from .bgc import derive_fgco2, derive_pht_estimate
from .cice import normalize_cice
from .mom6 import complete_grid_from_supergrid, normalize_mom6


def validate_required_period(dataset, options):
    """Require every monthly interval in an explicitly requested year range."""
    period = options.get("required_period")
    if period is None:
        return
    if len(period) != 2 or any(
        isinstance(v, bool) or not isinstance(v, int) for v in period
    ):
        raise ValueError("required_period must contain two integer years")
    start, end = period
    if start < 1 or end < start:
        raise ValueError("required_period years must be positive and ordered")
    if "time" not in dataset.coords:
        raise ValueError(
            "required_period applies to monthly fields, not static geometry"
        )
    expected = np.arange(start * 12 + 1, (end + 1) * 12 + 1)
    actual = dataset.time.dt.year.values * 12 + dataset.time.dt.month.values
    if not np.array_equal(actual, expected):
        raise ValueError(
            f"Expected every month of {start}–{end} exactly once; got {len(actual)} records"
        )
    if options.get("required_months", len(expected)) != len(expected):
        raise ValueError("required_months disagrees with required_period")


def normalize_native(
    native: xr.Dataset,
    *,
    short_name: str,
    component: str,
    grid: xr.Dataset,
    run_id: str,
    model_id: str = "ACCESS-OM3",
    supergrid: xr.Dataset | None = None,
    options: dict | None = None,
) -> xr.Dataset:
    """Return one lazy, native-grid evaluation field and its CF metadata.

    This opt-in path accepts a real development ``run_id`` without requiring
    a CMIP experiment identity. It does not produce a CMIP publication product.
    The caller owns the supplied open datasets and their lifetimes. No model
    field is resampled, horizontally remapped, rotated or written to disk.

    ``component`` selects ``mom6`` or ``cice``. ``options`` must declare the
    native grid (``native_grid``: T/Cu/Cv for MOM6, T for CICE) and can specify
    raw variable names, a requested period and explicit derivation parameters.
    Surface pH estimated from monthly mean Htotal requires
    ``allow_pht_estimate=True`` and ``co2_calc='mocsy'``.
    """
    if not isinstance(native, xr.Dataset) or not isinstance(grid, xr.Dataset):
        raise TypeError("native and grid must be open xarray Datasets")
    if model_id != "ACCESS-OM3":
        raise ValueError(
            "Native evaluation currently supports model_id='ACCESS-OM3' only"
        )
    if not isinstance(run_id, str) or not run_id.strip():
        raise ValueError("An explicit nonempty run_id is required")
    if component not in {"mom6", "cice"}:
        raise ValueError("component must be 'mom6' or 'cice'")
    spec = deepcopy({} if options is None else options)
    if not isinstance(spec, dict):
        raise TypeError("options must be a dictionary")
    spec["run_id"] = run_id
    # Metadata changes must never mutate the caller's datasets.
    native, grid = native.copy(deep=False), grid.copy(deep=False)
    if supergrid is not None:
        if component != "mom6" or not isinstance(supergrid, xr.Dataset):
            raise ValueError("supergrid must be a MOM6 xarray Dataset")
        grid = complete_grid_from_supergrid(grid, supergrid)
    selection = spec.get("index_selection", {})
    if selection:
        if component != "mom6" or not set(selection) <= {"yh", "xh", "yq", "xq"}:
            raise ValueError(
                "index_selection supports explicit MOM6 horizontal dimensions only"
            )
        slices = {}
        for dim, bounds in selection.items():
            if not isinstance(bounds, (list, tuple)) or not 2 <= len(bounds) <= 3:
                raise ValueError(
                    "Each index_selection must contain slice start, stop and optional step"
                )
            if any(
                v is not None and (isinstance(v, bool) or not isinstance(v, int))
                for v in bounds
            ):
                raise ValueError("index_selection endpoints must be integers or null")
            if len(bounds) == 3 and bounds[2] is not None and bounds[2] <= 0:
                raise ValueError("index_selection step must be positive")
            if dim not in native.dims and dim not in grid.dims:
                raise ValueError(f"index_selection dimension {dim!r} is absent")
            slices[dim] = slice(*bounds)
        native = native.isel({k: v for k, v in slices.items() if k in native.dims})
        grid = grid.isel({k: v for k, v in slices.items() if k in grid.dims})
    derived = None
    if component == "cice":
        result = normalize_cice(native, grid, spec, short_name)
    else:
        if short_name in {"fgco2", "phos"}:
            if short_name == "phos" and spec.get("allow_pht_estimate") is not True:
                raise ValueError(
                    "phos from Htotal requires explicit allow_pht_estimate=True"
                )
            derive = derive_fgco2 if short_name == "fgco2" else derive_pht_estimate
            derived = derive(native, spec)
            native = native.assign({short_name: derived.rename(short_name)})
            spec["native"] = {**spec.get("native", {}), "variable": short_name}
        result = normalize_mom6(native, grid, spec, short_name)
        if derived is not None:
            result[short_name].attrs.update(derived.attrs)
            for key in (
                "derivation_status",
                "comment",
                "wombatlite_source",
                "pH_scale",
            ):
                if key in derived.attrs:
                    result.attrs[key] = derived.attrs[key]
        if short_name == "fgco2":
            result = result.assign_coords(depth=0.0)
            result.depth.attrs = {
                "standard_name": "depth",
                "units": "m",
                "positive": "down",
            }
    validate_required_period(result, spec)
    result.attrs.update(
        evaluation_model_id=model_id,
        evaluation_run_id=run_id,
        evaluation_component=component,
        evaluation_status="Native evaluation normalization; not CMIP publication CMORisation",
        evaluation_options=json.dumps(options or {}, sort_keys=True),
    )
    return result
