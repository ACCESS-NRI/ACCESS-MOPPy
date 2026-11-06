"""Bounded native CICE T-grid normalization for opt-in evaluation."""

from __future__ import annotations

import numpy as np
import xarray as xr

from .mom6 import _only_field, _time_metadata, _wet_mask

_AREA_FACTORS = {
    "m2": 1.0,
    "m^2": 1.0,
    "m**2": 1.0,
    "cm2": 1e-4,
    "cm^2": 1e-4,
    "cm**2": 1e-4,
    "km2": 1e6,
}


def _clean(array):
    field = _only_field(array)
    for key in ("_FillValue", "missing_value"):
        fill = array.attrs.get(key, array.encoding.get(key))
        if fill is not None and np.isfinite(fill):
            field = field.where(field != fill)
    return field.where(np.isfinite(field))


def normalize_cice(native, grid, spec, short_name="siconc"):
    """Normalize native T-cell concentration or static area without remapping."""
    if short_name not in {"siconc", "areacello"}:
        raise ValueError(f"Unsupported native CICE field {short_name!r}")
    if spec.get("native_grid") not in {"T", "t", "cice-t"}:
        raise ValueError("CICE native_grid must explicitly identify T cells")
    names = {
        "concentration": "aice_m",
        "area": "tarea",
        "latitude": "TLAT",
        "longitude": "TLON",
        "mask": "tmask",
        **spec.get("native", {}),
    }
    for key in ("area", "latitude", "longitude", "mask"):
        if names[key] not in grid:
            raise ValueError(f"Missing CICE T-grid {key} variable {names[key]!r}")
    area, lat, lon, mask = (
        grid[names[key]] for key in ("area", "latitude", "longitude", "mask")
    )
    if area.ndim != 2 or any(a.dims != area.dims for a in (lat, lon, mask)):
        raise ValueError(
            "CICE T-grid area, geometry, and mask must share two dimensions"
        )
    xr.align(area, lat, lon, mask, join="exact")
    mask = _wet_mask(mask, "CICE T-grid mask")
    for coord, standard, unit in (
        (lat, "latitude", "degrees_north"),
        (lon, "longitude", "degrees_east"),
    ):
        if coord.attrs.get("units") != unit:
            raise ValueError(f"CICE {standard} requires {unit} units")
        if not bool((np.isfinite(coord) | (mask == 0)).all()):
            raise ValueError(f"CICE {standard} requires finite wet T-cell positions")
    input_area_units = area.attrs.get("units")
    area_units = spec.get("area_units", input_area_units)
    if input_area_units is not None and area_units != input_area_units:
        raise ValueError("CICE area_units disagrees with native area metadata")
    if area_units not in _AREA_FACTORS:
        raise ValueError(f"Explicit supported area units required, got {area_units!r}")
    area = _clean(area) * _AREA_FACTORS[area_units]
    if not bool(((area > 0) | (mask == 0)).all()):
        raise ValueError("CICE wet cell area must be positive and finite")
    area = area.where(mask > 0)
    area.attrs = {
        "standard_name": "cell_area",
        "long_name": "Native CICE T-grid cell area",
        "units": "m2",
    }
    result = xr.Dataset({"areacello": _only_field(area)})
    result = result.assign_coords(latitude=_only_field(lat), longitude=_only_field(lon))
    result.latitude.attrs = {"standard_name": "latitude", "units": "degrees_north"}
    result.longitude.attrs = {"standard_name": "longitude", "units": "degrees_east"}
    for coord, output in ((lat, "latitude"), (lon, "longitude")):
        bounds_name = coord.attrs.get("bounds")
        if bounds_name:
            if bounds_name not in grid:
                raise ValueError(
                    f"Missing CICE {output} corner variable {bounds_name!r}"
                )
            bounds = grid[bounds_name]
            extra = [d for d in bounds.dims if d not in area.dims]
            if bounds.ndim != 3 or len(extra) != 1 or bounds.sizes[extra[0]] != 4:
                raise ValueError(f"Invalid native corner dimensions for {output}")
            if bounds.attrs.get("units", coord.attrs.get("units")) != coord.attrs.get(
                "units"
            ):
                raise ValueError(f"Native {output} center/corner units differ")
            if not bool((np.isfinite(bounds) | (mask == 0)).all()):
                raise ValueError(
                    f"Native {output} corners require finite wet positions"
                )
            result[f"vertices_{output}"] = _only_field(
                bounds.transpose(*area.dims, extra[0])
            )
            result[output].attrs["bounds"] = f"vertices_{output}"
    result.attrs = {
        "adapter_status": "native evaluation normalization; not CMIP publication",
        "native_grid_component": "T",
    }
    if short_name == "areacello":
        return result
    if names["concentration"] not in native:
        raise ValueError(
            f"Missing native CICE concentration {names['concentration']!r}"
        )
    source = native[names["concentration"]]
    if source.dims != ("time", *area.dims):
        raise ValueError(
            f"CICE concentration requires (time,{','.join(area.dims)}) dimensions"
        )
    for dim in area.dims:
        if source.sizes[dim] != area.sizes[dim]:
            raise ValueError(f"Native field/grid size mismatch on {dim}")
    xr.align(source, area, join="exact")
    missing_positions = [
        names[key] for key in ("latitude", "longitude") if names[key] not in native
    ]
    if missing_positions:
        if spec.get("grid_identity_verified") is not True or not spec.get(
            "grid_reference"
        ):
            raise ValueError(
                "Native CICE coordinates are absent; require an audited grid_reference and grid_identity_verified=True"
            )
        result.attrs["grid_identity_method"] = (
            "caller-audited native/static grid association"
        )
        result.attrs["grid_reference"] = str(spec["grid_reference"])
    else:
        result.attrs["grid_identity_method"] = (
            "native/static latitude and longitude comparison"
        )
    for key, expected in (("latitude", lat), ("longitude", lon)):
        if names[key] in native:
            observed = native[names[key]]
            if "time" in observed.dims:
                if observed.ndim != 3 or not bool(
                    (
                        (observed == observed.isel(time=0))
                        | (observed.isnull() & observed.isel(time=0).isnull())
                    ).all()
                ):
                    raise ValueError(f"Native/grid {key} mismatch across time")
                observed = observed.isel(time=0)
            if observed.dims != expected.dims or not bool(
                np.isclose(observed, expected, equal_nan=True).all()
            ):
                raise ValueError(f"Native/grid {key} mismatch")
    native_units = source.attrs.get("units")
    units = spec.get("concentration_units", native_units)
    if native_units is not None and units != native_units:
        raise ValueError("CICE concentration_units disagrees with native metadata")
    if units in {"1", "fraction"}:
        factor = 100.0
    elif units in {"%", "percent"}:
        factor = 1.0
    else:
        raise ValueError(
            f"Explicit fraction/percent concentration units required, got {units!r}"
        )
    concentration = (_clean(source) * factor).where(mask > 0)
    concentration.attrs = {
        "standard_name": "sea_ice_area_fraction",
        "long_name": "Sea ice concentration",
        "units": "%",
        "cell_measures": "area: areacello",
        "cell_methods": "time: mean",
    }
    result["siconc"] = _only_field(concentration)
    return _time_metadata(result, native, {"time_bounds": "time_bounds", **spec})
