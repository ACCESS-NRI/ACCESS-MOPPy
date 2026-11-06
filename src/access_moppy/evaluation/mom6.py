"""Bounded MOM6 normalization for opt-in native evaluation.

Velocity components remain on their native Cu/Cv faces, unrotated and
uninterpolated. These outputs are not CMIP products.
"""

from __future__ import annotations

_FIELDS = {
    "thetao": ("T", "sea_water_potential_temperature", "degC", {"degC", "Celsius"}),
    "so": ("T", "sea_water_salinity", "1e-3", {"psu", "1e-3"}),
    "uo": ("Cu", "sea_water_x_velocity", "m s-1", {"m s-1"}),
    "vo": ("Cv", "sea_water_y_velocity", "m s-1", {"m s-1"}),
    "tauuo": ("Cu", "surface_downward_x_stress", "N m-2", {"N m-2"}),
    "hfds": ("T", "surface_downward_heat_flux_in_sea_water", "W m-2", {"W m-2"}),
    "phos": ("T", "sea_water_ph_reported_on_total_scale", "1", {"1"}),
    "fgco2": (
        "T",
        "surface_downward_mass_flux_of_carbon_dioxide_expressed_as_carbon",
        "kg m-2 s-1",
        {"kg m-2 s-1"},
    ),
    "areacello": ("T", "cell_area", "m2", {"m2"}),
    "volcello": ("T", "ocean_volume", "m3", {"m3"}),
}
_GRIDS = {
    "T": (("yh", "xh"), "", "wet", "areacello"),
    "Cu": (("yh", "xq"), "_u", "wet_u", "areacello_cu"),
    "Cv": (("yq", "xh"), "_v", "wet_v", "areacello_cv"),
}


def complete_grid_from_supergrid(grid, supergrid):
    """Fill static T/Cu/Cv geolocation from the model's original supergrid.

    Call before any index crop. The supergrid x/y point arrays are sampled at
    their documented native C-grid locations; available wet static positions
    must agree. No horizontal interpolation or velocity rotation occurs.
    """
    import numpy as np
    import xarray as xr

    if "x" not in supergrid or "y" not in supergrid:
        raise ValueError("MOM6 supergrid requires x and y point arrays")
    x, y = supergrid["x"], supergrid["y"]
    if x.ndim != 2 or y.dims != x.dims or y.shape != x.shape:
        raise ValueError("MOM6 supergrid x/y must have matching 2D dimensions")
    for array, label, accepted in (
        (x, "x", {"degrees_east", "degree", "degrees"}),
        (y, "y", {"degrees_north", "degree", "degrees"}),
    ):
        if array.attrs.get("units") not in accepted:
            raise ValueError(f"MOM6 supergrid {label} requires degree units")
    result = grid.copy(deep=False)
    sy, sx = x.dims
    samples = {
        "T": (slice(1, None, 2), slice(1, None, 2)),
        "Cu": (slice(1, None, 2), slice(None, None, 2)),
        "Cv": (slice(None, None, 2), slice(1, None, 2)),
    }
    for component, (dims, suffix, wet_name, _) in _GRIDS.items():
        if wet_name not in grid:
            raise ValueError(f"Missing native {component} wet mask {wet_name}")
        row, col = samples[component]
        wet = np.asarray(grid[wet_name].values) > 0
        for raw, name, longitude in (
            (y, "geolat" + suffix, False),
            (x, "geolon" + suffix, True),
        ):
            sampled = np.ma.filled(
                np.ma.masked_invalid(raw.isel({sy: row, sx: col}).values), np.nan
            )
            if (
                sampled.shape != tuple(grid.sizes[d] for d in dims)
                or grid[name].dims != dims
            ):
                raise ValueError(
                    f"Supergrid {component} shape does not match static {name}"
                )
            if not np.isfinite(sampled).all():
                raise ValueError(
                    f"Supergrid {component} {name} contains missing positions"
                )
            original = np.ma.filled(np.ma.masked_invalid(grid[name].values), np.nan)
            available = wet & np.isfinite(original)
            difference = sampled - original
            if longitude:
                difference = (difference + 180.0) % 360.0 - 180.0
            if not np.all(np.abs(difference[available]) <= 1e-3):
                raise ValueError(
                    f"Supergrid {component} {name} disagrees with wet static positions"
                )
            result[name] = xr.DataArray(
                sampled,
                dims=dims,
                coords={d: grid[d] for d in dims if d in grid.coords},
                attrs=dict(grid[name].attrs),
            )
    result.attrs = dict(grid.attrs)
    result.attrs["geographic_geometry_source"] = (
        "original MOM6 ocean_hgrid supergrid x/y, sampled at T/Cu/Cv points"
    )
    return result


def _only_field(array):
    """Drop unrelated staggered auxiliary coordinates without loading data."""
    import xarray as xr

    return xr.DataArray(
        array.data,
        dims=array.dims,
        coords={d: array.coords[d] for d in array.dims if d in array.coords},
        attrs=dict(array.attrs),
        name=array.name,
    )


def _units(array, accepted, target, label):
    actual = array.attrs.get("units")
    if actual not in accepted:
        raise ValueError(
            f"{label} units {actual!r} unsupported; expected {sorted(accepted)}"
        )
    result = _only_field(array)
    import numpy as np

    for key in ("_FillValue", "missing_value"):
        fill = array.attrs.get(key, array.encoding.get(key))
        if fill is not None and np.isfinite(fill):
            result = result.where(result != fill)
    result = result.where(np.isfinite(result))
    result.attrs["units"] = target
    return result


def _wet_mask(array, label):
    """Use finite ocean flags; CF-masked cells are outside the valid mask."""
    import numpy as np

    if array.attrs.get("units", "1") not in {"", "1"}:
        raise ValueError(f"{label} must be dimensionless")
    mask = _only_field(array)
    declared_missing = False
    for key in ("_FillValue", "missing_value"):
        marker = array.attrs.get(key, array.encoding.get(key))
        if marker is not None:
            declared_missing = True
            if np.isfinite(marker):
                mask = mask.where(mask != marker)
    if declared_missing:
        # MOM6/CICE static outputs encode masked cells as CF fill values.
        # Preserve their exclusion rather than treating them as ocean cells.
        mask = mask.fillna(0)
    if not bool(((mask == 0) | (mask == 1)).all()):
        raise ValueError(
            f"{label} must contain zero/one flags or declared CF missing cells"
        )
    return mask


def _time_metadata(result, native, spec):
    import numpy as np

    if "time" not in native.coords:
        raise ValueError("MOM6 monthly input requires a time coordinate")
    time = native.time
    name = (
        time.attrs.get("bounds")
        or time.encoding.get("bounds")
        or spec.get("time_bounds", "time_bnds")
    )
    if name not in native:
        raise ValueError("MOM6 monthly input requires explicit time bounds")
    bounds = native[name]
    extra = [d for d in bounds.dims if d != "time"]
    if bounds.ndim != 2 or len(extra) != 1 or bounds.sizes[extra[0]] != 2:
        raise ValueError("MOM6 time bounds require two endpoints per interval")
    bounds = bounds.transpose("time", extra[0])
    if bounds.sizes["time"] != time.size:
        raise ValueError("MOM6 time bounds and time length differ")
    lower, upper = (bounds.isel({extra[0]: i}, drop=True) for i in (0, 1))
    if not bool((lower < upper).all()) or bool((lower.isnull() | upper.isnull()).any()):
        raise ValueError("MOM6 time bounds require finite increasing intervals")
    if bounds.sizes["time"] > 1 and not bool(
        (
            upper.isel(time=slice(None, -1)).values
            == lower.isel(time=slice(1, None)).values
        ).all()
    ):
        raise ValueError("MOM6 time bounds must be contiguous and ordered")
    # Operate on the small time coordinate arrays: xarray can otherwise coerce
    # cftime subtraction to numpy timedeltas, which cannot be added back to
    # non-Gregorian cftime objects.
    lower_values, upper_values = lower.values, upper.values
    midpoint = lower_values + (upper_values - lower_values) / 2
    result = result.assign_coords(time=("time", midpoint))
    result["time_bnds"] = (("time", "bnds"), bounds.values)
    result.time.attrs = {"standard_name": "time", "bounds": "time_bnds"}
    for key in ("units", "calendar"):
        if key in time.encoding:
            result.time.encoding[key] = time.encoding[key]
    months = result.time.dt.year.values * 12 + result.time.dt.month.values
    start_months = lower.dt.year.values * 12 + lower.dt.month.values
    end_months = upper.dt.year.values * 12 + upper.dt.month.values
    if (
        not np.all(end_months - start_months == 1)
        or not np.all(lower.dt.day.values == 1)
        or not np.all(upper.dt.day.values == 1)
        or any(
            np.any(getattr(endpoint.dt, part).values != 0)
            for endpoint in (lower, upper)
            for part in ("hour", "minute", "second", "microsecond")
        )
    ):
        raise ValueError("MOM6 monthly time bounds must span complete calendar months")
    if len(months) > 1 and not np.all(np.diff(months) == 1):
        raise ValueError("Duplicate, unordered, or missing months in MOM6 input")
    return result


def _depth_metadata(result, native, selected):
    import numpy as np

    if "z_l" not in selected.dims:
        if "surface_depth" in selected.attrs:
            result = result.assign_coords(depth=float(selected.attrs["surface_depth"]))
            result.depth.attrs = {
                "standard_name": "depth",
                "units": "m",
                "positive": "down",
            }
        return result
    if "z_l" not in native.coords or "z_i" not in native:
        raise ValueError("z_l and z_i are required for depth bounds")
    z_l = native.z_l
    z_i = native.z_i
    if z_l.attrs.get("units") not in {"m", "meters"} or z_i.attrs.get("units") not in {
        "m",
        "meters",
    }:
        raise ValueError("Depth center and interface units must be metres")
    if z_i.ndim != 1 or z_i.size != z_l.size + 1:
        raise ValueError("z_i must have one more interface than z_l centers")
    centers = z_l.values
    edges = z_i.values
    if (
        not np.isfinite(centers).all()
        or not np.isfinite(edges).all()
        or not np.all(np.diff(centers) > 0)
        or not np.all(np.diff(edges) > 0)
        or not np.all((edges[:-1] <= centers) & (centers <= edges[1:]))
    ):
        raise ValueError(
            "MOM6 depth centers and bounds must be finite, increasing, and consistent"
        )
    indices = selected.z_l.values
    full = z_l.values
    positions = np.searchsorted(full, indices)
    if np.any(positions >= len(full)) or not np.array_equal(full[positions], indices):
        raise ValueError("Selected depth levels do not match native z_l")
    result = result.assign_coords(z_l=("z_l", indices))
    result.z_l.attrs = {
        "standard_name": "depth",
        "long_name": "Depth at cell center",
        "units": "m",
        "positive": "down",
        "bounds": "z_l_bnds",
    }
    result["z_l_bnds"] = (
        ("z_l", "bnds"),
        np.stack((edges[positions], edges[positions + 1]), axis=1),
    )
    return result


def normalize_mom6(native, grid, spec, short_name):
    """Return a lazy, CF-shaped Dataset for one bounded native MOM6 field.

    ``spec['native']['variable']`` selects a raw variable (default: short_name,
    except volcello uses thkcello). Optional ``depth_selection`` is either
    ``{'index': 0}`` for a scalar depth, ``{'maximum': metres}`` for retained
    native levels, or ``{'nearest_shallower': metres}`` for one level strictly
    shallower than the target. ``volcello`` always uses z_l thkcello times
    static T area.
    """
    import numpy as np
    import xarray as xr

    if short_name not in _FIELDS:
        raise ValueError(f"Unsupported native MOM6 field {short_name!r}")
    component, standard_name, output_units, accepted = _FIELDS[short_name]
    dims, suffix, mask_name, area_name = _GRIDS[component]
    grid_labels = {
        "T": {"T", "t", "mom6-t"},
        "Cu": {"Cu", "cu", "mom6-u"},
        "Cv": {"Cv", "cv", "mom6-v"},
    }
    if spec.get("native_grid") not in grid_labels[component]:
        raise ValueError(
            f"Declared grid {spec.get('native_grid')!r} disagrees with {component} for {short_name}"
        )
    lat, lon, mask = (
        grid[name] for name in ("geolat" + suffix, "geolon" + suffix, mask_name)
    )
    for array, label in ((lat, "latitude"), (lon, "longitude"), (mask, "mask")):
        if array.dims != dims:
            raise ValueError(
                f"Native {component} {label} dimensions {array.dims} differ from {dims}"
            )
    for array, label, unit in (
        (lat, "latitude", "degrees_north"),
        (lon, "longitude", "degrees_east"),
    ):
        if array.attrs.get("units") != unit:
            raise ValueError(f"Native {component} {label} requires {unit} units")
        if not bool(np.isfinite(array).all()):
            raise ValueError(
                f"Native {component} {label} requires finite positions; complete the original supergrid first"
            )
    mask = _wet_mask(mask, f"Native {component} wet mask")
    variable = spec.get("native", {}).get(
        "variable", "thkcello" if short_name == "volcello" else short_name
    )
    if short_name == "areacello":
        if variable != "areacello":
            raise ValueError("areacello must come from static native T area")
        source = grid[area_name]
    else:
        if variable not in native:
            raise ValueError(f"Missing native MOM6 variable {variable!r}")
        source = native[variable]
    if short_name == "volcello" and (
        variable != "thkcello" or source.dims != ("time", "z_l", *dims)
    ):
        raise ValueError("volcello requires thkcello+z on time,z_l,yh,xh")
    if short_name != "areacello":
        expected = (
            ("time", "z_l", *dims)
            if short_name in {"thetao", "so", "uo", "vo", "volcello"}
            else ("time", *dims)
        )
        if source.dims != expected:
            raise ValueError(
                f"{short_name} requires native dimensions {expected}, got {source.dims}"
            )
    if (
        source.sizes[dims[0]] != lat.sizes[dims[0]]
        or source.sizes[dims[1]] != lat.sizes[dims[1]]
    ):
        raise ValueError(f"Native field/grid size mismatch on {component}")
    for d in dims:
        if (d in source.coords) != (d in grid.coords):
            raise ValueError(f"Native field/grid coordinate presence mismatch on {d}")
        if d in source.coords and not np.array_equal(source[d].values, grid[d].values):
            raise ValueError(f"Native field/grid coordinate mismatch on {d}")
    area = _units(grid[area_name], {"m2"}, "m2", area_name)
    if area.dims != dims or not bool(((area > 0) | (mask == 0)).all()):
        raise ValueError(f"Native {component} wet area must be positive")
    area = area.where(_only_field(mask) > 0)
    if short_name == "areacello":
        field = area
    elif short_name == "volcello":
        thickness = _units(source, {"m", "meters"}, "m", "thkcello")
        # Use only the T area. Do not infer face volumes or geometric factors.
        field = thickness * area
    else:
        field = _units(source, accepted, output_units, short_name)
        field = field.where(_only_field(mask) > 0)
    selection = spec.get("depth_selection")
    if selection:
        if "z_l" not in field.dims or set(selection) not in (
            {"index"},
            {"maximum"},
            {"nearest_shallower"},
        ):
            raise ValueError(
                "depth_selection must select z_l by index, maximum, or nearest_shallower"
            )
        if "index" in selection:
            index = int(selection["index"])
            if index < 0 or index >= field.sizes["z_l"]:
                raise ValueError("depth_selection index is out of range")
            field = field.isel(z_l=index, drop=True)
            field.attrs["surface_depth"] = float(source.z_l.values[index])
        elif "maximum" in selection:
            field = field.sel(z_l=slice(None, float(selection["maximum"])))
            if field.sizes["z_l"] == 0:
                raise ValueError("depth_selection contains no z_l centers")
        else:
            target = float(selection["nearest_shallower"])
            centers = source.z_l.values
            shallower = np.flatnonzero(centers < target)
            index = int(shallower[-1]) if len(shallower) else -1
            if index < 0:
                raise ValueError("No native z_l center shallower than requested depth")
            field = field.isel(z_l=slice(index, index + 1))
    field = _only_field(field)
    field.name = short_name
    field.attrs = {
        "standard_name": standard_name,
        "long_name": source.attrs.get("long_name", short_name),
        "units": output_units,
    }
    for key in ("_FillValue", "missing_value"):
        fill = source.attrs.get(key, source.encoding.get(key))
        if fill is not None:
            field.attrs[key] = fill
    if short_name not in {"areacello", "volcello"}:
        field.attrs["cell_methods"] = "time: mean"
    if short_name == "volcello":
        field.attrs["cell_methods"] = "time: mean"
    result = xr.Dataset({short_name: field})
    result = result.assign_coords(latitude=_only_field(lat), longitude=_only_field(lon))
    result.latitude.attrs = {"standard_name": "latitude", "units": "degrees_north"}
    result.longitude.attrs = {"standard_name": "longitude", "units": "degrees_east"}
    if component == "T" and short_name != "areacello":
        result["areacello"] = _only_field(area)
        result.areacello.attrs = {"standard_name": "cell_area", "units": "m2"}
        result[short_name].attrs["cell_measures"] = "area: areacello"
    # MOM6 xh/yh/xq/yq are nominal one-dimensional positions in degree
    # units. Iris otherwise treats them as additional geographic coordinates
    # alongside the supplied two-dimensional geolat/geolon. Alignment against
    # the native static axes was checked above; expose index axes here.
    for dim in dims:
        result = result.assign_coords({dim: (dim, np.arange(result.sizes[dim]))})
        result[dim].attrs = {"long_name": f"native MOM6 {dim} grid index", "units": "1"}
    if "z_l" in source.dims:
        if selection and "index" in selection:
            result = result.assign_coords(
                depth=float(source.z_l.values[int(selection["index"])])
            )
            result.depth.attrs = {
                "standard_name": "depth",
                "units": "m",
                "positive": "down",
            }
        else:
            result = _depth_metadata(result, native, field)
    if "time" in source.dims:
        result = _time_metadata(result, native, spec)
    if "z_l" in result.dims:
        # Preserve the native z_i-derived depth bounds on a CF depth axis.
        result = result.rename({"z_l": "lev", "z_l_bnds": "lev_bnds"})
        result.lev.attrs["bounds"] = "lev_bnds"
    result.attrs = {
        "adapter_status": "native evaluation normalization; not CMIP publication",
        "native_grid_component": component,
        "native_grid_interpretation": "T tracer centers"
        if component == "T"
        else (
            "Cu native zonal faces; unrotated"
            if component == "Cu"
            else "Cv native meridional faces; unrotated"
        ),
        "native_variable": variable,
    }
    return result
