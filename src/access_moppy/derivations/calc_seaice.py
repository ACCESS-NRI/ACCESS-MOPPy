#!/usr/bin/env python
# This file contains a collection of functions to calculate sea ice
# derived variables from ACCESS model output.
#
# To propose new calculations and/or update to existing ones see documentation:
#
# and open a new issue on github.

import json
import logging
from functools import lru_cache

import numpy as np
import xarray as xr

from access_moppy.utilities import get_bundled_resource_path


@lru_cache(maxsize=1)
def _transport_grid():
    """APP4's four straits and their verified ACCESS-ESM1 grid metrics."""
    return json.loads(
        get_bundled_resource_path("seaice/ACCESS-ESM1_transport_lines.json").read_text()
    )


def calc_line_transport(
    quantity, u, v, tlat, tlon, ulat, ulon, *, quantity_kind="area", ice_density=None
):
    """Reconstruct signed sea-ice transport through the four APP4 straits.

    ``quantity`` is either fractional ice area or ice mass per *grid-cell*
    area (kg m-2); ``u/v`` are native-grid ice velocities in m s-1. Inputs
    must share (time, nj, ni) on the ACCESS-ESM1 300x360 grid. The supplied
    T/U coordinates are checked against the ancillary at every strait point
    before applying the fixed indices. No interpolation or vector rotation
    is introduced. ``hue/hun`` are bundled in metres at just these points.

    This reproduces APP4's indexed product and signed segment sums, with
    output (time, siline). Masked terms are skipped; a wholly missing strait
    stays missing, while zero quantity with masked velocity contributes zero.
    Time-dependent arrays remain lazy.

    CICE5's siu/siv are ice-area-weighted means, so multiplying by aice
    recovers the averaged area-velocity product (subject to ice masking).
    For mass this is an APP4-compatible *reconstruction*: monthly simass
    times area-weighted velocity does not recover the timestep mass flux.
    It excludes snow and is not the native combined sidmasstranx/y diagnostic.
    For legacy CICE4 only, pass grid-cell mean thickness ``hi`` (m) and an
    explicit ``ice_density`` (APP4 used 900 kg m-3) instead of mass per area.
    """
    units = {
        "area": ({"1", "fraction"}, "m2 s-1"),
        "mass": ({"kg m-2", "kg m^-2", "kg/m2", "kg/m^2"}, "kg s-1"),
    }
    if quantity_kind not in units:
        raise ValueError("quantity_kind must be 'area' or 'mass'")
    if ice_density is not None:
        if quantity_kind != "mass" or not np.isfinite(ice_density) or ice_density <= 0:
            raise ValueError(
                "ice_density must be positive and used only for mass transport"
            )
        if quantity.attrs.get("units") != "m":
            raise ValueError("ice_density requires grid-cell mean ice thickness in m")
        quantity = (quantity * ice_density).assign_attrs(units="kg m-2")
    accepted, output_units = units[quantity_kind]
    for name, field, allowed in (
        ("quantity", quantity, accepted),
        ("u", u, {"m/s", "m s-1", "m s^-1"}),
        ("v", v, {"m/s", "m s-1", "m s^-1"}),
    ):
        if field.attrs.get("units") not in allowed:
            raise ValueError(
                f"Line transport {name} units {field.attrs.get('units')!r}; "
                f"expected one of {sorted(allowed)}"
            )
        if set(field.dims) != {"time", "nj", "ni"}:
            raise ValueError(
                f"Line transport {name} must have (time, nj, ni) dimensions"
            )

    grid = _transport_grid()
    shape = tuple(grid["grid_shape"])
    for field in (quantity, u, v):
        if (field.sizes["nj"], field.sizes["ni"]) != shape:
            raise ValueError(
                f"APP4 strait indices require the ACCESS-ESM1 {shape} grid"
            )
    quantity, u, v = xr.align(quantity, u, v, join="exact", copy=False)
    points = [p for line in grid["lines"] for p in line["points"]]
    index = {
        d: xr.DataArray([p[k] for p in points], dims="transport_point")
        for d, k in (("nj", "j"), ("ni", "i"))
    }
    for name, coord in (("TLAT", tlat), ("TLON", tlon), ("ULAT", ulat), ("ULON", ulon)):
        if (
            set(coord.dims) != {"nj", "ni"}
            or tuple(coord.sizes[d] for d in ("nj", "ni")) != shape
        ):
            raise ValueError(f"{name} must describe the full ACCESS-ESM1 (nj, ni) grid")
        actual = coord.isel(index).values
        expected = np.array([p[name] for p in points])
        if coord.attrs.get("units") in {"radian", "radians"}:
            actual = np.rad2deg(actual)
        elif coord.attrs.get("units") not in {
            "degrees_north",
            "degrees_east",
            "degrees",
            "degree_north",
            "degree_east",
        }:
            raise ValueError(f"{name} must have degree or radian units")
        difference = actual - expected
        if name.endswith("LON"):
            difference = (difference + 180) % 360 - 180
        if not np.all(np.isfinite(difference) & (np.abs(difference) < 1e-4)):
            raise ValueError(f"{name} does not match the ACCESS-ESM1 APP4 strait grid")

    q = quantity.isel(index)
    velocity = xr.where(
        xr.DataArray([p["component"] == "x" for p in points], dims="transport_point"),
        u.isel(index),
        v.isel(index),
    )
    dtype = np.result_type(quantity.dtype, u.dtype, v.dtype, np.float32)
    lengths = xr.DataArray(
        np.array([p["edge_length"] for p in points], dtype=dtype),
        dims="transport_point",
    )
    flux = xr.where(q == 0, 0, q * velocity * lengths)
    start = 0
    transports = []
    for line in grid["lines"]:
        stop = start + len(line["points"])
        transports.append(
            flux.isel(transport_point=slice(start, stop)).sum(
                "transport_point", skipna=True, min_count=1
            )
        )
        start = stop
    result = xr.concat(transports, dim="siline").transpose("time", "siline")
    # Rebuild only the output coordinates: no sampled T/U coordinates, index
    # arrays, or source cell measures belong on the reduced variable.
    result = xr.DataArray(
        result.data,
        dims=("time", "siline"),
        coords={
            "time": quantity["time"],
            "siline": [line["name"] for line in grid["lines"]],
        },
        attrs={"units": output_units},
    )
    result["siline"].attrs = {"standard_name": "region", "long_name": "ocean passage"}
    result.attrs["comment"] = (
        "APP4-compatible strait reconstruction using native-grid velocity and "
        "hue/hun edge lengths; signs follow native x/y components. "
        + (
            "Ice mass only, excluding snow. Products of time-mean ice mass and "
            "velocity approximate the time-mean mass flux; this is not the native "
            "combined ice-and-snow transport diagnostic."
            if quantity_kind == "mass"
            else "Concentration and velocity retain their model time-averaging convention."
        )
    )
    return result


def calc_hemi_seaice(invar, tarea, hemi, extent=False):
    """
    Calculate hemispheric sea ice properties (area, volume, or extent).

    This function computes sea ice properties over either the northern or southern
    hemisphere. It can calculate:
    - Sea ice area/volume: by multiplying ice concentration/thickness with grid cell area
    - Sea ice extent: by summing grid cell areas where ice concentration ≥ 15%

    Parameters
    ----------
    invar : xarray.DataArray
        Input sea ice variable to process:
        - For area calculation: sea ice concentration (aice, fractional 0-1)
        - For volume calculation: sea ice thickness (hi, meters)
        - For extent calculation: sea ice concentration (aice, fractional 0-1)
    tarea : xarray.DataArray
        Grid cell area (m²). Must have compatible dimensions with invar.
    hemi : str
        Hemisphere to calculate over:
        - 'north': Northern hemisphere (latitude ≥ 0°)
        - 'south': Southern hemisphere (latitude < 0°)
    extent : bool, default False
        Calculation mode:
        - False: Calculate area/volume (invar × tarea)
        - True: Calculate extent (sum tarea where invar ≥ 0.15)

    Returns
    -------
    xarray.DataArray
        Hemispheric sum of the requested property:
        - Units: m² (for area/extent) or m³ (for volume)
        - Dimensions: Time dimension preserved, spatial dimensions summed

    Examples
    --------
    Calculate northern hemisphere sea ice extent:

    >>> extent = calc_hemi_seaice(aice, areacello, 'north', extent=True)

    Calculate southern hemisphere sea ice area:

    >>> area = calc_hemi_seaice(aice, areacello, 'south', extent=False)

    Calculate northern hemisphere sea ice volume:

    >>> volume = calc_hemi_seaice(ice_thickness, areacello, 'north', extent=False)

    Notes
    -----
    - Sea ice extent uses the standard 15% concentration threshold
      (per Notz et al. 2016 for CMIP variables)
    - Function automatically detects latitude coordinates in input data
    - Handles coordinate dimension alignment between invar and tarea
    - Time dimension is preserved; spatial dimensions are summed
    """
    # Get latitude coordinates from input data
    lat = None

    # Try to find latitude coordinate in invar first
    for coord_name in invar.coords:
        if "lat" in coord_name.lower():
            lat = invar.coords[coord_name]
            break

    # If not found in invar, try tarea
    if lat is None:
        for coord_name in tarea.coords:
            if "lat" in coord_name.lower():
                lat = tarea.coords[coord_name]
                break

    # If still not found, raise error
    if lat is None:
        raise ValueError("Cannot find latitude coordinate in input data")

    # Ensure latitude coordinates align with data dimensions
    invar_dims = invar.dims[1:] if invar.dims[0] == "time" else invar.dims

    # Align latitude coordinate with data dimensions if needed
    if any(d not in invar_dims for d in lat.dims):
        # Create mapping from lat dims to invar dims
        lat_mapping = {}
        for i, d in enumerate(lat.dims):
            if i < len(invar_dims):
                lat_mapping[d] = invar_dims[i]
        lat = lat.rename(lat_mapping)

    # if calculating extent sum carea and aice is used as filter
    # with volume and area invar is multiplied by carea first
    if extent:
        var = tarea.where(invar >= 0.15).where(invar <= 1.0)
    else:
        var = invar * tarea

    if hemi == "north":
        var = var.where(lat >= 0.0)
    elif hemi == "south":
        var = var.where(lat < 0.0)
    else:
        logging.error("Invalid hemisphere '%s'. Supported: 'north', 'south'.", hemi)
        raise ValueError(
            f"Invalid hemisphere '{hemi}'. Supported values: 'north' or 'south'."
        )

    # sum over latitude and longitude (skip time dimension)
    # This implements CMIP7 cell_methods: "area: sum time: mean"
    # (spatial sum, temporal mean)
    spatial_dims = [d for d in var.dims if d != "time"]
    vout = var.sum(dim=spatial_dims, skipna=True)

    return vout


def calc_seaice_extent(aice, areacello, region="north"):
    """
    Calculate hemispheric sea ice extent with CMIP-standard units.

    This function calculates sea ice extent (total area of grid cells with ≥15%
    ice concentration) for a specified hemisphere and converts the result to
    CMIP-standard units (10⁶ km²).

    Parameters
    ----------
    aice : xarray.DataArray
        Sea ice concentration (fractional, range 0-1).
        Must contain latitude coordinates for hemispheric selection.
    areacello : xarray.DataArray
        Ocean grid cell area in m².
        Must have compatible dimensions with aice.
    region : str, default 'north'
        Hemisphere to calculate extent for:
        - 'north': Northern hemisphere (latitude ≥ 0°)
        - 'south': Southern hemisphere (latitude < 0°)

    Returns
    -------
    xarray.DataArray
        Sea ice extent in 10⁶ km² (CMIP standard units).
        Time dimension preserved if present in input.

    Examples
    --------
    Calculate Arctic sea ice extent:

    >>> arctic_extent = calc_seaice_extent(aice, areacello, region="north")

    Calculate Antarctic sea ice extent:

    >>> antarctic_extent = calc_seaice_extent(aice, areacello, region="south")

    Notes
    -----
    - Uses the standard 15% concentration threshold for extent calculation
    - Output units are 10⁶ km² as required by CMIP protocols
    - This is a convenience wrapper around calc_hemi_seaice
    """
    # Calculate extent using the general function
    extent_m2 = calc_hemi_seaice(aice, areacello, region, extent=True)

    # Convert from m^2 to 1e6 km^2 for CMIP standard units
    extent_1e6km2 = extent_m2 / 1e12  # m^2 to 1e6 km^2

    return extent_1e6km2


#: Sea-ice concentration (fraction) at or above which a cell counts towards
#: sea-ice extent (Notz et al. 2016, "at least 15%").
_EXTENT_THRESHOLD = 0.15


def _hemisphere_sum(field, tarea, tlat, hemi):
    """Area-weighted sum of a per-unit-area sea-ice field over one hemisphere.

    The hemisphere is chosen from the cell-centre latitude ``tlat``, not from
    the grid index: on the ACCESS tripolar grid the equator falls between rows
    136 and 137, not at ``nj // 2``.

    ``field``, ``tarea`` and ``tlat`` must share the same horizontal
    dimensions.  xarray broadcasts arrays on differently named dimensions
    against each other, so pairing CICE ``(nj, ni)`` fields with an ocean
    ``(yt_ocean, xt_ocean)`` area builds a 4-D outer product instead of a
    cell-by-cell product; that is refused here rather than run out of memory.

    Parameters
    ----------
    field : xarray.DataArray
        Quantity per unit cell area, on (time, nj, ni).  Land cells are NaN.
    tarea : xarray.DataArray
        T-cell area in m², on (nj, ni).
    tlat : xarray.DataArray
        T-cell latitude in degrees north, on (nj, ni).
    hemi : str
        ``"north"`` (``tlat >= 0``) or ``"south"`` (``tlat < 0``).

    Returns
    -------
    xarray.DataArray
        ``field * tarea`` summed over the hemisphere, keeping time.
    """
    spatial_dims = [d for d in field.dims if d != "time"]
    for name, other in (("tarea", tarea), ("tlat", tlat)):
        if set(other.dims) != set(spatial_dims):
            raise ValueError(
                f"{name} dims {other.dims} do not match the sea-ice field's "
                f"horizontal dims {tuple(spatial_dims)}; use the CICE T-cell "
                "area and latitude (tarea, TLAT) from the same history file."
            )
    if hemi == "north":
        in_hemi = tlat >= 0.0
    elif hemi == "south":
        in_hemi = tlat < 0.0
    else:
        raise ValueError(
            f"Invalid hemisphere '{hemi}'. Supported values: 'north' or 'south'."
        )
    return (field * tarea.astype("float64")).where(in_hemi).sum(spatial_dims)


def calc_siarean(siconc, tarea, tlat):
    """
    Calculate Sea-Ice Area North.

    Sums sea ice concentration times grid cell area over the cells whose
    centre latitude is in the Northern Hemisphere.

    Parameters
    ----------
    siconc : xarray.DataArray
        Sea ice concentration as a fraction (0-1), on (time, nj, ni)
    tarea : xarray.DataArray
        T-cell area in m², on (nj, ni)
    tlat : xarray.DataArray
        T-cell latitude in degrees north, on (nj, ni)

    Returns
    -------
    xarray.DataArray
        Total sea ice area in the Northern Hemisphere (10⁶ km²)

    Examples
    --------
    >>> north_area = calc_siarean(aice, tarea, TLAT)
    """
    return _hemisphere_sum(siconc, tarea, tlat, "north") / 1e12  # m² to 1e6 km²


def calc_siareas(siconc, tarea, tlat):
    """
    Calculate Sea-Ice Area South.

    Sums sea ice concentration times grid cell area over the cells whose
    centre latitude is in the Southern Hemisphere.

    Parameters
    ----------
    siconc : xarray.DataArray
        Sea ice concentration as a fraction (0-1), on (time, nj, ni)
    tarea : xarray.DataArray
        T-cell area in m², on (nj, ni)
    tlat : xarray.DataArray
        T-cell latitude in degrees north, on (nj, ni)

    Returns
    -------
    xarray.DataArray
        Total sea ice area in the Southern Hemisphere (10⁶ km²)

    Examples
    --------
    >>> south_area = calc_siareas(aice, tarea, TLAT)
    """
    return _hemisphere_sum(siconc, tarea, tlat, "south") / 1e12  # m² to 1e6 km²


def calc_sivoln(sivol, tarea, tlat):
    """
    Calculate Sea-Ice Volume North.

    Sums sea ice volume per unit area times grid cell area over the cells
    whose centre latitude is in the Northern Hemisphere.

    Parameters
    ----------
    sivol : xarray.DataArray
        Sea ice volume per unit grid-cell area (m), on (time, nj, ni)
    tarea : xarray.DataArray
        T-cell area in m², on (nj, ni)
    tlat : xarray.DataArray
        T-cell latitude in degrees north, on (nj, ni)

    Returns
    -------
    xarray.DataArray
        Total sea ice volume in the Northern Hemisphere (10³ km³)

    Examples
    --------
    >>> north_volume = calc_sivoln(sivol, tarea, TLAT)
    """
    return _hemisphere_sum(sivol, tarea, tlat, "north") / 1e12  # m³ to 1e3 km³


def calc_sivols(sivol, tarea, tlat):
    """
    Calculate Sea-Ice Volume South.

    Sums sea ice volume per unit area times grid cell area over the cells
    whose centre latitude is in the Southern Hemisphere.

    Parameters
    ----------
    sivol : xarray.DataArray
        Sea ice volume per unit grid-cell area (m), on (time, nj, ni)
    tarea : xarray.DataArray
        T-cell area in m², on (nj, ni)
    tlat : xarray.DataArray
        T-cell latitude in degrees north, on (nj, ni)

    Returns
    -------
    xarray.DataArray
        Total sea ice volume in the Southern Hemisphere (10³ km³)

    Examples
    --------
    >>> south_volume = calc_sivols(sivol, tarea, TLAT)
    """
    return _hemisphere_sum(sivol, tarea, tlat, "south") / 1e12  # m³ to 1e3 km³


def calc_sisnmassn(sisnmass, tarea, tlat):
    """
    Calculate Snow Mass on Sea Ice North.

    Sums snow mass per unit area times grid cell area over the cells whose
    centre latitude is in the Northern Hemisphere.

    Parameters
    ----------
    sisnmass : xarray.DataArray
        Snow mass on sea ice per unit grid-cell area (kg/m²), on (time, nj, ni)
    tarea : xarray.DataArray
        T-cell area in m², on (nj, ni)
    tlat : xarray.DataArray
        T-cell latitude in degrees north, on (nj, ni)

    Returns
    -------
    xarray.DataArray
        Total snow mass on sea ice in the Northern Hemisphere (kg)

    Examples
    --------
    >>> north_snow_mass = calc_sisnmassn(sisnmass, tarea, TLAT)
    """
    return _hemisphere_sum(sisnmass, tarea, tlat, "north")


def calc_sisnmasss(sisnmass, tarea, tlat):
    """
    Calculate Snow Mass on Sea Ice South.

    Sums snow mass per unit area times grid cell area over the cells whose
    centre latitude is in the Southern Hemisphere.

    Parameters
    ----------
    sisnmass : xarray.DataArray
        Snow mass on sea ice per unit grid-cell area (kg/m²), on (time, nj, ni)
    tarea : xarray.DataArray
        T-cell area in m², on (nj, ni)
    tlat : xarray.DataArray
        T-cell latitude in degrees north, on (nj, ni)

    Returns
    -------
    xarray.DataArray
        Total snow mass on sea ice in the Southern Hemisphere (kg)

    Examples
    --------
    >>> south_snow_mass = calc_sisnmasss(sisnmass, tarea, TLAT)
    """
    return _hemisphere_sum(sisnmass, tarea, tlat, "south")


def calc_sisnconc(siconc):
    """
    Calculate Snow Area Fraction on Sea Ice (binary approximation).

    For ESM1.6/CICE, snow concentration is not a prognostic variable.
    Following Notz et al. (2016), this returns a binary value: 1 (100%)
    where sea ice is present and 0 where it is absent.

    Parameters
    ----------
    siconc : xarray.DataArray
        Sea ice concentration in % (0-100).

    Returns
    -------
    xarray.DataArray
        Snow area fraction in percent (0 or 100).

    References
    ----------
    Notz et al. (2016): "D1.5 Snow area fraction (sisnconc) Area fraction of
    the sea-ice surface that is covered by snow. In many models that do not
    explicitly resolve an areal fraction of snow, this variable will always
    be either 0 or 1."

    Examples
    --------
    >>> snc = calc_sisnconc(siconc)
    """
    return (siconc > 0) * 100.0


# Numerical ice-presence tolerance in percent (1e-4 as a fraction), separate
# from the 15% extent criterion. Suppress residual concentrations that amplify
# snow-mass noise at the ice edge (ACCESS-MOPPy #732).
_SNOW_THICKNESS_MIN_SICONC_PERCENT = 0.01


def calc_sisnthick(sisnmass, siconc, *, snow_density=317.0):
    """
    Calculate Snow Thickness on Sea Ice.

    For ESM1.6/CICE, true snow thickness (averaged over the snow-covered
    fraction of sea ice) is not prognostic. It is derived from snow mass
    and ice concentration using a fixed snow density, following the
    convention agreed for CMIP6/CMIP7 ESM1.6 submissions.

    Formula: sisnthick = sisnmass / (snow_density * siconc/100).
    The ACCESS-ESM1.6 mapping supplies the CICE5 density of 330 kg/m³.
    Concentrations below 0.01% are treated as ice-free and return 0.
    Missing snow mass or concentration remains missing.

    Parameters
    ----------
    sisnmass : xarray.DataArray
        Snow mass per unit area on sea ice (kg m-2), averaged over the
        full grid cell.
    siconc : xarray.DataArray
        Sea ice concentration in % (0-100).
    snow_density : float, default 317.0
        Snow density in kg/m³ used to produce the input snow mass. The default
        retains legacy behaviour for callers without a model-specific value;
        ACCESS-ESM1.6 uses 330.0, as defined by CICE5's ACCESS driver.

    Returns
    -------
    xarray.DataArray
        Snow thickness in metres (m), averaged over the snow-covered area.

    Examples
    --------
    >>> snd = calc_sisnthick(sisnmass, siconc, snow_density=330.0)
    """
    valid = sisnmass.notnull() & siconc.notnull()
    ice_present = siconc >= _SNOW_THICKNESS_MIN_SICONC_PERCENT
    # Mask the denominator first, including for lazy Dask evaluation, so the
    # division never evaluates at zero or negligible concentration.
    siconc_frac = (siconc / 100.0).where(ice_present)
    thickness = sisnmass / (snow_density * siconc_frac)
    return thickness.where(ice_present, 0.0).where(valid)


def _extent_mask(siconc):
    """1 where a cell counts towards sea-ice extent, 0 elsewhere (incl. land)."""
    return ((siconc >= _EXTENT_THRESHOLD) & (siconc <= 1.0)).astype("float64")


def calc_siextentn(siconc, tarea, tlat):
    """
    Calculate Sea-Ice Extent North.

    Sums the area of the cells with at least 15% sea ice concentration whose
    centre latitude is in the Northern Hemisphere.

    Parameters
    ----------
    siconc : xarray.DataArray
        Sea ice concentration as a fraction (0-1), on (time, nj, ni)
    tarea : xarray.DataArray
        T-cell area in m², on (nj, ni)
    tlat : xarray.DataArray
        T-cell latitude in degrees north, on (nj, ni)

    Returns
    -------
    xarray.DataArray
        Total sea ice extent in the Northern Hemisphere (10⁶ km²)

    Examples
    --------
    >>> north_extent = calc_siextentn(aice, tarea, TLAT)
    """
    extent = _hemisphere_sum(_extent_mask(siconc), tarea, tlat, "north")
    return extent / 1e12  # m² to 1e6 km²


def calc_siextents(siconc, tarea, tlat):
    """
    Calculate Sea-Ice Extent South.

    Sums the area of the cells with at least 15% sea ice concentration whose
    centre latitude is in the Southern Hemisphere.

    Parameters
    ----------
    siconc : xarray.DataArray
        Sea ice concentration as a fraction (0-1), on (time, nj, ni)
    tarea : xarray.DataArray
        T-cell area in m², on (nj, ni)
    tlat : xarray.DataArray
        T-cell latitude in degrees north, on (nj, ni)

    Returns
    -------
    xarray.DataArray
        Total sea ice extent in the Southern Hemisphere (10⁶ km²)

    Examples
    --------
    >>> south_extent = calc_siextents(aice, tarea, TLAT)
    """
    extent = _hemisphere_sum(_extent_mask(siconc), tarea, tlat, "south")
    return extent / 1e12  # m² to 1e6 km²
