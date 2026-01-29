#!/usr/bin/env python
# This file contains a collection of functions to calculate sea ice
# derived variables from ACCESS model output.
#
# To propose new calculations and/or update to existing ones see documentation:
#
# and open a new issue on github.

import logging


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
        logging.error(f"invalid hemisphere: {hemi}")
        raise ValueError(f"invalid hemisphere: {hemi}")

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


def calc_siarean(siconc, tarea):
    """
    Calculate Sea-Ice Area North.

    Computes the total sea ice area in the Northern Hemisphere by multiplying
    sea ice concentration by grid cell area and summing over the northern half
    of the domain.

    Parameters
    ----------
    siconc : xarray.DataArray
        Sea ice concentration in percent (0-100%)
        Must have dimensions including 'ni' and 'nj'
    tarea : xarray.DataArray
        Grid cell area in m²
        Must have dimensions compatible with siconc
        Note: tarea is equivalent to areacello in CMIP terminology

    Returns
    -------
    xarray.DataArray
        Total sea ice area in the Northern Hemisphere (10⁶ km²)

    Examples
    --------
    >>> north_area = calc_siarean(siconc, tarea)
    """
    return (
        (siconc / 100 * tarea)
        .isel(nj=slice(len(siconc.nj) // 2, None))
        .sum(["ni", "nj"])
        / 1e12  # Convert from m² to 1e6 km²
    )


def calc_siareas(siconc, tarea):
    """
    Calculate Sea-Ice Area South.

    Computes the total sea ice area in the Southern Hemisphere by multiplying
    sea ice concentration by grid cell area and summing over the southern half
    of the domain.

    Parameters
    ----------
    siconc : xarray.DataArray
        Sea ice concentration in percent (0-100%)
        Must have dimensions including 'ni' and 'nj'
    tarea : xarray.DataArray
        Grid cell area in m²
        Must have dimensions compatible with siconc

    Returns
    -------
    xarray.DataArray
        Total sea ice area in the Southern Hemisphere (10⁶ km²)

    Examples
    --------
    >>> south_area = calc_siareas(siconc, tarea)
    """
    return (
        (siconc / 100 * tarea).isel(nj=slice(0, len(siconc.nj) // 2)).sum(["ni", "nj"])
        / 1e12  # Convert from m² to 1e6 km²
    )


def calc_sivoln(sivol, tarea):
    """
    Calculate Sea-Ice Volume North.

    Computes the total sea ice volume in the Northern Hemisphere by multiplying
    sea ice volume by grid cell area and summing over the northern half
    of the domain.

    Parameters
    ----------
    sivol : xarray.DataArray
        Sea ice volume per unit area (m)
        Must have dimensions including 'ni' and 'nj'
    tarea : xarray.DataArray
        Grid cell area in m²
        Must have dimensions compatible with sivol

    Returns
    -------
    xarray.DataArray
        Total sea ice volume in the Northern Hemisphere (10³ km³)

    Examples
    --------
    >>> north_volume = calc_sivoln(sivol, tarea)
    """
    return (
        (sivol * tarea).isel(nj=slice(len(sivol.nj) // 2, None)).sum(["ni", "nj"])
        / 1e9  # Convert from m³ to 1e3 km³
    )


def calc_sivols(sivol, tarea):
    """
    Calculate Sea-Ice Volume South.

    Computes the total sea ice volume in the Southern Hemisphere by multiplying
    sea ice volume by grid cell area and summing over the southern half
    of the domain.

    Parameters
    ----------
    sivol : xarray.DataArray
        Sea ice volume per unit area (m)
        Must have dimensions including 'ni' and 'nj'
    tarea : xarray.DataArray
        Grid cell area in m²
        Must have dimensions compatible with sivol

    Returns
    -------
    xarray.DataArray
        Total sea ice volume in the Southern Hemisphere (10³ km³)

    Examples
    --------
    >>> south_volume = calc_sivols(sivol, tarea)
    """
    return (
        (sivol * tarea).isel(nj=slice(0, len(sivol.nj) // 2)).sum(["ni", "nj"])
        / 1e9  # Convert from m³ to 1e3 km³
    )


def calc_sisnmassn(sisnmass, tarea):
    """
    Calculate Snow Mass on Sea Ice North.

    Computes the total snow mass on sea ice in the Northern Hemisphere by
    multiplying snow mass by grid cell area and summing over the northern
    half of the domain.

    Parameters
    ----------
    sisnmass : xarray.DataArray
        Snow mass per unit area on sea ice (kg/m²)
        Must have dimensions including 'ni' and 'nj'
    tarea : xarray.DataArray
        Grid cell area in m²
        Must have dimensions compatible with sisnmass

    Returns
    -------
    xarray.DataArray
        Total snow mass on sea ice in the Northern Hemisphere (kg)

    Examples
    --------
    >>> north_snow_mass = calc_sisnmassn(sisnmass, tarea)
    """
    return (
        (sisnmass * tarea).isel(nj=slice(len(sisnmass.nj) // 2, None)).sum(["ni", "nj"])
    )


def calc_sisnmasss(sisnmass, tarea):
    """
    Calculate Snow Mass on Sea Ice South.

    Computes the total snow mass on sea ice in the Southern Hemisphere by
    multiplying snow mass by grid cell area and summing over the southern
    half of the domain.

    Parameters
    ----------
    sisnmass : xarray.DataArray
        Snow mass per unit area on sea ice (kg/m²)
        Must have dimensions including 'ni' and 'nj'
    tarea : xarray.DataArray
        Grid cell area in m²
        Must have dimensions compatible with sisnmass

    Returns
    -------
    xarray.DataArray
        Total snow mass on sea ice in the Southern Hemisphere (kg)

    Examples
    --------
    >>> south_snow_mass = calc_sisnmasss(sisnmass, tarea)
    """
    return (sisnmass * tarea).isel(nj=slice(0, len(sisnmass.nj) // 2)).sum(["ni", "nj"])


def calc_siextentn(siconc, tarea):
    """
    Calculate Sea-Ice Extent North.

    Computes the total sea ice extent in the Northern Hemisphere by summing
    grid cell areas where sea ice concentration exceeds 15% over the northern
    half of the domain.

    Parameters
    ----------
    siconc : xarray.DataArray
        Sea ice concentration in percent (0-100%)
        Must have dimensions including 'ni' and 'nj'
    tarea : xarray.DataArray
        Grid cell area in m²
        Must have dimensions compatible with siconc

    Returns
    -------
    xarray.DataArray
        Total sea ice extent in the Northern Hemisphere (10⁶ km²)

    Examples
    --------
    >>> north_extent = calc_siextentn(siconc, tarea)
    """
    return (
        ((siconc > 15) * tarea)
        .isel(nj=slice(len(siconc.nj) // 2, None))
        .sum(["ni", "nj"])
        / 1e12  # Convert from m² to 1e6 km²
    )


def calc_siextents(siconc, tarea):
    """
    Calculate Sea-Ice Extent South.

    Computes the total sea ice extent in the Southern Hemisphere by summing
    grid cell areas where sea ice concentration exceeds 15% over the southern
    half of the domain.

    Parameters
    ----------
    siconc : xarray.DataArray
        Sea ice concentration in percent (0-100%)
        Must have dimensions including 'ni' and 'nj'
    tarea : xarray.DataArray
        Grid cell area in m²
        Must have dimensions compatible with siconc

    Returns
    -------
    xarray.DataArray
        Total sea ice extent in the Southern Hemisphere (10⁶ km²)

    Examples
    --------
    >>> south_extent = calc_siextents(siconc, tarea)
    """
    return (
        ((siconc > 15) * tarea).isel(nj=slice(0, len(siconc.nj) // 2)).sum(["ni", "nj"])
        / 1e12  # Convert from m² to 1e6 km²
    )
