#!/usr/bin/env python
import xarray as xr


def calc_global_ave_ocean(var, rho_dzt, area_t):
    """Calculate mass-weighted global average of an ocean variable.

    This function calculates a proper mass-weighted global average of any ocean
    variable (typically temperature), accounting for varying grid cell areas and
    ocean mass per unit area.

    Parameters
    ----------
    var : xarray.DataArray
        Ocean variable to average (e.g., temperature)
        Dimensions should include (time, depth, lat, lon) or subset thereof
    rho_dzt: xarray.DataArray
        Sea water mass per unit area with dimensions (time, depth, lat, lon)
        Units: kg/m²
    area_t : xarray.DataArray
        Grid cell areas with dimensions (lat, lon)
        Units: m²

    Returns
    -------
    vnew : xarray.DataArray
        Mass-weighted global average of the input variable
        Dimensions: (time,) if input has depth dimension, otherwise reduced dimensions
    """
    # Calculate total mass per grid cell (mass per unit area × area)
    total_mass = rho_dzt * area_t

    # Determine which axes to average over based on input dimensions
    # Get spatial dimension names for ocean data
    spatial_dims = [
        dim for dim in var.dims if dim in ["st_ocean", "yt_ocean", "xt_ocean"]
    ]

    # Calculate mass-weighted average using xarray's weighted functionality
    vnew = var.weighted(total_mass).mean(dim=spatial_dims)
    return vnew


def calc_rsdoabsorb(sw_heat: xr.DataArray, swflux: xr.DataArray) -> xr.DataArray:
    """Calculate net rate of absorption of shortwave energy in ocean layer.

    CMIP variable: rsdoabsorb

    This function combines penetrative shortwave heating with surface shortwave flux
    for the top ocean layer, and uses only penetrative heating for deeper layers.

    Parameters
    ----------
    sw_heat : xarray.DataArray
        Penetrative shortwave heating with dimensions (time, st_ocean, yt_ocean, xt_ocean)
        Units: W/m^2
    swflux : xarray.DataArray
        Shortwave flux into ocean (>0 heats ocean) with dimensions (time, yt_ocean, xt_ocean)
        Units: W/m^2

    Returns
    -------
    rsdoabsorb : xarray.DataArray
        Net rate of absorption of shortwave energy in ocean layer (rsdoabsorb)
        Same dimensions as sw_heat input
        Units: W/m^2
    """
    # Surface layer: add flux to heat
    surface_layer = (sw_heat.isel(st_ocean=0) + swflux).expand_dims("st_ocean")

    # Deeper layers: use heat as-is
    deeper_layers = sw_heat.isel(st_ocean=slice(1, None))

    # Concatenate surface and deeper layers
    rsdoabsorb = xr.concat([surface_layer, deeper_layers], dim="st_ocean")

    return rsdoabsorb


def calc_zostoga(pot_temp, dzt, areacello, depth_coord="st_ocean"):
    """Calculate Global Average Thermosteric Sea Level Change.

    This function computes thermosteric sea level change by comparing
    in-situ density to reference density at 4°C, integrating over depth,
    and computing the global average.

    Uses simplified density calculations suitable for thermosteric computations.

    Parameters
    ----------
    pot_temp : xarray.DataArray
        Potential temperature in degrees Celsius
        Dimensions: (time, depth, lat, lon)
    dzt : xarray.DataArray
        Model level thickness with same dimensions as pot_temp
        Units: m
    areacello : xarray.DataArray
        Ocean grid cell areas
        Dimensions: (lat, lon)
        Units: m²
    depth_coord : str, optional
        Name of the depth coordinate, default 'st_ocean'

    Returns
    -------
    zostoga : xarray.DataArray
        Global Average Thermosteric Sea Level Change
        Dimensions: (time,)
        Units: m

    Notes
    -----
    Uses simplified seawater density approximation:
    - Reference salinity: 35 PSU
    - Reference temperature: 4°C
    - Linear thermal expansion coefficient: ~2e-4 /°C
    """

    # Simple approximation for seawater density temperature dependence
    # ρ(T) ≈ ρ₀[1 - α(T - T₀)] where α ≈ 2e-4 /°C for seawater
    rho_0 = 1025.0  # Reference density at 4°C, kg/m³
    temp_ref = 4.0  # Reference temperature, °C
    alpha = 2e-4  # Thermal expansion coefficient, /°C

    # Calculate density at in-situ temperature
    rho_insitu = rho_0 * (1.0 - alpha * (pot_temp - temp_ref))

    # Calculate reference density at 4°C
    rho_ref = rho_0  # At reference temperature

    # Calculate thermosteric height change and integrate over depth
    # (1 - ρ/ρ₄) gives fractional density difference
    thermo_height = (1.0 - rho_insitu / rho_ref) * dzt
    integrated_height = thermo_height.sum(dim=depth_coord, skipna=True)

    # Calculate area-weighted global average
    zostoga = integrated_height.weighted(areacello).mean(dim=["yt_ocean", "xt_ocean"])

    return zostoga


def calc_ocean_depth_integral(var, rho, dzt, depth_coord="st_ocean"):
    """Calculate depth integral of product of sea water density and ocean variable.

    This function computes the depth-integrated product of density and any
    ocean variable. Commonly used for calculating column-integrated properties
    like salt content, heat content, etc.

    Parameters
    ----------
    var : xarray.DataArray
        Ocean variable to integrate (e.g., salinity, temperature, tracers)
        Dimensions: (time, depth, lat, lon)
    rho : xarray.DataArray
        Sea water density with same dimensions as var
        Units: kg/m³
    dzt : xarray.DataArray
        Model level thickness with same dimensions as var
        Units: m
    depth_coord : str, optional
        Name of the depth coordinate, default 'st_ocean'

    Returns
    -------
    integral : xarray.DataArray
        Depth-integrated product of density and variable
        Dimensions: (time, lat, lon)
        Units: [var_units] × kg/m²

    Examples
    --------
    # For salinity content (somint):
    somint = calc_ocean_depth_integral(salinity, density, dzt)

    # For heat content:
    heat_content = calc_ocean_depth_integral(temperature, density, dzt)
    """
    # Calculate product of variable, density, and layer thickness
    # This gives the "content" per layer: var × ρ × Δz
    layer_content = var * rho * dzt

    # Integrate over depth by summing all layers
    # skipna=True handles any missing values gracefully
    integral = layer_content.sum(dim=depth_coord, skipna=True)

    return integral


def calc_overturning_streamfunction(
    ty_trans,
    gm_trans=None,
    submeso_trans=None,
    depth_coord="st_ocean",
    lon_coord="xu_ocean",
    to_sverdrups=False,
):
    """Calculate ocean overturning mass streamfunction.

    Computes the meridional overturning circulation by:
    1. Summing meridional transport over longitude
    2. Cumulative summing over depth
    3. Adding GM and submeso components if provided
    4. Removing barotropic component

    Parameters
    ----------
    ty_trans : xarray.DataArray
        Meridional mass transport (ty_trans)
        Dimensions: (time, depth, lat, lon)
        Units: kg/s
    gm_trans : xarray.DataArray, optional
        GM (Gent-McWilliams) transport component
        Same dimensions as ty_trans
    submeso_trans : xarray.DataArray, optional
        Submesoscale transport component
        Same dimensions as ty_trans
    depth_coord : str, optional
        Name of depth coordinate, default 'st_ocean'
    lon_coord : str, optional
        Name of longitude coordinate, default 'xu_ocean'
    to_sverdrups : bool, optional
        If True, convert from kg/s to sverdrups (×10⁹), default False

    Returns
    -------
    streamfunction : xarray.DataArray
        Ocean overturning mass streamfunction
        Dimensions: (time, depth, lat)
        Units: kg/s (or Sv if to_sverdrups=True)
    """

    # Sum meridional transport over longitude
    ty_zonal_sum = ty_trans.sum(dim=lon_coord)

    # Calculate overturning streamfunction via cumulative sum over depth
    streamfunction = ty_zonal_sum.cumsum(dim=depth_coord)

    # Add GM component if provided
    if gm_trans is not None:
        gm_zonal_sum = gm_trans.sum(dim=lon_coord)
        streamfunction = streamfunction + gm_zonal_sum

    # Add submesoscale component if provided
    if submeso_trans is not None:
        submeso_zonal_sum = submeso_trans.sum(dim=lon_coord)
        streamfunction = streamfunction + submeso_zonal_sum

    # Remove barotropic component (depth-integrated transport)
    # This ensures the streamfunction goes to zero at the bottom
    barotropic = ty_zonal_sum.sum(dim=depth_coord)
    streamfunction = streamfunction - barotropic

    # Convert to sverdrups if requested
    if to_sverdrups:
        streamfunction = streamfunction * 1e-9  # kg/s to Sv (10⁹ kg/s)

    return streamfunction


def calc_total_mass_transport(
    resolved_trans, gm_trans=None, submeso_trans=None, depth_coord="st_ocean"
):
    """Calculate total ocean mass transport including GM and submesoscale components.

    This function computes the corrected umo/vmo transport by combining:
    1. Resolved transport (tx_trans or ty_trans)
    2. GM (Gent-McWilliams) transport component via vertical difference
    3. Submesoscale transport component via vertical difference

    The vertical difference operation follows:
    diffz_gm = diff([zero_layer; gm_trans], axis=depth)
    where zero_layer is prepended to account for surface boundary conditions.

    Parameters
    ----------
    resolved_trans : xarray.DataArray
        Resolved transport (tx_trans or ty_trans)
        Dimensions: (time, depth, lat, lon)
        Units: kg/s
    gm_trans : xarray.DataArray, optional
        GM transport component (tx_trans_gm or ty_trans_gm)
        Same dimensions as resolved_trans
        Units: kg/s
    submeso_trans : xarray.DataArray, optional
        Submesoscale transport component (tx_trans_submeso or ty_trans_submeso)
        Same dimensions as resolved_trans
        Units: kg/s
    depth_coord : str, optional
        Name of depth coordinate, default 'st_ocean'

    Returns
    -------
    total_transport : xarray.DataArray
        Total mass transport including all components
        Same dimensions as resolved_trans
        Units: kg/s

    Examples
    --------
    # For umo (zonal mass transport):
    umo = calc_total_mass_transport(tx_trans, tx_trans_gm, tx_trans_submeso)

    # For vmo (meridional mass transport):
    vmo = calc_total_mass_transport(ty_trans, ty_trans_gm, ty_trans_submeso)

    Notes
    -----
    The vertical difference operation accounts for the fact that GM and submeso
    transports represent volume fluxes that need to be converted to proper
    mass transports by taking vertical derivatives with appropriate boundary
    conditions (zero at surface).

    Physical justification:
    The CMIP6/7 variables umo and vmo should represent the total ocean mass
    transport, including both resolved and parameterized components:

    1. **Resolved transport** (tx_trans/ty_trans): Direct advection by the
       resolved velocity field

    2. **GM transport**: Represents bolus transport due to mesoscale eddies
       parameterized by the Gent-McWilliams scheme. This is essential for
       coarse resolution models where eddies are not explicitly resolved.

    3. **Submesoscale transport**: Parameterizes transport by sub-mesoscale
       processes (mixed layer instabilities, etc.) that operate at scales
       smaller than the model grid.

    The inclusion of all transport components ensures that CMORized umo/vmo
    fields accurately represent the total mass transport for climate analysis,
    consistent with CMIP data request requirements.

    References
    ----------
    - Gent, P. R., & McWilliams, J. C. (1990). Isopycnal mixing in ocean
      circulation models. Journal of Physical Oceanography, 20(1), 150-155.
    - Griffies, S. M. (2012). Elements of the Modular Ocean Model (MOM).
      GFDL Ocean Group Technical Report No. 7.
    - CMIP6 Model Output Requirements:
      https://pcmdi.llnl.gov/CMIP6/Guide/dataUsers.html
    """

    # Start with resolved transport
    total_transport = resolved_trans

    def _calc_diffz(transport_3d, depth_coord):
        """Calculate vertical difference with zero surface layer."""
        if transport_3d is None:
            return None

        # Create zero layer with same horizontal dimensions as transport
        # but only one depth level at the surface
        zero_layer = transport_3d.isel({depth_coord: 0}) * 0.0
        zero_layer = zero_layer.expand_dims(
            depth_coord, axis=transport_3d.dims.index(depth_coord)
        )

        # Concatenate zero layer on top of 3D transport
        transport_with_zero = xr.concat([zero_layer, transport_3d], dim=depth_coord)

        # Calculate vertical difference
        # This gives the transport divergence contribution
        diffz = transport_with_zero.diff(dim=depth_coord)

        return diffz

    # Add GM component if provided
    if gm_trans is not None:
        diffz_gm = _calc_diffz(gm_trans, depth_coord)
        total_transport = total_transport + diffz_gm

    # Add submesoscale component if provided
    if submeso_trans is not None:
        diffz_submeso = _calc_diffz(submeso_trans, depth_coord)
        total_transport = total_transport + diffz_submeso

    return total_transport


def calc_umo_corrected(
    tx_trans, tx_trans_gm=None, tx_trans_submeso=None, depth_coord="st_ocean"
):
    """Calculate corrected zonal mass transport (umo) including GM and submeso terms.

    This is a convenience function that calls calc_total_mass_transport
    with the appropriate zonal transport components.

    Parameters
    ----------
    tx_trans : xarray.DataArray
        Resolved zonal mass transport
        Units: kg/s
    tx_trans_gm : xarray.DataArray, optional
        GM zonal transport component
        Units: kg/s
    tx_trans_submeso : xarray.DataArray, optional
        Submesoscale zonal transport component
        Units: kg/s
    depth_coord : str, optional
        Name of depth coordinate, default 'st_ocean'

    Returns
    -------
    umo : xarray.DataArray
        Corrected zonal mass transport (umo)
        Units: kg/s
    """
    return calc_total_mass_transport(
        tx_trans, tx_trans_gm, tx_trans_submeso, depth_coord
    )


def calc_vmo_corrected(
    ty_trans, ty_trans_gm=None, ty_trans_submeso=None, depth_coord="st_ocean"
):
    """Calculate corrected meridional mass transport (vmo) including GM and submeso terms.

    This is a convenience function that calls calc_total_mass_transport
    with the appropriate meridional transport components.

    Parameters
    ----------
    ty_trans : xarray.DataArray
        Resolved meridional mass transport
        Units: kg/s
    ty_trans_gm : xarray.DataArray, optional
        GM meridional transport component
        Units: kg/s
    ty_trans_submeso : xarray.DataArray, optional
        Submesoscale meridional transport component
        Units: kg/s
    depth_coord : str, optional
        Name of depth coordinate, default 'st_ocean'

    Returns
    -------
    vmo : xarray.DataArray
        Corrected meridional mass transport (vmo)
        Units: kg/s
    """
    return calc_total_mass_transport(
        ty_trans, ty_trans_gm, ty_trans_submeso, depth_coord
    )


def ocean_floor(var, depth_dim="st_ocean"):
    """Extract the bottom-most (seafloor) value from an ocean variable using fully lazy operations.

    This function finds the deepest valid (non-NaN) value along the depth
    dimension for each horizontal grid point, effectively extracting the
    seafloor value of any ocean variable.

    Parameters
    ----------
    var : xarray.DataArray
        Ocean variable with depth dimension
        Dimensions: (..., depth, lat, lon)
    depth_dim : str, optional
        Name of the depth dimension, default "st_ocean"

    Returns
    -------
    xarray.DataArray
        Bottom-most valid values of the input variable
        Dimensions: (..., lat, lon) - depth dimension removed

    Notes
    -----
    - Fully lazy operation using xarray/dask
    - Uses argmax on reversed valid mask for guaranteed lazy computation
    - Preserves chunking and coordinates
    """
    # Create a mask for valid (non-NaN) values
    valid_mask = ~var.isnull()

    # Reverse the depth dimension to find the last valid value
    # by finding the first valid value from the bottom
    reversed_mask = valid_mask.isel({depth_dim: slice(None, None, -1)})

    # Find the index of the first valid value from bottom (which is the last from top)
    # argmax on reversed boolean array gives us the first True from bottom
    bottom_idx_reversed = reversed_mask.argmax(dim=depth_dim)

    # Convert back to original indexing
    depth_size = var.sizes[depth_dim]
    bottom_idx = depth_size - 1 - bottom_idx_reversed

    # Handle case where there are no valid values (all NaN)
    # If no valid data, argmax returns 0, so we need to mask these cases
    has_valid_data = valid_mask.any(dim=depth_dim)
    bottom_idx = bottom_idx.where(has_valid_data, 0)

    # Use isel with integer index (guaranteed lazy)
    # Need to broadcast bottom_idx to match var's shape for vectorized indexing
    seafloor_values = var.isel({depth_dim: bottom_idx})

    # Mask out points where there were no valid values originally
    seafloor_values = seafloor_values.where(has_valid_data)

    return seafloor_values


def calc_areacello(area_t, ht, drop_time=True):
    """Calculate ocean grid-cell area for sea floor.

    This function calculates areacello by using the tracer grid cell areas
    but masking out land cells where the bathymetric depth is zero.
    Fully lazy operation using xarray/dask.

    Parameters
    ----------
    area_t : xarray.DataArray
        Tracer grid cell areas
        Dimensions: (lat, lon) or (yt_ocean, xt_ocean)
        Units: m²
    ht : xarray.DataArray
        Bathymetric depth (positive values)
        Same horizontal dimensions as area_t
        Units: m
    drop_time : bool, optional
        Whether to drop the time dimension from the result, default True.
        Since areacello is time-independent, this should typically be True.

    Returns
    -------
    areacello : xarray.DataArray
        Ocean grid-cell area for sea floor, with land cells masked
        Dimensions: (lat, lon) or (yt_ocean, xt_ocean) if drop_time=True,
                   otherwise same dimensions as area_t
        Units: m²

    Notes
    -----
    - Fully lazy operation using xarray/dask
    - Land cells are identified where ht == 0 and are masked using _FillValue
    - This ensures areacello only represents actual ocean grid cells
    - Preserves chunking and coordinates
    - Time dimension is dropped by default since areacello is time-independent
    """
    # Mask land cells where bathymetric depth is zero
    # ht == 0 indicates land cells that should be masked
    # Use _FillValue if available, otherwise fall back to default
    # This is a fully lazy operation that preserves dask chunking
    fill_value = getattr(area_t, "_FillValue", None)
    areacello = area_t.where(ht != 0.0, other=fill_value)

    # Drop time dimension if requested (default behavior)
    # Since areacello is time-independent, we typically want to remove time dimension
    # This operation is fully lazy - dimension checking and isel/drop_vars preserve dask chunking
    if drop_time and "time" in areacello.dims:
        areacello = areacello.isel(time=0).drop_vars("time", errors="ignore")

    return areacello
