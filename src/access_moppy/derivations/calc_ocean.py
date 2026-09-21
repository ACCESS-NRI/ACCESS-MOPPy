#!/usr/bin/env python
import logging
import warnings

import xarray as xr

logger = logging.getLogger(__name__)


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


def calc_zostoga(pot_temp, dzt_ref, areacello, temp_ref=None, depth_coord="st_ocean"):
    """Calculate Global Average Thermosteric Sea Level Change.

    Computes thermosteric sea level using a temperature-dependent thermal
    expansion coefficient integrated against a reference layer-thickness field.
    Using a fixed reference thickness (rather than the time-varying model dzt)
    is required for Boussinesq models such as MOM5 to isolate the thermosteric
    signal from the barotropic free-surface contribution.

    All operations are dask-lazy: no ``.compute()`` or ``.values`` calls are
    made, so large datasets can be processed out-of-core.

    Parameters
    ----------
    pot_temp : xarray.DataArray
        Potential temperature in Kelvin (K), as provided by the model.
        The function converts to degrees Celsius internally before applying
        the Gill 1982 thermal expansion formula.
        Dimensions: (time, depth, lat, lon)
    dzt_ref : xarray.DataArray
        Reference (time-invariant) model level thickness.
        Dimensions: (depth, lat, lon) or (depth,)
        Units: m
    areacello : xarray.DataArray
        Ocean grid cell areas.
        Dimensions: (lat, lon)
        Units: m²
    temp_ref : float or xarray.DataArray or None, optional
        Reference temperature in Kelvin (K), matching the units of ``pot_temp``.
        If None (default), falls back to 277.15 K (= 4 °C) with a scientific
        warning.  Using a scalar 277.15 K is a temporary approximation — it
        computes an absolute steric height relative to 4 °C rather than a
        temporal anomaly, which is not CMIP-compliant.  The result will carry
        a large, physically meaningless baseline offset whose magnitude depends
        on how far the mean ocean temperature departs from 4 °C.  For a
        physically correct result, pass a 3-D reference-period mean temperature
        field with dimensions (depth, lat, lon) in K (e.g. the piControl
        year-1 mean of ``pot_temp``).
    depth_coord : str, optional
        Name of the depth coordinate, default 'st_ocean'

    Returns
    -------
    zostoga : xarray.DataArray
        Global Average Thermosteric Sea Level Change.
        Dimensions: (time,)
        Units: m

    Notes
    -----
    Uses a temperature-dependent thermal expansion coefficient (Gill 1982):
        α(T) ≈ 5.27×10⁻⁵ + 7.1×10⁻⁶·T − 4×10⁻⁸·T²  [°C⁻¹]
    valid for S ≈ 35 PSU at surface pressure. This avoids the large errors
    introduced by a constant α in cold deep and polar waters.
    """
    if temp_ref is None:
        warnings.warn(
            "calc_zostoga: 'temp_ref' was not provided.  Falling back to a "
            "scalar reference temperature of 277.15 K (= 4 °C).\n"
            "Scientific implications:\n"
            "  * zostoga will be an ABSOLUTE steric height relative to 4 °C, "
            "not a temporal anomaly as required by CMIP.  The result will "
            "carry a large, physically meaningless baseline offset whose "
            "magnitude depends on how far the mean ocean temperature departs "
            "from 4 °C.\n"
            "  * Differences between time steps are still meaningful, but the "
            "absolute values and any multi-model comparison will be incorrect.\n"
            "To fix this, pass a 3-D reference-period mean temperature field "
            "in K (e.g. the piControl year-1 mean of pot_temp) as 'temp_ref'.",
            UserWarning,
            stacklevel=2,
        )
        temp_ref = 277.15

    # Convert both pot_temp and temp_ref from Kelvin to Celsius.
    # All inputs are expected in K (matching the model/mapping convention);
    # the Gill 1982 α(T) formula and the (T − T_ref) anomaly both require °C.
    pot_temp_c = pot_temp - 273.15
    temp_ref_c = temp_ref - 273.15

    # Temperature-dependent thermal expansion coefficient (Gill 1982, simplified)
    # α(T) ≈ 5.27e-5 + 7.1e-6·T − 4e-8·T²  [°C⁻¹], valid for S≈35 PSU
    alpha = 5.27e-5 + 7.1e-6 * pot_temp_c - 4e-8 * pot_temp_c**2

    # Thermosteric height contribution: α(T) × (T − T_ref) × dz_ref
    # dzt_ref is time-invariant so it does not carry the free-surface
    # barotropic signal that is present in the model's time-varying dzt.
    thermo_height = alpha * (pot_temp_c - temp_ref_c) * dzt_ref

    # Integrate over depth (lazy with dask)
    integrated_height = thermo_height.sum(dim=depth_coord, skipna=True)

    # Area-weighted global average (lazy with dask)
    zostoga = integrated_height.weighted(areacello.fillna(0)).mean(
        dim=["yt_ocean", "xt_ocean"]
    )

    return zostoga


def calc_overturning_streamfunction(
    ty_trans,
    gm_trans=None,
    submeso_trans=None,
    basin_mask=None,
    depth_coord="st_ocean",
    lon_coord="xt_ocean",
    lat_coord="yu_ocean",
    to_sverdrups=False,
):
    """Calculate ocean overturning mass streamfunction.

    Computes the meridional overturning circulation by:
    1. Summing meridional transport over longitude, within a basin
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
    basin_mask : xarray.DataArray, optional
        Region flags on the tracer grid, as returned by :func:`calc_basin`,
        normally supplied by a nested ``calc_basin`` call in the mapping. When
        given, the streamfunction is computed separately over each of the three
        CMIP basins and the result gains a ``basin`` dimension. When omitted,
        the sum spans every longitude and there is no ``basin`` dimension —
        which is not compliant for msftmz and friends, but is what the
        single-basin callers of this function still expect.
    depth_coord : str, optional
        Name of depth coordinate, default 'st_ocean' ('potrho' in density space)
    lon_coord : str, optional
        Name of longitude coordinate, default 'xt_ocean'. MOM5 writes the
        meridional transport on the north face of the tracer cell, so its
        longitude axis is the tracer one ('grid_xt_ocean' in density space).
    lat_coord : str, optional
        Name of latitude coordinate, default 'yu_ocean' ('grid_yu_ocean' in
        density space). Only used to align ``basin_mask``.
    to_sverdrups : bool, optional
        If True, convert from kg/s to sverdrups (×10⁹), default False

    Returns
    -------
    streamfunction : xarray.DataArray
        Ocean overturning mass streamfunction
        Dimensions: (time, basin, depth, lat), or (time, depth, lat) when no
        ``basin_mask`` is given
        Units: kg/s (or Sv if to_sverdrups=True)

    Notes
    -----
    - ``gm_trans`` is added to the depth-cumulated transport without being
      cumulated itself: MOM5 writes ``ty_trans_gm`` as the vertically
      integrated bolus transport, not as a per-layer flux. ``ty_trans_submeso``
      follows the same convention. Cumulating them would inflate the
      streamfunction several-fold.
    - Latitudes where a basin has no cells at all (the Atlantic-Arctic south of
      ~35°S, say) are left missing rather than zero: no basin sum is defined
      there.
    """
    weights = _basin_weights(basin_mask, ty_trans, lat_coord, lon_coord)

    if weights is None:
        streamfunction = _streamfunction_over(
            ty_trans, gm_trans, submeso_trans, None, depth_coord, lon_coord
        )
    else:
        per_basin = [
            _streamfunction_over(
                ty_trans,
                gm_trans,
                submeso_trans,
                weights.sel(basin=label),
                depth_coord,
                lon_coord,
            ).where(weights.sel(basin=label).sum(dim=lon_coord) > 0)
            for label in BASIN_LABELS
        ]
        streamfunction = xr.concat(per_basin, dim=_basin_coordinate())
        streamfunction = streamfunction.transpose(..., "basin", depth_coord, lat_coord)

    # Convert to sverdrups if requested
    if to_sverdrups:
        streamfunction = streamfunction * 1e-9  # kg/s to Sv (10⁹ kg/s)

    return streamfunction


def _streamfunction_over(
    ty_trans, gm_trans, submeso_trans, weights, depth_coord, lon_coord
):
    """Return the streamfunction summed over the cells ``weights`` selects.

    ``weights`` is 1 on the cells of one basin and 0 elsewhere, or None to sum
    over every longitude.
    """

    def zonal_sum(transport):
        if weights is not None:
            transport = transport * weights
        return transport.sum(dim=lon_coord)

    # Sum meridional transport over longitude
    ty_zonal_sum = zonal_sum(ty_trans)

    # Calculate overturning streamfunction via cumulative sum over depth
    streamfunction = ty_zonal_sum.cumsum(dim=depth_coord)

    # Add GM component if provided
    if gm_trans is not None:
        streamfunction = streamfunction + zonal_sum(gm_trans)

    # Add submesoscale component if provided
    if submeso_trans is not None:
        streamfunction = streamfunction + zonal_sum(submeso_trans)

    # Remove barotropic component (depth-integrated transport)
    # This ensures the streamfunction goes to zero at the bottom
    barotropic = ty_zonal_sum.sum(dim=depth_coord)
    streamfunction = streamfunction - barotropic

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

    # xarray does not allow dask-backed indexers for vectorized isel.
    # Materialize only the small integer indexer, while keeping var lazy.
    if getattr(bottom_idx.data, "chunks", None) is not None:
        bottom_idx = bottom_idx.compute()

    # Use isel with integer index
    # Need to broadcast bottom_idx to match var's shape for vectorized indexing
    seafloor_values = var.isel({depth_dim: bottom_idx})

    # Mask out points where there were no valid values originally
    seafloor_values = seafloor_values.where(has_valid_data)

    return seafloor_values


def calc_msftbarot(tx_trans, depth_coord="st_ocean", lat_coord="yt_ocean"):
    """Calculate the barotropic mass streamfunction (msftbarot)

    Computes ``msftbarot`` by depth-integrating the zonal mass transport and then
    cumulatively summing from the southern boundary northward.

    The barotropic streamfunction ψ satisfies:

    .. math::

        \\psi(y, x) = \\int_{y_{\\text{south}}}^{y} \\bar{U}(y', x)\\, dy'

    where :math:`\\bar{U}` is the depth-integrated zonal mass transport.
    Integrating northward from Antarctica means the Drake-Passage transport is
    absorbed naturally into the running sum, so no separate reference-point
    correction is required.

    Parameters
    ----------
    tx_trans : xarray.DataArray
        Zonal mass transport with dimensions (..., depth, lat, lon).
        Units: kg/s
    depth_coord : str, optional
        Name of the depth coordinate.  Default ``'st_ocean'`` (MOM5/ACCESS-ESM).
        Use ``'zl'`` for MOM6/ACCESS-OM3.
    lat_coord : str, optional
        Name of the latitude coordinate along which to integrate.
        Default ``'yt_ocean'`` (MOM5/ACCESS-ESM).  Use ``'yh'`` for MOM6/ACCESS-OM3.

    Returns
    -------
    msftbarot : xarray.DataArray
        Barotropic mass streamfunction with the depth dimension removed.
        Units: kg/s

    Notes
    -----
    The reference value ψ = 0 is located at the southernmost grid row
    (near Antarctica).  This is consistent with the CMIP6/7 standard
    interpretation of ``ocean_barotropic_mass_streamfunction``.

    For MOM5 models this function replaces the two-step APP4 procedure of
    (1) reading ``psiu`` and (2) adding a Drake-Passage offset computed with
    hard-coded grid indices.  The cumulative-sum approach is mathematically
    equivalent but works for any horizontal resolution and grid topology.
    """
    # Step 1 – depth-integrate to get the column-integrated zonal transport
    u_bar = tx_trans.sum(dim=depth_coord)

    # Step 2 – cumulative sum from south to north
    msftbarot = u_bar.cumsum(dim=lat_coord)

    return msftbarot


def calc_opottempmint(pot_temp, pot_rho_0, dzt, depth_coord="st_ocean"):
    """Calculate depth-integral of the product of potential temperature and density.

    CMIP6 variable `opottempmint`: integral_wrt_depth_of_product_of_potential_temperature_and_sea_water_density

    Parameters
    ----------
    pot_temp : xarray.DataArray
        Potential temperature in K. Converted to degC internally.
    pot_rho_0 : xarray.DataArray
        In-situ density in kg m-3.
    dzt : xarray.DataArray
        Layer thickness in m.
    depth_coord : str, optional
        Name of the depth dimension, default 'st_ocean'.

    Returns
    -------
    xarray.DataArray
        Depth-integrated product, units degC kg m-2.
    """
    pot_temp_c = pot_temp - 273.15  # K -> degC
    return (pot_temp_c * pot_rho_0 * dzt).sum(dim=depth_coord, skipna=True)


def calc_hfgeou(ht):
    """Create upward geothermal heat flux at sea floor for ACCESS-ESM1.6.

    In ACCESS-ESM1.6 the geothermal heat flux is zero everywhere.  This
    function generates the required CMIP ``hfgeou`` field on the fly rather
    than relying on a large pre-computed resource file.  It returns a
    zero-valued DataArray on the 2-D ocean horizontal grid with land cells
    masked.

    Parameters
    ----------
    ht : xarray.DataArray
        Bathymetric depth (positive values), with 0 for land cells.
        Dimensions: (yt_ocean, xt_ocean) or (time, yt_ocean, xt_ocean)
        Units: m

    Returns
    -------
    hfgeou : xarray.DataArray
        Upward geothermal heat flux at sea floor.
        Dimensions: (yt_ocean, xt_ocean)
        Units: W m-2

    Notes
    -----
    - Fully lazy operation using xarray/dask.
    - Land cells (where ``ht == 0``) are masked with NaN.
    - Time dimension is dropped if present (``hfgeou`` is time-independent).
    """
    # Drop time dimension if present (hfgeou is a fixed field)
    if "time" in ht.dims:
        ht = ht.isel(time=0, drop=True)

    # Create a zero-valued array on the same horizontal grid
    hfgeou = xr.zeros_like(ht)

    # Mask land cells (where bathymetric depth is zero)
    hfgeou = hfgeou.where(ht != 0.0)

    return hfgeou


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


def calc_hfds(
    sfc_hflux_from_runoff,
    sfc_hflux_coupler,
    sfc_hflux_pme,
    frazil_3d_int_z=None,
    frazil_2d=None,
):
    """Calculate surface downward heat flux in sea water (hfds).

    Sums the base surface heat flux components and adds the appropriate frazil
    term. ``frazil_3d_int_z`` is preferred; ``frazil_2d`` is used as a fallback
    for ACCESS-ESM1.6 runs that use the ``pop_icediag`` frazil scheme (frazil
    confined to the top 5 ocean layers), where ``frazil_3d_int_z`` is not saved.

    Parameters
    ----------
    sfc_hflux_from_runoff : xarray.DataArray
        Heat flux from runoff. Units: W m-2
    sfc_hflux_coupler : xarray.DataArray
        Heat flux from the coupler. Units: W m-2
    sfc_hflux_pme : xarray.DataArray
        Heat flux from precipitation minus evaporation. Units: W m-2
    frazil_3d_int_z : xarray.DataArray or None, optional
        Vertically integrated 3-D frazil heat flux. Used preferentially when
        available. Units: W m-2
    frazil_2d : xarray.DataArray or None, optional
        2-D frazil heat flux. Used as a fallback when ``frazil_3d_int_z`` is
        not available.

    Returns
    -------
    hfds : xarray.DataArray
        Surface downward heat flux in sea water. Units: W m-2
    """
    base = sfc_hflux_from_runoff + sfc_hflux_coupler + sfc_hflux_pme
    if frazil_3d_int_z is not None:
        return base + frazil_3d_int_z
    elif frazil_2d is not None:
        logger.warning(
            "frazil_3d_int_z not available; using frazil_2d for hfds calculation "
            "(appropriate for ACCESS-ESM1.6 runs with the pop_icediag frazil scheme)"
        )
        return base + frazil_2d
    else:
        logger.warning(
            "Neither frazil_3d_int_z nor frazil_2d is available; "
            "computing hfds without a frazil contribution"
        )
        return base


#: CMIP region flag values for ``basin``, in the order the CMOR tables declare
#: them (``flag_values`` "0 1 2 ... 10"). The bundled ACCESS-ESM basin mask is
#: numbered on this same scale — it descends from the APP4
#: ``lsmask_ACCESS-OM2_1deg_20110618.nc`` mask, whose codes were chosen to match
#: the CMIP list — so :func:`calc_basin` renumbers nothing. Only ``global_land``
#: is absent from the file, which stores land as its fill value instead.
BASIN_FLAG_MEANINGS = (
    "global_land southern_ocean atlantic_ocean pacific_ocean arctic_ocean "
    "indian_ocean mediterranean_sea black_sea hudson_bay baltic_sea red_sea"
)
BASIN_FLAG_VALUES = " ".join(str(i) for i in range(11))

#: The three regions the CMOR ``basin`` axis requests, in table order.
BASIN_LABELS = ("atlantic_arctic_ocean", "indian_pacific_ocean", "global_ocean")

#: Which region flags (see :data:`BASIN_FLAG_MEANINGS`) make up each of them.
#: The marginal seas go with the ocean they drain into, and ``global_ocean``
#: takes every wet cell, the Southern Ocean included. The Southern Ocean is
#: deliberately in no sector: the mask calls everything south of ~35°S southern
#: ocean, which is where the Atlantic and Indo-Pacific sectors stop being
#: distinguishable, and it is also where CMIP expects them to stop.
BASIN_REGION_FLAGS = {
    # atlantic, arctic, mediterranean, black, hudson, baltic
    "atlantic_arctic_ocean": (2, 4, 6, 7, 8, 9),
    # pacific, indian, red sea
    "indian_pacific_ocean": (3, 5, 10),
    # every basin there is, land (flag 0) excluded
    "global_ocean": tuple(range(1, 11)),
}


def _basin_coordinate():
    """Return the CMOR ``basin`` axis as a labelled coordinate."""
    return xr.DataArray(
        list(BASIN_LABELS),
        dims="basin",
        name="basin",
        attrs={"standard_name": "region", "long_name": "ocean basin"},
    )


def _basin_weights(basin_mask, transport, lat_coord, lon_coord):
    """Return 1/0 weights selecting each CMIP basin, or None without a mask.

    The weights are aligned to ``transport`` by position, not by coordinate
    value: the mask is on the tracer grid (``yt_ocean``) while the meridional
    transport sits on the cell's north face (``yu_ocean``), so the two share a
    shape but not a latitude axis. Row j of the mask is taken to describe the
    transport through the north face of row j — the cell the water leaves. The
    alternative, a mask built on the v-points themselves, would need a
    separate resource file and would differ only along the basin edges.
    """
    if basin_mask is None:
        return None

    missing = [dim for dim in (lat_coord, lon_coord) if dim not in transport.dims]
    if missing:
        raise ValueError(
            f"Transport has no dimension(s) {missing}: it is on {transport.dims}. "
            f"Pass the model's own names as lat_coord/lon_coord."
        )

    expected = (transport.sizes[lat_coord], transport.sizes[lon_coord])
    if basin_mask.shape != expected:
        raise ValueError(
            f"Basin mask has shape {basin_mask.shape}, but the transport is "
            f"{expected} on ({lat_coord}, {lon_coord}). The mask must be on "
            f"the same horizontal grid, ordered (latitude, longitude)."
        )

    # Drop the mask's own coordinates: yt_ocean and yu_ocean hold different
    # latitudes, and keeping both would align the two grids to an empty
    # intersection rather than cell by cell.
    mask = xr.DataArray(basin_mask.values, dims=(lat_coord, lon_coord))

    return xr.concat(
        [
            xr.where(mask.isin(BASIN_REGION_FLAGS[label]), 1, 0)
            for label in BASIN_LABELS
        ],
        dim=_basin_coordinate(),
    ).astype(transport.dtype)


#: Dimension names in ``fx.basin_ACCESS-ESM.nc`` (FERRET-era uppercase) mapped
#: to the MOM5 tracer-grid names the rest of the ocean pipeline expects.
_BASIN_DIM_RENAME = {"XT_OCEAN": "xt_ocean", "YT_OCEAN": "yt_ocean"}


def calc_basin(basin_mask, land_flag=0):
    """Turn the bundled ACCESS-ESM basin mask into the CMOR ``basin`` field.

    CMIP variable: basin (Ofx / ocean.basin.ti-u-hxy-u.fx)

    The bundled mask (``fx.basin_ACCESS-ESM.nc``, variable ``BASIN_MASK``) is
    already numbered on the CMIP region scale, but it is not shaped like the
    CMOR variable: it carries a singleton depth axis, its dimensions are named
    in uppercase, land is stored as the fill value rather than as a flag, and it
    is typed as float. This normalises all four.

    Parameters
    ----------
    basin_mask : xarray.DataArray
        Raw basin mask, normally supplied by a nested ``load_ressource_data``
        call in the mapping.
        Dimensions: (ST_OCEAN1_1, YT_OCEAN, XT_OCEAN) or the lowercase
        equivalents, with or without the singleton depth axis.
    land_flag : int, optional
        Flag value to write where the mask has no basin, default 0
        (``global_land``).

    Returns
    -------
    basin : xarray.DataArray
        Region selection index.
        Dimensions: (yt_ocean, xt_ocean)
        Type: int32, carrying ``flag_values``/``flag_meanings``

    Notes
    -----
    - The result is an integer field with no missing values: every cell holds a
      flag, land included. That is what ``standard_name = "region"`` asks for,
      and it is why no ``_FillValue`` is set here.
    - ``black_sea`` (flag 7) is unused on the 1° ACCESS-ESM ocean grid, where
      the Black Sea is not resolved as a separate basin. A gap in the values
      present is expected; the flag list is fixed by the CMOR table.
    - Not lazy: the mask is a single 300x360 field loaded from a bundled
      resource, so it is computed eagerly to keep the dtype cast exact.
    """
    basin = basin_mask.rename(
        {k: v for k, v in _BASIN_DIM_RENAME.items() if k in basin_mask.dims}
    )

    # Drop the singleton depth axis the resource carries (ST_OCEAN1_1), along
    # with any other degenerate non-horizontal axis, so the field is strictly
    # (yt_ocean, xt_ocean) as Ofx.basin requires.
    extra_dims = [
        dim
        for dim in basin.dims
        if dim not in ("yt_ocean", "xt_ocean") and basin.sizes[dim] == 1
    ]
    if extra_dims:
        basin = basin.squeeze(extra_dims, drop=True)

    unexpected = [dim for dim in basin.dims if dim not in ("yt_ocean", "xt_ocean")]
    if unexpected:
        raise ValueError(
            f"Basin mask has unexpected non-horizontal dimension(s) {unexpected} "
            f"that are not degenerate; expected only (yt_ocean, xt_ocean). "
            f"Dimensions found: {dict(basin.sizes)}"
        )

    # Land is the fill value in the resource; CMIP wants it as flag 0.
    basin = basin.fillna(land_flag).astype("int32")

    # The mask's own provenance attributes (long_name "MASK_TTCELL[K=1]",
    # missing_value, history) describe the resource, not the CMOR variable, and
    # a leftover missing_value on a gap-free integer field is wrong. Replace
    # them outright; the CMORiser overlays the table's own metadata on top.
    basin.attrs = {
        "standard_name": "region",
        "long_name": "Region Selection Index",
        "units": "1",
        "flag_values": BASIN_FLAG_VALUES,
        "flag_meanings": BASIN_FLAG_MEANINGS,
    }
    basin.encoding = {}
    return basin
