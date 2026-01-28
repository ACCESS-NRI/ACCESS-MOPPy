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
    spatial_dims = [dim for dim in var.dims if dim in ['st_ocean', 'yt_ocean', 'xt_ocean']]
    
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
    surface_layer = (sw_heat.isel(st_ocean=0) + swflux).expand_dims('st_ocean')
    
    # Deeper layers: use heat as-is
    deeper_layers = sw_heat.isel(st_ocean=slice(1, None))
    
    # Concatenate surface and deeper layers
    rsdoabsorb = xr.concat([surface_layer, deeper_layers], dim='st_ocean')
    
    return rsdoabsorb


def calc_zostoga(pot_temp, dzt, areacello, depth_coord='st_ocean'):
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
    temp_ref = 4.0   # Reference temperature, °C
    alpha = 2e-4     # Thermal expansion coefficient, /°C
    
    # Calculate density at in-situ temperature
    rho_insitu = rho_0 * (1.0 - alpha * (pot_temp - temp_ref))
    
    # Calculate reference density at 4°C
    rho_ref = rho_0  # At reference temperature
    
    # Calculate thermosteric height change and integrate over depth
    # (1 - ρ/ρ₄) gives fractional density difference
    thermo_height = (1.0 - rho_insitu / rho_ref) * dzt
    integrated_height = thermo_height.sum(dim=depth_coord, skipna=True)
    
    # Calculate area-weighted global average
    zostoga = integrated_height.weighted(areacello).mean(dim=['yt_ocean', 'xt_ocean'])
    
    return zostoga


def calc_ocean_depth_integral(var, rho, dzt, depth_coord='st_ocean'):
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