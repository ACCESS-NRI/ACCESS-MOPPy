import click
import xarray as xr
import logging

@click.pass_context
def K_degC(ctx, var, inverse=False):
    """Converts temperature from/to K to/from degC.

    Parameters
    ----------
    ctx : click context
        Includes obj dict with 'cmor' settings, exp attributes
    var : Xarray DataArray 
        temperature array

    Returns
    -------
    vout : Xarray DataArray 
        temperature array in degrees Celsius or Kelvin if inverse is True

    """    
    var_log = logging.getLogger(ctx.obj['var_log'])
    if not inverse and 'K' in var.units:
        var_log.info("temp in K, converting to degC")
        vout = var - 273.15
    elif inverse and 'C' in var.units:
        var_log.info("temp in degC, converting to K")
        vout = var + 273.15
    return vout

@click.pass_context
def calc_zostoga(ctx, ptemp, dht):
    """Returns Global Average Thermosteric Sea Level Change 
    
    See https://github.com/ACCESS-Community-Hub/ACCESS-MOPPeR/issues/182
    for details. 
    NB. no one tested if this gives correct results yet!!!

    Parameters
    ----------
    ctx : click context
        Includes obj dict with 'cmor' settings, exp attributes
    ptemp: DataArray
        Potential temperature in degrees Celsius
    dht: DataArray
        Model level thickness 

    Returns
    -------
    zostoga: DataArray
        Global Average Thermosteric Sea Level Change (time) variable 

    """
    var_log = logging.getLogger(ctx.obj['var_log'])
    t, dep, la, lo = ptemp.dims
    # gsw p_from_z expect negative depths
    depth = -1*ptemp[dep]
    # get latitude from grid ancil file
    coords = ptemp.encoding['coordinates'].split()
    lat, dum1, dum2, dum3 = get_coords(coords)
    # rename latitude index dimensions so they are the same as output
    ptemp_lalo = [la, lo]
    if any(x not in ptemp_lalo for x in lat.dims):
        for i,d in enumerate(lat.dims):
            lat = lat.rename({d: ptemp_lalo[i]})
    areacello = get_areacello()
    # press is absolute pressure minus 10.1325 dbar
    press = gsw.conversions.p_from_z(depth, lat)
    # constant salinity 35.00
    cso35 = xr.full_like(ptemp, 35.00)
    # constant temperature 4.00
    ctemp4 = xr.full_like(ptemp, 4.00)
    # calculate density with potential T and at constant 4 deg T
    rho = gsw.density.rho(cso35, ptemp, press)
    rho4 = gsw.density.rho(cso35, ctemp4, press)
    tmp = ((1. - rho/rho4) * dht).sum(dim=dep, skipna=True)
    # rename reindex coordinates to avoid differences
    if any(x not in ptemp_lalo for x in areacello.dims):
        for i,d in enumerate(areacello.dims):
            areacello = areacello.rename({d: ptemp_lalo[i]})
    areacello = areacello.reindex_like(tmp.isel(time=0),
        method='nearest')
    zostoga = ((tmp * areacello).sum(dim=[la, lo], skipna=True) / 
        areacello.sum(dim=[la, lo], skipna=True))
    return zostoga