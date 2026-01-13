import logging

import click


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
    var_log = logging.getLogger(ctx.obj["var_log"])
    if not inverse and "K" in var.units:
        var_log.info("temp in K, converting to degC")
        vout = var - 273.15
    elif inverse and "C" in var.units:
        var_log.info("temp in degC, converting to K")
        vout = var + 273.15
    return vout
