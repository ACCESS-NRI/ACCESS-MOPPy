from access_moppy.derivations.calc_utils import sum_vars


def optical_depth(var, lwave):
    """
    Calculates the optical depth.
    First selects all variables at selected wavelength then sums them.

    Parameters
    ----------
    var: DataArray
        variable from Xarray dataset
    lwave: int
        level corresponding to desidered wavelength

    Returns
    -------
    vout: DataArray
        Optical depth

    """
    # Explicitly find the pseudo_level dimension name
    pseudo_level_dims = [dim for dim in var[0].dims if "pseudo_level" in dim]
    if not pseudo_level_dims:
        raise ValueError("No pseudo_level dimension found in variable")
    pseudo_level = pseudo_level_dims[0]

    var_list = [v.sel({pseudo_level: lwave}) for v in var]
    vout = sum_vars(var_list)
    vout = vout.rename({pseudo_level: "pseudo_level"})
    return vout


# @click.pass_context
# def calc_depositions(ctx, var, weight=None):
#    """Returns aerosol depositions
#
#    At the moment is assuming sea salt will need more work to be
#    adapted for other depositions.
#    Original variables are mol s-1 output is kg m-2 s-1, so we
#    multiply by molecular weight.
#    Sea salt is assumed to be NaCl: 0.05844 kg.mol-1
#    NB we are using only surface level as: "Dry deposition occurs
#    when aerosol bumps into something at surface level, so it doesn't
#    make sense for there to be data in the levels above"
#    (personal communication from M. Woodhouse)
#
#    Parameters
#    ----------
#    ctx : click context
#        Includes obj dict with 'cmor' settings, exp attributes
#    var : list(xarray.DataArray)
#        List of input variables to sum
#    weight: float
#        Weight of 1 mole, default is None and it uses NaCl weight (to be updated)
#
#    Returns
#    -------
#    varout : xarray.DataArray
#        Areosol depositions
#
#    """
#
#    # var_log = logging.getLogger(ctx.obj['var_log'])
#    varlist = []
#    for v in var:
#        v0 = v.sel(model_theta_level_number=1).squeeze(dim="model_theta_level_number")
#        varlist.append(v0)
#    if weight is None:
#        weight = 0.05844
#    deps = sum_vars(varlist) * weight
#    return deps
