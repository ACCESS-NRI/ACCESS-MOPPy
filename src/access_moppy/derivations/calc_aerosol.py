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
