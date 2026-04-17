"""Utility to calculate the correct a and b coefficients for the UM hybrid
height vertical coordinate scheme.

Background
----------
The UM (Unified Model) atmosphere uses a "hybrid height" vertical coordinate
where model level heights are defined as::

    z(k) = a(k) + b(k) * z_surface

The ``a`` coefficient represents the pure height component (metres above sea
level) and ``b`` is a dimensionless orography-following factor that smoothly
decreases from 1 at the model surface to 0 when levels become purely
height-based.  Both coefficients are derived from the raw eta (η) levels
stored in the UM vertlevs namelist file.

The correct formula (from UM7 setcona.F90, ``height_gen_smooth`` option; see
also eq. 4.1 of Davies et al., 2005, doi:10.1256/qj.04.101) is::

    a(k) = η(k) × z_top_of_model
    b(k) = (1 − η(k) / η[first_constant_r_rho_level])²

The quadratic term means that b ≠ η.


The bug in ACCESS-ESM1.5 and ACCESS-CM2 CMIP6 output
------------------------------------------------------
ACCESS-ESM1.5 and ACCESS-CM2 CMIP6 output incorrectly stored the raw η values
directly as the ``sigma_theta`` (and ``sigma_rho``) coordinate variables,
omitting the quadratic transformation.  This produces subtly wrong vertical
coordinates that affect any analysis relying on the orography-following part
of the hybrid height formula.

ACCESS-ESM1.6 (the first model officially supported by ACCESS-MOPPy) stores
the correctly transformed b values in its output files, so no correction is
needed for ESM1.6 data.


Why this utility may still be useful for ACCESS-ESM1.5 / ACCESS-CM2 users
--------------------------------------------------------------------------
ACCESS-MOPPy officially targets ACCESS-ESM1.6 and later models.  However,
users who need to compare with, or re-process, archived ACCESS-ESM1.5 or
ACCESS-CM2 data may encounter the incorrect ``sigma_theta`` values described
above.

This script can be used to:

1. Verify or regenerate the correct a and b coefficients from a vertlevs file.
2. Understand what the quadratic correction looks like numerically compared
   with the raw η values stored in older files.
3. Pre-process ACCESS-ESM1.5 / ACCESS-CM2 NetCDF files to replace the
   incorrect ``sigma_theta`` / ``sigma_rho`` values with the correctly
   computed b coefficients before feeding them into further analysis workflows.

The vertlevs files for each model configuration can typically be found at paths
like:

* ESM1.5 / ESM1.6:
  ``/g/data/vk83/configurations/inputs/access-esm1p5/share/atmosphere/grids/
  resolution_independent/2020.05.19/vertlevs_G3``
* CM2 / CM2.1:
  ``~access/umdir/vn10.6/ctldata/vert/vertlevs_L85_50t_35s_85km``


Usage
-----
From the command line (after installing access_moppy)::

    moppy-calc-ab-coeffts /path/to/vertlevs_file

Or in Python::

    from access_moppy.calc_hybrid_height_coeffs import calc_ab
    a_theta, b_theta, a_rho, b_rho = calc_ab("/path/to/vertlevs_file")


Dependencies
------------
This utility requires ``f90nml`` to parse the Fortran namelist vertlevs file.
Install it with::

    pip install f90nml
    # or, if using pixi:
    pixi add f90nml

``f90nml`` is listed as an optional dependency of access_moppy under the
``[atmos-tools]`` extra::

    pip install "access_moppy[atmos-tools]"


References
----------
* UM7 setcona.F90 (height_gen_smooth option):
  https://github.com/ACCESS-NRI/UM7/blob/b5f58fcf8487c177b0d75878bcf015102a90dc7c/umbase_hg3/src/control/top_level/setcona.F90#L503
* Davies, T., Cullen, M. J. P., Malcolm, A. J., Mawson, M. H., Staniforth, A.,
  White, A. A., & Wood, N. (2005). A new dynamical core for the Met Office's
  global and regional modelling of the atmosphere. Q. J. R. Meteorol. Soc.,
  131(608), 1759–1782. https://doi.org/10.1256/qj.04.101
* Original script by Martin Dix:
  https://gist.github.com/MartinDix/14d6ab8fa6997c18f5bf5456d22756d5
"""

import sys

import numpy as np


def calc_ab(vfile):
    """Calculate the a and b hybrid-height coefficients from a UM vertlevs file.

    Parameters
    ----------
    vfile : str or path-like
        Path to the Fortran namelist file containing the ``&VERTLEVS``
        namelist (e.g. ``vertlevs_G3`` for ACCESS-ESM1.5 / ESM1.6).

    Returns
    -------
    a_theta : numpy.ndarray, shape (model_levels + 1,)
        a coefficient (metres) for theta (full / cell-centre) levels.
        Index 0 corresponds to the surface level (k=0 in UM convention).
    b_theta : numpy.ndarray, shape (model_levels + 1,)
        Correctly computed b coefficient for theta levels (dimensionless).
        Index 0 corresponds to the surface (b = 1 there, by construction).
    a_rho : numpy.ndarray, shape (model_levels + 1,)
        a coefficient (metres) for rho (half / cell-interface) levels.
        Index 0 is unused (rho levels start at k=1 in UM convention).
    b_rho : numpy.ndarray, shape (model_levels + 1,)
        Correctly computed b coefficient for rho levels (dimensionless).

    Notes
    -----
    The UM stores raw η (eta) values in the vertlevs namelist, ranging from
    0 at the surface to 1 at the model top.  The orography-following b
    coefficient is derived via a quadratic formula:

        b(k) = (1 − η(k) / η_etadot)²

    where ``η_etadot = eta_rho[first_constant_r_rho_level]`` is the η value
    of the first rho level at which the coordinate becomes purely
    height-based (b = 0 for all levels at or above this index).

    This quadratic transformation was **incorrectly omitted** in the CMIP6
    output produced by ACCESS-ESM1.5 and ACCESS-CM2, which wrote raw η as
    the ``sigma_theta`` coordinate variable.  ACCESS-ESM1.6 output already
    contains the correctly transformed values.

    Raises
    ------
    ImportError
        If ``f90nml`` is not installed.  Install it via
        ``pip install f90nml`` or ``pip install 'access_moppy[atmos-tools]'``.
    """
    try:
        import f90nml
    except ImportError as exc:
        raise ImportError(
            "The 'f90nml' package is required to read UM vertlevs files.  "
            "Install it with:  pip install f90nml\n"
            "  or:  pip install 'access_moppy[atmos-tools]'"
        ) from exc

    vertlevs = f90nml.read(vfile)["vertlevs"]

    # eta_theta includes the surface (k=0) as its first element.
    eta_theta_levels = np.array(vertlevs["eta_theta"])  # shape: (model_levels+1,)

    # eta_rho does NOT include k=0 in the namelist; prepend a sentinel value
    # so that array index k directly corresponds to UM level k (1-based).
    eta_rho_levels = np.array([-1e20] + vertlevs["eta_rho"])  # shape: (model_levels+1,)

    z_top_of_model = vertlevs["z_top_of_model"]

    # first_constant_r_rho_level is the first rho level at which the atmosphere
    # is fully decoupled from orography, i.e. b = 0 at and above this level.
    first_constant_r_rho_level = vertlevs["first_constant_r_rho_level"]

    # η_etadot: the reference eta value used in the quadratic formula.
    # This is the eta_rho value at the transition level.
    eta_etadot = eta_rho_levels[first_constant_r_rho_level]

    nlev = len(eta_theta_levels)
    a_theta = np.zeros(nlev)
    b_theta = np.zeros(nlev)
    a_rho = np.zeros(nlev)
    b_rho = np.zeros(nlev)

    # --- Orography-following region (levels 1 .. first_constant_r_rho_level-1) ---
    # The quadratic transformation b = (1 − η / η_etadot)² ensures a smooth
    # transition: b ≈ 1 near the surface (small η) and b → 0 as η → η_etadot.
    for k in range(1, first_constant_r_rho_level):
        a_rho[k] = eta_rho_levels[k] * z_top_of_model
        b_rho[k] = (1.0 - eta_rho_levels[k] / eta_etadot) ** 2
        a_theta[k] = eta_theta_levels[k] * z_top_of_model
        b_theta[k] = (1.0 - eta_theta_levels[k] / eta_etadot) ** 2

    # --- Pure height-based region (levels first_constant_r_rho_level and above) ---
    # b = 0 here; levels are independent of orography.
    a_theta[first_constant_r_rho_level:] = (
        eta_theta_levels[first_constant_r_rho_level:] * z_top_of_model
    )
    b_theta[first_constant_r_rho_level:] = 0.0

    a_rho[first_constant_r_rho_level:] = (
        eta_rho_levels[first_constant_r_rho_level:] * z_top_of_model
    )
    b_rho[first_constant_r_rho_level:] = 0.0

    return a_theta, b_theta, a_rho, b_rho


def main():
    """Command-line entry point: calculate and print a/b coefficients.

    Usage::

        moppy-calc-ab-coeffts <vertlevs_file>
    """
    if len(sys.argv) != 2:
        print(
            "Usage: moppy-calc-ab-coeffts <vertlevs_file>",
            file=sys.stderr,
        )
        sys.exit(1)

    vertlevs_file = sys.argv[1]
    a_theta, b_theta, a_rho, b_rho = calc_ab(vertlevs_file)

    print("Theta (full) levels a, b")
    for k in range(1, len(a_theta)):
        print(f"{k:02d} {a_theta[k]:12.6f} {b_theta[k]:10.8f}")

    print("Rho (half) levels a, b")
    for k in range(1, len(a_rho)):
        print(f"{k:02d} {a_rho[k]:12.6f} {b_rho[k]:10.8f}")


if __name__ == "__main__":
    main()
