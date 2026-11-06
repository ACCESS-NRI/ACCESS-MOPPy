"""Run-audited WOMBATlite derivations for opt-in native evaluation."""

from __future__ import annotations

WOMBATLITE_SOURCE = (
    "https://github.com/ACCESS-NRI/GFDL-generic-tracers/blob/"
    "8afe03201dbf9d6a1412f37159bf25e186fc36ee/generic_tracers/"
    "generic_WOMBATlite.F90"
)
CARBON_MOLAR_MASS_KG_PER_MOL = 0.012011


def _field_only(array):
    """Drop unrelated coordinates while retaining dimensions and lazy data."""
    import xarray as xr

    return xr.DataArray(
        array.data,
        dims=array.dims,
        coords={d: array.coords[d] for d in array.dims if d in array.coords},
        attrs=dict(array.attrs),
        name=array.name,
    )


def _require_units(array, accepted, label):
    units = array.attrs.get("units")
    if units not in accepted:
        raise ValueError(f"{label} units {units!r}; expected one of {sorted(accepted)}")


def _clean(array):
    import numpy as np

    field = _field_only(array)
    for key in ("_FillValue", "missing_value"):
        fill = array.attrs.get(key, array.encoding.get(key))
        if fill is not None and np.isfinite(fill):
            field = field.where(field != fill)
    return field.where(np.isfinite(field))


def derive_fgco2(native, spec):
    """Derive an air-sea carbon flux candidate from native WOMBATlite output.

    WOMBATlite adds a virtual salinity correction to ``dic/stf``; the generic
    tracer driver also adds configured river DIC runoff. Remove both to leave
    gas exchange. ``salt_flux_added`` is archived in kg salt m-2 s-1 while
    WOMBATlite's callback expects g m-2 s-1, hence the explicit x1000.

    Required spec keys:
      - ``native.dic_stf`` and ``native.salt_flux_added`` variable names
      - ``parameters.dic_global`` in mol kg-1 and ``parameters.sal_global`` in
        g kg-1, from the actual run's WOMBATlite namelist
      - ``native.friver`` and ``parameters.runoff_dic_mol_kg`` from run config
      - ``dic_surface_components_verified`` set True after checking run config
      - ``run_id`` and ``run_config_reference`` to audit parameter provenance
    """
    import numpy as np
    import xarray as xr

    native_names = spec.get("native", {})
    stf_name = native_names.get("dic_stf", "dic_stf")
    salt_name = native_names.get("salt_flux_added", "salt_flux_added")
    river_name = native_names.get("friver", "friver")
    if any(n not in native for n in (stf_name, salt_name, river_name)):
        raise ValueError(
            f"Native {stf_name!r}, {salt_name!r}, and {river_name!r} are required"
        )
    stf, salt_flux, river = native[stf_name], native[salt_name], native[river_name]
    if any(a.dims != ("time", "yh", "xh") for a in (stf, salt_flux, river)):
        raise ValueError(
            "dic_stf, salt_flux_added, and friver require (time,yh,xh) dimensions"
        )
    _require_units(stf, {"mole/m^2/sec", "mol m-2 s-1", "mol/m2/s"}, stf_name)
    _require_units(salt_flux, {"kg m-2 s-1", "kg/m^2/s"}, salt_name)
    _require_units(river, {"kg m-2 s-1", "kg/m^2/s"}, river_name)
    # Exact coordinate alignment guards against subtracting a similarly shaped
    # salt-flux field from a different grid or time range.
    stf, salt_flux, river = xr.align(stf, salt_flux, river, join="exact", copy=False)
    stf, salt_flux, river = (_clean(a) for a in (stf, salt_flux, river))

    params = spec.get("parameters", {})
    if any(
        key not in params for key in ("dic_global", "sal_global", "runoff_dic_mol_kg")
    ):
        raise ValueError(
            "Explicit run parameters dic_global, sal_global, and runoff_dic_mol_kg are required"
        )
    dic_global = float(params["dic_global"])
    sal_global = float(params["sal_global"])
    runoff_dic = float(params["runoff_dic_mol_kg"])
    if not spec.get("run_id") or not spec.get("run_config_reference"):
        raise ValueError(
            "run_id and run_config_reference are required to audit carbon parameters"
        )
    if not np.isfinite(dic_global) or dic_global <= 0:
        raise ValueError("dic_global must be a positive finite mol kg-1 scalar")
    if not np.isfinite(sal_global) or sal_global <= 0:
        raise ValueError("sal_global must be a positive finite g kg-1 scalar")
    if not np.isfinite(runoff_dic) or runoff_dic < 0:
        raise ValueError(
            "runoff_dic_mol_kg must be a nonnegative finite mol kg-1 scalar"
        )
    if spec.get("dic_surface_components_verified") is not True:
        raise ValueError(
            "Verify the run has no other DIC surface sources before labelling fgco2"
        )

    dic_vstf = (dic_global / sal_global) * (salt_flux * 1000.0)
    dic_river = runoff_dic * river
    carbon_flux = (stf - dic_vstf - dic_river) * CARBON_MOLAR_MASS_KG_PER_MOL
    result = _field_only(carbon_flux)
    result.name = "fgco2"
    result.attrs = {
        "standard_name": "surface_downward_mass_flux_of_carbon_dioxide_expressed_as_carbon",
        "long_name": "Air-sea CO2 flux into ocean, corrected for virtual DIC and river runoff",
        "units": "kg m-2 s-1",
        "positive": "down",
        "cell_methods": "time: mean",
        "comment": (
            "Derived as (dic_stf - (dic_global/sal_global)*1000*salt_flux_added "
            "- runoff_dic_mol_kg*friver) * 0.012011 kg C mol-1. Positive into "
            "the ocean. friver and salt_flux_added must be matching monthly "
            "kg m-2 s-1 fields."
        ),
        "source_model_variable": stf_name,
        "virtual_flux_source_variable": salt_name,
        "river_flux_source_variable": river_name,
        "wombatlite_source": WOMBATLITE_SOURCE,
        "wombatlite_dic_global_mol_kg-1": dic_global,
        "wombatlite_sal_global_g_kg-1": sal_global,
        "wombatlite_virtual_flux_formula": "(dic_global / sal_global) * 1000 * salt_flux_added",
        "runoff_dic_mol_kg-1": runoff_dic,
        "run_id": str(spec["run_id"]),
        "run_config_reference": str(spec["run_config_reference"]),
        "derivation_status": "run-audited native derived field; not a direct archive variable or CMIP product",
    }
    return result


def derive_pht_estimate(native, spec):
    """Return surface total-scale pH estimated from monthly mean WOMBATlite H+.

    The run must use the MOCSY carbonate path (the pinned WOMBATlite default).
    The nonlinear transform of a monthly mean concentration is deliberately
    labelled an estimate, not a monthly mean pH diagnostic.
    """
    import numpy as np

    if spec.get("allow_pht_estimate") is not True:
        raise ValueError("pHT estimate requires allow_pht_estimate=True")
    if spec.get("co2_calc") != "mocsy":
        raise ValueError("Total-scale pHT derivation requires co2_calc='mocsy'")
    if not spec.get("run_id") or not spec.get("run_config_reference"):
        raise ValueError(
            "run_id and run_config_reference are required to audit pHT estimate"
        )
    native_name = spec.get("native", {}).get("htotal", "htotal")
    if native_name not in native:
        raise ValueError(f"Missing native WOMBATlite variable {native_name!r}")
    htotal = native[native_name]
    if htotal.dims != ("time", "zl", "yh", "xh"):
        raise ValueError("htotal must have (time,zl,yh,xh) dimensions")
    _require_units(htotal, {"mol/kg", "mol kg-1"}, native_name)
    for key in ("_FillValue", "missing_value"):
        fill = htotal.attrs.get(key, htotal.encoding.get(key))
        if fill is not None and np.isfinite(fill):
            htotal = htotal.where(htotal != fill)
    surface = htotal.isel(zl=0, drop=True)
    # Preserve lazy backing arrays; invalid nonpositive values become missing.
    pht = -np.log10(surface.where(surface > 0))
    result = _field_only(pht)
    result.name = "pht_estimate"
    result.attrs = {
        "long_name": "Estimated surface pH on the total scale from monthly mean H+ concentration",
        "units": "1",
        "pH_scale": "total",
        "cell_methods": "time: mean (comment: nonlinear estimate from mean Htotal; not mean pHT)",
        "comment": (
            "MOCSY returns pH on the total scale and WOMBATlite sets "
            "htotal=10**(-ph). This field is -log10(monthly mean Htotal), "
            "not monthly mean pHT; positive finite Htotal required."
        ),
        "source_model_variable": native_name,
        "run_id": str(spec["run_id"]),
        "run_config_reference": str(spec["run_config_reference"]),
        "wombatlite_source": WOMBATLITE_SOURCE,
        "derivation_status": "nonlinear monthly-mean-derived estimate; not exact monthly mean pHT",
    }
    return result
