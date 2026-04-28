import operator
from functools import reduce

from access_moppy.derivations.calc_aerosol import optical_depth
from access_moppy.derivations.calc_atmos import (
    calculate_areacella,
    cl_level_to_height,
    cli_level_to_height,
    clw_level_to_height,
    level_to_height,
)
from access_moppy.derivations.calc_land import (
    calc_carbon_pool_kg_m2,
    calc_cland_with_wood_products,
    calc_landcover,
    calc_mass_pool_kg_m2,
    calc_mrsfl,
    calc_mrsll,
    calc_mrsol,
    calc_nitrogen_pool_kg_m2,
    calc_topsoil,
    calc_tsl,
    extract_tilefrac,
    weighted_tile_sum,
)
from access_moppy.derivations.calc_ocean import (
    calc_areacello,
    calc_global_ave_ocean,
    calc_hfds,
    calc_hfgeou,
    calc_msftbarot,
    calc_opottempmint,
    calc_overturning_streamfunction,
    calc_rsdoabsorb,
    calc_total_mass_transport,
    calc_umo_corrected,
    calc_vmo_corrected,
    calc_zostoga,
    ocean_floor,
)
from access_moppy.derivations.calc_seaice import (
    calc_hemi_seaice,
    calc_seaice_extent,
    calc_siarean,
    calc_siareas,
    calc_siextentn,
    calc_siextents,
    calc_sisnmassn,
    calc_sisnmasss,
    calc_sivoln,
    calc_sivols,
)
from access_moppy.derivations.calc_utils import (
    calculate_monthly_maximum,
    calculate_monthly_minimum,
    drop_axis,
    drop_time_axis,
    squeeze_axis,
)

custom_functions = {
    "add": lambda *args: reduce(operator.add, args),
    "subtract": lambda a, b: a - b,
    "multiply": lambda a, b: a * b,
    "divide": lambda a, b: a / b,
    "power": lambda a, b: a**b,
    "sum": lambda x, **kwargs: x.sum(**kwargs),
    "mean": lambda *args: sum(args) / len(args),
    "kelvin_to_celsius": lambda x: x - 273.15,
    "celsius_to_kelvin": lambda x: x + 273.15,
    "cli_level_to_height": cli_level_to_height,
    "clw_level_to_height": clw_level_to_height,
    "cl_level_to_height": cl_level_to_height,
    "level_to_height": level_to_height,
    "calculate_areacella": calculate_areacella,
    "isel": lambda x, **kwargs: x.isel(**kwargs),
    "calc_topsoil": calc_topsoil,
    "calc_landcover": calc_landcover,
    "extract_tilefrac": extract_tilefrac,
    "optical_depth": optical_depth,
    "calculate_monthly_minimum": calculate_monthly_minimum,
    "calculate_monthly_maximum": calculate_monthly_maximum,
    "drop_axis": drop_axis,
    "drop_time_axis": drop_time_axis,
    "squeeze_axis": squeeze_axis,
    "weighted_tile_sum": weighted_tile_sum,
    "calc_cland_with_wood_products": calc_cland_with_wood_products,
    "calc_carbon_pool_kg_m2": calc_carbon_pool_kg_m2,
    "calc_mass_pool_kg_m2": calc_mass_pool_kg_m2,
    "calc_nitrogen_pool_kg_m2": calc_nitrogen_pool_kg_m2,
    "calc_mrsfl": calc_mrsfl,
    "calc_mrsll": calc_mrsll,
    "calc_mrsol": calc_mrsol,
    "calc_tsl": calc_tsl,
    "calc_hfds": calc_hfds,
    "calc_msftbarot": calc_msftbarot,
    "calc_hfgeou": calc_hfgeou,
    "calc_overturning_streamfunction": calc_overturning_streamfunction,
    "calc_rsdoabsorb": calc_rsdoabsorb,
    "calc_zostoga": calc_zostoga,
    "calc_global_ave_ocean": calc_global_ave_ocean,
    "calc_opottempmint": calc_opottempmint,
    "calc_total_mass_transport": calc_total_mass_transport,
    "calc_umo_corrected": calc_umo_corrected,
    "calc_vmo_corrected": calc_vmo_corrected,
    "ocean_floor": ocean_floor,
    "calc_areacello": calc_areacello,
    "calc_seaice_extent": calc_seaice_extent,
    "calc_hemi_seaice": calc_hemi_seaice,
    "calc_siarean": calc_siarean,
    "calc_siareas": calc_siareas,
    "calc_sivoln": calc_sivoln,
    "calc_sivols": calc_sivols,
    "calc_sisnmassn": calc_sisnmassn,
    "calc_sisnmasss": calc_sisnmasss,
    "calc_siextentn": calc_siextentn,
    "calc_siextents": calc_siextents,
}


def evaluate_expression(expr, context):
    if isinstance(expr, dict):
        if "literal" in expr:
            return expr["literal"]
        if "optional" in expr:
            # Return the variable if present in context, else None
            return context.get(expr["optional"])
        op = expr["operation"]
        args = [
            evaluate_expression(arg, context)
            for arg in expr.get("args", expr.get("operands", []))
        ]
        kwargs = {
            k: evaluate_expression(v, context)
            for k, v in expr.get("kwargs", {}).items()
        }
        return custom_functions[op](*args, **kwargs)

    elif isinstance(expr, list):
        # Recursively evaluate items in the list
        return [evaluate_expression(item, context) for item in expr]

    elif isinstance(expr, str):
        # Lookup variable name in context
        return context[expr]

    elif isinstance(expr, (int, float)):
        return expr

    else:
        raise ValueError(f"Unsupported expression: {expr}")
