import operator
from functools import reduce

from access_moppy.derivations.calc_atmos import level_to_height
from access_moppy.derivations.calc_land import (
    average_tile,
    calc_landcover,
    calc_topsoil,
    extract_tilefrac,
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
    "level_to_height": level_to_height,
    "calc_topsoil": calc_topsoil,
    "calc_landcover": calc_landcover,
    "extract_tilefrac": extract_tilefrac,
    "average_tile": average_tile,
}


def evaluate_expression(expr, context):
    if isinstance(expr, dict):
        if "literal" in expr:
            return expr["literal"]
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
