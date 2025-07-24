custom_functions = {
    "add": lambda a, b: a + b,
    "subtract": lambda a, b: a - b,
    "multiply": lambda a, b: a * b,
    "divide": lambda a, b: a / b,
    "power": lambda a, b: a**b,
    "sum": lambda x, **kwargs: x.sum(**kwargs),
    "mean": lambda *args: sum(args) / len(args),
    "kelvin_to_celsius": lambda x: x - 273.15,
    "celsius_to_kelvin": lambda x: x + 273.15,
}


def evaluate_expression(expr, context):
    if isinstance(expr, str):  # variable name
        return context[expr]
    elif isinstance(expr, (int, float)):  # literal number
        return expr
    elif isinstance(expr, list):
        return [
            evaluate_expression(item, context)
            if isinstance(item, (dict, str))
            else item
            for item in expr
        ]
    elif isinstance(expr, dict):
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
    else:
        raise ValueError(f"Unsupported expression: {expr}")
