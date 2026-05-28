"""Optional variable-aware regridding support for ACCESS-MOPPy.

The first-pass implementation separates regridding policy and cached-weight
application from the CMORisation classes.  Weight generation is delegated to
xESMF when it is available; applying existing ESMF/xESMF weight files uses a
small NumPy sparse-matrix kernel so CI and batch jobs can reuse weights without
requiring ESMF at runtime.
"""

from __future__ import annotations

import argparse
import hashlib
import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Mapping

import numpy as np
import xarray as xr

REGRID_METHODS = {"conservative", "bilinear", "nearest_s2d"}
VECTOR_VARIABLES = {
    "tauu",
    "tauv",
    "tauuo",
    "tauvo",
    "ua",
    "uas",
    "uo",
    "umo",
    "va",
    "vas",
    "vo",
    "vmo",
    "wa",
    "wo",
}
MASK_NAMES = {"basin", "sftlf", "sftof"}
FRACTION_NAMES = {"siconc", "sivol", "siarean"}
STATE_UNITS = {"K", "degC", "degree_Celsius", "psu", "1e-3", "m", "Pa"}


class RegridError(RuntimeError):
    """Raised when optional regridding cannot be applied safely."""


@dataclass(frozen=True)
class WeightConfig:
    """Configuration for cached regridding weights."""

    mode: str = "reuse_or_create"
    path: Path | None = None
    cache_dir: Path | None = None
    mask_policy: str = "nomask"

    @classmethod
    def from_config(cls, config: str | Mapping[str, Any] | None) -> "WeightConfig":
        if config is None or config == "auto":
            return cls()
        if isinstance(config, str):
            return cls(path=Path(config), mode="reuse")
        if not isinstance(config, Mapping):
            raise TypeError("regrid.weights must be a string, mapping, or omitted")
        return cls(
            mode=str(config.get("mode", "reuse_or_create")),
            path=Path(config["path"]) if config.get("path") else None,
            cache_dir=Path(config["cache_dir"]) if config.get("cache_dir") else None,
            mask_policy=str(config.get("mask_policy", "nomask")),
        )


@dataclass(frozen=True)
class RegridConfig:
    """Configuration for optional regridding."""

    enabled: bool = False
    target_grid: str = "cmip7-1x1"
    method: str = "auto"
    grid_label: str = "gr"
    weights: WeightConfig = field(default_factory=WeightConfig)
    variable_methods: Mapping[str, str] = field(default_factory=dict)
    variable_classes: Mapping[str, str] = field(default_factory=dict)

    @classmethod
    def from_config(cls, config: Mapping[str, Any] | None) -> "RegridConfig":
        if config is None:
            return cls(enabled=False)
        if not isinstance(config, Mapping):
            raise TypeError("regrid configuration must be a mapping")
        return cls(
            enabled=bool(config.get("enabled", False)),
            target_grid=str(config.get("target_grid", "cmip7-1x1")),
            method=str(config.get("method", "auto")),
            grid_label=str(config.get("grid_label", "gr")),
            weights=WeightConfig.from_config(config.get("weights", "auto")),
            variable_methods=dict(config.get("variable_methods", {})),
            variable_classes=dict(config.get("variable_classes", {})),
        )

    @property
    def requires_regridding(self) -> bool:
        return self.enabled


def select_regrid_method(
    variable_name: str,
    variable_attrs: Mapping[str, Any] | None = None,
    config: RegridConfig | Mapping[str, Any] | None = None,
) -> str:
    """Select a safe first-pass regridding method for a CMOR variable.

    Explicit per-variable configuration wins.  ``method: auto`` then uses a
    deliberately conservative rules layer: flux-like quantities use first-order
    conservative, smooth states use bilinear, categorical/mask-like quantities
    use nearest-neighbour, and vector fields are refused until vector-aware grid
    location/rotation support is implemented.
    """

    cfg = (
        config
        if isinstance(config, RegridConfig)
        else RegridConfig.from_config(config)
    )
    attrs = variable_attrs or {}
    name = variable_name.split(".")[-1]

    if name in cfg.variable_methods:
        method = cfg.variable_methods[name]
        _validate_method(method)
        return method

    variable_class = cfg.variable_classes.get(name)
    if variable_class == "vector" or name in VECTOR_VARIABLES:
        raise RegridError(
            f"Variable '{name}' appears to be a vector/staggered-grid field. "
            "Scalar regridding is unsafe; configure a vector-aware workflow."
        )
    if variable_class == "flux":
        return "conservative"
    if variable_class == "mask":
        return "nearest_s2d"
    if variable_class == "state":
        return "bilinear"

    if cfg.method != "auto":
        _validate_method(cfg.method)
        return cfg.method

    standard_name = str(attrs.get("standard_name", "")).lower()
    cell_methods = str(attrs.get("cell_methods", "")).lower()
    units = str(attrs.get("units", ""))

    if name in MASK_NAMES or "status_flag" in standard_name or "region" in name:
        return "nearest_s2d"
    if name in FRACTION_NAMES:
        return "conservative"
    if (
        "flux" in standard_name
        or "precipitation" in standard_name
        or "evaporation" in standard_name
        or "sum" in cell_methods
        or units in {"kg m-2 s-1", "W m-2", "mol m-2 s-1"}
    ):
        return "conservative"
    if units in STATE_UNITS or any(
        key in standard_name for key in ("temperature", "salinity")
    ):
        return "bilinear"
    return "bilinear"


def apply_optional_regridding(
    ds: xr.Dataset,
    variable_name: str,
    config: RegridConfig | Mapping[str, Any] | None,
    *,
    source_id: str | None = None,
    source_grid_label: str | None = None,
) -> xr.Dataset:
    """Apply configured regridding to a CMORised dataset if enabled."""

    cfg = (
        config
        if isinstance(config, RegridConfig)
        else RegridConfig.from_config(config)
    )
    if not cfg.enabled:
        return ds
    if variable_name not in ds:
        raise RegridError(f"Cannot regrid missing variable '{variable_name}'")

    method = select_regrid_method(variable_name, ds[variable_name].attrs, cfg)
    weight_path = _resolve_weight_path(ds, cfg, method, source_id, source_grid_label)

    if weight_path.exists():
        out = _apply_cached_weights(ds, variable_name, cfg, method, weight_path)
    elif cfg.weights.mode in {"create", "reuse_or_create"}:
        _ensure_safe_weight_generation_inputs(ds, variable_name, method)
        out = _create_and_apply_weights(ds, variable_name, cfg, method, weight_path)
    else:
        raise RegridError(
            f"Regridding weights not found: {weight_path}. "
            "Use weights.mode: reuse_or_create/create or provide a cached file."
        )

    out.attrs["grid_label"] = cfg.grid_label
    out.attrs["grid"] = f"regridded to {cfg.target_grid} using {method}"
    out[variable_name].attrs["coordinates"] = _coordinates_attr(out[variable_name])
    out[variable_name].attrs.pop("cell_measures", None)
    return out


def build_target_grid(target_grid: str) -> xr.Dataset:
    """Build a small built-in regular target grid definition."""

    normalised = target_grid.lower()
    if normalised not in {"1x1", "cmip7-1x1", "global_1x1"}:
        raise RegridError(
            f"Unknown target_grid '{target_grid}'. "
            "First pass supports 'cmip7-1x1'/'1x1'."
        )

    lon = np.arange(0.5, 360.0, 1.0)
    lat = np.arange(-89.5, 90.0, 1.0)
    lon_bnds = np.column_stack((lon - 0.5, lon + 0.5))
    lat_bnds = np.column_stack((lat - 0.5, lat + 0.5))
    return xr.Dataset(
        coords={
            "lon": (
                "lon",
                lon,
                {
                    "standard_name": "longitude",
                    "units": "degrees_east",
                    "bounds": "lon_bnds",
                },
            ),
            "lat": (
                "lat",
                lat,
                {
                    "standard_name": "latitude",
                    "units": "degrees_north",
                    "bounds": "lat_bnds",
                },
            ),
            "bnds": np.arange(2),
        },
        data_vars={
            "lon_bnds": (("lon", "bnds"), lon_bnds, {"units": "degrees_east"}),
            "lat_bnds": (("lat", "bnds"), lat_bnds, {"units": "degrees_north"}),
        },
        attrs={"grid_label": "gr", "grid": target_grid},
    )


def _validate_method(method: str) -> None:
    if method not in REGRID_METHODS:
        raise RegridError(
            f"Unsupported regrid method '{method}'. "
            f"Expected one of {sorted(REGRID_METHODS)}"
        )


def _resolve_weight_path(
    ds: xr.Dataset,
    cfg: RegridConfig,
    method: str,
    source_id: str | None,
    source_grid_label: str | None,
) -> Path:
    if cfg.weights.path is not None:
        return cfg.weights.path
    cache_dir = cfg.weights.cache_dir or Path("regrid_weights")
    cache_dir.mkdir(parents=True, exist_ok=True)
    source = source_id or str(ds.attrs.get("source_id", "unknown-source"))
    grid = source_grid_label or str(ds.attrs.get("grid_label", "native"))
    digest = _grid_hash(ds, cfg.target_grid, method, cfg.weights.mask_policy)
    name = (
        f"{source}_{grid}_to_{cfg.target_grid}_{method}_"
        f"{cfg.weights.mask_policy}_{digest}.nc"
    )
    return cache_dir / name


def _grid_hash(ds: xr.Dataset, target_grid: str, method: str, mask_policy: str) -> str:
    payload: dict[str, Any] = {
        "target_grid": target_grid,
        "method": method,
        "mask_policy": mask_policy,
    }
    for coord_name in ("lat", "lon", "latitude", "longitude"):
        if coord_name in ds:
            coord = ds[coord_name]
            payload[coord_name] = {"dims": coord.dims, "shape": coord.shape}
            values = np.asarray(coord.values)
            if values.size:
                payload[coord_name]["sample"] = [
                    float(values.flat[0]),
                    float(values.flat[-1]),
                ]
    blob = json.dumps(payload, sort_keys=True).encode("utf-8")
    return hashlib.sha256(blob).hexdigest()[:8]


def _ensure_safe_weight_generation_inputs(
    ds: xr.Dataset, variable_name: str, method: str
) -> None:
    if method != "conservative":
        return
    bounds = set(ds.variables)
    has_rect_bounds = {"lat_bnds", "lon_bnds"}.issubset(bounds)
    has_curv_bounds = {"vertices_latitude", "vertices_longitude"}.issubset(bounds)
    if not (has_rect_bounds or has_curv_bounds):
        raise RegridError(
            f"Conservative regridding for '{variable_name}' requires source "
            "cell bounds/corners "
            "(lat_bnds/lon_bnds or vertices_latitude/vertices_longitude)."
        )


def _create_and_apply_weights(
    ds: xr.Dataset,
    variable_name: str,
    cfg: RegridConfig,
    method: str,
    weight_path: Path,
) -> xr.Dataset:
    try:
        import xesmf as xe  # type: ignore[import-not-found]
    except ImportError as exc:
        raise RegridError(
            "xESMF/ESMF is required to generate regridding weights. "
            "Install optional dependencies or provide a cached weights file."
        ) from exc

    target = build_target_grid(cfg.target_grid)
    regridder = xe.Regridder(
        ds, target, method, filename=str(weight_path), reuse_weights=False
    )
    regridded = regridder(ds[variable_name])
    out = _replace_regridded_variable(ds, variable_name, regridded, target)
    return out


def _apply_cached_weights(
    ds: xr.Dataset,
    variable_name: str,
    cfg: RegridConfig,
    method: str,
    weight_path: Path,
) -> xr.Dataset:
    del method  # Method is encoded in the weight file; kept for future validation.
    weights = xr.open_dataset(weight_path)
    try:
        row, col, data = _read_weight_triplets(weights)
        variable = ds[variable_name]
        h_dims = _infer_horizontal_dims(variable, ds)
        regridded = _apply_sparse_weights(
            variable, row, col, data, cfg.target_grid, h_dims
        )
        target = build_target_grid(cfg.target_grid)
        return _replace_regridded_variable(ds, variable_name, regridded, target)
    finally:
        weights.close()


def _read_weight_triplets(
    weights: xr.Dataset,
) -> tuple[np.ndarray, np.ndarray, np.ndarray]:
    row_name = "row" if "row" in weights else "dst_address"
    col_name = "col" if "col" in weights else "src_address"
    data_name = "S" if "S" in weights else "weights"
    missing = [name for name in (row_name, col_name, data_name) if name not in weights]
    if missing:
        raise RegridError(f"Weight file is missing ESMF sparse variables: {missing}")
    row = np.asarray(weights[row_name].values, dtype=np.int64)
    col = np.asarray(weights[col_name].values, dtype=np.int64)
    data = np.asarray(weights[data_name].values, dtype=np.float64)
    if row.size and row.min() >= 1:
        row = row - 1
    if col.size and col.min() >= 1:
        col = col - 1
    return row, col, data


def _infer_horizontal_dims(variable: xr.DataArray, ds: xr.Dataset) -> tuple[str, str]:
    dims = variable.dims
    for pair in (("lat", "lon"), ("latitude", "longitude"), ("j", "i"), ("y", "x")):
        if pair[0] in dims and pair[1] in dims:
            return pair
    if "latitude" in ds and "longitude" in ds:
        lat_dims = [dim for dim in ds["latitude"].dims if dim in dims]
        lon_dims = [dim for dim in ds["longitude"].dims if dim in dims]
        common = [dim for dim in lat_dims if dim in lon_dims]
        if len(common) >= 2:
            return common[-2], common[-1]
    raise RegridError(f"Could not infer horizontal dimensions for '{variable.name}'")


def _apply_sparse_weights(
    variable: xr.DataArray,
    row: np.ndarray,
    col: np.ndarray,
    weights: np.ndarray,
    target_grid: str,
    horizontal_dims: tuple[str, str],
) -> xr.DataArray:
    target = build_target_grid(target_grid)
    target_shape = (target.sizes["lat"], target.sizes["lon"])
    n_out = target_shape[0] * target_shape[1]

    transposed_dims = [
        dim for dim in variable.dims if dim not in horizontal_dims
    ] + list(horizontal_dims)
    arr = np.asarray(variable.transpose(*transposed_dims).values)
    lead_shape = arr.shape[:-2]
    src_size = arr.shape[-2] * arr.shape[-1]
    if col.size and int(col.max()) >= src_size:
        raise RegridError(
            "Weight file source grid is larger than the variable horizontal grid"
        )
    if row.size and int(row.max()) >= n_out:
        raise RegridError(
            "Weight file target grid does not match configured target_grid"
        )

    flat_in = arr.reshape((-1, src_size))
    flat_out = np.zeros(
        (flat_in.shape[0], n_out), dtype=np.result_type(arr.dtype, weights.dtype)
    )
    for sample in range(flat_in.shape[0]):
        np.add.at(flat_out[sample], row, weights * flat_in[sample, col])

    out = flat_out.reshape(lead_shape + target_shape)
    dims = transposed_dims[:-2] + ["lat", "lon"]
    coords = {
        dim: variable.coords[dim]
        for dim in transposed_dims[:-2]
        if dim in variable.coords
    }
    coords.update({"lat": target["lat"], "lon": target["lon"]})
    return xr.DataArray(
        out, dims=dims, coords=coords, attrs=dict(variable.attrs), name=variable.name
    )


def _replace_regridded_variable(
    ds: xr.Dataset,
    variable_name: str,
    regridded: xr.DataArray,
    target: xr.Dataset,
) -> xr.Dataset:
    keep = [
        name
        for name in ds.data_vars
        if name == variable_name
        or not (
            _looks_like_native_grid_var(name)
            or _has_native_horizontal_dims(ds[name])
        )
    ]
    out = ds[keep].copy()
    out = out.drop_vars(variable_name, errors="ignore")
    out = out.drop_vars(
        [
            name
            for name in (
                "lat",
                "lon",
                "latitude",
                "longitude",
                "vertices_latitude",
                "vertices_longitude",
            )
            if name in out.variables
        ],
        errors="ignore",
    )
    out = out.assign_coords({"lat": target["lat"], "lon": target["lon"]})
    out["lat_bnds"] = target["lat_bnds"]
    out["lon_bnds"] = target["lon_bnds"]
    out[variable_name] = regridded
    out["lat"].attrs.update(target["lat"].attrs)
    out["lon"].attrs.update(target["lon"].attrs)
    out["lat_bnds"].attrs.update(target["lat_bnds"].attrs)
    out["lon_bnds"].attrs.update(target["lon_bnds"].attrs)
    return out


def _has_native_horizontal_dims(variable: xr.DataArray) -> bool:
    horizontal = {"lat", "lon", "latitude", "longitude", "i", "j", "x", "y"}
    return bool(horizontal & set(variable.dims))


def _looks_like_native_grid_var(name: str) -> bool:
    return name in {
        "latitude",
        "longitude",
        "vertices_latitude",
        "vertices_longitude",
        "i",
        "j",
        "vertices",
    }


def _coordinates_attr(variable: xr.DataArray) -> str:
    coords = [
        coord
        for coord in ("time", "lev", "lat", "lon", "height")
        if coord in variable.coords or coord in variable.dims
    ]
    return " ".join(coords)


def create_weights(
    source_grid: str, target_grid: str, method: str, output: str
) -> Path:
    """Create xESMF weights from a source-grid NetCDF file."""

    _validate_method(method)
    source = xr.open_dataset(source_grid)
    try:
        _ensure_safe_weight_generation_inputs(source, "source_grid", method)
        target = build_target_grid(target_grid)
        try:
            import xesmf as xe  # type: ignore[import-not-found]
        except ImportError as exc:
            raise RegridError(
                "xESMF/ESMF is required to generate regridding weights"
            ) from exc
        xe.Regridder(source, target, method, filename=output, reuse_weights=False)
        return Path(output)
    finally:
        source.close()


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(
        description="Create ACCESS-MOPPy cached regridding weights"
    )
    subparsers = parser.add_subparsers(dest="command", required=True)
    create = subparsers.add_parser("create", help="create xESMF/ESMF weight file")
    create.add_argument(
        "--source-grid",
        required=True,
        help="NetCDF file containing source grid coordinates/bounds",
    )
    create.add_argument(
        "--target-grid", required=True, help="Target grid name, e.g. cmip7-1x1"
    )
    create.add_argument("--method", required=True, choices=sorted(REGRID_METHODS))
    create.add_argument("--output", required=True, help="Output weight NetCDF file")
    args = parser.parse_args(argv)
    if args.command == "create":
        path = create_weights(
            args.source_grid, args.target_grid, args.method, args.output
        )
        print(f"Created regridding weights: {path}")


if __name__ == "__main__":
    main()
