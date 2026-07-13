"""Utility integration test to compare nbp method 2 and method 3 outputs.

This test intentionally does not change production mappings. It compares:

- Method 2: tile-based nbp formula currently used in mappings
- Method 3: tracer-flux shortcut (fld_s03i100 * 12/44)

The comparison is run on a limited set of monthly atmosphere files and a
limited number of time steps, so it can be used as a focused validation tool.
"""

from __future__ import annotations

import os
from pathlib import Path

import numpy as np
import pytest
import xarray as xr

DATA_ROOT_ENV_VAR = "ACCESS_MOPPY_DATA_ROOT"
MONTHLY_FILE_GLOB = "output*/atmosphere/netCDF/*_mon.nc"

# Method 2 constants from ACCESS-ESM mapping formula.
WOOD_RESP_SCALE = -3.1688087814029657e-14

# Method 3 conversion from kgCO2 to kgC.
CO2_TO_C_SCALE = 12.0 / 44.0

REQUIRED_VARS = (
    "fld_s03i100",
    "fld_s03i262",
    "fld_s03i293",
    "fld_s03i317",
    "fld_s03i395",
    "fld_s03i907",
    "fld_s03i908",
    "fld_s03i909",
)


def _discover_monthly_files(data_root: Path, max_files: int) -> list[Path]:
    return sorted(data_root.glob(MONTHLY_FILE_GLOB))[:max_files]


@pytest.mark.integration
@pytest.mark.slow
def test_nbp_method2_matches_method3_on_limited_range():
    """Compare nbp method 2 and method 3 over a small time subset."""
    data_root_value = os.getenv(DATA_ROOT_ENV_VAR)
    if not data_root_value:
        pytest.skip(f"{DATA_ROOT_ENV_VAR} is not set")

    data_root = Path(data_root_value)
    if not data_root.exists():
        pytest.skip(f"Data root does not exist: {data_root}")

    max_files = int(os.getenv("ACCESS_MOPPY_NBP_COMPARE_MAX_FILES", "2"))
    time_steps = int(os.getenv("ACCESS_MOPPY_NBP_COMPARE_TIME_STEPS", "3"))
    atol = float(os.getenv("ACCESS_MOPPY_NBP_COMPARE_ATOL", "1e-6"))

    files = _discover_monthly_files(data_root, max_files=max_files)
    if not files:
        pytest.skip(f"No monthly atmosphere files found under {data_root}")

    ds = xr.open_mfdataset(
        [str(path) for path in files],
        combine="by_coords",
        data_vars="minimal",
        coords="minimal",
        compat="override",
        join="outer",
    )

    missing_vars = [var for var in REQUIRED_VARS if var not in ds.variables]
    if missing_vars:
        pytest.skip(f"Required variables missing from test files: {missing_vars}")

    ds = ds[list(REQUIRED_VARS)].isel(time=slice(0, time_steps)).load()

    wood_resp_sum = ds["fld_s03i907"] + ds["fld_s03i908"] + ds["fld_s03i909"]

    # Wood respiration fluxes already account for tile fraction; sum over tiles directly.
    summed_tile_wood_resp = wood_resp_sum.sum(dim="pseudo_level_0") * ds["fld_s03i395"]

    method2 = (
        ds["fld_s03i262"]
        - ds["fld_s03i293"]
        + summed_tile_wood_resp * WOOD_RESP_SCALE
    ) / ds["fld_s03i395"]

    method3 = ds["fld_s03i100"] * CO2_TO_C_SCALE

    valid_points = (
        (ds["fld_s03i395"] > 0)
        & np.isfinite(method2)
        & np.isfinite(method3)
    )

    valid_count = int(valid_points.sum().item())
    assert valid_count > 0, "No valid points available for method comparison"

    abs_diff = np.abs((method2 - method3).where(valid_points))
    max_abs_diff = float(abs_diff.max(skipna=True))

    assert max_abs_diff <= atol, (
        "nbp method mismatch detected: "
        f"max_abs_diff={max_abs_diff:.6e}, atol={atol:.6e}, "
        f"files={len(files)}, time_steps={time_steps}, valid_points={valid_count}"
    )
