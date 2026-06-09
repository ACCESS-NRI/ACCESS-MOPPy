"""
access_moppy.qc
===============

Quality-control framework for CMORised ACCESS-ESM output.

Built-in checks cover:

* **metadata** — required CMIP6 global attributes, variable name, units,
  cell_methods.
* **data_integrity** — valid_min/valid_max from CMOR table, missing-value
  fraction, _FillValue consistency.
* **temporal** — time monotonicity, duplicate time steps, time_bnds validity.
* **spatial** — latitude/longitude ranges, coordinate bounds presence.
* **global_stats** — per-variable physical min/max thresholds and global-mean
  climatological ranges, derived from the ESM1.6 pre-publication QC document.

Quick start::

    import xarray as xr
    from access_moppy.qc import QCRunner
    from access_moppy.vocabulary_processors import CMIP6Vocabulary

    vocab = CMIP6Vocabulary(
        compound_name="Amon.tas",
        experiment_id="historical",
        source_id="ACCESS-ESM1-5",
        variant_label="r1i1p1f1",
        grid_label="gn",
    )

    ds = xr.open_dataset("tas_Amon_ACCESS-ESM1-5_historical_r1i1p1f1_gn_185001-201412.nc")
    report = QCRunner().run(ds, context={"vocab": vocab, "cmor_name": "tas"})
    print(report.summary())
    report.to_json("qc_tas.json")

Adding a custom check::

    from access_moppy.qc import QCCheck, register_check, QCRunner

    class MyCheck(QCCheck):
        name = "custom.my_check"

        def run(self, ds, context):
            if "my_var" not in ds:
                return self._skip("my_var not present")
            return self._pass("my_var found")

    register_check(MyCheck())
    report = QCRunner().run(ds, context={"cmor_name": "tas"})

Running only selected checks::

    from access_moppy.qc import QCRunner
    from access_moppy.qc.base import get_checks

    checks = get_checks(["temporal.time_monotonicity", "temporal.time_duplicates"])
    report = QCRunner(checks=checks).run(ds, context={"cmor_name": "tas"})
"""
from .base import QCCheck, QCResult, QCStatus, get_checks, register_check
from .report import QCReport
from .runner import QCRunner

# Importing checks triggers registration of all built-in checks.
from . import checks  # noqa: F401

__all__ = [
    "QCCheck",
    "QCResult",
    "QCStatus",
    "QCReport",
    "QCRunner",
    "register_check",
    "get_checks",
]
