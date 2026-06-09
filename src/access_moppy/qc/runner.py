"""
access_moppy.qc.runner
=======================

:class:`QCRunner` — applies a collection of checks to a dataset.
"""
from __future__ import annotations

import logging
from typing import Any

import xarray as xr

from .base import QCCheck, QCResult, QCStatus, get_checks
from .report import QCReport

logger = logging.getLogger(__name__)


class QCRunner:
    """Apply a collection of :class:`~access_moppy.qc.base.QCCheck` instances
    to a dataset and return a :class:`~access_moppy.qc.report.QCReport`.

    Parameters
    ----------
    checks:
        Explicit list of :class:`~access_moppy.qc.base.QCCheck` instances to
        run.  When ``None`` (the default) all checks in the global registry are
        used — i.e. all checks registered by importing
        ``access_moppy.qc.checks``.

    Examples
    --------
    Run all registered checks against a CMORised dataset::

        from access_moppy.qc import QCRunner
        from access_moppy.vocabulary_processors import CMIP6Vocabulary

        vocab = CMIP6Vocabulary(compound_name="Amon.tas", ...)
        runner = QCRunner()
        report = runner.run(ds, context={"vocab": vocab, "cmor_name": "tas"})
        print(report.summary())

    Run only a specific subset of checks::

        from access_moppy.qc import QCRunner
        from access_moppy.qc.base import get_checks

        checks = get_checks(["temporal.time_monotonicity", "temporal.time_duplicates"])
        runner = QCRunner(checks=checks)
        report = runner.run(ds, context={"cmor_name": "tas"})
    """

    def __init__(self, checks: list[QCCheck] | None = None) -> None:
        # None is intentional: defer to registry at run time so that checks
        # registered after __init__ are still included.
        self._checks = checks

    def run(
        self,
        ds: xr.Dataset,
        context: dict[str, Any] | None = None,
    ) -> QCReport:
        """Run all checks and return a :class:`~access_moppy.qc.report.QCReport`.

        Args:
            ds:
                The CMORised xarray Dataset to inspect.
            context:
                Optional metadata dict.  Recognised keys:

                - ``"cmor_name"`` — the CMIP variable name, e.g. ``"tas"``.
                - ``"vocab"``     — a vocabulary instance
                  (:class:`~access_moppy.vocabulary_processors.CMIP6Vocabulary`
                  etc.) carrying the CMOR table entry for the variable.
                - ``"compound_name"`` — e.g. ``"Amon.tas"``; used to derive
                  ``cmor_name`` when the latter is absent.

        Returns:
            :class:`~access_moppy.qc.report.QCReport` with one result per check.
        """
        checks = self._checks if self._checks is not None else get_checks()
        ctx = dict(context or {})

        # Derive cmor_name from compound_name when not explicitly set
        if "cmor_name" not in ctx and "compound_name" in ctx:
            _, cmor_name = ctx["compound_name"].split(".", 1)
            ctx["cmor_name"] = cmor_name

        results: list[QCResult] = []
        for check in checks:
            try:
                result = check.run(ds, ctx)
            except Exception as exc:  # noqa: BLE001
                logger.warning(
                    "Check '%s' raised an unexpected exception: %s",
                    check.name,
                    exc,
                    exc_info=True,
                )
                result = QCResult(
                    check_name=check.name,
                    status=QCStatus.WARN,
                    message=f"Check raised an unexpected exception: {exc}",
                )
            results.append(result)
            logger.debug(
                "  [%s] %s: %s",
                result.status.value.upper(),
                result.check_name,
                result.message,
            )

        report = QCReport(results=results)
        logger.info(
            "QC complete — %d passed, %d warnings, %d failed, %d skipped",
            len(report.passed),
            len(report.warnings),
            len(report.failures),
            len(report.skipped),
        )
        return report
