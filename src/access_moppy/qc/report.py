"""
access_moppy.qc.report
=======================

:class:`QCReport` — structured container for the results of a full QC run.
"""
from __future__ import annotations

import json
from dataclasses import dataclass, field
from typing import Any

from .base import QCResult, QCStatus


@dataclass
class QCReport:
    """Collects the :class:`~access_moppy.qc.base.QCResult` objects from one
    :meth:`~access_moppy.qc.runner.QCRunner.run` call and provides summary
    views and serialisation helpers.

    Attributes
    ----------
    results:
        Ordered list of all check results.
    """

    results: list[QCResult] = field(default_factory=list)

    # ------------------------------------------------------------------
    # Filtered views
    # ------------------------------------------------------------------

    @property
    def passed(self) -> list[QCResult]:
        """Results with status PASS."""
        return [r for r in self.results if r.status == QCStatus.PASS]

    @property
    def warnings(self) -> list[QCResult]:
        """Results with status WARN."""
        return [r for r in self.results if r.status == QCStatus.WARN]

    @property
    def failures(self) -> list[QCResult]:
        """Results with status FAIL."""
        return [r for r in self.results if r.status == QCStatus.FAIL]

    @property
    def skipped(self) -> list[QCResult]:
        """Results with status SKIP."""
        return [r for r in self.results if r.status == QCStatus.SKIP]

    @property
    def overall_status(self) -> QCStatus:
        """Aggregate status: FAIL if any failures, WARN if any warnings, else PASS."""
        if self.failures:
            return QCStatus.FAIL
        if self.warnings:
            return QCStatus.WARN
        if self.passed:
            return QCStatus.PASS
        return QCStatus.SKIP

    # ------------------------------------------------------------------
    # Human-readable output
    # ------------------------------------------------------------------

    def summary(self) -> str:
        """Return a multi-line plain-text summary.

        Only FAIL and WARN results are listed individually; PASS and SKIP
        are shown as counts only.
        """
        lines = [
            f"QC Report  |  "
            f"{len(self.passed)} passed  "
            f"{len(self.warnings)} warnings  "
            f"{len(self.failures)} failed  "
            f"{len(self.skipped)} skipped",
            f"Overall status: {self.overall_status.value.upper()}",
        ]

        for label, items in (("FAIL", self.failures), ("WARN", self.warnings)):
            for r in items:
                lines.append(f"  [{label}] {r.check_name}: {r.message}")
                for k, v in r.details.items():
                    lines.append(f"         {k}: {v}")

        return "\n".join(lines)

    # ------------------------------------------------------------------
    # Machine-readable output
    # ------------------------------------------------------------------

    def to_dict(self) -> dict[str, Any]:
        """Serialise to a plain Python dict suitable for JSON encoding."""
        return {
            "overall_status": self.overall_status.value,
            "summary": {
                "passed": len(self.passed),
                "warnings": len(self.warnings),
                "failures": len(self.failures),
                "skipped": len(self.skipped),
            },
            "results": [
                {
                    "check": r.check_name,
                    "status": r.status.value,
                    "message": r.message,
                    "details": r.details,
                }
                for r in self.results
            ],
        }

    def to_json(self, path: str | None = None, indent: int = 2) -> str:
        """Serialise to JSON.

        Args:
            path: Optional file path to write to.
            indent: JSON indentation level.

        Returns:
            JSON string.
        """
        text = json.dumps(self.to_dict(), indent=indent, default=str)
        if path is not None:
            with open(path, "w", encoding="utf-8") as fh:
                fh.write(text)
        return text

    # ------------------------------------------------------------------
    # Dunder
    # ------------------------------------------------------------------

    def __str__(self) -> str:
        return self.summary()

    def __repr__(self) -> str:
        return (
            f"QCReport(passed={len(self.passed)}, "
            f"warnings={len(self.warnings)}, "
            f"failures={len(self.failures)}, "
            f"skipped={len(self.skipped)})"
        )
