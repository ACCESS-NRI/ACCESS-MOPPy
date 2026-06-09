"""
access_moppy.qc.base
====================

Core abstractions for the QC framework:

- :class:`QCStatus`  — enumerated check outcome
- :class:`QCResult`  — single check result
- :class:`QCCheck`   — base class for all checks
- :func:`register_check` / :func:`get_checks` — global check registry
"""
from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Any

import xarray as xr


class QCStatus(str, Enum):
    """Outcome of a single QC check."""

    PASS = "pass"
    WARN = "warn"
    FAIL = "fail"
    SKIP = "skip"


@dataclass
class QCResult:
    """Result of running one :class:`QCCheck` against a dataset.

    Attributes
    ----------
    check_name:
        Unique identifier of the check (e.g. ``"temporal.time_monotonicity"``).
    status:
        Outcome of the check.
    message:
        Human-readable summary of the outcome.
    details:
        Optional dict of diagnostic key/value pairs for machine consumption
        (e.g. ``{"actual_min": -0.5, "valid_min": 0.0}``).
    """

    check_name: str
    status: QCStatus
    message: str
    details: dict[str, Any] = field(default_factory=dict)


class QCCheck:
    """Base class for all QC checks.

    Subclass this and implement :meth:`run`.  The :attr:`name` class attribute
    must be a unique dot-separated identifier such as
    ``"temporal.time_monotonicity"``.

    Convenience helper methods (:meth:`_pass`, :meth:`_warn`, :meth:`_fail`,
    :meth:`_skip`) build :class:`QCResult` objects so subclasses do not need to
    import :class:`QCResult` or :class:`QCStatus` directly.

    Parameters passed as keyword arguments to the helpers end up in
    ``QCResult.details`` for machine-readable diagnostics.
    """

    name: str = ""

    def run(self, ds: xr.Dataset, context: dict[str, Any]) -> QCResult:
        """Run the check against *ds* and return a :class:`QCResult`.

        Args:
            ds:
                The CMORised xarray Dataset to inspect.
            context:
                Dict carrying metadata needed by some checks.  Common keys:

                - ``"cmor_name"``   — the CMIP variable name (e.g. ``"tas"``).
                - ``"vocab"``       — a vocabulary instance
                  (:class:`~access_moppy.vocabulary_processors.CMIP6Vocabulary`
                  etc.).
                - ``"compound_name"`` — e.g. ``"Amon.tas"``.

        Returns:
            A :class:`QCResult` with an appropriate :class:`QCStatus`.
        """
        raise NotImplementedError(f"QCCheck subclass '{type(self).__name__}' must implement run()")

    # ------------------------------------------------------------------
    # Convenience result builders
    # ------------------------------------------------------------------

    def _pass(self, message: str = "OK", **details: Any) -> QCResult:
        return QCResult(self.name, QCStatus.PASS, message, details)

    def _warn(self, message: str, **details: Any) -> QCResult:
        return QCResult(self.name, QCStatus.WARN, message, details)

    def _fail(self, message: str, **details: Any) -> QCResult:
        return QCResult(self.name, QCStatus.FAIL, message, details)

    def _skip(self, reason: str) -> QCResult:
        return QCResult(self.name, QCStatus.SKIP, reason)


# ---------------------------------------------------------------------------
# Global registry
# ---------------------------------------------------------------------------

_REGISTRY: dict[str, QCCheck] = {}


def register_check(check: QCCheck) -> QCCheck:
    """Register *check* in the global registry and return it.

    Typically called at module level in each ``checks/*.py`` file so that
    importing the module is sufficient to make the check available.

    Args:
        check: An instantiated :class:`QCCheck`.

    Returns:
        The same *check* instance (allows use as a decorator).
    """
    _REGISTRY[check.name] = check
    return check


def get_checks(names: list[str] | None = None) -> list[QCCheck]:
    """Return checks from the global registry.

    Args:
        names:
            Optional list of check names to retrieve.  When ``None`` all
            registered checks are returned in registration order.

    Returns:
        List of :class:`QCCheck` instances.
    """
    if names is None:
        return list(_REGISTRY.values())
    return [_REGISTRY[n] for n in names if n in _REGISTRY]
