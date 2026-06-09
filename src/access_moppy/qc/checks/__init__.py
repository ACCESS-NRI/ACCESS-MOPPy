"""
access_moppy.qc.checks
=======================

Importing this package registers all built-in QC checks in the global registry.
"""
from . import data_integrity  # noqa: F401
from . import global_stats    # noqa: F401
from . import metadata        # noqa: F401
from . import spatial         # noqa: F401
from . import temporal        # noqa: F401
