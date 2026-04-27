import logging

from . import _version
from ._config import _creator
from .driver import ACCESS_ESM_CMORiser
from .utilities import check_for_updates

__version__ = _version.get_versions()["version"]

# Add a NullHandler so library logs are silent unless the caller configures logging.
logging.getLogger(__name__).addHandler(logging.NullHandler())


def _is_jupyter() -> bool:
    """Return True when running inside a Jupyter kernel."""
    try:
        from IPython import get_ipython

        ip = get_ipython()
        return ip is not None and "IPKernelApp" in ip.config
    except Exception:
        return False


if _is_jupyter():
    # In notebooks, show DEBUG and above on stderr so progress messages are
    # visible without any extra configuration by the user.
    _handler = logging.StreamHandler()
    _handler.setFormatter(logging.Formatter("%(levelname)s %(name)s: %(message)s"))
    _pkg_logger = logging.getLogger(__name__)
    _pkg_logger.setLevel(logging.DEBUG)
    _pkg_logger.addHandler(_handler)

_logger = logging.getLogger(__name__)

# Log the configuration information
_logger.debug("Loaded Configuration:")
_logger.debug("Creator Name: %s", _creator.creator_name)
_logger.debug("Organisation: %s", _creator.organisation)
_logger.debug("Creator Email: %s", _creator.creator_email)
_logger.debug("Creator URL: %s", _creator.creator_url)

check_for_updates()
