from . import _version
from ._config import _creator
from .driver import ACCESS_ESM_CMORiser

__version__ = _version.get_versions()["version"]

# Print the configuration information
print("\nLoaded Configuration:")
print(f"Creator Name: {_creator.creator_name}")
print(f"Organisation: {_creator.organisation}")
print(f"Creator Email: {_creator.creator_email}")
print(f"Creator URL: {_creator.creator_url}")
