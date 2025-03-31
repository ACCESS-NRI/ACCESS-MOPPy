from . import _version
from .access_mopper import *
from .ocean_supergrid import Supergrid

__version__ = _version.get_versions()['version']

import os
import yaml

CONFIG_DIR = os.path.expanduser("~/.mopper")
CONFIG_PATH = os.path.join(CONFIG_DIR, "user.yml")

def prompt_user_config():
    """Prompt the user for configuration details and save them in user.yml."""
    print("No configuration file found. Please enter the following details:")
    config_data = {
        "creator_name": input("Your name: ").strip(),
        "institution": input("Institution (e.g. ANU): ").strip(),
        "organisation": input("Organisation (e.g. ACCESS-NRI): ").strip(),
        "creator_email": input("Your email: ").strip(),
        "creator_url": input("Your ORCID or website: ").strip(),
    }

    # Ensure the directory exists
    os.makedirs(CONFIG_DIR, exist_ok=True)

    # Save data to YAML file
    with open(CONFIG_PATH, "w") as file:
        yaml.safe_dump(config_data, file, default_flow_style=False)

    print(f"Configuration saved to {CONFIG_PATH}")
    return config_data

def load_mopper_config():
    """Load ~/.mopper/user.yml, or prompt the user to create it if missing."""
    if os.path.isfile(CONFIG_PATH):
        with open(CONFIG_PATH, "r") as file:
            config_data = yaml.safe_load(file)
        
        # Print the configuration information
        print("\nLoaded Configuration:")
        print(f"Creator Name: {config_data.get('creator_name')}")
        print(f"Institution: {config_data.get('institution')}")
        print(f"Organisation: {config_data.get('organisation')}")
        print(f"Creator Email: {config_data.get('creator_email')}")
        print(f"Creator URL: {config_data.get('creator_url')}")
        
        return config_data
    else:
        return prompt_user_config()

# Load config when the package is imported
MOPPER_CONFIG = load_mopper_config()

_creator = Creator(**MOPPER_CONFIG)