import os

import yaml

CONFIG_DIR = os.path.expanduser("~/.moppy")
CONFIG_PATH = os.path.join(CONFIG_DIR, "user.yml")


def prompt_user_config():
    """Prompt the user for configuration details and save them in user.yml."""
    print("No configuration file found. Please enter the following details:")
    config_data = {
        "creator_name": input("Your name: ").strip(),
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


def load_moppy_config():
    """Load ~/.moppy/user.yml, or prompt the user to create it if missing."""
    if os.path.isfile(CONFIG_PATH):
        with open(CONFIG_PATH, "r") as file:
            config_data = yaml.safe_load(file)

        # Ensure all required keys are present in the configuration
        required_keys = ["creator_name", "organisation", "creator_email", "creator_url"]
        missing_keys = [
            key
            for key in required_keys
            if key not in config_data or not config_data[key]
        ]

        if missing_keys:
            print(f"Missing or empty configuration keys: {', '.join(missing_keys)}")
            return prompt_user_config()

        return config_data
    else:
        return prompt_user_config()


# Load config when the package is imported
MOPPY_CONFIG = load_moppy_config()


class Creator:
    institution: str = ""
    organisation: str = ""
    creator_name: str = ""
    creator_email: str = ""
    creator_url: str = ""


_creator = Creator()

# Initialise creator information for all experiments
_creator.creator_name = MOPPY_CONFIG["creator_name"]
_creator.organisation = MOPPY_CONFIG["organisation"]
_creator.creator_email = MOPPY_CONFIG["creator_email"]
_creator.creator_url = MOPPY_CONFIG["creator_url"]
