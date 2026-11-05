"""Update the Pixi WCRP checker pin to the latest stable, non-yanked PyPI release."""

import argparse
import json
import os
from pathlib import Path
from urllib.request import urlopen

import tomlkit
from packaging.version import InvalidVersion, Version

PACKAGE = "cc-plugin-wcrp"
PYPI_URL = f"https://pypi.org/pypi/{PACKAGE}/json"


def latest_stable_release(releases: dict) -> Version:
    """Use PEP 440, since upstream's GitHub release flags also admit betas."""
    candidates = []
    for name, files in releases.items():
        try:
            version = Version(name)
        except InvalidVersion:
            continue
        if version.is_prerelease or version.is_devrelease or version.local:
            continue
        if any(not file["yanked"] for file in files):
            candidates.append(version)
    if not candidates:
        raise ValueError("PyPI has no stable, non-yanked cc-plugin-wcrp release")
    return max(candidates)


def update_pin(path: Path, latest: Version) -> tuple[Version, bool]:
    """Preserve TOML formatting and refuse to downgrade or replace a custom spec."""
    document = tomlkit.parse(path.read_text(encoding="utf-8"))
    dependencies = document["tool"]["pixi"]["pypi-dependencies"]
    constraint = dependencies[PACKAGE]
    if not isinstance(constraint, str) or not constraint.startswith("=="):
        raise ValueError("Expected an exact cc-plugin-wcrp pin (==<version>)")
    current = Version(constraint[2:])
    changed = latest > current
    if changed:
        dependencies[PACKAGE] = f"=={latest}"
        path.write_text(tomlkit.dumps(document), encoding="utf-8")
    return current, changed


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--pyproject", type=Path, default=Path("pyproject.toml"))
    args = parser.parse_args()

    # Fail before changing any files if PyPI is unavailable or its response is invalid.
    with urlopen(PYPI_URL, timeout=30) as response:
        latest = latest_stable_release(json.load(response)["releases"])
    current, changed = update_pin(args.pyproject, latest)

    if output_path := os.environ.get("GITHUB_OUTPUT"):
        with open(output_path, "a", encoding="utf-8") as output:
            output.write(f"current={current}\nlatest={latest}\n")
            output.write(f"changed={str(changed).lower()}\n")
    if changed:
        print(f"{PACKAGE}: {current} -> {latest}")
    else:
        print(f"{PACKAGE}: keeping {current}; latest stable release is {latest}")


if __name__ == "__main__":
    main()
