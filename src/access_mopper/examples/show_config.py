import shutil
import sys
from importlib.resources import files
from pathlib import Path


def main():
    example_file = files("access_mopper.examples").joinpath("batch_config.yml")

    if len(sys.argv) == 2:
        target_path = Path(sys.argv[1])
        shutil.copy(example_file, target_path)
        print(f"Example config copied to {target_path}")
    else:
        with example_file.open("r") as f:
            print(f.read())
