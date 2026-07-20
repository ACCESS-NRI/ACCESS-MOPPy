import argparse
import shutil
from importlib.resources import files
from pathlib import Path


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="moppy-example-config",
        description=(
            "Display or copy the ACCESS-MOPPy example batch configuration file."
        ),
    )
    parser.add_argument(
        "output",
        nargs="?",
        metavar="DEST",
        help=(
            "Destination path to copy the example config to. "
            "If omitted, the config is printed to stdout."
        ),
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    args = _build_parser().parse_args(argv)
    example_file = files("access_moppy.examples").joinpath("batch_config.yml")

    if args.output:
        target_path = Path(args.output)
        shutil.copy(example_file, target_path)
        print(f"Example config copied to {target_path}")
    else:
        with example_file.open("r") as f:
            print(f.read(), end="")
    return 0
