import importlib.resources as resources
from pathlib import Path
from tempfile import gettempdir
import subprocess
import access_mopper.vocabularies.cmip6_cmor_tables.Tables as cmor_tables

import pandas as pd
import pytest

from access_mopper import ACCESS_ESM_CMORiser

DATA_DIR = Path(__file__).parent / "data"


@pytest.fixture
def parent_experiment_config():
    return {
        "parent_experiment_id": "piControl",
        "parent_activity_id": "CMIP",
        "parent_source_id": "ACCESS-ESM1-5",
        "parent_variant_label": "r1i1p1f1",
        "parent_time_units": "days since 0001-01-01 00:00:00",
        "parent_mip_era": "CMIP6",
        "branch_time_in_child": 0.0,
        "branch_time_in_parent": 54786.0,
        "branch_method": "standard",
    }


def test_model_function():
    test_file = DATA_DIR / "esm1-6/atmosphere/aiihca.pa-101909_mon.nc"
    assert test_file.exists(), "Test data file missing!"


def load_filtered_variables(mappings):
    with resources.files("access_mopper.mappings").joinpath(mappings).open() as f:
        df = pd.read_json(f, orient="index")
    return df.index.tolist()


@pytest.mark.parametrize(
    "cmor_name", load_filtered_variables("Mappings_CMIP6_Amon.json")
)
def test_cmorise_CMIP6_Amon(parent_experiment_config, cmor_name):
    file_pattern = DATA_DIR / "esm1-6/atmosphere/aiihca.pa-101909_mon.nc"
    output_dir = Path(gettempdir()) / "cmor_output"

    with resources.path(cmor_tables, "CMIP6_Amon.json") as table_path:
        try:
            cmoriser = ACCESS_ESM_CMORiser(
                input_paths=file_pattern,
                compound_name="Amon." + cmor_name,
                experiment_id="historical",
                source_id="ACCESS-ESM1-5",
                variant_label="r1i1p1f1",
                grid_label="gn",
                activity_id="CMIP",
                parent_info=parent_experiment_config,
                output_path=output_dir,
            )
            cmoriser.run()

            output_files = list(output_dir.glob(f"{cmor_name}_Amon_*.nc"))
            assert (
                output_files
            ), f"No output files found for {cmor_name} in {output_dir}"

            cmd = [
                "PrePARE",
                "--variable",
                cmor_name,
                "--table-path",
                str(table_path),
                str(output_files[0]),
            ]
            result = subprocess.run(cmd, capture_output=True, text=True, check=False)

            if result.returncode != 0:
                pytest.fail(
                    f"PrePARE failed for {output_files[0]}:\n"
                    f"stdout:\n{result.stdout}\n\nstderr:\n{result.stderr}"
                )

        except Exception as e:
            pytest.fail(
                f"Failed processing {cmor_name} with table {table_path.name}: {e}"
            )


@pytest.mark.parametrize(
    "cmor_name", load_filtered_variables("Mappings_CMIP6_Lmon.json")
)
def test_cmorise_CMIP6_Lmon(parent_experiment_config, cmor_name):
    file_pattern = DATA_DIR / "esm1-6/atmosphere/aiihca.pa-101909_mon.nc"
    output_dir = Path(gettempdir()) / "cmor_output"

    with resources.path(cmor_tables, "CMIP6_Lmon.json") as table_path:
        try:
            cmoriser = ACCESS_ESM_CMORiser(
                input_paths=file_pattern,
                compound_name="Lmon." + cmor_name,
                experiment_id="historical",
                source_id="ACCESS-ESM1-5",
                variant_label="r1i1p1f1",
                grid_label="gn",
                activity_id="CMIP",
                parent_info=parent_experiment_config,
                output_path=output_dir,
            )
            cmoriser.run()

            output_files = list(output_dir.glob(f"{cmor_name}_Lmon_*.nc"))
            assert (
                output_files
            ), f"No output files found for {cmor_name} in {output_dir}"

            cmd = [
                "PrePARE",
                "--variable",
                cmor_name,
                "--table-path",
                str(table_path),
                str(output_files[0]),
            ]
            result = subprocess.run(cmd, capture_output=True, text=True, check=False)

            if result.returncode != 0:
                pytest.fail(
                    f"PrePARE failed for {output_files[0]}:\n"
                    f"stdout:\n{result.stdout}\n\nstderr:\n{result.stderr}"
                )

        except Exception as e:
            pytest.fail(
                f"Failed processing {cmor_name} with table {table_path.name}: {e}"
            )


@pytest.mark.parametrize(
    "cmor_name", load_filtered_variables("Mappings_CMIP6_Emon.json")
)
def test_cmorise_CMIP6_Emon(parent_experiment_config, cmor_name):
    file_pattern = DATA_DIR / "esm1-6/atmosphere/aiihca.pa-101909_mon.nc"
    output_dir = Path(gettempdir()) / "cmor_output"

    with resources.path(cmor_tables, "CMIP6_Emon.json") as table_path:
        try:
            cmoriser = ACCESS_ESM_CMORiser(
                input_paths=file_pattern,
                compound_name="Emon." + cmor_name,
                experiment_id="historical",
                source_id="ACCESS-ESM1-5",
                variant_label="r1i1p1f1",
                grid_label="gn",
                activity_id="CMIP",
                parent_info=parent_experiment_config,
                output_path=output_dir,
            )
            cmoriser.run()

            output_files = list(output_dir.glob(f"{cmor_name}_Emon_*.nc"))
            assert (
                output_files
            ), f"No output files found for {cmor_name} in {output_dir}"

            cmd = [
                "PrePARE",
                "--variable",
                cmor_name,
                "--table-path",
                str(table_path),
                str(output_files[0]),
            ]
            result = subprocess.run(cmd, capture_output=True, text=True, check=False)

            if result.returncode != 0:
                pytest.fail(
                    f"PrePARE failed for {output_files[0]}:\n"
                    f"stdout:\n{result.stdout}\n\nstderr:\n{result.stderr}"
                )

        except Exception as e:
            pytest.fail(
                f"Failed processing {cmor_name} with table {table_path.name}: {e}"
            )
