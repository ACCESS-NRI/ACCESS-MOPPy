import importlib.resources as resources
from pathlib import Path

import pandas as pd
import pytest
from access_mopper.configurations import ACCESS_ESM16_CMIP6, cmorise

DATA_DIR = Path(__file__).parent / "data"


@pytest.fixture
def model():
    # Create and save the model
    model_instance = ACCESS_ESM16_CMIP6(
        experiment_id="piControl-spinup",
        realization_index="1",
        initialization_index="1",
        physics_index="1",
        forcing_index="1",
        parent_mip_era="no parent",
        parent_activity_id="no parent",
        parent_experiment_id="no parent",
        parent_source_id="no parent",
        parent_variant_label="no parent",
        parent_time_units="no parent",
        branch_method="no parent",
        branch_time_in_parent=0.0,
        branch_time_in_child=0.0,
    )
    model_instance.save_to_file("model.json")
    return model_instance


def test_model_function():
    test_file = DATA_DIR / "esm1-6/atmosphere/aiihca.pa-101909_mon.nc"
    assert test_file.exists(), "Test data file missing!"


def load_filtered_variables(mappings):
    # Load and filter variables from the JSON file
    with resources.files("access_mopper.mappings").joinpath(mappings).open() as f:
        df = pd.read_json(f, orient="index")
    return df.index.tolist()


# @pytest.mark.parametrize("cmor_name", load_filtered_variables("Mappings_CMIP6_Omon.json"))
# def test_cmorise_CMIP6_Omon(model, cmor_name):
#     file_pattern = "/home/romain/PROJECTS/ACCESS-MOPPeR/Test_data//cj877/history/ocn/ocean-2d-sea_level-1-monthly-mean-ym_0326_01.nc"
#     try:
#         mop.cmorise(
#             file_paths=glob.glob(file_pattern),
#             compound_name="Omon."+ cmor_name,
#             reference_time="1850-01-01 00:00:00",
#             cmor_dataset_json="model.json",
#             mip_table="CMIP6_Omon.json"
#         )
#     except Exception as e:
#         pytest.fail(f"Failed processing {cmor_name} with table CMIP6_Omon.json: {e}")


@pytest.mark.parametrize(
    "cmor_name", load_filtered_variables("Mappings_CMIP6_Amon.json")
)
def test_cmorise_CMIP6_Amon(model, cmor_name):
    file_pattern = DATA_DIR / "esm1-6/atmosphere/aiihca.pa-101909_mon.nc"
    try:
        cmorise(
            file_paths=file_pattern,
            compound_name="Amon." + cmor_name,
            cmor_dataset_json="model.json",
            mip_table="CMIP6_Amon.json",
        )
    except Exception as e:
        pytest.fail(f"Failed processing {cmor_name} with table CMIP6_Amon.json: {e}")


@pytest.mark.parametrize(
    "cmor_name", load_filtered_variables("Mappings_CMIP6_Lmon.json")
)
def test_cmorise_CMIP6_Lmon(model, cmor_name):
    file_pattern = DATA_DIR / "esm1-6/atmosphere/aiihca.pa-101909_mon.nc"
    try:
        cmorise(
            file_paths=file_pattern,
            compound_name="Lmon." + cmor_name,
            cmor_dataset_json="model.json",
            mip_table="CMIP6_Lmon.json",
        )
    except Exception as e:
        pytest.fail(f"Failed processing {cmor_name} with table CMIP6_Lmon.json: {e}")


@pytest.mark.parametrize(
    "cmor_name", load_filtered_variables("Mappings_CMIP6_Emon.json")
)
def test_cmorise_CMIP6_Emon(model, cmor_name):
    file_pattern = DATA_DIR / "esm1-6/atmosphere/aiihca.pa-101909_mon.nc"
    try:
        cmorise(
            file_paths=file_pattern,
            compound_name="Emon." + cmor_name,
            cmor_dataset_json="model.json",
            mip_table="CMIP6_Emon.json",
        )
    except Exception as e:
        pytest.fail(f"Failed processing {cmor_name} with table CMIP6_Emon.json: {e}")
