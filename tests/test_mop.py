import glob
import importlib.resources as resources
import os
from pathlib import Path

import iris
import pandas as pd
import pytest
from esmvalcore.preprocessor import cmor_check_data, cmor_check_metadata

from access_mopper.configurations import ACCESS_ESM16_CMIP6, ACCESS_OM3_CMIP6

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


@pytest.fixture
def model_om3():
    # Create and save the om3 model
    model_instance = ACCESS_OM3_CMIP6(
        experiment_id="historical",
        realization_index="1",
        initialization_index="1",
        physics_index="1",
        forcing_index="1",
        parent_mip_era="CMIP6",
        parent_activity_id="CMIP",
        parent_experiment_id="piControl",
        parent_source_id="ACCESS-ESM1-5",
        parent_variant_label="r1i1p1f1",
        parent_time_units="days since 0101-1-1",
        branch_method="standard",
        branch_time_in_child=0.0,
        branch_time_in_parent=0.0,
    )
    model_instance.save_to_file("model_om3.json")
    return model_instance


def test_model_function():
    test_file = DATA_DIR / "esm1-6/atmosphere/aiihca.pa-101909_mon.nc"
    assert test_file.exists(), "Test data file missing!"


def load_filtered_variables(mappings):
    # Load and filter variables from the JSON file
    with resources.files("access_mopper.mappings").joinpath(mappings).open() as f:
        df = pd.read_json(f, orient="index")
    return df.index.tolist()


def load_filtered_variables_om3_2d(mappings):
    # Load and filter variables from the JSON file
    with resources.files("access_mopper.mappings").joinpath(mappings).open() as f:
        df = pd.read_json(f, orient="index")
        list_2d = [var for var in df.index if len(df.loc[var, "dimensions"]) == 3]
    return list_2d


def load_filtered_variables_om3_3d(mappings):
    # Load and filter variables from the JSON file
    with resources.files("access_mopper.mappings").joinpath(mappings).open() as f:
        df = pd.read_json(f, orient="index")
        list_3d = [var for var in df.index if len(df.loc[var, "dimensions"]) == 4]
    return list_3d


def esmvaltool_cmor_check(cube, cmor_name, mip, cmor_table="CMIP6", check_level=5):
    # Use ESMValTool’s built-in functions cmor_check_metadata and cmor_check_data to assist with testing.
    cmor_check_metadata(
        cube, cmor_table, mip, short_name=cmor_name, check_level=check_level
    )
    cmor_check_data(
        cube, cmor_table, mip, short_name=cmor_name, check_level=check_level
    )


@pytest.mark.parametrize(
    "cmor_name", load_filtered_variables("Mappings_CMIP6_Amon.json")
)
def test_cmorise_CMIP6_Amon(model, cmor_name):
    file_pattern = DATA_DIR / "esm1-6/atmosphere/aiihca.pa-101909_mon.nc"
    try:
        model.cmorise(
            file_paths=file_pattern,
            compound_name="Amon." + cmor_name,
            cmor_dataset_json="model.json",
            mip_table="CMIP6_Amon.json",
        )
        test_cube = iris.load(model.filename)[0]
        esmvaltool_cmor_check(test_cube, cmor_name, mip="Amon")
    except Exception as e:
        pytest.fail(f"Failed processing {cmor_name} with table CMIP6_Amon.json: {e}")


@pytest.mark.parametrize(
    "cmor_name", load_filtered_variables("Mappings_CMIP6_Lmon.json")
)
def test_cmorise_CMIP6_Lmon(model, cmor_name):
    file_pattern = DATA_DIR / "esm1-6/atmosphere/aiihca.pa-101909_mon.nc"
    try:
        model.cmorise(
            file_paths=file_pattern,
            compound_name="Lmon." + cmor_name,
            cmor_dataset_json="model.json",
            mip_table="CMIP6_Lmon.json",
        )
        test_cube = iris.load(model.filename)[0]
        esmvaltool_cmor_check(test_cube, cmor_name, mip="Lmon")
    except Exception as e:
        pytest.fail(f"Failed processing {cmor_name} with table CMIP6_Lmon.json: {e}")


@pytest.mark.parametrize(
    "cmor_name", load_filtered_variables("Mappings_CMIP6_Emon.json")
)
def test_cmorise_CMIP6_Emon(model, cmor_name):
    file_pattern = DATA_DIR / "esm1-6/atmosphere/aiihca.pa-101909_mon.nc"
    try:
        model.cmorise(
            file_paths=file_pattern,
            compound_name="Emon." + cmor_name,
            cmor_dataset_json="model.json",
            mip_table="CMIP6_Emon.json",
        )
        test_cube = iris.load(model.filename)[0]
        esmvaltool_cmor_check(test_cube, cmor_name, mip="Emon")
    except Exception as e:
        pytest.fail(f"Failed processing {cmor_name} with table CMIP6_Emon.json: {e}")


@pytest.mark.parametrize(
    "cmor_name", load_filtered_variables_om3_2d("Mappings_OM3_Omon.json")
)
def test_cmorise_OM3_2d(model_om3, cmor_name):
    file_pattern = (
        DATA_DIR / f"om3/2d/access-om3.mom6.2d.{cmor_name}.1mon.mean.1902_01.nc"
    )
    try:
        model_om3.supergrid = glob.glob(str(DATA_DIR / "om3/supergrid/*.nc"))
        model_om3.cmorise(
            file_paths=file_pattern,
            compound_name="Omon." + cmor_name,
            cmor_dataset_json="model_om3.json",
            mip_table="CMIP6_Omon.json",
        )
        test_cube = iris.load(model_om3.filename)[0]
        esmvaltool_cmor_check(test_cube, cmor_name, mip="Omon")
    except Exception as e:
        pytest.fail(f"Failed processing {cmor_name} with table CMIP6_Omon.json: {e}")


@pytest.mark.parametrize(
    "cmor_name", load_filtered_variables_om3_3d("Mappings_OM3_Omon.json")
)
def test_cmorise_OM3_3d(model_om3, cmor_name):
    print("Working directory:", os.getcwd())
    file_pattern = (
        DATA_DIR / f"om3/3d/access-om3.mom6.3d.{cmor_name}.1mon.mean.1924_01.nc"
    )
    try:
        model_om3.supergrid = glob.glob(str(DATA_DIR / "om3/supergrid/*.nc"))
        model_om3.cmorise(
            file_paths=file_pattern,
            compound_name="Omon." + cmor_name,
            cmor_dataset_json="model_om3.json",
            mip_table="CMIP6_Omon.json",
        )
        test_cube = iris.load(model_om3.filename)[0]
        esmvaltool_cmor_check(test_cube, cmor_name, mip="Omon")
    except Exception as e:
        pytest.fail(f"Failed processing {cmor_name} with table CMIP6_Omon.json: {e}")
