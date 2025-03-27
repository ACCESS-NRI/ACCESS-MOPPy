import pytest
import access_mopper as mop
import pandas as pd
import glob
import importlib.resources as resources



@pytest.fixture
def model():
    # Create and save the model
    model_instance = mop.ACCESS_CM2_CMIP6(
        experiment_id="historical",
        realization_index="3",
        initialization_index="1",
        physics_index="1",
        forcing_index="1",
        parent_activity_id="CMIP",
        parent_experiment_id="piControl",
        parent_source_id="ACCESS-CM2",
        parent_variant_label="r1i1p1f1",
        parent_time_units="days since 1850-01-01 00:00:00",
        branch_method="standard",
        branch_time_in_parent=0.0,
        branch_time_in_child=0.0
    )
    model_instance.save_to_file("model.json")
    return model_instance


def load_filtered_variables(mappings):
    # Load and filter variables from the JSON file
    with resources.files("access_mopper.mappings").joinpath(mappings).open() as f:
        df = pd.read_json(f, orient="index")
    #filtered_df = df[df['dimensions'].apply(lambda x: len(x) == 3)]
    #return filtered_df.index.tolist()
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


@pytest.mark.parametrize("cmor_name", load_filtered_variables("Mappings_CMIP6_Amon.json"))
def test_cmorise_CMIP6_Amon(model, cmor_name):
    #file_pattern = "/home/romain/PROJECTS/ACCESS-MOPPeR/Test_data/di787/history/atm/netCDF/di787a.pm*.nc"
    file_pattern = "/home/romain/PROJECTS/ACCESS-MOPPeR/Test_data/esm1-6/atmosphere/aiihca.pa-096110_mon.nc"
    
    try:
        mop.cmorise(
            file_paths=glob.glob(file_pattern),
            compound_name="Amon."+ cmor_name,
            reference_time="1850-01-01 00:00:00",
            cmor_dataset_json="model.json",
            mip_table="CMIP6_Amon.json"
        )
    except Exception as e:
        pytest.fail(f"Failed processing {cmor_name} with table CMIP6_Amon.json: {e}")


@pytest.mark.parametrize("cmor_name", load_filtered_variables("Mappings_CMIP6_Lmon.json"))
def test_cmorise_CMIP6_Lmon(model, cmor_name):
    #file_pattern = "/home/romain/PROJECTS/ACCESS-MOPPeR/Test_data/di787/history/atm/netCDF/di787a.pm*.nc"
    file_pattern = "/home/romain/PROJECTS/ACCESS-MOPPeR/Test_data/esm1-6/atmosphere/aiihca.pa-096110_mon.nc"
    try:
        mop.cmorise(
            file_paths=glob.glob(file_pattern),
            compound_name="Lmon."+ cmor_name,
            reference_time="1850-01-01 00:00:00",
            cmor_dataset_json="model.json",
            mip_table="CMIP6_Lmon.json"
        )
    except Exception as e:
        pytest.fail(f"Failed processing {cmor_name} with table CMIP6_Lmon.json: {e}")