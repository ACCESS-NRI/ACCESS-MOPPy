import yaml
from typing import Dict, Any
import operator
from dataclasses import dataclass, asdict
import json
import cmor
import xarray as xr
import importlib.resources as resources
import os
from .calc_land import extract_tilefrac, calc_landcover, calc_topsoil, average_tile
from .ocean_supergrid import Supergrid

grid_filepath = "/home/romain/PROJECTS/ACCESS-MOPPeR/grids/access-om2/input_20201102/mom_025deg/ocean_hgrid.nc"
ocean_grid = Supergrid(grid_filepath)

# Supported operators
OPERATORS = {
    '+': operator.add,
    '-': operator.sub,
    '*': operator.mul,
    '/': operator.truediv,
    '**': operator.pow
}


def load_yaml(file_path: str) -> Dict[str, Any]:
    with open(file_path, "r") as file:
        return yaml.safe_load(file)
    
@dataclass
class Creator:
    institution: str = None
    organisation: str = None
    contact: str = None
    creator_name: str = None
    creator_email: str = None
    creator_url: str = None
        
@dataclass
class ACCESS_Dataset:

    # General attributes
    Conventions: str = "CF-1.7, ACDD-1.3"
    comment: str = "post-processed using ACCESS-MOPPeR v2, please contact ACCESS-NRI for questions"
    license: str = "https://creativecommons.org/licenses/by/4.0/"
    
    # General information for ACCESS models
    source_id: str = None
    source: str = None
    keywords: str = None
    references: str = None
    forcing: str = None
    calendar: str = None
    grid: str = None
    grid_label: str = None
    nominal_resolution: str = None
    parent: bool = None
    tracking_id_prefix: str = None


    def initialise(self, access_configuration):
        current_dir = os.path.dirname(os.path.abspath(__file__))
        yaml_data = load_yaml(os.path.join(current_dir, "ACCESS_configurations.yml"))
        access_cm2_attributes = yaml_data[access_configuration]

        for key, value in access_cm2_attributes.items():
            if hasattr(self, key):
                setattr(self, key, value) 
    
    # Method to save the instance data to a file
    def save_to_file(self, file_path: str):
        # Convert the dataclass to a dictionary and then to a JSON string
        json_data = json.dumps(self.__dict__, indent=4)

        # Write the JSON string to a file
        with open(file_path, "w") as f:
            f.write(json_data)
        print(f"Data saved to {file_path}")

    def to_json(self):
        return json.dumps(asdict(self), indent=4)

@dataclass
class ACCESS_Experiment(Creator, ACCESS_Dataset):
    title: str = None
    exp_description: str = None
    product_version: str = None
    date_created: str = None
    time_coverage_start: str = None
    time_coverage_end: str = None
    outpath: str = "MOPPeR_outputs"


@dataclass
class CMIP6_Experiment(ACCESS_Experiment):
    Conventions: str = None
    institution_id: str = None
    source_id: str = None
    source_type: str = "AOGCM"
    experiment_id: str = None
    activity_id: str = "CMIP"
    realization_index: str = None
    initialization_index: str = None
    physics_index: str = None
    forcing_index: str = None
    tracking_prefix: str = "hdl:21.14100"
    parent_experiment_id: str = "none"
    parent_activity_id: str = "none"
    parent_source_id: str = "none"
    parent_variant_label: str = "none"
    sub_experiment: str = "none"
    sub_experiment_id: str = "none"
    branch_method: str = "none"
    branch_time_in_child: str = ""
    branch_time_in_parent: str = ""     
    _controlled_vocabulary_file: str = "CMIP6_CV.json"
    _AXIS_ENTRY_FILE: str = "CMIP6_coordinate.json"
    _FORMULA_VAR_FILE: str = "CMIP6_formula_terms.json"
    _cmip6_option: str = "CMIP6"
    mip_era: str = "CMIP6"
    parent_mip_era: str = "CMIP6"
    parent_time_units: str = None
    _history_template: str = "%s ;rewrote data to be consistent with <activity_id> for variable <variable_id> found in table <table_id>."
    output_path_template: str = "<mip_era><activity_id><institution_id><source_id><experiment_id><_member_id><table><variable_id><grid_label><version>"
    output_file_template: str = "<variable_id><table><source_id><experiment_id><_member_id><grid_label>"
    license: str = "CMIP6 model data produced by CSIRO is licensed under a Creative Commons Attribution-ShareAlike 4.0 International License (https://creativecommons.org/licenses/). Consult https://pcmdi.llnl.gov/CMIP6/TermsOfUse for terms of use governing CMIP6 output, including citation requirements and proper acknowledgment.  Further information about this data, including some limitations, can be found via the further_info_url (recorded as a global attribute in this file). The data producers and data providers make no warranty, either express or implied, including, but not limited to, warranties of merchantability and fitness for a particular purpose. All liabilities arising from the supply of the information (including any liability arising in negligence) are excluded to the fullest extent permitted by law."
   
@dataclass
class ACCESS_CM2(ACCESS_Experiment):

    def __post_init__(self):
        ACCESS_Dataset.initialise(self, "ACCESS-CM2")

@dataclass
class ACCESS_CM2_CMIP6(CMIP6_Experiment):
    
    def __post_init__(self):
        ACCESS_Dataset.initialise(self, "ACCESS-CM2")

def get_mapping(compound_name):
    mip_table, cmor_name = compound_name.split(".")
    filename = f"Mappings_CMIP6_{mip_table}.json"
    
    # Use importlib.resources to access the file
    with resources.files("access_mopper.mappings").joinpath(filename).open("r") as file:
        data = json.load(file)
    
    return data[cmor_name]

def cmorise(file_paths, compound_name, reference_time, cmor_dataset_json, mip_table):
    
    mip_name, cmor_name = compound_name.split(".")

    # Open the matching files with xarray
    ds = xr.open_mfdataset(file_paths, combine='by_coords', decode_times=False)

    print(file_paths) 
    # Extract required variables and coordinates
    mapping = get_mapping(compound_name=compound_name)

    if mapping["calculation"]["type"] == "direct":
        access_var = mapping["calculation"]["formula"]
        variable_units = mapping["units"]
        positive = mapping["positive"]
        print(access_var)
        var = ds[access_var]
    else:
        access_vars = {var: ds[var] for var in  mapping["model_variables"]}
        formula = mapping["calculation"]["formula"]
        variable_units = mapping["units"]
        positive = mapping["positive"]
        custom_functions = {"level_to_height": lambda x: x, 
                            "extract_tilefrac": extract_tilefrac, 
                            "calc_landcover": calc_landcover,
                            "calc_topsoil": calc_topsoil,
                            "average_tile": average_tile}
        try:
            context = {**access_vars, **OPERATORS, **custom_functions}
            var = eval(formula, {"__builtins__": None}, context)
        except Exception as e:
            raise ValueError(f"Error evaluating formula '{formula}': {e}")

    dim_mapping = mapping["dimensions"]
    axes = {dim_mapping.get(axis, axis): axis for axis in var.dims}

    data = var.values
    lat_axis = axes.pop("latitude")
    if mip_name == "Omon":
        lat = ocean_grid.lat
        lat_bnds = ocean_grid.lat_bnds
    else:
        lat = ds[lat_axis].values
        lat_bnds = ds[ds[lat_axis].attrs["bounds"]].values
    
    lon_axis = axes.pop("longitude")
    if mip_name == "Omon":
        lon = ocean_grid.lon
        lon_bnds = ocean_grid.lon_bnds
    else:
        lon = ds[lon_axis].values
        lon_bnds = ds[ds[lon_axis].attrs["bounds"]].values
    
    # Convert time to numeric values
    time_axis = axes.pop("time")
    time_numeric = ds[time_axis].values
    time_units = ds[time_axis].attrs["units"]
    time_bnds = ds[ds[time_axis].attrs["bounds"]].values
    # TODO: Check that the calendar is the same than the one defined in the model.json
    # Convert if not.
    calendar = ds[time_axis].attrs["calendar"]

    # CMOR setup
    ipth = opth = "Test"
    cmor.setup(inpath=ipth,
               set_verbosity=cmor.CMOR_NORMAL,
               netcdf_file_action=cmor.CMOR_REPLACE)
    
    cmor.dataset_json(cmor_dataset_json)
    current_dir = os.path.dirname(os.path.abspath(__file__))
    mip_table = os.path.join(current_dir, "cmor_tables", mip_table)
    cmor.load_table(mip_table)

    cmor_axes = [] 
    # Define CMOR axes
    cmorLat = cmor.axis("latitude",
                        coord_vals=lat,
                        cell_bounds=lat_bnds,
                        units="degrees_north")
    cmor_axes.append(cmorLat)
    cmorLon = cmor.axis("longitude",
                        coord_vals=lon,
                        cell_bounds=lon_bnds,
                        units="degrees_east")
    cmor_axes.append(cmorLon)
    cmorTime = cmor.axis("time",
                         coord_vals=time_numeric,
                         cell_bounds=time_bnds,
                         units=time_units)
    cmor_axes.append(cmorTime)

    if axes:
        for axis, dim in axes.items():
            coord_vals = var[dim].values
            try:
                cell_bounds = var[var[dim].attrs["bounds"]].values
            except KeyError:
                cell_bounds = None
            axis_units = var[dim].attrs["units"]
            cmor_axis = cmor.axis(axis,
                                  coord_vals=coord_vals,
                                  cell_bounds=cell_bounds,
                                  units=axis_units)
            cmor_axes.append(cmor_axis)

    # Define CMOR variable
    cmorVar = cmor.variable(cmor_name, variable_units, cmor_axes, positive=positive)
    
    # Write data to CMOR
    cmor.write(cmorVar, data, ntimes_passed=len(time_numeric))
    
    # Finalize and save the file
    filename = cmor.close(cmorVar, file_name=True)
    print("Stored in:", filename)
    
    cmor.close()
