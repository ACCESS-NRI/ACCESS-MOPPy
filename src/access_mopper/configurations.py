import operator
from dataclasses import dataclass
import json
import cmor
import xarray as xr
import importlib.resources as resources
import os
from .calc_land import extract_tilefrac, calc_landcover, calc_topsoil, average_tile
from .dataclasses import CMIP6_Experiment


# Supported operators
OPERATORS = {
    '+': operator.add,
    '-': operator.sub,
    '*': operator.mul,
    '/': operator.truediv,
    '**': operator.pow
}

@dataclass
class ACCESS_ESM16_CMIP6(CMIP6_Experiment):
    
    def __post_init__(self):
        self.initialise("ACCESS-ESM1-5")


def get_mapping(compound_name):
    mip_table, cmor_name = compound_name.split(".")
    filename = f"Mappings_CMIP6_{mip_table}.json"
    
    # Use importlib.resources to access the file
    with resources.files("access_mopper.mappings").joinpath(filename).open("r") as file:
        data = json.load(file)
    
    return data[cmor_name]

def cmorise(file_paths, compound_name, cmor_dataset_json, mip_table):
    
    cmor_name = compound_name.split(".")[1]

    # Open the matching files with xarray
    ds = xr.open_mfdataset(file_paths, combine='by_coords', decode_times=False)

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
    lat = ds[lat_axis].values
    lat_bnds = ds[ds[lat_axis].attrs["bounds"]].values
    lon_axis = axes.pop("longitude")
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
