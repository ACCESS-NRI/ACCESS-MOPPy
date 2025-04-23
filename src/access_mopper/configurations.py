import operator
import os
from dataclasses import dataclass

import cmor
import numpy as np
import xarray as xr

from .calc_atmos import level_to_height
from .calc_land import average_tile, calc_landcover, calc_topsoil, extract_tilefrac
from .dataclasses import CMIP6_Experiment
from .ocean_supergrid import om2_grid, om3_grid

# Supported operators
OPERATORS = {
    "+": operator.add,
    "-": operator.sub,
    "*": operator.mul,
    "/": operator.truediv,
    "**": operator.pow,
}


@dataclass
class ACCESS_ESM16_CMIP6(CMIP6_Experiment):
    mapping_file_prefix: str = "Mappings_CMIP6_"
    supergrid = om2_grid

    def __post_init__(self):
        self.initialise("ACCESS-ESM1-5")

    def cmorise(self, file_paths, compound_name, cmor_dataset_json, mip_table):
        cmor_name = compound_name.split(".")[1]

        # Open the matching files with xarray
        ds = xr.open_mfdataset(file_paths, combine="by_coords", decode_times=False)

        # Extract required variables and coordinates
        mapping = self.get_mapping(compound_name=compound_name)

        if mapping["calculation"]["type"] == "direct":
            access_var = mapping["calculation"]["formula"]
            variable_units = mapping["units"]
            positive = mapping["positive"]
            var = ds[access_var]
        else:
            access_vars = {var: ds[var] for var in mapping["model_variables"]}
            formula = mapping["calculation"]["formula"]
            variable_units = mapping["units"]
            positive = mapping["positive"]
            custom_functions = {
                "level_to_height": level_to_height,
                "extract_tilefrac": extract_tilefrac,
                "calc_landcover": calc_landcover,
                "calc_topsoil": calc_topsoil,
                "average_tile": average_tile,
            }
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
        # calendar = ds[time_axis].attrs["calendar"]

        # CMOR setup
        ipth = "Test"
        cmor.setup(
            inpath=ipth,
            set_verbosity=cmor.CMOR_NORMAL,
            netcdf_file_action=cmor.CMOR_REPLACE,
        )

        cmor.dataset_json(cmor_dataset_json)
        current_dir = os.path.dirname(os.path.abspath(__file__))
        mip_table = os.path.join(current_dir, "cmor_tables", mip_table)
        cmor.load_table(mip_table)

        cmor_axes = []
        # Define CMOR axes
        cmorLat = cmor.axis(
            "latitude", coord_vals=lat, cell_bounds=lat_bnds, units="degrees_north"
        )
        cmor_axes.append(cmorLat)
        cmorLon = cmor.axis(
            "longitude", coord_vals=lon, cell_bounds=lon_bnds, units="degrees_east"
        )
        cmor_axes.append(cmorLon)
        cmorTime = cmor.axis(
            "time", coord_vals=time_numeric, cell_bounds=time_bnds, units=time_units
        )
        cmor_axes.append(cmorTime)

        if axes:
            for axis, dim in axes.items():
                coord_vals = var[dim].values
                try:
                    cell_bounds = var[var[dim].attrs["bounds"]].values
                except KeyError:
                    cell_bounds = None
                axis_units = var[dim].attrs["units"]
                cmor_axis = cmor.axis(
                    axis,
                    coord_vals=coord_vals,
                    cell_bounds=cell_bounds,
                    units=axis_units,
                )
                cmor_axes.append(cmor_axis)

        # Define CMOR variable
        cmorVar = cmor.variable(cmor_name, variable_units, cmor_axes, positive=positive)

        # Write data to CMOR
        cmor.write(cmorVar, data, ntimes_passed=len(time_numeric))

        # Finalize and save the file
        filename = cmor.close(cmorVar, file_name=True)
        print("Stored in:", filename)

        cmor.close()

    def cmorise_ocean(self, file_paths, compound_name, cmor_dataset_json, mip_table):
        _, cmor_name = compound_name.split(".")

        # Open the matching files with xarray
        ds = xr.open_mfdataset(file_paths, combine="by_coords", decode_times=False)

        # Extract required variables and coordinates
        mapping = self.get_mapping(compound_name=compound_name)

        if mapping["calculation"]["type"] == "direct":
            access_var = mapping["calculation"]["formula"]
            variable_units = mapping["units"]
            positive = mapping["positive"]
            var = ds[access_var]
        else:
            raise ValueError("Not implemented")

        dim_mapping = mapping["dimensions"]
        _ = (
            self.supergrid.h_cells()
            if "xt_ocean" in var.dims
            else self.supergrid.q_cells()
        )
        axes = {dim_mapping.get(axis, axis): axis for axis in var.dims}

        i_axis = axes.pop("longitude")
        i_axis = ds[i_axis].values
        j_axis = axes.pop("latitude")
        j_axis = ds[j_axis].values
        x = np.arange(i_axis.size, dtype="float")
        x_bnds = np.array([[x_ - 0.5, x_ + 0.5] for x_ in x])
        y = np.arange(j_axis.size, dtype="float")
        y_bnds = np.array([[y_ - 0.5, y_ + 0.5] for y_ in y])

        data = var.values
        lat = self.supergrid.lat
        lat_bnds = self.supergrid.lat_bnds

        lon = self.supergrid.lon
        lon_bnds = self.supergrid.lon_bnds

        # Convert time to numeric values
        time_axis = axes.pop("time")
        time_numeric = ds[time_axis].values
        time_units = ds[time_axis].attrs["units"]
        time_bnds = ds[ds[time_axis].attrs["bounds"]].values
        # TODO: Check that the calendar is the same than the one defined in the model.json
        # Convert if not.
        # calendar = ds[time_axis].attrs["calendar"]

        # CMOR setup
        ipth = "Test"
        cmor.setup(
            inpath=ipth,
            set_verbosity=cmor.CMOR_NORMAL,
            netcdf_file_action=cmor.CMOR_REPLACE,
        )

        cmor.dataset_json(cmor_dataset_json)
        current_dir = os.path.dirname(os.path.abspath(__file__))
        grid_table_id = cmor.load_table(
            os.path.join(current_dir, "cmor_tables", "CMIP6_grids.json")
        )
        cmor.set_table(grid_table_id)

        cmor_axes = []
        # Define CMOR axes
        yaxis_id = cmor.axis(
            table_entry="j_index", units="1", coord_vals=y, cell_bounds=y_bnds
        )
        xaxis_id = cmor.axis(
            table_entry="i_index", units="1", coord_vals=x, cell_bounds=x_bnds
        )

        grid_id = cmor.grid(
            axis_ids=np.array([yaxis_id, xaxis_id]),
            latitude=lat,
            longitude=lon,
            latitude_vertices=lat_bnds,
            longitude_vertices=lon_bnds,
        )
        cmor_axes.append(grid_id)

        # Now, load the Omon table to set up the time axis and variable
        current_dir = os.path.dirname(os.path.abspath(__file__))
        mip_table = os.path.join(current_dir, "cmor_tables", mip_table)
        omon_table_id = cmor.load_table(mip_table)
        cmor.set_table(omon_table_id)

        cmorTime = cmor.axis(
            "time", coord_vals=time_numeric, cell_bounds=time_bnds, units=time_units
        )
        cmor_axes.append(cmorTime)

        if axes:
            for axis, dim in axes.items():
                coord_vals = var[dim].values
                try:
                    cell_bounds = var[var[dim].attrs["bounds"]].values
                except KeyError:
                    cell_bounds = None
                axis_units = var[dim].attrs["units"]
                cmor_axis = cmor.axis(
                    axis,
                    coord_vals=coord_vals,
                    cell_bounds=cell_bounds,
                    units=axis_units,
                )
                cmor_axes.append(cmor_axis)

        # Define CMOR variable
        cmorVar = cmor.variable(cmor_name, variable_units, cmor_axes, positive=positive)

        # Write data to CMOR
        data = np.moveaxis(data, 0, -1)
        cmor.write(cmorVar, data, ntimes_passed=len(time_numeric))

        # Finalize and save the file
        filename = cmor.close(cmorVar, file_name=True)
        print("Stored in:", filename)

        cmor.close()


@dataclass
class ACCESS_OM3_CMIP6(CMIP6_Experiment):
    mapping_file_prefix: str = "Mappings_OM3_"
    supergrid = om3_grid

    def __post_init__(self):
        self.initialise("ACCESS-OM3")

    def cmorise(self, file_paths, compound_name, cmor_dataset_json, mip_table):
        _, cmor_name = compound_name.split(".")

        # Open the matching files with xarray
        ds = xr.open_mfdataset(file_paths, combine="by_coords", decode_times=False)

        # Extract required variables and coordinates
        mapping = self.get_mapping(compound_name=compound_name)

        if mapping["calculation"]["type"] == "direct":
            access_var = mapping["calculation"]["formula"]
            variable_units = mapping["units"]
            positive = mapping["positive"]
            var = ds[access_var]
        else:
            raise ValueError("Not implemented")

        dim_mapping = mapping["dimensions"]
        _ = self.supergrid.h_cells() if "xh" in var.dims else self.supergrid.q_cells()
        axes = {dim_mapping.get(axis, axis): axis for axis in var.dims}

        i_axis = axes.pop("longitude")
        i_axis = ds[i_axis].values
        j_axis = axes.pop("latitude")
        j_axis = ds[j_axis].values
        x = np.arange(i_axis.size, dtype="float")
        x_bnds = np.array([[x_ - 0.5, x_ + 0.5] for x_ in x])
        y = np.arange(j_axis.size, dtype="float")
        y_bnds = np.array([[y_ - 0.5, y_ + 0.5] for y_ in y])

        data = var.values
        lat = self.supergrid.lat
        lat_bnds = self.supergrid.lat_bnds

        lon = self.supergrid.lon
        lon_bnds = self.supergrid.lon_bnds

        # Convert time to numeric values
        time_axis = axes.pop("time")
        time_numeric = ds[time_axis].values
        time_units = ds[time_axis].attrs["units"]
        time_bnds = ds[ds[time_axis].attrs["bounds"]].values
        # TODO: Check that the calendar is the same than the one defined in the model.json
        # Convert if not.
        # calendar = ds[time_axis].attrs["calendar"]

        # CMOR setup
        ipth = "Test"
        cmor.setup(
            inpath=ipth,
            set_verbosity=cmor.CMOR_NORMAL,
            netcdf_file_action=cmor.CMOR_REPLACE,
        )

        cmor.dataset_json(cmor_dataset_json)
        current_dir = os.path.dirname(os.path.abspath(__file__))
        grid_table_id = cmor.load_table(
            os.path.join(current_dir, "cmor_tables", "CMIP6_grids.json")
        )
        cmor.set_table(grid_table_id)

        cmor_axes = []
        # Define CMOR axes
        yaxis_id = cmor.axis(
            table_entry="j_index", units="1", coord_vals=y, cell_bounds=y_bnds
        )
        xaxis_id = cmor.axis(
            table_entry="i_index", units="1", coord_vals=x, cell_bounds=x_bnds
        )

        grid_id = cmor.grid(
            axis_ids=np.array([yaxis_id, xaxis_id]),
            latitude=lat,
            longitude=lon,
            latitude_vertices=lat_bnds,
            longitude_vertices=lon_bnds,
        )
        cmor_axes.append(grid_id)

        # Now, load the Omon table to set up the time axis and variable
        current_dir = os.path.dirname(os.path.abspath(__file__))
        mip_table = os.path.join(current_dir, "cmor_tables", mip_table)
        omon_table_id = cmor.load_table(mip_table)
        cmor.set_table(omon_table_id)

        cmorTime = cmor.axis(
            "time", coord_vals=time_numeric, cell_bounds=time_bnds, units=time_units
        )
        cmor_axes.append(cmorTime)

        if axes:
            for axis, dim in axes.items():
                coord_vals = var[dim].values
                try:
                    cell_bounds = var[var[dim].attrs["bounds"]].values
                except KeyError:
                    cell_bounds = None
                axis_units = var[dim].attrs["units"]
                cmor_axis = cmor.axis(
                    axis,
                    coord_vals=coord_vals,
                    cell_bounds=cell_bounds,
                    units=axis_units,
                )
                cmor_axes.append(cmor_axis)

        # Define CMOR variable
        cmorVar = cmor.variable(cmor_name, variable_units, cmor_axes, positive=positive)

        # Write data to CMOR
        data = np.moveaxis(data, 0, -1)
        cmor.write(cmorVar, data, ntimes_passed=len(time_numeric))

        # Finalize and save the file
        filename = cmor.close(cmorVar, file_name=True)
        print("Stored in:", filename)

        cmor.close()
