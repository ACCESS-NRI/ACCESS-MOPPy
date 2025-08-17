from pathlib import Path
from typing import Any, Dict, List, Optional, Union

import numpy as np

from access_mopper.base import CMIP6_CMORiser
from access_mopper.derivations import custom_functions, evaluate_expression
from access_mopper.ocean_supergrid import Supergrid
from access_mopper.vocabulary_processors import CMIP6Vocabulary


class CMIP6_Ocean_CMORiser(CMIP6_CMORiser):
    """
    CMORiser subclass for ocean variables using curvilinear supergrid coordinates.
    """

    def __init__(
        self,
        input_paths: Union[str, List[str]],
        output_path: str,
        cmor_name: str,
        cmip6_vocab: CMIP6Vocabulary,
        variable_mapping: Dict[str, Any],
        drs_root: Optional[Path] = None,
    ):
        super().__init__(
            input_paths=input_paths,
            output_path=output_path,
            cmor_name=cmor_name,
            cmip6_vocab=cmip6_vocab,
            variable_mapping=variable_mapping,
            drs_root=drs_root,
        )

        nominal_resolution = cmip6_vocab._get_nominal_resolution()
        self.supergrid = Supergrid(nominal_resolution)
        self.grid_info = None
        self.grid_type = None

    def infer_grid_type(self):
        coord_sets = {
            "T": {"xt_ocean", "yt_ocean"},
            "U": {"xu_ocean", "yu_ocean"},
            "V": {"xv_ocean", "yv_ocean"},
            "Q": {"xq_ocean", "yq_ocean"},
        }
        present_coords = set(self.ds.coords)
        for grid, required in coord_sets.items():
            if required.issubset(present_coords):
                return grid
        raise ValueError("Could not infer grid type from dataset coordinates.")

    def select_and_process_variables(self):
        input_vars = self.mapping[self.cmor_name]["model_variables"]
        calc = self.mapping[self.cmor_name]["calculation"]

        required_vars = set(input_vars)
        self.load_dataset(required_vars=required_vars)
        self.sort_time_dimension()

        if calc["type"] == "direct":
            self.ds[self.cmor_name] = self.ds[input_vars[0]]
        elif calc["type"] == "formula":
            context = {var: self.ds[var] for var in input_vars}
            context.update(custom_functions)
            self.ds[self.cmor_name] = evaluate_expression(calc, context)
        else:
            raise ValueError(f"Unsupported calculation type: {calc['type']}")

        dim_rename = {
            "xt_ocean": "i",
            "yt_ocean": "j",
            "xu_ocean": "i",
            "yu_ocean": "j",
            "xq_ocean": "i",
            "yq_ocean": "j",
            "xv_ocean": "i",
            "yv_ocean": "j",
        }
        dims_to_rename = {
            k: v for k, v in dim_rename.items() if k in self.ds[self.cmor_name].dims
        }
        self.ds[self.cmor_name] = self.ds[self.cmor_name].rename(dims_to_rename)
        self.ds[self.cmor_name] = self.ds[self.cmor_name].transpose("time", "j", "i")

        self.grid_type = self.infer_grid_type()
        # Drop all other data variables except the CMOR variable
        self.ds = self.ds[[self.cmor_name]]

        # Drop unused coordinates
        used_coords = set()
        for dim in self.ds[self.cmor_name].dims:
            if dim in self.ds.coords:
                used_coords.add(dim)
            else:
                # Might be implicit dimension (e.g. from formula), check all coords
                for coord in self.ds.coords:
                    if dim in self.ds[coord].dims:
                        used_coords.add(coord)
        self.ds = self.ds.drop_vars([c for c in self.ds.coords if c not in used_coords])

    def update_attributes(self):
        grid_type = self.grid_type
        self.grid_info = self.supergrid.extract_grid(grid_type)

        self.ds = self.ds.assign_coords(
            {
                "i": self.grid_info["i"],
                "j": self.grid_info["j"],
                "vertices": self.grid_info["vertices"],
            }
        )

        self.ds["latitude"] = self.grid_info["latitude"]
        self.ds["longitude"] = self.grid_info["longitude"]
        self.ds["vertices_latitude"] = self.grid_info["vertices_latitude"]
        self.ds["vertices_longitude"] = self.grid_info["vertices_longitude"]

        self.ds["latitude"].attrs.update(
            {
                "standard_name": "latitude",
                "units": "degrees_north",
                "bounds": "vertices_latitude",
            }
        )
        self.ds["longitude"].attrs.update(
            {
                "standard_name": "longitude",
                "units": "degrees_east",
                "bounds": "vertices_longitude",
            }
        )
        self.ds["vertices_latitude"].attrs.update(
            {"standard_name": "latitude", "units": "degrees_north"}
        )
        self.ds["vertices_longitude"].attrs.update(
            {"standard_name": "longitude", "units": "degrees_east"}
        )

        self.ds.attrs = {
            k: v
            for k, v in self.vocab.get_required_global_attributes().items()
            if v not in (None, "")
        }

        if "nv" in self.ds.dims:
            self.ds = self.ds.rename_dims({"nv": "bnds"}).rename_vars({"nv": "bnds"})
            self.ds["bnds"].attrs.update(
                {"long_name": "vertex number of the bounds", "units": "1"}
            )

        cmor_attrs = self.vocab.variable
        self.ds[self.cmor_name].attrs.update(
            {k: v for k, v in cmor_attrs.items() if v not in (None, "")}
        )
        var_type = cmor_attrs.get("type", "double")
        self.ds[self.cmor_name] = self.ds[self.cmor_name].astype(
            self.type_mapping.get(var_type, np.float64)
        )

        # Check calendar and units
        self._check_calendar("time")
