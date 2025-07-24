from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Union

import netCDF4 as nc
import numpy as np
import xarray as xr
from cftime import num2date

from access_mopper.derivations import custom_functions, evaluate_expression
from access_mopper.ocean_supergrid import Supergrid
from access_mopper.utilities import load_cmip6_mappings, type_mapping
from access_mopper.vocabulary_processors import CMIP6Vocabulary


class CMIP6_CMORiser:
    """
    Base class for CMIP6 CMORisers, providing shared logic for CMORisation.
    """

    type_mapping = type_mapping

    def __init__(
        self,
        input_paths: Union[str, List[str]],
        output_path: str,
        cmor_name: str,
        cmip6_vocab: Any,
        variable_mapping: Dict[str, Any],
        drs_root: Optional[Path] = None,
    ):
        self.input_paths = (
            input_paths if isinstance(input_paths, list) else [input_paths]
        )
        self.output_path = output_path
        self.cmor_name = cmor_name
        self.vocab = cmip6_vocab
        self.mapping = variable_mapping
        self.drs_root = Path(drs_root) if drs_root is not None else None
        self.version_date = datetime.now().strftime("%Y%m%d")
        self.ds = None

    def __getitem__(self, key):
        return self.ds[key]

    def __getattr__(self, attr):
        # This is only called if the attr is not found on CMORiser itself
        return getattr(self.ds, attr)

    def __setitem__(self, key, value):
        self.ds[key] = value

    def __repr__(self):
        return repr(self.ds)

    def load_dataset(self):
        self.ds = xr.open_mfdataset(
            self.input_paths, combine="by_coords", engine="netcdf4", decode_cf=False
        )

    def select_and_process_variables(self):
        raise NotImplementedError(
            "Subclasses must implement select_and_process_variables."
        )

    def _check_units(self, var: str, expected: str) -> bool:
        actual = self.ds[var].attrs.get("units")
        if "days since ?" in expected:
            return actual and actual.lower().startswith("days since")
        if actual and expected and actual != expected:
            raise ValueError(f"Mismatch units for {var}: {actual} != {expected}")
        return True

    def _check_calendar(self, var: str):
        calendar = self.ds[var].attrs.get("calendar")
        units = self.ds[var].attrs.get("units")
        if not calendar or not units:
            return
        try:
            dates = xr.cftime_range(
                start=units.split("since")[1].strip(), periods=3, calendar=calendar
            )
        except Exception as e:
            raise ValueError(f"Failed calendar check for {var}: {e}")
        if calendar in ("noleap", "365_day"):
            for d in dates:
                if d.month == 2 and d.day == 29:
                    raise ValueError(f"{calendar} must not have 29 Feb: found {d}")
        elif calendar == "360_day":
            for d in dates:
                if d.day > 30:
                    raise ValueError(f"360_day calendar has day > 30: {d}")

    def _check_range(self, var: str, vmin: float, vmax: float):
        arr = self.ds[var]
        if hasattr(arr.data, "map_blocks"):
            too_small = (arr < vmin).any().compute()
            too_large = (arr > vmax).any().compute()
        else:
            too_small = (arr < vmin).any().item()
            too_large = (arr > vmax).any().item()
        if too_small:
            raise ValueError(f"Values of '{var}' below valid_min: {vmin}")
        if too_large:
            raise ValueError(f"Values of '{var}' above valid_max: {vmax}")

    def drop_intermediates(self):
        for var in self.mapping[self.cmor_name]["model_variables"]:
            if var in self.ds.data_vars and var != self.cmor_name:
                self.ds = self.ds.drop_vars(var)

    def update_attributes(self):
        raise NotImplementedError("Subclasses must implement update_attributes.")

    def reorder(self):
        def ordered(ds, core=("lat", "lon", "time", "height")):
            seen = set()
            order = []
            for name in core:
                if name in ds.variables:
                    order.append(name)
                    seen.add(name)
                bnds = f"{name}_bnds"
                if bnds in ds.variables:
                    order.append(bnds)
                    seen.add(bnds)
            for v in ds.variables:
                if v not in seen:
                    order.append(v)
            return ds[order]

        self.ds = ordered(self.ds)

    def _build_drs_path(self, attrs: Dict[str, str]) -> Path:
        drs_components = [
            attrs.get("mip_era", "CMIP6"),
            attrs["activity_id"],
            attrs["institution_id"],
            attrs["source_id"],
            attrs["experiment_id"],
            attrs["variant_label"],
            attrs["table_id"],
            attrs["variable_id"],
            attrs["grid_label"],
            f"v{self.version_date}",
        ]
        return self.drs_root.joinpath(*drs_components)

    def _update_latest_symlink(self, versioned_path: Path):
        latest_link = versioned_path.parent / "latest"
        try:
            if latest_link.is_symlink() or latest_link.exists():
                latest_link.unlink()
            latest_link.symlink_to(versioned_path.name, target_is_directory=True)
        except Exception as e:
            print(f"Warning: Failed to update latest symlink at {latest_link}: {e}")

    def write(self):
        attrs = self.ds.attrs
        required_keys = [
            "variable_id",
            "table_id",
            "source_id",
            "experiment_id",
            "variant_label",
            "grid_label",
        ]
        missing = [k for k in required_keys if k not in attrs]
        if missing:
            raise ValueError(
                f"Missing required CMIP6 global attributes for filename: {missing}"
            )

        time_var = self.ds[self.cmor_name].coords["time"]
        units = time_var.attrs["units"]
        calendar = time_var.attrs.get("calendar", "standard").lower()
        times = num2date(time_var.values[[0, -1]], units=units, calendar=calendar)
        start, end = [f"{t.year:04d}{t.month:02d}" for t in times]
        time_range = f"{start}-{end}"

        filename = (
            f"{attrs['variable_id']}_{attrs['table_id']}_{attrs['source_id']}_"
            f"{attrs['experiment_id']}_{attrs['variant_label']}_"
            f"{attrs['grid_label']}_{time_range}.nc"
        )

        if self.drs_root:
            drs_path = self._build_drs_path(attrs)
            drs_path.mkdir(parents=True, exist_ok=True)
            path = drs_path / filename
            self._update_latest_symlink(drs_path)
        else:
            path = Path(self.output_path) / filename
            path.parent.mkdir(parents=True, exist_ok=True)

        with nc.Dataset(path, "w", format="NETCDF4") as dst:
            for k, v in attrs.items():
                dst.setncattr(k, v)
            for dim, size in self.ds.sizes.items():
                if dim == "time":
                    dst.createDimension(dim, None)  # Unlimited dimension
                else:
                    dst.createDimension(dim, size)
            for var in self.ds.variables:
                vdat = self.ds[var]
                fill = None if var.endswith("_bnds") else vdat.attrs.get("_FillValue")
                v = (
                    dst.createVariable(var, str(vdat.dtype), vdat.dims, fill_value=fill)
                    if fill
                    else dst.createVariable(var, str(vdat.dtype), vdat.dims)
                )
                if not var.endswith("_bnds"):
                    for a, val in vdat.attrs.items():
                        if a != "_FillValue":
                            v.setncattr(a, val)
                v[:] = vdat.values

        print(f"CMORised output written to {path}")

    def run(self):
        self.load_dataset()
        self.select_and_process_variables()
        self.drop_intermediates()
        self.update_attributes()
        self.reorder()
        self.write()


class CMIP6_Atmosphere_CMORiser(CMIP6_CMORiser):
    """
    Handles CMORisation of NetCDF datasets using CMIP6 metadata (Atmosphere/Land).
    """

    def select_and_process_variables(self):
        bnds_required = [
            v["out_name"] + "_bnds"
            for v in self.vocab.axes.values()
            if v.get("must_have_bounds") == "yes"
        ]

        input_vars = self.mapping[self.cmor_name]["model_variables"]
        calc = self.mapping[self.cmor_name]["calculation"]
        self.ds = self.ds[input_vars + bnds_required]

        if calc["type"] == "direct":
            self.ds = self.ds.rename({input_vars[0]: self.cmor_name})
        elif calc["type"] == "formula":
            context = {var: self.ds[var] for var in input_vars}
            context.update(custom_functions)
            self.ds[self.cmor_name] = evaluate_expression(calc, context)
        else:
            raise ValueError(f"Unsupported calculation type: {calc['type']}")

        cmor_dims = self.vocab.variable["dimensions"].split()
        transpose_order = [
            self.vocab.axes[dim]["out_name"]
            for dim in cmor_dims
            if "value" not in self.vocab.axes[dim]
        ]
        self.ds[self.cmor_name] = self.ds[self.cmor_name].transpose(*transpose_order)

    def update_attributes(self):
        self.ds.attrs = {
            k: v
            for k, v in self.vocab.get_required_global_attributes().items()
            if v not in (None, "")
        }

        self.ds = self.ds.rename(self.mapping[self.cmor_name]["dimensions"])

        required_coords = {
            v["out_name"] for v in self.vocab.axes.values() if "value" in v
        }.union({v["out_name"] for v in self.vocab.axes.values()})
        self.ds = self.ds.drop_vars(
            [c for c in self.ds.coords if c not in required_coords], errors="ignore"
        )

        cmor_attrs = self.vocab.variable
        self._check_units(self.cmor_name, cmor_attrs.get("units"))

        self.ds[self.cmor_name].attrs.update(
            {k: v for k, v in cmor_attrs.items() if v not in (None, "")}
        )
        var_type = cmor_attrs.get("type", "double")
        self.ds[self.cmor_name] = self.ds[self.cmor_name].astype(
            self.type_mapping.get(var_type, np.float64)
        )

        try:
            if cmor_attrs.get("valid_min") not in (None, "") and cmor_attrs.get(
                "valid_max"
            ) not in (None, ""):
                vmin = self.type_mapping.get(var_type, np.float64)(
                    cmor_attrs["valid_min"]
                )
                vmax = self.type_mapping.get(var_type, np.float64)(
                    cmor_attrs["valid_max"]
                )
                self._check_range(self.cmor_name, vmin, vmax)
        except ValueError as e:
            raise ValueError(
                f"Failed to validate value range for {self.cmor_name}: {e}"
            )

        for dim, meta in self.vocab.axes.items():
            name = meta["out_name"]
            dtype = self.type_mapping.get(meta.get("type", "double"), np.float64)
            if name in self.ds:
                self._check_units(name, meta.get("units", ""))
                if meta.get("standard_name") == "time":
                    self._check_calendar(name)
                original_units = self.ds[name].attrs.get("units", "")
                coord_attrs = {
                    k: v
                    for k, v in {
                        "standard_name": meta.get("standard_name"),
                        "long_name": meta.get("long_name"),
                        "units": meta.get("units"),
                        "axis": meta.get("axis"),
                        "positive": meta.get("positive"),
                        "valid_min": dtype(meta["valid_min"])
                        if "valid_min" in meta
                        else None,
                        "valid_max": dtype(meta["valid_max"])
                        if "valid_max" in meta
                        else None,
                    }.items()
                    if v is not None
                }
                if coord_attrs.get(
                    "units"
                ) == "days since ?" and original_units.lower().startswith("days since"):
                    coord_attrs["units"] = original_units
                updated = self.ds[name].astype(dtype)
                updated.attrs.update(coord_attrs)
                self.ds[name] = updated
            elif "value" in meta:
                val = meta["value"]
                # Handle character type (e.g., string coordinate)
                if meta["type"] == "character":
                    arr = xr.DataArray(
                        np.array(
                            val, dtype="S"
                        ),  # ensure type is character (byte string)
                        dims=(),
                        attrs={
                            k: v
                            for k, v in {
                                "standard_name": meta.get("standard_name"),
                                "long_name": meta.get("long_name"),
                                "units": meta.get("units"),
                                "axis": meta.get("axis"),
                                "positive": meta.get("positive"),
                                "valid_min": meta.get("valid_min"),
                                "valid_max": meta.get("valid_max"),
                            }.items()
                            if v is not None
                        },
                    )
                else:
                    arr = xr.DataArray(
                        dtype(val),
                        dims=(),
                        attrs={
                            k: v
                            for k, v in {
                                "standard_name": meta.get("standard_name"),
                                "long_name": meta.get("long_name"),
                                "units": meta.get("units"),
                                "axis": meta.get("axis"),
                                "positive": meta.get("positive"),
                                "valid_min": dtype(meta["valid_min"])
                                if "valid_min" in meta
                                else None,
                                "valid_max": dtype(meta["valid_max"])
                                if "valid_max" in meta
                                else None,
                            }.items()
                            if v is not None
                        },
                    )
                self.ds = self.ds.assign_coords({name: arr})


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

        self.ds = self.ds[input_vars]

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


class ACCESS_ESM_CMORiser:
    """
    Coordinates the CMORisation process using CMIP6Vocabulary and CMORiser.
    Handles DRS, versioning, and orchestrates the workflow.
    """

    def __init__(
        self,
        input_paths: Union[str, list],
        compound_name: str,
        experiment_id: str,
        source_id: str,
        variant_label: str,
        grid_label: str,
        activity_id: str = None,
        output_path: Optional[Union[str, Path]] = ".",
        drs_root: Optional[Union[str, Path]] = None,
        parent_info: Optional[Dict[str, Dict[str, Any]]] = None,
    ):
        self.input_paths = input_paths
        self.output_path = Path(output_path)
        self.compound_name = compound_name
        self.variable_mapping = load_cmip6_mappings(compound_name)
        self.drs_root = Path(drs_root) if isinstance(drs_root, str) else drs_root
        self.parent_info = parent_info if parent_info else {}

        self.vocab = CMIP6Vocabulary(
            compound_name=compound_name,
            experiment_id=experiment_id,
            source_id=source_id,
            variant_label=variant_label,
            grid_label=grid_label,
            activity_id=activity_id,
            parent_info=self.parent_info,
        )

        table, cmor_name = compound_name.split(".")
        if table in ("Amon", "Lmon", "SImon", "SImon"):
            self.cmoriser = CMIP6_Atmosphere_CMORiser(
                input_paths=self.input_paths,
                output_path=str(self.output_path),
                cmor_name=cmor_name,
                cmip6_vocab=self.vocab,
                variable_mapping=self.variable_mapping,
                drs_root=drs_root if drs_root else None,
            )
        elif table in ("Oyr", "Oday", "Omon", "Omon_curvilinear"):
            self.cmoriser = CMIP6_Ocean_CMORiser(
                input_paths=self.input_paths,
                output_path=str(self.output_path),
                cmor_name=cmor_name,
                cmip6_vocab=self.vocab,
                variable_mapping=self.variable_mapping,
                drs_root=drs_root if drs_root else None,
            )

    def __getitem__(self, key):
        return self.cmoriser.ds[key]

    def __getattr__(self, attr):
        # This is only called if the attr is not found on CMORiser itself
        return getattr(self.cmoriser.ds, attr)

    def __setitem__(self, key, value):
        self.cmoriser.ds[key] = value

    def __repr__(self):
        return repr(self.cmoriser.ds)

    def run(self):
        """
        Execute the CMORisation workflow.
        """
        self.cmoriser.run()
