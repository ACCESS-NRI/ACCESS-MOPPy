from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Union
import os
import tempfile
import shutil
import subprocess

import netCDF4 as nc
import dask
from dask.diagnostics import ProgressBar
import xarray as xr
from cftime import num2date

from access_mopper.utilities import type_mapping


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

    def load_dataset(self, required_vars: Optional[List[str]] = None):
        def _preprocess(ds):
            return ds[list(required_vars & set(ds.data_vars))]

        self.ds = xr.open_mfdataset(
            self.input_paths,
            combine="nested",  # avoids costly dimension alignment
            concat_dim="time",
            engine="netcdf4",
            decode_cf=False,
            chunks={},
            preprocess=_preprocess,
            parallel=True,  # <--- enables concurrent preprocessing
        )

    def sort_time_dimension(self):
        if "time" in self.ds.dims:
            self.ds = self.ds.sortby("time")
            # Clean up potential duplication
            self.ds = self.ds.sel(time=~self.ds.get_index("time").duplicated())

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

        # TODO: Remove at some point. ESM1.6 should have this fixed.
        if calendar == "GREGORIAN":
            # Replace GREGORIAN with Proleptic Gregorian
            self.ds[var].attrs["calendar"] = "proleptic_gregorian"
            # Replace calendar type attribute with proleptic_gregorian
            if "calendar_type" in self.ds[var].attrs:
                self.ds[var].attrs["calendar_type"] = "proleptic_gregorian"
        calendar = calendar.lower() if calendar else None

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

        # with nc.Dataset(path, "w", format="NETCDF4") as dst:
        #     for k, v in attrs.items():
        #         dst.setncattr(k, v)
        #     for dim, size in self.ds.sizes.items():
        #         if dim == "time":
        #             dst.createDimension(dim, None)  # Unlimited dimension
        #         else:
        #             dst.createDimension(dim, size)
        #     for var in self.ds.variables:
        #         vdat = self.ds[var]
        #         fill = None if var.endswith("_bnds") else vdat.attrs.get("_FillValue")
        #         v = (
        #             dst.createVariable(var, str(vdat.dtype), vdat.dims, fill_value=fill)
        #             if fill
        #             else dst.createVariable(var, str(vdat.dtype), vdat.dims)
        #         )
        #         if not var.endswith("_bnds"):
        #             for a, val in vdat.attrs.items():
        #                 if a != "_FillValue":
        #                     v.setncattr(a, val)
        #         v[:] = vdat.values

        # Updated write method with memory management and Dask support
        data_size_gb = self.ds.nbytes / 1e9
        has_dask = any(hasattr(self.ds[var].data, 'chunks') for var in self.ds.data_vars)
        
        # Threshold: use parallel writing for data > 5GB with Dask arrays
        use_parallel = data_size_gb > 5 and has_dask
        
        if use_parallel:
            print(f"Data size {data_size_gb:.2f}GB, using parallel chunked writing strategy")
            self._write_parallel(path, attrs)
        else:
            print(f"Data size {data_size_gb:.2f}GB, using standard writing")
            self._write_standard(path, attrs)

        print(f"CMORised output written to {path}")

    def _write_standard(self, path, attrs):
        """
        Standard write method (original logic)
        """
        with nc.Dataset(path, "w", format="NETCDF4") as dst:
            # Write global attributes
            for k, v in attrs.items():
                dst.setncattr(k, v)
            
            # Create dimensions
            for dim, size in self.ds.sizes.items():
                if dim == "time":
                    dst.createDimension(dim, None)  # Unlimited dimension
                else:
                    dst.createDimension(dim, size)
            
            # Write variables
            for var in self.ds.variables:
                vdat = self.ds[var]
                fill = None if var.endswith("_bnds") else vdat.attrs.get("_FillValue")
                
                if fill:
                    v = dst.createVariable(var, str(vdat.dtype), vdat.dims, fill_value=fill)
                else:
                    v = dst.createVariable(var, str(vdat.dtype), vdat.dims)
                
                if not var.endswith("_bnds"):
                    for a, val in vdat.attrs.items():
                        if a != "_FillValue":
                            v.setncattr(a, val)
                
                # This is where the original error occurred - loading all data at once
                v[:] = vdat.values
    
    # def _write_parallel(self, path, attrs):
    #     """
    #     Parallel chunked write method (new implementation)
    #     """
    #     # 1. Get optimal temporary directory
    #     temp_base = self._get_optimal_temp_dir()
        
    #     # 2. Create temporary working directory
    #     with tempfile.TemporaryDirectory(dir=temp_base, prefix='cmor_chunks_') as temp_dir:
    #         temp_dir = Path(temp_dir)
    #         print(f"  Temporary directory: {temp_dir}")
            
    #         # 3. Split along time dimension
    #         chunk_size = 12  # 12 time steps per file
    #         total_time = self.ds.dims['time']
    #         n_chunks = (total_time + chunk_size - 1) // chunk_size
            
    #         print(f"  Splitting into {n_chunks} chunks ({chunk_size} time steps each)")
            
    #         # 4. Define function to write a single chunk
    #         def write_chunk(chunk_idx, start_idx, end_idx):
    #             """Write a single time chunk"""
    #             chunk_file = temp_dir / f"chunk_{chunk_idx:04d}.nc"
    #             chunk_ds = self.ds.isel(time=slice(start_idx, end_idx))
                
    #             # Use standard method to write this small chunk
    #             with nc.Dataset(chunk_file, "w", format="NETCDF4") as dst:
    #                 # Write global attributes
    #                 for k, v in attrs.items():
    #                     dst.setncattr(k, v)
                    
    #                 # Create dimensions
    #                 for dim, size in chunk_ds.sizes.items():
    #                     if dim == "time":
    #                         dst.createDimension(dim, None)
    #                     else:
    #                         dst.createDimension(dim, size)
                    
    #                 # Write variables
    #                 for var in chunk_ds.variables:
    #                     vdat = chunk_ds[var]
    #                     fill = None if var.endswith("_bnds") else vdat.attrs.get("_FillValue")
                        
    #                     if fill:
    #                         v = dst.createVariable(var, str(vdat.dtype), vdat.dims, fill_value=fill)
    #                     else:
    #                         v = dst.createVariable(var, str(vdat.dtype), vdat.dims)
                        
    #                     if not var.endswith("_bnds"):
    #                         for a, val in vdat.attrs.items():
    #                             if a != "_FillValue":
    #                                 v.setncattr(a, val)
                        
    #                     # Small chunk can be safely loaded
    #                     v[:] = vdat.values
                
    #             return chunk_file
            
    #         # 5. Write all chunks in parallel
    #         print(f"  Writing {n_chunks} temporary files in parallel...")
    #         tasks = []
    #         for i in range(n_chunks):
    #             start = i * chunk_size
    #             end = min((i + 1) * chunk_size, total_time)
    #             tasks.append(dask.delayed(write_chunk)(i, start, end))
            
    #         with ProgressBar():
    #             chunk_files = dask.compute(*tasks, scheduler='threads', num_workers=4)
            
    #         print(f"  ✓ Temporary files written successfully")
            
    #         # 6. Merge files
    #         print(f"  Merging to final output...")
    #         self._merge_chunks(chunk_files, path)
            
    #         print(f"  ✓ Merge complete, temporary files cleaned up")
    
    # def _merge_chunks(self, chunk_files, output_path):
    #     """
    #     Merge temporary chunk files
    #     Prefer NCO, fallback to xarray
    #     """
    #     import subprocess
    #     import shutil
    #     import xarray as xr
        
    #     # Check if NCO is available
    #     has_nco = shutil.which('ncrcat') is not None
        
    #     if has_nco:
    #         try:
    #             # Use NCO to merge (fast)
    #             print("    Using NCO for merging...")
    #             cmd = ['ncrcat', '-O', '-o', str(output_path)]
    #             cmd.extend([str(f) for f in chunk_files])
                
    #             subprocess.run(cmd, check=True, capture_output=True)
    #             return
    #         except subprocess.CalledProcessError as e:
    #             print(f"    NCO merge failed, falling back to xarray: {e.stderr.decode()}")
        
    #     # Fallback: use xarray to merge
    #     print("    Using xarray for merging...")
    #     datasets = [xr.open_dataset(f, engine='netcdf4') for f in chunk_files]
    #     merged = xr.concat(datasets, dim='time')
        
    #     # Write final file
    #     merged.to_netcdf(output_path, engine='netcdf4')
        
    #     # Cleanup
    #     for ds in datasets:
    #         ds.close()

    def _write_parallel(self, path, attrs):
        """
        Parallel chunked write method using temporary folder in target directory
        """
        from pathlib import Path
        import shutil
        import dask
        from dask.diagnostics import ProgressBar
        
        path = Path(path)
        data_size_gb = self.ds.nbytes / 1e9
        
        # Create temp folder next to the output file
        temp_dir = path.parent / f'.tmp_{path.stem}_{os.getpid()}'
        temp_dir.mkdir(parents=True, exist_ok=True)
        
        print(f"  Data size: {data_size_gb:.2f} GB")
        print(f"  Temporary directory: {temp_dir}")

        try:
            from dask.distributed import Client
            client = Client.current()
            client.close()
            print("  Closed existing Dask distributed client")
        except (ValueError, ImportError):
            pass  # No client exists, continue
        
        try:
            # Check available space in target directory
            usage = shutil.disk_usage(path.parent)
            available_gb = usage.free / 1e9
            required_gb = data_size_gb * 1.5  # Need 1.5x for temp files
            
            print(f"  Available space: {available_gb:.2f} GB")
            print(f"  Required space: {required_gb:.2f} GB")
            
            if available_gb < required_gb:
                raise RuntimeError(
                    f"Insufficient disk space in {path.parent}\n"
                    f"  Required: {required_gb:.2f} GB\n"
                    f"  Available: {available_gb:.2f} GB"
                )
            
            # Split along time dimension
            chunk_size = 12  # 12 time steps per chunk
            total_time = self.ds.dims['time']
            n_chunks = (total_time + chunk_size - 1) // chunk_size
            
            print(f"  Splitting into {n_chunks} chunks ({chunk_size} time steps each)")
            
            # Define function to write a single chunk
            def write_chunk(chunk_idx, start_idx, end_idx):
                """Write a single time chunk"""
                chunk_file = temp_dir / f"chunk_{chunk_idx:04d}.nc"
                chunk_ds = self.ds.isel(time=slice(start_idx, end_idx))
                
                # Write using netCDF4
                with nc.Dataset(chunk_file, "w", format="NETCDF4") as dst:
                    # Write global attributes
                    for k, v in attrs.items():
                        dst.setncattr(k, v)
                    
                    # Create dimensions
                    for dim, size in chunk_ds.sizes.items():
                        if dim == "time":
                            dst.createDimension(dim, None)
                        else:
                            dst.createDimension(dim, size)
                    
                    # Write variables
                    for var in chunk_ds.variables:
                        vdat = chunk_ds[var]
                        fill = None if var.endswith("_bnds") else vdat.attrs.get("_FillValue")
                        
                        if fill:
                            v = dst.createVariable(var, str(vdat.dtype), vdat.dims, fill_value=fill)
                        else:
                            v = dst.createVariable(var, str(vdat.dtype), vdat.dims)
                        
                        if not var.endswith("_bnds"):
                            for a, val in vdat.attrs.items():
                                if a != "_FillValue":
                                    v.setncattr(a, val)
                        
                        # Write data (small chunk is safe)
                        v[:] = vdat.values
                
                print(f"    ✓ Chunk {chunk_idx:04d}/{n_chunks-1}")
                return chunk_file
            
            # Write all chunks in parallel
            print(f"  Writing {n_chunks} temporary files in parallel...")
            tasks = []
            for i in range(n_chunks):
                start = i * chunk_size
                end = min((i + 1) * chunk_size, total_time)
                tasks.append(dask.delayed(write_chunk)(i, start, end))
            
            with ProgressBar():
                chunk_files = dask.compute(*tasks, scheduler='threads', num_workers=int(os.environ.get('PBS_NCPUS', 4)))
            
            print(f"  ✓ All temporary files written")
            
            # Merge files
            print(f"  Merging to final output: {path.name}")
            self._merge_chunks(chunk_files, path)
            
            print(f"  ✓ Merge complete")
        
        finally:
            # Always cleanup temporary directory, even if error occurs
            if temp_dir.exists():
                print(f"  Cleaning up temporary files...")
                try:
                    shutil.rmtree(temp_dir)
                    print(f"  ✓ Temporary directory removed: {temp_dir}")
                except Exception as e:
                    print(f"  Warning: Could not remove {temp_dir}: {e}")
                    print(f"  Please manually remove: rm -rf {temp_dir}")


    def _merge_chunks(self, chunk_files, output_path):
        """
        Merge temporary chunk files
        Prefer NCO, fallback to xarray
        """
        # import subprocess
        # import shutil
        # import xarray as xr

        chunk_files = sorted(chunk_files)
        
        # Check if NCO is available
        has_nco = shutil.which('ncrcat') is not None
        
        if has_nco:
            try:
                # Use NCO to merge (fastest)
                print("    Using NCO ncrcat for merging...")
                cmd = ['ncrcat', '-O', '-o', str(output_path)]
                cmd.extend([str(f) for f in chunk_files])
                
                result = subprocess.run(
                    cmd, 
                    check=True, 
                    capture_output=True,
                    text=True
                )
                print("    ✓ NCO merge successful")
                return
                
            except subprocess.CalledProcessError as e:
                print(f"    NCO merge failed: {e.stderr}")
                print("    Falling back to xarray...")
            except Exception as e:
                print(f"    Error with NCO: {e}")
                print("    Falling back to xarray...")
        
        # Fallback: use xarray to merge
        print("    Using xarray for merging...")
        try:
            datasets = []
            for cf in chunk_files:
                ds = xr.open_dataset(cf, engine='netcdf4')
                datasets.append(ds)
            
            # Concatenate along time dimension
            merged = xr.concat(datasets, dim='time')
            
            # Write final file with compression
            encoding = {
                var: {'zlib': True, 'complevel': 4} 
                for var in merged.data_vars
            }
            merged.to_netcdf(output_path, engine='netcdf4', encoding=encoding)
            
            # Close datasets
            for ds in datasets:
                ds.close()
            
            print("    ✓ xarray merge successful")
            
        except Exception as e:
            print(f"    ERROR during merge: {e}")
            raise
    
    def _get_optimal_temp_dir(self):
        """
        Get optimal temporary directory
        Priority: PBS_JOBFS > TMPDIR > /dev/shm > /tmp
        """
        
        # 1. PBS job local storage (NCI Gadi specific, fastest)
        if 'PBS_JOBFS' in os.environ:
            jobfs = Path(os.environ['PBS_JOBFS'])
            if jobfs.exists():
                return str(jobfs)
        
        # 2. User-specified temporary directory
        if 'TMPDIR' in os.environ:
            tmpdir = Path(os.environ['TMPDIR'])
            if tmpdir.exists():
                return str(tmpdir)
        
        # 3. Shared memory (good for small data)
        data_size_gb = self.ds.nbytes / 1e9
        if data_size_gb < 10 and Path('/dev/shm').exists():
            return '/dev/shm'
        
        # 4. Default temporary directory
        return '/tmp'



    # def write(self):
    #     import gc

    #     import psutil

    #     attrs = self.ds.attrs
    #     required_keys = [
    #         "variable_id",
    #         "table_id",
    #         "source_id",
    #         "experiment_id",
    #         "variant_label",
    #         "grid_label",
    #     ]
    #     missing = [k for k in required_keys if k not in attrs]
    #     if missing:
    #         raise ValueError(
    #             f"Missing required CMIP6 global attributes for filename: {missing}"
    #         )

    #     time_var = self.ds[self.cmor_name].coords["time"]
    #     units = time_var.attrs["units"]
    #     calendar = time_var.attrs.get("calendar", "standard").lower()
    #     times = num2date(time_var.values[[0, -1]], units=units, calendar=calendar)
    #     start, end = [f"{t.year:04d}{t.month:02d}" for t in times]
    #     time_range = f"{start}-{end}"

    #     filename = (
    #         f"{attrs['variable_id']}_{attrs['table_id']}_{attrs['source_id']}_"
    #         f"{attrs['experiment_id']}_{attrs['variant_label']}_"
    #         f"{attrs['grid_label']}_{time_range}.nc"
    #     )

    #     if self.drs_root:
    #         drs_path = self._build_drs_path(attrs)
    #         drs_path.mkdir(parents=True, exist_ok=True)
    #         path = drs_path / filename
    #         self._update_latest_symlink(drs_path)
    #     else:
    #         path = Path(self.output_path) / filename
    #         path.parent.mkdir(parents=True, exist_ok=True)

    #     with nc.Dataset(path, "w", format="NETCDF4") as dst:
    #         # Set global attributes
    #         for k, v in attrs.items():
    #             dst.setncattr(k, v)

    #         # Create dimensions
    #         for dim, size in self.ds.sizes.items():
    #             if dim == "time":
    #                 dst.createDimension(dim, None)  # Unlimited dimension
    #             else:
    #                 dst.createDimension(dim, size)

    #         # Create variables and write data
    #         for var in self.ds.variables:
    #             vdat = self.ds[var]
    #             fill = None if var.endswith("_bnds") else vdat.attrs.get("_FillValue")

    #             # Create variable
    #             v = (
    #                 dst.createVariable(var, str(vdat.dtype), vdat.dims, fill_value=fill)
    #                 if fill
    #                 else dst.createVariable(var, str(vdat.dtype), vdat.dims)
    #             )

    #             # Set variable attributes (except _FillValue which is handled above)
    #             if not var.endswith("_bnds"):
    #                 for a, val in vdat.attrs.items():
    #                     if a != "_FillValue":
    #                         v.setncattr(a, val)

    #             # Write data with memory management
    #             print(f"Writing variable: {var} (shape: {vdat.shape})")

    #             try:
    #                 # Check data size and available memory
    #                 data_size_gb = vdat.nbytes / 1e9
    #                 available_mem_gb = psutil.virtual_memory().available / 1e9

    #                 print(f"  Data size: {data_size_gb:.2f} GB")
    #                 print(f"  Available memory: {available_mem_gb:.2f} GB")

    #                 # If data is small enough or not chunked, write directly
    #                 if data_size_gb < available_mem_gb * 0.5 and (
    #                     not hasattr(vdat, "chunks") or vdat.chunks is None
    #                 ):
    #                     print(f"  Writing {var} directly...")
    #                     v[:] = vdat.values

    #                 # For large or chunked data, write in time slices
    #                 elif "time" in vdat.dims:
    #                     print(f"  Writing {var} in time slices...")
    #                     time_axis = vdat.dims.index("time")
    #                     n_times = vdat.shape[time_axis]

    #                     # Write one time step at a time
    #                     for t in range(n_times):
    #                         if t % 10 == 0:  # Progress indicator
    #                             print(f"    Time step {t+1}/{n_times}")

    #                         # Create slice for time dimension
    #                         slices = [slice(None)] * len(vdat.dims)
    #                         slices[time_axis] = t

    #                         # Get data for this time step
    #                         time_data = vdat[tuple(slices)]

    #                         # Compute if it's a dask array
    #                         if hasattr(time_data, "compute"):
    #                             time_data = time_data.compute()

    #                         # Write to netCDF
    #                         v_slices = [slice(None)] * len(v.dimensions)
    #                         v_slices[time_axis] = t
    #                         v[tuple(v_slices)] = time_data.values

    #                         # Garbage collect every 5 time steps
    #                         if t % 5 == 4:
    #                             gc.collect()

    #                 # For non-time variables or small spatial data
    #                 else:
    #                     print(f"  Writing {var} with chunking...")
    #                     if hasattr(vdat, "compute"):
    #                         # For dask arrays, compute in chunks
    #                         chunk_data = vdat.compute()
    #                         v[:] = chunk_data.values
    #                     else:
    #                         v[:] = vdat.values

    #             except MemoryError as e:
    #                 print(
    #                     f"  Memory error writing {var}, trying alternative approach..."
    #                 )

    #                 # Fallback: write in smaller spatial chunks
    #                 if len(vdat.shape) >= 3:  # Has spatial dimensions
    #                     # Write in blocks
    #                     if "time" in vdat.dims:
    #                         time_axis = vdat.dims.index("time")
    #                         for t in range(vdat.shape[time_axis]):
    #                             print(
    #                                 f"    Fallback: Time step {t+1}/{vdat.shape[time_axis]}"
    #                             )
    #                             slices = [slice(None)] * len(vdat.dims)
    #                             slices[time_axis] = t

    #                             time_slice = vdat[tuple(slices)]
    #                             if hasattr(time_slice, "compute"):
    #                                 time_slice = time_slice.compute()

    #                             v_slices = [slice(None)] * len(v.dimensions)
    #                             v_slices[time_axis] = t
    #                             v[tuple(v_slices)] = time_slice.values

    #                             gc.collect()
    #                     else:
    #                         # For non-time variables, just try direct assignment
    #                         if hasattr(vdat, "compute"):
    #                             vdat = vdat.compute()
    #                         v[:] = vdat.values
    #                 else:
    #                     raise e

    #             except Exception as e:
    #                 print(f"Error writing variable {var}: {e}")
    #                 raise e

    #             print(f"  Finished writing {var}")
    #             gc.collect()

    #     print(f"CMORised output written to {path}")

    #     # Final garbage collection
    #     gc.collect()

    def run(self, write_output: bool = False):
        self.select_and_process_variables()
        self.drop_intermediates()
        self.update_attributes()
        self.reorder()
        if write_output:
            self.write()
