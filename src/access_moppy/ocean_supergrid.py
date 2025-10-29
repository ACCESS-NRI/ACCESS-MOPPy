import os
import tempfile

import numpy as np
import requests
import xarray as xr
from tqdm import tqdm


class Supergrid:
    def __init__(self, nominal_resolution: str):
        """Initialize the Supergrid class with a specified nominal resolution."""

        self.nominal_resolution = nominal_resolution
        self.supergrid_path = self.get_supergrid_path(nominal_resolution)
        self.load_supergrid(self.supergrid_path)

    def get_supergrid_path(self, nominal_resolution: str) -> str:
        """Get the path to the supergrid file based on the nominal resolution.
        If the file is not found on Gadi, it will attempt to download it from Google Drive.
        """
        if not self.nominal_resolution:
            raise ValueError("nominal_resolution must be provided")
        # Mapping nominal resolution to file names
        supergrid_filenames = {
            "100 km": "mom1deg.nc",
            "25 km": "mom025deg.nc",
            "10 km": "mom01deg.nc",
        }

        if nominal_resolution not in supergrid_filenames:
            raise ValueError(
                f"Unknown or unsupported nominal resolution: {nominal_resolution}"
            )

        supergrid_filename = supergrid_filenames[nominal_resolution]
        gadi_supergrid_dir = "/g/data/xp65/public/apps/access_moppy_data/grids"
        gadi_supergrid_path = os.path.join(gadi_supergrid_dir, supergrid_filename)

        # Check if running on Gadi and file exists
        if os.path.exists(gadi_supergrid_path):
            supergrid_path = gadi_supergrid_path
        else:
            # Not on Gadi or file not available, download from Google Drive
            # Mapping nominal resolution to Google Drive file IDs
            gdrive_file_ids = {
                "100 km": "1Ito5EspxaICiTD1cfzcpcWTGNYg29fQf",
                "25 km": "1aNO1Y7HeU4YHjPi1Wsw_xRbp-SQG3NoA",
                "10 km": "GOOGLE_DRIVE_FILE_ID_FOR_10KM",
            }
            file_id = gdrive_file_ids[nominal_resolution]
            tmp_dir = tempfile.gettempdir()
            supergrid_path = os.path.join(tmp_dir, supergrid_filename)
            if not os.path.exists(supergrid_path):
                try:

                    def download_from_gdrive(file_id, dest_path):
                        # Download files from Google Drive (no token handling)
                        URL = (
                            f"https://drive.google.com/uc?export=download&id={file_id}"
                        )
                        with requests.get(URL, stream=True) as response:
                            response.raise_for_status()
                            total = int(response.headers.get("content-length", 0))
                            with (
                                open(dest_path, "wb") as f,
                                tqdm(
                                    total=total,
                                    unit="B",
                                    unit_scale=True,
                                    desc=f"Downloading {os.path.basename(dest_path)}",
                                ) as pbar,
                            ):
                                for chunk in response.iter_content(32768):
                                    if chunk:
                                        f.write(chunk)
                                        pbar.update(len(chunk))

                    download_from_gdrive(file_id, supergrid_path)
                except Exception as e:
                    raise RuntimeError(
                        f"Could not download supergrid file for {nominal_resolution}: {e}"
                    )
        return supergrid_path

    def load_supergrid(self, supergrid_file: str):
        """Load the supergrid dataset from the specified file."""
        raise NotImplementedError("Subclasses must implement load_supergrid.")

    def _get_xy(self, grid_type: str):
        """Extract grid x, y based on the specified grid type."""
        raise NotImplementedError("Subclasses must implement _get_xy.")

    def extract_grid(self, grid_type: str):
        """Extract grid coordinates and bounds based on the specified grid type."""
        x, y = self._get_xy(grid_type)

        # Calculate corner coordinates
        if grid_type["x"] == "T":
            # For T-grid (tracer points), corners are directly located at every other
            # point on the supergrid (even indices correspond to corners)
            corners_x = self.supergrid["x_full"][0::2, 0::2]
        else:
            # For non-T grids (e.g., U-grid), we need to reconstruct corner coordinates
            # by extending the tracer grid (xt) to include the outer boundary edges.

            # Extend xt in the y-direction by appending the last row reversed
            # This ensures periodic or symmetric coverage along j (latitude-like axis)
            xt_ext = xr.concat(
                [self.xt, self.xt.isel(j_full=-1, i_full=slice(None, None, -1))],
                dim="j_full",
            )
            # Extend xt_ext in the x-direction by appending the first column
            # This completes the wrap-around along i (longitude-like axis)
            corners_x = xr.concat([xt_ext, xt_ext.isel(i_full=0)], dim="i_full")

        if grid_type["y"] == "T":
            corners_y = self.supergrid["y_full"][0::2, 0::2]
        else:
            # Extract corner coordinates for U,Q grids
            yt_ext = xr.concat(
                [self.yt, self.yt.isel(j_full=-1, i_full=slice(None, None, -1))],
                dim="j_full",
            )
            corners_y = xr.concat([yt_ext, yt_ext.isel(i_full=0)], dim="i_full")

        corners_x = (corners_x + 360) % 360

        i_coord = xr.DataArray(
            np.arange(x.shape[1]),
            dims="i",
            name="i",
            attrs={"long_name": "cell index along first dimension", "units": "1"},
        )
        j_coord = xr.DataArray(
            np.arange(y.shape[0]),
            dims="j",
            name="j",
            attrs={"long_name": "cell index along second dimension", "units": "1"},
        )
        vertices = xr.DataArray(np.arange(4), dims="vertices", name="vertices")

        lat = xr.DataArray(y, dims=("j", "i"), name="latitude")
        lon = xr.DataArray((x + 360) % 360, dims=("j", "i"), name="longitude")

        lat_bnds = (
            xr.concat(
                [
                    corners_y[:-1, :-1].expand_dims(vertices=[0]),
                    corners_y[:-1, 1:].expand_dims(vertices=[1]),
                    corners_y[1:, 1:].expand_dims(vertices=[2]),
                    corners_y[1:, :-1].expand_dims(vertices=[3]),
                ],
                dim="vertices",
            )
            .rename({"j_full": "j", "i_full": "i"})
            .transpose("j", "i", "vertices")
            .rename("vertices_latitude")
        )

        lon_bnds = (
            xr.concat(
                [
                    corners_x[:-1, :-1].expand_dims(vertices=[0]),
                    corners_x[:-1, 1:].expand_dims(vertices=[1]),
                    corners_x[1:, 1:].expand_dims(vertices=[2]),
                    corners_x[1:, :-1].expand_dims(vertices=[3]),
                ],
                dim="vertices",
            )
            .rename({"j_full": "j", "i_full": "i"})
            .transpose("j", "i", "vertices")
            .rename("vertices_longitude")
        )

        return {
            "i": i_coord,
            "j": j_coord,
            "vertices": vertices,
            "latitude": lat,
            "longitude": lon,
            "vertices_latitude": lat_bnds,
            "vertices_longitude": lon_bnds,
        }


class Supergrid_cgrid(Supergrid):
    """C-grid supergrid handling for MOM6 models."""

    def __init__(self, nominal_resolution: str):
        """Initialize the Supergrid_cgrid class with a specified nominal resolution."""
        super().__init__(nominal_resolution)

    def load_supergrid(self, supergrid_file: str):
        """Load the supergrid dataset from the specified file."""
        if not supergrid_file:
            raise ValueError("supergrid_file must be provided")

        self.supergrid = xr.open_dataset(supergrid_file).rename_dims(
            {"nxp": "i_full", "nyp": "j_full"}
        )
        # Extract grid positions for different Arakawa C-grid points
        # - T-points: tracer (cell centers)
        # - U-points: zonal velocity (east-west)
        # - V-points: meridional velocity (north-south)
        # - Q-points: cell corners
        self.supergrid = self.supergrid.rename_vars({"x": "x_full", "y": "y_full"})
        self.xt = self.supergrid["x_full"][1::2, 1::2]
        self.yt = self.supergrid["y_full"][1::2, 1::2]
        self.xu = self.supergrid["x_full"][1::2, :-1:2]
        self.yu = self.supergrid["y_full"][1::2, :-1:2]
        self.xv = self.supergrid["x_full"][:-1:2, 1::2]
        self.yv = self.supergrid["y_full"][:-1:2, 1::2]
        self.xq = self.supergrid["x_full"][::2, ::2]
        self.yq = self.supergrid["y_full"][::2, ::2]

    def _get_xy(self, grid_type: str):
        # Extract grid coordinates and bounds based on the specified grid type
        if grid_type["x"] == "T":
            x = self.xt
        elif grid_type["x"] == "U":
            x = self.xu
        elif grid_type["x"] == "V":
            x = self.xv
        elif grid_type["x"] == "Q":
            x = self.xq[1::, 1::]
        else:
            raise ValueError(f"Unsupported grid_type: x {grid_type['x']}")

        # Extract grid coordinates and bounds based on the specified grid type
        if grid_type["y"] == "T":
            y = self.yt
        elif grid_type["y"] == "U":
            y = self.yu
        elif grid_type["y"] == "V":
            y = self.yv
        elif grid_type["y"] == "Q":
            y = self.yq[1::, 1::]
        else:
            raise ValueError(f"Unsupported grid_type: y {grid_type['y']}")
        return x, y


class Supergrid_bgrid(Supergrid):
    """B-grid supergrid handling for MOM5 models."""

    def __init__(self, nominal_resolution: str):
        """Initialize the Supergrid_bgrid class with a specified nominal resolution."""
        super().__init__(nominal_resolution)

    def load_supergrid(self, supergrid_file: str):
        """Load the supergrid dataset from the specified file."""
        if not supergrid_file:
            raise ValueError("supergrid_file must be provided")

        self.supergrid = xr.open_dataset(supergrid_file).rename_dims(
            {"nxp": "i_full", "nyp": "j_full"}
        )
        self.supergrid = self.supergrid.rename_vars({"x": "x_full", "y": "y_full"})
        # Extract grid positions for Arakawa B-grid:
        # - T-points (mass points) at cell centers
        # - U-points (velocity points) at the same position as T-points but can differ in spacing
        self.xt = self.supergrid["x_full"][1::2, 1::2]
        self.yt = self.supergrid["y_full"][1::2, 1::2]
        self.xu = self.supergrid["x_full"][2::2, 2::2]
        self.yu = self.supergrid["y_full"][2::2, 2::2]

    def _get_xy(self, grid_type: str):
        # Extract grid coordinates and bounds based on the specified grid type
        if grid_type["x"] == "T":
            x = self.xt
        elif grid_type["x"] == "U":
            x = self.xu
        else:
            raise ValueError(f"Unsupported grid_type: x {grid_type['x']}")

        # Extract grid coordinates and bounds based on the specified grid type
        if grid_type["y"] == "T":
            y = self.yt
        elif grid_type["y"] == "U":
            y = self.yu
        else:
            raise ValueError(f"Unsupported grid_type: y {grid_type['y']}")
        return x, y
