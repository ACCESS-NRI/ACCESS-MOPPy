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
        gadi_supergrid_dir = "/g/data/xp65/public/apps/access_mopper_data/grids"
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
        if not supergrid_file:
            raise ValueError("supergrid_file must be provided")

        self.supergrid = xr.open_dataset(supergrid_file).rename_dims(
            {"nxp": "i_full", "nyp": "j_full"}
        )
        self.supergrid = self.supergrid.rename_vars({"x": "x_full", "y": "y_full"})
        self.xt = self.supergrid["x_full"][1::2, 1::2]
        self.yt = self.supergrid["y_full"][1::2, 1::2]
        self.xu = self.supergrid["x_full"][1::2, ::2]
        self.yu = self.supergrid["y_full"][1::2, ::2]
        self.xv = self.supergrid["x_full"][::2, 1::2]
        self.yv = self.supergrid["y_full"][::2, 1::2]
        self.xq = self.supergrid["x_full"][::2, ::2]
        self.yq = self.supergrid["y_full"][::2, ::2]

    def extract_grid(self, grid_type: str):
        if grid_type == "T":
            x = self.xt
            y = self.yt
            corners_x = self.xq
            corners_y = self.yq
        elif grid_type == "U":
            x = self.xu
            y = self.yu
            corners_x = self.supergrid["x_full"]
            corners_y = self.supergrid["y_full"]
        elif grid_type == "V":
            x = self.xv
            y = self.yv
            corners_x = self.supergrid["x_full"]
            corners_y = self.supergrid["y_full"]
        elif grid_type == "Q":
            x = self.xq
            y = self.yq
            corners_x = self.xq
            corners_y = self.yq
        else:
            raise ValueError(f"Unsupported grid_type: {grid_type}")

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
