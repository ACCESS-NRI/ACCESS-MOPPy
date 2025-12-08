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
        """
        Load the grid cell quantities following C-grid conventions in
        https://gist.github.com/adcroft/c1e207024fe1189b43dddc5f1fe7dd6c

        With these we can easily get the grid metrics for any Arakawa grid
        type
        """
        self.supergrid = xr.open_dataset(supergrid_file)

        x = self.supergrid["x"].values
        y = self.supergrid["y"].values

        # h-cell quantities
        # -----------------
        hcell_centres_x = x[1::2, 1::2]
        hcell_corners_x = np.zeros((*hcell_centres_x.shape, 4))
        hcell_corners_x[:, :, 0] = x[:-1:2, :-1:2]  # SW corner
        hcell_corners_x[:, :, 1] = x[:-1:2, 2::2]  # SE corner
        hcell_corners_x[:, :, 2] = x[2::2, 2::2]  # NE corner
        hcell_corners_x[:, :, 3] = x[2::2, :-1:2]  # NW corner

        hcell_centres_y = y[1::2, 1::2]
        hcell_corners_y = np.zeros((*hcell_centres_y.shape, 4))
        hcell_corners_y[:, :, 0] = y[:-1:2, :-1:2]  # SW corner
        hcell_corners_y[:, :, 1] = y[:-1:2, 2::2]  # SE corner
        hcell_corners_y[:, :, 2] = y[2::2, 2::2]  # NE corner
        hcell_corners_y[:, :, 3] = y[2::2, :-1:2]  # NW corner

        self.hcell_centres_x = hcell_centres_x
        self.hcell_centres_y = hcell_centres_y
        self.hcell_corners_x = hcell_corners_x
        self.hcell_corners_y = hcell_corners_y

        # q-cell quantities
        # -----------------
        # Extend grid over boundaries
        # Here we extend all edges so that the locations can
        # be used for both asymmetric and symmetric grids,
        # which include q-cells on the first row/col of the
        # supergrid

        # The extension is done as follows:
        # 1. Append reversed penultimate row to top for
        # tripolar fold
        # 2. Append second col to RHS for wrap-around
        # periodicity
        # 3. Append penultimate col to LHS for wrap-around
        # periodicity, accounting for the fact that we have
        # just appended to the RHS
        # 4. Add row to bottom, assuming same y-spacing
        # as between the first two rows, for open
        # boundary
        penult_row_rev = np.fliplr(x[-2:-1, :])
        x_ext = np.append(x, penult_row_rev, axis=0)
        second_col = x_ext[:, 1:2]
        x_ext = np.append(x_ext, second_col, axis=1)
        penult_col = x_ext[:, -3:-2]
        x_ext = np.append(penult_col, x_ext, axis=1)
        bottom_row = x_ext[:1, :]
        x_ext = np.append(bottom_row, x_ext, axis=0)

        penult_row_rev = np.fliplr(y[-2:-1, :])
        y_ext = np.append(y, penult_row_rev, axis=0)
        second_col = y_ext[:, 1:2]
        y_ext = np.append(y_ext, second_col, axis=1)
        penult_col = y_ext[:, -3:-2]
        y_ext = np.append(penult_col, y_ext, axis=1)
        bottom_row_extrap = 2 * y_ext[:1, :] - y_ext[1:2, :]
        y_ext = np.append(bottom_row_extrap, y_ext, axis=0)

        qcell_centres_x = x[::2, ::2]
        qcell_corners_x = np.zeros((*qcell_centres_x.shape, 4))
        qcell_corners_x[:, :, 0] = x_ext[:-1:2, :-1:2]  # SW corner
        qcell_corners_x[:, :, 1] = x_ext[:-1:2, 2::2]  # SE corner
        qcell_corners_x[:, :, 2] = x_ext[2::2, 2::2]  # NE corner
        qcell_corners_x[:, :, 3] = x_ext[2::2, :-1:2]  # NW corner

        qcell_centres_y = y[::2, ::2]
        qcell_corners_y = np.zeros((*qcell_centres_y.shape, 4))
        qcell_corners_y[:, :, 0] = y_ext[:-1:2, :-1:2]  # SW corner
        qcell_corners_y[:, :, 1] = y_ext[:-1:2, 2::2]  # SE corner
        qcell_corners_y[:, :, 2] = y_ext[2::2, 2::2]  # NE corner
        qcell_corners_y[:, :, 3] = y_ext[2::2, :-1:2]  # NW corner

        self.qcell_centres_x = qcell_centres_x
        self.qcell_centres_y = qcell_centres_y
        self.qcell_corners_x = qcell_corners_x
        self.qcell_corners_y = qcell_corners_y

        # u-cell quantities
        # -----------------
        ucell_centres_x = x[1::2, ::2]
        ucell_corners_x = np.zeros((*ucell_centres_x.shape, 4))
        ucell_corners_x[:, :, 0] = x_ext[1:-2:2, :-1:2]  # SW corner
        ucell_corners_x[:, :, 1] = x_ext[1:-2:2, 2::2]  # SE corner
        ucell_corners_x[:, :, 2] = x_ext[3:-1:2, 2::2]  # NE corner
        ucell_corners_x[:, :, 3] = x_ext[3:-1:2, :-1:2]  # NW corner

        ucell_centres_y = y[1::2, ::2]
        ucell_corners_y = np.zeros((*ucell_centres_y.shape, 4))
        ucell_corners_y[:, :, 0] = y_ext[1:-2:2, :-1:2]  # SW corner
        ucell_corners_y[:, :, 1] = y_ext[1:-2:2, 2::2]  # SE corner
        ucell_corners_y[:, :, 2] = y_ext[3:-1:2, 2::2]  # NE corner
        ucell_corners_y[:, :, 3] = y_ext[3:-1:2, :-1:2]  # NW corner

        self.ucell_centres_x = ucell_centres_x
        self.ucell_centres_y = ucell_centres_y
        self.ucell_corners_x = ucell_corners_x
        self.ucell_corners_y = ucell_corners_y

        # v-cell quantities
        # -----------------
        vcell_centres_x = x[::2, 1::2]
        vcell_corners_x = np.zeros((*vcell_centres_x.shape, 4))
        vcell_corners_x[:, :, 0] = x_ext[:-1:2, 1:-2:2]  # SW corner
        vcell_corners_x[:, :, 1] = x_ext[:-1:2, 3:-1:2]  # SE corner
        vcell_corners_x[:, :, 2] = x_ext[2::2, 3:-1:2]  # NE corner
        vcell_corners_x[:, :, 3] = x_ext[2::2, 1:-2:2]  # NW corner

        vcell_centres_y = y[::2, 1::2]
        vcell_corners_y = np.zeros((*vcell_centres_y.shape, 4))
        vcell_corners_y[:, :, 0] = y_ext[:-1:2, 1:-2:2]  # SW corner
        vcell_corners_y[:, :, 1] = y_ext[:-1:2, 3:-1:2]  # SE corner
        vcell_corners_y[:, :, 2] = y_ext[2::2, 3:-1:2]  # NE corner
        vcell_corners_y[:, :, 3] = y_ext[2::2, 1:-2:2]  # NW corner

        self.vcell_centres_x = vcell_centres_x
        self.vcell_centres_y = vcell_centres_y
        self.vcell_corners_x = vcell_corners_x
        self.vcell_corners_y = vcell_corners_y

    def extract_grid(self, grid_type: str, arakawa: str, symmetric=None):
        """
        Extract grid coordinates and bounds based on the specified grid type.

        Parameters
        ----------
        grid_type: str
            A string indicating the grid cell location to extract. Can be one of
            "U", "V", "T", "C".
        arakawa: str
            The Arakawa grid type. Only "B" and "C" are currently supported.
        symmetric: boolean
            If true, return grid for MOM6 symmetric memory mode. Only used if
            arakawa="C"
        """

        if (arakawa == "C") & (symmetric is None):
            raise ValueError("Must specify symmetric as True or False when arakawa='C'")

        match arakawa:
            case "B":
                match grid_type:
                    case "T":
                        x_centers = self.hcell_centres_x  # geolon_t
                        x_bounds = self.hcell_corners_x
                        y_centers = self.hcell_centres_y  # geolat_t
                        y_bounds = self.hcell_corners_y
                    case "U":
                        x_centers = self.qcell_centres_x[1:, 1:]  # geolon_c
                        x_bounds = self.qcell_corners_x[1:, 1:, :]
                        y_centers = self.hcell_centres_y  # geolat_t
                        y_bounds = self.hcell_corners_y
                    case "V":
                        x_centers = self.hcell_centres_x  # geolon_t
                        x_bounds = self.hcell_corners_x
                        y_centers = self.qcell_centres_y[1:, 1:]  # geolat_c
                        y_bounds = self.qcell_corners_y[1:, 1:, :]
                    case "C":
                        x_centers = self.qcell_centres_x[1:, 1:]  # geolon_c
                        x_bounds = self.qcell_corners_x[1:, 1:, :]
                        y_centers = self.qcell_centres_y[1:, 1:]  # geolat_c
                        y_bounds = self.qcell_corners_y[1:, 1:, :]
                    case _:
                        raise ValueError(
                            f"grid_type={grid_type} is not a supported grid_type for arakawa={arakawa}"
                        )
            case "C":
                i_start = 0 if symmetric else 1
                match grid_type:
                    case "T":
                        x_centers = self.hcell_centres_x  # geolon
                        x_bounds = self.hcell_corners_x
                        y_centers = self.hcell_centres_y  # geolat
                        y_bounds = self.hcell_corners_y
                    case "U":
                        x_centers = self.ucell_centres_x[:, i_start:]  # geolon_u
                        x_bounds = self.ucell_corners_x[:, i_start:, :]
                        y_centers = self.ucell_centres_y[:, i_start:]  # geolat_u
                        y_bounds = self.ucell_corners_y[:, i_start:, :]
                    case "V":
                        x_centers = self.vcell_centres_x[i_start:, :]  # geolon_v
                        x_bounds = self.vcell_corners_x[i_start:, :, :]
                        y_centers = self.vcell_centres_y[i_start:, :]  # geolat_v
                        y_bounds = self.vcell_corners_y[i_start:, :, :]
                    case "C":
                        x_centers = self.qcell_centres_x[i_start:, i_start:]  # geolon_c
                        x_bounds = self.qcell_corners_x[i_start:, i_start:, :]
                        y_centers = self.qcell_centres_y[i_start:, i_start:]  # geolat_c
                        y_bounds = self.qcell_corners_y[i_start:, i_start:, :]
                    case _:
                        raise ValueError(
                            f"grid_type={grid_type} is not a supported grid_type for arakawa={arakawa}"
                        )
            case _:
                raise ValueError(f"arakawa={arakawa} is not supported")

        lat = xr.DataArray(y_centers, dims=("j", "i"), name="latitude")
        lon = xr.DataArray(x_centers, dims=("j", "i"), name="longitude")
        lat_bnds = xr.DataArray(
            y_bounds, dims=("j", "i", "vertices"), name="vertices_latitude"
        )
        lon_bnds = xr.DataArray(
            x_bounds, dims=("j", "i", "vertices"), name="vertices_longitude"
        )

        i_coord = xr.DataArray(
            np.arange(x_centers.shape[1]),
            dims="i",
            name="i",
            attrs={"long_name": "cell index along first dimension", "units": "1"},
        )
        j_coord = xr.DataArray(
            np.arange(y_centers.shape[0]),
            dims="j",
            name="j",
            attrs={"long_name": "cell index along second dimension", "units": "1"},
        )
        vertices = xr.DataArray(np.arange(4), dims="vertices", name="vertices")

        lat = xr.DataArray(y_centers, dims=("j", "i"), name="latitude")
        lon = xr.DataArray((x_centers + 360) % 360, dims=("j", "i"), name="longitude")

        lat_bnds = xr.DataArray(
            y_bounds, dims=("j", "i", "vertices"), name="vertices_latitude"
        )
        lon_bnds = xr.DataArray(
            (x_bounds + 360) % 360,
            dims=("j", "i", "vertices"),
            name="vertices_longitude",
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
