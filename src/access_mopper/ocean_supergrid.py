import numpy as np
import xarray as xr


class Supergrid:
    def __init__(self, supergrid_file):
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
