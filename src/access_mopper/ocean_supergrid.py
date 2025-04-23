import numpy as np
import xarray as xr


class Supergrid(object):
    # See description in the following document
    # https://gist.github.com/rbeucher/b67c2b461557bc215a70017ea8dd337b

    def __init__(self, supergrid_file):
        self.supergrid_file = supergrid_file
        self.supergrid = xr.open_dataset(supergrid_file)

        # T point locations
        self.xt = self.supergrid["x"][1::2, 1::2]
        self.yt = self.supergrid["y"][1::2, 1::2]
        # Corner point locations
        self.xq = self.supergrid["x"][::2, ::2]
        self.yq = self.supergrid["y"][::2, ::2]
        # U point locations
        self.xu = self.supergrid["x"][1::2, ::2]
        self.yu = self.supergrid["y"][1::2, ::2]
        # V point locations
        self.xv = self.supergrid["x"][::2, 1::2]
        self.yv = self.supergrid["y"][::2, 1::2]

    def h_cells(self):
        self.lat = self.yt.values
        self.lat_bnds = np.zeros((*self.yt.shape, 4))
        self.lat_bnds[..., 0] = self.yq[1:, :-1]  # SW corner
        self.lat_bnds[..., 1] = self.yq[1:, 1:]  # SE corner
        self.lat_bnds[..., 2] = self.yq[:-1, 1:]  # NE corner
        self.lat_bnds[..., 3] = self.yq[:-1, :-1]  # NW corner

        self.lon = (self.xt.values + 360) % 360
        self.xq = (self.xq + 360) % 360
        self.lon_bnds = np.zeros((*self.xt.shape, 4))
        self.lon_bnds[..., 0] = self.xq[1:, :-1]  # SW corner
        self.lon_bnds[..., 1] = self.xq[1:, 1:]  # SE corner
        self.lon_bnds[..., 2] = self.xq[:-1, 1:]  # NE corner
        self.lon_bnds[..., 3] = self.xq[:-1, :-1]  # NW corner

    def q_cells(self):
        self.lat = self.yq.values
        self.lat_bnds = np.zeros((*self.yt.shape, 4))
        self.lat_bnds[..., 0] = self.yt[1:, :-1]  # SW corner
        self.lat_bnds[..., 1] = self.yt[1:, 1:]  # SE corner
        self.lat_bnds[..., 2] = self.yt[:-1, 1:]  # NE corner
        self.lat_bnds[..., 3] = self.yt[:-1, :-1]  # NW corner

        self.lon = (self.xq.values + 360) % 360
        self.xt = (self.xt + 360) % 360
        self.lon_bnds = np.zeros((*self.xq.shape, 4))
        self.lon_bnds[..., 0] = self.xt[1:, :-1]  # SW corner
        self.lon_bnds[..., 1] = self.xt[1:, 1:]  # SE corner
        self.lon_bnds[..., 2] = self.xt[:-1, 1:]  # NE corner
        self.lon_bnds[..., 3] = self.xt[:-1, :-1]  # NW corner


grid_filepath = "/home/romain/PROJECTS/ACCESS-MOPPeR/grids/access-om2/input_20201102/mom_025deg/ocean_hgrid.nc"
om2_grid = Supergrid(grid_filepath)
om3_grid = Supergrid(
    "/home/romain/PROJECTS/ACCESS-MOPPeR/grids/access-om3/New_grid_input_files_025deg_75zlevels/ocean_hgrid.nc"
)
