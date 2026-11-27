"""Generate the ACCESS-ESM1.6 mrsofc resource from an archived UM restart."""

import argparse
from math import fsum
from pathlib import Path

import iris
import netCDF4
import numpy as np

STASH_FIELD_CAPACITY = "m01s00i041"
# UM7 2026.02.000, umbase_hg3/src/include/constant/soil_thick.h.
LAYER_THICKNESS_M = (0.022, 0.058, 0.154, 0.409, 1.085, 2.872)
# CABLE 2025.11.000, src/params/cable_phys_constants_mod.F90.
WATER_DENSITY_KG_M3 = 1000.0


def generate(restart, grid_template, output):
    field = iris.load_cube(
        str(restart), iris.AttributeConstraint(STASH=STASH_FIELD_CAPACITY)
    )
    if field.ndim != 2 or str(field.units) != "1":
        raise ValueError(
            f"Expected a 2D volumetric fraction for {STASH_FIELD_CAPACITY}"
        )

    fraction = np.ma.masked_invalid(field.data)
    if not np.all((fraction.compressed() >= 0) & (fraction.compressed() <= 1)):
        raise ValueError("Field capacity fraction is outside the range [0, 1]")
    capacity = fraction * fsum(LAYER_THICKNESS_M) * WATER_DENSITY_KG_M3

    with netCDF4.Dataset(grid_template) as template:
        for axis, name in enumerate(("lat", "lon")):
            coordinate = field.coord({"lat": "latitude", "lon": "longitude"}[name])
            if field.coord_dims(coordinate) != (axis,):
                raise ValueError(f"Unexpected dimension order for {name}")
            np.testing.assert_array_equal(coordinate.points, template[name][:])

        previous = template["mrsofc"]
        np.testing.assert_array_equal(
            np.ma.getmaskarray(capacity), np.ma.getmaskarray(previous[:])
        )

        with netCDF4.Dataset(output, "w", clobber=False) as resource:
            for name, dimension in template.dimensions.items():
                resource.createDimension(name, len(dimension))
            for name in ("lat", "lon", "lat_bnds", "lon_bnds", "mrsofc"):
                source = template[name]
                target = resource.createVariable(
                    name,
                    source.datatype,
                    source.dimensions,
                    fill_value=getattr(source, "_FillValue", None),
                )
                target.setncatts(
                    {
                        key: source.getncattr(key)
                        for key in source.ncattrs()
                        if key != "_FillValue"
                    }
                )
                target[:] = capacity if name == "mrsofc" else source[:]

            resource.source = (
                f"ACCESS-ESM1.6 UM restart {restart}, {STASH_FIELD_CAPACITY}"
            )
            resource.history = (
                "Integrated the CABLE volumetric soil field capacity over the six "
                "soil layers (4.6 m) and multiplied by water density (1000 kg m-3). "
                "Coordinates and bounds copied from the original ACCESS-ESM resource."
            )


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("restart", type=Path)
    parser.add_argument("grid_template", type=Path)
    parser.add_argument("output", type=Path)
    args = parser.parse_args()
    generate(args.restart, args.grid_template, args.output)


if __name__ == "__main__":
    main()
