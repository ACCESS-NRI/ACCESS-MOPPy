"""Check selected ACCESS-ESM1-6 overturning fields on Gadi."""

import argparse
import json
from hashlib import file_digest
from pathlib import Path

import numpy as np
from netCDF4 import Dataset, chartostring

NATIVE = Path("/g/data/p73/archive/CMIP7/ACCESS-ESM1-6/production")
PUBLISHED = Path("/g/data/im55/publications/MIP-DRS7/CMIP7")
SUPERGRID = Path("/g/data/xp65/public/apps/access_moppy_data/grids/mom1deg.nc")
MASK = (
    Path(__file__).resolve().parents[2]
    / "src/access_moppy/resources/fx.basin_ACCESS-ESM.nc"
)
ROWS = [200, 240, 250, 270, 285, 299]
BASINS = {
    "atlantic_arctic_ocean": (2, 4),
    "indian_pacific_ocean": (3, 5),
    "global_ocean": (),
}


def sha256(path):
    with path.open("rb") as stream:
        return file_digest(stream, "sha256").hexdigest()


def data(variable, key=slice(None)):
    return np.ma.filled(variable[key], np.nan)


def describe(variable, *attributes):
    return {
        "dimensions": variable.dimensions,
        "shape": variable.shape,
        **{name: variable.getncattr(name) for name in attributes},
    }


def available_i_transports(native_dir, year, vertical):
    names = (
        ("tx_trans", "tx_trans_gm", "tx_trans_submeso")
        if vertical == "ol"
        else ("tx_trans_rho", "tx_trans_rho_gm")
    )
    return {
        name: [
            path.name
            for path in sorted(native_dir.glob(f"ocean-3d-{name}-*-y_{year}.nc"))
        ]
        for name in names
    }


def comparison(expected, actual):
    valid = np.isfinite(expected) & np.isfinite(actual)
    abs_difference = np.abs(actual[valid] - expected[valid])
    return {
        "finite_pairs": int(valid.sum()),
        "mask_differences": int((np.isfinite(expected) != np.isfinite(actual)).sum()),
        "exact_values_and_masks": bool(
            np.array_equal(expected, actual, equal_nan=True)
        ),
        "max_abs_difference_kg_s": float(abs_difference.max()) if valid.any() else None,
    }


def sum_available(array, axis):
    return np.where(
        np.isfinite(array).any(axis=axis), np.nansum(array, axis=axis), np.nan
    )


def reconstruct(components, flags, codes):
    if codes:
        components = [
            np.where(np.isin(flags, codes), field, np.nan) for field in components
        ]
    resolved, *parameterized = [sum_available(field, axis=-1) for field in components]
    cumulative = np.where(np.isfinite(resolved), np.nancumsum(resolved, axis=0), np.nan)
    for field in parameterized:
        cumulative += field
    return cumulative - sum_available(resolved, axis=0)


def geometry(face_latitude, nominal_latitude, wet, flags):
    result = {}
    for basin, codes in BASINS.items():
        selected = wet & np.isin(flags, codes) if codes else wet
        result[basin] = [
            {
                "j": row,
                "nominal_latitude": float(nominal),
                "wet_faces": int(inside.sum()),
                "min_face_midpoint_latitude": float(lat[inside].min())
                if inside.any()
                else None,
                "max_face_midpoint_latitude": float(lat[inside].max())
                if inside.any()
                else None,
                "wet_faces_more_than_one_degree_away": int(
                    (inside & (np.abs(lat - nominal) > 1)).sum()
                ),
            }
            for row, nominal, lat, inside in zip(
                ROWS, nominal_latitude, face_latitude, selected
            )
        ]
    return result


def check_pair(
    native_dir, published_dir, year, version, vertical, flags, face_latitude
):
    components = []
    native_metadata = {}
    native_time_axes = {}
    names = (
        ("ty_trans", "ty_trans_gm", "ty_trans_submeso")
        if vertical == "ol"
        else ("ty_trans_rho", "ty_trans_rho_gm")
    )
    for name in names:
        path = native_dir / f"ocean-3d-{name}-1mon-mean-y_{year}.nc"
        with Dataset(path) as ds:
            variable = ds[name]
            time, _, lat, _ = variable.dimensions
            model_latitude = data(ds[lat])
            native_time_axes[name] = (
                float(data(ds[time], 0)),
                ds[time].units,
                ds[time].calendar,
            )
            native_metadata[name] = {
                "path": str(path),
                **describe(variable, "long_name", "units"),
            }
            components.append(data(variable, (0, slice(None), ROWS, slice(None))))

    published_arrays = {}
    published_time_axes = {}
    published_metadata = {}
    for name, horizontal, lat in (("msftm", "hyb", "lat"), ("msfty", "ht", "rlat")):
        branding = f"tavg-{vertical}-{horizontal}-sea"
        directory = published_dir / name / branding / "g116" / version
        (path,) = directory.glob(f"*_{year}01-*.nc")
        with Dataset(path) as ds:
            time = ds["time"]
            assert all(
                (data(time, 0), time.units, time.calendar) == native_axis
                for native_axis in native_time_axes.values()
            )
            assert chartostring(ds["basin"][:]).tolist() == list(BASINS)
            variable = ds[name]
            published_time_axes[name] = data(time)
            published_arrays[name] = data(variable)
            latitude = ds[lat]
            published_metadata[name] = {
                "path": str(path),
                "grid_label": ds.grid_label,
                "horizontal_label": ds.horizontal_label,
                "nominal_resolution": ds.nominal_resolution,
                "native_sample_time": float(data(time, 0)),
                "time_units": time.units,
                "calendar": time.calendar,
                "variable": describe(
                    variable, "standard_name", "units", "cell_methods"
                ),
                "latitude": {
                    **describe(latitude, "standard_name", "units", "bounds"),
                    "matches_native_coordinate": bool(
                        np.array_equal(data(latitude), model_latitude)
                    ),
                    "range": [float(model_latitude[0]), float(model_latitude[-1])],
                    "spacing_range": [
                        float(np.diff(model_latitude).min()),
                        float(np.diff(model_latitude).max()),
                    ],
                    "northern_bound": float(data(ds[latitude.bounds], (-1, -1))),
                },
            }

    # Index the full reduced arrays so NumPy keeps (basin, level, row) ordering.
    expected = np.stack(
        [reconstruct(components, flags, codes) for codes in BASINS.values()]
    )
    return {
        "native": native_metadata,
        "available_native_i_transport_files": available_i_transports(
            native_dir, year, vertical
        ),
        "published": published_metadata,
        "published_pair_full_file": {
            **comparison(published_arrays["msftm"], published_arrays["msfty"]),
            "exact_time_coordinates": bool(
                np.array_equal(
                    published_time_axes["msftm"], published_time_axes["msfty"]
                )
            ),
        },
        "native_reconstruction": {
            name: comparison(expected, field[0][..., ROWS])
            for name, field in published_arrays.items()
        },
        "geometry": geometry(
            face_latitude,
            model_latitude[ROWS],
            np.isfinite(components[0]).any(axis=0),
            flags,
        ),
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--mask", type=Path, default=MASK)
    args = parser.parse_args()

    with Dataset(args.mask) as ds:
        flags = data(ds["BASIN_MASK"]).squeeze()
        mask_latitude = data(ds["YT_OCEAN"])
        mask_longitude = data(ds["XT_OCEAN"])
    with Dataset(SUPERGRID) as ds:
        # The north face of tracer cell (j, i) is supergrid row 2j+2.
        supergrid_latitude = data(ds["y"])
    face_latitude = supergrid_latitude[2::2, 1::2][ROWS]

    result = {
        "basin_mask": {
            "sha256": sha256(args.mask),
            "shape": flags.shape,
            "code_counts": {
                int(code): int((flags == code).sum())
                for code in np.unique(flags[np.isfinite(flags)])
            },
            "masked_cells": int(np.isnan(flags).sum()),
            "basins": BASINS,
        },
        "supergrid": {"path": str(SUPERGRID), "sha256": sha256(SUPERGRID)},
        "cases": {},
    }
    for activity, experiment, run, year, version, verticals in (
        ("C4MIP", "1pctCO2-bgc", "1pctCO2-bgc-01", "0201", "v20261119", ("ol", "rho")),
        (
            "CMIP",
            "piControl",
            "Ndep2-PI-CNP-concentrations",
            "0101",
            "v20261117",
            ("rho",),
        ),
    ):
        native_dir = NATIVE / run / "output000/ocean"
        published_dir = (
            PUBLISHED
            / activity
            / "ACCESS-Consortium/ACCESS-ESM1-6"
            / experiment
            / "r1i1p1f1/glb/mon"
        )
        with Dataset(native_dir / "ocean-2d-geolat_t-fx.nc") as ds:
            longitude_difference = (
                mask_longitude - data(ds["xt_ocean"]) + 180
            ) % 360 - 180
            tracer_latitude = data(ds["geolat_t"])
            grid_checks = {
                "basin_mask_t_y_max_difference_degrees": float(
                    np.max(np.abs(mask_latitude - data(ds["yt_ocean"])))
                ),
                "basin_mask_t_x_max_difference_degrees": float(
                    np.max(np.abs(longitude_difference))
                ),
                "supergrid_t_y_max_difference_degrees": float(
                    np.nanmax(np.abs(supergrid_latitude[1::2, 1::2] - tracer_latitude))
                ),
            }
        with Dataset(native_dir / "ocean-2d-geolat_c-fx.nc") as ds:
            grid_checks["supergrid_corner_y_max_difference_degrees"] = float(
                np.nanmax(np.abs(supergrid_latitude[2::2, 2::2] - data(ds["geolat_c"])))
            )

        with Dataset(native_dir / f"ocean-2d-psiv-1mon-mean-y_{year}.nc") as ds:
            psiv = describe(ds["psiv"], "long_name", "units")
        case = {"grid_checks": grid_checks, "native_psiv": psiv}
        for vertical in verticals:
            case[vertical] = check_pair(
                native_dir,
                published_dir,
                year,
                version,
                vertical,
                flags[ROWS],
                face_latitude,
            )
        result["cases"][experiment] = case

    print(json.dumps(result, indent=2, allow_nan=False))


if __name__ == "__main__":
    main()
