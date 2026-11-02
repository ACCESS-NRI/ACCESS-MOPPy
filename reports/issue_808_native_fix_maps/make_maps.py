"""Plot actual #808 mapping changes on native ACCESS-ESM1.6 historical data.

Run from this evidence checkout with the repository development environment:
  PYTHONPATH=src python reports/issue_808_native_fix_maps/make_maps.py \
    --native-root /path/to/esm-historical \
    --corrected-root /path/to/native-integration-outputs

The corrected output root must contain the five 195101-195112 CMIP7 files
written by tests/integration/test_baseline_land_means.py at FIXED_COMMIT.
No source NetCDF data are modified or uploaded.
"""

import argparse
import hashlib
import json
import subprocess
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np
import xarray as xr
from matplotlib.colors import Normalize
from matplotlib.ticker import MaxNLocator

from access_moppy.derivations import evaluate_expression
from access_moppy.utilities import load_model_mappings

BASE_COMMIT = "9a428b1d4b82768a98bad374e5415339e967c21b"
FIXED_COMMIT = "f22939099b15dcceb930e580820f1bd73d8ff808"
FIELDS = {
    "cSoil": ("Soil carbon", "kg C m$^{-2}$", 1.0, 30.0),
    "cVeg": ("Vegetation carbon", "kg C m$^{-2}$", 1.0, 40.0),
    "lai": ("Leaf area index", "m$^2$ leaf m$^{-2}$ ground", 1.0, 7.0),
    "fBNF": (
        "Biological nitrogen fixation",
        "$10^{-10}$ kg N m$^{-2}$ s$^{-1}$",
        1e10,
        4.5,
    ),
    "snc": ("Snow-covered land", "%", 1.0, 100.0),
}


def map_longitudes(data):
    return data.assign_coords(lon=((data.lon + 180) % 360) - 180).sortby("lon")


def longitude_label(value):
    return f"{abs(int(value))}°{'W' if value < 0 else 'E'}" if value else "0°"


def latitude_label(value):
    return f"{abs(int(value))}°{'S' if value < 0 else 'N'}" if value else "0°"


def map_panel(ax, values, landfrac, extent, norm, cmap):
    ax.set_facecolor("#e6edf3")
    artist = ax.pcolormesh(
        values.lon,
        values.lat,
        values,
        shading="nearest",
        cmap=cmap,
        norm=norm,
        rasterized=True,
    )
    # Outline the model's own land mask, without external geographic data.
    ax.contour(
        landfrac.lon,
        landfrac.lat,
        (landfrac > 0).astype(float),
        levels=[0.5],
        colors="#4a5564",
        linewidths=0.42,
        alpha=0.8,
    )
    ax.set_xlim(extent[0], extent[1])
    ax.set_ylim(extent[2], extent[3])
    ax.set_aspect("equal", adjustable="box")
    if extent[0] == -180:
        xticks = [-120, -60, 0, 60, 120]
        yticks = [-60, -30, 0, 30, 60]
    elif extent[2] >= 40:
        xticks = [-120, -100, -80, -60]
        yticks = [40, 50, 60, 70, 80]
    else:
        xticks = [100, 115, 130, 145]
        yticks = [-10, 0, 10, 20, 30]
    ax.set_xticks(xticks, [longitude_label(t) for t in xticks])
    ax.set_yticks(yticks, [latitude_label(t) for t in yticks])
    ax.tick_params(labelsize=8.5, length=2, color="#94a3b8")
    ax.grid(alpha=0.2, linewidth=0.5, color="#64748b")
    for spine in ax.spines.values():
        spine.set_edgecolor("#b5c0cb")
        spine.set_linewidth(0.7)
    return artist


def make_figure(name, before, after, landfrac, outdir):
    title, units, scale, upper = FIELDS[name]
    a, b = before * scale, after * scale
    difference = b - a
    maximum_difference = float(difference.max())
    diff_upper = MaxNLocator(nbins=5).tick_values(0, maximum_difference)[-1]
    if diff_upper <= 0:
        diff_upper = 1.0
    field_norm = Normalize(vmin=0, vmax=upper)
    diff_norm = Normalize(vmin=0, vmax=diff_upper)
    fig = plt.figure(figsize=(15.6, 10.0), facecolor="white")
    grid = fig.add_gridspec(
        2,
        3,
        left=0.055,
        right=0.98,
        top=0.83,
        bottom=0.23,
        hspace=0.27,
        wspace=0.15,
    )
    axes = np.array([[fig.add_subplot(grid[r, c]) for c in range(3)] for r in range(2)])
    global_extent = (-180, 180, -90, 90)
    if name == "snc":
        region_extent = (-130, -50, 40, 80)
        region_name = "North America"
    else:
        region_extent = (95, 155, -15, 35)
        region_name = "Southeast Asia"
    for row, extent in enumerate((global_extent, region_extent)):
        artist = map_panel(axes[row, 0], a, landfrac, extent, field_norm, "YlGnBu")
        map_panel(axes[row, 1], b, landfrac, extent, field_norm, "YlGnBu")
        diff_artist = map_panel(
            axes[row, 2], difference, landfrac, extent, diff_norm, "Reds"
        )
    for ax, label in zip(
        axes[0],
        (
            "A  Before: original recipe",
            "B  After: PR #810 recipe",
            "C  Increase: B − A",
        ),
    ):
        ax.set_title(label, loc="left", fontsize=11.5, weight="semibold", pad=12)
    for col, label in enumerate(("Before", "After", "Difference")):
        axes[1, col].set_title(
            f"{region_name}  ·  {label}", loc="left", fontsize=10, pad=9
        )

    # Separate color scales for the quantities and the difference; both rows
    # use the same limits. No regional rescaling or percentile clipping.
    cb1 = fig.colorbar(
        artist, cax=fig.add_axes([0.095, 0.174, 0.525, 0.017]), orientation="horizontal"
    )
    cb1.set_label(f"A and B share the same scale  |  {units}", fontsize=10, labelpad=5)
    cb1.ax.tick_params(labelsize=9)
    cb2 = fig.colorbar(
        diff_artist,
        cax=fig.add_axes([0.715, 0.174, 0.23, 0.017]),
        orientation="horizontal",
    )
    diff_units = "percentage points" if name == "snc" else units
    cb2.set_label(f"Increase  |  {diff_units}", fontsize=10, labelpad=5)
    cb2.ax.tick_params(labelsize=9)

    fig.text(
        0.055, 0.955, f"{name}  |  {title}", fontsize=23, weight="bold", color="#182b40"
    )
    fig.text(
        0.055,
        0.919,
        "Coastal normalization fix applied to native model output",
        fontsize=14,
        color="#334e68",
    )
    fig.text(
        0.055,
        0.883,
        "ACCESS-ESM1-6  ·  historical native samples  ·  1951 annual mean  ·  identical inputs before and after",
        fontsize=10.5,
        color="#526475",
    )
    fig.text(
        0.055,
        0.082,
        "A and B evaluate the actual recipes on the same native tiles. B agrees with corrected CMOR output (rtol = 1e−6).",
        fontsize=10.5,
        color="#334e68",
    )
    fig.text(
        0.055,
        0.055,
        "Monthly means weighted by duration. Ocean masked in both views; native grid cells and land-mask outlines retained.",
        fontsize=10.5,
        color="#334e68",
    )
    fig.text(
        0.055,
        0.027,
        "Historical validation only: this is not the published piControl run. Publication/erratum work remains open.  •  #808 / #810",
        fontsize=10,
        color="#7f3b21",
    )
    output = outdir / f"{name}_native_fix.png"
    fig.savefig(
        output,
        dpi=180,
        facecolor="white",
        metadata={
            "Description": "Issue #808: old and fixed recipes on identical historical native inputs; not published piControl data."
        },
    )
    plt.close(fig)
    return {
        "figure": output.name,
        "display_scale_factor": scale,
        "display_units": units,
        "field_color_limits": [0, upper],
        "difference_color_limits": [0, float(diff_upper)],
        "maximum_difference_original_units": maximum_difference / scale,
        "regional_extent": region_extent,
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--native-root", type=Path, required=True)
    parser.add_argument("--corrected-root", type=Path, required=True)
    parser.add_argument(
        "--output-dir", type=Path, default=Path(__file__).resolve().parent
    )
    args = parser.parse_args()
    args.output_dir.mkdir(parents=True, exist_ok=True)
    mapping_path = "src/access_moppy/mappings/ACCESS-ESM1-6_mappings.json"
    baseline = json.loads(
        subprocess.check_output(["git", "show", f"{BASE_COMMIT}:{mapping_path}"])
    )
    assert (
        subprocess.check_output(["git", "show", f"{FIXED_COMMIT}:{mapping_path}"])
        == Path(mapping_path).read_bytes()
    )
    assert not subprocess.check_output(
        [
            "git",
            "diff",
            BASE_COMMIT,
            FIXED_COMMIT,
            "--",
            "src/access_moppy/derivations/calc_land.py",
        ]
    )
    mappings = {
        name: load_model_mappings(
            f"{'landIce' if name == 'snc' else 'land'}.{name}.tavg-u-hxy-lnd.mon.glb",
            "ACCESS-ESM1-6",
        )[name]
        for name in FIELDS
    }
    wanted = sorted(
        {field for mapping in mappings.values() for field in mapping["model_variables"]}
    )
    before = {name: [] for name in FIELDS}
    after = {name: [] for name in FIELDS}
    weights = []
    sources = []
    fraction = None
    for month in range(1, 13):
        paths = list(
            args.native_root.glob(
                f"output*/atmosphere/netCDF/*pa-1951{month:02}_mon.nc"
            )
        )
        assert len(paths) == 1, paths
        path = paths[0]
        with xr.open_dataset(path, engine="netcdf4", decode_times=False) as raw:
            context = {
                name: raw[name]
                .isel(time=0, drop=True)
                .reset_coords(drop=True)
                .astype("float64")
                .load()
                for name in wanted
            }
            bounds = raw[raw.time.attrs["bounds"]].values[0]
            assert raw.time.attrs["units"].startswith("days since")
            weights.append(float(bounds[1] - bounds[0]))
            if fraction is None:
                fraction = context["fld_s03i395"]
                latb, lonb = (
                    np.deg2rad(raw.lat_bnds.astype("float64")),
                    np.deg2rad(raw.lon_bnds.astype("float64")),
                )
                area = (
                    6371000.0**2
                    * abs(np.sin(latb.isel(bnds=1)) - np.sin(latb.isel(bnds=0)))
                    * abs(lonb.isel(bnds=1) - lonb.isel(bnds=0))
                ).load()
            else:
                xr.testing.assert_equal(fraction, context["fld_s03i395"])
            sources.append(
                {
                    "file": str(path),
                    "size_bytes": path.stat().st_size,
                    "time_bounds": bounds.tolist(),
                    "time_units": raw.time.attrs["units"],
                    "float64_input_field_sha256": {
                        name: hashlib.sha256(context[name].values.tobytes()).hexdigest()
                        for name in wanted
                    },
                }
            )
        tile_dim = next(d for d in context["fld_s03i317"].dims if "pseudo_level" in d)
        np.testing.assert_allclose(
            context["fld_s03i317"].sum(tile_dim).values[fraction.values > 0],
            1,
            atol=2e-7,
        )
        for name, mapping in mappings.items():
            old = baseline["landIce" if name == "snc" else "land"][name]
            a = evaluate_expression(old["calculation"], context).where(fraction > 0)
            b = evaluate_expression(mapping["calculation"], context)
            np.testing.assert_allclose(
                b * fraction, a, rtol=1e-12, atol=0, equal_nan=True
            )
            np.testing.assert_array_equal(
                a.where(fraction == 1), b.where(fraction == 1)
            )
            assert np.isnan(b.values[fraction.values == 0]).all()
            before[name].append(a.expand_dims(month=[month]))
            after[name].append(b.expand_dims(month=[month]))

    np.testing.assert_array_equal(
        weights, [31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31]
    )
    weights = xr.DataArray(weights, dims="month", coords={"month": range(1, 13)})
    report = {
        "issue": "https://github.com/ACCESS-NRI/ACCESS-MOPPy/issues/808",
        "pull_request": "https://github.com/ACCESS-NRI/ACCESS-MOPPy/pull/810",
        "before_commit": BASE_COMMIT,
        "after_commit": FIXED_COMMIT,
        "model": "ACCESS-ESM1-6",
        "native_experiment": "historical",
        "period": "1951-01/1951-12",
        "method": "Evaluate original and corrected CMIP7 mapping recipes on identical native inputs, then duration-weight annual means. Verify every corrected monthly value against written CMOR output at rtol=1e-6, atol=0. Before ocean values masked only for visual comparison.",
        "limitations": "Not matching native piControl data; published figures remain inferred. Unit conversions and monthly snow-presence approximation unchanged. Inventory, formal erratum and production reprocessing remain outstanding.",
        "month_weights_days": weights.values.tolist(),
        "sources": sources,
        "integral_method": "sum(annual_mean * areacella * native_land_fraction); areacella from native bounds, radius 6371000 m. For snc, divide by 100 to obtain snow area.",
        "coastal_cells": int(((fraction > 0) & (fraction < 1)).sum()),
        "variables": {},
    }
    plt.rcParams.update({"font.family": "DejaVu Sans", "axes.unicode_minus": True})
    for name in FIELDS:
        a = xr.concat(before[name], "month")
        b = xr.concat(after[name], "month")
        paths = sorted(
            {
                p.resolve()
                for p in args.corrected_root.rglob(
                    f"{name}_tavg-u-hxy-lnd_mon_glb_g115_ACCESS-ESM1-6_historical_r1i1p1f1_195101-195112.nc"
                )
            }
        )
        assert len(paths) == 1, paths
        with xr.open_dataset(paths[0], engine="netcdf4") as output:
            np.testing.assert_array_equal(output.lat, b.lat)
            np.testing.assert_array_equal(output.lon, b.lon)
            np.testing.assert_array_equal(output.time.dt.month, b.month)
            np.testing.assert_allclose(
                output[name].values, b.values, rtol=1e-6, atol=0, equal_nan=True
            )
            assert "area: mean where land" in output[name].attrs["cell_methods"]
        a_mean = a.weighted(weights).mean("month")
        b_mean = b.weighted(weights).mean("month")
        metadata = make_figure(
            name,
            map_longitudes(a_mean),
            map_longitudes(b_mean),
            map_longitudes(fraction),
            args.output_dir,
        )
        old_total = float((a_mean * area * fraction).sum())
        new_total = float((b_mean * area * fraction).sum())
        report["variables"][name] = {
            **metadata,
            "units": mappings[name]["units"],
            "corrected_output": str(paths[0]),
            "corrected_output_sha256": hashlib.sha256(
                paths[0].read_bytes()
            ).hexdigest(),
            "before_land_area_integral_in_mapped_units_times_m2": old_total,
            "after_land_area_integral_in_mapped_units_times_m2": new_total,
            "old_integral_bias_percent": 100 * (old_total / new_total - 1),
            "after_min_max": [float(b_mean.min()), float(b_mean.max())],
            "figure_sha256": hashlib.sha256(
                (args.output_dir / metadata["figure"]).read_bytes()
            ).hexdigest(),
        }
        print(f"Created {metadata['figure']}; verified all 12 corrected months")
    (args.output_dir / "provenance.json").write_text(
        json.dumps(report, indent=2) + "\n"
    )


if __name__ == "__main__":
    main()
