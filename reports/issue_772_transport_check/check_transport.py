"""Compare an APP4-compatible prototype with real Gadi CICE data.

Usage: python check_transport.py /tmp/issue772_gadi_sample.npz /tmp/issue772-app_functions.py
This validates arithmetic and characterizes approximations, not CMOR compliance.
"""

import csv
import hashlib
import json
import sys
from pathlib import Path
from types import SimpleNamespace

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np
import xarray as xr

HERE = Path(__file__).resolve().parent
SAMPLE = Path(sys.argv[1])
APP_SOURCE = Path(sys.argv[2])
z = np.load(SAMPLE)
metadata = json.loads(str(z["metadata_json"]))
LINES = {
    "fram_strait": [("x", 267, 267, 279, 279), ("y", 268, 284, 278, 278)],
    "canadian_archipelago": [("y", 206, 212, 285, 285), ("x", 235, 235, 287, 288)],
    "barents_opening": [("y", 292, 300, 271, 271), ("x", 300, 300, 260, 271)],
    "bering_strait": [("y", 110, 111, 246, 246)],
}
HUE = (z["app_grid_hue"] / 100).astype("float32")
HUN = (z["app_grid_hun"] / 100).astype("float32")


def sums(tx, ty, j_offset=0, i_offset=0):
    result = []
    for segments in LINES.values():
        result.append(
            sum(
                np.nansum(
                    {"x": tx, "y": ty}[component][
                        ...,
                        j0 - j_offset : j1 - j_offset + 1,
                        i0 - i_offset : i1 - i_offset + 1,
                    ],
                    axis=(-2, -1),
                )
                for component, i0, i1, j0, j1 in segments
            )
        )
    return np.stack(result, axis=-1)


def prototype(q, u, v):
    def field(a):
        return xr.DataArray(a, dims=("time", "nj", "ni"))

    tx = field(q) * field(u) * xr.DataArray(HUE, dims=("nj", "ni"))
    ty = field(q) * field(v) * xr.DataArray(HUN, dims=("nj", "ni"))
    result = []
    for segments in LINES.values():
        result.append(
            sum(
                {"x": tx, "y": ty}[component]
                .isel(ni=slice(i0, i1 + 1), nj=slice(j0, j1 + 1))
                .sum(("nj", "ni"), skipna=True)
                for component, i0, i1, j0, j1 in segments
            )
        )
    return (
        xr.concat(result, xr.IndexVariable("line", list(LINES)))
        .transpose("time", "line")
        .values
    )


def write_csv(name, rows):
    with (HERE / name).open("w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=list(rows[0]), lineterminator="\n")
        writer.writeheader()
        writer.writerows(rows)


def stats(a, b):
    valid = np.isfinite(a) & np.isfinite(b)
    diff = abs(a[valid] - b[valid])
    return {
        "max_abs_difference": float(diff.max()),
        "max_abs_difference_divided_by_peak": float(
            diff.max() / max(abs(b[valid]).max(), 1e-30)
        ),
        "identical_missing_mask": bool(np.array_equal(np.isnan(a), np.isnan(b))),
    }


source = APP_SOURCE.read_text()
namespace = {"np": np, "ancillary_path": ""}
grid = SimpleNamespace(
    variables={"hue": z["app_grid_hue"], "hun": z["app_grid_hun"]}, close=lambda: None
)
namespace["cdms2"] = SimpleNamespace(open=lambda *args: grid)
for name in [
    "geticeTransportLines",
    "transAcrossLine",
    "iceareaTransport",
    "icearealineTransports",
    "iceTransport",
    "icelineTransports",
]:
    body = source.split(f"def {name}(", 1)[1].split("\ndef ", 1)[0]
    exec(f"def {name}(" + body, namespace)

u, v = z["p73_siu"], z["p73_siv"]
q, mass, snow = z["p73_aice"], z["p73_simass"], z["p73_sisnmass"]
masked = np.ma.masked_invalid
area = prototype(q, u, v)
ice = prototype(mass, u, v)
snow_flux = prototype(snow, u, v)
reference_area = namespace["icearealineTransports"](*map(masked, (q, u, v)))
# simass/900 is only an adapter for APP4's hard-coded 900, not a physical
# density assumption for ESM1-6. Production should use simass directly.
reference_ice = namespace["icelineTransports"](*map(masked, (mass / 900, u, v)))
np.testing.assert_allclose(area, reference_area, rtol=2e-6, atol=1e-3)
np.testing.assert_allclose(ice, reference_ice, rtol=2e-6, atol=2.0)
native_combined = sums(z["p73_sidmasstranx"], z["p73_sidmasstrany"])
summary = {
    "app4_area_agreement": stats(area, reference_area),
    "app4_ice_agreement": stats(ice, reference_ice),
    "archive_agreement": {},
    "grid_comparison": {},
    "annual_mean": {},
    "daily_comparison": [],
}
for name in [
    "aice",
    "siu",
    "siv",
    "simass",
    "sisnmass",
    "sivol",
    "sidmasstranx",
    "sidmasstrany",
]:
    summary["archive_agreement"][name] = stats(z["jq44_" + name], z["p73_" + name])
for name in ["htn", "hte", "ulat", "ulon", "angle"]:
    summary["grid_comparison"]["app_vs_model_" + name] = stats(
        z["app_grid_" + name], z["model_grid_" + name]
    )
for name in ["tlat", "tlon", "ulat", "ulon"]:
    diff = z["app_grid_" + name] * 180 / np.pi - z["p73_" + name.upper()]
    if name.endswith("lon"):
        diff = (diff + 180) % 360 - 180
    summary["grid_comparison"]["app_vs_history_" + name + "_max_abs_degrees"] = float(
        np.nanmax(abs(diff))
    )
volume = z["p73_sivol"]
rho = mass[volume > 0.01] / volume[volume > 0.01]
summary["inferred_ice_density_kg_m3"] = {
    "min": float(rho.min()),
    "median": float(np.median(rho)),
    "max": float(rho.max()),
}

# Explicit sensitivity experiment, not an asserted physical correction:
# average the four T-cell values surrounding each NE-corner velocity point,
# with land zero. There is no polar wrap in any selected line.
q0 = np.nan_to_num(q, nan=0.0)
q_u = (
    q0
    + np.roll(q0, -1, -1)
    + np.roll(q0, -1, -2)
    + np.roll(np.roll(q0, -1, -1), -1, -2)
) / 4
area_staggered = prototype(q_u, u, v)
days = np.diff(z["p73_time_bounds"], axis=-1).ravel()
monthly_rows = []
for i, line in enumerate(LINES):
    summary["annual_mean"][line] = {
        "area_m2_s": float(np.average(area[:, i], weights=days)),
        "ice_kg_s": float(np.average(ice[:, i], weights=days)),
        "snow_kg_s": float(np.average(snow_flux[:, i], weights=days)),
        "native_combined_kg_s": float(np.average(native_combined[:, i], weights=days)),
        "reconstructed_combined_kg_s": float(
            np.average(ice[:, i] + snow_flux[:, i], weights=days)
        ),
        "area_four_T_cell_sensitivity_m2_s": float(
            np.average(area_staggered[:, i], weights=days)
        ),
    }
    annual = summary["annual_mean"][line]
    annual["combined_difference_percent_of_abs_native"] = (
        100
        * (annual["reconstructed_combined_kg_s"] - annual["native_combined_kg_s"])
        / abs(annual["native_combined_kg_s"])
    )
    for month in range(12):
        monthly_rows.append(
            {
                "month": f"1850-{month + 1:02d}",
                "line": line,
                "area_m2_s": float(area[month, i]),
                "ice_kg_s": float(ice[month, i]),
                "snow_kg_s": float(snow_flux[month, i]),
                "native_combined_kg_s": float(native_combined[month, i]),
                "four_T_cell_area_m2_s": float(area_staggered[month, i]),
            }
        )
write_csv("monthly_transports.csv", monthly_rows)

daily_rows = []
for month in [1, 9]:
    dq, du, dv = [z[f"daily{month:02d}_{n}"] for n in ["aice", "siu", "siv"]]
    hue, hun = HUE[244:291, 108:304], HUN[244:291, 108:304]
    daily = sums(dq * du * hue, dq * dv * hun, 244, 108)
    weights = np.diff(z[f"daily{month:02d}_time_bounds"], axis=-1).ravel()
    mean_daily = np.average(daily, axis=0, weights=weights)
    for i, line in enumerate(LINES):
        monthly = float(area[month - 1, i])
        daily_mean = float(mean_daily[i])
        row = {
            "month": f"1850-{month:02d}",
            "line": line,
            "monthly_input_m2_s": monthly,
            "mean_daily_input_m2_s": daily_mean,
            "monthly_minus_daily_m2_s": monthly - daily_mean,
            "difference_percent_of_abs_daily": 100
            * (monthly - daily_mean)
            / abs(daily_mean)
            if abs(daily_mean) > 1e-6
            else None,
        }
        daily_rows.append(row)
summary["daily_comparison"] = daily_rows
write_csv("daily_vs_monthly.csv", daily_rows)

geography = []
for line, segments in LINES.items():
    for component, i0, i1, j0, j1 in segments:
        for j in range(j0, j1 + 1):
            for i in range(i0, i1 + 1):
                geography.append(
                    {
                        "line": line,
                        "component": component,
                        "i": i,
                        "j": j,
                        "ULAT": float(z["p73_ULAT"][j, i]),
                        "ULON": float((z["p73_ULON"][j, i] + 180) % 360 - 180),
                        "TLAT": float(z["p73_TLAT"][j, i]),
                        "TLON": float((z["p73_TLON"][j, i] + 180) % 360 - 180),
                        "T_wet": int(z["p73_tmask"][j, i]),
                        "edge_m": float({"x": HUE, "y": HUN}[component][j, i]),
                    }
                )
write_csv("strait_cells.csv", geography)

metadata.update(
    {
        "sample_sha256": hashlib.sha256(SAMPLE.read_bytes()).hexdigest(),
        "app4_source_url": "https://github.com/ACCESS-Community-Hub/APP4/blob/master/subroutines/app_functions.py",
        "app4_source_sha256": hashlib.sha256(APP_SOURCE.read_bytes()).hexdigest(),
        "sample_file": str(SAMPLE),
        "strait_segments": LINES,
        "cice5_release": "2026.01.000",
        "cice5_commit": "cf5df9d4d26265dc5c79e558e5a67834b51fd38d",
        "cice5_history_source_url": "https://github.com/ACCESS-NRI/cice5/blob/cf5df9d4d26265dc5c79e558e5a67834b51fd38d/source/ice_history.F90",
        "cice5_executable": "/g/data/vk83/apps/spack/1.1/restricted/ukmo/release/linux-x86_64_v4/cice5-2026.01.000-y5g2qcrhzizcfpgshvkpm3xdxwy2th4h/bin/cice_access_360x300_30x300.exe",
        "cice5_executable_manifest_md5": "2bac2d6ce23248e4be497f630d3d0ad2",
    }
)
(HERE / "provenance.json").write_text(json.dumps(metadata, indent=2) + "\n")
(HERE / "results.json").write_text(json.dumps(summary, indent=2) + "\n")

fig, axes = plt.subplots(2, 2, figsize=(11, 7), sharex=True, layout="constrained")
for i, (ax, line) in enumerate(zip(axes.flat, LINES)):
    ax.plot(
        range(1, 13), area[:, i], marker="o", label="APP4-compatible, monthly inputs"
    )
    ax.plot(
        range(1, 13),
        area_staggered[:, i],
        ls="--",
        label="Four T-cell mean sensitivity",
    )
    rows = [r for r in daily_rows if r["line"] == line]
    ax.scatter(
        [int(r["month"][-2:]) for r in rows],
        [r["mean_daily_input_m2_s"] for r in rows],
        marker="x",
        s=80,
        c="black",
        label="Mean of daily reconstructions",
        zorder=5,
    )
    ax.axhline(0, color="0.5", lw=0.7)
    ax.grid(alpha=0.2)
    ax.set_title(line.replace("_", " ").title())
    ax.set_ylabel("Signed area transport (m² s⁻¹)")
    ax.set_xticks([1, 3, 6, 9, 12])
    ax.set_xlabel("Month in 1850")
handles, labels = axes.flat[0].get_legend_handles_labels()
fig.legend(handles, labels, loc="outside lower center", ncol=1, frameon=False)
fig.suptitle(
    "ACCESS-ESM1-6 historical r1i1p1f1 — reconstruction sensitivity\nSigns follow APP4 segment sums; daily reconstruction is not a ground-truth flux",
    fontsize=12,
)
fig.savefig(HERE / "area_transport_comparison.png", dpi=160)
plt.close(fig)

fig, axes = plt.subplots(2, 2, figsize=(10, 8), layout="constrained")
limits = [(-20, 20, 76, 84), (-95, -60, 70, 84), (10, 35, 68, 80), (-175, -160, 62, 69)]
lon = (z["p73_TLON"] + 180) % 360 - 180
lat = z["p73_TLAT"]
wet = z["p73_tmask"] > 0
for ax, line, (x0, x1, y0, y1) in zip(axes.flat, LINES, limits):
    window = (lon > x0) & (lon < x1) & (lat > y0) & (lat < y1)
    for mask, color, label in [
        (wet, "#cce5f5", "Ocean T cell"),
        (~wet, "#aaaaaa", "Land T cell"),
    ]:
        selected = window & mask
        ax.scatter(lon[selected], lat[selected], s=12, c=color, marker="s", label=label)
    for component, color in [("x", "#c43d3d"), ("y", "#442288")]:
        cells = [
            r for r in geography if r["line"] == line and r["component"] == component
        ]
        ax.scatter(
            [r["ULON"] for r in cells],
            [r["ULAT"] for r in cells],
            s=25,
            c=color,
            marker="o",
            label=component + " component",
            zorder=5,
        )
    ax.set_xlim(x0, x1)
    ax.set_ylim(y0, y1)
    ax.grid(alpha=0.2)
    ax.set_title(line.replace("_", " ").title())
    ax.set_xlabel("Longitude (degrees east)")
    ax.set_ylabel("Latitude (degrees north)")
handles, labels = axes.flat[0].get_legend_handles_labels()
fig.legend(handles, labels, loc="outside lower center", ncol=4, frameon=False)
fig.suptitle(
    "APP4 strait indices on the sampled ACCESS-ESM1-6 grid\nT-cell land mask and selected velocity points; geographic-coordinate plots",
    fontsize=12,
)
fig.savefig(HERE / "strait_grid_locations.png", dpi=160)
plt.close(fig)

print(json.dumps(summary, indent=2))
