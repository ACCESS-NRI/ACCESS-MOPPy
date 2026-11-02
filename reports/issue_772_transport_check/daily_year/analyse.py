"""Compare a full year of daily/monthly APP4-style ice-mass reconstructions.

Usage: python analyse.py /tmp/issue772-daily-year-1850.npz
This is a temporal-averaging sensitivity experiment, not validation against
true ice flux. Low-concentration masked contributions are explicitly audited.
"""

import csv
import hashlib
import json
import sys
from pathlib import Path

import cftime
import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np

HERE = Path(__file__).resolve().parent
SAMPLE = Path(sys.argv[1])
# daily_year -> issue report -> reports -> repository
GRID = (
    HERE.parents[2]
    / "src/access_moppy/resources/seaice/ACCESS-ESM1_transport_lines.json"
)
z = np.load(SAMPLE)
metadata = json.loads(str(z["metadata_json"]))
grid = json.loads(GRID.read_text())
points = [dict(p, line=line["name"]) for line in grid["lines"] for p in line["points"]]
assert metadata["points"] == [
    {k: p[k] for k in ("line", "component", "j", "i")} for p in points
]
names = [line["name"] for line in grid["lines"]]
groups = [np.array([p["line"] == name for p in points]) for name in names]
edge = np.array([p["edge_length"] for p in points], dtype="float64")
x_component = np.array([p["component"] == "x" for p in points])
RHO = 917.0
MASK_CUTOFF = 0.001

for name in ("TLAT", "TLON", "ULAT", "ULON"):
    difference = z[name] - np.array([p[name] for p in points])
    if name.endswith("LON"):
        difference = (difference + 180) % 360 - 180
    assert np.all(np.abs(difference) < 1e-4), name
assert np.all(z["tmask"] == 1)


def values(name):
    return z[name].astype("float64")


def weighted(a, weights):
    return np.average(a, axis=0, weights=weights)


def totals(contributions):
    return np.stack(
        [contributions[..., group].sum(axis=-1) for group in groups], axis=-1
    )


def csv_file(name, rows):
    with (HERE / name).open("w", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(rows[0]), lineterminator="\n")
        writer.writeheader()
        writer.writerows(rows)


durations = {}
dates = {}
for prefix in ("daily", "monthly"):
    attrs = metadata["attributes"][prefix + "_time"]
    bounds = values(prefix + "_time_bounds")
    np.testing.assert_array_equal(bounds[1:, 0], bounds[:-1, 1])
    durations[prefix] = np.diff(bounds, axis=1).ravel()
    dates[prefix] = cftime.num2date(
        bounds[:, 0], attrs["units"], calendar=attrs["calendar"]
    )
    assert all(date.year == 1850 for date in dates[prefix])
    np.testing.assert_array_equal(
        [date.month for date in dates[prefix]], z[prefix + "_month"]
    )
    for name, units in (
        ("aice", "1"),
        ("sithick", "m"),
        ("siu", "m/s"),
        ("siv", "m/s"),
    ):
        assert metadata["attributes"][prefix + "_" + name]["units"] == units
    np.testing.assert_allclose(
        z[prefix + "_siconc"], z[prefix + "_aice"] * 100, atol=1e-5
    )
assert len(dates["daily"]) == 365
np.testing.assert_array_equal(durations["daily"], np.ones(365))
np.testing.assert_array_equal(
    z["daily_time_bounds"][[0, -1], [0, 1]], z["monthly_time_bounds"][[0, -1], [0, 1]]
)
assert durations["monthly"].sum() == 365

data = {}
for prefix in ("daily", "monthly"):
    a = values(prefix + "_aice")
    h = values(prefix + "_sithick")
    velocity = np.where(x_component, values(prefix + "_siu"), values(prefix + "_siv"))
    assert np.isfinite(a).all() and np.all((a >= 0) & (a <= 1))
    np.testing.assert_array_equal(np.isfinite(h), np.isfinite(velocity))
    mass = (
        np.where(a == 0, 0, RHO * a * h)
        if prefix == "daily"
        else values("monthly_simass")
    )
    flux = np.where(mass == 0, 0, mass * velocity * edge)
    masked = ~np.isfinite(flux)
    # Fail rather than silently zero any missing contribution outside the
    # observed ice-presence censoring regime. This does not prove zero flux.
    assert np.all(a[masked] <= MASK_CUTOFF)
    flux_zero = np.where(masked, 0, flux)
    data[prefix] = {
        "a": a,
        "h": h,
        "v": velocity,
        "mass": mass,
        "flux": flux,
        "flux_zero": flux_zero,
        "masked": masked,
        "line": totals(flux_zero),
        "area_line": totals(np.where(a == 0, 0, a * np.nan_to_num(velocity) * edge)),
    }

# Independently verify rho and the monthly aice/thickness identity, where the
# intensive diagnostic exists; no second concentration factor is used on simass.
mass = values("monthly_simass")
volume = values("monthly_sivol")
np.testing.assert_allclose(mass, volume * RHO, rtol=2e-7, atol=5e-4)
reconstructed_mass = np.where(
    data["monthly"]["a"] == 0, 0, RHO * data["monthly"]["a"] * data["monthly"]["h"]
)
np.testing.assert_allclose(
    reconstructed_mass[np.isfinite(reconstructed_mass)],
    mass[np.isfinite(reconstructed_mass)],
    rtol=3e-7,
    atol=5e-4,
)

# Compare independently extracted points with the earlier full-grid/window
# sample if it is still available locally.
old_path = Path("/tmp/issue772_gadi_sample.npz")
old_verified = False
if old_path.exists():
    old = np.load(old_path)
    jj = np.array([p["j"] for p in points])
    ii = np.array([p["i"] for p in points])
    for name in ("aice", "sithick", "siu", "siv", "simass"):
        np.testing.assert_array_equal(
            z["monthly_" + name], old["p73_" + name][:, jj, ii]
        )
    for month in (1, 9):
        for name in ("aice", "sithick", "siu", "siv"):
            np.testing.assert_array_equal(
                z["daily_" + name][z["daily_month"] == month],
                old[f"daily{month:02d}_" + name][:, jj - 244, ii - 108],
            )
    old_verified = True

monthly_rows, coverage_rows, daily_rows = [], [], []
mass_closure, velocity_closure = [], []
monthly_daily, monthly_coarse = [], []
for month in range(1, 13):
    select = z["daily_month"] == month
    weights = durations["daily"][select]
    np.testing.assert_array_equal(
        z["daily_time_bounds"][select][[0, -1], [0, 1]],
        z["monthly_time_bounds"][month - 1],
    )
    assert weights.sum() == durations["monthly"][month - 1]
    daily_mean = weighted(data["daily"]["line"][select], weights)
    a = data["daily"]["a"][select]
    m = np.nan_to_num(data["daily"]["mass"][select])
    v = np.nan_to_num(data["daily"]["v"][select])
    mean_a = weighted(a, weights)
    mean_m = weighted(m, weights)
    mean_v = np.divide(
        weighted(a * v, weights), mean_a, out=np.zeros(50), where=mean_a > 0
    )
    coarse = totals(mean_m * mean_v * edge)
    monthly_daily.append(daily_mean)
    monthly_coarse.append(coarse)
    mass_closure.append(mean_m - data["monthly"]["mass"][month - 1])
    velocity_closure.append(mean_v - data["monthly"]["v"][month - 1])
    for k, (name, group) in enumerate(zip(names, groups)):
        baseline = float(data["monthly"]["line"][month - 1, k])
        fine = float(daily_mean[k])
        missing = data["daily"]["masked"][select][:, group]
        monthly_missing = data["monthly"]["masked"][month - 1, group]
        bad_a = data["daily"]["a"][select][:, group][missing]
        coverage_rows.append(
            {
                "month": month,
                "line": name,
                "days": int(select.sum()),
                "points": int(group.sum()),
                "daily_masked_point_days": int(missing.sum()),
                "daily_days_with_some_masked_points": int(missing.any(axis=1).sum()),
                "daily_days_with_all_points_masked": int(missing.all(axis=1).sum()),
                "monthly_masked_points": int(monthly_missing.sum()),
                "monthly_all_points_masked": bool(monthly_missing.all()),
                "maximum_concentration_at_masked_daily_point": float(bad_a.max())
                if bad_a.size
                else 0.0,
            }
        )
        monthly_rows.append(
            {
                "month": month,
                "line": name,
                "monthly_input_kg_s": baseline,
                "daily_input_monthly_mean_kg_s": fine,
                "difference_kg_s": fine - baseline,
                "difference_percent_of_abs_monthly": 100
                * (fine - baseline)
                / abs(baseline)
                if baseline
                else None,
                "coarsened_daily_input_kg_s": float(coarse[k]),
                "between_day_covariance_contribution_kg_s": float(fine - coarse[k]),
                "archive_vs_coarsened_residual_kg_s": float(coarse[k] - baseline),
                "daily_mean_preserving_wholly_missing_days_kg_s": fine
                if not missing.all(axis=1).any()
                else None,
                "monthly_mean_preserving_wholly_missing_strait_kg_s": baseline
                if not monthly_missing.all()
                else None,
                "monthly_input_area_m2_s": float(
                    data["monthly"]["area_line"][month - 1, k]
                ),
                "daily_input_area_monthly_mean_m2_s": float(
                    weighted(data["daily"]["area_line"][select], weights)[k]
                ),
            }
        )

for t, date in enumerate(dates["daily"]):
    for k, (name, group) in enumerate(zip(names, groups)):
        missing = data["daily"]["masked"][t, group]
        daily_rows.append(
            {
                "date": date.strftime("%Y-%m-%d"),
                "line": name,
                "mass_transport_kg_s_low_ice_masks_zeroed": float(
                    data["daily"]["line"][t, k]
                ),
                "masked_points": int(missing.sum()),
                "all_points_masked": bool(missing.all()),
            }
        )

monthly_daily = np.array(monthly_daily)
monthly_coarse = np.array(monthly_coarse)
annual_daily = weighted(data["daily"]["line"], durations["daily"])
annual_monthly = weighted(data["monthly"]["line"], durations["monthly"])
annual_coarse = weighted(monthly_coarse, durations["monthly"])
np.testing.assert_allclose(
    annual_daily, weighted(monthly_daily, durations["monthly"]), rtol=1e-13
)
annual_rows = []
for k, (name, group) in enumerate(zip(names, groups)):
    d, m, c = map(float, (annual_daily[k], annual_monthly[k], annual_coarse[k]))
    annual_rows.append(
        {
            "line": name,
            "monthly_input_annual_mean_kg_s": m,
            "daily_input_annual_mean_kg_s": d,
            "difference_kg_s": d - m,
            "difference_percent_of_abs_monthly": 100 * (d - m) / abs(m) if m else None,
            "between_day_covariance_contribution_kg_s": d - c,
            "archive_vs_coarsened_residual_kg_s": c - m,
            "daily_integrated_transport_kg": d * 365 * 86400,
            "monthly_integrated_transport_kg": m * 365 * 86400,
            "daily_days_all_points_masked": int(
                data["daily"]["masked"][:, group].all(axis=1).sum()
            ),
            "monthly_months_all_points_masked": int(
                data["monthly"]["masked"][:, group].all(axis=1).sum()
            ),
        }
    )

unmasked_comparisons = []
for name in names:
    complete = [
        row
        for row in coverage_rows
        if row["line"] == name
        and row["daily_masked_point_days"] == 0
        and row["monthly_masked_points"] == 0
    ]
    months = [row["month"] for row in complete]
    if not months:
        continue
    selected = [
        row for row in monthly_rows if row["line"] == name and row["month"] in months
    ]
    weights = [row["days"] for row in complete]
    monthly = float(
        np.average([row["monthly_input_kg_s"] for row in selected], weights=weights)
    )
    daily = float(
        np.average(
            [row["daily_input_monthly_mean_kg_s"] for row in selected], weights=weights
        )
    )
    unmasked_comparisons.append(
        {
            "line": name,
            "months": months,
            "days": sum(weights),
            "monthly_input_mean_kg_s": monthly,
            "daily_input_mean_kg_s": daily,
            "difference_percent_of_abs_monthly": 100 * (daily - monthly) / abs(monthly)
            if monthly
            else None,
        }
    )

csv_file("monthly_comparison.csv", monthly_rows)
csv_file("annual_comparison.csv", annual_rows)
csv_file("mask_coverage.csv", coverage_rows)
csv_file("daily_transport.csv", daily_rows)
summary = {
    "sample": str(SAMPLE),
    "sample_sha256": hashlib.sha256(SAMPLE.read_bytes()).hexdigest(),
    "year": 1850,
    "model": "ACCESS-ESM1-6",
    "experiment": "historical",
    "member": "r1i1p1f1",
    "days": 365,
    "months": 12,
    "points": 50,
    "calendar": metadata["attributes"]["daily_time"]["calendar"],
    "ice_density_kg_m3": RHO,
    "accumulation_dtype": "float64",
    "method": "917*aice*sithick*native_velocity*APP4_edge_length for each daily mean, strait sum then duration-weighted monthly/annual mean",
    "mask_policy": "For comparison only, missing flux terms are set to zero after verifying concentration <= 0.001. This is not proof that their physical transport is zero. Wholly missing strait-day/month counts and missing-preserving monthly alternatives are retained.",
    "limitations": "No independent ice-only transport truth; within-day covariance and APP4 spatial discretization remain unresolved. CICE combined ice+snow transport is not a direct ice-only reference.",
    "independent_previous_sample_matches": old_verified,
    "daily_masked_contribution_count": int(data["daily"]["masked"].sum()),
    "daily_masked_max_concentration": float(
        data["daily"]["a"][data["daily"]["masked"]].max()
    ),
    "monthly_masked_contribution_count": int(data["monthly"]["masked"].sum()),
    "monthly_thickness_mass_identity_max_abs_kg_m2": float(
        np.nanmax(abs(reconstructed_mass - mass))
    ),
    "daily_reconstructed_mass_monthly_closure_max_abs_kg_m2": float(
        np.nanmax(abs(np.array(mass_closure)))
    ),
    "daily_area_weighted_velocity_monthly_closure_max_abs_m_s": float(
        np.nanmax(abs(np.array(velocity_closure)))
    ),
    "annual": annual_rows,
    "months_with_no_masked_contributions": unmasked_comparisons,
}
(HERE / "results.json").write_text(
    json.dumps(summary, indent=2, allow_nan=False) + "\n"
)
metadata.update(
    {
        "sample_sha256": summary["sample_sha256"],
        "extractor_sha256": hashlib.sha256(
            (HERE / "extract.py").read_bytes()
        ).hexdigest(),
        "analysis_sha256": hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
        "grid_resource_sha256": hashlib.sha256(GRID.read_bytes()).hexdigest(),
    }
)
(HERE / "provenance.json").write_text(json.dumps(metadata, indent=2) + "\n")

plt.rcParams.update({"font.size": 10})
fig, axes = plt.subplots(2, 2, figsize=(11, 7), layout="constrained")
for k, (ax, name) in enumerate(zip(axes.flat, names)):
    ax.plot(
        range(1, 13),
        data["monthly"]["line"][:, k] / 1e6,
        "o-",
        color="#d95f02",
        label="Monthly-input reconstruction",
    )
    ax.plot(
        range(1, 13),
        monthly_daily[:, k] / 1e6,
        "s-",
        color="#0072b2",
        label="Daily inputs, averaged to month",
    )
    ax.axhline(0, color="0.5", linewidth=0.7)
    ax.grid(alpha=0.2)
    ax.set_title(name.replace("_", " ").title())
    ax.set_xticks(range(1, 13))
    ax.set_xlabel("Month in 1850")
    ax.set_ylabel("Signed ice mass transport (million kg/s)")
fig.legend(
    *axes.flat[0].get_legend_handles_labels(),
    loc="outside lower center",
    ncol=2,
    frameon=False,
)
fig.suptitle(
    "ACCESS-ESM1-6: daily versus monthly mass reconstruction\nAPP4 straits and signs; low-ice masked contributions set to zero for this comparison",
    fontsize=13,
)
fig.savefig(HERE / "daily_vs_monthly_mass.png", dpi=180)
fig.savefig(HERE / "daily_vs_monthly_mass.pdf")
plt.close(fig)
print(json.dumps(summary, indent=2, allow_nan=False))
