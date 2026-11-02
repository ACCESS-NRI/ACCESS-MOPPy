"""Validate the additional #808 land means through native CMOR/repack output.

Set ACCESS_MOPPY_DATA_ROOT to the ACCESS-ESM1-6 historical archive and optionally
ACCESS_MOPPY_TEST_YEAR to a particular year (default: first available). This
compares actual production mappings with explicit native-field calculations,
not the published piControl release or an independently validated ESM1-5 run.
"""

import os
from pathlib import Path

import numpy as np
import pytest
import xarray as xr

from access_moppy.driver import ACCESS_ESM_CMORiser
from access_moppy.file_discovery import _extract_year_from_path, discover_files


@pytest.mark.integration
@pytest.mark.parametrize(
    ("name", "weighted", "products", "conversion"),
    [
        ("cLand", list(range(851, 861)), [898, 899, 900], 1 / 1000),
        ("cLeaf", [852], [], 1 / 1000),
        ("cLitter", [855, 856, 857], [], 1 / 1000),
        ("cProduct", [], [898, 899, 900], 1 / 1000),
        ("cRoot", [854], [], 1 / 1000),
        ("nLand", list(range(861, 871)), [901, 902, 903], 1 / 1000),
        ("nLitter", [864, 865, 866], [], 1 / 1000),
        ("nMineral", [870], [], 1 / 1000),
        ("nProduct", [], [901, 902, 903], 1 / 1000),
        ("nSoil", [867, 868, 869, 870], [], 1 / 1000),
        ("nVeg", [861, 862, 863], [], 1 / 1000),
        ("fNdep", [884], [], 1 / (1000 * 86400)),
        ("fNgas", [920], [], 1 / (1000 * 86400)),
        ("fNleach", [918], [], 1 / (1000 * 86400)),
        ("fNloss", [918, 920], [], 1 / (1000 * 86400)),
        ("fNnetmin", [917], [], 1 / (1000 * 86400)),
        ("fNup", [919], [], 1 / (1000 * 86400)),
        ("fDeforestToProduct", [], [895], 1 / (1000 * 365.2422 * 86400)),
        ("fProductDecomp", [], [907, 908, 909], 1 / (1000 * 365.2422 * 86400)),
    ],
)
def test_native_additional_land_means(
    name,
    weighted,
    products,
    conversion,
    tmp_path,
    parent_experiment_config,
    record_property,
):
    root = os.environ.get("ACCESS_MOPPY_DATA_ROOT")
    if not root or not Path(root).is_dir():
        pytest.skip("Set ACCESS_MOPPY_DATA_ROOT to an ACCESS-ESM1-6 historical archive")
    table = "Lmon" if name in {"cLeaf", "cLitter", "cProduct", "cRoot"} else "Emon"
    product = f"{table}.{name}"
    paths = discover_files(root, product, model_id="ACCESS-ESM1-6")
    assert paths
    year = int(
        os.environ.get("ACCESS_MOPPY_TEST_YEAR", _extract_year_from_path(paths[0]))
    )
    paths = discover_files(
        root, product, model_id="ACCESS-ESM1-6", start_year=year, end_year=year
    )
    assert len(paths) == 12
    expected_months, fractions, roundoff, expected_float32 = [], [], [], []
    for path in paths:
        with xr.open_dataset(path, engine="netcdf4") as raw:
            fields = {}
            for n in set(weighted + products + [317, 395]):
                field = raw[f"fld_s03i{n}"]
                # CASA time_0 has a different midpoint within the same month.
                # Match native monthly samples; the written-output check below
                # also verifies production normalization to calendar-month time.
                for dim in [d for d in field.dims if d.startswith("time")]:
                    assert field.sizes[dim] == 1
                    np.testing.assert_array_equal(raw[dim].dt.year, raw.time.dt.year)
                    np.testing.assert_array_equal(raw[dim].dt.month, raw.time.dt.month)
                    field = field.isel({dim: 0}, drop=True)
                fields[n] = field.reset_coords(drop=True).astype("float64").load()
            if name == "cRoot":
                assert "ROOT" in raw.fld_s03i854.attrs["long_name"].upper()
            tiles, fraction = fields[317], fields[395]
            tile_dim = next(d for d in tiles.dims if "pseudo_level" in d)
            np.testing.assert_allclose(
                tiles.sum(tile_dim).values[fraction.values > 0], 1, rtol=0, atol=2e-7
            )
            total = 0
            if weighted:
                total = (sum(fields[n] for n in weighted) * tiles).sum(tile_dim)
            if products:
                # Product diagnostics already contain tile-area weighting.
                total += sum(fields[n] for n in products).sum(tile_dim)
            expected_months.append(
                (total * conversion)
                .where(fraction > 0)
                .expand_dims(time=raw.time.values)
            )
            fractions.append(fraction.expand_dims(time=raw.time.values))
            if name == "fNnetmin":
                # Signed tile contributions nearly cancel in some cells.
                # Bound float32 arithmetic with gamma_(n+4) * sum(abs(terms)),
                # rather than an arbitrary absolute tolerance near zero.
                eps = np.finfo("float32").eps
                gamma = (tiles.sizes[tile_dim] + 4) * eps
                gamma /= 1 - gamma
                bound = gamma * abs(fields[917] * tiles).sum(tile_dim) * conversion
                roundoff.append(bound.expand_dims(time=raw.time.values))
                direct32 = (
                    (fields[917].astype("float32") * tiles.astype("float32")).sum(
                        tile_dim
                    )
                    * np.float32(conversion)
                ).where(fraction > 0)
                expected_float32.append(direct32.expand_dims(time=raw.time.values))
            latb, lonb = np.deg2rad(raw.lat_bnds), np.deg2rad(raw.lon_bnds)
            area = (
                6371000.0**2
                * abs(np.sin(latb.isel(bnds=1)) - np.sin(latb.isel(bnds=0)))
                * abs(lonb.isel(bnds=1) - lonb.isel(bnds=0))
            ).load()
    expected = xr.concat(expected_months, dim="time")
    fraction = xr.concat(fractions, dim="time")
    coast, pure_land, ocean = (
        (fraction > 0) & (fraction < 1),
        fraction == 1,
        fraction == 0,
    )
    assert coast.any() and pure_land.any() and ocean.any()
    parent = {
        **parent_experiment_config,
        "parent_source_id": "ACCESS-ESM1-6",
        "parent_mip_era": "CMIP7",
    }
    with ACCESS_ESM_CMORiser(
        input_data=paths,
        compound_name=f"land.{name}.tavg-u-hxy-lnd.mon.glb",
        cmip_version="CMIP7",
        source_id="ACCESS-ESM1-6",
        experiment_id="historical",
        variant_label="r1i1p1f1",
        parent_info=parent,
        output_path=tmp_path,
        split_years=1,
    ) as cmoriser:
        cmoriser.run()
        cmoriser.write()
    outputs = list(tmp_path.rglob("*.nc"))
    assert len(outputs) == 1
    with xr.open_dataset(outputs[0]) as output:
        actual = output[name].load()
        assert actual.dims == expected.dims and actual.shape == expected.shape
        for dim in expected.dims:
            np.testing.assert_array_equal(actual[dim].values, expected[dim].values)
        assert "area: mean where land" in actual.attrs["cell_methods"]
        assert output.attrs["area_label"] == "lnd"
        if name == "fNnetmin":
            np.testing.assert_array_equal(
                actual, xr.concat(expected_float32, dim="time")
            )
            budget = 1e-6 * abs(expected) + xr.concat(roundoff, dim="time")
            assert not (abs(actual - expected) > budget).any()
        else:
            np.testing.assert_allclose(
                actual, expected, rtol=1e-6, atol=0, equal_nan=True
            )
            for mask in (coast, pure_land):
                np.testing.assert_allclose(
                    actual.values[mask.values],
                    expected.values[mask.values],
                    rtol=1e-6,
                    atol=0,
                )
        assert np.isnan(actual.values[ocean.values]).all()
        # areacella * sftlf/100: preserve the land-area denominator globally.
        actual_total = (actual * area * fraction).sum(["lat", "lon"])
        expected_total = (expected * area * fraction).sum(["lat", "lon"])
        np.testing.assert_allclose(actual_total, expected_total, rtol=1e-6, atol=0)
    record_property("native_files", ";".join(map(str, paths)))
    record_property("coastal_cells_per_month", int(coast.isel(time=0).sum()))
    record_property("first_month_land_area_integral", float(actual_total.isel(time=0)))
