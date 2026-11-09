"""Product-specific recipes must agree across processing and discovery (#803)."""

import copy
import json
from pathlib import Path
from unittest.mock import Mock

import numpy as np
import pytest
import xarray as xr
import yaml

from access_moppy import file_discovery, utilities
from access_moppy.derivations import evaluate_expression, optional_model_variables
from access_moppy.derivations.calc_land import land_tile_mean, mask_land
from access_moppy.driver import ACCESS_ESM_CMORiser
from access_moppy.esmval.variable_mapper import VariableIndex
from access_moppy.vocabulary_processors import CMIP6Vocabulary, CMIP7Vocabulary

HELD_PRODUCTS = (
    "Emon.rsus",
    "Eday.sbl",
)
MODELS = ("ACCESS-ESM1-5", "ACCESS-ESM1-6")


@pytest.mark.parametrize("version", ["CMIP6", "CMIP7"])
def test_sublimation_native_realm_changes_resolution_only(version):
    cls = CMIP6Vocabulary if version == "CMIP6" else CMIP7Vocabulary
    name = "LImon.sbl" if version == "CMIP6" else "landIce.sbl.tavg-u-hxy-lnd.mon.glb"
    vocab = cls(
        compound_name=name,
        source_id="ACCESS-ESM1-6",
        experiment_id="historical",
        variant_label="r1i1p1f1",
        grid_label="gn",
        native_realm="land",
    )
    assert vocab._get_nominal_resolution() == "250 km"
    assert vocab.variable["modeling_realm"] == "landIce"
    vocab.native_realm = "landIce"
    with pytest.raises(ValueError, match="no native resolution"):
        vocab._get_nominal_resolution()
    vocab.native_realm = None
    with pytest.raises(ValueError, match="registers no model component"):
        vocab._get_nominal_resolution()


def test_new_mip_table_does_not_use_legacy_limon_land_mean(tmp_path):
    # MIP_LImon.sbl changed to a whole-cell mean; the exact legacy recipe must
    # not silently produce land means when the CMIP6Plus MIP backend is used.
    with pytest.raises(ValueError, match="does not support CMIP6Plus"):
        ACCESS_ESM_CMORiser(
            input_folder=tmp_path,
            compound_name="LImon.sbl",
            cmip_version="CMIP6Plus",
            source_id="ACCESS-ESM1-6",
            experiment_id="historical",
            variant_label="r1i1p1f1",
            output_path=tmp_path / "output",
        )


@pytest.mark.parametrize("input_kind", ["paths", "dataset"])
def test_missing_three_hourly_diagnostics_are_reported_before_resampling(
    tmp_path, input_kind
):
    raw = xr.Dataset(
        {"unrelated": (("lat", "lon"), [[1.0]])}, coords={"lat": [0.0], "lon": [0.0]}
    )
    path = tmp_path / "input.nc"
    raw.to_netcdf(path)
    with ACCESS_ESM_CMORiser(
        input_data=[path] if input_kind == "paths" else raw,
        compound_name="3hr.tran",
        cmip_version="CMIP7",
        source_id="ACCESS-ESM1-6",
        experiment_id="historical",
        variant_label="r1i1p1f1",
        output_path=tmp_path / "out",
    ) as cmoriser:
        with pytest.raises(KeyError, match="requested frequency") as error:
            cmoriser.run()
    for name in ("fld_s03i174", "fld_s03i317", "fld_s03i395"):
        assert name in str(error.value)
    assert not list((tmp_path / "out").rglob("*.nc"))


HFDS_BASE_FLUXES = ["sfc_hflux_from_runoff", "sfc_hflux_coupler", "sfc_hflux_pme"]


def _load_hfds(tmp_path, model, names):
    time = xr.DataArray(
        np.arange(12) * 30.0 + 15.0,
        dims="time",
        attrs={"units": "days since 2000-01-01", "calendar": "proleptic_gregorian"},
    )
    raw = xr.Dataset(
        {n: (("time", "yt_ocean", "xt_ocean"), np.ones((12, 2, 3))) for n in names},
        coords={"time": time, "yt_ocean": [0.0, 1.0], "xt_ocean": [0.0, 1.0, 2.0]},
    )
    with ACCESS_ESM_CMORiser(
        input_data=raw,
        compound_name="Omon.hfds",
        cmip_version="CMIP6" if model == "ACCESS-ESM1-5" else "CMIP7",
        source_id=model,
        experiment_id="historical",
        variant_label="r1i1p1f1",
        output_path=tmp_path / "out",
    ) as cmoriser:
        inner = cmoriser.cmoriser
        inner.load_dataset(
            required_vars=inner.mapping[inner.cmor_name]["model_variables"]
        )
        return inner.ds


@pytest.mark.parametrize("model", MODELS)
@pytest.mark.parametrize("frazil", ["frazil_2d", "frazil_3d_int_z"])
def test_optional_formula_inputs_may_be_absent(tmp_path, model, frazil):
    # hfds reads both frazil fields as {"optional": ...}; ACCESS-ESM1-6 only
    # writes frazil_2d, so the check must not require the other one.
    ds = _load_hfds(tmp_path, model, HFDS_BASE_FLUXES + [frazil])
    assert set(HFDS_BASE_FLUXES + [frazil]) <= set(ds.data_vars)


@pytest.mark.parametrize("model", MODELS)
def test_missing_required_formula_input_is_still_reported(tmp_path, model):
    with pytest.raises(KeyError, match="requested frequency") as error:
        _load_hfds(
            tmp_path, model, ["sfc_hflux_from_runoff", "sfc_hflux_coupler", "frazil_2d"]
        )
    assert "sfc_hflux_pme" in str(error.value)
    assert "frazil" not in str(error.value)


def test_optional_model_variables_walks_nested_expressions():
    calc = {
        "type": "formula",
        "operation": "add",
        "args": [
            "a",
            {"operation": "multiply", "args": [{"optional": "b"}, {"literal": "c"}]},
        ],
        "kwargs": {"x": {"optional": "d"}, "y": [{"optional": "a"}]},
    }
    # "a" is also read directly, so it stays required.
    assert optional_model_variables(calc) == {"b", "d"}


@pytest.mark.parametrize("model", MODELS)
@pytest.mark.parametrize("lazy", [False, True])
@pytest.mark.parametrize(
    "product,expected",
    [
        ("Eday.clt", [np.nan, 40, 40, 40]),
        ("Eday.evspsbl", [np.nan, 5, 5, 5]),
        ("LImon.sbl", [np.nan, 2, 2, np.nan]),
        ("3hr.tran", [0, 0.5, 2, 2]),
    ],
)
def test_domain_specific_calculations(model, lazy, product, expected):
    # Ocean, quarter-land coast, snowy land, snow-free land. Tile means are 2;
    # evaporation is 1 (canopy) + 2 (soil/transpiration) + 2 (sublimation).
    tiles = xr.DataArray(
        [[0, 0.25, 0.25, 0.25], [0, 0.75, 0.75, 0.75]],
        dims=("pseudo_level", "cell"),
        coords={"pseudo_level": [1, 2]},
    )
    flux = xr.DataArray(
        [[np.nan, 5, 5, 5], [np.nan, 1, 1, 1]],
        dims=("pseudo_level_0", "cell"),
        coords={"pseudo_level_0": [1, 2]},
    )
    context = {
        "fld_s02i204": xr.DataArray([0.4] * 4, dims="cell"),
        "fld_s03i317": tiles,
        "fld_s03i395": xr.DataArray([0, 0.25, 1, 1], dims="cell"),
        "fld_s03i287": xr.ones_like(flux),
        "fld_s03i288": flux,
        "fld_s03i331": flux,
        "fld_s03i174": flux,
        "fld_s08i236": xr.ones_like(flux) * xr.DataArray([0, 1, 1, 0], dims="cell"),
    }
    if lazy:
        context = {key: value.chunk({"cell": 2}) for key, value in context.items()}
    name = product.split(".")[1]
    mapping = utilities.load_model_mappings(product, model)[name]
    result = evaluate_expression(mapping["calculation"], context)
    assert result.dims == ("cell",)
    if lazy:
        assert result.chunks is not None
    np.testing.assert_allclose(result.values, expected)
    alias = utilities.load_cmip6_to_cmip7_mapping()[product]
    assert utilities.load_model_mappings(alias, model) == {name: mapping}
    assert (
        VariableIndex(model).get(*product.split(".")).model_variables
        == mapping["model_variables"]
    )


def test_land_tile_mean_preserves_missing_active_tiles_and_rejects_misalignment():
    tiles = xr.DataArray(
        [[0.25, 0], [0.75, 1]],
        dims=("pseudo_level", "cell"),
        coords={"pseudo_level": [1, 2]},
    )
    flux = xr.DataArray(
        [[np.nan, np.nan], [1, 1]], dims=tiles.dims, coords=tiles.coords
    )
    np.testing.assert_allclose(land_tile_mean(flux, tiles), [np.nan, 1])
    with pytest.raises(ValueError, match="align"):
        land_tile_mean(flux, tiles.assign_coords(pseudo_level=[2, 3]))


@pytest.mark.parametrize("model", MODELS)
@pytest.mark.parametrize(
    "product,stream,freq",
    [
        ("Eday.clt", "pe", "day"),
        ("Eday.evspsbl", "pe", "day"),
        ("LImon.sbl", "pa", "mon"),
        ("3hr.tran", "pi", "3hr"),
    ],
)
def test_domain_specific_discovery(tmp_path, model, product, stream, freq):
    folder = tmp_path / "output000" / "atmosphere" / "netCDF"
    folder.mkdir(parents=True)
    label = {"day": "dai", "mon": "mon", "3hr": "3hr"}[freq]
    packed = folder / f"test.{stream}-195101_{label}.nc"
    packed.touch()
    expected = [packed]
    if model == "ACCESS-ESM1-6":
        mapping = utilities.load_model_mappings(product, model)[product.split(".")[1]]
        for field in mapping["model_variables"]:
            frequency = {"day": "1day", "mon": "1mon", "3hr": "3hr"}[freq]
            split = (
                folder.parent
                / f"access-esm1p6.um7p3.2d.{field}.{frequency}.mean.1951.nc"
            )
            split.touch()
            expected.append(split)
    assert file_discovery.discover_files(tmp_path, product, model_id=model) == sorted(
        expected
    )


@pytest.mark.parametrize("model", MODELS)
@pytest.mark.parametrize("lazy", [False, True])
def test_emon_rsds_masks_ocean_without_scaling_coastal_flux(model, lazy):
    flux = xr.DataArray([[200.0, 200.0, 200.0, 200.0, 0.0]], dims=("time", "lon"))
    fraction = xr.DataArray([0.0, 0.25, 1.0, np.nan, 0.5], dims="lon")
    if lazy:
        flux = flux.chunk({"lon": 2})
        fraction = fraction.chunk({"lon": 2})
    mapping = utilities.load_model_mappings("Emon.rsds", model)["rsds"]
    assert mapping["model_variables"] == ["fld_s01i235", "fld_s03i395"]
    assert mapping["calculation"]["partitionable"] is True
    result = evaluate_expression(
        mapping["calculation"],
        {
            "fld_s01i235": flux,
            "fld_s03i395": fraction,
        },
    )
    if lazy:
        assert result.chunks is not None
    np.testing.assert_allclose(result.values, [[np.nan, 200.0, 200.0, np.nan, 0.0]])
    assert utilities.load_model_mappings("land.rsds.tavg-u-hxy-lnd.mon.glb", model) == {
        "rsds": mapping
    }
    assert (
        VariableIndex(model).get("Emon", "rsds").model_variables
        == mapping["model_variables"]
    )
    assert utilities.load_model_mappings("Amon.rsds", model)["rsds"][
        "model_variables"
    ] == ["fld_s01i235"]


def test_land_mask_rejects_different_grid_or_coordinates():
    flux = xr.DataArray([1.0, 2.0], dims="lon", coords={"lon": [0, 1]})
    with pytest.raises(ValueError, match="grid dimensions"):
        mask_land(flux, xr.DataArray([1.0, 1.0], dims="lon_u"))
    with pytest.raises(ValueError, match="align"):
        mask_land(flux, xr.DataArray([1.0, 1.0], dims="lon", coords={"lon": [1, 2]}))


def test_emon_rsds_discovery_includes_native_land_fraction(tmp_path):
    atmosphere = tmp_path / "output000" / "atmosphere"
    atmosphere.mkdir(parents=True)
    paths = [
        atmosphere / f"access-esm1p6.um7p3.2d.{name}.1mon.mean.1951.nc"
        for name in ("fld_s01i235", "fld_s03i395")
    ]
    for path in paths:
        path.touch()
    assert file_discovery.discover_files(
        tmp_path, "Emon.rsds", model_id="ACCESS-ESM1-6"
    ) == sorted(paths)


@pytest.fixture
def recipes(tmp_path, monkeypatch):
    # Synthetic native diagnostics: these do not claim ACCESS land-flux support.
    raw = {
        "atmosphere": {
            "rsds": {
                "supported_compound_names": ["Amon.rsds", "day.rsds", "Emon.rsds"],
                "model_variables": ["grid_flux"],
                "calculation": {"type": "direct", "formula": "grid_flux"},
                "file_pattern": "grid_*.nc",
                "units": "W m-2",
            },
        },
        "land": {
            "Emon.rsds": {
                "model_variables": ["land_flux"],
                "calculation": {"type": "direct", "formula": "land_flux"},
                "file_pattern": "land_*.nc",
            },
        },
    }
    path = tmp_path / "TEST_mappings.json"
    path.write_text(json.dumps(raw))
    monkeypatch.setattr(utilities, "files", lambda _: tmp_path)
    monkeypatch.setattr(file_discovery, "_load_full_mappings", lambda _: raw)
    return raw, path


def test_exact_recipe_drives_inputs_calculation_and_component(recipes, tmp_path):
    for filename in ("grid_1850.nc", "land_1850.nc"):
        (tmp_path / filename).touch()

    context = {
        # Ocean, 25% land, pure land. Masking grid_flux cannot produce 400.
        "grid_flux": xr.DataArray([100.0, 175.0, 300.0], dims="cell"),
        "land_flux": xr.DataArray([np.nan, 400.0, 300.0], dims="cell"),
    }
    for product, component, native in (
        ("Amon.rsds", "atmosphere", "grid_flux"),
        ("Emon.rsds", "land", "land_flux"),
    ):
        entry = utilities.load_model_mappings(product, "TEST")["rsds"]
        assert entry["model_variables"] == [native]
        xr.testing.assert_identical(
            evaluate_expression(entry["calculation"]["formula"], context),
            context[native],
        )
        assert utilities.load_variable_component(product, "TEST") == component
        paths = file_discovery.discover_files(tmp_path, product, model_id="TEST")
        prefix = "grid" if component == "atmosphere" else "land"
        assert paths == [tmp_path / f"{prefix}_1850.nc"]
    # Complete replacement: no inherited units or other fallback metadata.
    assert "units" not in utilities.load_model_mappings("Emon.rsds", "TEST")["rsds"]


def test_shared_recipe_and_rejected_fallback(recipes):
    assert utilities.load_model_mappings("Amon.rsds", "TEST") == (
        utilities.load_model_mappings("day.rsds", "TEST")
    )
    assert utilities.load_model_mappings("Eday.rsds", "TEST") == {}
    with pytest.raises(ValueError, match="Ambiguous mapping"):
        utilities.load_variable_entry("rsds", "TEST")


def test_support_index_uses_exact_recipes_and_allowlists(recipes):
    _, path = recipes
    index = VariableIndex.from_json(path)
    assert index.all_compound_names() == ["Amon.rsds", "Emon.rsds", "day.rsds"]
    assert index.get("Emon", "rsds").model_variables == ["land_flux"]
    assert index.get("Emon", "rsds").component == "land"
    assert not index.is_supported("Eday", "rsds")


def test_discovery_expands_inputs_from_the_selected_component(recipes, tmp_path):
    raw, _ = recipes
    for component, key in (("atmosphere", "rsds"), ("land", "Emon.rsds")):
        raw[component][key].pop("file_pattern")
    raw["model_info"] = {
        "file_discovery": {
            "output_dir_pattern": ".",
            "components": {
                component: {
                    "subdir": component,
                    "frequency_patterns": {
                        "mon": "{model_var}_*.nc",
                    },
                }
                for component in ("atmosphere", "land")
            },
        }
    }
    (tmp_path / "land").mkdir()
    expected = tmp_path / "land" / "land_flux_1850.nc"
    expected.touch()
    assert file_discovery.discover_files(tmp_path, "Emon.rsds", model_id="TEST") == [
        expected
    ]


@pytest.mark.parametrize("exact", [False, True])
def test_duplicate_recipes_are_rejected(recipes, exact):
    raw, _ = recipes
    key = "Emon.rsds" if exact else "rsds"
    source = "land" if exact else "atmosphere"
    raw["ocean"] = {key: copy.deepcopy(raw[source][key])}
    with pytest.raises(ValueError, match="Ambiguous mapping"):
        utilities._resolve_variable_entry(raw, "Emon.rsds" if exact else "Amon.rsds")


def test_disallowed_exact_entry_does_not_fall_back(recipes):
    raw, _ = recipes
    raw["atmosphere"]["rsds"].pop("supported_compound_names")
    raw["land"]["Emon.rsds"]["supported_compound_names"] = []
    assert utilities._resolve_variable_entry(raw, "Emon.rsds") is None


@pytest.mark.parametrize("model", MODELS)
@pytest.mark.parametrize("product", HELD_PRODUCTS)
def test_held_products_and_cmip7_aliases_have_no_mapping(model, product):
    branded = utilities.load_cmip6_to_cmip7_mapping()[product]
    assert utilities.load_model_mappings(product, model) == {}
    assert utilities.load_model_mappings(branded.lower(), model) == {}
    table, name = product.split(".")
    assert not VariableIndex(model).is_supported(table, name)


@pytest.mark.parametrize("model", MODELS)
@pytest.mark.parametrize("product", HELD_PRODUCTS)
@pytest.mark.parametrize("cmip_version", ["CMIP6", "CMIP7"])
@pytest.mark.parametrize("input_kind", ["paths", "dataset", "folder"])
def test_held_products_stop_before_input_or_output(
    model, product, cmip_version, input_kind, tmp_path, monkeypatch
):
    discover = Mock()
    construct = Mock()
    monkeypatch.setattr("access_moppy.driver.discover_files", discover)
    monkeypatch.setattr("access_moppy.driver.Atmosphere_CMORiser", construct)
    request = (
        utilities.load_cmip6_to_cmip7_mapping()[product]
        if cmip_version == "CMIP7"
        else product
    )
    inputs = {
        "paths": {"input_data": [tmp_path / "missing.nc"]},
        "dataset": {"input_data": xr.Dataset()},
        "folder": {"input_folder": tmp_path},
    }
    with (
        pytest.warns(utilities.MappingNotFoundWarning),
        pytest.raises(ValueError, match=f"No supported mapping for '{product}'"),
    ):
        ACCESS_ESM_CMORiser(
            compound_name=request,
            source_id=model,
            experiment_id="piControl",
            variant_label="r1i1p1f1",
            cmip_version=cmip_version,
            output_path=tmp_path / "output",
            **inputs[input_kind],
        )
    discover.assert_not_called()
    construct.assert_not_called()
    assert not (tmp_path / "output").exists()


@pytest.mark.parametrize("model", MODELS)
def test_compatible_products_keep_their_existing_calculations(model):
    for product in (
        "Amon.rsds",
        "day.rsds",
        "Amon.rsus",
        "day.clt",
        "Amon.evspsbl",
        "Amon.sbl",
        "Lmon.tran",
    ):
        branded = utilities.load_cmip6_to_cmip7_mapping()[product]
        entry = utilities.load_model_mappings(product, model)
        assert entry
        assert utilities.load_model_mappings(branded, model) == entry
    assert utilities.load_model_mappings("Amon.tas", model) == (
        utilities.load_model_mappings("E1hr.tas", model)
    )
    for legacy, mip in (
        ("Amon.rsds", "APmon.rsds"),
        ("day.rsus", "APday.rsus"),
        ("day.clt", "APday.clt"),
        ("Amon.evspsbl", "APmon.evspsbl"),
        ("Amon.sbl", "LI3hrPt.sbl"),
        ("Lmon.tran", "LPmon.tran"),
    ):
        assert utilities.load_model_mappings(legacy, model) == (
            utilities.load_model_mappings(mip, model)
        )


@pytest.mark.parametrize("product", HELD_PRODUCTS)
def test_discovery_rejects_unsupported_product(tmp_path, product):
    with pytest.raises(file_discovery.FileDiscoveryError, match="No supported mapping"):
        file_discovery.discover_files(tmp_path, product, model_id="ACCESS-ESM1-6")


def test_shipped_configs_exclude_products_without_verified_native_inputs():
    examples = Path(utilities.__file__).parent / "examples"
    aliases = utilities.load_cmip7_to_cmip6_mapping()
    aliases = {name.lower(): product for name, product in aliases.items()}
    excluded = {*HELD_PRODUCTS, "Eday.evspsbl", "3hr.tran"}
    for path in examples.glob("batch_config*.yml"):
        config = yaml.safe_load(path.read_text())
        products = {aliases.get(name.lower(), name) for name in config["variables"]}
        assert not products.intersection(excluded), path.name


def test_emon_rsds_is_enabled_in_additional_templates():
    examples = Path(utilities.__file__).parent / "examples"
    enabled = [
        path
        for path in examples.glob("*_additional.yml")
        if "land.rsds.tavg-u-hxy-lnd.mon.glb"
        in yaml.safe_load(path.read_text())["variables"]
    ]
    assert len(enabled) == 12
