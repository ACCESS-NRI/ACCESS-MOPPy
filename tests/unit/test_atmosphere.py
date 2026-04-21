"""
Unit tests for Atmosphere_CMORiser.

Covers bug fixes in select_and_process_variables() and update_attributes():
  1. time_0 non-singleton dimension is dropped before transpose
     (land variables such as cVeg/cSoil acquire a time_0 dimension > 1 when
     xarray broadcasts multiple model variables with differing dim orders)
  2. Inherited units attribute is cleared after formula calculations
     (raw PP variables carry units="1"; the formula converts to kg m-2 but
     xarray does not update the attribute, causing _check_units to fail)
  3. Formula that changes time resolution (daily→monthly) rebuilds self.ds
     rather than using __setitem__ which would silently reindex back to daily
  4. update_attributes() skips astype() for already-decoded (cftime/datetime64)
     time coordinates to avoid TypeError when casting cftime to float64
"""

import warnings
from unittest.mock import MagicMock, patch

import numpy as np
import pandas as pd
import pytest
import xarray as xr

from access_moppy.atmosphere import Atmosphere_CMORiser
from access_moppy.base import CMORiser

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_vocab(dimensions="time lat lon", units="kg m-2"):
    """Return a minimal mock vocabulary object."""
    vocab = MagicMock()
    vocab.variable = {"dimensions": dimensions, "units": units, "type": "double"}
    vocab.axes = {
        "lat": {"out_name": "lat"},
        "lon": {"out_name": "lon"},
        "time": {"out_name": "time"},
    }
    # _get_axes returns (required_axes, rename_map)
    vocab._get_axes.return_value = ([], {})
    # _get_required_bounds_variables returns (required_bounds, rename_map)
    vocab._get_required_bounds_variables.return_value = ({}, {})
    return vocab


def _make_cmoriser(
    ds, cmor_name, dimensions="time lat lon", units="kg m-2", tmp_path=None
):
    """
    Instantiate an Atmosphere_CMORiser with a pre-loaded xarray Dataset.
    The instance's internal ds is replaced after init so we can inject
    arbitrary test data without going through load_dataset().
    """
    import tempfile

    out = str(tmp_path or tempfile.mkdtemp())
    vocab = _make_vocab(dimensions=dimensions, units=units)

    mapping = {
        cmor_name: {
            "model_variables": [cmor_name],
            "calculation": {"type": "direct", "formula": cmor_name},
        }
    }

    cmoriser = Atmosphere_CMORiser(
        input_data=ds,
        output_path=out,
        vocab=vocab,
        variable_mapping=mapping,
        compound_name=f"Lmon.{cmor_name}",
        validate_frequency=False,
        enable_chunking=False,
        enable_compression=False,
    )
    return cmoriser


# ---------------------------------------------------------------------------
# Tests for Fix 1: time_0 dimension removal before transpose
# ---------------------------------------------------------------------------


class TestTime0DimensionHandling:
    """
    Ensure that a time_0 dimension that is absent from the CMOR transpose order
    is dropped regardless of its size, so that transpose() does not raise a
    ValueError.
    """

    @pytest.mark.unit
    def test_time0_nonsingleton_dropped_before_transpose(self, tmp_path):
        """
        time_0 with size > 1 must be dropped before transpose.

        This reproduces the original crash:
          ValueError: ('time', 'lon', 'lat') must be a permuted list of
          ('time', 'time_0', 'lat', 'lon'), unless `...` is included
        """
        nt, n0, nlat, nlon = 12, 12, 5, 5
        data = np.ones((nt, n0, nlat, nlon), dtype=np.float32)

        ds = xr.Dataset(
            {"cVeg": (["time", "time_0", "lat", "lon"], data, {"units": "kg m-2"})},
            coords={
                "time": np.arange(nt, dtype=float),
                "time_0": np.arange(n0, dtype=float),
                "lat": np.linspace(-90, 90, nlat),
                "lon": np.linspace(0, 360, nlon),
            },
        )

        cmoriser = _make_cmoriser(
            ds, "cVeg", dimensions="time lat lon", tmp_path=tmp_path
        )

        # Bypass full load_dataset; inject ds directly and run only the
        # transpose/squeeze portion via select_and_process_variables.
        # We patch load_dataset so the pre-injected ds is preserved.
        with patch.object(cmoriser, "load_dataset"):
            cmoriser.ds = ds.copy()
            # vocab._get_axes / _get_required_bounds_variables already mocked
            cmoriser.select_and_process_variables()

        assert (
            "time_0" not in cmoriser.ds["cVeg"].dims
        ), "time_0 should have been dropped before transpose"

    @pytest.mark.unit
    def test_time0_singleton_still_dropped(self, tmp_path):
        """
        time_0 with size == 1 must also be removed (handled by squeeze).
        Ensures the pre-existing squeeze logic still works alongside the new fix.
        """
        nt, nlat, nlon = 12, 5, 5
        data = np.ones((nt, 1, nlat, nlon), dtype=np.float32)

        ds = xr.Dataset(
            {"cVeg": (["time", "time_0", "lat", "lon"], data, {"units": "kg m-2"})},
            coords={
                "time": np.arange(nt, dtype=float),
                "time_0": [0.0],
                "lat": np.linspace(-90, 90, nlat),
                "lon": np.linspace(0, 360, nlon),
            },
        )

        cmoriser = _make_cmoriser(
            ds, "cVeg", dimensions="time lat lon", tmp_path=tmp_path
        )

        with patch.object(cmoriser, "load_dataset"):
            cmoriser.ds = ds.copy()
            cmoriser.select_and_process_variables()

        assert "time_0" not in cmoriser.ds["cVeg"].dims

    @pytest.mark.unit
    def test_no_time0_unaffected(self, tmp_path):
        """
        Variables without time_0 (e.g. Amon.tas) must pass through unchanged.
        """
        nt, nlat, nlon = 12, 5, 5
        data = np.ones((nt, nlat, nlon), dtype=np.float32)

        ds = xr.Dataset(
            {"tas": (["time", "lat", "lon"], data, {"units": "K"})},
            coords={
                "time": np.arange(nt, dtype=float),
                "lat": np.linspace(-90, 90, nlat),
                "lon": np.linspace(0, 360, nlon),
            },
        )

        cmoriser = _make_cmoriser(
            ds, "tas", dimensions="time lat lon", units="K", tmp_path=tmp_path
        )

        with patch.object(cmoriser, "load_dataset"):
            cmoriser.ds = ds.copy()
            cmoriser.select_and_process_variables()

        assert "time_0" not in cmoriser.ds["tas"].dims
        assert cmoriser.ds["tas"].dims == ("time", "lat", "lon")

    @pytest.mark.unit
    def test_time0_not_in_cmor_dims_is_dropped(self, tmp_path):
        """
        time_0 present as outer dim (fld_s03i236 style) should also be dropped.
        """
        nt, n0, nlat, nlon = 12, 12, 5, 5
        data = np.ones((n0, nt, nlat, nlon), dtype=np.float32)

        ds = xr.Dataset(
            {"tas": (["time_0", "time", "lat", "lon"], data, {"units": "K"})},
            coords={
                "time_0": np.arange(n0, dtype=float),
                "time": np.arange(nt, dtype=float),
                "lat": np.linspace(-90, 90, nlat),
                "lon": np.linspace(0, 360, nlon),
            },
        )

        cmoriser = _make_cmoriser(
            ds, "tas", dimensions="time lat lon", units="K", tmp_path=tmp_path
        )

        with patch.object(cmoriser, "load_dataset"):
            cmoriser.ds = ds.copy()
            cmoriser.select_and_process_variables()

        assert "time_0" not in cmoriser.ds["tas"].dims


# ---------------------------------------------------------------------------
# Tests for Fix 2: units attribute cleared after formula calculation
# ---------------------------------------------------------------------------


class TestFormulaUnitsClearing:
    """
    After evaluate_expression(), the result inherits the raw model variable's
    units attribute (e.g. "1").  This must be cleared so that update_attributes()
    can write the correct CMOR units without _check_units raising a ValueError.
    """

    @pytest.mark.unit
    def test_formula_result_units_cleared(self, tmp_path):
        """
        After a formula-type calculation, the result variable must have
        no units attribute so that the CMOR units can be applied cleanly.
        """

        nt, nlat, nlon = 3, 5, 5
        data = np.ones((nt, nlat, nlon), dtype=np.float32)

        # Two input variables with different (wrong) units inherited from PP
        ds = xr.Dataset(
            {
                "var_a": (["time", "lat", "lon"], data, {"units": "1"}),
                "var_b": (["time", "lat", "lon"], data, {"units": "1"}),
            },
            coords={
                "time": np.arange(nt, dtype=float),
                "lat": np.linspace(-90, 90, nlat),
                "lon": np.linspace(0, 360, nlon),
            },
        )

        vocab = _make_vocab(dimensions="time lat lon", units="kg m-2")
        vocab._get_axes.return_value = ([], {})
        vocab._get_required_bounds_variables.return_value = ({}, {})

        mapping = {
            "cVeg": {
                "model_variables": ["var_a", "var_b"],
                "calculation": {
                    "type": "formula",
                    "operation": "add",
                    "operands": ["var_a", "var_b"],
                },
            }
        }

        import tempfile

        cmoriser = Atmosphere_CMORiser(
            input_data=ds,
            output_path=str(tmp_path or tempfile.mkdtemp()),
            vocab=vocab,
            variable_mapping=mapping,
            compound_name="Lmon.cVeg",
            validate_frequency=False,
            enable_chunking=False,
            enable_compression=False,
        )

        with patch.object(cmoriser, "load_dataset"):
            cmoriser.ds = ds.copy()
            cmoriser.select_and_process_variables()

        # units attribute must be absent (cleared) after formula calculation
        assert "units" not in cmoriser.ds["cVeg"].attrs, (
            "Inherited units attribute should have been cleared after formula "
            "calculation so update_attributes() can apply the correct CMOR units"
        )

    @pytest.mark.unit
    def test_direct_calc_preserves_units(self, tmp_path):
        """
        Direct (rename-only) calculations must NOT have their units cleared —
        the attribute check in _check_units should be able to validate them.
        """
        nt, nlat, nlon = 3, 5, 5
        data = np.ones((nt, nlat, nlon), dtype=np.float32)

        ds = xr.Dataset(
            {"fld_tas": (["time", "lat", "lon"], data, {"units": "K"})},
            coords={
                "time": np.arange(nt, dtype=float),
                "lat": np.linspace(-90, 90, nlat),
                "lon": np.linspace(0, 360, nlon),
            },
        )

        cmoriser = _make_cmoriser(
            ds, "fld_tas", dimensions="time lat lon", units="K", tmp_path=tmp_path
        )
        # Patch the mapping to direct-rename fld_tas → tas
        cmoriser.mapping = {
            "tas": {
                "model_variables": ["fld_tas"],
                "calculation": {"type": "direct", "formula": "fld_tas"},
            }
        }
        cmoriser.cmor_name = "tas"

        with patch.object(cmoriser, "load_dataset"):
            cmoriser.ds = ds.copy()
            cmoriser.select_and_process_variables()

        # For direct calc, units attribute should still be present
        assert "units" in cmoriser.ds["tas"].attrs
        assert cmoriser.ds["tas"].attrs["units"] == "K"

    @pytest.mark.unit
    def test_formula_with_inherited_wrong_units_no_longer_raises(self, tmp_path):
        """
        Before the fix, a formula result with units="1" would cause
        _check_units to raise ValueError("Mismatch units for cVeg: 1 != kg m-2").
        After the fix, clearing the attribute allows the full run() to complete.
        """
        nt, nlat, nlon = 3, 5, 5
        data = np.random.rand(nt, nlat, nlon).astype(np.float32)

        ds = xr.Dataset(
            {
                "var_a": (["time", "lat", "lon"], data, {"units": "1"}),
                "var_b": (
                    ["time", "lat", "lon"],
                    np.ones((nt, nlat, nlon), np.float32),
                    {"units": "1"},
                ),
            },
            coords={
                "time": np.arange(nt, dtype=float),
                "lat": np.linspace(-90, 90, nlat),
                "lon": np.linspace(0, 360, nlon),
            },
        )

        vocab = _make_vocab(dimensions="time lat lon", units="kg m-2")
        vocab._get_axes.return_value = ([], {})
        vocab._get_required_bounds_variables.return_value = ({}, {})

        mapping = {
            "cVeg": {
                "model_variables": ["var_a", "var_b"],
                "calculation": {
                    "type": "formula",
                    "operation": "add",
                    "operands": ["var_a", "var_b"],
                },
            }
        }

        import tempfile

        cmoriser = Atmosphere_CMORiser(
            input_data=ds,
            output_path=str(tmp_path or tempfile.mkdtemp()),
            vocab=vocab,
            variable_mapping=mapping,
            compound_name="Lmon.cVeg",
            validate_frequency=False,
            enable_chunking=False,
            enable_compression=False,
        )

        with patch.object(cmoriser, "load_dataset"):
            cmoriser.ds = ds.copy()
            # select_and_process_variables must not raise due to units mismatch
            cmoriser.select_and_process_variables()


def _make_monthly_ds(nlat=5, nlon=5):
    """
    Return a dataset with 12 monthly numeric time steps (no bounds).

    Time values are mid-month offsets in "days since 1850-01-01" so that
    _infer_frequency() classifies them as "monthly" (28-31 day spacing).
    lat/lon are regular grids suitable for latitude/longitude bounds tests.
    """
    mid_month = [
        15.5,
        45.0,
        74.5,
        105.0,
        135.5,
        166.0,
        196.5,
        227.5,
        258.0,
        288.5,
        319.0,
        349.5,
    ]
    nt = len(mid_month)
    data = np.ones((nt, nlat, nlon), dtype=np.float32)

    return xr.Dataset(
        {"tas": (["time", "lat", "lon"], data, {"units": "K"})},
        coords={
            "time": xr.Variable(
                "time",
                np.array(mid_month),
                {"units": "days since 1850-01-01", "calendar": "proleptic_gregorian"},
            ),
            "lat": np.linspace(-90.0, 90.0, nlat),
            "lon": np.linspace(0.0, 355.0, nlon),
        },
    )


def _bare_cmoriser(ds, tmp_path):
    """Return an Atmosphere_CMORiser with ds already injected."""
    cmoriser = _make_cmoriser(
        ds, "tas", dimensions="time lat lon", units="K", tmp_path=tmp_path
    )
    cmoriser.ds = ds.copy()
    return cmoriser


class TestCalculateMissingBoundsVariables:
    """
    Unit tests for Atmosphere_CMORiser.calculate_missing_bounds_variables().

    Scenarios covered:
      - time_bnds auto-calculated when absent; bounds attribute set on time
      - lat_bnds auto-calculated when absent; bounds attribute set on lat
      - lon_bnds auto-calculated when absent; bounds attribute set on lon
      - bounds attribute set on coordinate even when bounds variable
        already existed in the input (Bug 3 regression)
      - UserWarning emitted when auto-calculating missing bounds
      - Unknown coordinate type: warns and does NOT set bounds attribute
      - ValueError raised when coordinate itself is missing from dataset
    """

    @pytest.mark.unit
    def test_time_bnds_calculated_when_missing(self, tmp_path):
        """
        When time_bnds is absent, calculate_missing_bounds_variables must
        calculate it and add it to the dataset with shape (time, 2).
        """
        ds = _make_monthly_ds()
        cmoriser = _bare_cmoriser(ds, tmp_path)

        bnds_required = {"time_bnds": {"out_name": "time", "must_have_bounds": "yes"}}
        cmoriser.calculate_missing_bounds_variables(bnds_required)

        assert "time_bnds" in cmoriser.ds, "time_bnds should have been created"
        assert cmoriser.ds["time_bnds"].ndim == 2
        assert cmoriser.ds["time_bnds"].shape[0] == 12
        assert cmoriser.ds["time_bnds"].shape[1] == 2

    @pytest.mark.unit
    def test_time_bounds_attribute_set_when_calculated(self, tmp_path):
        """
        After calculating missing time_bnds, the time coordinate must have
        its bounds attribute set to 'time_bnds'.
        """
        ds = _make_monthly_ds()
        cmoriser = _bare_cmoriser(ds, tmp_path)

        bnds_required = {"time_bnds": {"out_name": "time", "must_have_bounds": "yes"}}
        cmoriser.calculate_missing_bounds_variables(bnds_required)

        assert (
            cmoriser.ds["time"].attrs.get("bounds") == "time_bnds"
        ), "time coordinate must have bounds='time_bnds' after calculation"

    @pytest.mark.unit
    def test_lat_bnds_calculated_when_missing(self, tmp_path):
        """
        When lat_bnds is absent, calculate_missing_bounds_variables must
        calculate it and add it to the dataset with shape (lat, 2).
        """
        ds = _make_monthly_ds()
        cmoriser = _bare_cmoriser(ds, tmp_path)

        bnds_required = {"lat_bnds": {"out_name": "lat", "must_have_bounds": "yes"}}
        cmoriser.calculate_missing_bounds_variables(bnds_required)

        assert "lat_bnds" in cmoriser.ds, "lat_bnds should have been created"
        assert cmoriser.ds["lat_bnds"].ndim == 2
        assert cmoriser.ds["lat_bnds"].shape[1] == 2
        assert cmoriser.ds["lat"].attrs.get("bounds") == "lat_bnds"

    @pytest.mark.unit
    def test_lon_bnds_calculated_when_missing(self, tmp_path):
        """
        When lon_bnds is absent, calculate_missing_bounds_variables must
        calculate it and add it to the dataset with shape (lon, 2).
        """
        ds = _make_monthly_ds()
        cmoriser = _bare_cmoriser(ds, tmp_path)

        bnds_required = {"lon_bnds": {"out_name": "lon", "must_have_bounds": "yes"}}
        cmoriser.calculate_missing_bounds_variables(bnds_required)

        assert "lon_bnds" in cmoriser.ds, "lon_bnds should have been created"
        assert cmoriser.ds["lon_bnds"].ndim == 2
        assert cmoriser.ds["lon_bnds"].shape[1] == 2
        assert cmoriser.ds["lon"].attrs.get("bounds") == "lon_bnds"

    @pytest.mark.unit
    def test_bounds_attribute_set_when_variable_already_exists(self, tmp_path):
        """
        Regression test for Bug 3: when the bounds variable is already present
        in the dataset but the coordinate lacks the bounds attribute, the
        function must still set the attribute.

        Before the fix, the entire block was guarded by
        `if bnds_var not in self.ds.data_vars`, so the attribute was never
        set when the variable already existed.
        """
        ds = _make_monthly_ds()

        # Add time_bnds manually but WITHOUT setting bounds attribute on time
        n = ds.sizes["time"]
        fake_bnds = np.zeros((n, 2), dtype=float)
        ds["time_bnds"] = xr.DataArray(fake_bnds, dims=["time", "bnds"])
        assert "bounds" not in ds["time"].attrs

        cmoriser = _bare_cmoriser(ds, tmp_path)
        bnds_required = {"time_bnds": {"out_name": "time", "must_have_bounds": "yes"}}
        cmoriser.calculate_missing_bounds_variables(bnds_required)

        assert (
            cmoriser.ds["time"].attrs.get("bounds") == "time_bnds"
        ), "bounds attribute must be set even when time_bnds already existed"

    @pytest.mark.unit
    def test_existing_bounds_variable_not_overwritten(self, tmp_path):
        """
        When the bounds variable already exists, it must NOT be recalculated;
        only the coordinate attribute should be updated.
        """
        ds = _make_monthly_ds()

        sentinel = np.full((ds.sizes["time"], 2), 999.0)
        ds["time_bnds"] = xr.DataArray(sentinel, dims=["time", "bnds"])

        cmoriser = _bare_cmoriser(ds, tmp_path)
        bnds_required = {"time_bnds": {"out_name": "time", "must_have_bounds": "yes"}}
        cmoriser.calculate_missing_bounds_variables(bnds_required)

        np.testing.assert_array_equal(
            cmoriser.ds["time_bnds"].values,
            sentinel,
            err_msg="Pre-existing time_bnds data must not be overwritten",
        )

    @pytest.mark.unit
    def test_warning_issued_when_bounds_missing(self, tmp_path):
        """
        A UserWarning must be emitted when bounds are absent and are being
        auto-calculated.
        """
        ds = _make_monthly_ds()
        cmoriser = _bare_cmoriser(ds, tmp_path)

        bnds_required = {"time_bnds": {"out_name": "time", "must_have_bounds": "yes"}}
        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always")
            cmoriser.calculate_missing_bounds_variables(bnds_required)

        user_warnings = [w for w in caught if issubclass(w.category, UserWarning)]
        assert len(user_warnings) >= 1
        assert "time_bnds" in str(user_warnings[0].message)

    @pytest.mark.unit
    def test_no_warning_when_bounds_already_present(self, tmp_path):
        """
        No UserWarning about missing bounds should be emitted when the bounds
        variable is already in the dataset.
        """
        ds = _make_monthly_ds()
        n = ds.sizes["time"]
        ds["time_bnds"] = xr.DataArray(np.zeros((n, 2)), dims=["time", "bnds"])

        cmoriser = _bare_cmoriser(ds, tmp_path)
        bnds_required = {"time_bnds": {"out_name": "time", "must_have_bounds": "yes"}}

        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always")
            cmoriser.calculate_missing_bounds_variables(bnds_required)

        missing_warnings = [
            w
            for w in caught
            if issubclass(w.category, UserWarning)
            and "not found in raw data" in str(w.message)
        ]
        assert len(missing_warnings) == 0

    @pytest.mark.unit
    def test_unknown_coordinate_warns_and_skips_attribute(self, tmp_path):
        """
        For an unrecognised coordinate (not time/lat/lon), the function must
        emit a UserWarning and must NOT set a bounds attribute on the coordinate,
        since no calculation was performed.
        """
        ds = _make_monthly_ds()
        ds = ds.assign_coords(lev=xr.DataArray([100.0, 500.0, 850.0], dims=["lev"]))

        cmoriser = _bare_cmoriser(ds, tmp_path)
        bnds_required = {"lev_bnds": {"out_name": "lev", "must_have_bounds": "yes"}}

        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always")
            cmoriser.calculate_missing_bounds_variables(bnds_required)

        user_warnings = [w for w in caught if issubclass(w.category, UserWarning)]
        assert any(
            "lev_bnds" in str(w.message) for w in user_warnings
        ), "A UserWarning about lev_bnds must be emitted"
        assert (
            "bounds" not in cmoriser.ds["lev"].attrs
        ), "bounds attribute must not be set for unhandled coordinate types"

    @pytest.mark.unit
    def test_raises_value_error_when_coordinate_missing(self, tmp_path):
        """
        If the bounds variable is absent AND the corresponding coordinate is
        not in the dataset, a ValueError must be raised immediately.
        """
        ds = _make_monthly_ds()
        ds = ds.drop_vars("lat")

        cmoriser = _bare_cmoriser(ds, tmp_path)
        bnds_required = {"lat_bnds": {"out_name": "lat", "must_have_bounds": "yes"}}

        with pytest.raises(ValueError, match="lat"):
            cmoriser.calculate_missing_bounds_variables(bnds_required)

    @pytest.mark.unit
    def test_multiple_bounds_all_calculated(self, tmp_path):
        """
        When bnds_required contains multiple entries (time, lat, lon),
        all three bounds variables and their coordinate attributes must be set.
        """
        ds = _make_monthly_ds()
        cmoriser = _bare_cmoriser(ds, tmp_path)

        bnds_required = {
            "time_bnds": {"out_name": "time", "must_have_bounds": "yes"},
            "lat_bnds": {"out_name": "lat", "must_have_bounds": "yes"},
            "lon_bnds": {"out_name": "lon", "must_have_bounds": "yes"},
        }
        cmoriser.calculate_missing_bounds_variables(bnds_required)

        for bnds_var, coord in [
            ("time_bnds", "time"),
            ("lat_bnds", "lat"),
            ("lon_bnds", "lon"),
        ]:
            assert bnds_var in cmoriser.ds, f"{bnds_var} should have been created"
            assert (
                cmoriser.ds[coord].attrs.get("bounds") == bnds_var
            ), f"{coord}.attrs['bounds'] must equal '{bnds_var}'"


# ---------------------------------------------------------------------------
# Helpers for tasmax/tasmin time-resolution and update_attributes tests
# ---------------------------------------------------------------------------


def _make_cmoriser_for_update_attributes(time_values, time_attrs=None):
    """
    Build a minimal Atmosphere_CMORiser to exercise update_attributes().

    Only the attributes accessed inside update_attributes are populated.
    """
    cmoriser = object.__new__(Atmosphere_CMORiser)
    cmoriser.cmor_name = "tasmax"
    cmoriser.type_mapping = CMORiser.type_mapping

    if time_attrs is None:
        time_attrs = {"units": "days since 1850-01-01", "calendar": "standard"}

    ds = xr.Dataset(
        {
            "tasmax": xr.DataArray(
                np.array([310.0]),
                dims=["time"],
                coords={"time": (["time"], time_values, time_attrs)},
                attrs={"standard_name": "air_temperature", "units": "K"},
            )
        }
    )
    cmoriser.ds = ds

    vocab = MagicMock()
    vocab.get_required_global_attributes.return_value = {
        "source_id": "TEST",
        "experiment_id": "historical",
    }
    vocab.axes = {
        "time": {
            "out_name": "time",
            "standard_name": "time",
            "units": "days since 1850-01-01",
            "type": "double",
            "axis": "T",
        }
    }
    vocab.variable = {
        "units": "K",
        "standard_name": "air_temperature",
        "type": "real",
    }
    cmoriser.vocab = vocab

    cmoriser._check_units = MagicMock()
    cmoriser._check_calendar = MagicMock()
    cmoriser._check_range = MagicMock()

    return cmoriser


def _make_cmoriser_for_formula(daily_ds):
    """
    Build a minimal Atmosphere_CMORiser to exercise the formula path
    inside select_and_process_variables().
    """
    cmoriser = object.__new__(Atmosphere_CMORiser)
    cmoriser.cmor_name = "tasmax"
    cmoriser.type_mapping = CMORiser.type_mapping
    cmoriser.ds = daily_ds

    cmoriser.mapping = {
        "tasmax": {
            "calculation": {
                "type": "formula",
                "formula": "calculate_monthly_maximum(tasmax)",
            },
            "model_variables": ["tasmax"],
        }
    }

    vocab = MagicMock()
    vocab._get_axes.return_value = ({}, {})
    vocab._get_required_bounds_variables.return_value = ({}, {})
    vocab.variable = {"dimensions": "time"}
    vocab.axes = {"time": {"out_name": "time"}}
    cmoriser.vocab = vocab

    cmoriser.load_dataset = MagicMock()
    cmoriser.sort_time_dimension = MagicMock()
    cmoriser.remove_spurious_time_dimensions = MagicMock()

    return cmoriser


# ---------------------------------------------------------------------------
# Tests: update_attributes – decoded-time astype skip
# ---------------------------------------------------------------------------


class TestUpdateAttributesDecodedTime:
    """
    Cover the branch that skips astype() when the time coordinate is already
    decoded (cftime or datetime64), preventing TypeError when casting cftime
    objects to float64.
    """

    @pytest.mark.unit
    def test_cftime_time_not_cast_to_float(self):
        """cftime (dtype=object) time must not be cast to float64."""
        cf_time = xr.cftime_range(
            "2020-01-31", periods=1, freq="ME", calendar="gregorian"
        )
        cmoriser = _make_cmoriser_for_update_attributes(cf_time)

        assert cmoriser.ds["time"].dtype == object

        cmoriser.update_attributes()

        assert cmoriser.ds["time"].dtype == object

    @pytest.mark.unit
    def test_datetime64_time_not_cast_to_float(self):
        """numpy datetime64 time must not be cast to float64."""
        dt_time = pd.date_range("2020-01-31", periods=1, freq="ME")
        cmoriser = _make_cmoriser_for_update_attributes(dt_time)

        assert np.issubdtype(cmoriser.ds["time"].dtype, np.datetime64)

        cmoriser.update_attributes()

        assert np.issubdtype(cmoriser.ds["time"].dtype, np.datetime64)

    @pytest.mark.unit
    def test_numeric_time_is_cast_to_float(self):
        """Numeric (float64) time IS cast according to the type mapping."""
        num_time = np.array([0.0, 31.0], dtype=np.float64)

        ds = xr.Dataset(
            {
                "tasmax": xr.DataArray(
                    np.array([310.0, 311.0]),
                    dims=["time"],
                    coords={
                        "time": xr.Variable(
                            "time",
                            num_time,
                            attrs={
                                "units": "days since 1850-01-01",
                                "calendar": "standard",
                            },
                        )
                    },
                    attrs={"units": "K"},
                )
            }
        )
        cmoriser = object.__new__(Atmosphere_CMORiser)
        cmoriser.cmor_name = "tasmax"
        cmoriser.type_mapping = CMORiser.type_mapping
        cmoriser.ds = ds

        vocab = MagicMock()
        vocab.get_required_global_attributes.return_value = {}
        vocab.axes = {
            "time": {
                "out_name": "time",
                "standard_name": "time",
                "units": "days since 1850-01-01",
                "type": "double",
                "axis": "T",
            }
        }
        vocab.variable = {"units": "K", "type": "real"}
        cmoriser.vocab = vocab
        cmoriser._check_units = MagicMock()
        cmoriser._check_calendar = MagicMock()
        cmoriser._check_range = MagicMock()

        cmoriser.update_attributes()

        assert np.issubdtype(cmoriser.ds["time"].dtype, np.floating)


# ---------------------------------------------------------------------------
# Tests: select_and_process_variables – time resolution change path
# ---------------------------------------------------------------------------


class TestSelectAndProcessVariablesTimeResolutionChange:
    """
    Cover the path where a formula reduces the number of time steps
    (e.g. daily→monthly for tasmax/tasmin): self.ds must be rebuilt from
    the result rather than assigned via __setitem__, which would silently
    reindex the monthly result back to the original daily time axis.
    """

    def _make_daily_ds(self):
        daily_time = pd.date_range("2020-01-01", periods=31, freq="D")
        ds = xr.Dataset(
            {
                "tasmax": xr.DataArray(
                    np.random.default_rng(0).normal(305, 5, 31),
                    dims=["time"],
                    coords={"time": daily_time},
                    attrs={"units": "K"},
                ),
                "lat_bnds": xr.DataArray(np.array([[0.0, 1.0]]), dims=["lat", "bnds"]),
            }
        )
        ds["time"].attrs = {"units": "days since 1850-01-01", "calendar": "standard"}
        return ds

    def _monthly_result(self):
        monthly_time = pd.date_range("2020-01-31", periods=1, freq="ME")
        return xr.DataArray(
            np.array([315.0]),
            dims=["time"],
            coords={"time": monthly_time},
        )

    @pytest.mark.unit
    def test_formula_rebuilds_dataset_when_time_shrinks(self):
        """Output has monthly time length (1), not daily (31)."""
        cmoriser = _make_cmoriser_for_formula(self._make_daily_ds())

        with patch(
            "access_moppy.atmosphere.evaluate_expression",
            return_value=self._monthly_result(),
        ):
            cmoriser.select_and_process_variables()

        assert "tasmax" in cmoriser.ds
        assert cmoriser.ds["tasmax"].sizes["time"] == 1

    @pytest.mark.unit
    def test_formula_preserves_time_independent_vars(self):
        """Time-independent variables (lat_bnds) survive the dataset rebuild."""
        cmoriser = _make_cmoriser_for_formula(self._make_daily_ds())

        with patch(
            "access_moppy.atmosphere.evaluate_expression",
            return_value=self._monthly_result(),
        ):
            cmoriser.select_and_process_variables()

        assert "lat_bnds" in cmoriser.ds

    @pytest.mark.unit
    def test_formula_restores_original_time_attrs(self):
        """Original time attrs (units/calendar) are restored on the new time coord."""
        cmoriser = _make_cmoriser_for_formula(self._make_daily_ds())

        with patch(
            "access_moppy.atmosphere.evaluate_expression",
            return_value=self._monthly_result(),
        ):
            cmoriser.select_and_process_variables()

        assert cmoriser.ds["time"].attrs.get("units") == "days since 1850-01-01"
        assert cmoriser.ds["time"].attrs.get("calendar") == "standard"

    @pytest.mark.unit
    def test_formula_same_time_length_uses_setitem(self):
        """When formula returns same number of time steps, __setitem__ path is used."""
        monthly_time = pd.date_range("2020-01-01", periods=12, freq="MS")
        monthly_ds = xr.Dataset(
            {
                "tasmax": xr.DataArray(
                    np.random.default_rng(3).normal(305, 5, 12),
                    dims=["time"],
                    coords={"time": monthly_time},
                    attrs={"units": "K"},
                )
            }
        )
        monthly_ds["time"].attrs = {"units": "days since 1850-01-01"}

        same_result = xr.DataArray(
            np.random.default_rng(4).normal(305, 5, 12),
            dims=["time"],
            coords={"time": monthly_time},
        )

        cmoriser = _make_cmoriser_for_formula(monthly_ds)

        with patch(
            "access_moppy.atmosphere.evaluate_expression",
            return_value=same_result,
        ):
            cmoriser.select_and_process_variables()

        assert cmoriser.ds["tasmax"].sizes["time"] == 12


class TestSoilDepthDimension:
    """
    Ensure that tsl (soil temperature) gets its soil_model_level_number
    dimension replaced with actual depth values in metres via calc_tsl,
    so that the CMIP6 sdepth unit check (m) and the transpose both succeed.

    Regression test for two cascading errors when running Lmon.tsl:
      1. ValueError: Dimensions {'depth'} do not exist. Expected one or more of
         ('time', 'soil_model_level_number', 'lat', 'lon')
      2. ValueError: Mismatch units for depth: 1 != m
    """

    @pytest.mark.unit
    def test_calc_tsl_replaces_level_with_depth_metres(self, tmp_path):
        """
        calc_tsl must swap soil_model_level_number for a 'depth' coordinate
        whose values are the CABLE layer mid-point depths in metres.

        This is the unit test for the calc_tsl function itself.
        """
        from access_moppy.derivations.calc_land import calc_tsl

        try:
            import xarray as xr
        except ImportError:
            pytest.skip("xarray not available")

        nz = 6
        # Build a minimal DataArray with soil_model_level_number as a dim
        da = xr.DataArray(
            [[float(i)] for i in range(1, nz + 1)],
            dims=["soil_model_level_number", "x"],
            coords={"soil_model_level_number": list(range(1, nz + 1))},
        )

        result = calc_tsl(da)

        assert "depth" in result.dims, "calc_tsl must produce a 'depth' dimension"
        assert (
            "soil_model_level_number" not in result.dims
        ), "soil_model_level_number must be dropped"
        expected_depths = [
            0.0109999999403954,
            0.0509999990463257,
            0.157000005245209,
            0.438499987125397,
            1.18550002574921,
            2.87199997901917,
        ]
        import math

        for got, exp in zip(result["depth"].values, expected_depths):
            assert math.isclose(
                got, exp, rel_tol=1e-6
            ), f"depth value {got} does not match expected {exp}"

    @pytest.mark.unit
    def test_tsl_select_and_process_produces_depth_dim(self, tmp_path):
        """
        select_and_process_variables for tsl must produce a 'depth' dimension
        (not soil_model_level_number) when the formula path calls calc_tsl.

        This reproduces both crashes:
          - ValueError: Dimensions {'depth'} do not exist
          - ValueError: Mismatch units for depth: 1 != m
        """
        import tempfile

        nt, nz, nlat, nlon = 3, 6, 5, 5
        data = np.ones((nt, nz, nlat, nlon), dtype=np.float32) * 280.0

        ds = xr.Dataset(
            {
                "fld_s08i225": (
                    ["time", "soil_model_level_number", "lat", "lon"],
                    data,
                    {"units": "K"},
                )
            },
            coords={
                "time": np.arange(nt, dtype=float),
                "soil_model_level_number": np.arange(1, nz + 1, dtype=float),
                "lat": np.linspace(-90, 90, nlat),
                "lon": np.linspace(0, 360, nlon),
            },
        )

        vocab = MagicMock()
        vocab.variable = {
            "dimensions": "longitude latitude sdepth time",
            "units": "K",
            "type": "double",
        }
        vocab.axes = {
            "longitude": {"out_name": "lon"},
            "latitude": {"out_name": "lat"},
            "sdepth": {"out_name": "depth"},
            "time": {"out_name": "time"},
        }
        # Formula path: calc_tsl already produces 'depth', rename map is a no-op
        vocab._get_axes.return_value = (
            {"sdepth": {"out_name": "depth"}},
            {"depth": "depth"},
        )
        vocab._get_required_bounds_variables.return_value = ({}, {})

        mapping = {
            "tsl": {
                "model_variables": ["fld_s08i225"],
                "calculation": {
                    "type": "formula",
                    "operation": "calc_tsl",
                    "args": ["fld_s08i225"],
                },
                "dimensions": {
                    "time": "time",
                    "depth": "depth",
                    "lat": "lat",
                    "lon": "lon",
                },
            }
        }

        cmoriser = Atmosphere_CMORiser(
            input_data=ds,
            output_path=str(tmp_path or tempfile.mkdtemp()),
            vocab=vocab,
            variable_mapping=mapping,
            compound_name="Lmon.tsl",
            validate_frequency=False,
            enable_chunking=False,
            enable_compression=False,
        )

        with patch.object(cmoriser, "load_dataset"):
            cmoriser.ds = ds.copy()
            # Must not raise ValueError about missing 'depth' dim or units mismatch
            cmoriser.select_and_process_variables()

        assert (
            "depth" in cmoriser.ds["tsl"].dims
        ), "tsl must have 'depth' as a dimension after processing"
        assert (
            "soil_model_level_number" not in cmoriser.ds["tsl"].dims
        ), "soil_model_level_number must have been replaced by depth"
        # depth coordinate values must be in metres (not integer level indices)
        depth_vals = cmoriser.ds["tsl"]["depth"].values
        assert all(
            v < 10.0 for v in depth_vals
        ), "depth values must be in metres (< 10 m), not level indices"

    @pytest.mark.unit
    def test_calc_tsl_no_explicit_coord_uses_sequential_levels(self):
        """
        When soil_model_level_number is a dimension but has no explicit
        coordinate array, calc_tsl must fall back to sequential indices
        (1, 2, 3, ...) to look up the depth values.

        Covers the else-branch:
            level_size = result.sizes["soil_model_level_number"]
            level_values = list(range(1, level_size + 1))
        """
        from access_moppy.derivations.calc_land import calc_tsl

        try:
            import xarray as xr
        except ImportError:
            pytest.skip("xarray not available")

        nz = 6
        # No coords= argument → soil_model_level_number is a bare dimension
        da = xr.DataArray(
            [[float(i)] for i in range(nz)],
            dims=["soil_model_level_number", "x"],
        )

        result = calc_tsl(da)

        assert "depth" in result.dims
        assert "soil_model_level_number" not in result.dims
        import math

        expected_depths = [
            0.0109999999403954,
            0.0509999990463257,
            0.157000005245209,
            0.438499987125397,
            1.18550002574921,
            2.87199997901917,
        ]
        for got, exp in zip(result["depth"].values, expected_depths):
            assert math.isclose(
                got, exp, rel_tol=1e-6
            ), f"depth value {got} does not match expected {exp}"


# ---------------------------------------------------------------------------
# Tests for update_attributes() bounds cleanup (CF §7.1)
# ---------------------------------------------------------------------------


class TestUpdateAttributesBndsCleanup:
    """
    After update_attributes(), _bnds variables must:
    - carry units/long_name from their parent coordinate
    - have _FillValue and coordinates stripped
    """

    def _make_bnds_cmoriser(self, ds, tmp_path):
        vocab = MagicMock()
        vocab.variable = {"dimensions": "lev lat lon", "units": "m", "type": "double"}
        vocab.axes = {
            "lev": {
                "out_name": "lev",
                "units": "m",
                "long_name": "height above sea level",
            },
            "b": {
                "out_name": "b",
                "units": "1",
                "long_name": "vertical coordinate formula term: b(k)",
            },
            "lat": {"out_name": "lat", "units": "degrees_north"},
            "lon": {"out_name": "lon", "units": "degrees_east"},
        }
        vocab.get_required_global_attributes.return_value = {}
        vocab._get_axes.return_value = ([], {})
        vocab._get_required_bounds_variables.return_value = ({}, {})
        mapping = {
            "zfull": {"model_variables": ["zfull"], "calculation": {"type": "direct"}}
        }
        cmoriser = Atmosphere_CMORiser(
            input_data=ds,
            output_path=str(tmp_path),
            vocab=vocab,
            variable_mapping=mapping,
            compound_name="fx.zfull",
            validate_frequency=False,
            enable_chunking=False,
            enable_compression=False,
        )
        cmoriser.ds = ds.copy()
        return cmoriser

    @pytest.mark.unit
    def test_bnds_vars_get_parent_units_and_stale_attrs_stripped(self, tmp_path):
        """b_bnds gets units from parent b; _FillValue and coordinates removed."""
        nlev, nlat, nlon = 5, 4, 4
        rng = np.random.default_rng(0)
        ds = xr.Dataset(
            {
                "zfull": (
                    ["lev", "lat", "lon"],
                    rng.random((nlev, nlat, nlon)),
                    {"units": "m"},
                ),
                "b": (
                    ["lev"],
                    np.linspace(0, 1, nlev),
                    {
                        "units": "1",
                        "long_name": "vertical coordinate formula term: b(k)",
                    },
                ),
                "b_bnds": (
                    ["lev", "bnds"],
                    np.tile(np.linspace(0, 1, nlev), (2, 1)).T,
                    {
                        "_FillValue": float("nan"),
                        "coordinates": "sigma_theta theta_level_height",
                        "units": "stale_units",
                    },
                ),
            },
            coords={
                "lev": np.linspace(0, 1, nlev),
                "lat": np.linspace(-90, 90, nlat),
                "lon": np.linspace(0, 360, nlon, endpoint=False),
                "bnds": [0, 1],
            },
        )
        cmoriser = self._make_bnds_cmoriser(ds, tmp_path)

        with (
            patch.object(cmoriser, "_check_units"),
            patch.object(cmoriser, "_check_calendar"),
            patch.object(cmoriser, "_check_range"),
        ):
            cmoriser.update_attributes()

        bnds = cmoriser.ds["b_bnds"]
        assert "_FillValue" not in bnds.attrs
        assert "coordinates" not in bnds.attrs
        assert bnds.attrs.get("units") == "1"
        assert bnds.attrs.get("long_name") == "vertical coordinate formula term: b(k)"

    @pytest.mark.unit
    def test_bnds_var_without_parent_gets_empty_attrs(self, tmp_path):
        """_bnds variable with no matching parent coord gets empty attrs (not error)."""
        nlev, nlat, nlon = 3, 4, 4
        rng = np.random.default_rng(1)
        ds = xr.Dataset(
            {
                "zfull": (
                    ["lev", "lat", "lon"],
                    rng.random((nlev, nlat, nlon)),
                    {"units": "m"},
                ),
                "orphan_bnds": (
                    ["lev", "bnds"],
                    np.zeros((nlev, 2)),
                    {"_FillValue": float("nan"), "units": "stale"},
                ),
            },
            coords={
                "lev": np.arange(nlev, dtype=float),
                "lat": np.linspace(-90, 90, nlat),
                "lon": np.linspace(0, 360, nlon, endpoint=False),
                "bnds": [0, 1],
            },
        )
        cmoriser = self._make_bnds_cmoriser(ds, tmp_path)

        with (
            patch.object(cmoriser, "_check_units"),
            patch.object(cmoriser, "_check_calendar"),
            patch.object(cmoriser, "_check_range"),
        ):
            cmoriser.update_attributes()

        # No parent 'orphan' in ds → attrs replaced with {}
        assert cmoriser.ds["orphan_bnds"].attrs == {}


class TestCalculateMissingBoundsDataVarBranch:
    """Cover the 'coord_name in self.ds.data_vars' branch of calculate_missing_bounds_variables."""

    @pytest.mark.unit
    def test_bounds_attr_set_on_data_var_not_coord(self, tmp_path):
        """bounds attr is written even when parent is a data_var, not a coord."""
        nlev = 5
        ds = xr.Dataset(
            {
                "b": (["lev"], np.linspace(0, 1, nlev), {"units": "1"}),
                "b_bnds": (
                    ["lev", "bnds"],
                    np.tile(np.linspace(0, 1, nlev), (2, 1)).T,
                ),
            },
            coords={"lev": np.arange(nlev, dtype=float), "bnds": [0, 1]},
        )
        vocab = _make_vocab()
        mapping = {"b": {"model_variables": ["b"], "calculation": {"type": "direct"}}}
        cmoriser = Atmosphere_CMORiser(
            input_data=ds,
            output_path=str(tmp_path),
            vocab=vocab,
            variable_mapping=mapping,
            compound_name="fx.b",
            validate_frequency=False,
            enable_chunking=False,
            enable_compression=False,
        )
        cmoriser.ds = ds.copy()

        # b is a data_var (not a coord)
        assert "b" in cmoriser.ds.data_vars
        assert "b" not in cmoriser.ds.coords

        cmoriser.calculate_missing_bounds_variables({"b_bnds": {"out_name": "b"}})

        assert cmoriser.ds["b"].attrs.get("bounds") == "b_bnds"
