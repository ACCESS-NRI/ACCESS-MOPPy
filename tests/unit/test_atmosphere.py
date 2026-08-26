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
    def test_missing_coord_error_lists_available_coordinates(self, tmp_path):
        """Missing-coordinate error must list the coordinates actually present."""
        ds = _make_monthly_ds()
        ds = ds.drop_vars("lat")

        cmoriser = _bare_cmoriser(ds, tmp_path)
        bnds_required = {"lat_bnds": {"out_name": "lat", "must_have_bounds": "yes"}}

        with pytest.raises(ValueError, match="Available coordinates") as exc_info:
            cmoriser.calculate_missing_bounds_variables(bnds_required)

        msg = str(exc_info.value)
        assert "time" in msg
        assert "lon" in msg

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


def _make_cmoriser_for_formula(
    daily_ds,
    *,
    cmor_name="tasmax",
    compound_name="Amon.tasmax",
    model_variable="tasmax",
):
    """
    Build a minimal Atmosphere_CMORiser to exercise the formula path
    inside select_and_process_variables().
    """
    cmoriser = object.__new__(Atmosphere_CMORiser)
    cmoriser.cmor_name = cmor_name
    cmoriser.compound_name = compound_name
    cmoriser.type_mapping = CMORiser.type_mapping
    cmoriser.ds = daily_ds

    cmoriser.mapping = {
        cmor_name: {
            "calculation": {
                "type": "formula",
                "formula": f"calculate_monthly_{'maximum' if cmor_name == 'tasmax' else 'minimum'}({model_variable})",
            },
            "model_variables": [model_variable],
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
# Tests for _replace_time_bounds_with_computed()
# ---------------------------------------------------------------------------


class TestReplaceTimeBoundsWithComputed:
    """
    Unit tests for Atmosphere_CMORiser._replace_time_bounds_with_computed().

    Background: the UM archive packs every atmosphere/land variable for a
    month into one file, so a raw 'time_bnds' opened via open_mfdataset
    carries a Dask graph tying every timestep's bounds to a single
    multi-file merge step. Unlike the main data variable, that graph cannot
    be pruned down to one output chunk's own files by dask.cull(), so every
    chunked write re-reads the *entire* input file list just for these two
    numbers per timestep (measured: 56% of total task time on a real
    348-file/3-chunk run). This method replaces a Dask-backed time_bnds
    with one computed directly from the (already resident) time coordinate,
    which is numerically identical (verified byte-for-byte against this
    model's native monthly output, including leap years) but avoids that
    graph entirely.

    Scenarios covered:
      - Dask-backed time_bnds is replaced with numerically-correct values
      - Replaced time_bnds is no longer Dask-backed (graph is gone)
      - Replaced time_bnds has attrs cleared to {} (matches raw model
        output's empty-attrs convention, so published files are unchanged)
      - Already-eager (non-Dask) time_bnds is left untouched
      - No 'bounds' attribute on time -> no-op
      - 'bounds' attribute points at a variable absent from ds -> no-op
      - No 'time' coordinate at all (e.g. fx variable) -> no-op, no raise
    """

    @pytest.mark.unit
    def test_dask_backed_time_bnds_is_replaced_with_correct_values(self, tmp_path):
        ds = _make_monthly_ds()
        n = ds.sizes["time"]
        from access_moppy.utilities import calculate_time_bounds

        expected = calculate_time_bounds(ds, time_coord="time", bnds_name="bnds")

        raw_bnds = np.zeros((n, 2), dtype=np.float64)  # deliberately wrong values
        ds["time_bnds"] = xr.DataArray(raw_bnds, dims=["time", "bnds"]).chunk(
            {"time": 1}
        )
        ds["time"].attrs["bounds"] = "time_bnds"

        cmoriser = _bare_cmoriser(ds, tmp_path)
        cmoriser._replace_time_bounds_with_computed()

        np.testing.assert_array_equal(
            cmoriser.ds["time_bnds"].values,
            expected.values,
            err_msg="Replaced time_bnds must match calculate_time_bounds() output",
        )

    @pytest.mark.unit
    def test_replaced_time_bnds_is_no_longer_dask_backed(self, tmp_path):
        import dask.array as da

        ds = _make_monthly_ds()
        n = ds.sizes["time"]
        ds["time_bnds"] = xr.DataArray(np.zeros((n, 2)), dims=["time", "bnds"]).chunk(
            {"time": 1}
        )
        ds["time"].attrs["bounds"] = "time_bnds"

        cmoriser = _bare_cmoriser(ds, tmp_path)
        cmoriser._replace_time_bounds_with_computed()

        assert not isinstance(cmoriser.ds["time_bnds"].data, da.Array), (
            "Replaced time_bnds must no longer carry the un-cullable "
            "open_mfdataset graph"
        )

    @pytest.mark.unit
    def test_replaced_time_bnds_attrs_cleared(self, tmp_path):
        ds = _make_monthly_ds()
        n = ds.sizes["time"]
        ds["time_bnds"] = xr.DataArray(np.zeros((n, 2)), dims=["time", "bnds"]).chunk(
            {"time": 1}
        )
        ds["time"].attrs["bounds"] = "time_bnds"

        cmoriser = _bare_cmoriser(ds, tmp_path)
        cmoriser._replace_time_bounds_with_computed()

        assert cmoriser.ds["time_bnds"].attrs == {}, (
            "attrs must be cleared to match the empty-attrs convention of "
            "the raw model output, so published files are byte-for-byte "
            "unchanged by this optimisation"
        )

    @pytest.mark.unit
    def test_already_eager_time_bnds_left_untouched(self, tmp_path):
        """
        When time_bnds is already eager (e.g. just synthesized moments
        earlier by calculate_missing_bounds_variables()), there is nothing
        to gain -- it must be left exactly as-is, not recomputed.
        """
        ds = _make_monthly_ds()
        n = ds.sizes["time"]
        sentinel = np.full((n, 2), 999.0)
        ds["time_bnds"] = xr.DataArray(sentinel, dims=["time", "bnds"])  # eager
        ds["time"].attrs["bounds"] = "time_bnds"

        cmoriser = _bare_cmoriser(ds, tmp_path)
        cmoriser._replace_time_bounds_with_computed()

        np.testing.assert_array_equal(
            cmoriser.ds["time_bnds"].values,
            sentinel,
            err_msg="Already-eager time_bnds must not be recomputed",
        )

    @pytest.mark.unit
    def test_noop_when_time_has_no_bounds_attribute(self, tmp_path):
        ds = _make_monthly_ds()
        assert "bounds" not in ds["time"].attrs

        cmoriser = _bare_cmoriser(ds, tmp_path)
        cmoriser._replace_time_bounds_with_computed()  # must not raise

        assert "time_bnds" not in cmoriser.ds

    @pytest.mark.unit
    def test_noop_when_bounds_variable_absent_from_dataset(self, tmp_path):
        ds = _make_monthly_ds()
        ds["time"].attrs["bounds"] = "time_bnds"  # points at a var that doesn't exist

        cmoriser = _bare_cmoriser(ds, tmp_path)
        cmoriser._replace_time_bounds_with_computed()  # must not raise

        assert "time_bnds" not in cmoriser.ds

    @pytest.mark.unit
    def test_noop_when_no_time_coordinate(self, tmp_path):
        """Fixed (fx) variables have no time coordinate at all."""
        ds = xr.Dataset(
            {"orog": (["lat", "lon"], np.ones((3, 3)))},
            coords={"lat": [0, 1, 2], "lon": [0, 1, 2]},
        )
        cmoriser = _bare_cmoriser(ds, tmp_path)
        cmoriser._replace_time_bounds_with_computed()  # must not raise


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
    def test_model_native_attributes_dropped(self):
        """Model-native attributes inherited via rename (grid_mapping with no
        container variable, um_stash_source) must be dropped from the output."""
        cf_time = xr.cftime_range(
            "2020-01-31", periods=1, freq="ME", calendar="gregorian"
        )
        cmoriser = _make_cmoriser_for_update_attributes(cf_time)
        cmoriser.ds["tasmax"].attrs["grid_mapping"] = "latitude_longitude"
        cmoriser.ds["tasmax"].attrs["um_stash_source"] = "m01s03i236"

        cmoriser.update_attributes()

        assert "grid_mapping" not in cmoriser.ds["tasmax"].attrs
        assert "um_stash_source" not in cmoriser.ds["tasmax"].attrs

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

    @pytest.mark.unit
    def test_character_coord_not_cast_to_float(self):
        """Character-type coordinates (e.g. vegtype) must NOT be cast to float."""
        veg_types = np.array(
            ["Evergreen_Needleleaf", "Evergreen_Broadleaf", "", "Shrub"],
            dtype=str,
        )
        ds = xr.Dataset(
            {
                "landCoverFrac": xr.DataArray(
                    np.ones((1, 4), dtype=np.float32),
                    dims=["time", "type"],
                    coords={
                        "time": xr.Variable(
                            "time",
                            np.array([0.0]),
                            attrs={
                                "units": "days since 1850-01-01",
                                "calendar": "standard",
                            },
                        ),
                        "type": ("type", veg_types),
                    },
                    attrs={"units": "%"},
                )
            }
        )

        cmoriser = object.__new__(Atmosphere_CMORiser)
        cmoriser.cmor_name = "landCoverFrac"
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
            },
            "vegtype": {
                "out_name": "type",
                "standard_name": "area_type",
                "type": "character",
            },
        }
        vocab.variable = {"units": "%", "type": "real"}
        cmoriser.vocab = vocab
        cmoriser._check_units = MagicMock()
        cmoriser._check_calendar = MagicMock()
        cmoriser._check_range = MagicMock()

        original_dtype = ds["type"].dtype

        cmoriser.update_attributes()

        # dtype must remain unchanged (string/unicode, NOT float64)
        assert not np.issubdtype(cmoriser.ds["type"].dtype, np.floating)
        assert cmoriser.ds["type"].dtype == original_dtype

    @pytest.mark.unit
    def test_days_since_placeholder_replaced_from_encoding(self):
        """'days since ?' placeholder must be replaced by units from encoding.

        When xr.decode_cf() processes pre-1582 proleptic_gregorian time, it
        converts numeric values to cftime objects and moves 'units'/'calendar'
        from attrs into encoding.  update_attributes() must still replace the
        CMIP6 CMOR-table placeholder 'days since ?' with the real units, even
        though they are now in encoding rather than attrs.
        """
        cf_time = xr.cftime_range(
            "0202-01-15", periods=2, freq="ME", calendar="proleptic_gregorian"
        )
        ds = xr.Dataset(
            {
                "tasmax": xr.DataArray(
                    np.array([310.0, 311.0]),
                    dims=["time"],
                    coords={"time": (["time"], cf_time, {})},
                    attrs={"units": "K"},
                )
            }
        )
        # Simulate the post-decode_cf state: units only in encoding, not attrs
        ds["time"].encoding["units"] = "days since 0001-01-01 00:00"
        ds["time"].encoding["calendar"] = "proleptic_gregorian"

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
                "units": "days since ?",  # CMIP6 CMOR table placeholder
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

        result_units = cmoriser.ds["time"].attrs.get("units")
        assert result_units != "days since ?", (
            "Placeholder 'days since ?' was not replaced; "
            "encoding-based units were not picked up."
        )
        assert result_units == "days since 0001-01-01 00:00"


# ---------------------------------------------------------------------------
# Tests: update_attributes – output dtype and _FillValue/missing_value
# precision (CMIP7 tables carry no per-variable "type", unlike CMIP6, so the
# cast must fall back to the source dtype instead of hardcoding float64; and
# whenever a cast does happen, the fill/missing value attrs must be re-cast
# to match so they don't drift out of sync with the data, e.g.
# float32(1e20) != float64(1e20) after promotion).
# ---------------------------------------------------------------------------


class TestUpdateAttributesDtypeAndFillValue:
    @pytest.mark.unit
    def test_missing_table_type_preserves_source_dtype(self):
        """No 'type' in the CMOR table entry: keep the source dtype (float32)
        instead of falling back to float64, matching CMIP7 table behaviour."""
        cf_time = xr.cftime_range(
            "2020-01-31", periods=1, freq="ME", calendar="gregorian"
        )
        cmoriser = _make_cmoriser_for_update_attributes(cf_time)
        cmoriser.ds["tasmax"] = cmoriser.ds["tasmax"].astype(np.float32)
        del cmoriser.vocab.variable["type"]

        assert cmoriser.ds["tasmax"].dtype == np.float32

        cmoriser.update_attributes()

        assert cmoriser.ds["tasmax"].dtype == np.float32

    @pytest.mark.unit
    def test_explicit_double_type_upcasts_and_recasts_fill_value(self):
        """When the table does specify 'double', the data is upcast as
        before, and _FillValue/missing_value are re-cast to float64 too so
        they stay bit-consistent with the now-float64 data."""
        cf_time = xr.cftime_range(
            "2020-01-31", periods=1, freq="ME", calendar="gregorian"
        )
        cmoriser = _make_cmoriser_for_update_attributes(cf_time)
        cmoriser.ds["tasmax"] = cmoriser.ds["tasmax"].astype(np.float32)
        cmoriser.ds["tasmax"].attrs["_FillValue"] = np.float32(1e20)
        cmoriser.ds["tasmax"].attrs["missing_value"] = np.float32(1e20)
        cmoriser.vocab.variable["type"] = "double"

        cmoriser.update_attributes()

        assert cmoriser.ds["tasmax"].dtype == np.float64
        assert isinstance(cmoriser.ds["tasmax"].attrs["_FillValue"], np.float64)
        assert isinstance(cmoriser.ds["tasmax"].attrs["missing_value"], np.float64)
        # The re-cast fill value must equal the source float32 value promoted
        # to float64 (the same promotion the data itself just went through),
        # not the naive float64 literal 1e20.
        assert cmoriser.ds["tasmax"].attrs["_FillValue"] == np.float64(np.float32(1e20))

    @pytest.mark.unit
    def test_source_range_attributes_dropped_unless_table_declares_them(self):
        """valid_range is never a CMOR attribute, and valid_min/valid_max are
        the table's to declare, so both must be stripped when they were only
        inherited from the source variable."""
        cf_time = xr.cftime_range(
            "2020-01-31", periods=1, freq="ME", calendar="gregorian"
        )
        cmoriser = _make_cmoriser_for_update_attributes(cf_time)
        cmoriser.ds["tasmax"].attrs["valid_range"] = np.array([-1e20, 1e20])
        cmoriser.ds["tasmax"].attrs["valid_min"] = -1e20
        cmoriser.ds["tasmax"].attrs["valid_max"] = 1e20

        cmoriser.update_attributes()

        attrs = cmoriser.ds["tasmax"].attrs
        assert "valid_range" not in attrs
        assert "valid_min" not in attrs
        assert "valid_max" not in attrs

    @pytest.mark.unit
    def test_valid_min_max_cast_to_target_dtype_and_range_checked(self):
        """valid_min/valid_max from the CMOR table must be cast to the same
        target dtype as the data before being passed to _check_range()."""
        cf_time = xr.cftime_range(
            "2020-01-31", periods=1, freq="ME", calendar="gregorian"
        )
        cmoriser = _make_cmoriser_for_update_attributes(cf_time)
        cmoriser.ds["tasmax"] = cmoriser.ds["tasmax"].astype(np.float32)
        del cmoriser.vocab.variable["type"]
        cmoriser.vocab.variable["valid_min"] = 173.0
        cmoriser.vocab.variable["valid_max"] = 373.0

        cmoriser.update_attributes()

        cmoriser._check_range.assert_called_once()
        called_name, vmin, vmax = cmoriser._check_range.call_args[0]
        assert called_name == "tasmax"
        assert isinstance(vmin, np.float32)
        assert isinstance(vmax, np.float32)
        assert vmin == np.float32(173.0)
        assert vmax == np.float32(373.0)


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
    @pytest.mark.parametrize(
        ("cmor_name", "compound_name"),
        [
            ("tasmax", "day.tasmax"),
            ("tasmin", "atmos.tas.tmin-h2m-hxy-u.day.glb"),
        ],
    )
    def test_daily_extrema_preserve_daily_time_axis(self, cmor_name, compound_name):
        source_name = "fld_s03i236"
        daily_ds = self._make_daily_ds().rename({"tasmax": source_name})
        cmoriser = _make_cmoriser_for_formula(
            daily_ds,
            cmor_name=cmor_name,
            compound_name=compound_name,
            model_variable=source_name,
        )

        with patch("access_moppy.atmosphere.evaluate_expression") as evaluate:
            cmoriser.select_and_process_variables()

        evaluate.assert_not_called()
        assert cmoriser.ds[cmor_name].sizes["time"] == 31
        np.testing.assert_array_equal(cmoriser.ds[cmor_name]["time"], daily_ds["time"])

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

    @pytest.mark.unit
    def test_formula_same_time_length_but_shifted_labels_rebuilds(self):
        """Shifted time labels must not be aligned away to all-NaN values."""
        monthly_time = pd.date_range("2020-01-01", periods=12, freq="MS")
        monthly_ds = xr.Dataset(
            {
                "tasmax": xr.DataArray(
                    np.random.default_rng(5).normal(305, 5, 12),
                    dims=["time"],
                    coords={"time": monthly_time},
                    attrs={"units": "K"},
                )
            }
        )
        monthly_ds["time"].attrs = {"units": "days since 1850-01-01"}

        shifted_time = pd.date_range("2020-01-16", periods=12, freq="MS")
        shifted_result = xr.DataArray(
            np.linspace(290.0, 301.0, 12),
            dims=["time"],
            coords={"time": shifted_time},
        )

        cmoriser = _make_cmoriser_for_formula(monthly_ds)

        with patch(
            "access_moppy.atmosphere.evaluate_expression",
            return_value=shifted_result,
        ):
            cmoriser.select_and_process_variables()

        np.testing.assert_allclose(cmoriser.ds["tasmax"].values, shifted_result.values)
        assert np.array_equal(cmoriser.ds["time"].values, shifted_result["time"].values)

    @pytest.mark.unit
    @pytest.mark.parametrize("cmor_name", ["tasmax", "tasmin"])
    def test_monthly_extrema_are_mean_of_daily_extrema(self, cmor_name):
        """Monthly tasmax/tasmin = mean over days of the daily extrema (#644).

        The mapping feeds the model's within-day extremum (fld_s03i236_max/_min)
        through calculate_monthly_mean; the result must be the monthly mean of
        that field — not a monthly max/min, and not a copy of the input.
        """
        source_name = f"fld_s03i236_{'max' if cmor_name == 'tasmax' else 'min'}"
        daily_time = pd.date_range("2020-01-01", periods=60, freq="D")
        daily = xr.DataArray(
            np.random.default_rng(8).normal(305, 5, 60),
            dims=["time"],
            coords={"time": daily_time},
            attrs={"units": "K"},
        )
        ds = xr.Dataset({source_name: daily})
        ds["time"].attrs = {"units": "days since 1850-01-01", "calendar": "standard"}

        cmoriser = _make_cmoriser_for_formula(
            ds,
            cmor_name=cmor_name,
            compound_name=f"Amon.{cmor_name}",
            model_variable=source_name,
        )
        cmoriser.mapping[cmor_name]["calculation"] = {
            "type": "formula",
            "operation": "calculate_monthly_mean",
            "operands": [source_name],
        }

        cmoriser.select_and_process_variables()

        expected = daily.resample(time="ME").mean()
        assert cmoriser.ds[cmor_name].sizes["time"] == 2
        np.testing.assert_allclose(
            cmoriser.ds[cmor_name].values, expected.values, rtol=1e-12
        )

    @pytest.mark.unit
    def test_formula_time_compare_exception_falls_back_to_rebuild(self):
        """If time-label comparison errors, fallback should still rebuild dataset."""
        monthly_time = pd.date_range("2020-01-01", periods=12, freq="MS")
        monthly_ds = xr.Dataset(
            {
                "tasmax": xr.DataArray(
                    np.random.default_rng(6).normal(305, 5, 12),
                    dims=["time"],
                    coords={"time": monthly_time},
                    attrs={"units": "K"},
                )
            }
        )
        monthly_ds["time"].attrs = {"units": "days since 1850-01-01"}

        same_size_result = xr.DataArray(
            np.linspace(280.0, 291.0, 12),
            dims=["time"],
            coords={"time": monthly_time},
        )

        cmoriser = _make_cmoriser_for_formula(monthly_ds)

        with (
            patch(
                "access_moppy.atmosphere.evaluate_expression",
                return_value=same_size_result,
            ),
            patch(
                "access_moppy.atmosphere.np.array_equal",
                side_effect=RuntimeError("boom"),
            ),
        ):
            cmoriser.select_and_process_variables()

        np.testing.assert_allclose(
            cmoriser.ds["tasmax"].values,
            same_size_result.values,
        )


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
    - not repeat their parent coordinate's units/standard_name/axis/calendar
      (CF §7.1; published CMOR output leaves these bounds attribute-free)
    - have _FillValue and coordinates stripped
    A parametric vertical coordinate's bounds (lev_bnds for hybrid height) keeps
    standard_name/units, which CMOR also writes.
    """

    def _make_bnds_cmoriser(self, ds, tmp_path):
        vocab = MagicMock()
        vocab.variable = {"dimensions": "lev lat lon", "units": "m", "type": "double"}
        vocab.axes = {
            "lev": {
                "out_name": "lev",
                "units": "m",
                "long_name": "height above sea level",
                "z_bounds_factors": "a: lev_bnds b: b_bnds orog: orog",
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
        # units belongs on the parent 'b' alone (CF §7.1); the stale source value
        # must not survive either
        assert "units" not in bnds.attrs
        assert bnds.attrs == {}

    @pytest.mark.unit
    def test_bnds_upcast_to_match_parent_coordinate_dtype(self, tmp_path):
        """A float32 b_bnds must be upcast to double alongside its parent 'b'
        coordinate, whose CMOR-declared "type" defaults to "double" (observed
        real-world mismatch: coordinate written as double, its bounds as
        float, in the same output file)."""
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
                    np.tile(np.linspace(0, 1, nlev), (2, 1)).T.astype(np.float32),
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

        assert cmoriser.ds["b_bnds"].dtype == np.float64

    @pytest.mark.unit
    def test_b_bnds_descending_pairs_are_normalized(self, tmp_path):
        """Descending b_bnds pairs are normalized to ascending [min, max]."""
        nlev, nlat, nlon = 5, 4, 4
        rng = np.random.default_rng(3)
        b = np.array([0.99, 0.95, 0.90, 0.80, 0.65], dtype=float)
        b_upper = np.array([1.00, 0.98, 0.93, 0.85, 0.70], dtype=float)
        b_lower = np.array([0.98, 0.93, 0.85, 0.70, 0.55], dtype=float)

        ds = xr.Dataset(
            {
                "zfull": (
                    ["lev", "lat", "lon"],
                    rng.random((nlev, nlat, nlon)),
                    {"units": "m"},
                ),
                "b": (
                    ["lev"],
                    b,
                    {
                        "units": "1",
                        "long_name": "vertical coordinate formula term: b(k)",
                    },
                ),
                # Intentionally descending within each pair: [upper, lower]
                "b_bnds": (
                    ["lev", "bnds"],
                    np.stack([b_upper, b_lower], axis=1),
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

        normalized = cmoriser.ds["b_bnds"].values
        assert np.all(normalized[:, 0] <= normalized[:, 1])
        assert np.all((b >= normalized[:, 0]) & (b <= normalized[:, 1]))

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


# Tests for stale units clearing after coordinate rename
def _make_cmoriser_with_rename(
    ds,
    cmor_name,
    axes_rename_map,
    bounds_rename_map=None,
    dimensions="time plev lat lon",
    tmp_path=None,
):
    """
    Build an Atmosphere_CMORiser whose vocab returns the given rename maps.
    The dataset is injected directly; load_dataset must be patched by the caller.
    `dimensions` must match the actual dims of the data variable so that the
    transpose step inside select_and_process_variables does not raise.
    """
    import tempfile

    out = str(tmp_path or tempfile.mkdtemp())
    vocab = MagicMock()
    vocab.variable = {"dimensions": dimensions, "units": "m", "type": "double"}
    vocab.axes = {
        "lat": {"out_name": "lat"},
        "lon": {"out_name": "lon"},
        "time": {"out_name": "time"},
        "plev": {"out_name": "plev"},
    }
    vocab._get_axes.return_value = ({}, axes_rename_map)
    vocab._get_required_bounds_variables.return_value = ({}, bounds_rename_map or {})

    mapping = {
        cmor_name: {
            "model_variables": [cmor_name],
            "calculation": {"type": "direct", "formula": cmor_name},
            "dimensions": {
                "time": "time",
                "pressure": "plev",
                "lat": "lat",
                "lon": "lon",
            },
        }
    }

    cmoriser = Atmosphere_CMORiser(
        input_data=ds,
        output_path=out,
        vocab=vocab,
        variable_mapping=mapping,
        compound_name=f"Amon.{cmor_name}",
        validate_frequency=False,
        enable_chunking=False,
        enable_compression=False,
    )
    return cmoriser


class TestStaleUnitsClearing:
    """
    Tests for the stale-units clearing loop added after self.ds.rename(rename_map)
    in select_and_process_variables (atmosphere.py).

    The loop is:
        for old_name, new_name in rename_map.items():
            if old_name != new_name and new_name in self.ds.coords:
                self.ds[new_name].attrs.pop("units", None)

    Scenarios covered:
      1. Genuinely renamed coordinate (pressure → plev, units="1") has units cleared.
      2. Identity entry (time → time) does NOT have units cleared.
      3. Multiple renames: renamed coord cleared, identity coord preserved simultaneously.
      4. Renamed target that is a data variable (not a coord) is NOT touched.
      5. Renamed coordinate with no units attribute is unaffected (no KeyError).
    """

    def _make_ds(self, with_pressure=True, time_units="days since 1850-01-01"):
        """Minimal dataset with optional pressure coord."""
        nt, nlat, nlon = 3, 5, 5
        coords = {
            "time": xr.Variable(
                "time",
                np.arange(nt, dtype=float),
                {"units": time_units, "calendar": "proleptic_gregorian"},
            ),
            "lat": np.linspace(-90, 90, nlat),
            "lon": np.linspace(0, 360, nlon),
        }
        data_vars = {
            "zg": (
                ["time", "pressure", "lat", "lon"]
                if with_pressure
                else ["time", "lat", "lon"],
                np.ones(
                    (nt, 3, nlat, nlon) if with_pressure else (nt, nlat, nlon),
                    dtype=np.float32,
                ),
                {"units": "m"},
            )
        }
        if with_pressure:
            coords["pressure"] = xr.Variable(
                "pressure", np.array([85000.0, 50000.0, 25000.0]), {"units": "1"}
            )

        return xr.Dataset(data_vars, coords=coords)

    @pytest.mark.unit
    def test_renamed_coord_units_cleared(self, tmp_path):
        """
        A coordinate renamed to a different CMIP name (pressure → plev) must have
        its stale units attribute removed so update_attributes can write "Pa" from
        the vocabulary without _check_units raising ValueError.
        """
        ds = self._make_ds()
        axes_rename_map = {"pressure": "plev", "time": "time"}

        cmoriser = _make_cmoriser_with_rename(
            ds, "zg", axes_rename_map, dimensions="time plev lat lon", tmp_path=tmp_path
        )

        with patch.object(cmoriser, "load_dataset"):
            cmoriser.ds = ds.copy()
            cmoriser.select_and_process_variables()

        assert "plev" in cmoriser.ds.coords, "pressure should have been renamed to plev"
        assert "units" not in cmoriser.ds["plev"].attrs, (
            "Stale units='1' must be cleared from plev after rename so "
            "update_attributes can assign 'Pa' from the CMIP vocabulary"
        )

    @pytest.mark.unit
    def test_identity_rename_preserves_time_units(self, tmp_path):
        """
        Identity entries in rename_map (time → time) must be skipped so that
        time.attrs['units'] = 'days since …' is preserved.

        Without this guard, update_attributes would write the literal placeholder
        'days since ?' instead of the original date string, breaking CF time parsing.
        """
        time_units = "days since 1850-01-01 00:00:00"
        ds = self._make_ds(with_pressure=False, time_units=time_units)
        axes_rename_map = {"time": "time"}  # identity

        cmoriser = _make_cmoriser_with_rename(
            ds, "zg", axes_rename_map, dimensions="time lat lon", tmp_path=tmp_path
        )
        cmoriser.mapping = {
            "zg": {
                "model_variables": ["zg"],
                "calculation": {"type": "direct", "formula": "zg"},
                "dimensions": {"time": "time", "lat": "lat", "lon": "lon"},
            }
        }

        with patch.object(cmoriser, "load_dataset"):
            cmoriser.ds = ds.copy()
            cmoriser.select_and_process_variables()

        assert cmoriser.ds["time"].attrs.get("units") == time_units, (
            "time.attrs['units'] must be preserved when rename_map contains "
            "the identity entry 'time → time'"
        )

    @pytest.mark.unit
    def test_mixed_renames_cleared_and_preserved(self, tmp_path):
        """
        When rename_map contains both a real rename (pressure → plev) and an
        identity (time → time), the real rename's units are cleared while the
        identity's units are preserved — both in the same call.
        """
        time_units = "days since 1850-01-01 00:00:00"
        ds = self._make_ds(time_units=time_units)
        axes_rename_map = {"pressure": "plev", "time": "time"}

        cmoriser = _make_cmoriser_with_rename(
            ds, "zg", axes_rename_map, dimensions="time plev lat lon", tmp_path=tmp_path
        )

        with patch.object(cmoriser, "load_dataset"):
            cmoriser.ds = ds.copy()
            cmoriser.select_and_process_variables()

        # Renamed coord: units cleared
        assert (
            "units" not in cmoriser.ds["plev"].attrs
        ), "plev.attrs['units'] must be cleared after rename from pressure"
        # Identity coord: units preserved
        assert (
            cmoriser.ds["time"].attrs.get("units") == time_units
        ), "time.attrs['units'] must not be touched by the identity rename"

    @pytest.mark.unit
    def test_renamed_data_var_units_not_cleared(self, tmp_path):
        """
        When the rename target is a data variable (not a coordinate), its units
        must NOT be cleared — the guard `new_name in self.ds.coords` prevents this.
        """
        nt, nlat, nlon = 3, 5, 5
        ds = xr.Dataset(
            {
                "zg": (
                    ["time", "lat", "lon"],
                    np.ones((nt, nlat, nlon), dtype=np.float32),
                    {"units": "m"},
                ),
                "aux_var": (["time"], np.ones(nt), {"units": "Pa"}),
            },
            coords={
                "time": xr.Variable(
                    "time",
                    np.arange(nt, dtype=float),
                    {"units": "days since 1850-01-01"},
                ),
                "lat": np.linspace(-90, 90, nlat),
                "lon": np.linspace(0, 360, nlon),
            },
        )
        # aux_var is renamed to plev_var but it's a data variable, not a coord
        axes_rename_map = {"aux_var": "plev_var"}

        cmoriser = _make_cmoriser_with_rename(
            ds, "zg", axes_rename_map, dimensions="time lat lon", tmp_path=tmp_path
        )
        cmoriser.mapping = {
            "zg": {
                "model_variables": ["zg"],
                "calculation": {"type": "direct", "formula": "zg"},
                "dimensions": {"time": "time", "lat": "lat", "lon": "lon"},
            }
        }

        with patch.object(cmoriser, "load_dataset"):
            cmoriser.ds = ds.copy()
            cmoriser.select_and_process_variables()

        if "plev_var" in cmoriser.ds:
            assert (
                cmoriser.ds["plev_var"].attrs.get("units") == "Pa"
            ), "units must not be cleared from a renamed data variable (non-coord)"

    @pytest.mark.unit
    def test_renamed_coord_without_units_does_not_raise(self, tmp_path):
        """
        If the renamed coordinate has no units attribute at all, the pop() call
        must be a no-op (not raise KeyError).
        """
        nt, nlat, nlon = 3, 5, 5
        ds = xr.Dataset(
            {
                "zg": (
                    ["time", "pressure", "lat", "lon"],
                    np.ones((nt, 3, nlat, nlon), dtype=np.float32),
                    {"units": "m"},
                )
            },
            coords={
                "time": xr.Variable(
                    "time",
                    np.arange(nt, dtype=float),
                    {"units": "days since 1850-01-01"},
                ),
                "pressure": xr.Variable(
                    "pressure", np.array([85000.0, 50000.0, 25000.0]), {}
                ),  # no units
                "lat": np.linspace(-90, 90, nlat),
                "lon": np.linspace(0, 360, nlon),
            },
        )
        axes_rename_map = {"pressure": "plev", "time": "time"}

        cmoriser = _make_cmoriser_with_rename(
            ds, "zg", axes_rename_map, dimensions="time plev lat lon", tmp_path=tmp_path
        )

        with patch.object(cmoriser, "load_dataset"):
            cmoriser.ds = ds.copy()
            # Must not raise KeyError when the coord has no units attribute
            cmoriser.select_and_process_variables()

        assert "plev" in cmoriser.ds.coords


class TestMissingModelVarValidation:
    """Tests for the early missing-variable check in select_and_process_variables.

    After load_dataset, if a required model variable is absent from self.ds,
    a KeyError must be raised immediately with a diagnostic message — rather
    than the cryptic ValueError at the later rename() step.
    """

    def _bounds_only_ds(self):
        """Dataset with only bounds/coordinates, no data variable."""
        return xr.Dataset(
            {
                "time_bnds": (["time", "bnds"], np.zeros((3, 2))),
                "lat_bnds": (["lat", "bnds"], np.zeros((5, 2))),
            },
            coords={
                "time": np.arange(3, dtype=float),
                "lat": np.linspace(-90, 90, 5),
                "lon": np.linspace(0, 360, 8, endpoint=False),
            },
        )

    @pytest.mark.unit
    def test_missing_model_var_raises_key_error(self, tmp_path):
        """KeyError is raised when the required model variable is absent after load_dataset."""
        ds = self._bounds_only_ds()
        cmoriser = _make_cmoriser(ds, "zg", tmp_path=tmp_path)
        cmoriser.mapping = {
            "zg": {
                "model_variables": ["fld_s16i201"],
                "calculation": {"type": "direct", "formula": "fld_s16i201"},
            }
        }
        with patch.object(cmoriser, "load_dataset"):
            cmoriser.ds = ds.copy()
            with pytest.raises(KeyError, match="fld_s16i201"):
                cmoriser.select_and_process_variables()

    @pytest.mark.unit
    def test_error_message_lists_available_vars(self, tmp_path):
        """The KeyError must include the available data variables."""
        ds = self._bounds_only_ds()
        cmoriser = _make_cmoriser(ds, "zg", tmp_path=tmp_path)
        cmoriser.mapping = {
            "zg": {
                "model_variables": ["fld_s16i201"],
                "calculation": {"type": "direct", "formula": "fld_s16i201"},
            }
        }
        with patch.object(cmoriser, "load_dataset"):
            cmoriser.ds = ds.copy()
            with pytest.raises(KeyError) as exc_info:
                cmoriser.select_and_process_variables()

        assert any(v in str(exc_info.value) for v in ["time_bnds", "lat_bnds"])

    @pytest.mark.unit
    def test_error_message_mentions_mapping(self, tmp_path):
        """The KeyError message must hint at the mapping's model_variables entry."""
        ds = self._bounds_only_ds()
        cmoriser = _make_cmoriser(ds, "zg", tmp_path=tmp_path)
        cmoriser.mapping = {
            "zg": {
                "model_variables": ["fld_s16i201"],
                "calculation": {"type": "direct", "formula": "fld_s16i201"},
            }
        }
        with patch.object(cmoriser, "load_dataset"):
            cmoriser.ds = ds.copy()
            with pytest.raises(KeyError) as exc_info:
                cmoriser.select_and_process_variables()

        assert "mapping" in str(exc_info.value).lower()

    @pytest.mark.unit
    def test_no_error_when_all_model_vars_present(self, tmp_path):
        """No KeyError when all required model variables are present in self.ds."""
        ds = xr.Dataset(
            {
                "fld_s16i201": (
                    ["time", "lat", "lon"],
                    np.ones((3, 5, 8), dtype="f4"),
                    {"units": "m"},
                )
            },
            coords={
                "time": np.arange(3, dtype=float),
                "lat": np.linspace(-90, 90, 5),
                "lon": np.linspace(0, 360, 8, endpoint=False),
            },
        )
        cmoriser = _make_cmoriser(ds, "fld_s16i201", tmp_path=tmp_path)
        cmoriser.mapping = {
            "zg": {
                "model_variables": ["fld_s16i201"],
                "calculation": {"type": "direct", "formula": "fld_s16i201"},
            }
        }
        cmoriser.cmor_name = "zg"

        with patch.object(cmoriser, "load_dataset"):
            cmoriser.ds = ds.copy()
            # Should not raise
            cmoriser.select_and_process_variables()

        assert "zg" in cmoriser.ds


class TestUnsupportedCalcType:
    """Tests for the else branch when calc['type'] is unknown."""

    @pytest.mark.unit
    def test_unsupported_calc_type_lists_supported(self, tmp_path):
        """ValueError must include cmor_name and the list of supported calc types."""
        ds = xr.Dataset(
            {
                "fld_s16i201": (
                    ["time", "lat", "lon"],
                    np.ones((3, 5, 8), dtype="f4"),
                    {"units": "m"},
                )
            },
            coords={
                "time": np.arange(3, dtype=float),
                "lat": np.linspace(-90, 90, 5),
                "lon": np.linspace(0, 360, 8, endpoint=False),
            },
        )
        cmoriser = _make_cmoriser(ds, "zg", tmp_path=tmp_path)
        cmoriser.mapping = {
            "zg": {
                "model_variables": ["fld_s16i201"],
                "calculation": {"type": "bogus_type"},
            }
        }
        cmoriser.cmor_name = "zg"

        with patch.object(cmoriser, "load_dataset"):
            cmoriser.ds = ds.copy()
            with pytest.raises(
                ValueError, match="Unsupported calculation type 'bogus_type'"
            ) as exc_info:
                cmoriser.select_and_process_variables()

        msg = str(exc_info.value)
        assert "'zg'" in msg
        assert "direct" in msg
        assert "formula" in msg
        assert "dataset_function" in msg
        assert "internal" in msg


# ---------------------------------------------------------------------------
# Tests for update_attributes() axis/positive stripping on the data variable
# ---------------------------------------------------------------------------


def _make_axis_strip_cmoriser(cmor_name, var_attrs, vocab_variable):
    """
    Build a minimal Atmosphere_CMORiser to exercise the axis/positive cleanup
    in update_attributes(). The main variable is a (time, lat, lon) field whose
    attrs may contain stray axis/positive leaked from a source vertical coord.
    """
    cmoriser = object.__new__(Atmosphere_CMORiser)
    cmoriser.cmor_name = cmor_name
    cmoriser.type_mapping = CMORiser.type_mapping

    ds = xr.Dataset(
        {
            cmor_name: xr.DataArray(
                np.ones((1, 2, 2), dtype=np.float32),
                dims=["time", "lat", "lon"],
                coords={
                    "time": (
                        ["time"],
                        np.array([0.0]),
                        {"units": "days since 1850-01-01", "calendar": "standard"},
                    ),
                    "lat": [0.0, 1.0],
                    "lon": [0.0, 1.0],
                },
                attrs=var_attrs,
            )
        }
    )
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
        },
        "lat": {"out_name": "lat"},
        "lon": {"out_name": "lon"},
    }
    vocab.variable = vocab_variable
    cmoriser.vocab = vocab
    cmoriser._check_units = MagicMock()
    cmoriser._check_calendar = MagicMock()
    cmoriser._check_range = MagicMock()
    return cmoriser


class TestUpdateAttributesAxisPositiveStripping:
    """
    A geophysical (time, lat, lon) data variable must not carry an `axis`
    attribute — the WCRP "Geophysical Variable Detection" check excludes any
    variable with `axis`, so mrsos (derived from a soil-layer field that leaks
    axis='Z'/positive='down') was wrongly classified as a coordinate.
    """

    @pytest.mark.unit
    def test_axis_stripped_from_data_variable(self):
        """A leaked axis='Z' is removed from the main variable."""
        cmoriser = _make_axis_strip_cmoriser(
            "mrsos",
            var_attrs={
                "standard_name": "mass_content_of_water_in_soil_layer",
                "units": "kg m-2",
                "axis": "Z",
            },
            vocab_variable={
                "standard_name": "mass_content_of_water_in_soil_layer",
                "units": "kg m-2",
                "type": "real",
            },
        )
        cmoriser.update_attributes()

        assert "axis" not in cmoriser.ds["mrsos"].attrs

    @pytest.mark.unit
    def test_undeclared_positive_stripped(self):
        """positive='down' is removed when the CMOR table declares no positive."""
        cmoriser = _make_axis_strip_cmoriser(
            "mrsos",
            var_attrs={
                "standard_name": "mass_content_of_water_in_soil_layer",
                "units": "kg m-2",
                "axis": "Z",
                "positive": "down",
            },
            vocab_variable={
                "standard_name": "mass_content_of_water_in_soil_layer",
                "units": "kg m-2",
                "type": "real",
                "positive": "",  # CMOR mrsos entry carries an empty positive
            },
        )
        cmoriser.update_attributes()

        assert "positive" not in cmoriser.ds["mrsos"].attrs

    @pytest.mark.unit
    def test_declared_positive_preserved(self):
        """A flux variable whose CMOR table declares positive keeps it."""
        cmoriser = _make_axis_strip_cmoriser(
            "hfls",
            var_attrs={
                "standard_name": "surface_upward_latent_heat_flux",
                "units": "W m-2",
                "positive": "down",  # stale/leaked value
            },
            vocab_variable={
                "standard_name": "surface_upward_latent_heat_flux",
                "units": "W m-2",
                "type": "real",
                "positive": "up",  # authoritative CMOR value
            },
        )
        cmoriser.update_attributes()

        assert cmoriser.ds["hfls"].attrs.get("positive") == "up"

    @pytest.mark.unit
    def test_normal_variable_without_axis_unaffected(self):
        """A regular variable (no axis/positive) passes through unchanged."""
        cmoriser = _make_axis_strip_cmoriser(
            "tas",
            var_attrs={"standard_name": "air_temperature", "units": "K"},
            vocab_variable={
                "standard_name": "air_temperature",
                "units": "K",
                "type": "real",
            },
        )
        cmoriser.update_attributes()

        assert "axis" not in cmoriser.ds["tas"].attrs
        assert "positive" not in cmoriser.ds["tas"].attrs
        assert cmoriser.ds["tas"].attrs.get("standard_name") == "air_temperature"


# ---------------------------------------------------------------------------
# Tests for _retarget_renamed_references (hybrid-height coordinate/formula_terms)
# ---------------------------------------------------------------------------


def _bare_atmos_cmoriser(ds):
    """An Atmosphere_CMORiser with only .ds set, for testing pure helpers."""
    cmoriser = object.__new__(Atmosphere_CMORiser)
    cmoriser.ds = ds
    return cmoriser


class TestRetargetRenamedReferences:
    """
    After Dataset.rename(), the `coordinates`/`formula_terms` attribute *strings*
    still reference the pre-rename input names. _retarget_renamed_references must
    re-point them at the new names so hybrid-height terms resolve, while leaving
    references that were not renamed (i.e. other variables) untouched.
    """

    # The full intended rename for cl-family variables.
    RENAME = {
        "theta_level_height": "lev",
        "sigma_theta": "b",
        "surface_altitude": "orog",
        "lat": "lat",  # identity entries must be harmless
        "lon": "lon",
        "time": "time",
    }

    def _cl_like_ds(self):
        return xr.Dataset(
            {
                "cl": (
                    ["lev"],
                    np.zeros(3),
                    {"coordinates": "sigma_theta surface_altitude theta_level_height"},
                ),
                "lev": (
                    ["lev"],
                    np.arange(3, dtype=float),
                    {
                        "formula_terms": (
                            "a: theta_level_height b: sigma_theta orog: surface_altitude"
                        )
                    },
                ),
            }
        )

    @pytest.mark.unit
    def test_coordinates_retargeted_to_new_names(self):
        cmoriser = _bare_atmos_cmoriser(self._cl_like_ds())
        cmoriser._retarget_renamed_references(self.RENAME)
        assert cmoriser.ds["cl"].attrs["coordinates"] == "b orog lev"

    @pytest.mark.unit
    def test_formula_terms_variables_remapped_term_keys_preserved(self):
        cmoriser = _bare_atmos_cmoriser(self._cl_like_ds())
        cmoriser._retarget_renamed_references(self.RENAME)
        # term keys (a:, b:, orog:) preserved; variable tokens remapped
        assert cmoriser.ds["lev"].attrs["formula_terms"] == "a: lev b: b orog: orog"

    @pytest.mark.unit
    def test_unrenamed_references_untouched(self):
        """A variable referencing names absent from rename_map is unchanged."""
        ds = xr.Dataset(
            {
                "tas": (
                    ["lat", "lon"],
                    np.zeros((2, 2)),
                    {"coordinates": "height"},
                ),
            },
            coords={"lat": [0.0, 1.0], "lon": [0.0, 1.0]},
        )
        cmoriser = _bare_atmos_cmoriser(ds)
        cmoriser._retarget_renamed_references(self.RENAME)
        assert cmoriser.ds["tas"].attrs["coordinates"] == "height"

    @pytest.mark.unit
    def test_empty_rename_map_is_noop(self):
        ds = self._cl_like_ds()
        cmoriser = _bare_atmos_cmoriser(ds)
        cmoriser._retarget_renamed_references({})
        assert (
            cmoriser.ds["cl"].attrs["coordinates"]
            == "sigma_theta surface_altitude theta_level_height"
        )

    @pytest.mark.unit
    def test_variables_without_references_unaffected(self):
        """Variables with no coordinates/formula_terms must not raise or change."""
        ds = xr.Dataset({"pr": (["lat"], np.zeros(2), {"units": "kg m-2 s-1"})})
        cmoriser = _bare_atmos_cmoriser(ds)
        cmoriser._retarget_renamed_references(self.RENAME)
        assert cmoriser.ds["pr"].attrs == {"units": "kg m-2 s-1"}


# ---------------------------------------------------------------------------
# Tests for lev_bnds formula_terms (CF §4.3.3)
# ---------------------------------------------------------------------------


class TestLevBndsFormulaTerms:
    """
    A parametric vertical coordinate's bounds variable needs its own
    formula_terms, referencing the *bounds* of each term. The value comes from
    the coordinate table's `z_bounds_factors`, not from the parent's
    formula_terms (which points at the coordinates).
    """

    HYBRID_HEIGHT_TERMS = "a: lev_bnds b: b_bnds orog: orog"
    PARENT_TERMS = "a: lev b: b orog: orog"

    def _make_ds(self, with_orog=True):
        nlev, nlat, nlon = 5, 4, 4
        rng = np.random.default_rng(0)
        lev = np.linspace(10.0, 5000.0, nlev)
        data_vars = {
            "cl": (
                ["lev", "lat", "lon"],
                rng.random((nlev, nlat, nlon)),
                {"units": "%"},
            ),
            "b": (["lev"], np.linspace(1.0, 0.0, nlev), {"units": "1"}),
            "b_bnds": (
                ["lev", "bnds"],
                np.tile(np.linspace(1.0, 0.0, nlev), (2, 1)).T,
            ),
            "lev_bnds": (["lev", "bnds"], np.tile(lev, (2, 1)).T),
        }
        if with_orog:
            data_vars["orog"] = (
                ["lat", "lon"],
                np.zeros((nlat, nlon)),
                {"units": "m"},
            )
        return xr.Dataset(
            data_vars,
            coords={
                "lev": (
                    ["lev"],
                    lev,
                    {"units": "m", "formula_terms": self.PARENT_TERMS},
                ),
                "lat": np.linspace(-90, 90, nlat),
                "lon": np.linspace(0, 360, nlon, endpoint=False),
                "bnds": [0, 1],
            },
        )

    def _make_cmoriser(self, ds, tmp_path, z_bounds_factors):
        vocab = MagicMock()
        vocab.variable = {"dimensions": "lev lat lon", "units": "%", "type": "double"}
        lev_axis = {
            "out_name": "lev",
            "units": "m",
            "long_name": "hybrid height coordinate",
            "standard_name": "atmosphere_hybrid_height_coordinate",
            "axis": "Z",
            "positive": "up",
            "z_factors": self.PARENT_TERMS,
        }
        if z_bounds_factors is not None:
            lev_axis["z_bounds_factors"] = z_bounds_factors
        vocab.axes = {
            "hybrid_height": lev_axis,
            "b": {"out_name": "b", "units": "1"},
            "lat": {"out_name": "lat", "units": "degrees_north"},
            "lon": {"out_name": "lon", "units": "degrees_east"},
        }
        vocab.get_required_global_attributes.return_value = {}
        vocab._get_axes.return_value = ([], {})
        vocab._get_required_bounds_variables.return_value = ({}, {})
        mapping = {"cl": {"model_variables": ["cl"], "calculation": {"type": "direct"}}}
        cmoriser = Atmosphere_CMORiser(
            input_data=ds,
            output_path=str(tmp_path),
            vocab=vocab,
            variable_mapping=mapping,
            compound_name="mon.cl",
            validate_frequency=False,
            enable_chunking=False,
            enable_compression=False,
        )
        cmoriser.ds = ds.copy()
        return cmoriser

    @staticmethod
    def _run(cmoriser):
        with (
            patch.object(cmoriser, "_check_units"),
            patch.object(cmoriser, "_check_calendar"),
            patch.object(cmoriser, "_check_range"),
        ):
            cmoriser.update_attributes()

    @pytest.mark.unit
    def test_lev_bnds_gets_z_bounds_factors(self, tmp_path):
        """lev_bnds carries the bounds-side terms, not the parent's."""
        ds = self._make_ds()
        cmoriser = self._make_cmoriser(ds, tmp_path, self.HYBRID_HEIGHT_TERMS)

        self._run(cmoriser)

        terms = cmoriser.ds["lev_bnds"].attrs.get("formula_terms")
        assert terms == self.HYBRID_HEIGHT_TERMS
        assert terms != self.PARENT_TERMS, "must point at bounds, not coordinates"

    @pytest.mark.unit
    def test_no_formula_terms_when_z_bounds_factors_empty(self, tmp_path):
        """hybrid_height_half declares an empty z_bounds_factors: write nothing."""
        ds = self._make_ds()
        cmoriser = self._make_cmoriser(ds, tmp_path, "")

        self._run(cmoriser)

        assert "formula_terms" not in cmoriser.ds["lev_bnds"].attrs

    @pytest.mark.unit
    def test_no_formula_terms_when_a_term_variable_is_missing(self, tmp_path):
        """A missing term would leave a dangling reference: write nothing."""
        ds = self._make_ds(with_orog=False)
        cmoriser = self._make_cmoriser(ds, tmp_path, self.HYBRID_HEIGHT_TERMS)

        self._run(cmoriser)

        assert "formula_terms" not in cmoriser.ds["lev_bnds"].attrs


# ---------------------------------------------------------------------------
# Tests for coordinate bounds on the internal-calculation path
# ---------------------------------------------------------------------------


class TestInternalCalculationBounds:
    """
    An internal calculation (e.g. areacella) builds its own grid, so no bounds
    come in from the source files. select_and_process_variables() must still
    create the bounds the axes declare, instead of returning without them.
    """

    NLAT, NLON = 145, 192

    def _make_cmoriser(self, tmp_path, bnds_required=("lat_bnds", "lon_bnds")):
        vocab = MagicMock()
        vocab.variable = {"dimensions": "lat lon", "units": "m2", "type": "double"}
        vocab.axes = {
            "latitude": {"out_name": "lat", "must_have_bounds": "yes"},
            "longitude": {"out_name": "lon", "must_have_bounds": "yes"},
        }
        vocab._get_axes.return_value = ([], {})
        vocab._get_required_bounds_variables.return_value = (
            {name: {} for name in bnds_required},
            {},
        )
        vocab.get_required_global_attributes.return_value = {}
        mapping = {
            "areacella": {
                "model_variables": None,
                "calculation": {
                    "type": "internal",
                    "function": "calculate_areacella",
                    "args": [],
                },
            }
        }
        return Atmosphere_CMORiser(
            input_data=xr.Dataset(),
            output_path=str(tmp_path),
            vocab=vocab,
            variable_mapping=mapping,
            compound_name="fx.areacella",
            validate_frequency=False,
            enable_chunking=False,
            enable_compression=False,
        )

    @pytest.mark.unit
    def test_bounds_created_for_internally_calculated_variable(self, tmp_path):
        """areacella gets lat_bnds/lon_bnds rather than no bounds at all."""
        cmoriser = self._make_cmoriser(tmp_path)

        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            cmoriser.select_and_process_variables()

        assert "lat_bnds" in cmoriser.ds
        assert "lon_bnds" in cmoriser.ds
        assert cmoriser.ds["lat_bnds"].shape == (self.NLAT, 2)
        assert cmoriser.ds["lon_bnds"].shape == (self.NLON, 2)

    @pytest.mark.unit
    def test_bounds_attribute_points_at_the_bounds_variable(self, tmp_path):
        """The coordinates must reference their bounds (WCRP ATTR001)."""
        cmoriser = self._make_cmoriser(tmp_path)

        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            cmoriser.select_and_process_variables()

        assert cmoriser.ds["lat"].attrs.get("bounds") == "lat_bnds"
        assert cmoriser.ds["lon"].attrs.get("bounds") == "lon_bnds"

    @pytest.mark.unit
    def test_bounds_values_match_the_shared_n96_grid(self, tmp_path):
        """Same values the discovery path produces for orog/sftlf on this grid.

        The first longitude cell straddles 0degE and must be unwrapped to a
        negative lower bound, not left as 359.0625 (see the lon_bnds wraparound
        fix); reusing calculate_longitude_bounds gets this for free.
        """
        cmoriser = self._make_cmoriser(tmp_path)

        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            cmoriser.select_and_process_variables()

        np.testing.assert_allclose(cmoriser.ds["lat_bnds"].values[0], [-90.0, -89.375])
        np.testing.assert_allclose(cmoriser.ds["lon_bnds"].values[0], [-0.9375, 0.9375])

    @pytest.mark.unit
    def test_no_stray_bnds_coordinate_variable(self, tmp_path):
        """'bnds' must stay a dimension; a bare coordinate variable would fail CF."""
        cmoriser = self._make_cmoriser(tmp_path)

        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            cmoriser.select_and_process_variables()

        assert "bnds" in cmoriser.ds.sizes
        assert "bnds" not in cmoriser.ds.coords


# ---------------------------------------------------------------------------
# Tests for CF §7.1 bounds-attribute clearing
# ---------------------------------------------------------------------------


class TestBoundsAttributesCleared:
    """
    CF §7.1: a bounds variable inherits its parent's semantics and must not
    repeat units/standard_name/axis/calendar. Published CMOR output leaves
    lat_bnds/lon_bnds/time_bnds attribute-free; the parametric lev_bnds keeps
    standard_name/units/formula_terms.

    The writer turns a cftime-valued time_bnds back into numbers using its
    units, so clearing the attribute must move it into `encoding` -- otherwise
    the encoder falls back to a 1850 epoch while `time` uses its own.
    """

    HYBRID_TERMS = "a: lev_bnds b: b_bnds orog: orog"

    def _make_cmoriser(self, ds, tmp_path, parametric=False):
        vocab = MagicMock()
        vocab.variable = {
            "dimensions": "time lev lat lon" if parametric else "time lat lon",
            "units": "K",
            "type": "double",
        }
        lev = {
            "out_name": "lev",
            "units": "m",
            "standard_name": "atmosphere_hybrid_height_coordinate",
            "axis": "Z",
            "positive": "up",
            "long_name": "hybrid height coordinate",
            "z_bounds_factors": self.HYBRID_TERMS,
        }
        vocab.axes = {
            "time": {"out_name": "time", "standard_name": "time"},
            "lat": {"out_name": "lat", "units": "degrees_north", "axis": "Y"},
            "lon": {"out_name": "lon", "units": "degrees_east", "axis": "X"},
        }
        if parametric:
            vocab.axes["hybrid_height"] = lev
            vocab.axes["b"] = {"out_name": "b", "units": "1"}
        vocab.get_required_global_attributes.return_value = {}
        vocab._get_axes.return_value = ([], {})
        vocab._get_required_bounds_variables.return_value = ({}, {})
        mapping = {
            "tas": {"model_variables": ["tas"], "calculation": {"type": "direct"}}
        }
        cmoriser = Atmosphere_CMORiser(
            input_data=ds,
            output_path=str(tmp_path),
            vocab=vocab,
            variable_mapping=mapping,
            compound_name="mon.tas",
            validate_frequency=False,
            enable_chunking=False,
            enable_compression=False,
        )
        cmoriser.ds = ds.copy()
        return cmoriser

    def _base_ds(self, time_values, time_attrs):
        n = len(time_values)
        nlat = nlon = 3
        tb = np.stack([time_values, time_values], axis=1)
        return xr.Dataset(
            {
                "tas": (
                    ["time", "lat", "lon"],
                    np.zeros((n, nlat, nlon)),
                    {"units": "K"},
                ),
                "time_bnds": (["time", "bnds"], tb, dict(time_attrs)),
                "lat_bnds": (["lat", "bnds"], np.zeros((nlat, 2)), {"units": "stale"}),
                "lon_bnds": (["lon", "bnds"], np.zeros((nlon, 2)), {"axis": "X"}),
            },
            coords={
                "time": (["time"], time_values, dict(time_attrs)),
                "lat": np.linspace(-90, 90, nlat),
                "lon": np.linspace(0, 360, nlon, endpoint=False),
                "bnds": [0, 1],
            },
        )

    @staticmethod
    def _run(cmoriser):
        with (
            patch.object(cmoriser, "_check_units"),
            patch.object(cmoriser, "_check_calendar"),
            patch.object(cmoriser, "_check_range"),
        ):
            cmoriser.update_attributes()

    @pytest.mark.unit
    def test_plain_bounds_end_up_attribute_free(self, tmp_path):
        """lat_bnds/lon_bnds/time_bnds match the published CMOR reference."""
        ds = self._base_ds(
            np.arange(3.0),
            {"units": "days since 0001-01-01", "calendar": "proleptic_gregorian"},
        )
        cmoriser = self._make_cmoriser(ds, tmp_path)

        self._run(cmoriser)

        for name in ("lat_bnds", "lon_bnds", "time_bnds"):
            assert cmoriser.ds[name].attrs == {}, name

    @pytest.mark.unit
    def test_parent_coordinates_keep_their_attributes(self, tmp_path):
        """Only the bounds are cleared -- the parents must be untouched."""
        ds = self._base_ds(
            np.arange(3.0),
            {"units": "days since 0001-01-01", "calendar": "proleptic_gregorian"},
        )
        cmoriser = self._make_cmoriser(ds, tmp_path)

        self._run(cmoriser)

        assert cmoriser.ds["lat"].attrs.get("units") == "degrees_north"
        assert cmoriser.ds["lat"].attrs.get("axis") == "Y"
        assert cmoriser.ds["lon"].attrs.get("units") == "degrees_east"

    @pytest.mark.unit
    def test_cftime_time_bnds_keeps_its_units_in_encoding(self, tmp_path):
        """Clearing attrs must not strand the encoder on a default epoch.

        The writer reads attrs first, then encoding; with neither it silently
        falls back to "days since 1850-01-01" and the bounds land ~1850 years
        from their own time coordinate.
        """
        times = xr.cftime_range(
            "0101-01-01", periods=3, freq="MS", calendar="proleptic_gregorian"
        ).values
        ds = self._base_ds(
            times,
            {"units": "days since 0001-01-01", "calendar": "proleptic_gregorian"},
        )
        cmoriser = self._make_cmoriser(ds, tmp_path)

        self._run(cmoriser)

        bnds = cmoriser.ds["time_bnds"]
        assert bnds.attrs == {}
        assert bnds.encoding.get("units") == "days since 0001-01-01"
        assert bnds.encoding.get("calendar") == "proleptic_gregorian"

    @pytest.mark.unit
    def test_numeric_time_bnds_needs_no_encoding(self, tmp_path):
        """Numeric bounds need no conversion, so nothing is stashed."""
        ds = self._base_ds(
            np.arange(3.0),
            {"units": "days since 0001-01-01", "calendar": "proleptic_gregorian"},
        )
        cmoriser = self._make_cmoriser(ds, tmp_path)

        self._run(cmoriser)

        assert cmoriser.ds["time_bnds"].encoding.get("units") is None

    @pytest.mark.unit
    def test_parametric_lev_bnds_keeps_formula_metadata(self, tmp_path):
        """hybrid-height lev_bnds keeps standard_name/units, drops axis/positive."""
        n, nlev, nlat, nlon = 3, 4, 3, 3
        ds = xr.Dataset(
            {
                "tas": (["time", "lev", "lat", "lon"], np.zeros((n, nlev, nlat, nlon))),
                "lev_bnds": (
                    ["lev", "bnds"],
                    np.zeros((nlev, 2)),
                    {"axis": "Z", "positive": "up", "long_name": "stale"},
                ),
                "b_bnds": (["lev", "bnds"], np.zeros((nlev, 2)), {"units": "1"}),
                "b": (["lev"], np.zeros(nlev), {"units": "1"}),
                "orog": (["lat", "lon"], np.zeros((nlat, nlon)), {"units": "m"}),
            },
            coords={
                "time": np.arange(float(n)),
                "lev": np.arange(float(nlev)),
                "lat": np.linspace(-90, 90, nlat),
                "lon": np.linspace(0, 360, nlon, endpoint=False),
                "bnds": [0, 1],
            },
        )
        cmoriser = self._make_cmoriser(ds, tmp_path, parametric=True)

        self._run(cmoriser)

        lev_bnds = cmoriser.ds["lev_bnds"].attrs
        assert lev_bnds.get("standard_name") == "atmosphere_hybrid_height_coordinate"
        assert lev_bnds.get("units") == "m"
        assert lev_bnds.get("formula_terms") == self.HYBRID_TERMS
        assert "axis" not in lev_bnds
        assert "positive" not in lev_bnds
        assert "long_name" not in lev_bnds
        # b_bnds is an ordinary bounds variable: cleared
        assert cmoriser.ds["b_bnds"].attrs == {}
