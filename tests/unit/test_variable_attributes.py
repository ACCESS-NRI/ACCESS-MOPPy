"""Unit tests for which CMOR table fields reach the written variable.

A CMOR table entry mixes metadata that describes the variable with directives
that tell CMOR how to build it.  Copying the entry wholesale put the directives
— ``dimensions``, ``out_name``, ``type``, ``frequency``, ``modeling_realm`` —
into the file, where no published CMIP6 dataset carries them and where
``dimensions`` claims a name CF Appendix A reserves for domain variables.
"""

from __future__ import annotations

from unittest.mock import Mock

import numpy as np
import pytest
import xarray as xr

from access_moppy.base import CMORiser

#: A CMIP6 ``Omon`` entry, which carries every directive field at once.
CMIP6_ENTRY = {
    "standard_name": "sea_surface_temperature",
    "long_name": "Sea Surface Temperature",
    "comment": "Temperature of upper boundary of the liquid ocean.",
    "units": "degC",
    "cell_methods": "area: mean where sea time: mean",
    "cell_measures": "area: areacello",
    "dimensions": "longitude latitude time",
    "out_name": "tos",
    "type": "real",
    "frequency": "mon",
    "modeling_realm": "ocean",
    "_FillValue": 1e20,
    "missing_value": 1e20,
}

#: The CMIP7 tables drop ``type``/``frequency``/``comment`` but keep the rest.
CMIP7_ENTRY = {
    "standard_name": "air_temperature",
    "long_name": "Near-Surface Air Temperature",
    "units": "K",
    "cell_methods": "area: time: mean",
    "cell_measures": "area: areacella",
    "dimensions": ["longitude", "latitude", "time", "height2m"],
    "out_name": "tas",
    "modeling_realm": "atmos",
    "_FillValue": 1e20,
    "missing_value": 1e20,
}

DIRECTIVES = ("dimensions", "out_name", "type", "frequency", "modeling_realm")


def _cmoriser(tmp_path, entry, name="tos"):
    vocab = Mock()
    vocab.mip_era = "CMIP6"
    vocab.variable = dict(entry)

    ds = xr.Dataset(
        {
            name: xr.DataArray(
                np.asarray([1.0, 2.0], dtype=np.float32),
                dims=["time"],
                coords={"time": xr.DataArray([0, 1], dims=["time"])},
            )
        }
    )

    cmoriser = CMORiser(
        input_data=ds,
        output_path=str(tmp_path),
        vocab=vocab,
        variable_mapping={name: {"dimensions": {"time": "time"}}},
        compound_name=f"Omon.{name}",
    )
    cmoriser.ds = ds
    return cmoriser


@pytest.mark.unit
@pytest.mark.parametrize(
    ("label", "entry"), [("CMIP6", CMIP6_ENTRY), ("CMIP7", CMIP7_ENTRY)]
)
def test_directive_fields_do_not_reach_the_variable(tmp_path, label, entry):
    """Only the describing fields are written; the directives are dropped."""
    cmoriser = _cmoriser(tmp_path, entry)

    cmoriser._apply_cmor_variable_attributes(cmoriser.vocab.variable)

    written = set(cmoriser.ds["tos"].attrs)
    leaked = written.intersection(DIRECTIVES)
    assert not leaked, f"{label}: {sorted(leaked)} must not be written"


@pytest.mark.unit
@pytest.mark.parametrize(
    ("label", "entry"), [("CMIP6", CMIP6_ENTRY), ("CMIP7", CMIP7_ENTRY)]
)
def test_describing_fields_are_kept(tmp_path, label, entry):
    """Everything CMOR does write survives the filter."""
    cmoriser = _cmoriser(tmp_path, entry)

    cmoriser._apply_cmor_variable_attributes(cmoriser.vocab.variable)

    attrs = cmoriser.ds["tos"].attrs
    for name in set(entry) - set(DIRECTIVES):
        assert attrs.get(name) == entry[name], f"{label}: {name} was dropped"


@pytest.mark.unit
def test_fill_values_survive(tmp_path):
    """Dropping these would write files with no fill value.

    ``_write_single`` reads ``_FillValue`` back from the variable attributes,
    and ``wcrp_cmip7`` requires both at severity HIGH with a constant of 1e20,
    so losing them is worse than the leak this filter exists to fix.
    """
    cmoriser = _cmoriser(tmp_path, CMIP6_ENTRY)

    cmoriser._apply_cmor_variable_attributes(cmoriser.vocab.variable)

    attrs = cmoriser.ds["tos"].attrs
    assert attrs["_FillValue"] == 1e20
    assert attrs["missing_value"] == 1e20


@pytest.mark.unit
def test_attributes_set_elsewhere_are_untouched(tmp_path):
    """The filter reads the table entry, so it cannot delete anything else.

    ``coordinates`` and ``units_metadata`` are set by other steps and are not
    table fields; a filter applied to the variable's final attributes would
    have to enumerate them or silently drop them.
    """
    cmoriser = _cmoriser(tmp_path, CMIP7_ENTRY)
    cmoriser.ds["tos"].attrs.update(
        {"coordinates": "height", "units_metadata": "temperature: on_scale"}
    )

    cmoriser._apply_cmor_variable_attributes(cmoriser.vocab.variable)

    attrs = cmoriser.ds["tos"].attrs
    assert attrs["coordinates"] == "height"
    assert attrs["units_metadata"] == "temperature: on_scale"


@pytest.mark.unit
def test_empty_table_values_are_still_skipped(tmp_path):
    """An empty field stays out, as before: CMOR writes no empty attributes."""
    entry = dict(CMIP7_ENTRY, comment="", positive=None)
    cmoriser = _cmoriser(tmp_path, entry)

    cmoriser._apply_cmor_variable_attributes(cmoriser.vocab.variable)

    attrs = cmoriser.ds["tos"].attrs
    assert "comment" not in attrs
    assert "positive" not in attrs


#: What ACCESS writes on its own variables and CMIP does not want. Unlike the
#: table directives above these arrive with the source file, so the allowlist
#: applied to the table entry never sees them.
MODEL_NATIVE = {
    "time_avg_info": "average_T1,average_T2,average_DT",  # MOM5
    "time_rep": "averaged",  # CICE5
    "cartesian_axis": "T",  # MOM5
    "edges": "st_edges_ocean",  # MOM5
    "um_stash_source": "m01s00i033",  # UM
    "um_version": "7.3",  # UM
    "source": "Unified Model",  # UM
}


def _dataset_with_native_attrs():
    """A dataset shaped like a model-level file: data variable, coordinate, aux.

    ``orog`` stands in for the ``formula_terms`` target that hybrid-height
    files carry. It is the case the old per-variable pops could not reach.
    """
    ds = xr.Dataset(
        {
            "tos": xr.DataArray(
                np.asarray([1.0, 2.0], dtype=np.float32),
                dims=["time"],
                coords={"time": xr.DataArray([0, 1], dims=["time"])},
            ),
            "orog": xr.DataArray(np.asarray([0.0], dtype=np.float32), dims=["cell"]),
        }
    )
    ds["tos"].attrs.update(
        {
            "standard_name": "sea_surface_temperature",
            "time_avg_info": MODEL_NATIVE["time_avg_info"],
        }
    )
    ds["time"].attrs.update(
        {"axis": "T", "cartesian_axis": "T", "calendar_type": "GREGORIAN"}
    )
    ds["orog"].attrs.update(
        {
            "standard_name": "surface_altitude",
            "um_stash_source": MODEL_NATIVE["um_stash_source"],
            "um_version": MODEL_NATIVE["um_version"],
            "source": MODEL_NATIVE["source"],
        }
    )
    return ds


def _cmoriser_with_native_attrs(tmp_path):
    cmoriser = _cmoriser(tmp_path, CMIP6_ENTRY)
    cmoriser.ds = _dataset_with_native_attrs()
    return cmoriser


@pytest.mark.unit
def test_model_native_attributes_are_dropped_from_the_data_variable(tmp_path):
    """MOM's ``time_avg_info`` reached the published archive on every ocean variable."""
    cmoriser = _cmoriser_with_native_attrs(tmp_path)

    cmoriser._drop_model_native_attributes()

    assert "time_avg_info" not in cmoriser.ds["tos"].attrs


@pytest.mark.unit
def test_model_native_attributes_are_dropped_from_coordinates(tmp_path):
    """``calendar_type``/``cartesian_axis`` sit on ``time``, never on the data variable.

    The pops that predate this method are scoped to ``self.cmor_name``, so
    nothing reached a coordinate.
    """
    cmoriser = _cmoriser_with_native_attrs(tmp_path)

    cmoriser._drop_model_native_attributes()

    attrs = cmoriser.ds["time"].attrs
    assert "cartesian_axis" not in attrs
    assert "calendar_type" not in attrs


@pytest.mark.unit
def test_model_native_attributes_are_dropped_from_auxiliary_variables(tmp_path):
    """The ``orog`` a model-level file carries as a ``formula_terms`` target.

    ``um_stash_source`` was already named for removal in the atmosphere
    CMORiser, yet 919 published files carry it here, because that pop only
    ever looked at the data variable.
    """
    cmoriser = _cmoriser_with_native_attrs(tmp_path)

    cmoriser._drop_model_native_attributes()

    attrs = cmoriser.ds["orog"].attrs
    for name in ("um_stash_source", "um_version", "source"):
        assert name not in attrs, f"{name} survived on an auxiliary variable"


@pytest.mark.unit
def test_describing_attributes_survive_the_sweep(tmp_path):
    """A denylist, so nothing outside it may be touched on any variable."""
    cmoriser = _cmoriser_with_native_attrs(tmp_path)

    cmoriser._drop_model_native_attributes()

    assert cmoriser.ds["tos"].attrs["standard_name"] == "sea_surface_temperature"
    assert cmoriser.ds["orog"].attrs["standard_name"] == "surface_altitude"
    assert cmoriser.ds["time"].attrs["axis"] == "T"


@pytest.mark.unit
def test_calendar_type_is_read_before_it_is_dropped(tmp_path):
    """Order matters: ``calendar_type`` is an input, not just noise.

    MOM writes it instead of the CF ``calendar``, and ``_check_calendar``
    rewrites a ``GREGORIAN`` value. Sweeping before that would take the
    calendar with it, so the sweep belongs at the end of
    ``update_attributes``.
    """
    cmoriser = _cmoriser_with_native_attrs(tmp_path)
    cmoriser.ds["time"].attrs.update(
        {"calendar": "GREGORIAN", "units": "days since 0001-01-01"}
    )

    cmoriser._check_calendar("time")
    cmoriser._drop_model_native_attributes()

    assert cmoriser.ds["time"].attrs["calendar"] == "proleptic_gregorian"
    assert "calendar_type" not in cmoriser.ds["time"].attrs


@pytest.mark.unit
def test_global_source_attribute_is_untouched(tmp_path):
    """``source`` is a legitimate CMIP7 global attribute (Global Attributes, Table 4).

    Only the variable-level ``source`` the UM writes is dropped.
    """
    cmoriser = _cmoriser_with_native_attrs(tmp_path)
    cmoriser.ds.attrs["source"] = "ACCESS-ESM1-6"

    cmoriser._drop_model_native_attributes()

    assert cmoriser.ds.attrs["source"] == "ACCESS-ESM1-6"
