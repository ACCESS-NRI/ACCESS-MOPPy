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
