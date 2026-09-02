"""Unit tests for writing the main variable under the CMOR ``out_name``.

A CMOR table entry is looked up by a key that need not match the name the
variable is written under: CMIP7 brands the key, so ``tas_tmax-h2m-hxy-u``
writes ``tas``, and CMIP6 does the same for a handful of variants, so
``ficeberg2d`` writes ``ficeberg``.  Mapping and file discovery stay keyed on
the table key; only the written variable takes ``out_name``.
"""

from __future__ import annotations

from unittest.mock import Mock

import numpy as np
import pytest
import xarray as xr

from access_moppy.base import CMORiser


def _cmoriser(tmp_path, compound_name, table_key, out_name, variable_id):
    """A CMORiser holding one variable named *table_key*, ready to write."""
    vocab = Mock()
    vocab.mip_era = "CMIP7"
    vocab.compound_name = compound_name
    vocab.variable = {"out_name": out_name, "units": "K"}
    vocab.generate_filename = Mock(return_value=f"{variable_id}.nc")
    vocab.get_required_attribute_names = Mock(return_value=[])

    ds = xr.Dataset(
        {
            table_key: xr.DataArray(
                np.asarray([285.0, 286.0], dtype=float),
                dims=["time"],
                attrs={"units": "K"},
                coords={"time": xr.DataArray([0, 1], dims=["time"])},
            )
        },
        attrs={"variable_id": variable_id, "units": "K"},
    )

    cmoriser = CMORiser(
        input_data=ds,
        output_path=str(tmp_path),
        vocab=vocab,
        variable_mapping={table_key: {"dimensions": {"time": "time"}}},
        compound_name=compound_name,
    )
    cmoriser.ds = ds
    return cmoriser


@pytest.mark.unit
@pytest.mark.parametrize(
    ("compound_name", "table_key", "out_name", "variable_id"),
    [
        # CMIP7 branded variables whose key differs from the written name.
        ("Amon.tasmax", "tasmax", "tas", "tas"),
        ("Lmon.mrsos", "mrsos", "mrsol", "mrsol"),
        # CMIP6 variant that carries its own out_name.
        ("Omon.ficeberg2d", "ficeberg2d", "ficeberg", "ficeberg2d"),
    ],
)
def test_variable_is_renamed_to_out_name(
    tmp_path, compound_name, table_key, out_name, variable_id
):
    """The written variable takes ``out_name``, not the table key.

    Getting this wrong leaves a file whose data variable disagrees with its own
    ``variable_id``, and ``cmip7repack`` — which locates the variable by that
    attribute — then silently packs nothing.
    """
    cmoriser = _cmoriser(tmp_path, compound_name, table_key, out_name, variable_id)

    assert cmoriser.cmor_name == table_key
    assert cmoriser.output_name == out_name

    cmoriser._apply_output_name()

    assert out_name in cmoriser.ds.data_vars
    assert table_key not in cmoriser.ds.data_vars
    # The mapping is still keyed on the table key.
    assert cmoriser.cmor_name == table_key


@pytest.mark.unit
def test_no_rename_when_out_name_matches_the_key(tmp_path):
    """The common case is untouched, so nothing moves for most variables."""
    cmoriser = _cmoriser(tmp_path, "Amon.tas", "tas", "tas", "tas")

    cmoriser._apply_output_name()

    assert list(cmoriser.ds.data_vars) == ["tas"]


@pytest.mark.unit
def test_applying_the_output_name_twice_is_a_no_op(tmp_path):
    """``write()`` may be called more than once on the same CMORiser."""
    cmoriser = _cmoriser(tmp_path, "Amon.tasmax", "tasmax", "tas", "tas")

    cmoriser._apply_output_name()
    cmoriser._apply_output_name()

    assert list(cmoriser.ds.data_vars) == ["tas"]


@pytest.mark.unit
def test_output_name_falls_back_to_the_key_without_a_table_entry(tmp_path):
    """A vocabulary that exposes no ``out_name`` keeps the current behaviour."""
    cmoriser = _cmoriser(tmp_path, "Amon.tas", "tas", "tas", "tas")
    cmoriser.vocab.variable = {}

    assert cmoriser.output_name == "tas"
