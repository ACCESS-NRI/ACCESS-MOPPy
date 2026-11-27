"""Scientific prose must resolve to the requested branded product."""

import sys
from types import ModuleType
from unittest.mock import Mock

import pytest

from access_moppy.registry_comments import get_cmip7_variable_comment
from access_moppy.vocabulary_processors import CMIP7Vocabulary

PRODUCT = "aerosol.od550dust.tavg-u-hxy-u.mon.glb"
RECORD = {
    "id": "od550dust_tavg-u-hxy-u",
    "compound_name": [PRODUCT],
    "comment": ["Dust optical thickness at 550 nm."],
}


@pytest.fixture
def lookup(monkeypatch):
    # Unit tests do not require the optional checker/registry installation.
    api = ModuleType("esgvoc.api")
    lookup = Mock(return_value=dict(RECORD))
    api.get_term_in_collection = lookup
    monkeypatch.setitem(sys.modules, "esgvoc.api", api)
    return lookup


def test_exact_branded_lookup_and_subset_schema(lookup):
    """Subset models are supported across ESGVOC 6.1 and 7.2."""
    lookup.return_value = Mock(model_dump=Mock(return_value=dict(RECORD)))
    assert get_cmip7_variable_comment(PRODUCT) == RECORD["comment"][0]
    lookup.assert_called_once_with(
        project_id="cmip7",
        collection_id="branded_variable",
        term_id="od550dust_tavg-u-hxy-u",
        selected_term_fields=["id", "comment", "compound_name"],
    )


@pytest.mark.parametrize(
    "change,reason",
    [
        ({"id": "od550dust_tavg-u-hxy-lnd"}, "exact branded"),
        ({"compound_name": [PRODUCT.replace("mon", "day")]}, "compound identity"),
        ({"compound_name": []}, "compound identity"),
        ({"comment": ["Hourly mean.", "Monthly mean."]}, "multiple permitted"),
        ({"comment": []}, "no comment description"),
        ({"comment": ["", "  "]}, "no comment description"),
        ({"comment": None}, "no usable comment"),
        ({"comment": [42]}, "no usable comment"),
    ],
)
def test_does_not_guess(lookup, change, reason):
    lookup.return_value = dict(RECORD, **change)
    with pytest.warns(UserWarning, match=reason):
        assert get_cmip7_variable_comment(PRODUCT) is None


def test_old_registry_description_is_not_a_fallback(lookup):
    lookup.return_value = {
        "id": RECORD["id"],
        "description": "A description from an older vocabulary.",
    }
    with pytest.warns(UserWarning, match="compound identity"):
        assert get_cmip7_variable_comment(PRODUCT) is None


def test_no_root_variable_fallback(lookup):
    lookup.return_value = dict(
        RECORD, comment=[], variable_root_name={"description": "Other layer"}
    )
    with pytest.warns(UserWarning, match="no comment description"):
        assert get_cmip7_variable_comment(PRODUCT) is None


def test_duplicate_identical_descriptions_are_unambiguous(lookup):
    lookup.return_value = dict(RECORD, comment=[" Dust. ", "Dust.", ""])
    assert get_cmip7_variable_comment(PRODUCT) == "Dust."


def test_missing_registry_is_optional(lookup):
    lookup.side_effect = RuntimeError("No installed database")
    with pytest.warns(UserWarning, match="registry unavailable"):
        assert get_cmip7_variable_comment(PRODUCT) is None


@pytest.mark.parametrize(
    "product,comment",
    [
        (
            "land.fNup.tavg-u-hxy-lnd.mon.glb",
            "The uptake of nitrogen by fixation: nitrogen fixation means the uptake "
            "of nitrogen gas directly from the atmosphere.",
        ),
        (
            "ocean.tossq.tavg-u-hxy-sea.mon.glb",
            "Square of temperature of liquid ocean, averaged over the day.",
        ),
        (
            "seaIce.siflfwbot.tavg-u-hxy-si.mon.glb",
            "Total flux of fresh water from water into sea ice. This flux is positive "
            "when fresh water enters the ocean, and is therefore negative during "
            "ice growth and positive during ice melt.",
        ),
    ],
)
def test_known_scientific_conflicts_stay_visible(lookup, product, comment):
    parts = product.split(".")
    record = {
        "id": f"{parts[1]}_{parts[2]}",
        "compound_name": [product],
        "comment": comment,
    }
    lookup.return_value = record
    with pytest.warns(UserWarning, match="known vocabulary conflict"):
        assert get_cmip7_variable_comment(product) is None
    # A future corrected record is accepted, not permanently blocked by name.
    lookup.return_value = dict(record, comment="Corrected description.")
    assert get_cmip7_variable_comment(product) == "Corrected description."


def test_cmip7_table_entry_uses_resolved_comment(lookup, monkeypatch):
    vocab = CMIP7Vocabulary.__new__(CMIP7Vocabulary)
    vocab.compound_name = PRODUCT
    vocab.cmor_name = "od550dust_tavg-u-hxy-u"
    monkeypatch.setattr(
        vocab,
        "_load_table",
        lambda: {"variable_entry": {vocab.cmor_name: {"comment": ""}}},
    )
    monkeypatch.setattr(vocab, "_get_cell_measures", lambda: "")
    monkeypatch.setattr(vocab, "_get_long_name_override", lambda: "")
    assert vocab._get_variable_entry()["comment"] == RECORD["comment"][0]
