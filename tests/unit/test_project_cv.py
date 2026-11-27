"""Local development identities retain the official CMOR variable tables."""

from copy import deepcopy

import pytest

from access_moppy.vocabulary_processors import (
    CMIP6PlusVocabulary,
    CMIP6Vocabulary,
    CMIP7Vocabulary,
)


def _vocabulary(project_cv):
    return CMIP6Vocabulary(
        compound_name="Omon.tos",
        experiment_id="om3-real-run",
        source_id="ACCESS-OM3",
        variant_label="r1i1p1f1",
        grid_label="gn",
        activity_id="ACCESS-DEV",
        project_cv=project_cv,
    )


@pytest.mark.unit
def test_project_cv_keeps_real_identity_and_official_variable_table(project_cv):
    original = deepcopy(project_cv)
    vocab = _vocabulary(project_cv)
    attrs = vocab.get_required_global_attributes()
    assert all(
        vocab.variable[key] == value
        for key, value in vocab.cmip_table["variable_entry"]["tos"].items()
    )
    assert attrs["experiment_id"] == "om3-real-run"
    assert attrs["source_id"] == "ACCESS-OM3"
    assert attrs["activity_id"] == "ACCESS-DEV"
    assert attrs["institution_id"] == "ACCESS-NRI"
    assert attrs["nominal_resolution"] == "25 km"
    assert "ocean" in attrs["grid"]
    assert "not a CMIP publication" in attrs["title"]
    assert "further_info_url" not in attrs
    assert "license" not in attrs
    assert attrs["tracking_id"].startswith("urn:uuid:")
    assert (
        attrs["access_moppy_project_description"]
        == original["experiment_id"]["om3-real-run"]["description"]
    )
    assert not vocab.requires_parent_information()
    assert project_cv == original
    assert "om3-real-run" not in vocab._load_controlled_vocab()["experiment_id"]
    assert "ACCESS-OM3" not in vocab._load_controlled_vocab()["source_id"]


@pytest.mark.unit
@pytest.mark.parametrize(
    "section,key,expected",
    [
        ("experiment_id", "description", "experiment and description"),
        ("experiment_id", "parent_experiment_id", "parent_experiment_id"),
        ("source_id", "label", "label"),
        ("source_id", "model_component", "model_component"),
        ("source_id", "institution_id", "institution_id"),
    ],
)
def test_incomplete_local_identity_fails(project_cv, section, key, expected):
    cv = deepcopy(project_cv)
    cv[section][next(iter(cv[section]))].pop(key)
    with pytest.raises(ValueError, match=expected):
        _vocabulary(cv)


@pytest.mark.unit
def test_conflict_and_cache_isolation(project_cv):
    cv = deepcopy(project_cv)
    cv["experiment_id"]["historical"] = cv["experiment_id"]["om3-real-run"]
    with pytest.raises(ValueError, match="conflicts with official entries"):
        _vocabulary(cv)
    first = _vocabulary(project_cv)
    first.vocab["experiment_id"]["om3-real-run"]["experiment"] = "changed"
    second = _vocabulary(project_cv)
    assert second.experiment["experiment"] == "ACCESS-OM3 development run"
    assert "om3-real-run" not in first._load_controlled_vocab()["experiment_id"]


@pytest.mark.unit
def test_project_cv_rejects_unsafe_identity_and_invalid_license(project_cv):
    cv = deepcopy(project_cv)
    cv["experiment_id"]["../escape"] = cv["experiment_id"].pop("om3-real-run")
    with pytest.raises(ValueError, match="safe nonempty strings"):
        _vocabulary(cv)
    cv = deepcopy(project_cv)
    cv["source_id"]["ACCESS-OM3"]["license_info"] = {"license": ""}
    with pytest.raises(ValueError, match="license_info"):
        _vocabulary(cv)


@pytest.mark.unit
def test_real_run_id_with_plus_is_preserved(project_cv):
    run_id = "MC_25km_jra_iaf+wombatlite-test4-d28e0359"
    cv = deepcopy(project_cv)
    cv["experiment_id"][run_id] = cv["experiment_id"].pop("om3-real-run")
    vocab = CMIP6Vocabulary(
        compound_name="Omon.tos",
        experiment_id=run_id,
        source_id="ACCESS-OM3",
        variant_label="r1i1p1f1",
        grid_label="gn",
        project_cv=cv,
    )
    assert vocab.get_required_global_attributes()["experiment_id"] == run_id


@pytest.mark.unit
def test_cmip7_rejects_partial_project_cv_support(project_cv):
    with pytest.raises(ValueError, match="CMIP6/CMIP6Plus only"):
        CMIP7Vocabulary(
            compound_name="ocean.thetao.tavg-h2m-hxy-u.mon.GLB",
            experiment_id="om3-real-run",
            source_id="ACCESS-OM3",
            variant_label="r1i1p1f1",
            grid_label="g999",
            project_cv=project_cv,
        )


@pytest.mark.unit
def test_cmip6plus_inherits_local_identity_without_official_license_claim(project_cv):
    vocab = CMIP6PlusVocabulary(
        compound_name="Omon.tos",
        experiment_id="om3-real-run",
        source_id="ACCESS-OM3",
        variant_label="r1i1p1f1",
        grid_label="gn",
        project_cv=project_cv,
    )
    attrs = vocab.get_required_global_attributes()
    assert attrs["source_id"] == "ACCESS-OM3"
    assert "license" not in attrs
    assert "further_info_url" not in attrs
