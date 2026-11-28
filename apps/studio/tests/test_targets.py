"""The selected output target must agree with the variable naming scheme."""

import pytest
from fastapi.testclient import TestClient

from moppy_studio.app import create_app
from moppy_studio.configuration import parse_yaml, template, validate
from moppy_studio.service import Service


@pytest.mark.parametrize("target", ["CMIP6", "CMIP6Plus", "CMIP7"])
def test_target_template_api(tmp_path, target):
    client = TestClient(create_app(Service(tmp_path / "state"), "x" * 40))
    response = client.get(
        "/api/template",
        params={"cmip_version": target},
        headers={"X-MOPPY-Token": "x" * 40},
    )
    assert response.status_code == 200
    data = response.json()
    config = data["config"]
    assert parse_yaml(data["yaml"]) == config
    assert config["cmip_version"] == target
    assert all(
        v.count(".") == (4 if target == "CMIP7" else 1) for v in config["variables"]
    )
    # Let the scientific runtime choose target/model-specific grid metadata.
    assert "grid_label" not in config
    config["scheduler_options"] = "#PBS -P iq82"
    assert validate(config, filesystem=False)["cmip_version"] == target


@pytest.mark.parametrize("target", ["CMIP6", "CMIP6Plus"])
def test_target_switch_rejects_cmip7_names(target):
    config = template()
    config["cmip_version"] = target
    config["scheduler_options"] = "#PBS -P iq82"
    with pytest.raises(ValueError, match="requires table.variable"):
        validate(config, filesystem=False)


def test_supported_table_aliases_and_mip_tables():
    for target, variable in [("CMIP7", "Amon.tas"), ("CMIP6Plus", "APmon.tas")]:
        config = template(cmip_version=target)
        config.update(variables=[variable], scheduler_options="#PBS -P iq82")
        assert validate(config, filesystem=False)["variables"] == [variable]


def test_unknown_template_target_is_rejected(tmp_path):
    client = TestClient(create_app(Service(tmp_path / "state"), "x" * 40))
    response = client.get(
        "/api/template?cmip_version=unknown", headers={"X-MOPPY-Token": "x" * 40}
    )
    assert response.status_code == 400
