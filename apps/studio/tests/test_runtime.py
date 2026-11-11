"""Exercise the subprocess boundary without importing the scientific stack."""

import json

from moppy_studio import runtime


def test_bridge_subprocess_keeps_web_environment_out_of_scientific_python(
    tmp_path, monkeypatch
):
    directory = tmp_path / "package with spaces"
    directory.mkdir()
    bridge = directory / "bridge.py"
    bridge.write_text(
        "import json, os, sys\n"
        "payload = json.load(sys.stdin)\n"
        "print('Runtime setup output')\n"
        "print('MOPPY_STUDIO_JSON=' + json.dumps({"
        "'action': sys.argv[1], 'payload': payload, "
        "'pythonpath': os.environ.get('PYTHONPATH'), "
        "'pythonhome': os.environ.get('PYTHONHOME'), "
        "'virtualenv': os.environ.get('VIRTUAL_ENV')}))\n"
    )
    monkeypatch.setattr(runtime, "BRIDGE", bridge)
    monkeypatch.setattr(runtime, "module_setup", lambda module: "")
    monkeypatch.setenv("PYTHONPATH", str(tmp_path / "web-site-packages"))
    monkeypatch.setenv("PYTHONHOME", str(tmp_path / "web-python"))
    monkeypatch.setenv("VIRTUAL_ENV", str(tmp_path / "web-venv"))
    payload = {"config": {"variables": ["Amon.tas"]}}
    result = runtime.Runtime().call("inspect", payload, "conda/analysis3-26.10")
    assert result == {
        "action": "inspect",
        "payload": json.loads(json.dumps(payload)),
        "pythonpath": None,
        "pythonhome": None,
        "virtualenv": None,
    }
