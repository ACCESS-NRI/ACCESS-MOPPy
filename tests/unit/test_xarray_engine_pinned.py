"""Every xarray open in the package must name its engine.

When ``engine`` is omitted, xarray auto-detects it by importing *every*
registered ``xarray.backends`` entry point and asking each one
``guess_can_open()``. The analysis3 environment registers 37 of them,
including argopy's ``argo`` and ``erddapy`` backends, which reach out to
raw.githubusercontent.com on import. A Gadi compute node has no route
there, so those imports block until they time out.

The cost is paid once per process, at the *first* open that does not name
an engine -- measured at 38-46s per batch job. It is not attached to any
particular call site: pinning some opens and not others simply moves the
tax to the first one still unpinned (measured: pinning only the input-file
opens moved 40s out of the Dask sizing probe and into the CMIP7 range gate
in ``qc/cmip7.py``). So this has to hold for the whole package, which is
why the check is structural rather than a test of one function.

Every file MOPPy opens is netCDF -- model output (NETCDF4 for the UM
atmosphere, NETCDF4_CLASSIC for MOM ocean and CICE ice), its own
intermediate output, and the bundled ``resources/*.nc`` -- all of which the
``netcdf4`` engine reads.
"""

import ast
from pathlib import Path

import pytest

import access_moppy

_OPENERS = {"open_dataset", "open_mfdataset", "open_zarr"}


def _unpinned_opens():
    """Yield ``path:lineno`` for every xarray open that omits ``engine``.

    A call that forwards ``**kwargs`` is accepted: ``CMORiser.load_dataset``
    builds one ``common_kwargs`` dict (which sets ``engine``) and expands it
    into three ``open_mfdataset`` calls, and the expansion is opaque here.
    """
    root = Path(access_moppy.__file__).parent
    for path in sorted(root.rglob("*.py")):
        if ".ipynb_checkpoints" in str(path):
            continue
        try:
            tree = ast.parse(path.read_text())
        except SyntaxError:  # pragma: no cover - not our source to fix
            continue
        for node in ast.walk(tree):
            if (
                isinstance(node, ast.Call)
                and isinstance(node.func, ast.Attribute)
                and node.func.attr in _OPENERS
            ):
                keywords = {kw.arg for kw in node.keywords}
                if "engine" not in keywords and None not in keywords:
                    yield f"{path.relative_to(root)}:{node.lineno}"


@pytest.mark.unit
def test_every_xarray_open_pins_the_engine():
    unpinned = list(_unpinned_opens())
    assert not unpinned, (
        "these xarray opens omit engine= and would pay the backend "
        f"auto-detection cost: {unpinned}"
    )


@pytest.mark.unit
def test_load_dataset_common_kwargs_pins_the_engine():
    """Close the ``**kwargs`` hole the structural check has to allow."""
    source = Path(access_moppy.__file__).parent / "base.py"
    tree = ast.parse(source.read_text())
    pinned = [
        value.value
        for node in ast.walk(tree)
        if isinstance(node, ast.Assign)
        and any(
            isinstance(t, ast.Name) and t.id == "common_kwargs" for t in node.targets
        )
        and isinstance(node.value, ast.Dict)
        for key, value in zip(node.value.keys, node.value.values)
        if isinstance(key, ast.Constant) and key.value == "engine"
    ]
    assert pinned == ["netcdf4"], (
        "load_dataset's common_kwargs must pin engine='netcdf4'; it is "
        "expanded into the open_mfdataset calls the structural check skips"
    )
