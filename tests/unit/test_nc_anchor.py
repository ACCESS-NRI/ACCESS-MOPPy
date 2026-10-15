"""Tests for the netCDF anchor worker plugin."""

import os
from types import SimpleNamespace

import pytest

from access_moppy.executors.nc_anchor import ANCHOR_FILENAME, NetCDFAnchorPlugin


@pytest.mark.unit
def test_setup_holds_file_open_and_teardown_closes_it(tmp_path):
    worker = SimpleNamespace(local_directory=str(tmp_path))
    plugin = NetCDFAnchorPlugin()

    plugin.setup(worker)

    assert os.path.exists(tmp_path / ANCHOR_FILENAME)
    assert plugin._anchor.isopen()

    plugin.teardown(worker)

    assert plugin._anchor is None


@pytest.mark.unit
def test_setup_reuses_an_existing_anchor_file(tmp_path):
    # A restarted worker can come back in the same local directory.
    worker = SimpleNamespace(local_directory=str(tmp_path))
    first = NetCDFAnchorPlugin()
    first.setup(worker)
    first.teardown(worker)

    second = NetCDFAnchorPlugin()
    second.setup(worker)

    assert second._anchor.isopen()
    second.teardown(worker)


@pytest.mark.unit
def test_teardown_without_setup_is_a_no_op(tmp_path):
    NetCDFAnchorPlugin().teardown(SimpleNamespace(local_directory=str(tmp_path)))


def _anchor_is_open(dask_worker):
    plugin = dask_worker.plugins.get(NetCDFAnchorPlugin.name)
    return plugin is not None and plugin._anchor is not None and plugin._anchor.isopen()


@pytest.mark.integration
def test_every_worker_holds_an_anchor_and_keeps_it_after_restart():
    from distributed import Client, LocalCluster

    # Same shape as the batch worker: separate processes, one thread each.
    with (
        LocalCluster(
            n_workers=2,
            threads_per_worker=1,
            processes=True,
            dashboard_address=None,
        ) as cluster,
        Client(cluster) as client,
    ):
        client.register_plugin(NetCDFAnchorPlugin())
        assert all(client.run(_anchor_is_open).values())

        # The nanny starts fresh worker processes; the scheduler must
        # re-apply the plugin to them.
        client.restart()
        client.wait_for_workers(2)
        result = client.run(_anchor_is_open)
        assert len(result) == 2
        assert all(result.values())
