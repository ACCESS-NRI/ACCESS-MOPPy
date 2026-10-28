"""Tests for the span-digest trim plugin and cluster recreation."""

import asyncio
import time
from types import SimpleNamespace

import pytest

from access_moppy.executors.dask_lifetime import (
    DEFAULT_DASK_RECREATE_EVERY,
    SPAN_DIGEST_TRIM_INTERVAL_S,
    SpanDigestTrimPlugin,
    cluster_recreate_due,
    is_span_digest,
    resolve_dask_recreate_every,
    trim_span_digests,
)

SPAN = "9a9facbe-4e62-4677-9267-548173ca4b76"


@pytest.mark.unit
@pytest.mark.parametrize(
    "key, expected",
    [
        (("execute", SPAN, "open_dataset", "thread-cpu", "seconds"), True),
        # Bounded: no span, or not an execute metric.
        (("execute", None, "open_dataset", "thread-cpu", "seconds"), False),
        (("get-data", "compress", "seconds"), False),
        ("task-duration", False),
        ("execute", False),
        (("execute",), False),
    ],
)
def test_is_span_digest(key, expected):
    assert is_span_digest(key) is expected


@pytest.mark.unit
def test_trim_span_digests_keeps_the_fixed_name_digests():
    digests = {
        ("execute", SPAN, "where", "thread-cpu", "seconds"): 1,
        ("execute", "another-span", "getitem", "other", "seconds"): 2,
        ("execute", None, "where", "thread-cpu", "seconds"): 3,
        ("get-data", "compress", "seconds"): 4,
        "task-duration": 5,
    }

    assert trim_span_digests(digests) == 2
    assert set(digests) == {
        ("execute", None, "where", "thread-cpu", "seconds"),
        ("get-data", "compress", "seconds"),
        "task-duration",
    }


@pytest.mark.unit
def test_trim_span_digests_without_digests():
    # Server.digests is None when crick is not installed.
    assert trim_span_digests(None) == 0
    assert trim_span_digests({}) == 0


@pytest.mark.unit
def test_plugin_trims_on_a_periodic_callback_and_stops_it_on_teardown():
    async def run():
        worker = SimpleNamespace(
            digests={("execute", SPAN, "where", "thread-cpu", "seconds"): 1},
            periodic_callbacks={},
        )
        plugin = SpanDigestTrimPlugin(interval_s=0.01)

        plugin.setup(worker)
        callback = worker.periodic_callbacks[SpanDigestTrimPlugin.name]
        assert callback.is_running()
        assert callback.callback_time == 10
        await asyncio.sleep(0.1)
        assert worker.digests == {}

        plugin.teardown(worker)
        assert not callback.is_running()
        assert SpanDigestTrimPlugin.name not in worker.periodic_callbacks
        # Tearing down twice, or before setup, is harmless.
        plugin.teardown(worker)

    asyncio.run(run())


@pytest.mark.unit
def test_plugin_defaults_to_the_module_interval():
    assert SpanDigestTrimPlugin().interval_s == SPAN_DIGEST_TRIM_INTERVAL_S


@pytest.mark.unit
@pytest.mark.parametrize("interval_s", [0, -1])
def test_plugin_rejects_a_non_positive_interval(interval_s):
    with pytest.raises(ValueError, match="interval_s"):
        SpanDigestTrimPlugin(interval_s=interval_s)


@pytest.mark.unit
@pytest.mark.parametrize(
    "partition_index, n_partitions, every, expected",
    [
        (0, 10, None, False),  # recreation off
        (2, 10, 3, True),
        (5, 10, 3, True),
        (3, 10, 3, False),
        (0, 10, 1, True),
        (9, 10, 1, False),  # nothing left to run after the last partition
        (8, 9, 3, False),
    ],
)
def test_cluster_recreate_due(partition_index, n_partitions, every, expected):
    assert cluster_recreate_due(partition_index, n_partitions, every) is expected


@pytest.mark.unit
@pytest.mark.parametrize("every", [0, -2, True, 2.5, "3"])
def test_cluster_recreate_due_rejects_a_bad_interval(every):
    with pytest.raises(ValueError, match="dask_recreate_every"):
        cluster_recreate_due(0, 10, every)


@pytest.mark.unit
@pytest.mark.parametrize(
    "config, expected",
    [
        ({}, None),  # one partition: nothing to recreate
        ({"source_partition_years": None}, None),
        ({"source_partition_years": 10}, DEFAULT_DASK_RECREATE_EVERY),
        ({"source_partition_years": 10, "dask_recreate_every": 4}, 4),
        ({"source_partition_years": 10, "dask_recreate_every": None}, None),
        ({"source_partition_years": 10, "dask_recreate_every": 0}, None),
        ({"source_partition_years": 10, "dask_recreate_every": False}, None),
        ({"dask_recreate_every": 3}, 3),  # explicit, partitioned or not
    ],
)
def test_resolve_dask_recreate_every(config, expected):
    assert resolve_dask_recreate_every(config) == expected


@pytest.mark.unit
@pytest.mark.parametrize("every", [-2, True, 2.5, "3", 0.0])
def test_resolve_dask_recreate_every_rejects_a_bad_interval(every):
    with pytest.raises(ValueError, match="dask_recreate_every"):
        resolve_dask_recreate_every(
            {"source_partition_years": 10, "dask_recreate_every": every}
        )


def _span_digest_count(dask_worker):
    return sum(is_span_digest(key) for key in dask_worker.digests)


def _fixed_name_digests(dask_worker):
    return sorted(str(key) for key in dask_worker.digests if not is_span_digest(key))


@pytest.mark.integration
def test_workers_drop_span_digests_and_keep_doing_so_after_restart():
    pytest.importorskip("crick")
    da = pytest.importorskip("dask.array")
    from distributed import Client, LocalCluster

    def compute_and_wait(client):
        for i in range(3):
            (da.ones((40, 40), chunks=10) + i).sum().compute()
        # Every compute() leaves span digests until the next trim.
        assert sum(client.run(_span_digest_count).values()) > 0
        deadline = time.monotonic() + 10
        while sum(client.run(_span_digest_count).values()):
            assert time.monotonic() < deadline, "span digests were never trimmed"
            time.sleep(0.1)

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
        client.register_plugin(SpanDigestTrimPlugin(interval_s=0.2))
        compute_and_wait(client)
        # The fixed-name digests (latency, transfer bandwidth, ...) survive.
        assert all(client.run(_fixed_name_digests).values())

        # The scheduler re-applies the plugin to restarted workers.
        client.restart()
        client.wait_for_workers(2)
        compute_and_wait(client)
