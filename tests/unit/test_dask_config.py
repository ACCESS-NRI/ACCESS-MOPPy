from types import SimpleNamespace

import pytest

from access_moppy.executors import dask_config


@pytest.fixture(autouse=True)
def fixed_system_memory(monkeypatch):
    monkeypatch.setattr(
        "psutil.virtual_memory",
        lambda: SimpleNamespace(total=256 * 1024**3),
    )


def test_recommend_dask_config_preserves_default_streaming_sizing(monkeypatch):
    monkeypatch.setattr(dask_config, "_estimate_worker_memory_gb", lambda *_: 16)

    config = dask_config.recommend_dask_config(
        "Amon.tas",
        ["input.nc"],
        "ACCESS-ESM1-6",
        n_cpus=8,
        mem_gb=64,
        enable_chunking=True,
        max_chunk_size_mb=128,
        write_prefetch=4,
    )

    assert config == {
        "n_workers": 4,
        "threads_per_worker": 1,
        "memory_limit": "16.00GB",
    }


def test_recommend_dask_config_accounts_for_larger_write_window(monkeypatch):
    monkeypatch.setattr(dask_config, "_estimate_worker_memory_gb", lambda *_: 16)

    config = dask_config.recommend_dask_config(
        "Amon.tas",
        ["input.nc"],
        "ACCESS-ESM1-6",
        n_cpus=8,
        mem_gb=64,
        enable_chunking=True,
        max_chunk_size_mb=1024,
        write_prefetch=8,
    )

    assert config["n_workers"] == 2
    assert config["memory_limit"] == "32.00GB"


def test_recommend_dask_config_rejects_allocation_below_streaming_floor(monkeypatch):
    monkeypatch.setattr(dask_config, "_estimate_worker_memory_gb", lambda *_: 16)

    with pytest.raises(MemoryError, match="requires at least 16.00GB"):
        dask_config.recommend_dask_config(
            "Amon.tas",
            ["input.nc"],
            "ACCESS-ESM1-6",
            n_cpus=8,
            mem_gb=15,
        )


def test_recommend_dask_config_uses_conservative_unchunked_floor(monkeypatch):
    monkeypatch.setattr(dask_config, "_estimate_worker_memory_gb", lambda *_: 12)

    with pytest.raises(MemoryError, match="requires at least 28.00GB"):
        dask_config.recommend_dask_config(
            "Amon.tas",
            ["input.nc"],
            "ACCESS-ESM1-6",
            n_cpus=8,
            mem_gb=27,
            enable_chunking=False,
        )


def test_peak_worker_memory_mb_returns_per_worker_results():
    client = SimpleNamespace(run=lambda fn: {"tcp://w1": 9821.4, "tcp://w2": 9650.2})

    result = dask_config.peak_worker_memory_mb(client)

    assert result == {"tcp://w1": 9821.4, "tcp://w2": 9650.2}


def test_peak_worker_memory_mb_returns_empty_dict_on_error():
    def _raise(fn):
        raise RuntimeError("no workers available")

    client = SimpleNamespace(run=_raise)

    assert dask_config.peak_worker_memory_mb(client) == {}


def test_peak_rss_mb_reads_ru_maxrss_as_kilobytes_on_linux(monkeypatch):
    monkeypatch.setattr(dask_config.platform, "system", lambda: "Linux")
    monkeypatch.setattr(
        dask_config.resource,
        "getrusage",
        lambda who: SimpleNamespace(ru_maxrss=2_000_000),
    )

    assert dask_config._peak_rss_mb() == pytest.approx(2_000_000 / 1024)


def test_peak_rss_mb_reads_ru_maxrss_as_bytes_on_macos(monkeypatch):
    monkeypatch.setattr(dask_config.platform, "system", lambda: "Darwin")
    monkeypatch.setattr(
        dask_config.resource,
        "getrusage",
        lambda who: SimpleNamespace(ru_maxrss=2_000_000_000),
    )

    assert dask_config._peak_rss_mb() == pytest.approx(2_000_000_000 / (1024 * 1024))
