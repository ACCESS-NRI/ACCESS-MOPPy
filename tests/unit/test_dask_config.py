import json
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


# -- cross-experiment worker-memory history ---------------------------------


def test_load_measured_floor_gb_returns_none_when_history_unset(monkeypatch):
    monkeypatch.delenv("MOPPY_WORKER_MEMORY_HISTORY", raising=False)

    assert dask_config._load_measured_floor_gb("Amon.tas", "ACCESS-ESM1-6", 100) is None


def test_load_measured_floor_gb_returns_none_when_file_missing(monkeypatch, tmp_path):
    monkeypatch.setenv("MOPPY_WORKER_MEMORY_HISTORY", str(tmp_path / "missing.json"))

    assert dask_config._load_measured_floor_gb("Amon.tas", "ACCESS-ESM1-6", 100) is None


def test_load_measured_floor_gb_returns_none_for_unknown_variable(
    monkeypatch, tmp_path
):
    history_path = tmp_path / "history.json"
    history_path.write_text(
        json.dumps(
            {
                "ACCESS-ESM1-6": {"Amon.tas": {"peak_rss_mb": 3500.0, "n_files": 100}},
            }
        )
    )
    monkeypatch.setenv("MOPPY_WORKER_MEMORY_HISTORY", str(history_path))

    assert dask_config._load_measured_floor_gb("Amon.pr", "ACCESS-ESM1-6", 100) is None


def test_load_measured_floor_gb_scales_measured_peak_by_safety_factor(
    monkeypatch, tmp_path
):
    history_path = tmp_path / "history.json"
    history_path.write_text(
        json.dumps(
            {
                "ACCESS-ESM1-6": {"Amon.ta": {"peak_rss_mb": 11400.0, "n_files": 1980}},
            }
        )
    )
    monkeypatch.setenv("MOPPY_WORKER_MEMORY_HISTORY", str(history_path))
    monkeypatch.setenv("MOPPY_WORKER_MEMORY_SAFETY_FACTOR", "2.0")

    floor = dask_config._load_measured_floor_gb("Amon.ta", "ACCESS-ESM1-6", 1980)

    assert floor == pytest.approx((11400.0 / 1024.0) * 2.0)


def test_load_measured_floor_gb_never_returns_below_the_minimum(monkeypatch, tmp_path):
    history_path = tmp_path / "history.json"
    history_path.write_text(
        json.dumps(
            {
                "ACCESS-ESM1-6": {"fx.areacella": {"peak_rss_mb": 5.0, "n_files": 1}},
            }
        )
    )
    monkeypatch.setenv("MOPPY_WORKER_MEMORY_HISTORY", str(history_path))

    assert (
        dask_config._load_measured_floor_gb("fx.areacella", "ACCESS-ESM1-6", 1) == 2.0
    )


def test_load_measured_floor_gb_returns_none_on_malformed_history(
    monkeypatch, tmp_path
):
    history_path = tmp_path / "history.json"
    history_path.write_text("not json")
    monkeypatch.setenv("MOPPY_WORKER_MEMORY_HISTORY", str(history_path))

    assert dask_config._load_measured_floor_gb("Amon.tas", "ACCESS-ESM1-6", 100) is None


def test_load_measured_floor_gb_used_when_current_run_is_same_scale(
    monkeypatch, tmp_path
):
    history_path = tmp_path / "history.json"
    history_path.write_text(
        json.dumps(
            {
                "ACCESS-ESM1-6": {"Amon.ta": {"peak_rss_mb": 11400.0, "n_files": 1980}},
            }
        )
    )
    monkeypatch.setenv("MOPPY_WORKER_MEMORY_HISTORY", str(history_path))

    assert (
        dask_config._load_measured_floor_gb("Amon.ta", "ACCESS-ESM1-6", 1980)
        is not None
    )


def test_load_measured_floor_gb_used_when_current_run_is_smaller_scale(
    monkeypatch, tmp_path
):
    history_path = tmp_path / "history.json"
    history_path.write_text(
        json.dumps(
            {
                "ACCESS-ESM1-6": {"Amon.ta": {"peak_rss_mb": 11400.0, "n_files": 1980}},
            }
        )
    )
    monkeypatch.setenv("MOPPY_WORKER_MEMORY_HISTORY", str(history_path))

    # A run covering fewer files than the one that produced this record is
    # at most as demanding -- safe to reuse.
    assert (
        dask_config._load_measured_floor_gb("Amon.ta", "ACCESS-ESM1-6", 12) is not None
    )


def test_load_measured_floor_gb_rejected_when_current_run_is_much_larger_scale(
    monkeypatch, tmp_path
):
    history_path = tmp_path / "history.json"
    # Recorded on a "one year" sanity-check run (12 monthly files).
    history_path.write_text(
        json.dumps(
            {
                "ACCESS-ESM1-6": {"Amon.ta": {"peak_rss_mb": 500.0, "n_files": 12}},
            }
        )
    )
    monkeypatch.setenv("MOPPY_WORKER_MEMORY_HISTORY", str(history_path))

    # A 165-year historical run of the same variable must not be sized off
    # a measurement from a run 1/165th the length -- that would silently
    # under-provision it.
    assert dask_config._load_measured_floor_gb("Amon.ta", "ACCESS-ESM1-6", 1980) is None


def test_load_measured_floor_gb_tolerates_small_scale_overage(monkeypatch, tmp_path):
    history_path = tmp_path / "history.json"
    history_path.write_text(
        json.dumps(
            {
                "ACCESS-ESM1-6": {"Amon.ta": {"peak_rss_mb": 11400.0, "n_files": 1980}},
            }
        )
    )
    monkeypatch.setenv("MOPPY_WORKER_MEMORY_HISTORY", str(history_path))

    # 2% more files than the recorded run (e.g. leap-year wobble) is within
    # the tolerance and should still be trusted.
    assert (
        dask_config._load_measured_floor_gb("Amon.ta", "ACCESS-ESM1-6", 2020)
        is not None
    )


def test_record_measured_peak_is_noop_when_history_unset(monkeypatch, tmp_path):
    monkeypatch.delenv("MOPPY_WORKER_MEMORY_HISTORY", raising=False)
    history_path = tmp_path / "history.json"

    dask_config.record_measured_peak("Amon.tas", "ACCESS-ESM1-6", {"w1": 3500.0}, 100)

    assert not history_path.exists()


def test_record_measured_peak_is_noop_for_empty_peaks(monkeypatch, tmp_path):
    history_path = tmp_path / "history.json"
    monkeypatch.setenv("MOPPY_WORKER_MEMORY_HISTORY", str(history_path))

    dask_config.record_measured_peak("Amon.tas", "ACCESS-ESM1-6", {}, 100)

    assert not history_path.exists()


def test_record_measured_peak_writes_the_max_worker_peak(monkeypatch, tmp_path):
    history_path = tmp_path / "history.json"
    monkeypatch.setenv("MOPPY_WORKER_MEMORY_HISTORY", str(history_path))

    dask_config.record_measured_peak(
        "Amon.tas", "ACCESS-ESM1-6", {"w1": 3500.0, "w2": 4200.0}, 1980
    )

    history = json.loads(history_path.read_text())
    record = history["ACCESS-ESM1-6"]["Amon.tas"]
    assert record["peak_rss_mb"] == 4200.0
    assert record["n_files"] == 1980
    assert record["n_observations"] == 1
    assert "updated" in record


def test_record_measured_peak_keeps_the_highest_peak_at_the_same_scale(
    monkeypatch, tmp_path
):
    history_path = tmp_path / "history.json"
    monkeypatch.setenv("MOPPY_WORKER_MEMORY_HISTORY", str(history_path))

    dask_config.record_measured_peak("Amon.tas", "ACCESS-ESM1-6", {"w1": 3500.0}, 1980)
    dask_config.record_measured_peak("Amon.tas", "ACCESS-ESM1-6", {"w1": 2000.0}, 1980)
    dask_config.record_measured_peak("Amon.tas", "ACCESS-ESM1-6", {"w1": 4200.0}, 1980)

    history = json.loads(history_path.read_text())
    record = history["ACCESS-ESM1-6"]["Amon.tas"]
    assert record["peak_rss_mb"] == 4200.0
    assert record["n_files"] == 1980
    assert record["n_observations"] == 3


def test_record_measured_peak_replaces_reference_when_scale_grows(
    monkeypatch, tmp_path
):
    history_path = tmp_path / "history.json"
    monkeypatch.setenv("MOPPY_WORKER_MEMORY_HISTORY", str(history_path))

    # A tiny sanity-check run first...
    dask_config.record_measured_peak("Amon.ta", "ACCESS-ESM1-6", {"w1": 500.0}, 12)
    # ...then the real, much larger production run measures a genuinely
    # bigger peak. The larger-scale measurement must win outright.
    dask_config.record_measured_peak("Amon.ta", "ACCESS-ESM1-6", {"w1": 11400.0}, 1980)

    record = json.loads(history_path.read_text())["ACCESS-ESM1-6"]["Amon.ta"]
    assert record["peak_rss_mb"] == 11400.0
    assert record["n_files"] == 1980
    assert record["n_observations"] == 2


def test_record_measured_peak_never_lowers_floor_from_a_smaller_run(
    monkeypatch, tmp_path
):
    history_path = tmp_path / "history.json"
    monkeypatch.setenv("MOPPY_WORKER_MEMORY_HISTORY", str(history_path))

    # The big production run establishes the reference...
    dask_config.record_measured_peak("Amon.ta", "ACCESS-ESM1-6", {"w1": 11400.0}, 1980)
    # ...a later, much smaller test run must not overwrite it, even though
    # its own peak is lower.
    dask_config.record_measured_peak("Amon.ta", "ACCESS-ESM1-6", {"w1": 500.0}, 12)

    record = json.loads(history_path.read_text())["ACCESS-ESM1-6"]["Amon.ta"]
    assert record["peak_rss_mb"] == 11400.0
    assert record["n_files"] == 1980
    assert record["n_observations"] == 2


def test_record_measured_peak_never_raises_when_directory_cannot_be_created(
    monkeypatch, tmp_path, capsys
):
    # Simulates the shared /g/data path not existing yet, or this user
    # lacking permission to create it -- must degrade to a stderr warning,
    # never propagate and fail the CMORisation job that measured the peak.
    history_path = tmp_path / "not_writable" / "history.json"
    monkeypatch.setenv("MOPPY_WORKER_MEMORY_HISTORY", str(history_path))

    def _raise_mkdir(*args, **kwargs):
        raise PermissionError("no permission to create shared cache directory")

    monkeypatch.setattr(dask_config.Path, "mkdir", _raise_mkdir)

    dask_config.record_measured_peak("Amon.tas", "ACCESS-ESM1-6", {"w1": 3500.0}, 1980)

    assert "Could not record worker-memory history" in capsys.readouterr().err
    assert not history_path.exists()


def test_record_measured_peak_preserves_other_variables(monkeypatch, tmp_path):
    history_path = tmp_path / "history.json"
    monkeypatch.setenv("MOPPY_WORKER_MEMORY_HISTORY", str(history_path))

    dask_config.record_measured_peak("Amon.tas", "ACCESS-ESM1-6", {"w1": 3500.0}, 1980)
    dask_config.record_measured_peak("Amon.pr", "ACCESS-ESM1-6", {"w1": 3600.0}, 1980)

    history = json.loads(history_path.read_text())
    assert set(history["ACCESS-ESM1-6"]) == {"Amon.tas", "Amon.pr"}


def test_estimate_worker_memory_gb_prefers_measured_history_over_probe(monkeypatch):
    monkeypatch.setattr(dask_config, "_load_measured_floor_gb", lambda *_: 22.5)

    # No input files would normally force the conservative heavy-tier
    # fallback; measured history must be checked first and win outright.
    result = dask_config._estimate_worker_memory_gb("Amon.ta", [], "ACCESS-ESM1-6")

    assert result == 22.5


def test_estimate_worker_memory_gb_passes_file_count_to_history_lookup(monkeypatch):
    seen = {}

    def _fake_lookup(variable, model_id, n_input_files):
        seen["n_input_files"] = n_input_files
        return None

    monkeypatch.setattr(dask_config, "_load_measured_floor_gb", _fake_lookup)
    monkeypatch.setattr(dask_config, "_floor_gb", lambda tier: 16)

    dask_config._estimate_worker_memory_gb(
        "Amon.ta", ["a.nc", "b.nc", "c.nc"], "ACCESS-ESM1-6"
    )

    assert seen["n_input_files"] == 3
