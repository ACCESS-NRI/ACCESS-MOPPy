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
    monkeypatch.setattr(dask_config, "_estimate_worker_memory_gb", lambda *_, **__: 16)

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
    monkeypatch.setattr(dask_config, "_estimate_worker_memory_gb", lambda *_, **__: 16)

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
    monkeypatch.setattr(dask_config, "_estimate_worker_memory_gb", lambda *_, **__: 16)

    with pytest.raises(MemoryError, match="requires at least 16.00GB"):
        dask_config.recommend_dask_config(
            "Amon.tas",
            ["input.nc"],
            "ACCESS-ESM1-6",
            n_cpus=8,
            mem_gb=15,
        )


def test_recommend_dask_config_uses_conservative_unchunked_floor(monkeypatch):
    monkeypatch.setattr(dask_config, "_estimate_worker_memory_gb", lambda *_, **__: 12)

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
                "ACCESS-ESM1-6": {
                    "Amon.tas": {"peak_rss_mb": 3500.0, "n_files_per_partition": 100}
                },
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
                "ACCESS-ESM1-6": {
                    "Amon.ta": {"peak_rss_mb": 11400.0, "n_files_per_partition": 1980}
                },
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
                "ACCESS-ESM1-6": {
                    "fx.areacella": {"peak_rss_mb": 5.0, "n_files_per_partition": 1}
                },
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
                "ACCESS-ESM1-6": {
                    "Amon.ta": {"peak_rss_mb": 11400.0, "n_files_per_partition": 1980}
                },
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
                "ACCESS-ESM1-6": {
                    "Amon.ta": {"peak_rss_mb": 11400.0, "n_files_per_partition": 1980}
                },
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
                "ACCESS-ESM1-6": {
                    "Amon.ta": {"peak_rss_mb": 500.0, "n_files_per_partition": 12}
                },
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
                "ACCESS-ESM1-6": {
                    "Amon.ta": {"peak_rss_mb": 11400.0, "n_files_per_partition": 1980}
                },
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
    assert record["n_files_per_partition"] == 1980
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
    assert record["n_files_per_partition"] == 1980
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
    assert record["n_files_per_partition"] == 1980
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
    assert record["n_files_per_partition"] == 1980
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


def test_estimate_worker_memory_gb_prefers_the_partition_scale_for_history(monkeypatch):
    seen = {}

    def _fake_lookup(variable, model_id, n_files_per_partition):
        seen["scale"] = n_files_per_partition
        return None

    monkeypatch.setattr(dask_config, "_load_measured_floor_gb", _fake_lookup)
    monkeypatch.setattr(dask_config, "_floor_gb", lambda tier: 16)

    # A decade-partitioned run holds one partition in a graph at a time, so
    # that -- not the whole multi-century file list -- is the scale to match.
    dask_config._estimate_worker_memory_gb(
        "Amon.ta",
        [f"file{i}.nc" for i in range(2076)],
        "ACCESS-ESM1-6",
        n_files_per_partition=120,
    )

    assert seen["scale"] == 120


def test_estimate_worker_memory_gb_uses_trivial_tier_for_self_contained(monkeypatch):
    monkeypatch.delenv("MOPPY_WORKER_MEMORY_HISTORY", raising=False)

    # Reads no files, so the heavy-tier fallback for a failed probe must not
    # apply -- there was nothing to probe in the first place.
    result = dask_config._estimate_worker_memory_gb(
        "fx.areacella", [], "ACCESS-ESM1-6", self_contained=True
    )

    assert result == 2


def test_recommend_dask_config_sizes_self_contained_below_the_heavy_floor(monkeypatch):
    monkeypatch.delenv("MOPPY_WORKER_MEMORY_HISTORY", raising=False)

    # Regression: the heavy floor made any allocation under 16GB raise
    # MemoryError for variables that measure 2-3GB for the whole job, which
    # blocked giving them a smaller allocation at all.
    config = dask_config.recommend_dask_config(
        "fx.areacella",
        [],
        "ACCESS-ESM1-6",
        n_cpus=12,
        mem_gb=8,
        self_contained=True,
    )

    assert config == {
        "n_workers": 4,
        "threads_per_worker": 1,
        "memory_limit": "2.00GB",
    }


def test_load_measured_floor_gb_reuses_a_longer_run_partitioned_the_same_way(
    monkeypatch, tmp_path
):
    history_path = tmp_path / "history.json"
    history_path.write_text(
        json.dumps(
            {
                "ACCESS-ESM1-6": {
                    "Amon.ta": {"peak_rss_mb": 3700.0, "n_files_per_partition": 120}
                },
            }
        )
    )
    monkeypatch.setenv("MOPPY_WORKER_MEMORY_HISTORY", str(history_path))

    # 173 years and 1000 years partitioned into decades have the same working
    # set; gating on the whole-run total would have rejected the longer one.
    floor = dask_config._load_measured_floor_gb("Amon.ta", "ACCESS-ESM1-6", 120)

    assert floor == pytest.approx((3700.0 / 1024.0) * 1.5)


def test_load_measured_floor_gb_ignores_records_without_a_partition_scale(
    monkeypatch, tmp_path
):
    history_path = tmp_path / "history.json"
    history_path.write_text(
        json.dumps(
            {
                "ACCESS-ESM1-6": {"Amon.ta": {"peak_rss_mb": 3700.0, "n_files": 2076}},
            }
        )
    )
    monkeypatch.setenv("MOPPY_WORKER_MEMORY_HISTORY", str(history_path))

    # Records predating the rename hold a whole-run total that cannot be
    # compared against a per-partition count, so they must be ignored rather
    # than silently under-sizing an unpartitioned run.
    assert dask_config._load_measured_floor_gb("Amon.ta", "ACCESS-ESM1-6", 120) is None


def test_record_measured_peak_records_the_partition_scale(monkeypatch, tmp_path):
    history_path = tmp_path / "history.json"
    monkeypatch.setenv("MOPPY_WORKER_MEMORY_HISTORY", str(history_path))

    dask_config.record_measured_peak(
        "Amon.ta", "ACCESS-ESM1-6", {"tcp://127.0.0.1:1": 3700.0}, 120
    )

    record = json.loads(history_path.read_text())["ACCESS-ESM1-6"]["Amon.ta"]
    assert record["n_files_per_partition"] == 120
    assert record["peak_rss_mb"] == 3700.0


@pytest.mark.parametrize(
    "source_partition_years, n_partitions, recreate_every, qc_plots, expected",
    [
        (10, 103, 8, False, True),  # recreated every 8 partitions
        (10, 8, None, False, True),  # few partitions, never recreated
        (10, 103, None, False, False),  # each worker sees all 103
        (10, 103, 9, False, False),
        (20, 52, 8, False, False),  # longer partitions than measured
        (10, 103, 8, True, False),  # QC plots
        (None, 1, None, False, False),  # not partitioned
    ],
)
def test_bounded_worker_lifetime(
    source_partition_years, n_partitions, recreate_every, qc_plots, expected
):
    assert (
        dask_config._bounded_worker_lifetime(
            source_partition_years, n_partitions, recreate_every, qc_plots
        )
        is expected
    )


def _time_axis(values, units):
    import xarray as xr

    return xr.Dataset(coords={"time": ("time", values, {"units": units})})


@pytest.mark.parametrize(
    "values, units, expected",
    [
        ([15.5, 45.0, 74.5, 105.0], "days since 0101-01-01", 29.5),
        ([1.5, 4.5, 7.5], "hours since 0101-01-01 00:00:00", 0.125),
        ([0.0, 86400.0], "seconds since 2000-01-01", 1.0),
        ([15.5], "days since 0101-01-01", None),  # one step: no spacing
        ([0.0, 1.0], "months since 0101-01-01", None),  # not a fixed length
        ([0.0, 1.0], "", None),
    ],
)
def test_time_step_days(values, units, expected):
    assert dask_config._time_step_days(_time_axis(values, units)) == expected


@pytest.mark.parametrize(
    "variable, expected",
    [
        ("Omon.tos", 30.0),
        ("Amon.tas", 30.0),
        ("day.pr", 1.0),
        ("3hr.tas", 0.125),
        ("fx.areacella", None),
        ("nonsense", None),
    ],
)
def test_output_step_days(variable, expected):
    assert dask_config._output_step_days(variable) == expected


@pytest.fixture
def probe_file(tmp_path, monkeypatch):
    """Write a small input file for the tier probe, without measured history."""
    import numpy as np
    import xarray as xr

    for env_name, _ in dask_config._FLOOR_ENV.values():
        monkeypatch.delenv(env_name, raising=False)
    monkeypatch.delenv("MOPPY_WORKER_MEMORY_HISTORY", raising=False)
    monkeypatch.delenv("MOPPY_MEDIUM_MB_PER_FILE", raising=False)

    def write(model_var, step_days, n_steps):
        path = tmp_path / f"{model_var}_{step_days}_{n_steps}.nc"
        xr.Dataset(
            {model_var: (("time", "y", "x"), np.zeros((n_steps, 4, 4), "f4"))},
            coords={
                "time": (
                    "time",
                    np.arange(n_steps) * step_days,
                    {"units": "days since 0101-01-01"},
                )
            },
        ).to_netcdf(path)
        return [str(path)]

    return write


@pytest.mark.parametrize(
    "variable, model_var, step_days, n_steps, bounded, expected",
    [
        # A year of monthly ocean output in one file is not resampled.
        ("Omon.tos", "surface_temp", 30.4, 12, False, 12),
        ("Omon.tos", "surface_temp", 30.4, 12, True, 3),
        # One month per file, as the atmosphere writes it.
        ("Omon.tos", "surface_temp", 30.4, 1, True, 3),
        # Daily input for a monthly output is: heavy, bounded or not.
        ("Omon.tos", "surface_temp", 1.0, 31, False, 16),
        ("Omon.tos", "surface_temp", 1.0, 31, True, 16),
        # 3-hourly input for a 3-hourly output is not.
        ("3hr.tas", "fld_s03i236", 0.125, 80, False, 12),
    ],
)
def test_estimate_worker_memory_gb_tiers(
    probe_file, variable, model_var, step_days, n_steps, bounded, expected
):
    floor = dask_config._estimate_worker_memory_gb(
        variable,
        probe_file(model_var, step_days, n_steps),
        "ACCESS-ESM1-6",
        bounded_lifetime=bounded,
    )

    assert floor == expected


def test_estimate_worker_memory_gb_treats_an_unreadable_step_as_resampled(
    probe_file, monkeypatch
):
    monkeypatch.setattr(dask_config, "_time_step_days", lambda ds: None)

    # Several steps per file and no way to read their spacing: they may be
    # sub-monthly, so size for resampling rather than risk under-sizing.
    floor = dask_config._estimate_worker_memory_gb(
        "Omon.tos",
        probe_file("surface_temp", 30.4, 12),
        "ACCESS-ESM1-6",
        bounded_lifetime=True,
    )

    assert floor == 16


def test_recommend_dask_config_uses_the_bounded_floor_for_short_worker_lifetimes(
    monkeypatch,
):
    seen = {}

    def _fake_estimate(*_, bounded_lifetime=False, **__):
        seen["bounded"] = bounded_lifetime
        return 3 if bounded_lifetime else 12

    monkeypatch.setattr(dask_config, "_estimate_worker_memory_gb", _fake_estimate)

    config = dask_config.recommend_dask_config(
        "Omon.tos",
        ["input.nc"],
        "ACCESS-ESM1-6",
        n_cpus=28,
        mem_gb=128,
        source_partition_years=10,
        n_partitions=103,
        recreate_every=8,
    )

    assert seen["bounded"] is True
    assert config["n_workers"] == 28
    assert config["memory_limit"] == f"{128 / 28:.2f}GB"

    dask_config.recommend_dask_config(
        "Omon.tos", ["input.nc"], "ACCESS-ESM1-6", n_cpus=28, mem_gb=128
    )
    assert seen["bounded"] is False


@pytest.mark.parametrize("bounded, expected", [(False, 14), (True, 3)])
def test_estimate_worker_memory_gb_bounds_medium_variables_too(
    probe_file, monkeypatch, bounded, expected
):
    # Every probed file now counts as a multi-field (carbon pool) read.
    monkeypatch.setenv("MOPPY_MEDIUM_MB_PER_FILE", "0")

    floor = dask_config._estimate_worker_memory_gb(
        "Omon.tos",
        probe_file("surface_temp", 30.4, 1),
        "ACCESS-ESM1-6",
        bounded_lifetime=bounded,
    )

    assert floor == expected


@pytest.mark.parametrize("variable", ["3hr.tas", "6hrPlevPt.ua", "E1hr.pr"])
def test_repack_heavy_output_for_sub_daily_output_reads_no_file(variable):
    assert dask_config.repack_heavy_output(variable, [], "ACCESS-ESM1-6") is True


@pytest.mark.parametrize(
    "step_days, n_steps",
    [
        (30.4, 12),  # a year of monthly ocean output in one file
        (30.4, 1),  # one month per file: no spacing to read
        (1.0, 31),  # daily input for a monthly output: a thirtieth of it
    ],
)
@pytest.mark.parametrize("threshold, expected", [(0.0007, True), (0.0009, False)])
def test_repack_heavy_output_estimates_output_mb_per_year(
    probe_file, monkeypatch, step_days, n_steps, threshold, expected
):
    # Each file holds 4 x 4 float32 values (64 bytes) per step, so every case
    # comes to ~780 bytes of monthly output per simulated year.
    monkeypatch.setattr(dask_config, "_REPACK_HEAVY_MB_PER_YEAR", threshold)

    heavy = dask_config.repack_heavy_output(
        "Omon.tos", probe_file("surface_temp", step_days, n_steps), "ACCESS-ESM1-6"
    )

    assert heavy is expected


def test_repack_heavy_output_counts_the_largest_model_variable_only(
    probe_file, monkeypatch
):
    """The output is one variable, so tiles summed into it are not counted."""
    monkeypatch.setattr(dask_config, "_REPACK_HEAVY_MB_PER_YEAR", 0.0009)
    monkeypatch.setattr(
        "access_moppy.utilities.load_model_mappings",
        lambda variable, model_id: {
            "tos": {"model_variables": ["surface_temp", "surface_temp"]}
        },
    )

    assert not dask_config.repack_heavy_output(
        "Omon.tos", probe_file("surface_temp", 30.4, 12), "ACCESS-ESM1-6"
    )


@pytest.mark.parametrize(
    "variable, kwargs",
    [
        ("Omon.tos", {"self_contained": True}),
        ("Omon.tos", {}),  # no input files
        ("fx.areacella", {}),
    ],
)
def test_repack_heavy_output_is_false_without_a_file_to_probe(variable, kwargs):
    assert (
        dask_config.repack_heavy_output(variable, [], "ACCESS-ESM1-6", **kwargs)
        is False
    )


def test_repack_heavy_output_is_false_when_the_probe_fails(tmp_path, monkeypatch):
    monkeypatch.setattr(dask_config, "_REPACK_HEAVY_MB_PER_YEAR", 0.0)

    assert (
        dask_config.repack_heavy_output(
            "Omon.tos", [str(tmp_path / "missing.nc")], "ACCESS-ESM1-6"
        )
        is False
    )
