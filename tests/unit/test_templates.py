from importlib.resources import files
from textwrap import dedent

import pytest
from jinja2 import Template


class TestTemplates:
    """Test Jinja2 template rendering for job scripts."""

    @pytest.mark.unit
    def test_python_script_template_rendering(self, batch_config, temp_dir):
        """Test that Python script template renders correctly."""
        # Mock template content based on your actual template
        template_content = '''#!/usr/bin/env python
"""
CMORisation script for variable {{ variable }}
"""
import os
from pathlib import Path
import glob
from access_moppy import ACCESS_ESM_CMORiser
from access_moppy.tracking import TaskTracker

def main():
    variable = os.environ['VARIABLE']
    file_patterns = {{ config.get('file_patterns', {}) | tojson }}

    pattern = file_patterns.get(variable)
    if not pattern:
        raise ValueError(f'No pattern found for variable {variable}')

if __name__ == "__main__":
    main()
'''

        template = Template(template_content)

        result = template.render(variable="Amon.tas", config=batch_config)

        assert "Amon.tas" in result
        assert "file_patterns" in result
        assert "main()" in result
        assert batch_config["file_patterns"]["Amon.tas"] in result

    @pytest.mark.unit
    def test_partitioned_worker_template_compiles(self):
        template_path = files("access_moppy.templates").joinpath(
            "cmor_python_script.j2"
        )
        rendered = Template(template_path.read_text()).render(
            variable="atmos.pr.tavg-u-hxy-u.mon.glb",
            config={
                "enable_chunking": True,
                "enable_resampling": True,
                "source_partition_years": 10,
                "split_years": "auto",
            },
            db_path="/tmp/cmor_tasks.db",
            var_dir="/tmp/logs/Amon_tas",
            package_path=".",
        )

        compile(rendered, str(template_path), "exec")
        assert "source_partition_years = 10" in rendered
        assert 'publication_profile="personal"' in rendered
        assert "partition_files_by_year" in rendered
        assert "Source partitioning skipped" in rendered
        assert "supports only direct mappings" not in rendered
        assert "calculation.get('partitionable') is not True" in rendered
        assert ".get('resumable') is False" in rendered
        assert "Resume skipped for" in rendered
        assert "('mon', 'day', '6hr', '3hr', '1hr')" in rendered
        assert "sub-daily variables support source partitioning" in rendered

        # The anchor must be in place before any worker opens a file, i.e.
        # right after the client starts and before the first CMORiser.
        client_at = rendered.index("client = dd.Client(")
        anchor_at = rendered.index("client.register_plugin(NetCDFAnchorPlugin())")
        assert "netCDF anchor held on" in rendered
        cmoriser_at = rendered.index("cmoriser = ACCESS_ESM_CMORiser(")
        assert client_at < anchor_at < cmoriser_at
        assert "self-contained " in rendered
        assert "mappings need no input files" in rendered
        assert "cannot be used for self-contained mappings" not in rendered
        assert "enforce_compliance" not in rendered

    @pytest.mark.unit
    def test_worker_template_resume_check_uses_compound_mapping(self):
        """The resume check must resolve the variable unambiguously.

        A bare short name such as ``rsds`` maps to more than one recipe and
        ``load_variable_entry`` raises ``Ambiguous mapping`` for it. The
        resume check has to pass the compound ``table.variable`` name instead,
        and must honour ``calculation_overrides`` for the output CMIP version
        just like the normal mapping path does.
        """
        template_path = files("access_moppy.templates").joinpath(
            "cmor_python_script.j2"
        )
        rendered = Template(template_path.read_text()).render(
            variable="Amon.rsds",
            config={"resume": True},
            db_path="/tmp/cmor_tasks.db",
            var_dir="/tmp/logs/Amon_rsds",
            package_path=".",
        )
        compile(rendered, str(template_path), "exec")

        # The old check passed only the bare short name and raised
        # ValueError: Ambiguous mapping for 'rsds'.
        assert "discovery_name.split('.')[1]" not in rendered
        assert "load_model_mappings(" in rendered
        # Resumability must come from the same mapping the worker uses,
        # including calculation_overrides for the output CMIP version.
        assert "discovery_name, model_id, cmip_version=cmip_version" in rendered
        # The bare-name lookup that raised Ambiguous mapping is gone.
        assert "load_variable_entry(discovery_name.split('.')[1]" not in rendered

    @pytest.mark.unit
    def test_resume_check_resolves_ambiguous_and_non_resumable_variables(self):
        """Execute the rendered resume check against real mappings.

        Before the fix the check resolved a bare short name and raised
        ``ValueError: Ambiguous mapping`` for any variable with several
        recipes (e.g. ``rsds``). It must instead resolve the compound name
        and still detect calculations marked ``resumable: false``.
        """
        template_path = files("access_moppy.templates").joinpath(
            "cmor_python_script.j2"
        )
        rendered = Template(template_path.read_text()).render(
            variable="Amon.rsds",
            config={"resume": True},
            db_path="/tmp/cmor_tasks.db",
            var_dir="/tmp/logs/Amon_rsds",
            package_path=".",
        )

        start = rendered.index(
            "                # A calculation whose values at one time depend"
        )
        end = rendered.index(
            "            if resume:\n                from access_moppy.batch_cmoriser"
        )
        block = dedent(rendered[start:end])

        def run(discovery_name, cmip_version):
            namespace = {
                "discovery_name": discovery_name,
                "model_id": "ACCESS-ESM1-6",
                "cmip_version": cmip_version,
                "variable": discovery_name,
                "resume": True,
            }
            exec(block, namespace)
            return namespace["resume"]

        # An ambiguous short name must no longer crash the worker.
        assert run("Amon.rsds", "CMIP6") is True
        # A calculation that is not local in time still disables resume.
        assert run("Omon.zostoga", "CMIP6") is False
        # CMIP7 branded compound names resolve through the same path.
        assert run("atmos.rsds.tavg-u-hxy-u.mon.glb", "CMIP7") is True

    @pytest.mark.unit
    def test_worker_template_passes_access_consortium_publication_profile(self):
        template_path = files("access_moppy.templates").joinpath(
            "cmor_python_script.j2"
        )
        rendered = Template(template_path.read_text()).render(
            variable="Amon.tas",
            config={"publication_profile": "access-consortium"},
            db_path="/tmp/cmor_tasks.db",
            var_dir="/tmp/logs/Amon_tas",
            package_path=".",
        )

        compile(rendered, str(template_path), "exec")
        assert 'publication_profile="access-consortium"' in rendered

    @pytest.mark.unit
    def test_worker_template_passes_cmip7repack_queue_settings(self):
        template_path = files("access_moppy.templates").joinpath(
            "cmor_python_script.j2"
        )

        def render(config):
            return Template(template_path.read_text()).render(
                variable="Amon.tas",
                config=config,
                db_path="/tmp/cmor_tasks.db",
                var_dir="/tmp/logs/Amon_tas",
                package_path=".",
            )

        rendered = render(
            {
                "cmip7repack_workers": 4,
                "cmip7repack_buffer_fraction": 0.3,
                "cmip7repack_buffer_max_gb": 20,
            }
        )
        compile(rendered, str(template_path), "exec")
        assert "repack_workers = 4" in rendered
        assert "auto_cpu_split(" not in rendered
        assert "cmoriser.cmoriser.repack_workers = repack_workers" in rendered
        assert "cmoriser.cmoriser.repack_buffer_fraction = 0.3" in rendered
        assert "cmoriser.cmoriser.repack_buffer_max_gb = 20" in rendered

        # "auto": the CPUs are split between the workers and the repacks
        # before the cluster starts, and only CMIP7 output can be
        # repack-heavy; the buffer keeps the CMORiser's own defaults.
        rendered = render({"cmip7repack_workers": "auto"})
        compile(rendered, str(template_path), "exec")
        split_at = rendered.index(
            "dask_config['n_workers'], repack_workers = auto_cpu_split("
        )
        assert (
            rendered.index("dask_config = recommend_dask_config(")
            < split_at
            < rendered.index("cmip_version == 'CMIP7' and repack_heavy_output(")
            < rendered.index("client = start_dask_client()")
        )
        assert "cmoriser.cmoriser.repack_workers = repack_workers" in rendered
        assert "repack_buffer" not in rendered

        # Unset: nothing is set, so the CMORiser's own defaults apply.
        rendered = render({})
        compile(rendered, str(template_path), "exec")
        assert "cmoriser.cmoriser.repack_" not in rendered
        assert "auto_cpu_split(" not in rendered

    @pytest.mark.unit
    def test_worker_template_trims_span_digests_on_every_cluster(self):
        template_path = files("access_moppy.templates").joinpath(
            "cmor_python_script.j2"
        )

        def render(config):
            return Template(template_path.read_text()).render(
                variable="Amon.tas",
                config=config,
                db_path="/tmp/cmor_tasks.db",
                var_dir="/tmp/logs/Amon_tas",
                package_path=".",
            )

        rendered = render({"source_partition_years": 10, "split_years": "auto"})
        compile(rendered, "cmor_python_script.py", "exec")
        assert "recreate_every=None," in rendered
        # Every client, the first and any recreated one, comes from the same
        # factory, which registers both worker plugins.
        factory_at = rendered.index("def start_dask_client():")
        trim_at = rendered.index("new_client.register_plugin(SpanDigestTrimPlugin())")
        first_client_at = rendered.index("client = start_dask_client()")
        assert factory_at < trim_at < first_client_at
        assert rendered.index("cmoriser = ACCESS_ESM_CMORiser(") > first_client_at
        # Rendered without dask_recreate_every (create_job_script fills in the
        # default), the template has no recreation.
        assert "cluster_recreate_due" not in rendered
        assert "retired_worker_peaks" not in rendered

        rendered = render(
            {
                "source_partition_years": 10,
                "split_years": "auto",
                "dask_recreate_every": 4,
            }
        )
        compile(rendered, "cmor_python_script.py", "exec")
        assert (
            "from access_moppy.executors.dask_lifetime import cluster_recreate_due"
            in rendered
        )
        due_at = rendered.index("if cluster_recreate_due(")
        assert "len(input_partitions),\n                4,\n" in rendered
        # Worker sizing sees how many partitions each worker lives through.
        sizing_at = rendered.index("dask_config = recommend_dask_config(")
        sizing = rendered[sizing_at : rendered.index("def start_dask_client():")]
        assert "n_partitions=len(input_partitions)," in sizing
        assert "recreate_every=4," in sizing
        assert "qc_plots=False," in sizing
        assert "source_partition_years if input_partitions[0][0] is not None" in sizing
        # After the partition's CMORiser is closed; the old workers' peak RSS is
        # kept before their cluster goes, and merged into the job's report.
        assert rendered.index("cmoriser.close()") < due_at
        keep_at = rendered.index(
            "retired_worker_peaks.update(peak_worker_memory_mb(client))"
        )
        assert due_at < keep_at < rendered.index("client = start_dask_client()", due_at)
        # Each cluster's repack window is logged before it goes.
        window_at = rendered.index("repack_publisher.log_repack_window(", due_at)
        assert keep_at < window_at < rendered.index("client.close()", due_at)
        assert "worker_peaks = {**retired_worker_peaks, **worker_peaks}" in rendered
        # A fixed number of repacks is never re-split.
        assert "adapt_cpu_split(\n" not in rendered
        assert "repack_publisher.resize(" not in rendered

        # "auto": the window re-splits the CPUs before the next cluster starts.
        rendered = render(
            {
                "source_partition_years": 10,
                "split_years": "auto",
                "dask_recreate_every": 4,
                "cmip7repack_workers": "auto",
            }
        )
        compile(rendered, "cmor_python_script.py", "exec")
        assert "dask_workers_max = dask_config['n_workers']" in rendered
        due_at = rendered.index("if cluster_recreate_due(")
        close_at = rendered.index("client.close()", due_at)
        adapt_at = rendered.index("new_split = adapt_cpu_split(", due_at)
        resize_at = rendered.index("repack_publisher.resize(repack_workers)", due_at)
        assert (
            rendered.index("repack_window = repack_publisher.log_repack_window(")
            < close_at
            < adapt_at
            < resize_at
            < rendered.index("client = start_dask_client()", due_at)
        )

    @pytest.mark.unit
    def test_compliance_check_worker_template_compiles(self):
        template_path = files("access_moppy.templates").joinpath(
            "cmor_python_script.j2"
        )
        rendered = Template(template_path.read_text()).render(
            variable="atmos.pr.tavg-u-hxy-u.mon.glb",
            config={
                "source_partition_years": 10,
                "split_years": "auto",
                "compliance_check": True,
                "compliance_check_min_weight": 1,
                "compliance_check_suite": ["cf:1.11", "wcrp_cmip7:1.0"],
            },
            db_path="/tmp/cmor_tasks.db",
            var_dir="/tmp/logs/Amon_tas",
            package_path=".",
        )

        compile(rendered, str(template_path), "exec")
        assert "from access_moppy.qc.compliance import enforce_compliance" in rendered
        assert "if partition_index == 0:" in rendered
        assert "cmoriser.cmoriser.first_write_hook = " in rendered
        assert "cmip_version=cmip_version," in rendered
        assert "min_weight=1," in rendered
        assert 'suites=["cf:1.11", "wcrp_cmip7:1.0"],' in rendered

        # The gate must be armed before write() runs, otherwise the whole time
        # series is on disk before the first file is validated.
        assert rendered.index("first_write_hook") < rendered.index("cmoriser.write()")

        # The verdict has to reach the task row, or it exists only as a JSON
        # report beside this script that nothing downstream reads.
        assert "on_record=_record_compliance," in rendered
        assert "status.set_compliance(record)" in rendered

    @pytest.mark.unit
    def test_worker_template_never_touches_the_database(self):
        """The single-writer invariant, pinned.

        Gadi mounts Lustre with localflock, so SQLite has no working cross-node
        locking and ~100 workers writing cmor_tasks.db corrupts it. Workers
        must report through their status file only; the monitor is the sole
        database writer. Any reintroduction of a tracker call here is the bug
        recurring, so it fails the build instead.
        """
        template_path = files("access_moppy.templates").joinpath(
            "cmor_python_script.j2"
        )
        rendered = Template(template_path.read_text()).render(
            variable="Amon.tas",
            config={"experiment_id": "historical", "compliance_check": True},
            db_path="/tmp/cmor_tasks.db",
            var_dir="/tmp/logs/Amon_tas",
            package_path=".",
        )

        for forbidden in ("TaskTracker", "tracking", "sqlite", "CMOR_TRACKER_DB"):
            assert forbidden not in rendered, (
                f"generated worker reaches the database via {forbidden!r}"
            )
        assert "TaskStatusFile(" in rendered

    @pytest.mark.unit
    def test_worker_template_merges_gate_results_across_partitions(self):
        """One variable can span several CMORisers; their gates must combine."""
        template_path = files("access_moppy.templates").joinpath(
            "cmor_python_script.j2"
        )
        rendered = Template(template_path.read_text()).render(
            variable="atmos.pr.tavg-u-hxy-u.mon.glb",
            config={"source_partition_years": 10, "split_years": "auto"},
            db_path="/tmp/cmor_tasks.db",
            var_dir="/tmp/logs/Amon_tas",
            package_path=".",
        )

        compile(rendered, str(template_path), "exec")
        assert "CMORiser.merge_gate_results(" in rendered
        assert rendered.index("merge_gate_results") < rendered.index(
            "status.set_output_summary("
        )

    @pytest.mark.unit
    @pytest.mark.parametrize(
        "config",
        [
            {"source_partition_years": 10, "split_years": "auto"},
            {
                "source_partition_years": 10,
                "split_years": "auto",
                "cmip7repack_workers": 4,
                "qc_plots": True,
            },
        ],
    )
    def test_worker_template_shares_one_repack_queue_across_partitions(self, config):
        """Repacks run on across partitions; only the end waits for them."""
        template_path = files("access_moppy.templates").joinpath(
            "cmor_python_script.j2"
        )
        rendered = Template(template_path.read_text()).render(
            variable="atmos.tas.tpt-h2m-hxy-u.3hr.glb",
            config=config,
            db_path="/tmp/cmor_tasks.db",
            var_dir="/tmp/logs/3hr_tas",
            package_path=".",
        )
        compile(rendered, str(template_path), "exec")

        def at(text):
            assert rendered.count(text) == 1, text
            return rendered.index(text)

        # One queue for the variable, made before the partition loop, whether
        # or not cmip7repack_workers is set.
        assert at("SharedRepackPublisher()") < at("for partition_index")
        # Each partition writes through it, after its repack settings are set.
        if "cmip7repack_workers" in config:
            assert at("cmoriser.cmoriser.repack_workers = repack_workers") < at(
                "repack_publisher.attach("
            )
        assert (
            at("for partition_index")
            < at("repack_publisher.attach(")
            < at("cmoriser.write()")
            < at("repack_publisher.written(")
        )
        # Drained once, after the loop; results are read only after that.
        assert (
            at("repack_publisher.written(")
            < at("repack_publisher.finish()")
            < at("repack_publisher.log_repack_window('to the end')")
            < at("written_files = repack_publisher.written_files")
            < at("output_summaries = repack_publisher.output_summaries")
            < at("status.set_output_summary(")
        )
        if config.get("qc_plots"):
            assert at("repack_publisher.finish()") < at("_gen_combined_plots(")
        # Closed in main()'s finally, which runs on success and failure alike.
        assert at("\n    finally:\n") < at("repack_publisher.close()")

    @pytest.mark.unit
    def test_pbs_script_template_rendering(self, batch_config, temp_dir):
        """Test that PBS script template renders correctly."""
        template_content = """#!/bin/bash
#PBS -N cmor_{{ variable | replace('.', '_') }}
#PBS -q {{ config.get('queue', 'normal') }}
#PBS -l ncpus={{ config.get('cpus_per_node', 4) }}
#PBS -l mem={{ config.get('mem', '16GB') }}
#PBS -l walltime={{ config.get('walltime', '01:00:00') }}

export VARIABLE="{{ variable }}"
export EXPERIMENT_ID="{{ config.get('experiment_id') }}"

python {{ python_script_path }}
"""

        template = Template(template_content)

        result = template.render(
            variable="Amon.tas",
            config=batch_config,
            python_script_path="/path/to/script.py",
        )

        assert "#PBS -N cmor_Amon_tas" in result
        assert "#PBS -l ncpus=4" in result
        assert "#PBS -l mem=16GB" in result
        assert 'export VARIABLE="Amon.tas"' in result
        assert 'export EXPERIMENT_ID="historical"' in result

    @pytest.mark.unit
    def test_template_variable_substitution(self):
        """Test various Jinja2 variable substitutions used in templates."""
        template_content = """
Variable: {{ variable }}
Table: {{ variable.split('.')[0] }}
CMOR Name: {{ variable.split('.')[1] }}
CPUs: {{ config.get('cpus_per_node', 1) }}
Memory: {{ config.get('mem', '8GB') }}
Patterns: {{ config.get('file_patterns', {}) | tojson }}
"""

        template = Template(template_content)
        config = {
            "cpus_per_node": 8,
            "mem": "32GB",
            "file_patterns": {"Amon.tas": "/path/to/files/*.nc"},
        }

        result = template.render(variable="Amon.tas", config=config)

        assert "Variable: Amon.tas" in result
        assert "Table: Amon" in result
        assert "CMOR Name: tas" in result
        assert "CPUs: 8" in result
        assert "Memory: 32GB" in result
        assert "/path/to/files/*.nc" in result
