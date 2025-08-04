from jinja2 import Template


class TestTemplates:
    """Test Jinja2 template rendering for job scripts."""

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
from access_mopper import ACCESS_ESM_CMORiser
from access_mopper.tracking import TaskTracker

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
        assert '"/path/to/files/*.nc"' in result
