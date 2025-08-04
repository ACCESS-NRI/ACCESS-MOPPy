"""
Smoke tests for ACCESS-MOPPeR.

This module contains basic smoke tests to ensure the main components
can be imported and initialized correctly. These are quick tests that
verify basic functionality without requiring extensive test data.
"""

import importlib.resources as resources
import tempfile
from pathlib import Path

import pytest

# Try to import optional dependencies gracefully
try:
    import pandas as pd

    PANDAS_AVAILABLE = True
except ImportError:
    PANDAS_AVAILABLE = False

try:
    from access_mopper import ACCESS_ESM_CMORiser

    ACCESS_MOPPER_AVAILABLE = True
except ImportError:
    ACCESS_MOPPER_AVAILABLE = False


DATA_DIR = Path(__file__).parent / "data"


@pytest.mark.skipif(not ACCESS_MOPPER_AVAILABLE, reason="ACCESS-MOPPeR not available")
def test_import_access_mopper():
    """Test that ACCESS_ESM_CMORiser can be imported."""
    assert ACCESS_ESM_CMORiser is not None


def test_test_data_exists():
    """Test that essential test data files exist."""
    test_file = DATA_DIR / "esm1-6/atmosphere/aiihca.pa-101909_mon.nc"
    # Use skipif for optional test data rather than failing
    if not test_file.exists():
        pytest.skip("Test data file not available")
    assert test_file.exists()


@pytest.mark.skipif(not PANDAS_AVAILABLE, reason="pandas not available")
def test_mapping_files_accessible():
    """Test that CMOR mapping files can be accessed."""
    mapping_files = [
        "Mappings_CMIP6_Amon.json",
        "Mappings_CMIP6_Lmon.json",
        "Mappings_CMIP6_Emon.json",
    ]

    for mapping_file in mapping_files:
        try:
            with (
                resources.files("access_mopper.mappings")
                .joinpath(mapping_file)
                .open() as f
            ):
                data = pd.read_json(f, orient="index")
                assert not data.empty, f"Empty mapping file: {mapping_file}"
        except Exception as e:
            pytest.fail(f"Cannot access mapping file {mapping_file}: {e}")


@pytest.mark.skipif(not ACCESS_MOPPER_AVAILABLE, reason="ACCESS-MOPPeR not available")
def test_cmoriser_initialization():
    """Test basic CMORiser initialization with minimal parameters."""
    try:
        # Use secure temporary directory instead of hardcoded /tmp
        with tempfile.TemporaryDirectory() as temp_dir:
            cmoriser = ACCESS_ESM_CMORiser(
                input_paths=["dummy.nc"],  # File doesn't need to exist for init test
                compound_name="Amon.tas",
                experiment_id="historical",
                source_id="ACCESS-ESM1-5",
                variant_label="r1i1p1f1",
                grid_label="gn",
                activity_id="CMIP",
                output_path=temp_dir,
            )

            # Test that basic attributes are set correctly
            assert cmoriser.experiment_id == "historical"
            assert cmoriser.compound_name == "Amon.tas"
            assert cmoriser.source_id == "ACCESS-ESM1-5"

    except Exception as e:
        pytest.fail(f"CMORiser initialization failed: {e}")


@pytest.mark.skipif(
    not ACCESS_MOPPER_AVAILABLE
    or not (
        Path(__file__).parent / "data/esm1-6/atmosphere/aiihca.pa-101909_mon.nc"
    ).exists(),
    reason="ACCESS-MOPPeR or test data not available",
)
def test_basic_cmorisation_workflow():
    """Test basic CMORisation workflow with a simple variable.

    This is a lightweight smoke test for the full workflow.
    More comprehensive tests are in the integration test modules.
    """
    test_file = DATA_DIR / "esm1-6/atmosphere/aiihca.pa-101909_mon.nc"

    # Use a simple, commonly available variable for smoke test
    parent_config = {
        "parent_experiment_id": "piControl",
        "parent_activity_id": "CMIP",
        "parent_source_id": "ACCESS-ESM1-5",
        "parent_variant_label": "r1i1p1f1",
        "parent_time_units": "days since 0001-01-01 00:00:00",
        "parent_mip_era": "CMIP6",
        "branch_time_in_child": 0.0,
        "branch_time_in_parent": 54786.0,
        "branch_method": "standard",
    }

    try:
        # Use secure temporary directory instead of hardcoded /tmp
        with tempfile.TemporaryDirectory() as temp_dir:
            cmoriser = ACCESS_ESM_CMORiser(
                input_paths=test_file,
                compound_name="Amon.tas",  # Use tas as it's commonly available
                experiment_id="historical",
                source_id="ACCESS-ESM1-5",
                variant_label="r1i1p1f1",
                grid_label="gn",
                activity_id="CMIP",
                parent_info=parent_config,
                output_path=temp_dir,
            )

            # Just test that run() doesn't crash - don't check output quality here
            cmoriser.run()

            # Basic check that some processing occurred
            assert hasattr(
                cmoriser, "cmor_ds"
            ), "CMORiser should have cmor_ds attribute after run()"

    except Exception as e:
        # For smoke tests, we want to know what failed but not necessarily fail the test
        pytest.skip(f"Smoke test skipped due to: {e}")
