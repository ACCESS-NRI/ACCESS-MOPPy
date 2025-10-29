import os
from unittest.mock import patch

import psutil
import pytest

from access_moppy import ACCESS_ESM_CMORiser
from tests.mocks.mock_data import create_chunked_dataset


class TestMemoryUsage:
    """Performance and memory usage tests."""

    @pytest.mark.slow
    def test_memory_usage_large_dataset(self, temp_dir):
        """Test memory usage with large simulated dataset."""
        process = psutil.Process(os.getpid())
        initial_memory = process.memory_info().rss / 1024 / 1024  # MB

        with patch("access_moppy.base.xr.open_mfdataset") as mock_open:
            # Create large chunked dataset
            large_dataset = create_chunked_dataset(
                n_time=3650,  # 10 years daily data
                n_lat=180,
                n_lon=360,
                chunks={"time": 365, "lat": 90, "lon": 180},
            )
            mock_open.return_value = large_dataset

            cmoriser = ACCESS_ESM_CMORiser(
                input_paths=["mock_large_file.nc"],
                compound_name="Amon.tas",
                output_path=temp_dir,
                experiment_id="historical",
                source_id="ACCESS-ESM1-5",
                variant_label="r1i1p1f1",
                grid_label="gn",
                activity_id="CMIP",
            )

            cmoriser.run()

            peak_memory = process.memory_info().rss / 1024 / 1024  # MB
            memory_increase = peak_memory - initial_memory

            # Memory increase should be reasonable (less than 2GB for chunked data)
            assert memory_increase < 2048, f"Memory usage too high: {memory_increase}MB"

    @pytest.mark.slow
    def test_chunking_effectiveness(self, temp_dir):
        """Test that chunking keeps memory usage reasonable."""
        chunk_sizes = [
            {"time": 100, "lat": 50, "lon": 100},
            {"time": 50, "lat": 25, "lon": 50},
            {"time": 10, "lat": 10, "lon": 20},
        ]

        memory_usage = []

        for chunks in chunk_sizes:
            process = psutil.Process(os.getpid())
            initial_memory = process.memory_info().rss / 1024 / 1024

            with patch("access_moppy.base.xr.open_mfdataset") as mock_open:
                dataset = create_chunked_dataset(
                    n_time=1000, n_lat=100, n_lon=200, chunks=chunks
                )
                mock_open.return_value = dataset

                cmoriser = ACCESS_ESM_CMORiser(
                    input_paths=["mock_file.nc"],
                    compound_name="Amon.tas",
                    output_path=temp_dir,
                    experiment_id="historical",
                    source_id="ACCESS-ESM1-5",
                    variant_label="r1i1p1f1",
                    grid_label="gn",
                    activity_id="CMIP",
                )

                cmoriser.run()

                peak_memory = process.memory_info().rss / 1024 / 1024
                memory_increase = peak_memory - initial_memory
                memory_usage.append(memory_increase)

        # Smaller chunks should generally use less peak memory
        # (though this isn't always strictly true due to overhead)
        assert all(
            usage < 1024 for usage in memory_usage
        ), "Memory usage too high for chunked data"
