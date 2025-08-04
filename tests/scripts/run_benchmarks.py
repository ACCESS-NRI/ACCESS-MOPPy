#!/usr/bin/env python
"""
Run performance benchmarks for ACCESS-MOPPeR.
"""

import json
import os
import sys
import time
from datetime import datetime
from pathlib import Path

import psutil

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent.parent / "src"))

import tempfile

from tests.mocks.mock_data import create_chunked_dataset


def benchmark_memory_usage():
    """Benchmark memory usage with different dataset sizes."""
    print("Running memory usage benchmarks...")

    results = {}
    test_sizes = [
        (100, 50, 100),  # Small
        (365, 100, 200),  # Medium
        (1000, 145, 192),  # Large
    ]

    for n_time, n_lat, n_lon in test_sizes:
        size_name = f"{n_time}x{n_lat}x{n_lon}"
        print(f"  Testing size: {size_name}")

        process = psutil.Process(os.getpid())
        initial_memory = process.memory_info().rss / 1024 / 1024  # MB

        start_time = time.time()

        # Create and process dataset
        with tempfile.TemporaryDirectory():
            dataset = create_chunked_dataset(
                n_time=n_time,
                n_lat=n_lat,
                n_lon=n_lon,
                chunks={
                    "time": min(n_time, 100),
                    "lat": min(n_lat, 50),
                    "lon": min(n_lon, 100),
                },
            )

            # Simulate some processing
            _ = dataset.load()  # Force computation

        end_time = time.time()
        final_memory = process.memory_info().rss / 1024 / 1024  # MB

        results[size_name] = {
            "processing_time_seconds": end_time - start_time,
            "memory_usage_mb": final_memory - initial_memory,
            "dataset_size": {"time": n_time, "lat": n_lat, "lon": n_lon},
        }

        print(f"    Time: {results[size_name]['processing_time_seconds']:.2f}s")
        print(f"    Memory: {results[size_name]['memory_usage_mb']:.1f}MB")

    return results


def benchmark_chunking_strategies():
    """Benchmark different chunking strategies."""
    print("Running chunking strategy benchmarks...")

    results = {}
    chunk_strategies = [
        {"time": 100, "lat": 50, "lon": 100},
        {"time": 50, "lat": 25, "lon": 50},
        {"time": 200, "lat": 100, "lon": 200},
        {"time": 10, "lat": 10, "lon": 20},
    ]

    # Fixed dataset size for comparison
    n_time, n_lat, n_lon = 1000, 100, 200

    for i, chunks in enumerate(chunk_strategies):
        strategy_name = (
            f"strategy_{i+1}_{chunks['time']}x{chunks['lat']}x{chunks['lon']}"
        )
        print(f"  Testing chunking: {strategy_name}")

        process = psutil.Process(os.getpid())
        initial_memory = process.memory_info().rss / 1024 / 1024

        start_time = time.time()

        dataset = create_chunked_dataset(
            n_time=n_time, n_lat=n_lat, n_lon=n_lon, chunks=chunks
        )

        # Force some computation
        _ = dataset.mean(dim="time").compute()

        end_time = time.time()
        final_memory = process.memory_info().rss / 1024 / 1024

        results[strategy_name] = {
            "processing_time_seconds": end_time - start_time,
            "memory_usage_mb": final_memory - initial_memory,
            "chunk_size": chunks,
        }

        print(f"    Time: {results[strategy_name]['processing_time_seconds']:.2f}s")
        print(f"    Memory: {results[strategy_name]['memory_usage_mb']:.1f}MB")

    return results


def main():
    """Run all benchmarks and save results."""
    print("ACCESS-MOPPeR Performance Benchmarks")
    print("=" * 40)

    all_results = {
        "timestamp": datetime.now().isoformat(),
        "system_info": {
            "cpu_count": psutil.cpu_count(),
            "memory_total_gb": psutil.virtual_memory().total / (1024**3),
            "python_version": sys.version,
        },
    }

    # Run benchmarks
    all_results["memory_usage"] = benchmark_memory_usage()
    print()
    all_results["chunking_strategies"] = benchmark_chunking_strategies()

    # Save results
    results_dir = Path(__file__).parent.parent / "reports" / "performance"
    results_dir.mkdir(parents=True, exist_ok=True)

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    results_file = results_dir / f"benchmark_results_{timestamp}.json"

    with open(results_file, "w") as f:
        json.dump(all_results, f, indent=2)

    print(f"\nResults saved to: {results_file}")

    # Print summary
    print("\nSummary:")
    print("--------")
    memory_results = all_results["memory_usage"]
    for size, result in memory_results.items():
        print(
            f"{size}: {result['processing_time_seconds']:.2f}s, {result['memory_usage_mb']:.1f}MB"
        )


if __name__ == "__main__":
    main()
