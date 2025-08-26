"""Mock file system operations for testing."""

import fnmatch

from pathlib import Path
from unittest.mock import patch


class MockFileSystem:
    """Mock file system for testing file operations."""

    def __init__(self):
        self.files = {}
        self.directories = set()

    def add_file(self, path, content="mock content"):
        """Add a mock file to the file system."""
        path = Path(path)
        self.files[str(path)] = content
        # Add parent directories
        for parent in path.parents:
            self.directories.add(str(parent))

    def add_directory(self, path):
        """Add a mock directory."""
        self.directories.add(str(Path(path)))

    def mock_exists(self, path):
        """Mock Path.exists() method."""
        path_str = str(path)
        return path_str in self.files or path_str in self.directories

    def mock_is_file(self, path):
        """Mock Path.is_file() method."""
        return str(path) in self.files

    def mock_is_dir(self, path):
        """Mock Path.is_dir() method."""
        return str(path) in self.directories

    def mock_glob(self, pattern):
        """Mock glob.glob() function."""
        return [
            file_path for file_path in self.files if fnmatch.fnmatch(file_path, pattern)
        ]

    def _pattern_matches(self, pattern, path):
        """Simple pattern matching (not full glob)."""
        if "*" in pattern:
            # Very basic wildcard matching
            parts = pattern.split("*")
            return all(part in path for part in parts if part)
        return pattern == path


def mock_access_file_structure(base_path="/mock/data"):
    """Create a mock ACCESS model output file structure."""
    fs = MockFileSystem()

    # Add typical ACCESS output directories
    for output_num in range(5):
        output_dir = f"{base_path}/output{output_num:03d}"
        fs.add_directory(output_dir)
        fs.add_directory(f"{output_dir}/atmosphere")
        fs.add_directory(f"{output_dir}/atmosphere/netCDF")
        fs.add_directory(f"{output_dir}/ocean")

        # Add mock atmosphere files
        for month in range(1, 13):
            atmos_file = f"{output_dir}/atmosphere/netCDF/aiihca.pa-{output_num:02d}{month:02d}09_mon.nc"
            fs.add_file(atmos_file)

        # Add mock ocean files
        ocean_file = f"{output_dir}/ocean/ocean-2d-surface_temp-1monthly-mean-{output_num:02d}.nc"
        fs.add_file(ocean_file)

    return fs


def patch_file_operations(mock_fs):
    """Context manager to patch file operations with mock filesystem."""

    class FilePatcher:
        def __enter__(self):
            self.patchers = [
                patch("pathlib.Path.exists", side_effect=mock_fs.mock_exists),
                patch("pathlib.Path.is_file", side_effect=mock_fs.mock_is_file),
                patch("pathlib.Path.is_dir", side_effect=mock_fs.mock_is_dir),
                patch("glob.glob", side_effect=mock_fs.mock_glob),
            ]
            for patcher in self.patchers:
                patcher.start()
            return self

        def __exit__(self, exc_type, exc_val, exc_tb):
            for patcher in self.patchers:
                patcher.stop()

    return FilePatcher()
