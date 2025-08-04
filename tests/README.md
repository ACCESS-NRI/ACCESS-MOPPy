# ACCESS-MOPPeR Test Suite

This directory contains the comprehensive test suite for ACCESS-MOPPeR.

## Test Structure

### 📁 Test Organization

```
tests/
├── conftest.py                     # Shared fixtures and utilities
├── pytest.ini                     # Test configuration
├── test_smoke.py                   # Basic smoke tests
├── test_mop.py                     # Legacy (deprecated)
├── unit/                           # Unit tests
│   ├── test_base.py               # BaseCMORiser tests
│   ├── test_batch_cmoriser.py     # Batch processing tests
│   ├── test_templates.py          # Template tests
│   └── test_tracking.py           # Tracking functionality tests
├── integration/                    # Integration tests
│   ├── test_cmoriser_integration.py    # CMORiser integration
│   ├── test_batch_integration.py       # Batch processing integration
│   └── test_full_cmorisation.py        # Full CMOR workflow tests
├── e2e/                           # End-to-end tests
│   └── test_end_to_end.py         # Real data processing tests
├── performance/                    # Performance and memory tests
│   └── test_memory_usage.py       # Memory usage and optimization tests
├── mocks/                         # Mock data and utilities
│   ├── mock_data.py               # Mock dataset generators
│   ├── mock_files.py              # Mock file utilities
│   └── mock_pbs.py                # Mock PBS/job scheduler
├── data/                          # Test data files
└── scripts/                       # Test utilities and scripts
```

## 🏃 Running Tests

### Run all tests
```bash
pytest tests/
```

### Run by category
```bash
# Unit tests (fast)
pytest tests/unit/ -m unit

# Integration tests (medium speed)
pytest tests/integration/ -m integration

# End-to-end tests (slow, requires test data)
pytest tests/e2e/ -m e2e

# Performance tests (very slow)
pytest tests/performance/ -m performance
```

### Run by speed
```bash
# Fast tests only (good for development)
pytest tests/ -m "not slow"

# Include slow tests (good for CI)
pytest tests/ -m "slow or not slow"
```

### Run smoke tests
```bash
# Quick verification that basic functionality works
pytest tests/test_smoke.py
```

## 🏷️ Test Markers

Tests are marked with the following categories:

- `unit`: Unit tests (fast, isolated)
- `integration`: Integration tests (medium speed, may use mocks)
- `e2e`: End-to-end tests (slow, requires real data)
- `slow`: Tests that take significant time
- `performance`: Performance and memory benchmarks
- `memory`: Memory usage tests

## 📊 Test Coverage

Run tests with coverage reporting:
```bash
pytest tests/ --cov=access_mopper --cov-report=html --cov-report=term
```

View coverage report:
```bash
open htmlcov/index.html
```

## 🔧 Test Configuration

Key configuration in `pytest.ini`:
- Test discovery patterns
- Marker definitions
- Warning filters
- Minimum pytest version requirements

Shared fixtures in `conftest.py`:
- `temp_dir`: Temporary directory for test outputs
- `parent_experiment_config`: Standard parent experiment metadata
- `mock_netcdf_dataset`: Mock xarray dataset
- `mock_config`: Standard CMIP6 configuration
- `batch_config`: Batch processing configuration

## 📝 Test Data

Test data is organized in `tests/data/`:
- `esm1-6/`: Small ACCESS-ESM1.5 sample files
- `om3/`: Ocean model test data
- `small/`: Minimal test datasets
- `fixtures/`: Fixed test configurations

### Adding Test Data

When adding new test data:
1. Keep files small (< 10MB if possible)
2. Use representative but minimal datasets
3. Document the source and contents
4. Use `pytest.mark.skipif` for optional data files

## 🏗️ Writing Tests

### Test Naming Convention
- Test files: `test_*.py`
- Test functions: `test_*`
- Test classes: `Test*`

### Best Practices

1. **Use appropriate test categories**: Mark tests with `@pytest.mark.unit`, `@pytest.mark.integration`, etc.

2. **Use fixtures for setup**: Leverage fixtures in `conftest.py` for common setup.

3. **Make tests independent**: Each test should be able to run in isolation.

4. **Use descriptive names**: Test names should clearly indicate what is being tested.

5. **Test error conditions**: Include tests for error handling and edge cases.

6. **Use subtests for parameterized tests**: When testing multiple similar cases.

### Example Test Structure

```python
import pytest
from access_mopper import ACCESS_ESM_CMORiser

class TestNewFeature:
    """Tests for new feature functionality."""

    @pytest.mark.unit
    def test_basic_functionality(self, mock_config):
        """Test basic feature works correctly."""
        # Test implementation

    @pytest.mark.integration
    @pytest.mark.skipif(not_data_available, reason="Test data not available")
    def test_with_real_data(self, parent_experiment_config):
        """Test feature with real data files."""
        # Test implementation

    @pytest.mark.slow
    def test_performance_characteristics(self):
        """Test that feature meets performance requirements."""
        # Performance test implementation
```

## 🐛 Debugging Tests

### Running specific tests
```bash
# Run single test
pytest tests/unit/test_base.py::TestBaseCMORiser::test_init_with_valid_params

# Run with verbose output
pytest tests/ -v

# Stop on first failure
pytest tests/ -x

# Drop into debugger on failure
pytest tests/ --pdb
```

### Test output
- Use `pytest.skip()` for tests that should be skipped
- Use `pytest.xfail()` for known failures
- Use `pytest.fail()` with descriptive messages

## 🔄 Migration from Legacy Tests

The original `test_mop.py` has been restructured:

**Old structure**:
- All tests in single file
- Duplicate parametrized tests
- Mixed concerns (smoke + integration + validation)

**New structure**:
- `test_smoke.py`: Basic import and initialization tests
- `tests/integration/test_full_cmorisation.py`: Comprehensive CMOR tests
- Proper separation of unit, integration, and e2e tests

### Backward Compatibility

The old `test_mop.py` file is maintained with a deprecation notice to ensure existing workflows continue to work.

## 🚀 CI/CD Integration

The project uses a multi-tier testing strategy to balance speed and coverage:

### Automatic Testing (CI)
**Triggered on**: Pull requests and pushes to non-main branches
**Tests Run**: Smoke tests + Unit tests
```bash
pytest tests/test_smoke.py tests/unit --cov=access_mopper --cov-report=xml
```

### Manual Testing (Workflow Dispatch)
**Triggered manually** via GitHub Actions interface

#### Available Test Suites:
1. **Unit Tests**: `pytest tests/test_smoke.py tests/unit`
2. **Integration Tests**: `pytest tests/integration`
3. **All Tests**: `pytest tests`

#### Full Test Suite Workflow:
Use the "Full Test Suite" workflow for comprehensive testing:
- **Integration Tests**: Medium-speed tests with real CMOR processing
- **End-to-End Tests**: Full workflow tests with real data
- **Performance Tests**: Memory usage and benchmark tests
- **All Tests**: Complete test suite with coverage reporting

### Workflow Files:
- `.github/workflows/ci.yml` - Main CI workflow (automatic + manual options)
- `.github/workflows/full-tests.yml` - Comprehensive testing (manual only)

### Running Tests Locally:
```bash
# Same as automatic CI
pytest tests/test_smoke.py tests/unit

# Same as manual integration
pytest tests/integration

# Same as manual all tests
pytest tests --cov=access_mopper --cov-report=html
```
