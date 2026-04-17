"""Tests for access_moppy.legacy_utilities.calc_hybrid_height_coeffs.

These tests exercise the calc_ab() function using synthetic vertlevs data
rather than real files – no f90nml or actual ACCESS files are required.
"""

import textwrap
from pathlib import Path
from unittest import mock

import numpy as np
import pytest

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _write_vertlevs(tmp_path: Path, eta_theta, eta_rho, z_top, first_const) -> Path:
    """Write a minimal UM-style vertlevs namelist file and return its path."""

    def _fmt(arr):
        return ", ".join(f"{v:.10f}" for v in arr)

    content = textwrap.dedent(f"""\
        &VERTLEVS
        z_top_of_model = {z_top:.6f},
        first_constant_r_rho_level = {first_const},
        eta_theta = {_fmt(eta_theta)},
        eta_rho = {_fmt(eta_rho)},
        /
        """)
    p = tmp_path / "test_vertlevs"
    p.write_text(content)
    return p


def _make_simple_vertlevs(tmp_path, n_lev=5, transition_at=3):
    """
    Construct a simple synthetic vertlevs with *n_lev* levels and the rho
    'transition' (first_constant_r_rho_level) at index *transition_at*.

    Levels are equally spaced in eta from 0 to 1.
    """
    z_top = 85000.0
    # eta_theta: 0, 1/n_lev, 2/n_lev, ... 1   (length n_lev+1, includes surface)
    eta_theta = np.linspace(0.0, 1.0, n_lev + 1)
    # eta_rho: midpoints between theta levels (length n_lev, no surface)
    eta_rho = 0.5 * (eta_theta[:-1] + eta_theta[1:])
    first_const = transition_at
    return _write_vertlevs(tmp_path, eta_theta, eta_rho, z_top, first_const)


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


class TestCalcAb:
    """Tests for calc_hybrid_height_coeffs.calc_ab()."""

    @pytest.fixture(autouse=True)
    def _require_f90nml(self):
        """Skip this test class if f90nml is not available."""
        pytest.importorskip("f90nml")

    @pytest.mark.unit
    def test_output_shapes(self, tmp_path):
        """a/b arrays should have length model_levels + 1."""
        from access_moppy.legacy_utilities.calc_hybrid_height_coeffs import calc_ab

        n_lev = 5
        vfile = _make_simple_vertlevs(tmp_path, n_lev=n_lev)
        a_theta, b_theta, a_rho, b_rho = calc_ab(vfile)
        assert a_theta.shape == (n_lev + 1,)
        assert b_theta.shape == (n_lev + 1,)
        assert a_rho.shape == (n_lev + 1,)
        assert b_rho.shape == (n_lev + 1,)

    @pytest.mark.unit
    def test_b_surface_is_one(self, tmp_path):
        """The lowest usable non-zero b_theta value should be close to 1."""
        from access_moppy.legacy_utilities.calc_hybrid_height_coeffs import calc_ab

        vfile = _make_simple_vertlevs(tmp_path, n_lev=5, transition_at=3)
        _, b_theta, _, b_rho = calc_ab(vfile)
        # Level 1 (index 1) is just above the surface; b should be nearly 1.
        # Exact value depends on spacing, but must be in (0, 1].
        assert 0.0 < b_theta[1] <= 1.0
        assert 0.0 < b_rho[1] <= 1.0

    @pytest.mark.unit
    def test_b_zero_above_transition(self, tmp_path):
        """b must be exactly 0.0 for all levels at or above first_constant_r_rho_level."""
        from access_moppy.legacy_utilities.calc_hybrid_height_coeffs import calc_ab

        transition = 3
        vfile = _make_simple_vertlevs(tmp_path, n_lev=5, transition_at=transition)
        _, b_theta, _, b_rho = calc_ab(vfile)
        np.testing.assert_array_equal(b_theta[transition:], 0.0)
        np.testing.assert_array_equal(b_rho[transition:], 0.0)

    @pytest.mark.unit
    def test_b_nonzero_below_transition(self, tmp_path):
        """b must be > 0 for all levels strictly below first_constant_r_rho_level."""
        from access_moppy.legacy_utilities.calc_hybrid_height_coeffs import calc_ab

        transition = 3
        vfile = _make_simple_vertlevs(tmp_path, n_lev=5, transition_at=transition)
        _, b_theta, _, b_rho = calc_ab(vfile)
        # Indices 1 .. transition-1 should be non-zero
        assert np.all(b_theta[1:transition] > 0.0)
        assert np.all(b_rho[1:transition] > 0.0)

    @pytest.mark.unit
    def test_a_equals_eta_times_ztop(self, tmp_path):
        """a(k) must equal eta(k) * z_top_of_model for all levels."""
        from access_moppy.legacy_utilities.calc_hybrid_height_coeffs import calc_ab

        n_lev = 5
        z_top = 85000.0
        transition = 3
        vfile = _make_simple_vertlevs(tmp_path, n_lev=n_lev, transition_at=transition)
        a_theta, _, a_rho, _ = calc_ab(vfile)

        eta_theta = np.linspace(0.0, 1.0, n_lev + 1)
        eta_rho = 0.5 * (eta_theta[:-1] + eta_theta[1:])

        np.testing.assert_allclose(a_theta[1:], eta_theta[1:] * z_top, rtol=1e-6)
        np.testing.assert_allclose(a_rho[1:], eta_rho * z_top, rtol=1e-6)

    @pytest.mark.unit
    def test_quadratic_formula(self, tmp_path):
        """b(k) must equal (1 - eta(k)/eta_etadot)^2 for the orography region."""
        from access_moppy.legacy_utilities.calc_hybrid_height_coeffs import calc_ab

        n_lev = 5
        transition = 3
        vfile = _make_simple_vertlevs(tmp_path, n_lev=n_lev, transition_at=transition)
        _, b_theta, _, b_rho = calc_ab(vfile)

        eta_theta = np.linspace(0.0, 1.0, n_lev + 1)
        eta_rho = 0.5 * (eta_theta[:-1] + eta_theta[1:])
        # eta_etadot is eta_rho at UM level first_constant_r_rho_level (1-based).
        # The namelist rho array is 1-based in the UM convention, so Python index
        # is (transition - 1).
        eta_etadot = eta_rho[transition - 1]

        for k in range(1, transition):
            expected_b_theta = (1.0 - eta_theta[k] / eta_etadot) ** 2
            # b_rho[k] uses the k-th rho level (1-based), which is eta_rho[k-1]
            expected_b_rho = (1.0 - eta_rho[k - 1] / eta_etadot) ** 2
            assert b_theta[k] == pytest.approx(expected_b_theta, rel=1e-6)
            assert b_rho[k] == pytest.approx(expected_b_rho, rel=1e-6)

    @pytest.mark.unit
    def test_b_differs_from_raw_eta(self, tmp_path):
        """b must NOT equal the raw eta values – this is the ESM1.5 bug check."""
        from access_moppy.legacy_utilities.calc_hybrid_height_coeffs import calc_ab

        n_lev = 5
        transition = 3
        vfile = _make_simple_vertlevs(tmp_path, n_lev=n_lev, transition_at=transition)
        _, b_theta, _, _ = calc_ab(vfile)

        eta_theta = np.linspace(0.0, 1.0, n_lev + 1)
        # For the orography-following levels, b ≠ η (raw).
        for k in range(1, transition):
            assert b_theta[k] != pytest.approx(eta_theta[k], rel=1e-6), (
                f"Level {k}: b_theta ({b_theta[k]}) must not equal raw eta ({eta_theta[k]}). "
                "This would be the ACCESS-ESM1.5 bug."
            )


class TestCalcAbMissingF90nml:
    """calc_ab should raise a clear ImportError if f90nml is missing."""

    @pytest.mark.unit
    def test_import_error_without_f90nml(self, tmp_path):
        from access_moppy.legacy_utilities.calc_hybrid_height_coeffs import calc_ab

        vfile = tmp_path / "dummy"
        vfile.write_text("&VERTLEVS /\n")

        with mock.patch.dict("sys.modules", {"f90nml": None}):
            with pytest.raises(ImportError, match="f90nml"):
                calc_ab(vfile)


class TestMainEntryPoint:
    """Smoke tests for the CLI entry point."""

    @pytest.fixture(autouse=True)
    def _require_f90nml(self):
        pytest.importorskip("f90nml")

    @pytest.mark.unit
    def test_main_prints_output(self, tmp_path, capsys):
        import sys

        from access_moppy.legacy_utilities.calc_hybrid_height_coeffs import main

        vfile = _make_simple_vertlevs(tmp_path, n_lev=5, transition_at=3)
        with mock.patch.object(sys, "argv", ["moppy-calc-ab-coeffts", str(vfile)]):
            main()

        captured = capsys.readouterr()
        assert "Theta (full) levels a, b" in captured.out
        assert "Rho (half) levels a, b" in captured.out

    @pytest.mark.unit
    def test_main_exits_without_args(self, capsys):
        import sys

        from access_moppy.legacy_utilities.calc_hybrid_height_coeffs import main

        with mock.patch.object(sys, "argv", ["moppy-calc-ab-coeffts"]):
            with pytest.raises(SystemExit) as exc_info:
                main()
        assert exc_info.value.code == 1
