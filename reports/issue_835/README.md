# ACCESS-ESM1-6 sea-ice snow thickness validation

Both `snd` fixes first shipped in MOPPy `1.9.0`. PR #812 applies the inclusive 0.01% ice-presence cutoff and propagates missing inputs. PR #813 specifies the CICE5 snow density of 330 kg m⁻³ for ACCESS-ESM1-6. A January 0101 `piControl` comparison on Gadi reproduced the MOPPy `1.8.1` published field from native data and verified both fixes in a fresh CMIP7 file. [Concentration and mask fix](https://github.com/ACCESS-NRI/ACCESS-MOPPy/pull/812) · [Density fix](https://github.com/ACCESS-NRI/ACCESS-MOPPy/pull/813) · [MOPPy 1.9.0](https://github.com/ACCESS-NRI/ACCESS-MOPPy/releases/tag/moppy-v1.9.0)

## Native inputs and conventions

CICE5 writes `sisnmass` as snow volume per grid-cell area multiplied by its ACCESS-driver snow density of 330 kg m⁻³. The January history file records `sisnmass` in kg m⁻² and `siconc` in percent. The validator also checks `siconc` against 100 times the native unitless `aice` fraction. [CICE5 snow density](https://github.com/ACCESS-NRI/cice5/blob/cf5df9d4d26265dc5c79e558e5a67834b51fd38d/drivers/access/ice_constants.F90#L21-L23) · [Snow mass definition](https://github.com/ACCESS-NRI/cice5/blob/cf5df9d4d26265dc5c79e558e5a67834b51fd38d/source/ice_history.F90#L1146-L1149) · [Snow volume accumulation](https://github.com/ACCESS-NRI/cice5/blob/cf5df9d4d26265dc5c79e558e5a67834b51fd38d/source/ice_history.F90#L2232)

The reference computes `sisnmass / (330 × (siconc / 100))` in metres wherever both inputs are present and `siconc` is at least 0.01%. This threshold is 0.0001 in fractional units. Finite inputs below the threshold give zero thickness. Missing mass or concentration gives fill. Existing regressions exercise the value exactly at the cutoff and the representable values immediately below and above it, including float32, float64 and Dask inputs.

## Gadi results

The validator reads one native CICE5 history file and the first month of the immutable `piControl/r1i1p1f1` im55 `g118` publication `v20261003`, tracking ID `hdl:21.14107/637a9c32-296a-4123-b647-4e108c4ef880`. It evaluates the resolved MOPPy mapping and runs the normal CMIP7 writer on the native file. It compares every cell against an independent NumPy calculation and checks the time, bounds, horizontal coordinate centres, branding and encoded fill values. [Validator](validate_native.py) · [Published sample](https://thredds.nci.org.au/thredds/dodsC/im55/publications/MIP-DRS7/CMIP7/CMIP/ACCESS-Consortium/ACCESS-ESM1-6/piControl/r1i1p1f1/glb/mon/snd/tavg-u-hxy-sn/g118/v20261003/snd_tavg-u-hxy-sn_mon_glb_g118_ACCESS-ESM1-6_piControl_r1i1p1f1_010101-010912.nc.html)

| January 0101 diagnostic | Published MOPPy 1.8.1 | Fresh CMIP7 output |
| --- | ---: | ---: |
| Maximum snow thickness | 203.27423 m | 3.643869 m |
| Cells with thickness above 10 m | 10 | 0 |
| Land cells encoded as fill | 0 | 38,218 |
| Positive thickness below 0.01% ice concentration | 1,534 | 0 |
| Positive thickness at or above 0.01% ice concentration | 19,158 | 19,158 |

Native `siconc` in the ten old extreme cells ranged from 3.27 × 10⁻⁹ to 2.70 × 10⁻⁷ percent. All 1,535 finite cells with positive concentration below 0.01% became zero in the fresh file. Each of the 19,158 retained positive thicknesses matched the old value multiplied by 317/330 within float32 precision. The new field was nonnegative and differed from the independent float64 reference by at most `1.40e-7 m`.

The run used MOPPy `a5d44a7361b367ee3963958be7ae1dcf92b7a73a`, CMIP7 tables `3b7171dc7a9e8aba115e7d8c70882ca8c75039b6`, and CMIP6 controlled vocabularies `78a465a7dabb01e357928ea223bc5da871a6e6dc`. All 29 focused regression tests passed. Gadi's repacker left the one-month scratch file uncompressed. The numerical and mask validation covers that one month.

## Reproduce on Gadi

Run from a checkout with the pinned vocabularies available and an existing private scratch directory. The validator writes its output to a fresh subdirectory of that scratch directory and prints the measured comparison as JSON.

```bash
module use /opt/Modules/modulefiles /apps/Modules/modulefiles
module load conda/analysis3-26.11
ulimit -v 4194304
export OPENBLAS_NUM_THREADS=1 OMP_NUM_THREADS=1 MKL_NUM_THREADS=1 NUMEXPR_NUM_THREADS=1
export PYTHONPATH="$PWD/src" PYTHONDONTWRITEBYTECODE=1
timeout --kill-after=10s 180s python reports/issue_835/validate_native.py "$SCRATCH_DIR"
timeout --kill-after=10s 120s python -m pytest -o addopts= --tb=short -q \
  tests/unit/test_derivations_calc_seaice.py::TestCalcSisnthick \
  tests/unit/test_seaice_snow_mappings.py
```

## Publication remediation

The 27 November 2026 im55 inventory records **48 affected dataset versions and 1,081 file references** across ten experiments. Sampled producer metadata identifies MOPPy `1.8.1` for those versions. The pinned file list has SHA-256 `5ba911c299ad533e5ef26db8fa7111d27c09aa6353b56ecdbb493f5f02230c31`. [Inventory PR #866](https://github.com/ACCESS-NRI/ACCESS-MOPPy/pull/866) · [Exact file list](https://github.com/ACCESS-NRI/ACCESS-MOPPy/blob/91a71f4a7c664ce9a53c92260d58a09ff8312915/reports/errata/2026-11-27-im55/C03/affected-files.txt) · [Erratum draft](https://github.com/ACCESS-NRI/ACCESS-MOPPy/blob/91a71f4a7c664ce9a53c92260d58a09ff8312915/reports/errata/2026-11-27-im55/C03/erratum-draft.json)

Regenerate the affected versions from native mass and concentration with a release containing both fixes. The density adjustment corrects retained snow thicknesses. Regeneration also removes the extreme-value artifacts and restores missing values using the native fields. Apply the same formula and mask checks to the replacement series. Register the erratum, coordinate withdrawal of the affected immutable versions and their replicas, and publish replacements under new version identifiers. Record the old and replacement version and tracking-ID mappings in #835 and keep it open until publication remediation is verified. [Publication issue #835](https://github.com/ACCESS-NRI/ACCESS-MOPPy/issues/835)
