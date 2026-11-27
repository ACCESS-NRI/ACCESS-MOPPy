# Soil-temperature depth validation and republication

The deepest CABLE soil layer spans 1.728–4.600 m. Its centre is 3.164 m. The existing CMIP7 publication labels it approximately 2.872 m, the layer thickness, and omits bounds for all six layers. That shifts depth selections and interpolation and affects calculations that depend on layer geometry. [Issue 850](https://github.com/ACCESS-NRI/ACCESS-MOPPy/issues/850)

## Software release status

The fix merged on 27 November 2026 as `1d99b4f5a029a378398e9e74160e383db2103bfd`. `calc_tsl` and the atmosphere writer now use the CABLE geometry shared with layered `mrsol`. The upstream `moppy-v1.9.1` tag points to `f84b99a228cdb0ed331b01a4a6a0fb237fef7a91`, an ancestor of the fix. The first tagged MOPPy release containing the fix remained pending on 27 November 2026. [Merged PR 851](https://github.com/ACCESS-NRI/ACCESS-MOPPy/pull/851), [MOPPy 1.9.1 release](https://github.com/ACCESS-NRI/ACCESS-MOPPy/releases/tag/moppy-v1.9.1)

## Native Gadi verification

The Gadi run used MOPPy `main` at `a5d44a7361b367ee3963958be7ae1dcf92b7a73a`. The public `ACCESS_ESM_CMORiser` wrote CMIP7 monthly `tsl` and CMIP6 `Lmon.tsl` from packed `fld_s08i225` in the archived ACCESS-ESM1-6 `piControl/r1i1p1f1` January 0101 atmosphere file. Each output was compared by layer position against independently decoded native temperatures and the matching January slice of the immutable `v20261002` CMIP7 publication, produced with MOPPy 1.8.1. The script checks the decoded month bounds and horizontal coordinates before comparing the arrays. [Native verification script](validate_native.py), [tested source commit](https://github.com/ACCESS-NRI/ACCESS-MOPPy/commit/a5d44a7361b367ee3963958be7ae1dcf92b7a73a)

| Output | Valid temperatures checked | Missing cells checked | Maximum absolute difference vs native | Maximum absolute difference vs published | Mask mismatches |
| --- | ---: | ---: | ---: | ---: | ---: |
| CMIP7 `tsl` | 65,190 | 101,850 | 0 K | 0 K | 0 |
| CMIP6 `Lmon.tsl` | 65,190 | 101,850 | 0 K | 0 K | 0 |

Each of the six layers had 10,865 valid values and 16,975 missing values. Both outputs stored the missing values as `_FillValue`. The two `tsl` files serialized the following positive-down depth centres and associated `depth_bnds` in metres. A fresh CMIP7 layered `mrsol` file from the same native month had exactly the same centres and bounds.

| Native layer | Centre (m) | Bounds (m) |
| ---: | ---: | --- |
| 1 | 0.011 | 0.000–0.022 |
| 2 | 0.051 | 0.022–0.080 |
| 3 | 0.157 | 0.080–0.234 |
| 4 | 0.4385 | 0.234–0.643 |
| 5 | 1.1855 | 0.643–1.728 |
| 6 | 3.164 | 1.728–4.600 |

The CMIP6 run used MOPPy's existing controlled-vocabulary override for ACCESS-ESM1-6 and verified its software path on the same sample. `cmip7repack` completed and left the two one-timestep CMIP7 outputs uncompressed. Archive-wide comparison and WCRP publication compliance remain part of replacement validation. The prior validation recorded in issue 850 covers twelve `esm-hist` months on the candidate fix. [Earlier pilot](https://github.com/ACCESS-NRI/ACCESS-MOPPy/issues/850)

All 40 existing cases in `tests/unit/test_atmosphere.py::TestSoilDepthDimension` and `tests/unit/test_soil_water_depth.py` passed on Gadi with synchronous Dask.

### Reproduce on Gadi

Run from the checkout with its vocabulary submodules initialized. The test used the CMIP7 tables at `3b7171dc7a9e8aba115e7d8c70882ca8c75039b6` and CMIP6 CVs at `78a465a7dabb01e357928ea223bc5da871a6e6dc`.

```bash
module use /opt/Modules/modulefiles /apps/Modules/modulefiles
module load conda/analysis3-26.11
export OPENBLAS_NUM_THREADS=1 OMP_NUM_THREADS=1 MKL_NUM_THREADS=1 NUMEXPR_NUM_THREADS=1
scratch=$(mktemp -d "/scratch/p73/$USER/moppy-850-XXXXXX")
PYTHONPATH="$PWD/src" nice -n 10 timeout --kill-after=5s 45s \
  python reports/issue_850/validate_native.py "$scratch"
```

The script reads the native and published files under `/g/data`, writes three single-month files to private scratch and prints a JSON result with exact input and output paths, per-layer counts, coordinates and numerical differences.

## Publication remediation

The 27 November 2026 im55 inventory covers **138 affected monthly CMIP7 dataset versions across 15 experiments and 2,261 file references**. Use the immutable H02 manifest to identify the files for replacement and reconcile the local inventory with live ESGF dataset identifiers and versions. The manifest SHA-256 is `d956937ae67fa6b47929476c2ec7d0498a630d6048a8bdc920cd8bb527c9c459`. [Erratum draft and dataset inventory](https://github.com/ACCESS-NRI/ACCESS-MOPPy/tree/91a71f4a7c664ce9a53c92260d58a09ff8312915/reports/errata/2026-11-27-im55/H02), [full affected-file list](https://github.com/ACCESS-NRI/ACCESS-MOPPy/blob/91a71f4a7c664ce9a53c92260d58a09ff8312915/reports/errata/2026-11-27-im55/H02/affected-files.txt), [manifest PR 866](https://github.com/ACCESS-NRI/ACCESS-MOPPy/pull/866)

1. Regenerate the affected `tsl` datasets with a producer build containing `1d99b4f5a029a378398e9e74160e383db2103bfd`. Record the exact producer revision and vocabulary pins.
2. Validate all replacement time ranges, depth centres, bounds and axis attributes. Compare replacement temperatures and masks to the affected versions by layer position and run the current CMIP7 publication checks.
3. Publish validated replacements under new version identifiers, retract confirmed incorrect ESGF versions and register the erratum linking the affected and replacement datasets. Issue 850 tracks that publication work. [WCRP CMIP7 data-manager guidance](https://wcrp-cmip.github.io/cmip7-guidance/latest/CMIP7/Guidance_for_ESGF/#3-dataset-retraction)
