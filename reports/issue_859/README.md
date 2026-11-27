# Top 10 cm soil moisture for issue 859

MOPPy PR 819 corrected the CABLE layer integration, preserved missing soil columns, and attached the scalar depth coordinate. The fix is commit `faf714673879ee80cd3f205cf169373098fc5e75`, first released in `moppy-v1.9.0`. Monthly `Lmon.mrsos` and daily `day.mrsos` use the same recipe for the CMIP7 product `mrsol_tavg-d10cm-hxy-lnd`. [Original issue 817](https://github.com/ACCESS-NRI/ACCESS-MOPPy/issues/817), [merged PR 819](https://github.com/ACCESS-NRI/ACCESS-MOPPy/pull/819), [release tag](https://github.com/ACCESS-NRI/ACCESS-MOPPy/tree/moppy-v1.9.0)

The first three native soil layers span 0–2.2, 2.2–8.0 and 8.0–23.4 cm. The independent reference takes all water from layers 1 and 2 and `0.020 / 0.154` (13.0%) of layer 3, assuming uniform water content within that layer. The legacy formula treated layer centres as interfaces and took about 46.2% of layer 3. The excess therefore varies with the amount of water in layer 3. [CABLE layer constants](https://github.com/CABLE-LSM/CABLE/blob/15f639dd33dfb15819304332d72c2b405b51b85e/src/offline/cable_parameters.F90#L1240)

## Gadi validation

On 27 November 2026, MOPPy commit `a5d44a7361b367ee3963958be7ae1dcf92b7a73a` wrote fresh CMIP7 `g115` NetCDF files from the archived ACCESS-ESM1-6 `piControl` member `r1i1p1f1`. The monthly check used the original packed January 0101 file. The daily check staged the first daily mean and its original coordinates and time bounds from the archived daily file. The comparison read `fld_s08i223` directly from both archive files with netCDF4 and computed the reference in NumPy float64.

| Native interval | Land values compared | Maximum absolute error in fresh output (kg m⁻²) | Ocean cells encoded as fill |
| --- | ---: | ---: | ---: |
| January 0101 | 10,865 | 0 | 16,975 |
| 1 January 0101 | 10,865 | 0 | 16,975 |

Both outputs preserve the native horizontal coordinates and time bounds. Both link the soil-water field to scalar depth 5 cm with positive direction down and bounds 0–10 cm. All contributing layers are valid on land and missing on ocean, as independently confirmed using native `fld_s03i395`. The deeper layers contain water, and the two outputs agree exactly with the reference that excludes it.

The first archived day in `v20261117`, produced by MOPPy 1.9.1, also passes these checks. In the January sample from the older monthly `v20261002`, produced by 1.8.1, the published-to-correct ratio ranges from 1.391 to 1.861, with an unweighted mean of 1.536 across the 10,865 valid land cells. The published values match the old formula within 3.16 × 10⁻⁶ kg m⁻² using the nominal layer centres. The file contains zeros in all 16,975 ocean cells and retains six unassociated depth indices labelled `cm`. These comparisons cover one native day and one native month from a single member.

The existing soil-water depth, output-name and mapping-resolution suites passed on Gadi (132 tests).

### Reproduce

Run `validate_native.py` from a checkout with the vocabulary submodules initialized. The test used CMIP7 tables at `3b7171dc7a9e8aba115e7d8c70882ca8c75039b6` and CMIP6 CVs at `78a465a7dabb01e357928ea223bc5da871a6e6dc`. [Validation script](validate_native.py)

```bash
module use /opt/Modules/modulefiles /apps/Modules/modulefiles
module load conda/analysis3-26.11
export OPENBLAS_NUM_THREADS=1 OMP_NUM_THREADS=1 MKL_NUM_THREADS=1 NUMEXPR_NUM_THREADS=1
scratch=$(mktemp -d "/scratch/p73/$USER/moppy-859-XXXXXX")
PYTHONPATH="$PWD/src" timeout 45s python reports/issue_859/validate_native.py "$scratch"
```

The script reads the native and versioned publication files under `/g/data`, writes to a new directory inside private scratch, and prints one JSON result per frequency. The writer's repack step completed and reported that the two single-timestep files were uncompressed. This run checked values, masks and coordinates on the generated files.

## Publication remediation

The 27 November im55 inventory identifies 58 affected monthly dataset versions and 1,141 file references, including all 10 AMIP members. The audit found corrected depth geometry in the sampled files for 80 ScenarioMIP monthly datasets and 18 daily datasets. Use the C02 erratum package for the exact replacement scope. [Erratum and affected-file manifest](https://github.com/ACCESS-NRI/ACCESS-MOPPy/tree/91a71f4a7c664ce9a53c92260d58a09ff8312915/reports/errata/2026-11-27-im55/C02), [manifest PR 866](https://github.com/ACCESS-NRI/ACCESS-MOPPy/pull/866)

1. Regenerate the 58 monthly datasets from native layer masses using a MOPPy build that includes PR 819. Record the exact producer commit and vocabulary pins for the replacement.
2. Validate the replacement timelines, associated 5 cm scalar depth and 0–10 cm bounds. Compare against native `layer1 + layer2 + (0.020 / 0.154) × layer3` and verify that missing soil columns stay missing.
3. Publish corrected dataset versions and link the erratum to the superseded versions. Keep the issue open until the replacement versions and the erratum are public. [Issue 859](https://github.com/ACCESS-NRI/ACCESS-MOPPy/issues/859)
