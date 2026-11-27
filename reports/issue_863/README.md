# ACCESS-ESM1.6 SST moment precision

The negative variance reported in ACCESS-MOPPy issue #863 is already present when the two archived MOM moments are evaluated in float64. The archived monthly `piControl` pair gives a minimum of −0.0119210 °C² in January 0101. MOPPy cannot recover the precision missing from the archived moments. [Issue #863](https://github.com/ACCESS-NRI/ACCESS-MOPPy/issues/863)

## Diagnostic definition

The `Omon.tossq` and `Oday.tossq` CMIP6 tables and the CMIP7 `tossq_tavg-u-hxy-sea` entry specify `square_of_sea_surface_temperature`, units `degC2`, and `area: mean where sea time: mean`. The reported field is the mean of squared SST. The within-period variance is `tossq − tos²`, with `tos` from the same grid cell and averaging interval. [CMIP6 Omon](../../src/access_moppy/vocabularies/cmip6_cmor_tables/Tables/CMIP6_Omon.json) · [CMIP6 Oday](../../src/access_moppy/vocabularies/cmip6_cmor_tables/Tables/CMIP6_Oday.json) · [CMIP7 ocean table](https://github.com/WCRP-CMIP/cmip7-cmor-tables/blob/3b7171dc7a9e8aba115e7d8c70882ca8c75039b6/tables/CMIP7_ocean.json)

The earlier fix made `tossq` derive from MOM's second-moment diagnostic. MOPPy also handles direct squared-Celsius input from standardised model output. [Second-moment fix](https://github.com/ACCESS-NRI/ACCESS-MOPPy/commit/23dc2e1a8e1cba3076dd720e133d8d3565d27cf9) · [Input unit handling](https://github.com/ACCESS-NRI/ACCESS-MOPPy/pull/843)

The archived moments in the Gadi samples are MOM `surface_temp` (`K`) and `squared_surface_temp` (`squared K`). Both are unpacked float32 time means with matching ocean masks and averaging support. The separate fixed-grid coordinates correspond to the published grid. The validation checks numeric timestamps, time bounds, calendar and native averaging duration for each record. The Celsius second moment is obtained from the Kelvin first and second moments as `m2 − 2 × 273.15 × m1 + 273.15²`. Subtracting the square of the Celsius mean cancels the offset in exact arithmetic and gives `m2 − m1²`.

## Rounding check

For each native float32 moment, the diagnostic takes the midpoint to each adjacent representable value as the endpoints of the last-rounding interval. For the positive Kelvin mean, the implied variance interval is `[m2_low − m1_high², m2_high − m1_low²]`. A negative upper endpoint would rule out final float32 rounding alone for that cell. An interval containing zero permits a nonnegative true variance. The interval is conditional on round-to-nearest storage and cannot reveal earlier error in the model's squaring or time accumulation.

## Gadi results

Each row contains 69,782 wet cells and 38,218 masked cells. The fresh CMOR files matched the published `tos` and `tossq` values exactly on all four records, with identical masks, grid coordinates and time bounds. The fresh and published files store `tos` as float32 and `tossq` as float64. Variance was computed in float64 for every comparison.

| Published case | Checked interval | Native negatives | Native minimum (°C²) | Fresh and published negatives | Fresh and published minimum (°C²) |
| --- | --- | ---: | ---: | ---: | ---: |
| Monthly `piControl` | January 0101 | 5,676 | −0.01192101 | 5,761 | −0.01190007 |
| Monthly `piControl` | February 0101 | 7,391 | −0.01209314 | 7,515 | −0.01207214 |
| Daily `1pctCO2-bgc` | 1 January 0201 | 27,017 | −0.01268607 | 27,755 | −0.01302655 |
| Daily `1pctCO2-bgc` | 2 January 0201 | 27,029 | −0.01264494 | 27,702 | −0.01298495 |

All 67,113 negative native cell values had rounding intervals that included zero. The largest upward allowance from the native remainder was 0.013236 °C². The float32 offset used for `tos` shifts the converted mean by about 0.000006104 °C. The existing `tossq` floor sets a negative converted **second moment** to zero in 0, 6, 58 and 58 cells, respectively. The floor raises the variance remainder in those cells when the mean is held fixed. January 0101 reproduces the reported residual with the floor inactive everywhere. Across that record the published variance differed from the native variance by at most 0.000395 °C². The fresh output also matches the current conversion formula and output rounding exactly.

## Verified file scope

The four checked records establish this precision limitation in the following two published `tossq` files. The companion `tos` versions and tracking IDs are recorded by the diagnostic. [Exact verified file list](verified_tossq_files.txt)

| Experiment and frequency | `tossq` version | `tossq` tracking ID |
| --- | --- | --- |
| `piControl` monthly | `v20261117` | `hdl:21.14107/a61eb1ff-9b94-43e3-91fd-c11da4519d5f` |
| `1pctCO2-bgc` daily | `v20261119` | `hdl:21.14107/1821ac17-6a7c-4ae0-811f-619479588d1c` |

The audit inventory covers 14 candidate dataset versions and 643 file references. Prevalence across that inventory and across the remaining records of the two verified files is unknown.

## Follow-up for MOM and model developers

Check the data type and rounding at diagnostic evaluation, time accumulation and NetCDF output for the production MOM build. For a short replay, accumulate both moments of the same Celsius SST samples with the same weights in float64, retain sufficient precision in both archived products, and compare derived variance against a directly accumulated central moment. If within-period SST variance is needed as a diagnostic in its own right, archive the central moment directly. Reprocessing the existing Kelvin archives can preserve the recorded moments, but cannot determine the true variance of an approximately constant SST field from the rounded pair.

Derived SST standard deviation is unresolved where the propagated precision interval spans zero. Consumers working with these data can report that limitation or mask unresolved values in a derived diagnostic. Adjusting `tossq` to force a zero residual would alter the reported second moment without recovering its source precision.

## Reproduction

Run the bounded comparison from a MOPPy source checkout with its vocabulary submodules populated. It reads two consecutive records from each native and published pair, then writes fresh `tos` and `tossq` files to a private scratch directory. Each invocation uses one synchronous Dask worker. The original runs used Gadi `conda/analysis3-26.11`, MOPPy `a5d44a7361b367ee3963958be7ae1dcf92b7a73a`, and CMIP7 tables `3b7171dc7a9e8aba115e7d8c70882ca8c75039b6`.

```bash
module use /opt/Modules/modulefiles /apps/Modules/modulefiles
module load conda/analysis3-26.11
export OPENBLAS_NUM_THREADS=1 OMP_NUM_THREADS=1 MKL_NUM_THREADS=1
export PYTHONPATH="$PWD/src"
private_scratch=$(mktemp -d /scratch/PROJECT/USER/sst-moments-XXXXXX)
timeout 90s python reports/issue_863/check_sst_moments.py "$private_scratch" piControl
timeout 90s python reports/issue_863/check_sst_moments.py "$private_scratch" 1pctCO2-bgc
```

All 19 selected existing SST temperature-conversion, `tossq` derivation and mapping tests also passed in the same Gadi environment.
