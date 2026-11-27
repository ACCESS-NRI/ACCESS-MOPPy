# ACCESS-ESM1-6 sea-ice parent grid verification

The November 2026 im55 sea-ice products meet the CMIP7 transitional grid-label guidance. The archive can retain `g118` for these existing products. Withdraw the draft M01 erratum for this grid label. [MOPPy #827](https://github.com/ACCESS-NRI/ACCESS-MOPPy/issues/827) · [CMIP7 grid-label guidance, §§2.2, 4 and 6.2](https://github.com/WCRP-CMIP/cmip7-guidance/blob/e116c32cdd5acbdd54d53f5c5412025936d9446a/docs/CMIP7/Grid_Labels_for_Derived_Products.md#22-alternative-use-the-parent-grid-label)

CMIP7 permits existing reduced products to use the last two-dimensional reporting grid used in the calculation. The `horizontal_label` and `region` identify the reduced product. A dedicated grid is recommended and a checker can still report a medium-severity recommendation when the parent grid is used. WCRP gives `siarea` as a worked example of the parent-grid approach. [CMIP7 grid-label guidance, §§3.3, 3.5 and 6.2](https://github.com/WCRP-CMIP/cmip7-guidance/blob/e116c32cdd5acbdd54d53f5c5412025936d9446a/docs/CMIP7/Grid_Labels_for_Derived_Products.md#62-hemispheric-sea-ice-area)

MOPPy 1.9.1 maps native ACCESS-ESM1-6 CICE tracer points to `g118`. The hemisphere calculations reduce native tracer-cell ice concentration and area. The two passage calculations use the native tracer-cell scalar quantity with native corner velocities and strait edge lengths. The shared transport calculation checks both tracer and corner coordinates at the passage indices before integrating directly on the native grid. The mass recipe performs the integration on daily data before averaging the result by month. [MOPPy 1.9.1 mapping](https://github.com/ACCESS-NRI/ACCESS-MOPPy/blob/f84b99a228cdb0ed331b01a4a6a0fb237fef7a91/src/access_moppy/mappings/ACCESS-ESM1-6_mappings.json) · [MOPPy 1.9.1 sea-ice calculations](https://github.com/ACCESS-NRI/ACCESS-MOPPy/blob/f84b99a228cdb0ed331b01a4a6a0fb237fef7a91/src/access_moppy/derivations/calc_seaice.py) · [EMD `g118` registration](https://github.com/WCRP-CMIP/Essential-Model-Documentation/blob/src-data/horizontal_grid_cell/g118.json)

## Published and native evidence

The Gadi check opened the first file in each of the 16 inventoried dataset versions. The global metadata, filename and directory agreed on `g118`, the branding suffix and the region. All 16 samples reported MOPPy 1.9.1. The complete inventory check verified the existence and filename structure of all 1,171 listed files and reproduced the original sorted file-list SHA-256. [Gadi evidence](gadi-evidence.json)

| Published product | Dataset versions | File paths | Branding | Region | Target dimensions |
|---|---:|---:|---|---|---|
| `siarea`, `siextent` | 8 | 820 | `tavg-u-hm-u` | `nh`, `sh` | `time` |
| `siareaacrossline` | 2 | 205 | `tavg-u-ht-u` | `glb` | `time`, `line` |
| `simassacrossline` | 6 | 146 | `tavg-u-ht-u` | `glb` | `time`, `line` |

All eight sampled passage files referenced the `line` coordinate and contained four named passages in the producer's order (`fram_strait`, `canadian_archipelago`, `barents_opening`, `bering_strait`). For each of the eight experiments, a matching published full-grid `siconc` sample used `g118` with 300 × 360 horizontal points. The corresponding native CICE `aice` grid matched the published coordinates at five sampled grid points. The largest absolute differences were `3.26 × 10⁻⁶°` in latitude and `1.11 × 10⁻⁵°` in longitude after wrap-around normalization. [Gadi evidence](gadi-evidence.json)

The verification read one reduced-file header per dataset version, the short passage labels and sampled parent coordinates. The inventory covered filenames and file existence. Field values and the full compliance-checker suite were outside the test scope. The assessment covers the sea-ice portion of #827. The native-row ocean questions and other metadata checks have their own scope.

For future production, `g011` and `g012` describe the Northern and Southern Hemisphere reductions, and `g014` describes the passage collection. Draft PR #830 can be considered as dedicated-grid work for future output. [CMIP7 guidance summary table](https://github.com/WCRP-CMIP/cmip7-guidance/blob/e116c32cdd5acbdd54d53f5c5412025936d9446a/docs/CMIP7/Grid_Labels_for_Derived_Products.md#4-summary-table) · [MOPPy PR #830](https://github.com/ACCESS-NRI/ACCESS-MOPPy/pull/830)

## Reproduction on Gadi

Use a Gadi session with read access to `im55` and `p73`. From the repository root, run the checker with the analysis module. The recorded run used Python 3.12.13 and netCDF4 1.7.2, one thread per numerical library, a 90-second limit and 1.875 GiB virtual memory. The script emits the evidence JSON to standard output.

```sh
module use /opt/Modules/modulefiles /apps/Modules/modulefiles
module load conda/analysis3-26.11
export OPENBLAS_NUM_THREADS=1 OMP_NUM_THREADS=1 MKL_NUM_THREADS=1
(
  ulimit -v 1966080
  nice -n 10 timeout --kill-after=5s 90s python -u reports/issue_827_reduced_seaice_grid/check_gadi.py
)
```
