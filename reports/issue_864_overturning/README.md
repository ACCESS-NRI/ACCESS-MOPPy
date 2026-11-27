# ACCESS-ESM1-6 overturning paths

[MOPPy #864](https://github.com/ACCESS-NRI/ACCESS-MOPPy/issues/864) asks whether the published `msftm` represents meridional transport at the reported latitude. The three sampled `msftm` files contain the same native MOM row integrals as their corresponding `msfty` files. The Atlantic–Arctic rows near the tripolar fold include transport faces several degrees south of their reported latitude. The magnitude of any difference from a separately computed latitude-following streamfunction remains to be determined.

Under the November 2026 CMIP7 guidance, `g116` is an accepted parent grid for data reduced directly from this mesh. The geometry issue concerns which transport path `msftm` represents. A dedicated grid label can describe an agreed output grid once the paths and representative latitudes have been established.

## What the Data Request distinguishes

- The `msftm` definitions use geographic latitude and specify native face paths that approximate successive geographic parallels at the model's resolution. The zig-zag wording in `cell_methods` allows a path that uses native faces. The corresponding processing notes still require the paths to approximate the reported latitudes. [Depth record, v1.2.2.6](https://cmip-data-request.github.io/cmip7-dreq-webview/latest/u/baa59d48-e5dd-11e5-8482-ac72891c3257.html), [density record, v1.2.2.5](https://cmip-data-request.github.io/cmip7-dreq-webview/v1.2.2.5/u/baa5a1da-e5dd-11e5-8482-ac72891c3257.html)
- The `msfty` definitions use `gridlatitude` and sum along grid longitude. The OMIP diagnostic protocol describes both approaches and notes that fixed model rows can closely approximate latitude sections in some regions. Appendix C4 describes selecting faces to approximate a geographic section. [Depth record, v1.2.2.6](https://cmip-data-request.github.io/cmip7-dreq-webview/latest/u/baa5a662-e5dd-11e5-8482-ac72891c3257.html), [density record, v1.2.2.6](https://cmip-data-request.github.io/cmip7-dreq-webview/latest/u/baa5aafe-e5dd-11e5-8482-ac72891c3257.html), [Griffies et al. (2016), Appendix C4 and §I6](https://gmd.copernicus.org/articles/9/3231/2016/gmd-9-3231-2016.pdf)
- WCRP allows the parent grid as a transitional option for `hyb` and as a last resort for native-grid `msfty`. It recommends dedicated grids and specifies `g262` for uniform one-degree bands beginning at 89.5°S. The sampled `g116` outputs have 300 nominal latitudes from 77.753°S to 90°N, with spacing from 0.247° to 1.000° before rounding. Their coordinates therefore do not fit `g262`. [WCRP guidance, 23 November 2026 revision](https://github.com/WCRP-CMIP/cmip7-guidance/blob/e116c32cdd5acbdd54d53f5c5412025936d9446a/docs/CMIP7/Grid_Labels_for_Derived_Products.md#4-summary-table), [g116 registry entry](https://github.com/WCRP-CMIP/Essential-Model-Documentation/blob/src-data/horizontal_grid_cell/g116.json), [g262 registry entry](https://github.com/WCRP-CMIP/Essential-Model-Documentation/blob/src-data/horizontal_grid_cell/g262.json)

The WCRP revision postdates the reduced-grid work in [#827](https://github.com/ACCESS-NRI/ACCESS-MOPPy/issues/827) and [#830](https://github.com/ACCESS-NRI/ACCESS-MOPPy/pull/830). It resolves the question of whether the dimensional reduction alone requires a different label. The ocean path question raised there still applies.

## Native and published values on Gadi

At baseline `a5d44a7361b367ee3963958be7ae1dcf92b7a73a`, the depth recipes `msftmz` and `msftyz` both use `ty_trans`, `ty_trans_gm` and `ty_trans_submeso`. The density recipes `msftmrho` and `msftyrho` both use `ty_trans_rho` and `ty_trans_rho_gm`. In both pairs the code sums over tracer-cell i at fixed native j. It cumulatively sums the resolved transport over vertical levels or density bins, adds the parameterized components, and subtracts the full resolved column sum. The outputs expose the same nominal native coordinate under `lat` or `rlat`, with different standard names and cell methods. [ESM1-6 mappings](../../src/access_moppy/mappings/ACCESS-ESM1-6_mappings.json), [derivation](../../src/access_moppy/derivations/calc_ocean.py)

The reproducer reads three immutable `im55` file pairs. Each pair has identical time coordinates, data values and missing-value masks across the whole file. The independent native calculation reads the January record from `p73` at six rows and reproduces both published products exactly in every finite comparison, with identical masks.

| Experiment and vertical coordinate | Published version and period | Finite values in each published file | Finite values in each January reconstruction |
| --- | --- | ---: | ---: |
| `1pctCO2-bgc` depth | `v20261119`, 020101–020912 | 3,546,828 | 591 |
| `1pctCO2-bgc` density | `v20261119`, 020101–020912 | 6,082,560 | 1,120 |
| `piControl` density | `v20261117`, 010101–010912 | 6,082,560 | 1,120 |

The bundled `BASIN_MASK` matches the native tracer coordinates to within 0.000004° in latitude and exactly in longitude modulo 360°. The calculation uses flags 2 and 4 for Atlantic–Arctic, 3 and 5 for Indian–Pacific, and every valid transport for global. The sub-basin sums exclude the Southern Ocean and the marginal seas that have their own flags. This check establishes which cells the current implementation selects. It leaves the scientific choice of face membership at basin boundaries and straits for review.

MOM records `ty_trans` as T-cell j-mass transport, with an i-dimension at tracer centers and a j-dimension on tracer north faces. The supergrid's tracer centers and northeast corners agree with those archived for both experiments to within 0.000004° latitude. For each selected row, the table uses supergrid north-face midpoints at `y[2*j+2, 2*i+1]` and counts faces with finite resolved transport on at least one level, after applying the Atlantic–Arctic flags. Here one degree provides a reference scale for the comparison.

| Zero-based j | Nominal N | Geographic N of Atlantic–Arctic face midpoints | Wet faces more than 1° from nominal |
| ---: | ---: | ---: | ---: |
| 200 | 33.988° | 33.988° | 0 / 71 |
| 240 | 63.155° | 63.155° | 0 / 64 |
| 250 | 67.750° | 66.667–67.749° | 3 / 70 |
| 270 | 76.884° | 69.906–76.884° | 116 / 194 |
| 285 | 83.684° | 72.274–83.683° | 149 / 208 |
| 299 | 90.000° | 72.513–89.889° | 242 / 262 |

The two run manifests and sampled ocean directories contain native transport components and horizontal `psiv`. Its header identifies it as a quasi-barotropic streamfunction with dimensions `time, yu_ocean, xt_ocean`. A native basin-by-level reference could not be identified in those outputs. The sampled forced run has monthly i and j transports for the depth calculation. In both sampled directories, the density i-transport files are annual while the corresponding j-transport files are monthly. A latitude-following monthly density product would need matching i transports or an approved method to derive them. Exact agreement with the offline reconstruction verifies provenance and the path taken. The scientific comparison calls for an independent latitude-following transport.

## Decisions for the MOM and CMIP7 teams

A scientifically defensible replacement requires agreement on the target paths and a reference transport calculation.

1. Confirm whether the Data Request should be fulfilled by computing `msftm` on latitude-following paths or by publishing the existing native-row diagnostic as `msfty` and treating the separate `msftm` request as unfulfilled. For the geographic product, agree the target latitudes and a conservative face-selection and orientation method for native i and j transports, including the northern fold.
2. Confirm the Atlantic–Arctic and Indian–Pacific boundary convention and wet-face mask, including marginal seas, straits, and the Southern Ocean. Choose a transport reference for both depth and density outputs. Check which resolved and parameterized components are available on both native face directions at matching frequencies, and agree on the vertical integration sign and treatment of the resolved column transport.
3. Validate the approved paths against the monthly native fields and compare with the archived row values at a low-latitude control and the high-latitude sections above. Check transport closure across the fold where applicable, confirm the wet masks and depth or density bounds, and use the resulting geometry and output coordinates in any EMD registration. For `msfty`, the current native row calculation and grid latitude are the comparison baseline.
4. Scope any correction using the confirmed geometry and archive versions. The 27 November inventory count of 28 dataset versions and 994 file references covers both `msftm` and `msfty`; it defines the investigation set. The numerical comparison here covers six files from two experiments and supports reviewing the corresponding `msftm` records first.

## Reproduce

From the MOPPy repository root on Gadi, with read access to `p73`, `im55` and `xp65`, run the probe with the bundled basin mask and standard supergrid. The command writes its JSON to standard output. Paths, sample times, coordinates, comparisons, and input mask and supergrid hashes are retained in [validation.json](validation.json).

```bash
module use /opt/Modules/modulefiles /apps/Modules/modulefiles
module load conda/analysis3-26.11
export OMP_NUM_THREADS=1 OPENBLAS_NUM_THREADS=1 MKL_NUM_THREADS=1 NUMEXPR_NUM_THREADS=1
timeout 55 python reports/issue_864_overturning/validate_native.py
```
