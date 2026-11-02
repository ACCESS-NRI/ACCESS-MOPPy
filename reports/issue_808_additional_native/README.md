# Additional land-mean recipes: native-data validation

This evidence accompanies [#808](https://github.com/ACCESS-NRI/ACCESS-MOPPy/issues/808)
and its relationship to [#115](https://github.com/ACCESS-NRI/ACCESS-MOPPy/issues/115).
The script evaluates the 19 additional CMIP7 recipes below, outside the five
published baseline variables fixed by PR #810. It does not change production
mappings or shared helpers.

| Group | Variables |
| --- | --- |
| Carbon pools | cLand, cLeaf, cLitter, cProduct, cRoot |
| Nitrogen pools | nLand, nLitter, nMineral, nProduct, nSoil, nVeg |
| Nitrogen fluxes | fNdep, fNgas, fNleach, fNloss, fNnetmin, fNup |
| Product fluxes | fDeforestToProduct, fProductDecomp |

## Method

The input is native ACCESS-ESM1.6 historical monthly output for January 1951
through December 1953, from `output101`, `output102` and `output103`. Native tile
fractions sum to one over land, within `2e-7`; every month has 2,434 fractional
coastal cells. The reference is calculated explicitly from native fields,
independently of the production mapping helpers:

- Tile-density pools and fluxes: sum `native_value * native_tile_fraction`.
- Product pools and fluxes already weighted over land: sum native values over
  tiles without applying tile fractions again, retaining the correction in #809.
- Retain the existing field selections and unit constants.
- Mask cells where native land fraction is zero.

Candidate expressions are temporary copies of the actual recipes. Exactly one
`landfrac` helper argument is changed to `1.0`, and `mask_land` wraps the result.
The test does not divide the original output to construct its reference.

The checks compare the original recipe and the candidate against that reference:

1. For all 36 months and all 19 variables, the original land values equal
   `reference * land_fraction` and fail the intended coastal land-mean check.
   Candidates match the reference, leave pure-land values unchanged and mask
   pure ocean (`rtol=1e-12`, `atol=0`).
2. January 1951 additionally exercises both tile-axis names (`pseudo_level_0`,
   `pseudo_level`), eager and Dask arrays, and both ESM1-5/ESM1-6 expressions.
   Dask results are verified to remain lazy before computation. The source
   recipes for these 19 variables are identical in the two model mappings.
3. Both original and candidate mappings process all 12 months of 1951 through
   `ACCESS_ESM_CMORiser`, CMOR writing and CMIP7 repacking. Written coordinates,
   dimensions and land-mean metadata are checked, followed by every grid-cell
   value, coastal values, pure-land invariance and candidate ocean masking
   (`rtol=1e-6`, `atol=0`, with the cancellation-aware exception below).
4. Monthly global integrals use **land area**, `areacella * sftlf / 100`.
   `areacella` is derived from native latitude/longitude bounds with a spherical
   radius of 6,371,000 m; `sftlf / 100` is native `fld_s03i395`.

CASA nitrogen diagnostics on `time_0` are sampled from the same native monthly
file, after checking their year and month. The production CMOR path's existing
monthly time-axis normalization is exercised by the output tests; output time
coordinates must match the native calendar-month coordinates exactly.

For `fNnetmin`, positive and negative tile contributions can nearly cancel. An
initial relative-only output comparison exposed float32 rounding near zero.
The output check therefore also allows the standard summation-error bound
`gamma_(17+4) * sum(abs(tile_value * tile_fraction)) / (1000*86400)`, with
`gamma_n = n*eps / (1-n*eps)` and `eps = float32.eps`. The original recipe's
bound is additionally multiplied by land fraction. The candidate must also
match a separately calculated native-float32 reference **exactly**. This avoids
loosening relative tolerance for all values or introducing an arbitrary
absolute tolerance. Global-integral checks retain `rtol=1e-6`, `atol=0`.

## Results and provenance

Completed on 2 November 2026; the validation process exited successfully.

| Check | Result |
| --- | --- |
| Native monthly normalization | 684 variable-month comparisons: all 19 original recipes fail the land-mean requirement; all candidates pass |
| Model expression / tile-axis / backend combinations | 152 combinations pass using January 1951 native data |
| Full CMOR writing and CMIP7 repacking | 38 runs: original and candidate for each of 19 variables, each covering 12 months |
| Candidate written output | All 228 variable-months match the reference, including coasts, pure land, ocean masking and land-area integrals |
| fNnetmin cancellation | Candidate exactly matches the independent float32 reference; error against float64 is at most 0.083 of the computed rounding bound |

See [results.json](results.json) for the completed checks, the precise candidate expressions,
reference field selections, source commit, package versions, input/output file
SHA-256 hashes and individual monthly land-area integrals. NetCDF outputs are
retained locally at the recorded paths, rather than committed to Git.

## Reproduce

Use the development environment with CMOR, cmip7-repack, Dask, xarray and netCDF4.
Run against MOPPy commit `f22939099b15dcceb930e580820f1bd73d8ff808`
(the five-variable #810 fix); the 19 recipes here are unchanged from its parent
`9a428b1d4b82768a98bad374e5415339e967c21b`.

```bash
export PATH=/home/romain/PROJECTS/ACCESS-MOPPy/.pixi/envs/dev/bin:$PATH
export PYTHONPATH=/tmp/access-moppy-issue-808/src
python reports/issue_808_additional_native/validate_native.py \
  --native-root /home/romain/PROJECTS/CMIP7_Test_data/esm-historical \
  --output /tmp/moppy-808-additional-native-complete
```

Use a new output directory for another run. The process exits unsuccessfully if
any numerical, coordinate, metadata or masking assertion fails. A successful
baseline check means the known defect was reproduced, not that the original
mapping is scientifically correct.

## Limits and outstanding work

- This validates **area normalization**, while retaining current inputs and
  conversions. Native pool/flux files omit unit attributes; these tests do not
  independently establish every conversion from model source.
- **cRoot has a separate wrong-input problem:** its recipe uses leaf carbon
  `fld_s03i852`, whereas root carbon is `fld_s03i854`. The tests record the native
  leaf/root discrepancy. Passing its normalization test does not validate the
  resulting field as root carbon; the field selection still needs correction.
- These are ESM1.6 historical inputs. ESM1.5 is covered only through expression
  equality and evaluation on the same ESM1.6 inputs, not an ESM1.5 native run.
- The samples are not paired with the published piControl data. This is not
  validation of a regenerated production release.
- The 19 production mappings remain unchanged. A separate code fix and review
  are still needed; PR #810 retains its five-variable scope.
- Daily snow and the three CMIP6-only soil sub-pools are outside this 19-variable
  test run.
- Complete affected-release inventory, formal erratum registration, production
  reprocessing and validation of replacement versions remain outstanding.
