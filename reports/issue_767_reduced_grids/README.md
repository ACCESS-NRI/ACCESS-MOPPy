# Issue #767 group 6: reduced-output grid metadata

**Five sea-ice products have verified grid-label fixes. The ocean product remains blocked by scientific geometry/registration questions.** This is a partial fix, not six passing products or a scientific validation of overturning transport.

- Producer issue: [ACCESS-MOPPy #827](https://github.com/ACCESS-NRI/ACCESS-MOPPy/issues/827).
- Generic registry/checker question: [cc-plugin-wcrp #98](https://github.com/ESGF/cc-plugin-wcrp/issues/98).
- Clean branch baseline: `99e671d978ae38a102ddbcce36cacf45c4102c41` (main); branch `codex/767-g6-reduced-grid`.
- Final fresh full-year run: `/home/romain/Documents/ChatGPT/Expand variable MOPPY/issue767-work/validation/g6/`.
- Fresh clean-main comparison run: sibling `validation/baseline/`. Both use retained native piControl year-0101 input and the verified official `/tmp/mom1deg.nc` supergrid.
- Original dirty checkout, original 113-output reports and all native files were preserved. Earlier uncommitted sea-ice edits were not bundled into this branch.

## Root cause and change

Sea-ice grid selection used the inferred source T/U mesh even after reduction. The hemispheric time series no longer have spatial dimensions, and line transport has a dedicated line dimension. Configuration now maps `hm_nh` to `g011`, `hm_sh` to `g012`, and line reductions to `g014`. Hemispheric selection requires an actual reduced output and the existing `hm` horizontal label. Regions and calculations are unchanged; the same resolved label reaches the global attribute, filename and DRS directory. Explicit caller labels and full-grid defaults retain precedence/behavior.

The pinned Universe 3.4.3 records establish that `g011`/`g012` are one-cell Northern/Southern Hemisphere representations; `g014` represents discrete ocean passages and uses a global catalog region, not a globe-spanning physical passage. They also agree with current upstream registry records. `evidence.json` retains resolved registry records and their database hash.

## Fresh validation: 3 November 2026

Environment: Python 3.13.15, cc-plugin-wcrp 2.4.0b2, compliance-checker 6.1.0, ESGVOC 7.2.0, Universe 3.4.3, CMIP7 2.5.4. The checker ran all checks with strict criteria on each newly generated output. No checker configuration, vocabulary database or output NetCDF was patched.

| Product | Final grid / region | Processing, structure, range | Grid findings | Other checker findings |
|---|---|---|---|---|
| SImon.siarean | g011 / nh | ✅ PASS | ✅ PASS | ⚠️ Optional comment |
| SImon.siareas | g012 / sh | ✅ PASS | ✅ PASS | ⚠️ Optional comment |
| SImon.siextentn | g011 / nh | ✅ PASS | ✅ PASS | ⚠️ Optional comment |
| SImon.siextents | g012 / sh | ✅ PASS | ✅ PASS | ⚠️ Optional comment |
| SImon.siareaacrossline | g014 / glb | ✅ PASS | ✅ PASS | ❌ Stock checker requires sector; ⚠️ optional comment |
| Omon.msftmrho | g116 / glb (unchanged, unresolved) | ✅ PASS | ⚠️ Wrong source topology and 108000-versus-300 count | ❌ Stock checker requires sector; ⚠️ optional comment |

Measured changes: nine sea-ice medium check records clear (four region mismatches, four hemisphere-label recommendations and one passage-label recommendation). The two `msftmrho` grid findings remain. Stock checker totals: two high findings on two outputs, two medium findings on one output, six optional findings. These files are not strict passes. Text-coordinate compatibility is tracked separately in [MOPPy #821/#824](https://github.com/ACCESS-NRI/ACCESS-MOPPy/pull/824) and [cc-plugin-wcrp #97](https://github.com/ESGF/cc-plugin-wcrp/pull/97).

All target data, coordinate and bounds arrays retain identical dimensions, dtypes, values and masks relative to the fresh clean-main run. The target-variable comparison covers 675,840 valid `msftm` values, 46 valid strait values and 48 hemispheric values; maximum absolute difference is zero. Units are unchanged. The all-variable audit separately checks every stored label, coordinate and bounds array and verifies DRS/filename/region agreement. `comparison.json` contains fresh checker findings and numerical results; `evidence.json` contains all-variable identity checks and output SHA256 hashes. Six full-year files contain 12 monthly records each.

The focused ocean, sea-ice, driver and line-transport suite passes **272 tests**. Coverage includes all five target reductions, explicit label preservation, unchanged full-grid selection, and on-disk line output including the shared mass-transport path. The first broader run omitted the dev interpreter's `bin` directory from PATH, causing four missing-`cmip7repack` environment failures; those logs were retained separately, PATH was corrected, and the full focused suite was rerun. Tests and output generation are fresh, not cached baseline evidence.

## Ocean blocker: native rows are not geographic latitude circles

`ty_trans_rho` identifies itself as `T-cell j-mass transport on pot_rho`. The current streamfunction calculation sums along the x dimension at fixed native j. The model nominal `grid_yu_ocean` coordinate is then emitted as `lat` for `msftmrho`. The official tripolar supergrid proves that this is not enough to establish geographic zonal geometry:

| Native row (zero based) | Nominal latitude | Geographic range of the northern T-cell faces |
|---|---|---|
| 240 | 63.1545 N | 63.1545–63.1545 N |
| 270 | 76.8843 N | 65.1215–76.8838 N |
| 299 | 90 N | 65.2105–89.8892 N |

The exact input attributes, file hashes, supergrid sampling and additional rows are retained in `native_row_geometry.json`. The `hyb` horizontal label broadly permits model y, whereas the registered `g262` grid type describes basin cells at each latitude. A scientifically accurate reduced native-row grid registration/representation is required before changing the label; alternatively, a true geographic-latitude product would need a justified transport calculation. Neither the variable branding nor the transport calculation is altered here. Existing source-grid `g116` metadata remain a known defect.

A candidate `g262` regeneration was investigated, then rejected as an unproven geometry change. Its outputs/reports remain separately under `validation/g6-candidate-rejected/`; they are **not** evidence for the final branch. That generic registry entry also has no fixed cell count, and the beta checker lacks a `basin_by_latitude` topology rule. Issue #98 records those independent questions; fixing the checker does not establish the correct interpretation of the ACCESS native-row product.

## Reproduction

Use the original retained selection/provenance/input artifacts and the shared `issue767-work/run_fresh.py` runner, with `PYTHONPATH` pointing to this worktree's `src`, the dev environment `bin` directory on PATH, and `ISSUE767_RUN_ROOT` pointing to a **new empty directory**. Invoke it once for each of the six exact products in `comparison.json`. It calls the full producer followed by the pinned strict checker and refuses to overwrite existing NetCDF outputs. Raw results, processing logs, checker JSON and checker logs are under the final run root above.

Run the all-variable/DRS audit with the dev Python:

```bash
python reports/issue_767_reduced_grids/audit.py BASELINE_RUN NEW_RUN UNIVERSE_DB
python -m pytest tests/unit/test_ocean.py tests/unit/test_sea_ice.py tests/unit/test_driver.py tests/unit/test_seaice_line_transport.py -q
```

`UNIVERSE_DB` is the retained read-only `reports/issue_767_rerun/checker_2_4/esgvoc/dbs/universe/3.4.3.db` in the original checkout. Do not regenerate into the retained baseline directories.
