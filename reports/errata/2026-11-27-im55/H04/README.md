# Missing soil water and transpiration become zero over the ocean

## What is going on?

Adding a column or set of tiles with no valid values currently produces zero for mrfso and land-mean tran. Zero is a real measurement, so readers cannot tell those cells were actually outside the land domain. mrso had the same problem and was recently fixed.

For example: The native piControl comparison has 10,865 valid land cells and 16,975 missing ocean cells. The published reduction keeps the land values but fills those ocean cells with zeros; an average based on the output mask then includes the ocean and is too low.

## What the audit actually found

First files of all 312 affected datasets were numerically sampled and contain unmasked ocean zeros. For January 0101 piControl, native comparisons for all three variables agree on 10,865 valid land cells but lose the mask on 16,975 ocean cells. The mechanism is skip-missing summation of entirely missing soil columns or tiles.

**Proposed severity:** High. **Status:** DRAFT — not submitted to ESGF.

The 27 November 2026 im55 inventory identifies **312 current dataset versions and 5,735 file references** for this item. This is the full release extent, not a claim that every value in every file was checked.

Example immutable file:
```
/g/data/im55/publications/MIP-DRS7/CMIP7/C4MIP/ACCESS-Consortium/ACCESS-ESM1-6/1pctCO2-bgc/r1i1p1f1/glb/day/mrso/tavg-u-hxy-lnd/g115/v20261119/mrso_tavg-u-hxy-lnd_day_glb_g115_ACCESS-ESM1-6_1pctCO2-bgc_r1i1p1f1_02010101-02041231.nc
```

## Who needs to act?

MOPPy development: extend the domain-preserving treatment from merged #854 to mrfso and land-mean tran, including partial missing and zero-weight tiles. Preserve legitimate zero values and the different grid-cell meaning of 3hr tran. No native-model error has been established.

## Proposed correction and acceptance check

Preserve an all-missing column/tile mask through reductions and apply the verified land domain. Test partially missing tiles and zero tile fractions explicitly. Reproduce land values unchanged where inputs are valid.

## Errata draft and exact file list

A formal draft and a complete newline-delimited list of the 5,735 affected-release files have been prepared. See [`affected-files.txt`](affected-files.txt), [`datasets.json`](datasets.json), and [`erratum-draft.json`](erratum-draft.json). File-list SHA-256: `a6976d4fa639b4ed1363ca2f8b0f3f64ce4c5840979517ebf555d144ee0f366b`.

Keep this issue open for publication remediation even when the code fix merges. Corrected public data need a new version and an erratum; do not overwrite existing versions.

Related work: #853, #854.

Audit tracking ID: **H04**. No published data have been modified.

Tracking issue: [858](https://github.com/ACCESS-NRI/ACCESS-MOPPy/issues/858).
