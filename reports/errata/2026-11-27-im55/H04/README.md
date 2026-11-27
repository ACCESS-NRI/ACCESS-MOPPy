# Missing soil water and transpiration become zero over the ocean

Adding a column or set of tiles with no valid values currently produces zero for mrfso and land-mean tran. Zero is a real measurement, so readers cannot tell those cells were actually outside the land domain. mrso had the same problem and was recently fixed.

For example: The native piControl comparison has 10,865 valid land cells and 16,975 missing ocean cells. The published reduction keeps the land values but fills those ocean cells with zeros; an average based on the output mask then includes the ocean and is too low.

**Recommended seriousness:** High. **Status:** DRAFT — not submitted to ESGF.

## Evidence and scope

First files of all 312 affected datasets were numerically sampled and contain unmasked ocean zeros. For January 0101 piControl, native comparisons for all three variables agree on 10,865 valid land cells but lose the mask on 16,975 ocean cells. The mechanism is skip-missing summation of entirely missing soil columns or tiles.

Complete file list within the identified release versions; numerical confirmation was sampled, not a scan of every value.

Inventory scope: **312 dataset versions / 5,735 file references**. [Exact list](affected-files.txt) · [Dataset/version inventory](datasets.json) · [Machine-readable draft](erratum-draft.json). List SHA-256: `a6976d4fa639b4ed1363ca2f8b0f3f64ce4c5840979517ebf555d144ee0f366b`.

## Who needs to act?

MOPPy: extend missing-domain preservation to mrfso and ESM1-6 land-mean tran. The mrso fix in #854 already exists. Preserve genuine zeros, handle missing positive-weight tiles, and keep the different grid-cell meaning of 3-hourly tran. No native-model defect was established.

## Correction and acceptance

Preserve an all-missing column/tile mask through reductions and apply the verified land domain. Test partially missing tiles and zero tile fractions explicitly. Reproduce land values unchanged where inputs are valid.

## Gadi validation

Fresh Gadi mrfso, tran and mrso preserve every valid land value, including 2,434 coastal cells, and encode all 16,975 ocean cells as missing. The new helper mode is opt-in; 167 focused tests passed.

[Implementation or validation PR](https://github.com/ACCESS-NRI/ACCESS-MOPPy/pull/865) at `dc0d16fcb92c5cfebf48a3da4e3a969647b83f96`.

Tracking: [858](https://github.com/ACCESS-NRI/ACCESS-MOPPy/issues/858) · Audit ID **H04**.

No published data or ESGF records were changed. Replacement versions and ESGF erratum identifiers remain unset.
