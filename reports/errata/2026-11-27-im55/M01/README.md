# Reduced sea-ice grid labels are permitted by current CMIP7 guidance

These hemisphere totals and strait transports carry the native sea-ice parent-grid label g118. The current CMIP7 guidance explicitly allows this for existing production, even though dedicated reduced-output grids are recommended for future work.

For example: A hemisphere total has one value per month rather than a full map. Its hm branding and nh/sh region describe that reduction; g118 can legitimately identify the mesh used before the reduction. A reader should use those fields together.

**Recommended seriousness:** None — accepted transitional convention. **Status:** WITHDRAWN — no erratum for parent-grid labelling.

## Evidence and scope

The 23 November 2026 WCRP guidance permits parent labels for hm hemispheric means and ht sea-ice transects. Gadi checks of one header per all 16 versions confirm matching labels in attributes/DRS/filenames, hm or ht branding, nh/sh or glb regions, and the appropriate scalar or four-line dimensions. Native CICE T-grid and published full-grid siconc coordinate probes confirm the stated g118 parent in all eight experiments.

The 16-version / 1,171-file list records the reviewed inventory, not affected data or a withdrawal list. All filenames were reconciled; numerical data arrays were not read for this check.

Inventory scope: **16 dataset versions / 1,171 file references**. [Exact list](reviewed-files.txt) · [Dataset/version inventory](datasets.json) · [Machine-readable draft](erratum-draft.json). List SHA-256: `ceb2545c394f115d098c58bc1823245ffd0e92fa86996d1e2de285ef6b899a27`.

[Current CMIP7 grid guidance](https://github.com/WCRP-CMIP/cmip7-guidance/blob/e116c32cdd5acbdd54d53f5c5412025936d9446a/docs/CMIP7/Grid_Labels_for_Derived_Products.md).

## Who needs to act?

MOPPy/CMIP7 tooling: align recommendations and checker messaging with the accepted parent-label transition. No model-code correction or erratum is required for M01. The broader #827 ocean-path questions remain separate.

## Correction and acceptance

Do not retract or republish these 1,171 files solely for their parent-grid labels. Treat dedicated-grid work in PR #830 as a prospective improvement, and retain the separate ocean-path investigation in #864.

## Gadi validation

All 16 representative headers and 1,171 filenames passed. Five native-versus-published grid coordinate probes for each of eight experiments agree within stored precision. No target data arrays were read; the evidence is about representation and parent-grid provenance.

[Implementation or validation PR](https://github.com/ACCESS-NRI/ACCESS-MOPPy/pull/876) at `982bf39e649abc7f1ab5749c2c2389c3a651b3f6`.

Tracking: [827](https://github.com/ACCESS-NRI/ACCESS-MOPPy/issues/827) · Audit ID **M01**.

No published data or ESGF records were changed. Replacement versions and ESGF erratum identifiers remain unset.
