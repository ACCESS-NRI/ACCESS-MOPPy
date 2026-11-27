# Investigate field capacity: are metres of water labelled as kg/m²?

## What is going on?

The published soil-water capacity tops out at only 1.20 kg/m² for a 4.6 m soil column. That looks more like a depth of water in metres than a mass per area, suggesting a possible missing water-density conversion. This is not confirmed yet.

For example: Taken literally, the biggest capacity would be only about 1.2 mm of water across the entire soil column. A drought metric dividing soil water by this capacity could be wildly inflated if the suspected ×1000 mistake is real.

## What the audit actually found

All 138 fixed-file headers were inspected; numerical samples across the 15 r1 experiments have the same suspicious range, 0–1.199801 kg m⁻² for a 4.6 m soil column. A missing ×1000 water-density conversion is plausible. However, the bundled resource already has the low values and the kg m⁻² label, so the native generation step has not been independently reconstructed. Do not apply a factor from plausibility alone.

**Proposed severity:** Potentially Critical. **Status:** PROVISIONAL — do not submit as a confirmed erratum.

The 27 November 2026 im55 inventory identifies **138 current dataset versions and 138 file references** for this item. These are candidate files to investigate, not a confirmed withdrawal list.

Example immutable file:
```
/g/data/im55/publications/MIP-DRS7/CMIP7/C4MIP/ACCESS-Consortium/ACCESS-ESM1-6/1pctCO2-bgc/r1i1p1f1/glb/fx/mrsofc/ti-u-hxy-lnd/g115/v20261008/mrsofc_ti-u-hxy-lnd_fx_glb_g115_ACCESS-ESM1-6_1pctCO2-bgc_r1i1p1f1.nc
```

## Who needs to act?

Resource/CABLE development: recover the restart field-capacity diagnostic and generation recipe from PR #246, then reconstruct the full-column quantity with explicit units. MOPPy must not insert ×1000 just because it seems plausible. Publication action stays provisional until that comparison is done.

## Proposed correction and acceptance check

Recover the restart field-capacity diagnostic and the resource-generation recipe referenced in PR #246. Recompute the six-layer integral with explicit volumetric fraction, layer thickness and water density; compare masks and cell-by-cell values before assigning a confirmed severity.

## Errata draft and exact file list

A formal draft and a complete newline-delimited list of the 138 candidate files have been prepared. See [`candidate-files.txt`](candidate-files.txt), [`datasets.json`](datasets.json), and [`erratum-draft.json`](erratum-draft.json). File-list SHA-256: `8ded1b12e83ce8b4f518ee412b96734560aabcb1427400aaf1c6f3dd17602d5a`.

The `Need Errata` label tracks the requested investigation; it does not declare this suspected problem confirmed. Do not retract or rescale on this issue alone.

Related work: #246.

Audit tracking ID: **P01**. No published data have been modified.

Tracking issue: [862](https://github.com/ACCESS-NRI/ACCESS-MOPPy/issues/862).
