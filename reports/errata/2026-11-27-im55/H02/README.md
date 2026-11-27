# Erratum: published soil temperatures still have the wrong deepest depth

## What is going on?

The deepest soil layer is 2.872 m thick, but that thickness was accidentally used as its centre depth. The real centre is 3.164 m, and layer bounds are missing from the old publication.

For example: A user asking for temperature around 3 m can interpolate against the wrong location, or miscalculate soil heat storage. The temperature numbers themselves do not need to be changed for this coordinate repair.

## What the audit actually found

Every first-file header across all 138 datasets uses 2.872 m, which is the sixth layer thickness, as its centre. Correct bounds are 1.728–4.6 m; all six depth bounds are absent. The error is 0.292 m in the deepest coordinate.

**Proposed severity:** High. **Status:** DRAFT — not submitted to ESGF.

The 27 November 2026 im55 inventory identifies **138 current dataset versions and 2,261 file references** for this item. This is the full release extent, not a claim that every value in every file was checked.

Example immutable file:
```
/g/data/im55/publications/MIP-DRS7/CMIP7/C4MIP/ACCESS-Consortium/ACCESS-ESM1-6/1pctCO2-bgc/r1i1p1f1/glb/mon/tsl/tavg-sl-hxy-lnd/g115/v20261008/tsl_tavg-sl-hxy-lnd_mon_glb_g115_ACCESS-ESM1-6_1pctCO2-bgc_r1i1p1f1_020101-020912.nc
```

## Who needs to act?

MOPPy development: #851 is already merged for #850. Validate current code and document the release containing it. Publication team: replace all 138 current tsl releases. Do not create a duplicate source fix.

## Proposed correction and acceptance check

Use the shared CABLE centres [.011,.051,.157,.4385,1.1855,3.164] m and native interfaces, with depth bounds. Preserve temperature values and masks, and verify coordinate alignment with layered mrsol.

## Errata draft and exact file list

A formal draft and a complete newline-delimited list of the 2,261 affected-release files have been prepared. See [`affected-files.txt`](affected-files.txt), [`datasets.json`](datasets.json), and [`erratum-draft.json`](erratum-draft.json). File-list SHA-256: `d956937ae67fa6b47929476c2ec7d0498a630d6048a8bdc920cd8bb527c9c459`.

Keep this issue open for publication remediation even when the code fix merges. Corrected public data need a new version and an erratum; do not overwrite existing versions.

Related work: #850, #851.

Audit tracking ID: **H02**. No published data have been modified.

Tracking issue: [850](https://github.com/ACCESS-NRI/ACCESS-MOPPy/issues/850).
