# Erratum: hemisphere totals and strait transports still wear a whole-grid label

## What is going on?

After reducing a sea-ice field to one hemisphere total or four strait values, the file still advertises the full tripolar grid g118. That describes the source mesh, not the output representation.

For example: A catalogue or automated reader can see a global grid label while the data contain just one number per month or four named passages. This can break discovery and grid handling without changing the actual totals.

## What the audit actually found

All 16 first-file headers retain g118/100 km, although hemispheric totals have only time and strait products have time × line. #827 identifies registered hemisphere grids g011/g012 and passage grid g014. The same passage-label issue extends to simassacrossline.

**Proposed severity:** Medium. **Status:** DRAFT — not submitted to ESGF.

The 27 November 2026 im55 inventory identifies **16 current dataset versions and 1,171 file references** for this item. This is the full release extent, not a claim that every value in every file was checked.

Example immutable file:
```
/g/data/im55/publications/MIP-DRS7/CMIP7/C4MIP/ACCESS-Consortium/ACCESS-ESM1-6/1pctCO2-bgc/r1i1p1f1/glb/mon/simassacrossline/tavg-u-ht-u/g118/v20261119/simassacrossline_tavg-u-ht-u_mon_glb_g118_ACCESS-ESM1-6_1pctCO2-bgc_r1i1p1f1_020101-020912.nc
```

## Who needs to act?

MOPPy development: existing PR #830 addresses reduced sea-ice grids; validate its coverage of simassacrossline before opening competing work. Publication team: update metadata and DRS consistently in new versions. Keep the unresolved ocean native-row geometry separate (P03).

## Proposed correction and acceptance check

Select the approved reduced grid labels consistently in metadata, filenames and DRS, then republish as new versions with a documented identifier migration. Keep the existing region and numerical values if separately validated.

## Errata draft and exact file list

A formal draft and a complete newline-delimited list of the 1,171 affected-release files have been prepared. See [`affected-files.txt`](affected-files.txt), [`datasets.json`](datasets.json), and [`erratum-draft.json`](erratum-draft.json). File-list SHA-256: `ceb2545c394f115d098c58bc1823245ffd0e92fa86996d1e2de285ef6b899a27`.

Keep this issue open for publication remediation even when the code fix merges. Corrected public data need a new version and an erratum; do not overwrite existing versions.

Related work: #827, #830.

Audit tracking ID: **M01**. No published data have been modified.

Tracking issue: [827](https://github.com/ACCESS-NRI/ACCESS-MOPPy/issues/827).
