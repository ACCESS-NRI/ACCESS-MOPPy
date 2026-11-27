# Hybrid-height bounds mix up the bottom and top of atmospheric layers

## What is going on?

We sort the b coefficients into numerical order on their own. But a and b must describe the same physical interface together. Sorting only b pairs the bottom height term with the top terrain term.

For example: Above the highest piControl terrain, reconstructed interfaces are displaced by up to 286 m and neighbouring layers overlap by up to 571 m. A calculation that integrates cloud over layer thickness can therefore be wrong even when the cloud values themselves are fine.

## What the audit actually found

All 422 first-file headers show b bounds in ascending numeric order while height-coefficient bounds follow increasing altitude. For the first layer a=[0,49.9989] m but b=[0.9942963,1]; the physically paired b vertices should be [1,0.9942963]. Reversing each b pair agrees with the ESMValCore reference within numerical precision. With z=a+b×orog, the piControl maximum orography (5,499.87 m) gives interface errors up to 285.95 m and adjacent-layer overlaps up to 570.63 m. The existing lev_bnds formula_terms explicitly pairs these arrays, so the mismatch affects the encoded physical boundaries.

**Proposed severity:** High. **Status:** DRAFT — not submitted to ESGF.

The 27 November 2026 im55 inventory identifies **422 current dataset versions and 6,991 file references** for this item. This is the full release extent, not a claim that every value in every file was checked.

Example immutable file:
```
/g/data/im55/publications/MIP-DRS7/CMIP7/C4MIP/ACCESS-Consortium/ACCESS-ESM1-6/1pctCO2-bgc/r1i1p1f1/glb/mon/cl/tavg-al-hxy-u/g115/v20261008/cl_tavg-al-hxy-u_mon_glb_g115_ACCESS-ESM1-6_1pctCO2-bgc_r1i1p1f1_020101-020912.nc
```

## Who needs to act?

MOPPy development: remove independent b-bound sorting while preserving source interface pairing and formula_terms. WCRP checker development: if VAR012 insists these decreasing coefficients have ascending bounds, report that checker assumption with a paired-coordinate example; do not distort scientific coordinates to satisfy it.

## Proposed correction and acceptance check

Preserve the common physical interface order of a and b. Remove independent np.sort on b_bnds, preserve lev_bnds formula_terms="a: lev_bnds b: b_bnds orog: orog", and validate reconstructed altitude continuity over ocean and high terrain. Do not sort one formula term just to satisfy a checker.

## Errata draft and exact file list

A formal draft and a complete newline-delimited list of the 6,991 affected-release files have been prepared. See [`affected-files.txt`](affected-files.txt), [`datasets.json`](datasets.json), and [`erratum-draft.json`](erratum-draft.json). File-list SHA-256: `827f9fb5cae29964f2607c15ff51fcbfa8a2e0cbb0719cbc40fca614541b5648`.

Keep this issue open for publication remediation even when the code fix merges. Corrected public data need a new version and an erratum; do not overwrite existing versions.



Audit tracking ID: **H01**. No published data have been modified.

Tracking issue: [857](https://github.com/ACCESS-NRI/ACCESS-MOPPy/issues/857).
