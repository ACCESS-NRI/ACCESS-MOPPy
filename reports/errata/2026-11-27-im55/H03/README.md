# Soil-layer thickness files call layer numbers depths in metres

## What is going on?

The static resource numbers its layers 1 to 6. Publication then labels those numbers as physical depth in metres, although the whole soil column is only 4.6 m deep.

For example: Matching slthick to soil temperature by depth can pair the wrong layers or find no match at all. The thickness values are sensible; it is the coordinate describing where those values belong that is wrong.

## What the audit actually found

All 138 fixed-file headers have the same false physical coordinate. The resource supplies dimensionless sdepth indices; publication relabels them as depth in metres. The sampled thickness data themselves are the correct [.022,.058,.154,.409,1.085,2.872] m.

**Proposed severity:** High. **Status:** DRAFT — not submitted to ESGF.

The 27 November 2026 im55 inventory identifies **138 current dataset versions and 138 file references** for this item. This is the full release extent, not a claim that every value in every file was checked.

Example immutable file:
```
/g/data/im55/publications/MIP-DRS7/CMIP7/C4MIP/ACCESS-Consortium/ACCESS-ESM1-6/1pctCO2-bgc/r1i1p1f1/glb/fx/slthick/ti-sl-hxy-lnd/g115/v20261008/slthick_ti-sl-hxy-lnd_fx_glb_g115_ACCESS-ESM1-6_1pctCO2-bgc_r1i1p1f1.nc
```

## Who needs to act?

MOPPy development: use the same CABLE layer centres and bounds as corrected soil-moisture/temperature products; inspect the static resource's ocean mask. CABLE developers only need to confirm provenance if the resource's geometry differs from the established constants.

## Proposed correction and acceptance check

Assign the shared native CABLE centres and bounds before publication. Check missing ocean values as well: the sampled resource-derived thickness also uses zero outside land. Preserve real layer thicknesses.

## Errata draft and exact file list

A formal draft and a complete newline-delimited list of the 138 affected-release files have been prepared. See [`affected-files.txt`](affected-files.txt), [`datasets.json`](datasets.json), and [`erratum-draft.json`](erratum-draft.json). File-list SHA-256: `7b2f366f83e1eb20391fdf93afc38db72a5fe8dada4c08fb5b61eb3c8c64b573`.

Keep this issue open for publication remediation even when the code fix merges. Corrected public data need a new version and an erratum; do not overwrite existing versions.

Related work: #817, #850.

Audit tracking ID: **H03**. No published data have been modified.

Tracking issue: [860](https://github.com/ACCESS-NRI/ACCESS-MOPPy/issues/860).
