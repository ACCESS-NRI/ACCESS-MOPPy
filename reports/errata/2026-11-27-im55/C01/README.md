# The phosphate numbers are about a thousand times too small

## What is going on?

We convert nitrate from an amount per kilogram of seawater to an amount per cubic metre, but skip that conversion when deriving phosphate from the same nitrate. The label says mol/m³ even though the number is still on the per-kilogram scale.

For example: If converted nitrate is 16 units, the model's 16:1 ratio calls for phosphate of 1 unit. The current output is about 0.000966 instead. That can make a nutrient inventory look almost empty.

## What the audit actually found

The native nitrate input is in mol kg⁻¹. Published po4 is exactly native no3 / 16, while published no3 includes the model density factor 1,035. In January 0101 piControl, 2,665,319 valid 3-D cells match native no3 / 16 exactly; no3 / (16 × po4) is 1,034.99985–1,035.00008. Surface comparisons at the first and last months of both the first and last files pass the same factor test in all six published experiments (24 comparisons).

**Proposed severity:** Critical. **Status:** DRAFT — not submitted to ESGF.

The 27 November 2026 im55 inventory identifies **6 current dataset versions and 301 file references** for this item. This is the full release extent, not a claim that every value in every file was checked.

Example immutable file:
```
/g/data/im55/publications/MIP-DRS7/CMIP7/C4MIP/ACCESS-Consortium/ACCESS-ESM1-6/1pctCO2-bgc/r1i1p1f1/glb/mon/po4/tavg-ol-hxy-sea/g118/v20261119/po4_tavg-ol-hxy-sea_mon_glb_g118_ACCESS-ESM1-6_1pctCO2-bgc_r1i1p1f1_020101-020912.nc
```

## Who needs to act?

MOPPy development: fix the ESM1-6 phosphate mapping. Ocean/WOMBAT developers: confirm the intended density and 16:1 diagnostic convention; the native/published comparison supports ×1035. Review ESM1-5 input units separately instead of applying the same factor blindly.

## Proposed correction and acceptance check

Correct the mapping to native no3 × 1035 / 16 (or the scientifically approved density convention), preserve masks and regenerate. Check every output against published no3 / 16, including deep water. Do not alter correctly converted no3 or other tracers by the same factor.

## Errata draft and exact file list

A formal draft and a complete newline-delimited list of the 301 affected-release files have been prepared. See [`affected-files.txt`](affected-files.txt), [`datasets.json`](datasets.json), and [`erratum-draft.json`](erratum-draft.json). File-list SHA-256: `b8046c916372794c4844e13da0465e3f290e0808bce6271bd0b5ac10ab825814`.

Keep this issue open for publication remediation even when the code fix merges. Corrected public data need a new version and an erratum; do not overwrite existing versions.



Audit tracking ID: **C01**. No published data have been modified.

Tracking issue: [856](https://github.com/ACCESS-NRI/ACCESS-MOPPy/issues/856).
