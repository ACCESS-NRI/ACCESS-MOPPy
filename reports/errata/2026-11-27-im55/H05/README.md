# Erratum: small negative rain and snowfall values remain in published files

## What is going on?

Some physically non-negative precipitation fields contain small negative values. The recent prc/prsn fix stops this in new output, but it does not change files that are already published.

For example: The largest negative in the audit sample is about −0.00000355 mm/day. That is tiny for a water budget, but a log transform or strict non-negative QC can still fail. This sample does not bound the worst value in the whole archive.

## What the audit actually found

Negative values occur in 18/30 sampled pr files, 20/20 prc and 24/32 prsn. Minimum values are −1.2132×10⁻²³, −4.1111×10⁻¹¹ and −1.1749×10⁻²¹ kg m⁻² s⁻¹ respectively. The largest absolute sampled negative is about 3.55×10⁻⁶ mm/day. These are sample minima, not bounds on the entire archive.

**Proposed severity:** High. **Status:** DRAFT — not submitted to ESGF.

The 27 November 2026 im55 inventory identifies **62 current dataset versions and 3,123 file references** for this item. This is the full release extent, not a claim that every value in every file was checked.

Example immutable file:
```
/g/data/im55/publications/MIP-DRS7/CMIP7/C4MIP/ACCESS-Consortium/ACCESS-ESM1-6/1pctCO2-bgc/r1i1p1f1/glb/day/pr/tavg-u-hxy-u/g115/v20261019/pr_tavg-u-hxy-u_day_glb_g115_ACCESS-ESM1-6_1pctCO2-bgc_r1i1p1f1_02010101-02041231.nc
```

## Who needs to act?

MOPPy development: #855 already fixes final prc/prsn results; verify masks and valid positives. Investigate pr separately before extending the clamp. Atmosphere/UM developers should receive a source diagnostic only if native evidence establishes an upstream problem beyond small numerical residue. Publication team must quantify complete-record incidence before setting the replacement scope.

## Proposed correction and acceptance check

Follow #852 for prc/prsn: clamp only the final physically non-negative result, preserve masks and valid values, and measure full-record negative counts and mass correction. Investigate pr separately before extending the fix. Never clamp generally signed fluxes.

## Errata draft and exact file list

A formal draft and a complete newline-delimited list of the 3,123 affected-release files have been prepared. See [`affected-files.txt`](affected-files.txt), [`datasets.json`](datasets.json), and [`erratum-draft.json`](erratum-draft.json). File-list SHA-256: `97e4359b2aca9780cf05303d6f2972d2656cd4377a078bfb6f53c4449d6fa9a1`.

Keep this issue open for publication remediation even when the code fix merges. Corrected public data need a new version and an erratum; do not overwrite existing versions.

Related work: #852, #855.

Audit tracking ID: **H05**. No published data have been modified.

Tracking issue: [861](https://github.com/ACCESS-NRI/ACCESS-MOPPy/issues/861).
