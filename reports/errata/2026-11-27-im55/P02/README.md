# Investigate negative SST variance after the second-moment fix

## What is going on?

The older tossq bug was fixed, but subtracting mean SST squared from the published SST second moment can still give a negative variance. That may come from precision or cancellation in the native moments rather than a remaining formula mistake.

For example: January 0101 piControl reaches −0.0119001 °C². A standard-deviation calculation takes the square root and returns an invalid result, because a mathematical variance cannot be negative.

## What the audit actually found

The old tossq=mean(tos)² defect is not reproduced in the current monthly sample. But tossq−tos² reaches −0.0119001 °C² in January 0101 piControl, which is inconsistent with a mathematical variance. Cancellation in the converted native moments or float32 precision is a plausible explanation, not yet a demonstrated root cause. Fourteen tossq datasets share the product family; this is not a claim that all fourteen have been shown to violate the inequality.

**Proposed severity:** Unclassified pending precision analysis. **Status:** PROVISIONAL — do not submit as a confirmed erratum.

The 27 November 2026 im55 inventory identifies **14 current dataset versions and 643 file references** for this item. These are candidate files to investigate, not a confirmed withdrawal list.

Example immutable file:
```
/g/data/im55/publications/MIP-DRS7/CMIP7/C4MIP/ACCESS-Consortium/ACCESS-ESM1-6/1pctCO2-bgc/r1i1p1f1/glb/day/tossq/tavg-u-hxy-sea/g118/v20261119/tossq_tavg-u-hxy-sea_day_glb_g118_ACCESS-ESM1-6_1pctCO2-bgc_r1i1p1f1_02010101-02041231.nc
```

## Who needs to act?

MOPPy and MOM/ocean developers: compare matched native first/second moments, units, packing and precision. Identify where precision is lost before choosing a correction. Do not blindly clamp second moments or change an entire archive based on one diagnostic.

## Proposed correction and acceptance check

Compare native first and second moments at matched time support, inspect packing and precision, and quantify negative-variance cells before issuing an erratum or changing moments.

## Errata draft and exact file list

A formal draft and a complete newline-delimited list of the 643 candidate files have been prepared. See [`candidate-files.txt`](candidate-files.txt), [`datasets.json`](datasets.json), and [`erratum-draft.json`](erratum-draft.json). File-list SHA-256: `38e5ee7640b19e7ea340ced6fdc8d779abf82907adbbc43260bbc0142681d8a2`.

The `Need Errata` label tracks the requested investigation; it does not declare this suspected problem confirmed. Do not retract or rescale on this issue alone.

Related work: #769.

Audit tracking ID: **P02**. No published data have been modified.

Tracking issue: [863](https://github.com/ACCESS-NRI/ACCESS-MOPPy/issues/863).
