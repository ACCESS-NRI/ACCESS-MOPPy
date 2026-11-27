# Erratum: some monthly topsoil moisture still includes too much soil

## What is going on?

The file says water in the top 10 cm, but the older calculation takes too much of the third soil layer. The code was fixed, but older monthly publications were not all replaced.

For example: In January 0101 piControl, affected land cells contain 39–86% more water than the correctly integrated top 10 cm. A user comparing daily and monthly topsoil could mistake processing differences for climate behaviour.

## What the audit actually found

Native CABLE interfaces are 0, .022, .080, .234, .643, 1.728 and 4.6 m. Correct topsoil mass is layer1 + layer2 + layer3 × .020/.154. The old recipe uses (.1−.051)/(.157−.051), including 46.2% of layer 3 instead of 13.0%. January 0101 piControl matches that old recipe within 1.31×10⁻⁶ kg m⁻². Its published/correct ratio is 1.391–1.861 over 10,865 land cells; the unweighted mean ratio is 1.536. Another 16,975 missing ocean cells become zero. All 58 first-file headers retain six orphan depth indices labelled cm.

**Proposed severity:** Critical. **Status:** DRAFT — not submitted to ESGF.

The 27 November 2026 im55 inventory identifies **58 current dataset versions and 1,141 file references** for this item. This is the full release extent, not a claim that every value in every file was checked.

Example immutable file:
```
/g/data/im55/publications/MIP-DRS7/CMIP7/C4MIP/ACCESS-Consortium/ACCESS-ESM1-6/1pctCO2-bgc/r1i1p1f1/glb/mon/mrsol/tavg-d10cm-hxy-lnd/g115/v20261008/mrsol_tavg-d10cm-hxy-lnd_mon_glb_g115_ACCESS-ESM1-6_1pctCO2-bgc_r1i1p1f1_020101-020912.nc
```

## Who needs to act?

MOPPy development: #817 already fixed the geometry; verify it rather than write a second implementation. Publication team: regenerate the 58 older monthly releases. CABLE geometry is established; no model-code bug is inferred.

## Proposed correction and acceptance check

Regenerate the 58 affected monthly datasets using the corrected native-interface integration. Write scalar depth 5 cm with 0–10 cm bounds and preserve missing columns. A constant scale factor cannot repair spatially varying soil profiles.

## Errata draft and exact file list

A formal draft and a complete newline-delimited list of the 1,141 affected-release files have been prepared. See [`affected-files.txt`](affected-files.txt), [`datasets.json`](datasets.json), and [`erratum-draft.json`](erratum-draft.json). File-list SHA-256: `6df87aaaf1a9a7d2d6f645a2b4f76a47d6a39add77bd74e369804694c0222c5c`.

Keep this issue open for publication remediation even when the code fix merges. Corrected public data need a new version and an erratum; do not overwrite existing versions.

Related work: #817.

Audit tracking ID: **C02**. No published data have been modified.

Tracking issue: [859](https://github.com/ACCESS-NRI/ACCESS-MOPPy/issues/859).
