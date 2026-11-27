# Erratum: older sea-ice snow thickness still blows up where almost no ice remains

## What is going on?

The old recipe divides snow mass by tiny positive ice concentrations. Near-zero numerical leftovers become huge snow thicknesses. It also uses snow density 317 instead of the model's 330 kg/m³ and turns missing concentration into zero snow.

For example: A sampled historical field contains 12,519.95 m of snow on sea ice. This is a tiny-denominator artefact, not a kilometre-high snowpack, and it can dominate an average.

## What the audit actually found

The piControl January 0101 field reaches 203.27 m; its ten cells above 10 m have ice concentrations of only 3.27×10⁻⁹–2.70×10⁻⁷ percent. Missing ice over 38,218 land cells becomes unmasked zero snow. A historical r6 sample reaches 12,519.95 m. Separately, density 317 kg m⁻³ was used instead of the CICE5 value 330, making ordinary positive thickness 4.10% high.

**Proposed severity:** Critical. **Status:** DRAFT — not submitted to ESGF.

The 27 November 2026 im55 inventory identifies **48 current dataset versions and 1,081 file references** for this item. This is the full release extent, not a claim that every value in every file was checked.

Example immutable file:
```
/g/data/im55/publications/MIP-DRS7/CMIP7/C4MIP/ACCESS-Consortium/ACCESS-ESM1-6/1pctCO2-bgc/r1i1p1f1/glb/mon/snd/tavg-u-hxy-sn/g118/v20261008/snd_tavg-u-hxy-sn_mon_glb_g118_ACCESS-ESM1-6_1pctCO2-bgc_r1i1p1f1_020101-020912.nc
```

## Who needs to act?

MOPPy development: fixes #812/#813 already exist; verify their output and preserve masks. Publication team: replace the 48 remaining old releases tracked by #835. CICE density 330 is documented; do not open a model bug for it.

## Proposed correction and acceptance check

Use the fixes in #812/#813: model snow density 330 kg m⁻³, the approved 0.01% concentration threshold, and propagation of missing inputs. Regenerate affected versions and validate masks as well as extrema. Multiplying by 317/330 alone is insufficient.

## Errata draft and exact file list

A formal draft and a complete newline-delimited list of the 1,081 affected-release files have been prepared. See [`affected-files.txt`](affected-files.txt), [`datasets.json`](datasets.json), and [`erratum-draft.json`](erratum-draft.json). File-list SHA-256: `5ba911c299ad533e5ef26db8fa7111d27c09aa6353b56ecdbb493f5f02230c31`.

Keep this issue open for publication remediation even when the code fix merges. Corrected public data need a new version and an erratum; do not overwrite existing versions.

Related work: #732, #812, #813, #835.

Audit tracking ID: **C03**. No published data have been modified.

Tracking issue: [835](https://github.com/ACCESS-NRI/ACCESS-MOPPy/issues/835).
