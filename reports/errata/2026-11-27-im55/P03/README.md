# Investigate whether overturning follows native rows or true latitude

## What is going on?

On a tripolar ocean mesh, a grid row can bend away from a geographic parallel. Summing transports along that row and labelling it as a true-latitude overturning section may describe the wrong geometry. The current reduced output also keeps the source grid label.

For example: An Arctic row can wander across latitudes. A transport assigned to a single latitude may then not be the transport across that parallel, even if the sum itself was computed correctly.

## What the audit actually found

The 28 msftm/msfty datasets retain g116 on reduced basin/latitude output. #827 identifies an unresolved distinction between summing native tripolar j rows and integrating geographic parallels. This audit confirms the reduced dimensions and labels; it does not independently resolve the Arctic transport geometry or validate basin masks.

**Proposed severity:** Potentially High for geometry; Medium for grid metadata. **Status:** PROVISIONAL — do not submit as a confirmed erratum.

The 27 November 2026 im55 inventory identifies **28 current dataset versions and 994 file references** for this item. These are candidate files to investigate, not a confirmed withdrawal list.

Example immutable file:
```
/g/data/im55/publications/MIP-DRS7/CMIP7/C4MIP/ACCESS-Consortium/ACCESS-ESM1-6/1pctCO2-bgc/r1i1p1f1/glb/mon/msftm/tavg-ol-hyb-sea/g116/v20261119/msftm_tavg-ol-hyb-sea_mon_glb_g116_ACCESS-ESM1-6_1pctCO2-bgc_r1i1p1f1_020101-020912.nc
```

## Who needs to act?

MOPPy and MOM/ocean diagnostic developers: establish the native integration paths, basin masks and intended msftm versus msfty meaning before relabelling or changing calculations. #827/#830 already document the scientific blocker. Avoid a cosmetic grid-label fix that claims an unverified topology.

## Proposed correction and acceptance check

Trace the native transport paths and compare msftm with msfty before choosing a replacement grid or changing coordinates. Do not relabel native rows as true latitude merely to make metadata pass.

## Errata draft and exact file list

A formal draft and a complete newline-delimited list of the 994 candidate files have been prepared. See [`candidate-files.txt`](candidate-files.txt), [`datasets.json`](datasets.json), and [`erratum-draft.json`](erratum-draft.json). File-list SHA-256: `e462830d0edf64a79369aa8ab9cb74b95d43b35a9322b494ceb4ad68ba6e1a35`.

The `Need Errata` label tracks the requested investigation; it does not declare this suspected problem confirmed. Do not retract or rescale on this issue alone.

Related work: #827, #830.

Audit tracking ID: **P03**. No published data have been modified.

Tracking issue: [864](https://github.com/ACCESS-NRI/ACCESS-MOPPy/issues/864).
