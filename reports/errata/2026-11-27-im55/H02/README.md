# Erratum: published soil temperatures still have the wrong deepest depth

The deepest soil layer is 2.872 m thick, but that thickness was accidentally used as its centre depth. The real centre is 3.164 m, and layer bounds are missing from the old publication.

For example: A user asking for temperature around 3 m can interpolate against the wrong location, or miscalculate soil heat storage. The temperature numbers themselves do not need to be changed for this coordinate repair.

**Recommended seriousness:** High. **Status:** DRAFT — not submitted to ESGF.

## Evidence and scope

Every first-file header across all 138 datasets uses 2.872 m, which is the sixth layer thickness, as its centre. Correct bounds are 1.728–4.6 m; all six depth bounds are absent. The error is 0.292 m in the deepest coordinate.

Complete file list within the identified release versions; numerical confirmation was sampled, not a scan of every value.

Inventory scope: **138 dataset versions / 2,261 file references**. [Exact list](affected-files.txt) · [Dataset/version inventory](datasets.json) · [Machine-readable draft](erratum-draft.json). List SHA-256: `d956937ae67fa6b47929476c2ec7d0498a630d6048a8bdc920cd8bb527c9c459`.

## Who needs to act?

PR #851 is merged on main, but the fix postdates the latest checked release MOPPy 1.9.1. Release a producer containing commit 1d99b4f5a029a378398e9e74160e383db2103bfd, then regenerate the 138 listed tsl releases. No duplicate implementation or model-code change is needed.

## Correction and acceptance

Use the shared CABLE centres [.011,.051,.157,.4385,1.1855,3.164] m and native interfaces, with depth bounds. Preserve temperature values and masks, and verify coordinate alignment with layered mrsol.

## Gadi validation

Fresh CMIP7 and CMIP6 Gadi outputs preserve every January 0101 native and published temperature and mask across all six layers (65,190 valid temperatures each), write the corrected centres/bounds and match layered mrsol geometry. All 40 focused tests passed.

[Implementation or validation PR](https://github.com/ACCESS-NRI/ACCESS-MOPPy/pull/873) at `15ab976aed9af54713ba732394a0898098daebed`.

Tracking: [850](https://github.com/ACCESS-NRI/ACCESS-MOPPy/issues/850) · Audit ID **H02**.

No published data or ESGF records were changed. Replacement versions and ESGF erratum identifiers remain unset.
