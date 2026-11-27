# Erratum: small negative rain and snowfall values remain in published files

Some physically non-negative precipitation fields contain small negative values. The recent prc/prsn fix stops this in new output, but it does not change files that are already published.

For example: The largest negative in the audit sample is about −0.00000355 mm/day. That is tiny for a water budget, but a log transform or strict non-negative QC can still fail. This sample does not bound the worst value in the whole archive.

**Recommended seriousness:** High. **Status:** DRAFT — not submitted to ESGF.

## Evidence and scope

Negative values occur in 18/30 sampled pr files, 20/20 prc and 24/32 prsn. Minimum values are −1.2132×10⁻²³, −4.1111×10⁻¹¹ and −1.1749×10⁻²¹ kg m⁻² s⁻¹ respectively. The largest absolute sampled negative is about 3.55×10⁻⁶ mm/day. These are sample minima, not bounds on the entire archive.

Complete file list within the identified release versions; numerical confirmation was sampled, not a scan of every value.

Inventory scope: **62 dataset versions / 3,123 file references**. [Exact list](affected-files.txt) · [Dataset/version inventory](datasets.json) · [Machine-readable draft](erratum-draft.json). List SHA-256: `97e4359b2aca9780cf05303d6f2972d2656cd4377a078bfb6f53c4449d6fa9a1`.

## Who needs to act?

MOPPy: extend the existing final-result nonnegative floor from #855 to ACCESS-ESM1-6 total precipitation pr. Native monthly/daily samples reproduce the published negatives, but do not establish an upstream model-arithmetic defect or a material water-budget error. No atmosphere-team code change is prescribed from these residues alone. Publication team: schedule complete-record incidence and mass-correction checks before reissuing affected versions.

## Correction and acceptance

Use the final-result floor for ESM1-6 pr and the existing prc/prsn fixes. Preserve missing values and positive sums even when one component is negative. Quantify full-record counts and total water-mass changes; the present manifest includes only datasets with observed negative samples.

## Gadi validation

Six fresh Gadi monthly/daily products reproduce the clipped final native result exactly. The pr fix clears one negative monthly cell and 59 daily cells, preserving every nonnegative value; 102 monthly and 406 daily positive prc sums with a negative component remain unchanged. Three extra writer checks preserve missing input as encoded fill. All 293 selected Gadi tests passed; 16 new pr cases failed on the baseline and passed with the fix.

[Implementation or validation PR](https://github.com/ACCESS-NRI/ACCESS-MOPPy/pull/877) at `4de42fdec3e0456e802e391f51eb7985000115f2`.

Tracking: [861](https://github.com/ACCESS-NRI/ACCESS-MOPPy/issues/861) · Audit ID **H05**.

No published data or ESGF records were changed. Replacement versions and ESGF erratum identifiers remain unset.
