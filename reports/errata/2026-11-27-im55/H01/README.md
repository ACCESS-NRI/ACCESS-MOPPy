# Hybrid-height bounds mix up the bottom and top of atmospheric layers

We sort the b coefficients into numerical order on their own. But a and b must describe the same physical interface together. Sorting only b pairs the bottom height term with the top terrain term.

For example: Above the highest piControl terrain, reconstructed interfaces are displaced by up to 286 m and neighbouring layers overlap by up to 571 m. A calculation that integrates cloud over layer thickness can therefore be wrong even when the cloud values themselves are fine.

**Recommended seriousness:** High. **Status:** DRAFT — not submitted to ESGF.

## Evidence and scope

All 422 first-file headers show b bounds in ascending numeric order while height-coefficient bounds follow increasing altitude. For the first layer a=[0,49.9989] m but b=[0.9942963,1]; the physically paired b vertices should be [1,0.9942963]. Reversing each b pair agrees with the ESMValCore reference within numerical precision. With z=a+b×orog, the piControl maximum orography (5,499.87 m) gives interface errors up to 285.95 m and adjacent-layer overlaps up to 570.63 m. The existing lev_bnds formula_terms explicitly pairs these arrays, so the mismatch affects the encoded physical boundaries.

Complete file list within the identified release versions; numerical confirmation was sampled, not a scan of every value.

Inventory scope: **422 dataset versions / 6,991 file references**. [Exact list](affected-files.txt) · [Dataset/version inventory](datasets.json) · [Machine-readable draft](erratum-draft.json). List SHA-256: `827f9fb5cae29964f2607c15ff51fcbfa8a2e0cbb0719cbc40fca614541b5648`.

## Who needs to act?

MOPPy: remove independent b-bound sorting and retain native a/b interface pairing. cc-plugin-wcrp 2.3.5 already accepts decreasing coefficients, so no new VAR012 checker issue is indicated. Existing unrelated CF advisory #662 remains. Publication team: replace the affected releases after physical-coordinate validation.

## Correction and acceptance

Preserve the common physical interface order of a and b. Remove independent np.sort on b_bnds, preserve lev_bnds formula_terms="a: lev_bnds b: b_bnds orog: orog", and validate reconstructed altitude continuity over ocean and high terrain. Do not sort one formula term just to satisfy a checker.

## Gadi validation

Fresh Gadi cl, cli, clw, pfull and model-level co23D preserve data, masks and coordinate centres. The sampled interface mismatch of up to 285.946 m and overlap of 570.625 m disappear; phalf remains unchanged. All 115 focused tests passed.

[Implementation or validation PR](https://github.com/ACCESS-NRI/ACCESS-MOPPy/pull/867) at `7040d684deaeddded9c6cebc6752a05fa2553ed4`.

Tracking: [857](https://github.com/ACCESS-NRI/ACCESS-MOPPy/issues/857) · Audit ID **H01**.

No published data or ESGF records were changed. Replacement versions and ESGF erratum identifiers remain unset.
