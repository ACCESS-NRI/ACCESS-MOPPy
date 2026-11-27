# Critical: soil field capacity uses wilting point and misses water-mass conversion

The capacity file uses wilting point, when plants already struggle to extract water, instead of field capacity, the water left after excess has drained away. It also omits the conversion from water depth to water mass.

For example: The old maximum says the whole 4.6 m soil column holds only about 1.2 mm of water at field capacity. The corrected maximum is about 1,766 mm. A drought ratio using the old capacity as its denominator would be greatly inflated. Multiplying by 1,000 alone still leaves the wrong soil property.

**Recommended seriousness:** Critical. **Status:** DRAFT — confirmed; not submitted to ESGF.

## Evidence and scope

All 138 published fixed files match the old resource. It equals native UM STASH 40 (wilting fraction) ×4.6 m on all 10,865 land cells. The deployed UM7/CABLE source establishes STASH 41 as field capacity, copied through the six layers. Integrating it through those layers with water density 1,000 kg/m3 produces corrected/old ratios of 1,388–2,906 on 7,431 positive cells. Native permanent-ice zeros and ocean masks are preserved.

All 138 listed fixed files were compared numerically over their full grids and confirmed to contain the old erroneous resource.

Inventory scope: **138 dataset versions / 138 file references**. [Exact list](affected-files.txt) · [Dataset/version inventory](datasets.json) · [Machine-readable draft](erratum-draft.json). List SHA-256: `8ded1b12e83ce8b4f518ee412b96734560aabcb1427400aaf1c6f3dd17602d5a`.

## Who needs to act?

MOPPy/resource developers: review PR #871. No model-physics change is indicated. Publication team: replace the 138 audited ESM1-6 files. ESM1-5 maintainers should investigate the shared legacy resource separately; its mapping and publication scope are unchanged by this fix.

## Correction and acceptance

Use the reproducibly generated ESM1-6-specific STASH 41 resource, validate the complete fixed field and publish replacements for all 138 listed files. Preserve valid ice zeros and the native mask; do not add lake-reservoir water or unverified tile weights.

## Gadi validation

Fresh Gadi fixed-field output agrees with an independent native six-layer reconstruction within 4.55e-13 kg/m2. An independent reviewer repeated the native comparison and checked deployed model-source conventions. Seven targeted local tests and the Gadi resource/mapping test passed.

[Implementation or validation PR](https://github.com/ACCESS-NRI/ACCESS-MOPPy/pull/871) at `036c5a285b64e15a3ddf1dee0382986cebe997fa`.

Tracking: [862](https://github.com/ACCESS-NRI/ACCESS-MOPPy/issues/862) · Audit ID **P01**.

No published data or ESGF records were changed. Replacement versions and ESGF erratum identifiers remain unset.
