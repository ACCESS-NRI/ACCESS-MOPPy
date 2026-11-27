# Overturning uses native rows where geographic latitude paths need review

A tripolar ocean grid row bends in the Arctic. The sampled msftm files contain the same native-row sums as msfty, although msftm is described as following approximate geographic latitudes. The numerical difference from an agreed latitude-following calculation is still unknown.

For example: The Atlantic–Arctic row labelled 76.884°N includes wet-face midpoints from 69.906°N to 76.884°N; 116 of 194 are more than one degree from that nominal latitude. A user interpreting it as a section near that parallel could assign the transport to the wrong path.

**Recommended seriousness:** Potentially High for transport geometry; no parent-grid-label erratum. **Status:** PROVISIONAL — do not submit as a confirmed erratum.

## Evidence and scope

Three complete msftm/msfty file pairs have identical values, times and masks. Independent January reconstruction from archived MOM j-face transports matches both on six sampled rows. Supergrid face coordinates establish the Arctic departures. This checks six published files from two experiments, not all 28 candidate versions. Current CMIP7 guidance permits parent g116; g262 does not describe these nonuniform 300 native rows.

Investigation scope: 28 versions / 994 files. Detailed comparisons cover three msftm/msfty pairs (six files); the candidate list is not a verified withdrawal list.

Inventory scope: **28 dataset versions / 994 file references**. [Exact list](candidate-files.txt) · [Dataset/version inventory](datasets.json) · [Machine-readable draft](erratum-draft.json). List SHA-256: `e462830d0edf64a79369aa8ab9cb74b95d43b35a9322b494ceb4ad68ba6e1a35`.

[Six comparison files](comparison-files.txt); msfty is included as a native-row comparator.

[Current CMIP7 grid guidance](https://github.com/WCRP-CMIP/cmip7-guidance/blob/e116c32cdd5acbdd54d53f5c5412025936d9446a/docs/CMIP7/Grid_Labels_for_Derived_Products.md).

## Who needs to act?

MOM/ocean and CMIP7 diagnostic teams: settle the scientific path/reference and cadence requirements. MOPPy: implement the approved derivation only after those decisions. Publication team: identify confirmed msftm versions after that comparison; the 28-version inventory also contains msfty and is investigation scope.

## Correction and acceptance

Agree target latitude paths, conservative i/j face selection/orientation, basin masks, components and reference transport before implementing a replacement. Check matching input cadence: sampled density i transports are annual, whereas j transports are monthly. Do not substitute g262 or relabel coordinates to imply an uncomputed geographic section.

## Gadi validation

All values in three complete published file pairs are identical. Independent native reconstruction is exact on six January rows for the depth and two density cases. The sample establishes path provenance, not error magnitude versus an approved alternative.

[Implementation or validation PR](https://github.com/ACCESS-NRI/ACCESS-MOPPy/pull/875) at `e9dc5baa9b94b922a4a06b5e684a8c4397459eab`.

Tracking: [864](https://github.com/ACCESS-NRI/ACCESS-MOPPy/issues/864) · Audit ID **P03**.

No published data or ESGF records were changed. Replacement versions and ESGF erratum identifiers remain unset.
