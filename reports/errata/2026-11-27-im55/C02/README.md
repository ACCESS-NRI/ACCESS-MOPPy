# Erratum: some monthly topsoil moisture still includes too much soil

The file says water in the top 10 cm, but the older calculation takes too much of the third soil layer. The code was fixed, but older monthly publications were not all replaced.

For example: In January 0101 piControl, affected land cells contain 39–86% more water than the correctly integrated top 10 cm. A user comparing daily and monthly topsoil could mistake processing differences for climate behaviour.

**Recommended seriousness:** Critical. **Status:** DRAFT — not submitted to ESGF.

## Evidence and scope

Native CABLE interfaces are 0, .022, .080, .234, .643, 1.728 and 4.6 m. Correct topsoil mass is layer1 + layer2 + layer3 × .020/.154. The old recipe uses (.1−.051)/(.157−.051), including 46.2% of layer 3 instead of 13.0%. January 0101 piControl matches that old recipe within 1.31×10⁻⁶ kg m⁻². Its published/correct ratio is 1.391–1.861 over 10,865 land cells; the unweighted mean ratio is 1.536. Another 16,975 missing ocean cells become zero. All 58 first-file headers retain six orphan depth indices labelled cm.

Complete file list within the identified release versions; numerical confirmation was sampled, not a scan of every value.

Inventory scope: **58 dataset versions / 1,141 file references**. [Exact list](affected-files.txt) · [Dataset/version inventory](datasets.json) · [Machine-readable draft](erratum-draft.json). List SHA-256: `6df87aaaf1a9a7d2d6f645a2b4f76a47d6a39add77bd74e369804694c0222c5c`.

## Who needs to act?

The source correction is already merged in PR #819 and released in MOPPy 1.9.0. Publication team: regenerate the 58 older monthly releases. The 80 ScenarioMIP monthly and 18 daily releases with corrected sampled geometry are excluded from this manifest. No CABLE model-code change is indicated.

## Correction and acceptance

Regenerate the 58 affected monthly datasets using the corrected native-interface integration. Write scalar depth 5 cm with 0–10 cm bounds and preserve missing columns. A constant scale factor cannot repair spatially varying soil profiles.

## Gadi validation

Fresh Gadi outputs from one real archived month and one real archived day exactly match the independent top-10 cm integral on 10,865 land cells, preserve 16,975 ocean masks, and write scalar depth 5 cm with bounds 0–10 cm. The old monthly output remains 39–86% high in that sample; 132 focused tests passed.

[Implementation or validation PR](https://github.com/ACCESS-NRI/ACCESS-MOPPy/pull/870) at `26ff8fd44814d56da795e5302edb5554e638cf11`.

Tracking: [859](https://github.com/ACCESS-NRI/ACCESS-MOPPy/issues/859) · Audit ID **C02**.

No published data or ESGF records were changed. Replacement versions and ESGF erratum identifiers remain unset.
