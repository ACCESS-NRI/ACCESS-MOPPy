# Erratum: older sea-ice snow thickness still blows up where almost no ice remains

The old recipe divides snow mass by tiny positive ice concentrations. Near-zero numerical leftovers become huge snow thicknesses. It also uses snow density 317 instead of the model's 330 kg/m³ and turns missing concentration into zero snow.

For example: A sampled historical field contains 12,519.95 m of snow on sea ice. This is a tiny-denominator artefact, not a kilometre-high snowpack, and it can dominate an average.

**Recommended seriousness:** Critical. **Status:** DRAFT — not submitted to ESGF.

## Evidence and scope

The piControl January 0101 field reaches 203.27 m; its ten cells above 10 m have ice concentrations of only 3.27×10⁻⁹–2.70×10⁻⁷ percent. Missing ice over 38,218 land cells becomes unmasked zero snow. A historical r6 sample reaches 12,519.95 m. Separately, density 317 kg m⁻³ was used instead of the CICE5 value 330, making ordinary positive thickness 4.10% high.

Complete file list within the identified release versions; numerical confirmation was sampled, not a scan of every value.

Inventory scope: **48 dataset versions / 1,081 file references**. [Exact list](affected-files.txt) · [Dataset/version inventory](datasets.json) · [Machine-readable draft](erratum-draft.json). List SHA-256: `5ba911c299ad533e5ef26db8fa7111d27c09aa6353b56ecdbb493f5f02230c31`.

## Who needs to act?

The threshold/mask and CICE5 snow-density corrections are already merged in PRs #812/#813 and released in MOPPy 1.9.0. Publication team: regenerate the 48 old releases. Native CICE density 330 kg/m3 is documented; no model-code repair is indicated.

## Correction and acceptance

Use the fixes in #812/#813: model snow density 330 kg m⁻³, the approved 0.01% concentration threshold, and propagation of missing inputs. Regenerate affected versions and validate masks as well as extrema. Multiplying by 317/330 alone is insufficient.

## Gadi validation

Fresh January 0101 Gadi output lowers the old 203.274 m maximum to 3.644 m, removes all ten values above 10 m in that sample, restores 38,218 land masks and applies the 0.01% ice-presence threshold. All retained positive thicknesses have the expected 317/330 density adjustment; 29 focused tests passed.

[Implementation or validation PR](https://github.com/ACCESS-NRI/ACCESS-MOPPy/pull/872) at `b0abc3050288d419384006d7aff9c45f9137a278`.

Tracking: [835](https://github.com/ACCESS-NRI/ACCESS-MOPPy/issues/835) · Audit ID **C03**.

No published data or ESGF records were changed. Replacement versions and ESGF erratum identifiers remain unset.
