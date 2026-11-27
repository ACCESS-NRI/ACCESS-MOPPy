# The phosphate numbers are about a thousand times too small

We convert nitrate from an amount per kilogram of seawater to an amount per cubic metre, but skip that conversion when deriving phosphate from the same nitrate. The label says mol/m³ even though the number is still on the per-kilogram scale.

For example: If converted nitrate is 16 units, the model's 16:1 ratio calls for phosphate of 1 unit. The current output is about 0.000966 instead. That can make a nutrient inventory look almost empty.

**Recommended seriousness:** Critical. **Status:** DRAFT — not submitted to ESGF.

## Evidence and scope

The native nitrate input is in mol kg⁻¹. Published po4 is exactly native no3 / 16, while published no3 includes the model density factor 1,035. In January 0101 piControl, 2,665,319 valid 3-D cells match native no3 / 16 exactly; no3 / (16 × po4) is 1,034.99985–1,035.00008. Surface comparisons at the first and last months of both the first and last files pass the same factor test in all six published experiments (24 comparisons).

Complete file list within the identified release versions; numerical confirmation was sampled, not a scan of every value.

Inventory scope: **6 dataset versions / 301 file references**. [Exact list](affected-files.txt) · [Dataset/version inventory](datasets.json) · [Machine-readable draft](erratum-draft.json). List SHA-256: `b8046c916372794c4844e13da0465e3f290e0808bce6271bd0b5ac10ab825814`.

## Who needs to act?

MOPPy: review the model-specific phosphate mapping repair. Gadi native units confirm ESM1-6 needs nitrate ×1035/16, while ESM1-5 needs nitrate ×0.001/16 because that archive uses mmol/m3. No model-physics defect was established. Publication team: replace the six listed ESM1-6 releases; ESM1-5 publication impact still needs a separate audit.

## Correction and acceptance

Correct the mapping to native no3 × 1035 / 16 (or the scientifically approved density convention), preserve masks and regenerate. Check every output against published no3 / 16, including deep water. Do not alter correctly converted no3 or other tracers by the same factor.

## Gadi validation

Fresh CMIP7 ESM1-6 and CMIP6 ESM1-5 outputs were checked through all 50 depths against native units and independent conversion. The ESM1-6 output agrees with published nitrate/16 within 4.66e-10 mol/m3. Masks agree; 227 focused tests passed.

[Implementation or validation PR](https://github.com/ACCESS-NRI/ACCESS-MOPPy/pull/868) at `cb9ca60cdf099decec39225bcd83e82464c6b10f`.

Tracking: [856](https://github.com/ACCESS-NRI/ACCESS-MOPPy/issues/856) · Audit ID **C01**.

No published data or ESGF records were changed. Replacement versions and ESGF erratum identifiers remain unset.
