# Soil-layer thickness files call layer numbers depths in metres

The static resource numbers its layers 1 to 6. Publication then labels those numbers as physical depth in metres, although the whole soil column is only 4.6 m deep.

For example: Matching slthick to soil temperature by depth can pair the wrong layers or find no match at all. The thickness values are sensible; it is the coordinate describing where those values belong that is wrong.

**Recommended seriousness:** High. **Status:** DRAFT — not submitted to ESGF.

## Evidence and scope

All 138 fixed-file headers have the same false physical coordinate. The resource supplies dimensionless sdepth indices; publication relabels them as depth in metres. The sampled thickness data themselves are the correct [.022,.058,.154,.409,1.085,2.872] m.

Complete file list within the identified release versions; numerical confirmation was sampled, not a scan of every value.

Inventory scope: **138 dataset versions / 138 file references**. [Exact list](affected-files.txt) · [Dataset/version inventory](datasets.json) · [Machine-readable draft](erratum-draft.json). List SHA-256: `7b2f366f83e1eb20391fdf93afc38db72a5fe8dada4c08fb5b61eb3c8c64b573`.

## Who needs to act?

MOPPy: apply shared CABLE physical centres/bounds to the slthick resource and mask its all-zero ocean columns. Both supported models use this resource; the implementation covers both. Native model geometry is consistent, so no model-code change is indicated.

## Correction and acceptance

Assign the shared native CABLE centres and bounds before publication. Check missing ocean values as well: the sampled resource-derived thickness also uses zero outside land. Preserve real layer thicknesses.

## Gadi validation

Fresh Gadi CMIP7 and CMIP6 slthick outputs preserve all 65,190 valid layer thicknesses, restore 16,975 ocean-column masks and align with fresh tsl/mrsol coordinates. All 277 focused tests passed.

[Implementation or validation PR](https://github.com/ACCESS-NRI/ACCESS-MOPPy/pull/869) at `c2711693ed375797d9bf17c0bdd0a0ffb83c6f5d`.

Tracking: [860](https://github.com/ACCESS-NRI/ACCESS-MOPPy/issues/860) · Audit ID **H03**.

No published data or ESGF records were changed. Replacement versions and ESGF erratum identifiers remain unset.
