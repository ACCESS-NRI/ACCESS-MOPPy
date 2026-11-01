# ISIMIP4b ocean biogeochemistry gap analysis

Status: 2 November 2026. Scope: the 17 `ocnBgchem` fields in section 5 of the [ISIMIP ACCESS-ESM1-6 request](https://docs.google.com/document/d/1C-reMPS6hWOvWYIMs3NTWfvzcywzpD4BD08ISU56xgo/edit?tab=t.0). Requested experiments are `piControl`, `historical`, `scen7-vl`, and `scen7-h`, with emission-driven counterparts where available. The request marks `chl`, `expc`, `intpp`, `o2`, `ph`, and `zooc` as Planned; the others are TBC. These are request statuses, not evidence of published or validated data.

## Findings by field

| Requested field | Assessment | Required work |
| --- | --- | --- |
| `chl` | Existing `Omon.chl` mapping from monthly 3D `pchl`; listed in the release batch candidates. | One-year archive pilot, units and CMOR/QC check, then process requested experiments. |
| `expc` | Existing `Omon.expc` mapping incorrectly selects `st_ocean=0`, producing a surface field for a full-depth (`ol`) request. It multiplies monthly mean `det` and monthly mean `det_vmove`; that generally differs from the monthly mean sinking flux. | Prefer a saved depth-resolved flux diagnostic. Otherwise verify whether higher-frequency `det` and sinking speed exist and compute the product before monthly averaging. Fix the mapping's vertical dimension and formula only after that check. |
| `intpoc` (`30S-90S`) | An `Omon.intpoc` mapping exists but asks for `poc`, which is not one of WOMBATlite's documented tracers. The current formula vertically sums that nonexistent `poc` input and does not implement the southern regional subset. | Agree a scientifically valid particulate organic carbon definition from the available pools; check the integration and units; add/verify regional extraction. Do not publish the current mapping. |
| `intpp` | Existing `Omon.intpp` mapping uses `pprod_gross_2d` directly. The CMIP7 target is **net carbon** production; the native field's name denotes **gross** production. Its input dimension map also includes an unnecessary depth key for this 2D source. | Check for archived `npp2d` or `npp3d` (net) and native units; convert/integrate to mol C m-2 s-1, then correct the mapping and QC. Do not publish `pprod_gross_2d` as `intpp`. |
| `o2` | Existing `Omon.o2` mapping from monthly 3D `o2`; listed in release batch candidates. | One-year archive pilot and QC. |
| `ph` | Existing `Omon.ph` mapping from 3D `htotal`; listed in release batch candidates. | Check total-scale interpretation, units, and the effect of taking `-log10` after monthly averaging rather than averaging instantaneous pH. Pilot and QC. |
| `phyc` | WOMBATlite has one phytoplankton carbon pool (`phy`); the previous MOPPy entry `phycos` has an input/formula name mismatch and does not map the requested `phyc`. | This change adds `Omon.phyc` from 3D `phy` with the same 1035 kg m-3 density convention as `zooc`. The eight requested experiment templates now include `phyc` as a local addition. Verify `phy` exists and has mol C kg-1 units in each target archive; pilot and QC before full processing. |
| `zooc` | Existing `Omon.zooc` mapping from monthly 3D `zoo`; listed in release batch candidates. | One-year archive pilot and QC. |
| `intppdiat`, `intppdiaz`, `intppmisc`, `intpppico` | WOMBATlite has one phytoplankton pool; no separately simulated production for these functional types. Implicit nitrogen fixation is not diazotroph carbon production. | Cannot reconstruct these model-defined components from existing archives. Report unavailable for ACCESS-ESM1.6; a future simulation would need a model with explicit groups and suitable diagnostics. |
| `phydiat`, `phymisc`, `phypico` | No corresponding separate phytoplankton carbon pools in WOMBATlite. | Cannot reconstruct from existing archives; report unavailable. |
| `zmeso`, `zmicro` | WOMBATlite has one zooplankton carbon pool, so it has no distinct size classes. | Cannot reconstruct from existing archives; report unavailable. |

## Processing order and evidence gates

1. Confirm the archived member's release tag and native monthly fields for each requested experiment. The repository's additional batch YAMLs are *candidates* based on released diagnostic profiles, not proof that a given archive contains the fields.
2. Pilot `chl`, `o2`, `zooc`, and `ph` on one year; pilot the new `phyc` mapping only where `phy` is saved. Check `st_ocean`, units, values, and CMIP7 compliance before scheduling full runs.
3. Resolve the `expc`, `intpoc`, and `intpp` science and source-data issues above before changing their production mappings. In particular, avoid publishing a gross field under a net name or a surface field under a full-depth identifier.
4. Record the nine functional-group requests as structurally unavailable with WOMBATlite. Do not manufacture a split of the total pools.

No Gadi archive is mounted in this local environment (`/g/data` is absent), so no native-file or end-to-end CMOR pilot was possible here. The patch adds the defensible `phyc` mapping and a focused synthetic formula test, lists `phyc` in the eight requested batch templates, and holds `expc` and `intpp` in all coupled additional templates pending source-data and science checks. It does not change publication status.

## Sources

- [ISIMIP4b ACCESS-ESM1-6 data request](https://docs.google.com/document/d/1C-reMPS6hWOvWYIMs3NTWfvzcywzpD4BD08ISU56xgo/edit?tab=t.0)
- [WOMBATlite model description: tracers and diagnostics](https://wombat-docs.readthedocs.io/stable/Model_description/WOMBATlite_model_description/)
- [ACCESS-Hive ACCESS-ESM1.6 model components](https://docs.access-hive.org.au/models/access_models/access-esm/)
- [piControl release standard MOM5 diagnostic profile](https://github.com/ACCESS-NRI/access-esm1.6-configs/blob/3233ef4241726324f0093597d11815b6d75f641a/ocean/diagnostic_profiles/diag_table_standard)
- [Scenario release standard MOM5 diagnostic profile](https://github.com/ACCESS-NRI/access-esm1.6-configs/blob/4562a5344f664b9eadcb4b67efd979abc3b10d09/ocean/diagnostic_profiles/diag_table_standard)
- [CMIP7 `ocnBgchem` table](../src/access_moppy/vocabularies/cmip7-cmor-tables/tables/CMIP7_ocnBgchem.json)
- [ACCESS-ESM1.6 mappings](../src/access_moppy/mappings/ACCESS-ESM1-6_mappings.json)
- [Release batch candidate notes](../src/access_moppy/examples/README_esm1-6_cmip7_additional.md)
