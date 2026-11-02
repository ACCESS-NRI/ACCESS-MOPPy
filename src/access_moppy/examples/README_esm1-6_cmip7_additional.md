# ACCESS-ESM1.6 CMIP7 release batches

The `batch_config_esm1-6_cmip7_<configuration>_additional.yml` files provide candidates **beyond** the [generic ACCESS-ESM1.6 baseline example](batch_config_esm1-6_cmip7_baseline.yml). There is one file for every ACCESS-NRI [released configuration](https://github.com/ACCESS-NRI/access-esm1.6-configs) available on 24 October 2026. Each file header records the exact release tag and selected atmosphere, ocean, and sea-ice diagnostic profiles.

| Configuration | Release tag | Additional request candidates |
| --- | --- | ---: |
| `piControl` | `release-piControl-2.1` | 116 |
| `esm-piControl` | `release-esm-piControl-2.1` | 117 |
| `historical` | `release-historical-1.3` | 127 |
| `esm-historical` | `release-esm-historical-1.3` | 129 |
| `1pctCO2` | `release-1pctCO2-1.0` | 110 |
| `1pctCO2-bgc` | `release-1pctCO2-bgc-1.0` | 132 |
| `1pctCO2-rad` | `release-1pctCO2-rad-1.0` | 132 |
| `abrupt-4xCO2` | `release-abrupt-4xCO2-1.0` | 110 |
| `abrupt-2xCO2` | `release-abrupt-2xCO2-2.0` | 119 |
| `abrupt-0p5xCO2` | `release-abrupt-0p5xCO2-2.0` | 119 |
| `scen7-h` | `release-scen7-h-1.0` | 125 |
| `scen7-vl` | `release-scen7-vl-1.0` | 125 |
| `esm-scen7-h` | `release-esm-scen7-h-1.0` | 127 |
| `esm-scen7-vl` | `release-esm-scen7-vl-1.0` | 127 |
| `esm-flat10` | `release-esm-flat10-1.0` | 127 |
| `esm-flat10-cdr` | `release-esm-flat10-cdr-1.0` | 127 |
| `esm-flat10-zec` | `release-esm-flat10-zec-1.0` | 127 |
| `amip` | `release-amip-1.0` | 45 |

Counts include candidates held as comments. Selection of the request candidates used CMIP7 Data Request `v1.2.2.5` (all priorities and opportunities) and the ACCESS-ESM1.6 mappings in this repository. Each request candidate is requested for its experiment, is outside the generic baseline example, has every mapped native input in the selected STASH, MOM5, or CICE profile at the requested cadence, and has a built-in MOPPy file search. Known pressure/model-level, climatology, depth-selection, and regional-subset mapping mismatches were excluded where relevant. Native fields requiring aggregation, missing outputs, and tables without MOPPy discovery rules were also excluded.

The 17 coupled configurations also include `ocean.rsntds.tavg-u-hxy-sea.mon.glb` as a local extension outside the CMIP7 Data Request priority groups. It uses monthly ocean `swflx`; confirm that field exists in the member's selected ocean output before running the batch. The `amip` configuration has no ocean output and excludes this field. The table counts above exclude this extension. The eight ISIMIP4b experiment templates (`piControl`, `esm-piControl`, `historical`, `esm-historical`, `scen7-h`, `scen7-vl`, `esm-scen7-h`, and `esm-scen7-vl`) also include monthly 3D `ocnBgchem.phyc` from the native `phy` diagnostic as a local addition excluded from the counts.

`ocnBgchem.expc` and `ocnBgchem.intpp` are held as comments in the coupled additional templates: the current `expc` mapping produces a surface field and uses the product of monthly means, while the `intpp` mapping uses gross rather than net production. Their resource overrides are removed. See the [ISIMIP4b ocnBgchem gap analysis](../../../reports/isimip4b_ocnBgchem_gap_analysis.md) for all 17 requested fields and evidence needed to enable the held variables.

The added monthly `od550dust` and `ztp` mappings are requested in all listed experiments; `prra` is included only where the Data Request requests its global monthly field. The proposed `hfdsn`, `fco2fos`, and `co2s` mappings remain excluded pending scientific validation.

`Emon.rsds` is enabled with a separate land-only recipe: native incoming shortwave radiation (`fld_s01i235`) is retained where native land fraction (`fld_s03i395`) is positive, with pure-ocean cells marked missing. Coastal values are not multiplied or divided by land fraction. Regenerate earlier `Emon.rsds` output to remove unmasked ocean values.

`LImon.sbl` is also enabled: it uses tile sublimation, masks ocean and snow-free land, and obtains native grid resolution from CABLE while retaining the `landIce` output realm. `Eday.clt` has a land-mask recipe. `Eday.evspsbl` and `3hr.tran` have separate tile recipes, but require native diagnostics saved daily or three-hourly; the sample archive only supplies their fluxes monthly.

`Emon.rsus` remains held pending verified land-only radiation diagnostics ([#803](https://github.com/ACCESS-NRI/ACCESS-MOPPy/issues/803)). The ambiguous `Eday.sbl` mapping also remains blocked. A metadata compliance pass does not establish the correct averaging domain. Previously generated `Emon.rsus` output should be withheld from publication and regenerated once its recipe is verified. See the [mapping reference](https://access-moppy.readthedocs.io/en/latest/reference/mapping_reference.html) for calculations and required inputs.

These are **pilot templates**. A released configuration's output definition does not establish which tag an archived ensemble member used, or prove that the files exist and CMORise successfully. Confirm the run's tag, set `input_folder`, `output_folder`, PBS project, member label, and parent lineage as applicable, then run a one-year pilot and QC before scheduling the whole archive. MOPPy selects `activity_id` from the CMIP7 experiment vocabulary. The commented `start_year` and `end_year` entries can bound that pilot to an available model year. `esm-historical` uses CMIP7 experiment ID `esm-hist` in its YAML.

The 1pctCO2 and abrupt-4xCO2 releases select subdaily atmosphere, WombatLite ocean, and CMIP high ice profiles; piControl and historical select standard profiles. The selected profiles, rather than just the experiment name, determine which output can be CMORised from an archive.
